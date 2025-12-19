//! Hybrid AI Scheduler - Schedules flows to external worker
//!
//! This example demonstrates the SCHEDULER side of a distributed ergon application.
//! It schedules flows and delivers signals, but does NOT execute flows itself.
//!
//! # Architecture
//!
//! ```
//! ┌─────────────────────┐
//! │  THIS BINARY        │
//! │  (Scheduler)        │
//! │                     │
//! │  1. Schedule flows  │
//! │  2. Deliver signals │
//! │  3. Monitor status  │
//! └──────┬──────────────┘
//!        │
//!        │ Postgres (shared state)
//!        │
//! ┌──────▼──────────────┐
//! │  Worker Binary      │
//! │  (see worker.rs)    │
//! │                     │
//! │  - Polls queue      │
//! │  - Executes flows   │
//! └─────────────────────┘
//! ```
//!
//! # Usage
//!
//! 1. Start infrastructure:
//!    ```bash
//!    docker-compose -f docker-compose.hybrid-ai.yml up -d
//!    ```
//!
//! 2. Run this scheduler:
//!    ```bash
//!    DATABASE_URL=postgres://ergon:ergon@localhost:5432/ergon \
//!    cargo run --example hybrid_ai_distributed_scheduler --features postgres
//!    ```

mod flows;

use ergon::executor::Scheduler;
use ergon::prelude::*;
use flows::{ContentFlow, HumanDecision};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://ergon:ergon@localhost:5432/ergon".to_string());

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ Trust & Safety Pipeline (Scheduler)                       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");
    println!("Using external workers from docker-compose");
    println!("Database: {}\n", database_url);

    // Connect to Postgres
    #[cfg(feature = "postgres")]
    let storage = Arc::new(ergon::storage::PostgresExecutionLog::new(&database_url).await?);

    #[cfg(not(feature = "postgres"))]
    compile_error!("This example requires the 'postgres' feature");

    storage.ping().await?;
    println!("✓ Connected to Postgres\n");

    // Create scheduler (no worker!)
    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    // Schedule the 3 flows
    println!("Scheduling flows...");

    let flow_safe = ContentFlow {
        content_id: "POST-SAFE".to_string(),
        text: "\x05".to_string(),
    };
    scheduler.schedule_with(flow_safe, Uuid::new_v4()).await?;
    println!("  ✓ Scheduled POST-SAFE");

    let flow_toxic = ContentFlow {
        content_id: "POST-TOXIC".to_string(),
        text: "a".to_string(),
    };
    scheduler.schedule_with(flow_toxic, Uuid::new_v4()).await?;
    println!("  ✓ Scheduled POST-TOXIC");

    let flow_review = ContentFlow {
        content_id: "POST-AMBIGUOUS".to_string(),
        text: "Rust".to_string(),
    };
    scheduler.schedule_with(flow_review, Uuid::new_v4()).await?;
    println!("  ✓ Scheduled POST-AMBIGUOUS\n");

    // Simulate human interaction
    println!("Waiting for flows to reach human review stage...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("\n[SYSTEM] Finding flow waiting for signal...");
    let signals = storage.get_waiting_signals().await?;

    if let Some(signal_info) = signals.iter().find(|s| {
        s.signal_name
            .as_ref()
            .map(|n| n == "review:POST-AMBIGUOUS")
            .unwrap_or(false)
    }) {
        println!(
            "  Found POST-AMBIGUOUS waiting at flow_id={}, step={}",
            signal_info.flow_id, signal_info.step
        );

        let decision = HumanDecision { approved: true };
        let signal_data = ergon::core::serialize_value(&decision)?;

        storage
            .store_signal_params(
                signal_info.flow_id,
                signal_info.step,
                "review:POST-AMBIGUOUS",
                &signal_data,
            )
            .await?;

        // Resume the flow so the worker picks it up
        storage.resume_flow(signal_info.flow_id).await?;

        println!("  ✓ Human approved POST-AMBIGUOUS\n");
    } else {
        println!("  Warning: POST-AMBIGUOUS not yet waiting for signal\n");
    }

    // Wait for all flows to complete
    println!("Waiting for external workers to complete flows...");

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let pending = storage.get_incomplete_flows().await?;

        if pending.is_empty() {
            println!("\n✓ All flows completed successfully!");
            break;
        } else {
            print!("  Still running: {} flows\r", pending.len());
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }

    println!("\n\nModeration pipeline complete.");
    println!("\nNote: External workers are still running in docker-compose.");
    println!("Stop them with: docker-compose -f docker-compose.hybrid-ai.yml down");

    Ok(())
}
