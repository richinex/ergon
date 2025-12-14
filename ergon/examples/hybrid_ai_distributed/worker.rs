//! Hybrid AI Worker - Executes flows scheduled by scheduler.rs
//!
//! This example demonstrates the WORKER side of a distributed ergon application.
//! It polls the queue for flows and executes them using registered flow types.
//!
//! **IMPORTANT**: In a real application, YOU build this worker binary in YOUR project.
//! Ergon provides the `Worker::new()` abstraction, YOU provide the flow types.
//!
//! # Architecture
//!
//! ```
//! ┌─────────────────────┐
//! │  Scheduler Binary   │
//! │  (see scheduler.rs) │
//! └──────┬──────────────┘
//!        │
//!        │ Postgres (shared state)
//!        │
//! ┌──────▼──────────────┐
//! │  THIS BINARY        │
//! │  (Worker)           │
//! │                     │
//! │  1. Poll queue      │
//! │  2. Execute flows   │
//! │  3. Handle signals  │
//! └─────────────────────┘
//! ```
//!
//! # Usage
//!
//! Run directly:
//! ```bash
//! DATABASE_URL=postgres://ergon:ergon@localhost:5432/ergon \
//! cargo run --example hybrid_ai_distributed_worker --features postgres
//! ```
//!
//! Or via Docker (see Dockerfile.hybrid-ai-worker):
//! ```bash
//! docker-compose -f docker-compose.hybrid-ai.yml up
//! ```

mod flows;

use ergon::executor::Worker;
use ergon::prelude::*;
use flows::{AuditLogFlow, ContentFlow};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let worker_id = std::env::var("WORKER_ID").unwrap_or_else(|_| {
        warn!("WORKER_ID not set, using default");
        "hybrid-ai-worker".to_string()
    });

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://ergon:ergon@localhost:5432/ergon".to_string());

    info!(worker_id = %worker_id, database_url = %database_url, "Starting hybrid AI worker");

    // Create storage
    #[cfg(feature = "postgres")]
    let storage = Arc::new(ergon::storage::PostgresExecutionLog::new(&database_url).await?);

    #[cfg(not(feature = "postgres"))]
    compile_error!("This example requires the 'postgres' feature");

    storage.ping().await?;
    info!("Connected to Postgres");

    // Create worker
    let worker = Worker::new(storage, &worker_id).with_poll_interval(Duration::from_millis(100));

    // Register YOUR flow types
    // In a real app, you'd import these from your flows library crate
    worker.register(|f: Arc<ContentFlow>| f.process()).await;
    worker.register(|f: Arc<AuditLogFlow>| f.run_audit()).await;
    info!("Registered ContentFlow and AuditLogFlow");

    // Start worker
    let handle = worker.start().await;
    info!("Worker started, waiting for flows");

    // Wait for shutdown
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received SIGINT, shutting down");
        }
        _ = shutdown_signal() => {
            info!("Received SIGTERM, shutting down");
        }
    }

    handle.shutdown().await;
    info!("Shutdown complete");
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM");
    sigterm.recv().await;
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    std::future::pending::<()>().await
}
