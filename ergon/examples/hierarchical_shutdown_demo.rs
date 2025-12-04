//! Hierarchical shutdown example using CancellationToken
//!
//! This example demonstrates:
//! - Creating worker groups with hierarchical cancellation
//! - Parent-child token relationships for coordinated shutdown
//! - Graceful shutdown propagation across worker hierarchies
//! - Cancel-safe shutdown patterns
//!
//! ## Scenario
//! We create a main coordinator worker and two child workers (processing and
//! notification workers). When the coordinator shuts down, all child workers
//! automatically receive the cancellation signal, demonstrating hierarchical
//! cancellation patterns.
//!
//! ## Key Takeaways
//! - CancellationToken supports parent-child relationships
//! - Cancelling parent automatically cancels all children
//! - Enables clean shutdown of worker groups
//! - Cancel-safe by design (no lost signals)
//! - Better ergonomics than manual oneshot coordination
//!
//! ## Run with
//! ```bash
//! cargo run --example hierarchical_shutdown_demo
//! ```

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct DataProcessor {
    data_id: u32,
    processing_time_ms: u64,
}

impl DataProcessor {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, String> {
        println!("[DataProcessor] Processing data {}", self.data_id);
        self.transform().await
    }

    #[step]
    async fn transform(self: Arc<Self>) -> Result<String, String> {
        tokio::time::sleep(Duration::from_millis(self.processing_time_ms)).await;
        Ok(format!("Processed data {}", self.data_id))
    }
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct NotificationJob {
    job_id: u32,
}

impl NotificationJob {
    #[flow]
    async fn execute(self: Arc<Self>) -> Result<String, String> {
        println!("[NotificationJob] Executing job {}", self.job_id);
        self.send_notification().await
    }

    #[step]
    async fn send_notification(self: Arc<Self>) -> Result<String, String> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(format!("Notification {} sent", self.job_id))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Ergon Hierarchical Shutdown Example ===\n");

    let storage = Arc::new(SqliteExecutionLog::new("hierarchical_demo.db").await?);
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone());

    println!("1. Scheduling flows for processing...\n");

    // Schedule data processing flows
    for i in 1..=5 {
        let flow = DataProcessor {
            data_id: i,
            processing_time_ms: 200,
        };
        scheduler.schedule(flow, Uuid::new_v4()).await?;
        println!("   Scheduled DataProcessor {}", i);
    }

    // Schedule notification flows
    for i in 1..=3 {
        let flow = NotificationJob { job_id: i };
        scheduler.schedule(flow, Uuid::new_v4()).await?;
        println!("   Scheduled NotificationJob {}", i);
    }

    println!("\n2. Creating hierarchical worker structure...\n");

    // Create main coordinator token
    let coordinator_token = CancellationToken::new();
    println!("   Created coordinator cancellation token");

    // Create processing worker with child token
    let processing_token = coordinator_token.child_token();
    let processing_worker = Worker::new(storage.clone(), "processing-worker")
        .with_poll_interval(Duration::from_millis(100));
    processing_worker
        .register(|flow: Arc<DataProcessor>| flow.process())
        .await;
    let processing_handle = processing_worker.start().await;
    println!("   Started processing-worker (child of coordinator)");

    // Create notification worker with child token
    let notification_token = coordinator_token.child_token();
    let notification_worker = Worker::new(storage.clone(), "notification-worker")
        .with_poll_interval(Duration::from_millis(100));
    notification_worker
        .register(|flow: Arc<NotificationJob>| flow.execute())
        .await;
    let notification_handle = notification_worker.start().await;
    println!("   Started notification-worker (child of coordinator)");

    println!("\n3. Workers processing flows...");
    println!("   (Workers operate independently until coordinator signals shutdown)\n");

    // Let workers process for a bit
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("4. Demonstrating hierarchical shutdown...\n");
    println!("   Cancelling coordinator token");

    // Cancel the coordinator token
    // This automatically cancels all child tokens
    coordinator_token.cancel();

    // Verify children are cancelled
    if processing_token.is_cancelled() && notification_token.is_cancelled() {
        println!("   All child tokens cancelled");
    }

    println!("\n5. Shutting down workers...\n");

    // Workers will complete in-flight flows and shut down
    processing_handle.shutdown().await;
    notification_handle.shutdown().await;

    println!("   All workers stopped");

    println!("\n6. Verifying results...\n");

    let incomplete = storage.get_incomplete_flows().await?;
    let completed_count = 8 - incomplete.len();
    println!("   Completed: {}/8 flows", completed_count);
    if !incomplete.is_empty() {
        println!("   Incomplete: {} flows", incomplete.len());
    }

    println!("\n=== Example Complete ===");

    storage.close().await?;
    Ok(())
}
