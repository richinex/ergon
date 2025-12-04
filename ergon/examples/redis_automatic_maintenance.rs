//! Redis automatic maintenance example
//!
//! This example demonstrates how the Worker automatically handles Redis maintenance:
//! - Moving ready delayed tasks from delayed queue to pending queue (every 1s)
//! - Recovering flows locked by crashed workers (every 60s)
//!
//! ## Prerequisites
//!
//! Start Redis:
//! ```bash
//! docker run --rm -p 6379:6379 redis:latest
//! ```
//!
//! ## Run
//!
//! ```bash
//! RUST_LOG=ergon=debug cargo run --example redis_automatic_maintenance --features redis
//! ```

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use chrono::Utc;

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct DelayedTaskFlow {
    task_id: u32,
    scheduled_for: String,
}

impl DelayedTaskFlow {
    #[flow]
    async fn execute(self: Arc<Self>) -> Result<String, String> {
        println!("[Flow] Executing delayed task {}", self.task_id);
        self.complete().await
    }

    #[step]
    async fn complete(self: Arc<Self>) -> Result<String, String> {
        Ok(format!("Task {} completed", self.task_id))
    }
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct LongRunningFlow {
    job_id: u32,
}

impl LongRunningFlow {
    #[flow]
    async fn execute(self: Arc<Self>) -> Result<String, String> {
        println!("[Flow] Starting long-running job {}", self.job_id);
        self.process().await
    }

    #[step]
    async fn process(self: Arc<Self>) -> Result<String, String> {
        tokio::time::sleep(Duration::from_secs(90)).await;
        Ok(format!("Job {} completed", self.job_id))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Ergon Redis Automatic Maintenance Example ===\n");

    println!("This example demonstrates automatic Redis maintenance:");
    println!("  - Delayed task processing (every 1s)");
    println!("  - Stale lock recovery (every 60s)");
    println!("  - No manual setup required\n");

    // Create Redis storage
    let storage = Arc::new(RedisExecutionLog::new("redis://127.0.0.1:6379").await?);
    println!("1. Connected to Redis");

    // Create scheduler
    let scheduler = Scheduler::new(storage.clone());
    println!("2. Created scheduler");

    // Create worker
    let worker = Worker::new(storage.clone(), "worker-1")
        .with_timers()
        .with_structured_tracing();

    // Register flow types
    worker
        .register(|flow: Arc<DelayedTaskFlow>| flow.execute())
        .await;

    worker
        .register(|flow: Arc<LongRunningFlow>| flow.execute())
        .await;

    println!("3. Registered flow types");

    // Start the worker
    let handle = worker.start().await;
    println!("4. Worker started (maintenance running automatically)\n");

    // Schedule some delayed tasks
    println!("Scheduling delayed tasks...");
    for i in 1..=3 {
        let delay_seconds = i * 2;
        let scheduled_at = Utc::now() + chrono::Duration::seconds(delay_seconds as i64);

        let flow = DelayedTaskFlow {
            task_id: i,
            scheduled_for: scheduled_at.to_rfc3339(),
        };

        let flow_id = Uuid::new_v4();
        scheduler.schedule(flow, flow_id).await?;

        println!(
            "   Task {} scheduled for {} (in {}s)",
            i, scheduled_at.format("%H:%M:%S"), delay_seconds
        );
    }

    println!("\nWaiting for delayed tasks to execute...");
    println!("(Watch for debug logs showing maintenance moving tasks to pending)\n");

    tokio::time::sleep(Duration::from_secs(10)).await;

    println!("All delayed tasks processed\n");

    println!("Scheduling long-running flow for stale lock demonstration");
    let long_flow = LongRunningFlow { job_id: 999 };
    let flow_id = Uuid::new_v4();
    scheduler.schedule(long_flow, flow_id).await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("\nShutting down worker...");
    handle.shutdown().await;
    println!("Worker stopped");

    Ok(())
}
