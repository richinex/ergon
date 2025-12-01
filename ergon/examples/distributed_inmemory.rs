//! In-memory distributed worker example
//!
//! This example demonstrates distributed execution using InMemoryExecutionLog,
//! which is useful for testing without requiring SQLite files.
//!
//! Note: InMemory storage is only suitable for single-process testing.
//! For true multi-machine distribution, use SqliteExecutionLog or PostgreSQL.
//!
//! Run with: cargo run --example distributed_inmemory

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone)]
struct ComputeTask {
    task_id: String,
    value: i32,
}

impl ComputeTask {
    #[flow]
    async fn compute(self: Arc<Self>) -> Result<i32, String> {
        println!("[Flow {}] Starting computation", self.task_id);

        let doubled = self.clone().double().await?;
        let result = self.clone().add_ten(doubled).await?;

        println!("[Flow {}] Completed: {}", self.task_id, result);
        Ok(result)
    }

    #[step]
    async fn double(self: Arc<Self>) -> Result<i32, String> {
        println!("[Step] Doubling {} -> {}", self.value, self.value * 2);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(self.value * 2)
    }

    #[step]
    async fn add_ten(self: Arc<Self>, input: i32) -> Result<i32, String> {
        println!("[Step] Adding 10: {} -> {}", input, input + 10);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(input + 10)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Ergon In-Memory Distributed Worker Example ===\n");

    // Create in-memory storage (no files!)
    let storage = Arc::new(InMemoryExecutionLog::new());

    println!("1. Creating scheduler with in-memory storage...");
    let scheduler = FlowScheduler::new(storage.clone());

    println!("2. Scheduling compute tasks...\n");

    // Schedule multiple tasks
    for i in 1..=5 {
        let task = ComputeTask {
            task_id: format!("TASK-{:03}", i),
            value: i * 10,
        };

        let flow_id = Uuid::new_v4();
        let task_id = scheduler.schedule(task, flow_id).await?;
        println!(
            "   Scheduled TASK-{:03} (task_id: {})",
            i,
            task_id.to_string().split('-').next().unwrap_or("")
        );
    }

    println!("\n3. Starting workers...\n");

    // Start worker 1
    let worker1 = FlowWorker::new(storage.clone(), "worker-1")
        .with_poll_interval(Duration::from_millis(50));
    worker1
        .register(|flow: Arc<ComputeTask>| flow.compute())
        .await;
    let handle1 = worker1.start().await;
    println!("   Started worker-1");

    // Start worker 2
    let worker2 = FlowWorker::new(storage.clone(), "worker-2")
        .with_poll_interval(Duration::from_millis(50));
    worker2
        .register(|flow: Arc<ComputeTask>| flow.compute())
        .await;
    let handle2 = worker2.start().await;
    println!("   Started worker-2\n");

    println!("4. Workers processing tasks...\n");

    // Let workers process
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("\n5. Shutting down workers...\n");

    handle1.shutdown().await;
    handle2.shutdown().await;

    println!("   All workers stopped");

    println!("\n6. Verifying results...\n");

    // Check incomplete flows
    let incomplete = storage.get_incomplete_flows().await?;
    if incomplete.is_empty() {
        println!("   ✓ All tasks completed successfully!");
    } else {
        println!("   ⚠ {} tasks incomplete", incomplete.len());
    }

    println!("\n=== Example Complete ===");
    println!(
        "\nNote: All data was stored in-memory. No files were created!"
    );

    Ok(())
}
