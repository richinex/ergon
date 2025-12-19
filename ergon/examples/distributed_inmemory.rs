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

#[derive(Serialize, Deserialize, Clone, FlowType)]
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
    // Create in-memory storage (no files!)
    let storage = Arc::new(InMemoryExecutionLog::new());

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    // Schedule multiple tasks
    for i in 1..=5 {
        let task = ComputeTask {
            task_id: format!("TASK-{:03}", i),
            value: i * 10,
        };

        let flow_id = Uuid::new_v4();
        let _task_id = scheduler.schedule_with(task, flow_id).await?;
    }

    // Start worker 1
    let worker1 =
        Worker::new(storage.clone(), "worker-1").with_poll_interval(Duration::from_millis(50));
    worker1
        .register(|flow: Arc<ComputeTask>| flow.compute())
        .await;
    let handle1 = worker1.start().await;

    // Start worker 2
    let worker2 =
        Worker::new(storage.clone(), "worker-2").with_poll_interval(Duration::from_millis(50));
    worker2
        .register(|flow: Arc<ComputeTask>| flow.compute())
        .await;
    let handle2 = worker2.start().await;

    // Let workers process
    tokio::time::sleep(Duration::from_secs(1)).await;

    handle1.shutdown().await;
    handle2.shutdown().await;

    // Check incomplete flows
    let incomplete = storage.get_incomplete_flows().await?;
    if incomplete.is_empty() {
        println!("All tasks completed successfully");
    } else {
        println!("{} tasks incomplete", incomplete.len());
    }

    Ok(())
}
