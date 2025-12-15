//! Backpressure control example using Semaphore
//!
//! This example demonstrates:
//! - Limiting concurrent flow execution with semaphores
//! - Preventing resource exhaustion under high load
//! - Automatic flow control and backpressure
//! - Comparing behavior with and without limits
//!
//! ## Scenario
//! We schedule 100 flows and run two workers:
//! - Worker 1: No limit (processes as fast as possible)
//! - Worker 2: Limited to 5 concurrent flows (controlled backpressure)
//!
//! This shows how backpressure prevents unbounded resource usage while
//! maintaining throughput.
//!
//! ## Key Takeaways
//! - Semaphore provides explicit flow control
//! - Prevents OOM and resource exhaustion
//! - RAII semantics ensure permits are released
//! - Recommended for production: 50-500 depending on flow complexity
//! - Natural rate limiting via poll interval vs explicit limits
//!
//! ## Run with
//! ```bash
//! cargo run --example backpressure_demo
//! ```

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct HeavyComputationFlow {
    job_id: u32,
    computation_ms: u64,
}

impl HeavyComputationFlow {
    #[flow]
    async fn execute(self: Arc<Self>) -> Result<String, String> {
        println!("[Flow {}] Starting heavy computation", self.job_id);
        self.compute().await
    }

    #[step]
    async fn compute(self: Arc<Self>) -> Result<String, String> {
        // Simulate heavy computation
        tokio::time::sleep(Duration::from_millis(self.computation_ms)).await;
        Ok(format!("Job {} computed", self.job_id))
    }
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct LightweightFlow {
    task_id: u32,
}

impl LightweightFlow {
    #[flow]
    async fn execute(self: Arc<Self>) -> Result<String, String> {
        println!("[Flow {}] Quick task", self.task_id);
        self.process().await
    }

    #[step]
    async fn process(self: Arc<Self>) -> Result<String, String> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(format!("Task {} done", self.task_id))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(SqliteExecutionLog::new("backpressure_demo.db").await?);
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone());

    // Schedule many heavy computation flows
    for i in 1..=30 {
        let flow = HeavyComputationFlow {
            job_id: i,
            computation_ms: 300,
        };
        scheduler.schedule(flow, Uuid::new_v4()).await?;
    }

    // Schedule many lightweight flows
    for i in 1..=20 {
        let flow = LightweightFlow { task_id: i };
        scheduler.schedule(flow, Uuid::new_v4()).await?;
    }

    // Worker 1: No backpressure limit (default behavior)
    let worker1 = Worker::new(storage.clone(), "unlimited-worker")
        .with_poll_interval(Duration::from_millis(50));
    worker1
        .register(|flow: Arc<HeavyComputationFlow>| flow.execute())
        .await;
    worker1
        .register(|flow: Arc<LightweightFlow>| flow.execute())
        .await;
    let handle1 = worker1.start().await;

    // Worker 2: With backpressure limit
    let worker2 = Worker::new(storage.clone(), "limited-worker")
        .with_poll_interval(Duration::from_millis(50))
        .with_max_concurrent_flows(10); // Limit to 10 concurrent
    worker2
        .register(|flow: Arc<HeavyComputationFlow>| flow.execute())
        .await;
    worker2
        .register(|flow: Arc<LightweightFlow>| flow.execute())
        .await;
    let handle2 = worker2.start().await;

    // Let workers process
    tokio::time::sleep(Duration::from_secs(5)).await;

    handle1.shutdown().await;
    handle2.shutdown().await;

    let incomplete = storage.get_incomplete_flows().await?;
    let completed_count = 50 - incomplete.len();

    println!("Completed: {}/50 flows", completed_count);
    if !incomplete.is_empty() {
        println!("Incomplete: {} flows", incomplete.len());
    }

    storage.close().await?;
    Ok(())
}
