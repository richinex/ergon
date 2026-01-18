use ergon::executor::{schedule_timer_named, ExecutionError, Worker};
use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

static LOGIC_COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct DirectTimerTest {
    id: String,
}

impl DirectTimerTest {
    // Logic step - runs once, result is cached
    #[step]
    async fn compute(self: Arc<Self>) -> Result<String, ExecutionError> {
        let count = LOGIC_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
        println!("{} Computing... (execution #{})", self.id, count);
        let result = format!("Computed data for {} (execution #{})", self.id, count);
        Ok(result)
    }

    // Flow with timer directly - no separate wait step
    #[flow]
    async fn run(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("{} Flow starting", self.id);

        // Call logic step first - result gets cached
        let data = self.clone().compute().await?;
        println!("{} Got result: {}", self.id, data);

        // Timer directly in flow
        schedule_timer_named(Duration::from_secs(1), "direct-timer").await?;

        println!("{} After timer, using cached data: {}", self.id, data);
        Ok(format!("Done: {}", data))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::SqliteExecutionLog::new("timer_in_flow.db").await?);
    storage.reset().await?;

    let worker = Worker::new(storage.clone(), "worker").with_timers();
    worker.register(|f: Arc<DirectTimerTest>| f.run()).await;
    let worker_handle = worker.start().await;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");
    let task_id = scheduler
        .schedule(DirectTimerTest { id: "TEST".into() })
        .await?;

    // Race-condition-free completion waiting
    let final_status = storage.wait_for_completion(task_id).await?;
    println!("Task status: {:?}", final_status);

    println!(
        "\nLogic step executed {} times (expected: 1)",
        LOGIC_COUNTER.load(Ordering::SeqCst)
    );

    worker_handle.shutdown().await;
    Ok(())
}
