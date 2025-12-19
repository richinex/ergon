use ergon::executor::{schedule_timer_named, ExecutionError, Worker};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct BadExample { id: String }

impl BadExample {
    // ❌ ANTI-PATTERN: Logic after suspension
    #[step]
    async fn bad_pattern(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("Before timer");

        schedule_timer_named(Duration::from_secs(1), "wait").await?;

        // ⚠️ THIS CODE NEVER EXECUTES!
        println!("After timer - YOU WILL NEVER SEE THIS!");
        let result = format!("Processed {}", self.id);

        Ok(result)  // This return value is ignored!
    }

    #[flow]
    async fn run_bad(self: Arc<Self>) -> Result<String, ExecutionError> {
        self.bad_pattern().await
    }
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct GoodExample { id: String }

impl GoodExample {
    // ✅ GOOD PATTERN: Suspension at end of step
    #[step]
    async fn wait_step(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("Before timer");
        schedule_timer_named(Duration::from_secs(1), "wait").await?;
        Ok(())  // Suspension is the LAST thing
    }

    #[step]
    async fn process_step(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("After timer - processing!");
        Ok(format!("Processed {}", self.id))
    }

    #[flow]
    async fn run_good(self: Arc<Self>) -> Result<String, ExecutionError> {
        self.clone().wait_step().await?;
        self.process_step().await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::SqliteExecutionLog::new("timer_antipattern.db").await?);
    storage.reset().await?;

    let worker = Worker::new(storage.clone(), "worker").with_timers();
    worker.register(|f: Arc<BadExample>| f.run_bad()).await;
    worker.register(|f: Arc<GoodExample>| f.run_good()).await;
    let worker_handle = worker.start().await;

    let scheduler = Scheduler::new(storage.clone());

    println!("\n=== BAD PATTERN (logic after suspension) ===");
    let bad_id = scheduler.schedule(BadExample { id: "BAD".into() }, Uuid::new_v4()).await?;

    let notify = storage.status_notify().clone();
    loop {
        if let Some(task) = storage.get_scheduled_flow(bad_id).await? {
            if matches!(task.status, ergon::storage::TaskStatus::Complete | ergon::storage::TaskStatus::Failed) {
                break;
            }
        }
        notify.notified().await;
    }

    println!("\n=== GOOD PATTERN (separate steps) ===");
    let good_id = scheduler.schedule(GoodExample { id: "GOOD".into() }, Uuid::new_v4()).await?;

    loop {
        if let Some(task) = storage.get_scheduled_flow(good_id).await? {
            if matches!(task.status, ergon::storage::TaskStatus::Complete | ergon::storage::TaskStatus::Failed) {
                break;
            }
        }
        notify.notified().await;
    }

    println!("\n✅ Done!");
    worker_handle.shutdown().await;
    Ok(())
}
