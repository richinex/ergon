use ergon::executor::{schedule_timer_named, ExecutionError, Worker};
use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

static COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct Test {
    id: String,
}

impl Test {
    #[step]
    async fn test_step(self: Arc<Self>) -> Result<String, ExecutionError> {
        let count = COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
        println!("[Execution #{}] Before timer", count);

        schedule_timer_named(Duration::from_secs(1), "wait").await?;

        println!("[Execution #{}] After timer", count);
        Ok(format!("Result from execution #{}", count))
    }

    #[flow]
    async fn run(self: Arc<Self>) -> Result<String, ExecutionError> {
        self.test_step().await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::SqliteExecutionLog::new("timer_behavior.db").await?);
    storage.reset().await?;

    let worker = Worker::new(storage.clone(), "worker").with_timers();
    worker.register(|f: Arc<Test>| f.run()).await;
    let worker_handle = worker.start().await;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");
    let task_id = scheduler.schedule(Test { id: "TEST".into() }).await?;

    let notify = storage.status_notify().clone();
    loop {
        if let Some(task) = storage.get_scheduled_flow(task_id).await? {
            if matches!(
                task.status,
                ergon::storage::TaskStatus::Complete | ergon::storage::TaskStatus::Failed
            ) {
                println!("\nFinal result status: {:?}", task.status);
                break;
            }
        }
        notify.notified().await;
    }

    println!("Total executions: {}", COUNTER.load(Ordering::SeqCst));
    worker_handle.shutdown().await;
    Ok(())
}
