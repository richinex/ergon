use ergon::core::RetryPolicy;
use ergon::executor::{ExecutionError, Worker};
use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

const FAST_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_delay: Duration::from_millis(100),
    max_delay: Duration::from_secs(1),
    backoff_multiplier: 1.5,
};

static ATTEMPTS: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct RetryTask {
    id: String,
}

impl RetryTask {
    #[flow(retry = FAST_RETRY)]
    async fn execute(self: Arc<Self>) -> Result<String, ExecutionError> {
        let attempt = ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        println!("{} Attempt #{}", self.id, attempt);
        if attempt < 4 {
            return Err("Simulated failure".to_string().into());
        }
        println!("{} Success on attempt {}", self.id, attempt);
        Ok(format!("Done after {} attempts", attempt))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::SqliteExecutionLog::new("simple_retry.db").await?);
    storage.reset().await?;
    let worker =
        Worker::new(storage.clone(), "worker").with_poll_interval(Duration::from_millis(50));
    worker.register(|f: Arc<RetryTask>| f.execute()).await;
    let worker_handle = worker.start().await;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");
    let task_id = scheduler
        .schedule_with(
            RetryTask {
                id: "TASK-001".into(),
            },
            Uuid::new_v4(),
        )
        .await?;

    let notify = storage.status_notify().clone();
    loop {
        if let Some(task) = storage.get_scheduled_flow(task_id).await? {
            if matches!(
                task.status,
                ergon::storage::TaskStatus::Complete | ergon::storage::TaskStatus::Failed
            ) {
                break;
            }
        }
        notify.notified().await;
    }
    println!(
        "Retry task complete! Total attempts: {}",
        ATTEMPTS.load(Ordering::SeqCst)
    );
    worker_handle.shutdown().await;
    Ok(())
}
