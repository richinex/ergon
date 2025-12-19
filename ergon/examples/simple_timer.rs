use ergon::executor::{schedule_timer_named, ExecutionError, Worker};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct TimedTask {
    id: String,
}

impl TimedTask {
    #[step]
    async fn wait_delay(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("{} Waiting 2 seconds...", self.id);
        schedule_timer_named(Duration::from_secs(2), "delay").await?;
        Ok(())
    }

    #[step]
    async fn complete_task(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("{} Complete!", self.id);
        Ok(format!("Task {} done", self.id))
    }

    #[flow]
    async fn execute(self: Arc<Self>) -> Result<String, ExecutionError> {
        self.clone().wait_delay().await?;
        self.complete_task().await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::InMemoryExecutionLog::new());
    storage.reset().await?;
    let worker = Worker::new(storage.clone(), "worker").with_timers();
    worker.register(|f: Arc<TimedTask>| f.execute()).await;
    let worker_handle = worker.start().await;

    let scheduler = Scheduler::new(storage.clone());
    let task_id = scheduler
        .schedule(
            TimedTask {
                id: "Task_001".into(),
            },
            Uuid::new_v4(),
        )
        .await?;

    // Notify on completion
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
    println!("Timer Done!");
    worker_handle.shutdown().await;
    Ok(())
}
