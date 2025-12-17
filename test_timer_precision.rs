// Quick test to measure timer firing precision
use ergon::executor::{schedule_timer, ExecutionError, Scheduler, Worker};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct TimerTest {
    id: u32,
}

impl TimerTest {
    #[flow]
    async fn test_timer(self: Arc<Self>) -> Result<Duration, ExecutionError> {
        let start = Instant::now();
        self.clone().wait_step().await?;
        Ok(start.elapsed())
    }

    #[step]
    async fn wait_step(self: Arc<Self>) -> Result<(), ExecutionError> {
        let start = Instant::now();
        schedule_timer(Duration::from_secs(1)).await?;
        println!("Timer fired after {:?} (expected 1s)", start.elapsed());
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    let storage = Arc::new(SqliteExecutionLog::new(":memory:").await?);
    let scheduler = Scheduler::new(storage.clone());

    let worker = Worker::new(storage.clone(), "test-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(100))  // Check every 100ms
        .with_poll_interval(Duration::from_millis(100));

    worker
        .register(|flow: Arc<TimerTest>| flow.test_timer())
        .await;

    let worker_handle = worker.start().await;

    // Run 5 timer tests
    for i in 0..5 {
        let flow = TimerTest { id: i };
        let flow_id = uuid::Uuid::new_v4();
        let task_id = scheduler.schedule(flow, flow_id).await?;

        let status_notify = storage.status_notify().clone();
        loop {
            if let Some(task) = storage.get_scheduled_flow(task_id).await? {
                match task.status {
                    ergon::storage::TaskStatus::Complete => break,
                    _ => status_notify.notified().await,
                }
            } else {
                break;
            }
        }
    }

    worker_handle.shutdown().await;
    Ok(())
}
