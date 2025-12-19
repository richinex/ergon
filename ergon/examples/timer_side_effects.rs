use ergon::executor::{schedule_timer_named, ExecutionError, Worker};
use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

static BEFORE_COUNTER: AtomicU32 = AtomicU32::new(0);
static AFTER_COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct SideEffectTest {
    id: String,
}

impl SideEffectTest {
    #[step]
    async fn step_with_side_effects(self: Arc<Self>) -> Result<String, ExecutionError> {
        // Side effect BEFORE suspension
        let before_count = BEFORE_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
        println!("BEFORE timer: execution #{}", before_count);

        // schedule_timer_named(Duration::from_secs(1), "wait").await?;

        // Side effect AFTER suspension
        let after_count = AFTER_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
        println!("AFTER timer: execution #{}", after_count);

        // The actual return value computation
        let result = format!(
            "Processed {} (before={}, after={})",
            self.id, before_count, after_count
        );
        println!("Computed result: {}", result);

        schedule_timer_named(Duration::from_secs(1), "wait").await?;

        Ok(result)
    }

    #[flow]
    async fn run(self: Arc<Self>) -> Result<String, ExecutionError> {
        let result = self.step_with_side_effects().await?;
        println!("Flow got result: {}", result);
        Ok(result)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::SqliteExecutionLog::new("side_effects.db").await?);
    storage.reset().await?;

    let worker = Worker::new(storage.clone(), "worker").with_timers();
    worker.register(|f: Arc<SideEffectTest>| f.run()).await;
    let worker_handle = worker.start().await;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");
    let task_id = scheduler
        .schedule(SideEffectTest { id: "TEST".into() })
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

    println!("\n=== FINAL COUNTS ===");
    println!(
        "Code BEFORE timer ran {} times",
        BEFORE_COUNTER.load(Ordering::SeqCst)
    );
    println!(
        "Code AFTER timer ran {} times",
        AFTER_COUNTER.load(Ordering::SeqCst)
    );

    worker_handle.shutdown().await;
    Ok(())
}
