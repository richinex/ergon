use ergon::executor::{schedule_timer_named, ExecutionError, Worker};
use ergon::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

static LOGIC_COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct FlowLogicExample { id: String }

impl FlowLogicExample {
    // ✅ Step only handles suspension - no business logic!
    #[step]
    async fn wait(self: Arc<Self>) -> Result<(), ExecutionError> {
        schedule_timer_named(Duration::from_secs(1), "delay").await?;
        Ok(())
    }

    // ✅ Business logic stays in the flow - runs exactly once!
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("Starting flow for {}", self.id);

        // Wait for timer
        self.clone().wait().await?;

        // Business logic runs AFTER timer, in flow (not step)
        let count = LOGIC_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
        let result = format!("Processed {} - execution #{}", self.id, count);
        println!("Flow logic: {}", result);

        Ok(result)
    }
}

// Compare with step-based logic
#[derive(Clone, Serialize, Deserialize, FlowType)]
struct StepLogicExample { id: String }

static STEP_LOGIC_COUNTER: AtomicU32 = AtomicU32::new(0);

impl StepLogicExample {
    #[step]
    async fn wait_and_process(self: Arc<Self>) -> Result<String, ExecutionError> {
        schedule_timer_named(Duration::from_secs(1), "delay").await?;

        // Logic after suspension - runs on replay
        let count = STEP_LOGIC_COUNTER.fetch_add(1, Ordering::SeqCst) + 1;
        let result = format!("Processed {} - execution #{}", self.id, count);
        println!("Step logic: {}", result);

        Ok(result)
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        self.wait_and_process().await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::SqliteExecutionLog::new("flow_logic.db").await?);
    storage.reset().await?;

    let worker = Worker::new(storage.clone(), "worker").with_timers();
    worker.register(|f: Arc<FlowLogicExample>| f.process()).await;
    worker.register(|f: Arc<StepLogicExample>| f.process()).await;
    let worker_handle = worker.start().await;

    let scheduler = Scheduler::new(storage.clone());
    let notify = storage.status_notify().clone();

    println!("=== Flow-based logic (✅ GOOD) ===");
    let task1 = scheduler.schedule(FlowLogicExample { id: "FLOW".into() }, Uuid::new_v4()).await?;
    loop {
        if let Some(task) = storage.get_scheduled_flow(task1).await? {
            if matches!(task.status, ergon::storage::TaskStatus::Complete | ergon::storage::TaskStatus::Failed) {
                break;
            }
        }
        notify.notified().await;
    }

    println!("\n=== Step-based logic (⚠️ CONFUSING) ===");
    let task2 = scheduler.schedule(StepLogicExample { id: "STEP".into() }, Uuid::new_v4()).await?;
    loop {
        if let Some(task) = storage.get_scheduled_flow(task2).await? {
            if matches!(task.status, ergon::storage::TaskStatus::Complete | ergon::storage::TaskStatus::Failed) {
                break;
            }
        }
        notify.notified().await;
    }

    println!("\n=== Results ===");
    println!("Flow logic executed {} times (expected: 1)", LOGIC_COUNTER.load(Ordering::SeqCst));
    println!("Step logic executed {} times (expected: 1, actual: may be > 1 due to replay)", STEP_LOGIC_COUNTER.load(Ordering::SeqCst));

    worker_handle.shutdown().await;
    Ok(())
}
