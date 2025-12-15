//! The simplest durable execution workflow.
//!
//! A minimal example showing how steps are cached and replayed automatically.
//! If this program crashes mid-execution, steps that completed won't re-run.
//!
//! Run with
//!
//! ```not_rust
//! cargo run --example mini_basic
//! ```

use std::sync::Arc;
use std::time::Duration;

use ergon::executor::Worker;
use ergon::prelude::*;
use ergon::storage::InMemoryExecutionLog;
use ergon::TaskStatus;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemoryExecutionLog::default());
    let scheduler = Scheduler::new(storage.clone());

    let task_id = scheduler
        .schedule(
            Order {
                id: "ORD-001".into(),
                amount: 99.99,
            },
            Uuid::new_v4(),
        )
        .await?;

    let worker = Worker::new(storage.clone(), "worker");
    worker.register(|f: Arc<Order>| f.process()).await;
    let handle = worker.start().await;

    loop {
        if let Some(scheduled) = storage.get_scheduled_flow(task_id).await? {
            if matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    handle.shutdown().await;
    Ok(())
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct Order {
    id: String,
    amount: f64,
}

impl Order {
    #[step]
    async fn validate(self: Arc<Self>) -> Result<(), String> {
        println!("Validated {}", self.id);
        Ok(())
    }

    #[step(depends_on = "validate")]
    async fn charge(self: Arc<Self>) -> Result<(), String> {
        println!("Charged ${:.2}", self.amount);
        Ok(())
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<(), String> {
        self.clone().validate().await?;
        self.clone().charge().await
    }
}
