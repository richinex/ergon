//! Durable function trees with parent-child flows.
//!
//! Demonstrates how parent flows can invoke child flows. Each child is a separate
//! durable function with its own retry boundaries. If a child fails, only the child
//! retries—the parent remains suspended without wasting resources.
//!
//! Run with
//!
//! ```not_rust
//! cargo run --example mini_child
//! ```

use std::sync::Arc;
use std::time::Duration;

use ergon::executor::{InvokeChild, Worker};
use ergon::prelude::*;
use ergon::storage::SqliteExecutionLog;
use ergon::TaskStatus;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "/tmp/ergon_mini_child.db";
    let _ = std::fs::remove_file(db_path);

    let storage = Arc::new(SqliteExecutionLog::new(db_path).await?);
    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    let task_id = scheduler
        .schedule_with(
            Order {
                id: "ORD-001".into(),
            },
            Uuid::new_v4(),
        )
        .await?;

    let worker = Worker::new(storage.clone(), "worker");
    worker.register(|f: Arc<Order>| f.fulfill()).await;
    worker.register(|f: Arc<Shipment>| f.create()).await;
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
    storage.close().await?;
    Ok(())
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct Order {
    id: String,
}

impl Order {
    #[step]
    async fn finalize_shipment(self: Arc<Self>, tracking: String) -> Result<String, String> {
        println!("Order shipped: {}", tracking);
        Ok(tracking)
    }

    #[flow]
    async fn fulfill(self: Arc<Self>) -> Result<String, String> {
        // Invoke child at flow level (not in step)
        let tracking = self
            .invoke(Shipment {
                order_id: self.id.clone(),
            })
            .result()
            .await
            .map_err(|e| e.to_string())?;

        // Pass result to atomic step
        self.finalize_shipment(tracking).await
    }
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = String)]
struct Shipment {
    order_id: String,
}

impl Shipment {
    #[flow]
    async fn create(self: Arc<Self>) -> Result<String, String> {
        let tracking = format!("TRK-{}", &self.order_id);
        println!("Created label: {}", tracking);
        Ok(tracking)
    }
}
