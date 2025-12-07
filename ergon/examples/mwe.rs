//! Ergon Minimal Example - Durable Workflow with Child Flow
//!
//! Demonstrates step dependencies, child flow invocation, and
//! automatic replay on recovery. Each step executes exactly once.

use chrono::Utc;
use ergon::executor::{InvokeChild, Worker};
use ergon::prelude::*;
use ergon::storage::SqliteExecutionLog;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Label {
    tracking: String,
}

/// Order (becomes a parent flow to Shipment)
#[derive(Clone, Serialize, Deserialize, FlowType)]
struct Order {
    id: String,
    amount: f64,
}

impl Order {
    #[step]
    async fn validate(self: Arc<Self>) -> Result<(), String> {
        println!("[{}] validating {}", ts(), self.id);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    #[step(depends_on = "validate")]
    async fn charge(self: Arc<Self>) -> Result<(), String> {
        println!("[{}] charging ${:.2}", ts(), self.amount);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    #[step(depends_on = "charge")]
    async fn ship(self: Arc<Self>) -> Result<Label, String> {
        let label = self
            .invoke(Shipment {
                order_id: self.id.clone(),
            })
            .result()
            .await
            .map_err(|e| e.to_string())?;

        println!("[{}] shipped: {}", ts(), label.tracking);
        Ok(label)
    }

    #[flow]
    async fn fulfill(self: Arc<Self>) -> Result<Label, String> {
        self.clone().validate().await?;
        self.clone().charge().await?;
        self.clone().ship().await
    }
}

// Shipment Flow (child)
#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = Label)]
struct Shipment {
    order_id: String,
}

impl Shipment {
    #[flow]
    async fn create(self: Arc<Self>) -> Result<Label, String> {
        println!("[{}] creating label for {}", ts(), self.order_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(Label {
            tracking: format!("TRK-{}", &Uuid::new_v4().to_string()[..8]),
        })
    }
}

fn ts() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = "/tmp/ergon_mwe.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);
    let scheduler = Scheduler::new(storage.clone());

    let order = Order {
        id: "ORD-001".into(),
        amount: 99.99,
    };
    let task_id = scheduler.schedule(order, Uuid::new_v4()).await?;
    println!("[{}] scheduled: {}\n", ts(), &task_id.to_string()[..8]);

    let worker =
        Worker::new(storage.clone(), "worker").with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<Order>| f.fulfill()).await;
    worker.register(|f: Arc<Shipment>| f.create()).await;

    let handle = worker.start().await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    handle.shutdown().await;

    storage.close().await?;
    Ok(())
}
