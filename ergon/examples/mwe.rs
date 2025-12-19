//! Ergon minimal example: durable workflow with child flow invocation.
//!
//! Demonstrates step dependencies, child flow invocation, and automatic replay
//! on recovery. Each step executes exactly once.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use ergon::executor::{InvokeChild, Worker};
use ergon::prelude::*;
use ergon::storage::SqliteExecutionLog;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A shipping label containing tracking information.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Label {
    tracking: String,
}

/// An order flow that becomes a parent to shipment flows.
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

    #[step]
    async fn finalize_shipment(self: Arc<Self>, label: Label) -> Result<Label, String> {
        println!("[{}] shipped: {}", ts(), label.tracking);
        Ok(label)
    }

    #[flow]
    async fn fulfill(self: Arc<Self>) -> Result<Label, String> {
        self.clone().validate().await?;
        self.clone().charge().await?;

        // Invoke child at flow level
        let label = self
            .invoke(Shipment {
                order_id: self.id.clone(),
            })
            .result()
            .await
            .map_err(|e| e.to_string())?;

        // Pass result to atomic step
        self.clone().finalize_shipment(label).await
    }
}

/// A shipment flow (child of order flow) that creates shipping labels.
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

        let tracking = format!("TRK-{}", &Uuid::new_v4().to_string()[..8]);
        Ok(Label { tracking })
    }
}

/// Returns the current timestamp formatted for logging.
fn ts() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Clean up any existing database file
    let db_path = "/tmp/ergon_mwe.db";
    let _ = std::fs::remove_file(db_path);

    // Initialize storage and scheduler
    let storage = Arc::new(SqliteExecutionLog::new(db_path).await?);
    let scheduler = Scheduler::new(storage.clone()).unversioned();

    // Schedule an order flow
    let order = Order {
        id: "ORD-001".into(),
        amount: 99.99,
    };
    let task_id = scheduler.schedule(order).await?;
    println!("[{}] scheduled: {}", ts(), &task_id.to_string()[..8]);

    // Set up worker with fast poll interval for demo purposes
    let worker =
        Worker::new(storage.clone(), "worker").with_poll_interval(Duration::from_millis(50));

    // Register flow handlers
    worker.register(|f: Arc<Order>| f.fulfill()).await;
    worker.register(|f: Arc<Shipment>| f.create()).await;

    // Start the worker and let it run
    let handle = worker.start().await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    handle.shutdown().await;

    // Clean shutdown
    storage.close().await?;
    Ok(())
}
