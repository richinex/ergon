//! Test: Can DAG execution handle multiple steps with .invoke()?
//!
//! Uses dag! macro which has stable hash-based step IDs

use chrono::Utc;
use ergon::executor::{InvokeChild, Worker};
use ergon::prelude::*;
use ergon::storage::SqliteExecutionLog;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Parent Flow - Uses DAG execution
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct Order {
    id: String,
}

impl Order {
    #[step]
    async fn validate(self: Arc<Self>) -> Result<(), String> {
        println!("[{}] Step: validate {}", ts(), self.id);
        Ok(())
    }

    #[step]
    async fn process_payment(self: Arc<Self>) -> Result<PaymentResult, String> {
        println!("[{}] Step: invoking Payment child", ts());

        let result = self
            .invoke(PaymentFlow {
                order_id: self.id.clone(),
                amount: 99.99,
            })
            .result()
            .await
            .map_err(|e| e.to_string())?;

        println!("[{}] Step: got payment result: {:?}", ts(), result);
        Ok(result)
    }

    #[step]
    async fn ship(self: Arc<Self>) -> Result<ShipmentResult, String> {
        println!("[{}] Step: invoking Shipment child", ts());

        let result = self
            .invoke(ShipmentFlow {
                order_id: self.id.clone(),
            })
            .result()
            .await
            .map_err(|e| e.to_string())?;

        println!("[{}] Step: got shipment result: {:?}", ts(), result);
        Ok(result)
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<ShipmentResult, String> {
        // Use dag! macro for stable hash-based step IDs
        dag! {
            self.register_validate();
            self.register_process_payment();
            self.register_ship()
        }
    }
}

// =============================================================================
// Child Flow 1 - Payment
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentResult {
    transaction_id: String,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = PaymentResult)]
struct PaymentFlow {
    order_id: String,
    amount: f64,
}

impl PaymentFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<PaymentResult, String> {
        println!("[{}]   CHILD: Processing payment ${:.2}", ts(), self.amount);
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(PaymentResult {
            transaction_id: format!("TXN-{}", &Uuid::new_v4().to_string()[..8]),
        })
    }
}

// =============================================================================
// Child Flow 2 - Shipment
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShipmentResult {
    tracking: String,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = ShipmentResult)]
struct ShipmentFlow {
    order_id: String,
}

impl ShipmentFlow {
    #[flow]
    async fn create(self: Arc<Self>) -> Result<ShipmentResult, String> {
        println!("[{}]   CHILD: Creating shipment", ts());
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(ShipmentResult {
            tracking: format!("TRK-{}", &Uuid::new_v4().to_string()[..8]),
        })
    }
}

// =============================================================================
// Utilities
// =============================================================================

fn ts() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = "/tmp/ergon_test_dag_multiple_invoke.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);
    let scheduler = Scheduler::new(storage.clone());

    println!("\n=== Test: DAG with Multiple .invoke() Calls ===\n");

    let order = Order {
        id: "ORD-001".into(),
    };
    let task_id = scheduler.schedule(order, Uuid::new_v4()).await?;
    println!("[{}] Scheduled: {}\n", ts(), &task_id.to_string()[..8]);

    let worker = Worker::new(storage.clone(), "worker")
        .with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<Order>| f.process()).await;
    worker.register(|f: Arc<PaymentFlow>| f.process()).await;
    worker.register(|f: Arc<ShipmentFlow>| f.create()).await;

    let handle = worker.start().await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    handle.shutdown().await;

    storage.close().await?;

    println!("\n=== Test Complete ===");
    Ok(())
}
