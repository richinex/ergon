//! Sequential execution with multiple .invoke() calls.
//!
//! Run with
//!
//! ```not_rust
//! cargo run --example test_sequential_multiple_invoke --features=sqlite
//! ```

use chrono::Utc;
use ergon::executor::{ExecutionError, InvokeChild};
use ergon::prelude::*;
use std::time::Duration;

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
    async fn finalize_payment(
        self: Arc<Self>,
        payment: PaymentResult,
    ) -> Result<PaymentResult, String> {
        println!("[{}] Step: finalized payment: {:?}", ts(), payment);
        Ok(payment)
    }

    #[step]
    async fn finalize_shipment(
        self: Arc<Self>,
        shipment: ShipmentResult,
    ) -> Result<ShipmentResult, String> {
        println!("[{}] Step: finalized shipment: {:?}", ts(), shipment);
        Ok(shipment)
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<ShipmentResult, ExecutionError> {
        println!("[{}] FLOW: Starting sequential execution", ts());

        self.clone()
            .validate()
            .await
            .map_err(ExecutionError::Failed)?;
        println!("[{}] FLOW: Validate complete", ts());

        let payment = self
            .invoke(PaymentFlow {
                order_id: self.id.clone(),
                amount: 99.99,
            })
            .result()
            .await
            .map_err(|e| ExecutionError::Failed(e.to_string()))?;

        self.clone()
            .finalize_payment(payment)
            .await
            .map_err(ExecutionError::Failed)?;
        println!("[{}] FLOW: Payment complete", ts());

        let shipment = self
            .invoke(ShipmentFlow {
                order_id: self.id.clone(),
            })
            .result()
            .await
            .map_err(|e| ExecutionError::Failed(e.to_string()))?;

        let result = self
            .clone()
            .finalize_shipment(shipment)
            .await
            .map_err(ExecutionError::Failed)?;
        println!("[{}] FLOW: Ship complete", ts());

        Ok(result)
    }
}

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

fn ts() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = "/tmp/ergon_test_sequential_multiple_invoke.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);
    let scheduler = Scheduler::new(storage.clone()).unversioned();

    let order = Order {
        id: "ORD-001".into(),
    };
    scheduler.schedule(order).await?;

    let worker =
        Worker::new(storage.clone(), "worker").with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<Order>| f.process()).await;
    worker.register(|f: Arc<PaymentFlow>| f.process()).await;
    worker.register(|f: Arc<ShipmentFlow>| f.create()).await;

    let handle = worker.start().await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    handle.shutdown().await;

    storage.close().await?;

    Ok(())
}
