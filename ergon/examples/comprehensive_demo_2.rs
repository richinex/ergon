//! Complete order processing system demonstrating all Ergon features.
//!
//! Run with
//!
//! ```not_rust
//! cargo run --example comprehensive_demo_2 --features=sqlite
//! ```

use ergon::core::RetryPolicy;
use ergon::executor::{schedule_timer_named, ExecutionError, InvokeChild};
use ergon::prelude::*;
use ergon::storage::TaskStatus;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = String)]
struct PaymentFlow {
    order_id: String,
    amount: f64,
}

impl PaymentFlow {
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        let mut hasher = DefaultHasher::new();
        self.order_id.hash(&mut hasher);
        let should_fail = (hasher.finish() % 10) < 3;

        self.clone().charge_card(should_fail).await?;
        Ok(format!("PAYMENT-{}", Uuid::new_v4().simple()))
    }

    #[step]
    async fn charge_card(self: Arc<Self>, should_fail: bool) -> Result<(), String> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if should_fail {
            return Err("NetworkTimeout".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = String)]
struct ShippingFlow {
    order_id: String,
    address: String,
}

impl ShippingFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        self.clone().prepare_package().await?;
        Ok(format!("TRACK-{}", Uuid::new_v4().simple()))
    }

    #[step]
    async fn prepare_package(self: Arc<Self>) -> Result<(), ExecutionError> {
        schedule_timer_named(Duration::from_secs(2), "warehouse-prep").await?;
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderFlow {
    order_id: String,
    customer_id: String,
    amount: f64,
    address: String,
}

impl OrderFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<OrderReceipt, ExecutionError> {
        let _validation = self.clone().run_validations().await?;

        let payment_id = self
            .invoke(PaymentFlow {
                order_id: self.order_id.clone(),
                amount: self.amount,
            })
            .result()
            .await?;

        let tracking_id = self
            .invoke(ShippingFlow {
                order_id: self.order_id.clone(),
                address: self.address.clone(),
            })
            .result()
            .await?;

        Ok(OrderReceipt {
            order_id: self.order_id.clone(),
            payment_id,
            tracking_id,
            status: "CONFIRMED".to_string(),
        })
    }

    async fn run_validations(self: Arc<Self>) -> Result<ValidationResult, ExecutionError> {
        dag! {
            self.register_validate_customer();
            self.register_check_inventory();
            self.register_check_fraud();
            self.register_aggregate_validations()
        }
    }

    #[step]
    async fn validate_customer(self: Arc<Self>) -> Result<bool, String> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if self.customer_id.starts_with("BLOCKED_") {
            return Err("CustomerBlocked".to_string());
        }
        Ok(true)
    }

    #[step]
    async fn check_inventory(self: Arc<Self>) -> Result<bool, String> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(true)
    }

    #[step]
    async fn check_fraud(self: Arc<Self>) -> Result<bool, String> {
        schedule_timer_named(Duration::from_secs(1), "fraud-check")
            .await
            .map_err(|e| e.to_string())?;
        Ok(true)
    }

    #[step(
        depends_on = ["validate_customer", "check_inventory", "check_fraud"],
        inputs(customer = "validate_customer", inventory = "check_inventory", fraud = "check_fraud")
    )]
    async fn aggregate_validations(
        self: Arc<Self>,
        customer: bool,
        inventory: bool,
        fraud: bool,
    ) -> Result<ValidationResult, String> {
        if !customer || !inventory || !fraud {
            return Err("ValidationFailed".to_string());
        }

        Ok(ValidationResult {
            customer_valid: customer,
            inventory_available: inventory,
            fraud_passed: fraud,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidationResult {
    customer_valid: bool,
    inventory_available: bool,
    fraud_passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderReceipt {
    order_id: String,
    payment_id: String,
    tracking_id: String,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(SqliteExecutionLog::new("data/comprehensive_demo_2.db").await?);
    storage.reset().await?;
    let scheduler = Scheduler::new(storage.clone()).unversioned();

    let mut worker_handles = Vec::new();
    for i in 1..=3 {
        let worker = Worker::new(storage.clone(), format!("worker-{}", i))
            .with_poll_interval(Duration::from_millis(100))
            .with_timers()
            .with_timer_interval(Duration::from_millis(50));

        worker.register(|f: Arc<OrderFlow>| f.process()).await;
        worker.register(|f: Arc<PaymentFlow>| f.process()).await;
        worker.register(|f: Arc<ShippingFlow>| f.process()).await;

        worker_handles.push(worker.start().await);
    }

    let orders = vec![
        OrderFlow {
            order_id: "ORDER-001".to_string(),
            customer_id: "CUST-123".to_string(),
            amount: 99.99,
            address: "123 Main St".to_string(),
        },
        OrderFlow {
            order_id: "ORDER-002".to_string(),
            customer_id: "CUST-456".to_string(),
            amount: 149.50,
            address: "456 Oak Ave".to_string(),
        },
    ];

    let mut task_ids = Vec::new();
    for order in orders {
        task_ids.push(scheduler.schedule(order).await?);
    }

    let status_notify = storage.status_notify().clone();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut all_complete = true;
            for &task_id in &task_ids {
                if let Some(task) = storage.get_scheduled_flow(task_id).await? {
                    if !matches!(task.status, TaskStatus::Complete | TaskStatus::Failed) {
                        all_complete = false;
                        break;
                    }
                }
            }
            if all_complete {
                break;
            }
            status_notify.notified().await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await??;

    for task_id in &task_ids {
        if let Some(task) = storage.get_scheduled_flow(*task_id).await? {
            println!(
                "{}: {:?} (retries: {})",
                task_id, task.status, task.retry_count
            );

            if let Some(inv) = storage.get_invocation(task.flow_id, 0).await? {
                if let Some(result_bytes) = inv.return_value() {
                    if let Ok(result) =
                        serde_json::from_slice::<Result<OrderReceipt, ExecutionError>>(result_bytes)
                    {
                        match result {
                            Ok(receipt) => println!("  {:?}", receipt),
                            Err(e) => println!("  error: {:?}", e),
                        }
                    }
                }
            }
        }
    }

    for handle in worker_handles {
        handle.shutdown().await;
    }
    storage.close().await?;

    Ok(())
}
