//! Comprehensive Ergon Demo: Complete Order Processing System
//!
//! This example demonstrates ALL major Ergon features in a single, realistic workflow:
//!
//! # Features Demonstrated
//!
//! 1. **Normal Flows**: Main order processing flow with completion notification
//! 2. **Child Flows (Signals)**: Payment and shipping as separate invokable flows
//! 3. **Timers**: Fraud check delay, shipping preparation delay
//! 4. **DAG Execution**: Parallel validation steps (inventory, customer, fraud)
//! 5. **Retry Policies**: Automatic retry for transient payment failures
//! 6. **Step Caching**: Successful steps aren't re-executed on retry
//! 7. **Error Handling**: Retryable (network) vs non-retryable (business logic) errors
//! 8. **Event-Driven Notifications**: work_notify, timer_notify, status_notify
//! 9. **Distributed Workers**: Multiple workers processing flows concurrently
//! 10. **Type Safety**: Compile-time guarantees for flow coordination
//!
//! # Architecture
//!
//! ```text
//! OrderFlow (main)
//!   ├─ validate_customer ────┐
//!   ├─ check_inventory   ────┤─── (parallel DAG)
//!   └─ check_fraud (timer) ──┘
//!         │
//!         ├─ PaymentFlow (child, retryable) ── signal parent
//!         │
//!         └─ ShippingFlow (child, timer) ──── signal parent
//! ```
//!
//! Run: cargo run --example comprehensive_demo --features=sqlite

use ergon::core::RetryPolicy;
use ergon::executor::{schedule_timer_named, ExecutionError, InvokeChild};
use ergon::prelude::*;
use ergon::storage::TaskStatus;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// ============================================================================
// CHILD FLOWS: Invokable flows that suspend parent until completion
// ============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = String)]
struct PaymentFlow {
    order_id: String,
    amount: f64,
}

impl PaymentFlow {
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!(
            "  [Payment] Processing ${:.2} for order {}",
            self.amount, self.order_id
        );

        let mut hasher = DefaultHasher::new();
        self.order_id.hash(&mut hasher);
        let hash_value = hasher.finish();
        let should_fail = (hash_value % 10) < 3;

        self.clone().charge_card(should_fail).await?;

        Ok(format!("PAYMENT-{}", Uuid::new_v4().simple()))
    }

    #[step]
    async fn charge_card(self: Arc<Self>, should_fail: bool) -> Result<(), String> {
        tokio::time::sleep(Duration::from_millis(100)).await;

        if should_fail {
            println!("  [Payment] Network error (will retry)");
            return Err("NetworkTimeout".to_string());
        }

        println!("  [Payment] Card charged successfully");
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
        println!(
            "  [Shipping] Preparing shipment to {} for order {}",
            self.address, self.order_id
        );

        // Timer: Wait for warehouse to prepare package
        self.clone().prepare_package().await?;

        Ok(format!("TRACK-{}", Uuid::new_v4().simple()))
    }

    #[step]
    async fn prepare_package(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("  [Shipping] Waiting for warehouse (2s delay)...");
        schedule_timer_named(Duration::from_secs(2), "warehouse-prep").await?;
        println!("  [Shipping] Package prepared and labeled");
        Ok(())
    }
}

// ============================================================================
// MAIN FLOW: DAG, child invocations, event-driven completion
// ============================================================================

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
        println!("\n[Order {}] Starting processing", self.order_id);

        let validation = self.clone().run_validations().await?;
        println!(
            "[Order {}] Validations complete: {:?}",
            self.order_id, validation
        );

        println!("  [Order] Invoking payment child flow...");
        let payment_id = self
            .invoke(PaymentFlow {
                order_id: self.order_id.clone(),
                amount: self.amount,
            })
            .result()
            .await?;
        println!("  [Order] Payment completed: {}", payment_id);

        println!("  [Order] Invoking shipping child flow...");
        let tracking_id = self
            .invoke(ShippingFlow {
                order_id: self.order_id.clone(),
                address: self.address.clone(),
            })
            .result()
            .await?;
        println!("  [Order] Shipping arranged: {}", tracking_id);

        let receipt = OrderReceipt {
            order_id: self.order_id.clone(),
            payment_id,
            tracking_id,
            status: "CONFIRMED".to_string(),
        };

        println!("[Order {}] Complete! Receipt: {:?}", self.order_id, receipt);
        Ok(receipt)
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
        println!("  [Validation] Checking customer {}...", self.customer_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        if self.customer_id.starts_with("BLOCKED_") {
            return Err("CustomerBlocked".to_string());
        }

        println!("  [Validation] Customer valid");
        Ok(true)
    }

    #[step]
    async fn check_inventory(self: Arc<Self>) -> Result<bool, String> {
        println!("  [Validation] Checking inventory...");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("  [Validation] Inventory available");
        Ok(true)
    }

    #[step]
    async fn check_fraud(self: Arc<Self>) -> Result<bool, String> {
        println!("  [Validation] Running fraud check (1s delay)...");
        schedule_timer_named(Duration::from_secs(1), "fraud-check")
            .await
            .map_err(|e| e.to_string())?;
        println!("  [Validation] Fraud check passed");
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
        println!("  [Validation] Aggregating results...");

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

// ============================================================================
// DOMAIN TYPES
// ============================================================================

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
    println!("=== COMPREHENSIVE ERGON DEMO ===\n");

    let storage = Arc::new(SqliteExecutionLog::new("data/comprehensive.db").await?);
    storage.reset().await?;
    let scheduler = Scheduler::new(storage.clone()).unversioned();

    let mut worker_handles = Vec::new();
    for i in 1..=3 {
        let storage_clone = storage.clone();
        let worker = Worker::new(storage_clone, format!("worker-{}", i))
            .with_poll_interval(Duration::from_millis(100))
            .with_timers()
            .with_timer_interval(Duration::from_millis(50));

        worker.register(|f: Arc<OrderFlow>| f.process()).await;
        worker.register(|f: Arc<PaymentFlow>| f.process()).await;
        worker.register(|f: Arc<ShippingFlow>| f.process()).await;

        let handle = worker.start().await;
        worker_handles.push(handle);
        println!("[Setup] Worker {} started", i);
    }

    println!("\n[Setup] Scheduling orders...\n");

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
        let task_id = scheduler.schedule(order.clone()).await?;
        task_ids.push(task_id);
        println!("[Client] Scheduled {}, task_id={}", order.order_id, task_id);
    }

    println!("\n[Client] Waiting for completion (event-driven)...\n");

    let status_notify = storage.status_notify().clone();
    let timeout = Duration::from_secs(30);

    tokio::time::timeout(timeout, async {
        loop {
            let mut all_complete = true;

            for &task_id in &task_ids {
                if let Some(task) = storage.get_scheduled_flow(task_id).await? {
                    match task.status {
                        TaskStatus::Complete => {}
                        TaskStatus::Failed => {
                            println!("[Client] Flow {} failed: {:?}", task_id, task.error_message);
                        }
                        _ => {
                            all_complete = false;
                        }
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

    println!("\n[Client] All orders complete!");
    println!("\n=== FINAL RESULTS ===\n");

    for task_id in &task_ids {
        if let Some(task) = storage.get_scheduled_flow(*task_id).await? {
            println!("Task: {}", task_id);
            println!("  Status: {:?}", task.status);
            println!("  Retry Count: {}", task.retry_count);

            if let Some(error) = task.error_message {
                println!("  Error: {}", error);
            }

            if let Some(inv) = storage.get_invocation(task.flow_id, 0).await? {
                if let Some(result_bytes) = inv.return_value() {
                    if let Ok(result) =
                        serde_json::from_slice::<Result<OrderReceipt, ExecutionError>>(result_bytes)
                    {
                        match result {
                            Ok(receipt) => println!("  Receipt: {:?}", receipt),
                            Err(e) => println!("  Error: {:?}", e),
                        }
                    }
                }
            }
            println!();
        }
    }

    for (i, handle) in worker_handles.into_iter().enumerate() {
        handle.shutdown().await;
        println!("[Cleanup] Worker {} stopped", i + 1);
    }

    storage.close().await?;
    println!("[Cleanup] Storage closed");

    Ok(())
}
