//! Multi-Worker Parent-Child Flow Distribution (SQLite)
//!
//! This example demonstrates:
//! - Multiple workers (3) processing from the same SQLite queue
//! - Parent flow invoking multiple children concurrently
//! - Load distribution across workers
//! - Which worker handles which flow (parent vs children)
//! - Proper coordination between workers for parent-child signaling
//!
//! ## Scenario: E-Commerce Order Processing
//!
//! A single parent flow (OrderFulfillment) spawns three child flows:
//! - InventoryCheck: Verifies product availability
//! - PaymentProcessing: Charges customer payment method
//! - ShippingLabel: Generates shipping label
//!
//! Three workers (warehouse-worker, payment-worker, shipping-worker) poll the same
//! SQLite queue. Each worker can pick up any flow type. The example shows which
//! worker handles which piece of the order.
//!
//! ## Key Observations
//!
//! - Parent suspends while children execute
//! - Different workers may handle parent vs children
//! - Children can be processed in parallel by different workers
//! - Parent resumes once all children complete
//! - SQLite queue safely coordinates work across workers
//!
//! ## Run
//!
//! ```bash
//! cargo run --example child_flow_multi_worker --features=sqlite
//! ```

use chrono::Utc;
use ergon::core::InvokableFlow;
use ergon::executor::InvokeChild;
use ergon::prelude::*;
use ergon::TaskStatus;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

// Counters to track execution
static INVENTORY_CHECKS: AtomicU32 = AtomicU32::new(0);
static PAYMENT_PROCESSED: AtomicU32 = AtomicU32::new(0);
static LABELS_GENERATED: AtomicU32 = AtomicU32::new(0);

// =============================================================================
// Domain Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InventoryStatus {
    product_id: String,
    available: bool,
    quantity: u32,
    checked_by_worker: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentConfirmation {
    transaction_id: String,
    amount: f64,
    processed_by_worker: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShippingInfo {
    tracking_number: String,
    carrier: String,
    generated_by_worker: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderResult {
    order_id: String,
    status: String,
    inventory: InventoryStatus,
    payment: PaymentConfirmation,
    shipping: ShippingInfo,
}

// =============================================================================
// Parent Flow - Order Fulfillment
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderFulfillment {
    order_id: String,
    product_id: String,
    customer_email: String,
    amount: f64,
}

impl OrderFulfillment {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<OrderResult, String> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "\n[{:.3}] PARENT[{}]: Processing order {}",
            timestamp(),
            &flow_id.to_string()[..8],
            self.order_id
        );

        // Invoke three children concurrently (Level 3 API)
        println!(
            "[{:.3}]    └─ Spawning 3 child flows (inventory, payment, shipping)...",
            timestamp()
        );

        // Child 1: Check Inventory
        let inventory = self
            .invoke(InventoryCheck {
                product_id: self.product_id.clone(),
            })
            .result()
            .await
            .map_err(|e| format!("Inventory check failed: {}", e))?;

        // Child 2: Process Payment
        let payment = self
            .invoke(PaymentProcessing {
                customer_email: self.customer_email.clone(),
                amount: self.amount,
            })
            .result()
            .await
            .map_err(|e| format!("Payment failed: {}", e))?;

        // Child 3: Generate Shipping Label
        let shipping = self
            .invoke(ShippingLabel {
                order_id: self.order_id.clone(),
                customer_email: self.customer_email.clone(),
            })
            .result()
            .await
            .map_err(|e| format!("Shipping label failed: {}", e))?;

        println!(
            "[{:.3}] PARENT[{}]: All children completed",
            timestamp(),
            &flow_id.to_string()[..8]
        );

        Ok(OrderResult {
            order_id: self.order_id.clone(),
            status: "completed".to_string(),
            inventory,
            payment,
            shipping,
        })
    }
}

// =============================================================================
// Child Flow 1 - Inventory Check
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct InventoryCheck {
    product_id: String,
}

impl InvokableFlow for InventoryCheck {
    type Output = InventoryStatus;
}

impl InventoryCheck {
    #[flow]
    async fn check(self: Arc<Self>) -> Result<InventoryStatus, String> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        let count = INVENTORY_CHECKS.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "[{:.3}]    ├─ CHILD[{}] InventoryCheck: Checking inventory for {} (check #{})",
            timestamp(),
            &flow_id.to_string()[..8],
            self.product_id,
            count
        );

        // Simulate inventory check
        tokio::time::sleep(Duration::from_millis(300)).await;

        let result = InventoryStatus {
            product_id: self.product_id.clone(),
            available: true,
            quantity: 50,
            checked_by_worker: "worker".to_string(),
        };

        println!(
            "[{:.3}]    │  └─ Inventory check complete: {} units available",
            timestamp(),
            result.quantity
        );

        Ok(result)
    }
}

// =============================================================================
// Child Flow 2 - Payment Processing
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct PaymentProcessing {
    customer_email: String,
    amount: f64,
}

impl InvokableFlow for PaymentProcessing {
    type Output = PaymentConfirmation;
}

impl PaymentProcessing {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<PaymentConfirmation, String> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        let count = PAYMENT_PROCESSED.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "[{:.3}]    ├─ CHILD[{}] PaymentProcessing: Processing payment ${:.2} (attempt #{})",
            timestamp(),
            &flow_id.to_string()[..8],
            self.amount,
            count
        );

        // Simulate payment processing
        tokio::time::sleep(Duration::from_millis(400)).await;

        let result = PaymentConfirmation {
            transaction_id: format!("TXN-{}", Uuid::new_v4().to_string()[..8].to_uppercase()),
            amount: self.amount,
            processed_by_worker: "worker".to_string(),
        };

        println!(
            "[{:.3}]    │  └─ Payment processed: {}",
            timestamp(),
            result.transaction_id
        );

        Ok(result)
    }
}

// =============================================================================
// Child Flow 3 - Shipping Label
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct ShippingLabel {
    order_id: String,
    customer_email: String,
}

impl InvokableFlow for ShippingLabel {
    type Output = ShippingInfo;
}

impl ShippingLabel {
    #[flow]
    async fn generate(self: Arc<Self>) -> Result<ShippingInfo, String> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        let count = LABELS_GENERATED.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "[{:.3}]    └─ CHILD[{}] ShippingLabel: Generating shipping label (label #{})",
            timestamp(),
            &flow_id.to_string()[..8],
            count
        );

        // Simulate label generation
        tokio::time::sleep(Duration::from_millis(250)).await;

        let result = ShippingInfo {
            tracking_number: format!("TRACK-{}", Uuid::new_v4().to_string()[..8].to_uppercase()),
            carrier: "FedEx".to_string(),
            generated_by_worker: "worker".to_string(),
        };

        println!(
            "[{:.3}]       └─ Label generated: {}",
            timestamp(),
            result.tracking_number
        );

        Ok(result)
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn timestamp() -> f64 {
    let now = Utc::now();
    now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0)
}

// =============================================================================
// Main - Multi-Worker Setup
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(SqliteExecutionLog::new("multi_worker.db").await?);
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone());

    let order1 = OrderFulfillment {
        order_id: "ORD-001".to_string(),
        product_id: "WIDGET-PRO".to_string(),
        customer_email: "alice@example.com".to_string(),
        amount: 149.99,
    };

    let order2 = OrderFulfillment {
        order_id: "ORD-002".to_string(),
        product_id: "GADGET-ULTRA".to_string(),
        customer_email: "bob@example.com".to_string(),
        amount: 299.99,
    };

    let task_id_1 = scheduler.schedule(order1, Uuid::new_v4()).await?;
    let task_id_2 = scheduler.schedule(order2, Uuid::new_v4()).await?;

    let storage_clone = storage.clone();
    let worker1 = tokio::spawn(async move {
        let worker = Worker::new(storage_clone, "warehouse-worker")
            .with_poll_interval(Duration::from_millis(100));

        worker
            .register(|flow: Arc<OrderFulfillment>| flow.process())
            .await;
        worker
            .register(|flow: Arc<InventoryCheck>| flow.check())
            .await;
        worker
            .register(|flow: Arc<PaymentProcessing>| flow.process())
            .await;
        worker
            .register(|flow: Arc<ShippingLabel>| flow.generate())
            .await;

        let handle = worker.start().await;
        handle
    });

    let storage_clone = storage.clone();
    let worker2 = tokio::spawn(async move {
        let worker = Worker::new(storage_clone, "payment-worker")
            .with_poll_interval(Duration::from_millis(100));

        worker
            .register(|flow: Arc<OrderFulfillment>| flow.process())
            .await;
        worker
            .register(|flow: Arc<InventoryCheck>| flow.check())
            .await;
        worker
            .register(|flow: Arc<PaymentProcessing>| flow.process())
            .await;
        worker
            .register(|flow: Arc<ShippingLabel>| flow.generate())
            .await;

        let handle = worker.start().await;
        handle
    });

    let storage_clone = storage.clone();
    let worker3 = tokio::spawn(async move {
        let worker = Worker::new(storage_clone, "shipping-worker")
            .with_poll_interval(Duration::from_millis(100));

        worker
            .register(|flow: Arc<OrderFulfillment>| flow.process())
            .await;
        worker
            .register(|flow: Arc<InventoryCheck>| flow.check())
            .await;
        worker
            .register(|flow: Arc<PaymentProcessing>| flow.process())
            .await;
        worker
            .register(|flow: Arc<ShippingLabel>| flow.generate())
            .await;

        let handle = worker.start().await;
        handle
    });

    let w1 = worker1.await?;
    let w2 = worker2.await?;
    let w3 = worker3.await?;

    // Wait for all flows to complete
    let timeout_duration = Duration::from_secs(10);
    let task_ids = vec![task_id_1, task_id_2];
    let wait_result = tokio::time::timeout(timeout_duration, async {
        loop {
            let mut all_complete = true;
            for &task_id in &task_ids {
                if let Some(scheduled) = storage.get_scheduled_flow(task_id).await? {
                    if !matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                        all_complete = false;
                        break;
                    }
                }
            }
            if all_complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await;

    match wait_result {
        Ok(_) => {}
        Err(_) => println!("[WARN] Timeout waiting for orders to complete"),
    }

    // Shutdown workers
    w1.shutdown().await;
    w2.shutdown().await;
    w3.shutdown().await;

    storage.close().await?;
    Ok(())
}
