//! Sequential Flow with Child Flows (SQLite)
//!
//! This example demonstrates:
//! 1. **Single Worker** execution
//! 2. **Sequential Steps** (steps run one after another)
//! 3. **Child Flow Invocation** (spawning sub-tasks)
//! 4. **Error Handling** with retryable errors
//!
//! ## Scenario: E-Commerce Order Fulfillment System
//!
//! Each order goes through a linear sequence:
//!
//! 1. validate_customer
//! 2. check_fraud
//! 3. reserve_inventory
//! 4. process_payment
//! 5. generate_label (CHILD FLOW)
//! 6. notify_customer
//!
//! ## Run
//!
//! ```bash
//! cargo run --example complex_1_worker_dag_sqlite --features=sqlite
//! ```

use chrono::Utc;
use dashmap::DashMap;
use ergon::core::InvokableFlow;
use ergon::executor::{ExecutionError, InvokeChild};
use ergon::prelude::*;
use ergon::Retryable;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::LazyLock;
use std::time::Duration;

// Global execution counters (for summary statistics only)
static VALIDATE_CUSTOMER_COUNT: AtomicU32 = AtomicU32::new(0);
static CHECK_FRAUD_COUNT: AtomicU32 = AtomicU32::new(0);
static RESERVE_INVENTORY_COUNT: AtomicU32 = AtomicU32::new(0);
static PROCESS_PAYMENT_COUNT: AtomicU32 = AtomicU32::new(0);
static GENERATE_LABEL_COUNT: AtomicU32 = AtomicU32::new(0);
static NOTIFY_CUSTOMER_COUNT: AtomicU32 = AtomicU32::new(0);

// Per-order attempt tracking for retry logic
static ORDER_ATTEMPTS: LazyLock<DashMap<String, OrderAttempts>> = LazyLock::new(DashMap::new);

/// Per-order attempt counters (each order tracks its own retry attempts)
#[derive(Default)]
struct OrderAttempts {
    validate_customer: AtomicU32,
    check_fraud: AtomicU32,
    reserve_inventory: AtomicU32,
    process_payment: AtomicU32,
    generate_label: AtomicU32,
    notify_customer: AtomicU32,
}

impl OrderAttempts {
    fn inc_validate(order_id: &str) -> u32 {
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.validate_customer.fetch_add(1, Ordering::Relaxed) + 1;
        }
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .validate_customer
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    fn inc_fraud(order_id: &str) -> u32 {
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.check_fraud.fetch_add(1, Ordering::Relaxed) + 1;
        }
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .check_fraud
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    fn inc_reserve(order_id: &str) -> u32 {
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.reserve_inventory.fetch_add(1, Ordering::Relaxed) + 1;
        }
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .reserve_inventory
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    fn inc_payment(order_id: &str) -> u32 {
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.process_payment.fetch_add(1, Ordering::Relaxed) + 1;
        }
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .process_payment
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    fn inc_label(order_id: &str) -> u32 {
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.generate_label.fetch_add(1, Ordering::Relaxed) + 1;
        }
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .generate_label
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    fn inc_notify(order_id: &str) -> u32 {
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.notify_customer.fetch_add(1, Ordering::Relaxed) + 1;
        }
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .notify_customer
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }
}

// =============================================================================
// Custom Error Types with Retryable
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PaymentError {
    // Transient - WILL retry
    NetworkTimeout,
    GatewayUnavailable,

    // Permanent - will NOT retry
    InsufficientFunds,
    CardDeclined,
    FraudDetected,
}

impl std::fmt::Display for PaymentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PaymentError::NetworkTimeout => write!(f, "Network timeout"),
            PaymentError::GatewayUnavailable => write!(f, "Payment gateway unavailable"),
            PaymentError::InsufficientFunds => write!(f, "Insufficient funds"),
            PaymentError::CardDeclined => write!(f, "Card declined"),
            PaymentError::FraudDetected => write!(f, "Fraud detected"),
        }
    }
}

impl Retryable for PaymentError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            PaymentError::NetworkTimeout | PaymentError::GatewayUnavailable
        )
    }
}

impl From<PaymentError> for ExecutionError {
    fn from(e: PaymentError) -> Self {
        ExecutionError::Failed(e.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum InventoryError {
    // Transient
    DatabaseTimeout,
    WarehouseSystemDown,

    // Permanent
    OutOfStock { product: String, requested: u32 },
    InvalidProductId,
}

impl std::fmt::Display for InventoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InventoryError::DatabaseTimeout => write!(f, "Database timeout"),
            InventoryError::WarehouseSystemDown => write!(f, "Warehouse system down"),
            InventoryError::OutOfStock { product, requested } => {
                write!(f, "Out of stock: {} (requested: {})", product, requested)
            }
            InventoryError::InvalidProductId => write!(f, "Invalid product ID"),
        }
    }
}

impl ergon::Retryable for InventoryError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            InventoryError::DatabaseTimeout | InventoryError::WarehouseSystemDown
        )
    }
}

impl From<InventoryError> for ExecutionError {
    fn from(e: InventoryError) -> Self {
        ExecutionError::Failed(e.to_string())
    }
}

// =============================================================================
// Domain Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShippingLabel {
    tracking_number: String,
    carrier: String,
    estimated_delivery: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderSummary {
    order_id: String,
    customer_id: String,
    amount: f64,
    payment_authorized: bool,
    inventory_reserved: bool,
    label: ShippingLabel,
    notification_sent: bool,
}

// =============================================================================
// Parent Flow - Order Fulfillment
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderFulfillment {
    order_id: String,
    customer_id: String,
    product_id: String,
    amount: f64,
    quantity: u32,
}

impl OrderFulfillment {
    /// Step 1: Validate Customer
    #[step]
    async fn validate_customer(self: Arc<Self>) -> Result<String, String> {
        let count = OrderAttempts::inc_validate(&self.order_id);
        VALIDATE_CUSTOMER_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] validate_customer (attempt #{})",
            timestamp(),
            &self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate occasional transient failure on first attempt for this order
        if count == 1 && self.customer_id == "CUST-RETRY" {
            println!("[{:.3}]      -> Transient error, will retry", timestamp());
            return Err("Customer validation timeout".to_string());
        }

        println!("[{:.3}]      -> Customer validated", timestamp());
        Ok(self.customer_id.clone())
    }

    /// Step 2: Check Fraud
    #[step]
    async fn check_fraud(self: Arc<Self>) -> Result<bool, String> {
        let count = OrderAttempts::inc_fraud(&self.order_id);
        CHECK_FRAUD_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] check_fraud (attempt #{})",
            timestamp(),
            &self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(150)).await;

        if self.customer_id == "CUST-FRAUD" {
            println!("[{:.3}]      -> FRAUD DETECTED (permanent)", timestamp());
            return Err("Fraud detected for customer".to_string());
        }

        println!("[{:.3}]      -> No fraud detected", timestamp());
        Ok(true)
    }

    /// Step 3: Reserve Inventory (uses Retryable)
    #[step]
    async fn reserve_inventory(self: Arc<Self>) -> Result<bool, InventoryError> {
        let count = OrderAttempts::inc_reserve(&self.order_id);
        RESERVE_INVENTORY_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] reserve_inventory (attempt #{})",
            timestamp(),
            &self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(120)).await;

        // Simulate transient error on first attempt for this order
        if count == 1 && self.product_id == "PROD-SLOW" {
            println!(
                "[{:.3}]      -> Warehouse system timeout (retryable)",
                timestamp()
            );
            return Err(InventoryError::WarehouseSystemDown);
        }

        // Check stock
        if self.product_id == "PROD-OOS" {
            println!("[{:.3}]      -> Out of stock (permanent)", timestamp());
            return Err(InventoryError::OutOfStock {
                product: self.product_id.clone(),
                requested: self.quantity,
            });
        }

        println!("[{:.3}]      -> Inventory reserved", timestamp());
        Ok(true)
    }

    /// Step 4: Process Payment (uses Retryable)
    #[step(depends_on = "validate_customer")]
    async fn process_payment(self: Arc<Self>) -> Result<bool, PaymentError> {
        let count = OrderAttempts::inc_payment(&self.order_id);
        PROCESS_PAYMENT_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] process_payment (attempt #{})",
            timestamp(),
            &self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check for insufficient funds
        if self.amount > 10000.0 {
            println!(
                "[{:.3}]      -> Insufficient funds (permanent)",
                timestamp()
            );
            return Err(PaymentError::InsufficientFunds);
        }

        println!("[{:.3}]      -> Payment authorized", timestamp());
        Ok(true)
    }

    /// Step 5: Process shipping label result
    #[step(depends_on = ["validate_customer", "check_fraud", "reserve_inventory", "process_payment"])]
    async fn process_shipping_label(
        self: Arc<Self>,
        label: ShippingLabel,
    ) -> Result<ShippingLabel, String> {
        let count = OrderAttempts::inc_label(&self.order_id);
        GENERATE_LABEL_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] process_shipping_label (attempt #{})",
            timestamp(),
            &self.order_id,
            count
        );

        println!(
            "[{:.3}]      -> Label processed: {}",
            timestamp(),
            label.tracking_number
        );
        Ok(label)
    }

    /// Step 6: Notify Customer - returns label for flow result
    #[step(
        depends_on = "process_shipping_label",
        inputs(label = "process_shipping_label")
    )]
    async fn notify_customer(
        self: Arc<Self>,
        label: ShippingLabel,
    ) -> Result<ShippingLabel, String> {
        let count = OrderAttempts::inc_notify(&self.order_id);
        NOTIFY_CUSTOMER_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] notify_customer (attempt #{})",
            timestamp(),
            &self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        println!(
            "[{:.3}]      -> Customer notified (tracking: {})",
            timestamp(),
            label.tracking_number
        );
        Ok(label)
    }

    /// Main SEQUENTIAL flow
    #[flow]
    async fn fulfill_order(self: Arc<Self>) -> Result<OrderSummary, String> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "\n[{:.3}] ORDER[{}] flow_id={}: Starting fulfillment (SEQUENTIAL)",
            timestamp(),
            self.order_id,
            &flow_id.to_string()[..8]
        );

        // Run steps SEQUENTIALLY
        let _customer = self.clone().validate_customer().await?;
        let _fraud = self.clone().check_fraud().await?;
        let _inventory = self
            .clone()
            .reserve_inventory()
            .await
            .map_err(|e| e.to_string())?;
        let _payment = self
            .clone()
            .process_payment()
            .await
            .map_err(|e| e.to_string())?;

        // Invoke child flow at flow level
        let label = self
            .invoke(LabelGenerator {
                order_id: self.order_id.clone(),
                customer_id: self.customer_id.clone(),
            })
            .result()
            .await
            .map_err(|e| format!("Label generation failed: {}", e))?;

        // Process label in atomic step
        let label = self.clone().process_shipping_label(label).await?;
        let label = self.clone().notify_customer(label).await?;

        println!(
            "[{:.3}] ORDER[{}] flow_id={}: Fulfillment complete\n",
            timestamp(),
            self.order_id,
            &flow_id.to_string()[..8]
        );

        Ok(OrderSummary {
            order_id: self.order_id.clone(),
            customer_id: self.customer_id.clone(),
            amount: self.amount,
            payment_authorized: true,
            inventory_reserved: true,
            label,
            notification_sent: true,
        })
    }
}

// =============================================================================
// Child Flow - Label Generator
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct LabelGenerator {
    order_id: String,
    customer_id: String,
}

impl InvokableFlow for LabelGenerator {
    type Output = ShippingLabel;
}

impl LabelGenerator {
    #[flow]
    async fn generate(self: Arc<Self>) -> Result<ShippingLabel, String> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{:.3}]     CHILD[{}]: Generating label for order {}",
            timestamp(),
            &flow_id.to_string()[..8],
            self.order_id
        );

        tokio::time::sleep(Duration::from_millis(200)).await;

        let label = ShippingLabel {
            tracking_number: format!("TRACK-{}", Uuid::new_v4().to_string()[..8].to_uppercase()),
            carrier: "FedEx".to_string(),
            estimated_delivery: "2024-01-15".to_string(),
        };

        println!(
            "[{:.3}]     CHILD[{}]: Label complete: {}",
            timestamp(),
            &flow_id.to_string()[..8],
            label.tracking_number
        );

        Ok(label)
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
// Main - Single-Worker Test (Isolating Logic Bugs)
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "/tmp/ergon_test_1_worker.db";
    let _ = std::fs::remove_file(db_path);

    let storage = Arc::new(SqliteExecutionLog::new(db_path).await?);

    let scheduler = Scheduler::new(storage.clone());

    // Schedule 3 orders with specific failure triggers
    let orders = vec![
        // Order 1: Triggers "CUST-RETRY" (fails validation once)
        OrderFulfillment {
            order_id: "ORD-001".to_string(),
            customer_id: "CUST-RETRY".to_string(), // <--- TRIGGER: Transient validation failure
            product_id: "PROD-001".to_string(),
            amount: 299.99,
            quantity: 2,
        },
        // Order 2: Triggers "PROD-SLOW" (fails inventory once)
        OrderFulfillment {
            order_id: "ORD-002".to_string(),
            customer_id: "CUST-002".to_string(),
            product_id: "PROD-SLOW".to_string(), // <--- TRIGGER: Transient inventory failure
            amount: 149.99,
            quantity: 1,
        },
        // Order 3: Clean run
        OrderFulfillment {
            order_id: "ORD-003".to_string(),
            customer_id: "CUST-003".to_string(),
            product_id: "PROD-003".to_string(),
            amount: 499.99,
            quantity: 3,
        },
    ];

    for order in &orders {
        scheduler.schedule(order.clone(), Uuid::new_v4()).await?;
    }

    // Start 1 worker
    let worker =
        Worker::new(storage.clone(), "test-worker").with_poll_interval(Duration::from_millis(100));

    worker
        .register(|flow: Arc<OrderFulfillment>| flow.fulfill_order())
        .await;
    worker
        .register(|flow: Arc<LabelGenerator>| flow.generate())
        .await;

    let handle = worker.start().await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    handle.shutdown().await;

    storage.close().await?;
    Ok(())
}
