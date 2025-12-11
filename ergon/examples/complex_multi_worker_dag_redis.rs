//! Complex Multi-Worker DAG with Parent-Child Flows (Redis)
//!
//! This example demonstrates the full power of Ergon by combining:
//! 1. **Multiple Workers** (4 workers) processing concurrently
//! 2. **Multiple Parent Flows** (3 orders) executing in parallel
//! 3. **DAG Execution** with parallel steps and dependencies
//! 4. **Child Flow Invocation** from DAG steps
//! 5. **Error Handling** with retryable vs permanent errors
//! 6. **Load Distribution** across workers
//!
//! ## Scenario: E-Commerce Order Fulfillment System
//!
//! Each order goes through a complex DAG:
//!
//! ```text
//! validate_customer ──┬──> process_payment ──┐
//!                     │                       │
//!                     └──> check_fraud ───────┤
//!                     │                       │
//!                     └──> reserve_inventory ─┤
//!                                             │
//!                                             ├──> generate_label (CHILD FLOW)
//!                                             │
//!                                             └──> notify_customer
//! ```
//!
//! - validate_customer, check_fraud, reserve_inventory run in PARALLEL
//! - process_payment waits for validate_customer
//! - generate_label is a CHILD FLOW (spawns separate task)
//! - notify_customer waits for ALL steps to complete
//!
//! ## Workers
//!
//! - validation-worker: Specializes in customer validation
//! - payment-worker: Handles payment processing
//! - warehouse-worker: Manages inventory
//! - shipping-worker: Generates labels and notifications
//!
//! ## Run
//!
//! ```bash
//! cargo run --example complex_multi_worker_dag_redis --features=redis
//! ```

use chrono::Utc;
use dashmap::DashMap;
use ergon::core::{FlowType, InvokableFlow};
use ergon::executor::{ExecutionError, InvokeChild, Worker};
use ergon::prelude::*;
use ergon::storage::RedisExecutionLog;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
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
    /// Increment and return validate_customer attempt counter
    fn inc_validate(order_id: &str) -> u32 {
        // Fast path: use read lock if entry exists
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.validate_customer.fetch_add(1, Ordering::Relaxed) + 1;
        }

        // Slow path: create entry with write lock only on first access
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .validate_customer
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    /// Increment and return check_fraud attempt counter
    fn inc_fraud(order_id: &str) -> u32 {
        // Fast path: use read lock if entry exists
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.check_fraud.fetch_add(1, Ordering::Relaxed) + 1;
        }

        // Slow path: create entry with write lock only on first access
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .check_fraud
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    /// Increment and return reserve_inventory attempt counter
    fn inc_reserve(order_id: &str) -> u32 {
        // Fast path: use read lock if entry exists
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.reserve_inventory.fetch_add(1, Ordering::Relaxed) + 1;
        }

        // Slow path: create entry with write lock only on first access
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .reserve_inventory
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    /// Increment and return process_payment attempt counter
    fn inc_payment(order_id: &str) -> u32 {
        // Fast path: use read lock if entry exists
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.process_payment.fetch_add(1, Ordering::Relaxed) + 1;
        }

        // Slow path: create entry with write lock only on first access
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .process_payment
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    /// Increment and return generate_label attempt counter
    fn inc_label(order_id: &str) -> u32 {
        // Fast path: use read lock if entry exists
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.generate_label.fetch_add(1, Ordering::Relaxed) + 1;
        }

        // Slow path: create entry with write lock only on first access
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .generate_label
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    /// Increment and return notify_customer attempt counter
    fn inc_notify(order_id: &str) -> u32 {
        // Fast path: use read lock if entry exists
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.notify_customer.fetch_add(1, Ordering::Relaxed) + 1;
        }

        // Slow path: create entry with write lock only on first access
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .notify_customer
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }
}

// =============================================================================
// Custom Error Types with RetryableError
// =============================================================================

/// Comprehensive order fulfillment error type
#[derive(Debug, Clone, Serialize, Deserialize)]
enum OrderError {
    // Payment errors - transient
    PaymentNetworkTimeout,
    PaymentGatewayUnavailable,

    // Payment errors - permanent
    InsufficientFunds,
    CardDeclined,
    FraudDetected,

    // Inventory errors - transient
    InventoryDatabaseTimeout,
    WarehouseSystemDown,

    // Inventory errors - permanent
    OutOfStock { product: String, requested: u32 },
    InvalidProductId,

    // Generic errors
    Failed(String),
}

impl std::fmt::Display for OrderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderError::PaymentNetworkTimeout => write!(f, "Network timeout"),
            OrderError::PaymentGatewayUnavailable => write!(f, "Payment gateway unavailable"),
            OrderError::InsufficientFunds => write!(f, "Insufficient funds"),
            OrderError::CardDeclined => write!(f, "Card declined"),
            OrderError::FraudDetected => write!(f, "Fraud detected"),
            OrderError::InventoryDatabaseTimeout => write!(f, "Database timeout"),
            OrderError::WarehouseSystemDown => write!(f, "Warehouse system down"),
            OrderError::OutOfStock { product, requested } => {
                write!(f, "Out of stock: {} (requested: {})", product, requested)
            }
            OrderError::InvalidProductId => write!(f, "Invalid product ID"),
            OrderError::Failed(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for OrderError {}

impl From<OrderError> for String {
    fn from(err: OrderError) -> Self {
        err.to_string()
    }
}

impl From<String> for OrderError {
    fn from(s: String) -> Self {
        OrderError::Failed(s)
    }
}

impl RetryableError for OrderError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            OrderError::PaymentNetworkTimeout
                | OrderError::PaymentGatewayUnavailable
                | OrderError::InventoryDatabaseTimeout
                | OrderError::WarehouseSystemDown
        )
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
// Parent Flow - Order Fulfillment (with DAG)
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
    /// Step 1: Validate Customer (runs in parallel)
    #[step]
    async fn validate_customer(self: Arc<Self>) -> Result<String, String> {
        let count = OrderAttempts::inc_validate(&self.order_id); // Lock released immediately
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

    /// Step 2: Check Fraud (runs in parallel with validate_customer)
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

    /// Step 3: Reserve Inventory (runs in parallel, uses RetryableError)
    #[step]
    async fn reserve_inventory(self: Arc<Self>) -> Result<bool, InventoryError> {
        let count = OrderAttempts::inc_reserve(&self.order_id); // Lock released immediately
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

    /// Step 4: Process Payment (depends on validate_customer, uses RetryableError)
    #[step(depends_on = "validate_customer")]
    async fn process_payment(self: Arc<Self>) -> Result<bool, PaymentError> {
        let count = OrderAttempts::inc_payment(&self.order_id); // Lock released immediately
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

    /// Step 5: Generate Shipping Label (CHILD FLOW - depends on all parallel steps)
    #[step(depends_on = ["validate_customer", "check_fraud", "reserve_inventory", "process_payment"])]
    async fn generate_shipping_label(self: Arc<Self>) -> Result<ShippingLabel, String> {
        let count = OrderAttempts::inc_label(&self.order_id);
        GENERATE_LABEL_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] generate_shipping_label (attempt #{}) - INVOKING CHILD FLOW",
            timestamp(),
            &self.order_id,
            count
        );

        // Invoke child flow for label generation
        let label = self
            .invoke(LabelGenerator {
                order_id: self.order_id.clone(),
                customer_id: self.customer_id.clone(),
            })
            .result()
            .await
            .map_err(|e| format!("Label generation failed: {}", e))?;

        println!(
            "[{:.3}]      -> Label generated: {}",
            timestamp(),
            label.tracking_number
        );
        Ok(label)
    }

    /// Step 6: Notify Customer (depends on label) - returns label for flow result
    #[step(
        depends_on = "generate_shipping_label",
        inputs(label = "generate_shipping_label")
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

    /// Main SEQUENTIAL flow (NO DAG)
    #[flow]
    async fn fulfill_order(self: Arc<Self>) -> Result<OrderSummary, String> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "\n[{:.3}] ORDER[{}] flow_id={}: Starting fulfillment (SEQUENTIAL - NO DAG)",
            timestamp(),
            self.order_id,
            &flow_id.to_string()[..8]
        );

        // Run steps SEQUENTIALLY (no DAG parallelism)
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
        let label = self.clone().generate_shipping_label().await?;
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

#[derive(Clone, Serialize, Deserialize)]
struct LabelGenerator {
    order_id: String,
    customer_id: String,
}

impl FlowType for LabelGenerator {
    fn type_id() -> &'static str {
        "LabelGenerator"
    }
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
// Main - Multi-Worker Stress Test
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = "redis://127.0.0.1:6379";
    // Use shorter stale lock timeout (30s instead of 5min) for testing
    // This allows recovery of stuck flows during test execution
    let storage = Arc::new(
        RedisExecutionLog::with_full_config(
            redis_url,
            7 * 24 * 3600,  // completed_ttl: 7 days
            30 * 24 * 3600, // signal_ttl: 30 days
            30_000,         // stale_lock_timeout_ms: 30 seconds (was 5 minutes!)
        )
        .await?,
    );
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone());

    // Schedule 3 orders with different characteristics
    let orders = vec![
        OrderFulfillment {
            order_id: "ORD-001".to_string(),
            customer_id: "CUST-001".to_string(), // No validation retry
            product_id: "PROD-001".to_string(),  // No inventory retry
            amount: 299.99,
            quantity: 2,
        },
        OrderFulfillment {
            order_id: "ORD-002".to_string(),
            customer_id: "CUST-002".to_string(),
            product_id: "PROD-002".to_string(),
            amount: 149.99,
            quantity: 1,
        },
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
        println!("   - {} scheduled", order.order_id);
    }

    // Start 4 workers

    let workers: Vec<_> = (1..=4)
        .map(|i| {
            let storage = storage.clone();
            let worker_name = match i {
                1 => "validation-worker",
                2 => "payment-worker",
                3 => "warehouse-worker",
                _ => "shipping-worker",
            };

            tokio::spawn(async move {
                let worker = Worker::new(storage, worker_name)
                    .with_poll_interval(Duration::from_millis(100)); // Reduced contention: was 50ms

                worker
                    .register(|flow: Arc<OrderFulfillment>| flow.fulfill_order())
                    .await;
                worker
                    .register(|flow: Arc<LabelGenerator>| flow.generate())
                    .await;

                let handle = worker.start().await;
                tokio::time::sleep(Duration::from_secs(45)).await; // Increased from 35s to allow all retries
                handle.shutdown().await;
            })
        })
        .collect();

    // Wait for all workers
    for worker in workers {
        worker.await?;
    }

    println!(
        "  validate_customer:   {}",
        VALIDATE_CUSTOMER_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  check_fraud:         {}",
        CHECK_FRAUD_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  reserve_inventory:   {}",
        RESERVE_INVENTORY_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  process_payment:     {}",
        PROCESS_PAYMENT_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  generate_label:      {} (child flow invocations)",
        GENERATE_LABEL_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  notify_customer:     {}",
        NOTIFY_CUSTOMER_COUNT.load(Ordering::Relaxed)
    );

    for i in 1..=3 {
        let order_id = format!("ORD-{:03}", i);
        if let Some(attempts) = ORDER_ATTEMPTS.get(&order_id) {
            println!(
                "  {}: validate={}, fraud={}, inventory={}, payment={}, label={}, notify={}",
                order_id,
                attempts.validate_customer.load(Ordering::Relaxed),
                attempts.check_fraud.load(Ordering::Relaxed),
                attempts.reserve_inventory.load(Ordering::Relaxed),
                attempts.process_payment.load(Ordering::Relaxed),
                attempts.generate_label.load(Ordering::Relaxed),
                attempts.notify_customer.load(Ordering::Relaxed)
            );
        }
    }

    storage.close().await?;
    Ok(())
}
