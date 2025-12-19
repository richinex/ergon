//! Complex Multi-Worker with Signals (SQLite)
//!
//! This example demonstrates the full power of Ergon by combining:
//! 1. **Multiple Workers** (4 workers) processing concurrently
//! 2. **Multiple Parent Flows** (3 orders) executing in parallel
//! 3. **Sequential Steps** with suspension/resumption on signals
//! 4. **External Signals** for human-in-the-loop approval workflows
//! 5. **Child Flow Invocation** from workflow steps
//! 6. **Error Handling** with retryable vs permanent errors
//! 7. **Load Distribution** across workers
//!
//! ## Scenario: E-Commerce Order Fulfillment with Manager Approval
//!
//! Each order goes through a SEQUENTIAL workflow with a signal-based approval step:
//!
//! ```text
//! validate_customer → check_fraud → reserve_inventory → await_manager_approval (SIGNAL) →
//!     process_payment → generate_label (CHILD FLOW) → notify_customer
//! ```
//!
//! **Key behaviors:**
//! - All steps run SEQUENTIALLY (not parallel)
//! - await_manager_approval may suspend the flow until signal arrives
//! - generate_label spawns a CHILD FLOW (separate task)
//! - On retry, cached results avoid re-suspension (appears as attempt #2)
//!
//! ## Signal Integration
//!
//! The example uses `await_external_signal()` to suspend the flow until a manager
//! provides approval. The signal returns an `ApprovalDecision` which can be either
//! approved or rejected. This demonstrates:
//!
//! - **Flow Suspension**: Flow pauses at signal step until external input arrives
//! - **Signal Caching**: Once received, signal result is cached (no re-suspension on retry)
//! - **Human-in-the-Loop**: Real-world approval workflows with external decision makers
//! - **Signal Source Abstraction**: Easy to swap signal sources (mock, HTTP, Redis, etc.)
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
//! cargo run --example complex_multi_worker_dag_with_signals --features=sqlite
//! ```

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use ergon::core::{FlowType, InvokableFlow};
use ergon::executor::{await_external_signal, InvokeChild, SignalSource, Worker};
use ergon::prelude::*;
use ergon::storage::SqliteExecutionLog;
use ergon::TaskStatus;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;

// Global execution counters (for summary statistics only)
static VALIDATE_CUSTOMER_COUNT: AtomicU32 = AtomicU32::new(0);
static CHECK_FRAUD_COUNT: AtomicU32 = AtomicU32::new(0);
static RESERVE_INVENTORY_COUNT: AtomicU32 = AtomicU32::new(0);
static AWAIT_APPROVAL_COUNT: AtomicU32 = AtomicU32::new(0);
static PROCESS_PAYMENT_COUNT: AtomicU32 = AtomicU32::new(0);
static GENERATE_LABEL_COUNT: AtomicU32 = AtomicU32::new(0);
static NOTIFY_CUSTOMER_COUNT: AtomicU32 = AtomicU32::new(0);

// Per-order attempt tracking for retry logic
static ORDER_ATTEMPTS: LazyLock<DashMap<String, OrderAttempts>> = LazyLock::new(DashMap::new);

// Per-order timing tracking
static ORDER_TIMINGS: LazyLock<DashMap<String, f64>> = LazyLock::new(DashMap::new);

/// Per-order attempt counters (each order tracks its own retry attempts)
#[derive(Default)]
struct OrderAttempts {
    validate_customer: AtomicU32,
    check_fraud: AtomicU32,
    reserve_inventory: AtomicU32,
    await_approval: AtomicU32,
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

    /// Increment and return await_approval attempt counter
    fn inc_approval(order_id: &str) -> u32 {
        // Fast path: use read lock if entry exists
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.await_approval.fetch_add(1, Ordering::Relaxed) + 1;
        }

        // Slow path: create entry with write lock only on first access
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .await_approval
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
// Custom Error Types with Retryable
// =============================================================================

/// Comprehensive order fulfillment error type
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
enum OrderError {
    // Payment errors - transient
    #[error("Network timeout")]
    PaymentNetworkTimeout,
    #[error("Payment gateway unavailable")]
    PaymentGatewayUnavailable,

    // Payment errors - permanent
    #[error("Insufficient funds")]
    InsufficientFunds,
    #[error("Card declined")]
    CardDeclined,
    #[error("Fraud detected")]
    FraudDetected,

    // Inventory errors - transient
    #[error("Database timeout")]
    InventoryDatabaseTimeout,
    #[error("Warehouse system down")]
    WarehouseSystemDown,

    // Inventory errors - permanent
    #[error("Out of stock: {product} (requested: {requested})")]
    OutOfStock { product: String, requested: u32 },
    #[error("Invalid product ID")]
    InvalidProductId,

    // Approval errors - permanent
    #[error("Manager rejected by {by} - {reason}")]
    ManagerRejected { by: String, reason: String },

    // Infrastructure errors - transient
    #[error("Infrastructure error: {0}")]
    Infrastructure(String),

    // Generic errors
    #[error("{0}")]
    Failed(String),
}

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

impl ergon::Retryable for OrderError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            OrderError::PaymentNetworkTimeout
                | OrderError::PaymentGatewayUnavailable
                | OrderError::InventoryDatabaseTimeout
                | OrderError::WarehouseSystemDown
                | OrderError::Infrastructure(_)
        )
    }
}

// =============================================================================
// Signal-Related Types
// =============================================================================

/// Decision made by a manager via external signal
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApprovalDecision {
    approved: bool,
    approver: String,
    comments: String,
    timestamp: i64,
}

/// Outcome of an approval step (both approved and rejected are valid outcomes)
#[derive(Clone, Debug, Serialize, Deserialize)]
enum ApprovalOutcome {
    Approved { by: String, comment: String },
    Rejected { by: String, reason: String },
}

impl From<ApprovalDecision> for ApprovalOutcome {
    fn from(decision: ApprovalDecision) -> Self {
        if decision.approved {
            ApprovalOutcome::Approved {
                by: decision.approver,
                comment: decision.comments,
            }
        } else {
            ApprovalOutcome::Rejected {
                by: decision.approver,
                reason: decision.comments,
            }
        }
    }
}

// =============================================================================
// Simulated Approval Signal Source
// =============================================================================

/// Simulates manager approvals with automatic decisions after a delay.
/// In a real application, this would be replaced with:
/// - HTTP webhook endpoint receiving approval decisions
/// - Redis pub/sub listening for approval messages
/// - Database polling for approval records
/// - Message queue consumer (Kafka, RabbitMQ, etc.)
struct SimulatedApprovalSource {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl SimulatedApprovalSource {
    fn new() -> Self {
        Self {
            signals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Simulate manager making an approval decision after a delay
    async fn simulate_approval(&self, signal_name: &str, delay: Duration, approve: bool) {
        tokio::time::sleep(delay).await;

        let decision = ApprovalDecision {
            approved: approve,
            approver: "manager@company.com".to_string(),
            comments: if approve {
                "High-value order approved!".to_string()
            } else {
                "Requires additional verification".to_string()
            },
            timestamp: chrono::Utc::now().timestamp(),
        };

        let data = ergon::core::serialize_value(&decision).unwrap();
        let mut signals = self.signals.write().await;
        signals.insert(signal_name.to_string(), data);
        println!(
            "[{:.3}]   [SIGNAL] Manager decision received for '{}'",
            timestamp(),
            signal_name
        );
    }
}

#[async_trait]
impl SignalSource for SimulatedApprovalSource {
    async fn poll_for_signal(&self, signal_name: &str) -> Option<Vec<u8>> {
        let signals = self.signals.read().await;
        signals.get(signal_name).cloned()
    }

    async fn consume_signal(&self, signal_name: &str) {
        let mut signals = self.signals.write().await;
        signals.remove(signal_name);
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
// Parent Flow - Order Fulfillment (with DAG and Signals)
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
    /// Step 1: Validate Customer (runs in parallel with reserve_inventory)
    #[step]
    async fn validate_customer(self: Arc<Self>) -> Result<String, String> {
        let count = OrderAttempts::inc_validate(&self.order_id);
        VALIDATE_CUSTOMER_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] validate_customer (execution #{})",
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

    /// Step 2: Check Fraud (depends on validate_customer)
    #[step(depends_on = "validate_customer")]
    async fn check_fraud(self: Arc<Self>) -> Result<bool, String> {
        let count = OrderAttempts::inc_fraud(&self.order_id);
        CHECK_FRAUD_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] check_fraud (execution #{})",
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

    /// Step 3: Reserve Inventory (runs in parallel with validation)
    #[step]
    async fn reserve_inventory(self: Arc<Self>) -> Result<bool, OrderError> {
        let count = OrderAttempts::inc_reserve(&self.order_id);
        RESERVE_INVENTORY_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] reserve_inventory (execution #{})",
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
            return Err(OrderError::WarehouseSystemDown);
        }

        // Check stock
        if self.product_id == "PROD-OOS" {
            println!("[{:.3}]      -> Out of stock (permanent)", timestamp());
            return Err(OrderError::OutOfStock {
                product: self.product_id.clone(),
                requested: self.quantity,
            });
        }

        println!("[{:.3}]      -> Inventory reserved", timestamp());
        Ok(true)
    }

    /// Step 4: Await Manager Approval (SIGNAL - depends on check_fraud)
    ///
    /// This step demonstrates REPLAY-BASED RESUMPTION for external signals.
    ///
    /// Key behaviors:
    /// - **Execution #1**: May suspend flow, waiting for signal to arrive
    /// - **Execution #2**: Replays from beginning, retrieves cached signal result
    /// - Signal result is cached (replay doesn't re-suspend)
    /// - Both approval and rejection are valid outcomes (step succeeds)
    /// - The flow decides what rejection means (permanent failure in this case)
    #[step(depends_on = "check_fraud")]
    async fn await_manager_approval(self: Arc<Self>) -> Result<ApprovalOutcome, OrderError> {
        let count = OrderAttempts::inc_approval(&self.order_id);
        AWAIT_APPROVAL_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] await_manager_approval (execution #{})",
            timestamp(),
            &self.order_id,
            count
        );

        // REPLAY-BASED RESUMPTION:
        // Execution #1: await_external_signal() may suspend until signal arrives
        // Execution #2: await_external_signal() returns cached result immediately
        let decision: ApprovalDecision =
            await_external_signal(&format!("order_approval_{}", self.order_id))
                .await
                .map_err(|e| OrderError::Infrastructure(e.to_string()))?;

        // Convert decision to outcome - BOTH approved and rejected are successful step outcomes
        let outcome: ApprovalOutcome = decision.into();

        // Log the outcome
        match &outcome {
            ApprovalOutcome::Approved { by, comment } => {
                println!(
                    "[{:.3}]      -> Manager APPROVED by {} - {}",
                    timestamp(),
                    by,
                    comment
                );
            }
            ApprovalOutcome::Rejected { by, reason } => {
                println!(
                    "[{:.3}]      -> Manager REJECTED by {} - {}",
                    timestamp(),
                    by,
                    reason
                );
            }
        }

        // Step succeeds with the outcome (cached for replay)
        Ok(outcome)
    }

    /// Step 5: Process Payment (depends on manager approval)
    #[step(
        depends_on = "await_manager_approval",
        inputs(approval = "await_manager_approval")
    )]
    async fn process_payment(
        self: Arc<Self>,
        approval: ApprovalOutcome,
    ) -> Result<bool, OrderError> {
        let count = OrderAttempts::inc_payment(&self.order_id);
        PROCESS_PAYMENT_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] process_payment (execution #{})",
            timestamp(),
            &self.order_id,
            count
        );

        // Check if manager rejected - this becomes a permanent payment failure
        match approval {
            ApprovalOutcome::Rejected { by, reason } => {
                println!(
                    "[{:.3}]      -> Payment blocked: rejected by {} - {}",
                    timestamp(),
                    by,
                    reason
                );
                return Err(OrderError::ManagerRejected { by, reason }); // Non-retryable
            }
            ApprovalOutcome::Approved { .. } => {
                // Continue with payment processing
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Check for insufficient funds
        if self.amount > 10000.0 {
            println!(
                "[{:.3}]      -> Insufficient funds (permanent)",
                timestamp()
            );
            return Err(OrderError::InsufficientFunds);
        }

        println!("[{:.3}]      -> Payment authorized", timestamp());
        Ok(true)
    }

    /// Step 6: Notify Customer (label comes from flow-level child invocation)
    #[step]
    async fn notify_customer(
        self: Arc<Self>,
        label: ShippingLabel,
    ) -> Result<ShippingLabel, OrderError> {
        let count = OrderAttempts::inc_notify(&self.order_id);
        NOTIFY_CUSTOMER_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] notify_customer (execution #{})",
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
    async fn fulfill_order(self: Arc<Self>) -> Result<OrderSummary, OrderError> {
        println!(
            "\n[{:.3}] ORDER[{}] Starting SEQUENTIAL fulfillment",
            timestamp(),
            self.order_id
        );
        let start = std::time::Instant::now();

        // Run steps SEQUENTIALLY (no DAG parallelism)
        let _customer = self.clone().validate_customer().await?;
        let _fraud = self.clone().check_fraud().await?;
        let _inventory = self.clone().reserve_inventory().await?;

        // SIGNAL STEP - Flow may suspend here until manager approves
        let approval = self.clone().await_manager_approval().await?;

        let _payment = self.clone().process_payment(approval).await?;

        // Child flow invocation happens at flow level (not in a step - steps must be atomic!)
        let count = OrderAttempts::inc_label(&self.order_id);
        GENERATE_LABEL_COUNT.fetch_add(1, Ordering::Relaxed);
        println!(
            "[{:.3}]   [{}] generate_shipping_label (execution #{})",
            timestamp(),
            &self.order_id,
            count
        );

        let label = self
            .invoke(LabelGenerator {
                order_id: self.order_id.clone(),
                customer_id: self.customer_id.clone(),
            })
            .result()
            .await
            .map_err(|e| OrderError::Failed(format!("Child flow failed: {}", e)))?;

        println!(
            "[{:.3}]      -> Label generated: {}",
            timestamp(),
            label.tracking_number
        );

        let label = self.clone().notify_customer(label).await?;

        let duration = start.elapsed();
        ORDER_TIMINGS.insert(self.order_id.clone(), duration.as_secs_f64());
        println!(
            "[{:.3}] ORDER[{}] SEQUENTIAL fulfillment complete in {:.3}s\n",
            timestamp(),
            self.order_id,
            duration.as_secs_f64()
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
    async fn generate(self: Arc<Self>) -> Result<ShippingLabel, OrderError> {
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
// Main - Multi-Worker with Signals
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(SqliteExecutionLog::new("complex_dag_signals.db").await?);
    storage.reset().await?;

    let signal_source = Arc::new(SimulatedApprovalSource::new());

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    // Schedule 3 orders with different characteristics
    let orders = vec![
        OrderFulfillment {
            order_id: "ORD-001".to_string(),
            customer_id: "CUST-001".to_string(),
            product_id: "PROD-001".to_string(),
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

    let mut task_ids = Vec::new();
    for order in &orders {
        let task_id = scheduler.schedule(order.clone()).await?;
        task_ids.push(task_id);

        // Simulate manager approving each order after a delay
        // In a real application, this would be external (HTTP webhook, Redis pub/sub, etc.)
        let signal_source_clone = signal_source.clone();
        let order_id = order.order_id.clone();
        tokio::spawn(async move {
            signal_source_clone
                .simulate_approval(
                    &format!("order_approval_{}", order_id),
                    Duration::from_secs(2), // Approve after 2 seconds
                    true,                   // All orders approved
                )
                .await;
        });
    }

    let workers: Vec<_> = (1..=4)
        .map(|i| {
            let storage = storage.clone();
            let signal_source = signal_source.clone();
            let worker_name = match i {
                1 => "validation-worker",
                2 => "payment-worker",
                3 => "warehouse-worker",
                _ => "shipping-worker",
            };

            tokio::spawn(async move {
                let worker = Worker::new(storage, worker_name)
                    .with_poll_interval(Duration::from_millis(100));

                worker
                    .register(|flow: Arc<OrderFulfillment>| flow.fulfill_order())
                    .await;
                worker
                    .register(|flow: Arc<LabelGenerator>| flow.generate())
                    .await;

                // IMPORTANT: Enable signal processing with .with_signals()
                // This allows the worker to deliver signals to suspended flows
                worker.with_signals(signal_source).start().await
            })
        })
        .collect();

    println!("   ✓ 4 workers started and polling for work\n");

    // ============================================================
    // PART 3: Client Status Monitoring (Event-Driven)
    // ============================================================
    // In production, the CLIENT would poll a status API endpoint:
    //   GET /api/tasks/:id -> returns {status: "pending|running|complete|failed"}
    //
    // This demonstrates event-driven waiting using status notifications
    // instead of polling every 500ms.
    // ============================================================

    let status_notify = storage.status_notify().clone();
    let timeout_duration = Duration::from_secs(30);
    tokio::time::timeout(timeout_duration, async {
        loop {
            let mut all_complete = true;
            for &task_id in &task_ids {
                // This simulates: GET /api/tasks/{task_id}
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
            // Wait for status change notification instead of polling
            status_notify.notified().await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await
    .ok();

    // Shutdown all workers
    for handle in workers {
        handle.await?.shutdown().await;
    }

    storage.close().await?;
    Ok(())
}
