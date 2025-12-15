//! Complex Multi-Worker with Signals (SQLite)
//!
//! This example demonstrates the full power of Ergon by combining:
//! 1. **Multiple Workers** (4 workers) processing concurrently
//! 2. **Multiple Parent Flows** (3 orders) executing in parallel
//! 3. **DAG-Based Parallel Steps** with suspension/resumption on signals
//! 4. **External Signals** for human-in-the-loop approval workflows
//! 5. **Child Flow Invocation** from workflow steps
//! 6. **Error Handling** with retryable vs permanent errors
//! 7. **Load Distribution** across workers
//!
//! ## Scenario: E-Commerce Order Fulfillment with Manager Approval
//!
//! Each order goes through a DAG workflow with parallel execution and signal-based approval:
//!
//! ```text
//!                     ┌──> check_fraud ─────────────> await_manager_approval ──┐
//!                     │                                                          │
//! validate_customer ──┼──> reserve_inventory ─────────────────────────────────┤
//!                     │                                                          ├──> process_payment ──┐
//!                     └──> verify_shipping_address ──────────────────────────────┘                      │
//!                                                                                                        │
//!                                                                                                        ├──> generate_label (CHILD FLOW) ──> notify_customer
//!                                                                                                        │
//!                     calculate_tax ─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! **Key behaviors:**
//! - Steps run in PARALLEL when dependencies allow (DAG parallelism)
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
use ergon::core::InvokableFlow;
use ergon::executor::{await_external_signal, ExecutionError, InvokeChild, SignalSource};
use ergon::prelude::*;
use ergon::{Retryable, TaskStatus};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::LazyLock;
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

// Step timestamp tracking for parallelism analysis
static STEP_TIMESTAMPS: LazyLock<DashMap<String, f64>> = LazyLock::new(DashMap::new);

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

impl Retryable for OrderError {
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

        let ts = timestamp();
        STEP_TIMESTAMPS.insert(format!("{}:validate_customer", self.order_id), ts);
        println!(
            "[{:.3}]   [{}] validate_customer START (execution #{})",
            ts, &self.order_id, count
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
            "[{:.3}]   [{}] check_fraud START (execution #{})",
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
    #[step(depends_on = "validate_customer")]
    async fn reserve_inventory(self: Arc<Self>) -> Result<bool, String> {
        let count = OrderAttempts::inc_reserve(&self.order_id);
        RESERVE_INVENTORY_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] reserve_inventory START (execution #{})",
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
            return Err(OrderError::WarehouseSystemDown.to_string());
        }

        // Check stock
        if self.product_id == "PROD-OOS" {
            println!("[{:.3}]      -> Out of stock (permanent)", timestamp());
            return Err(OrderError::OutOfStock {
                product: self.product_id.clone(),
                requested: self.quantity,
            }
            .to_string());
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
    async fn await_manager_approval(self: Arc<Self>) -> Result<ApprovalOutcome, String> {
        let count = OrderAttempts::inc_approval(&self.order_id);
        AWAIT_APPROVAL_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] await_manager_approval START (execution #{})",
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
                .map_err(|e| e.to_string())?;

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

    /// Step 4a: Verify Shipping Address (runs in parallel after validate_customer)
    #[step(depends_on = "validate_customer")]
    async fn verify_shipping_address(self: Arc<Self>) -> Result<String, String> {
        println!(
            "[{:.3}]   [{}] verify_shipping_address START",
            timestamp(),
            &self.order_id
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[{:.3}]      -> Address verified", timestamp());
        Ok(format!("ADDR-{}", self.customer_id))
    }

    /// Step 4b: Calculate Tax (runs in parallel, no dependencies)
    #[step]
    async fn calculate_tax(self: Arc<Self>) -> Result<f64, String> {
        println!(
            "[{:.3}]   [{}] calculate_tax START",
            timestamp(),
            &self.order_id
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let tax = self.amount * 0.08;
        println!("[{:.3}]      -> Tax calculated: ${:.2}", timestamp(), tax);
        Ok(tax)
    }

    /// Step 5: Process Payment (depends on manager approval, inventory, address, tax)
    #[step(
        depends_on = ["await_manager_approval", "reserve_inventory", "verify_shipping_address", "calculate_tax"],
        inputs(approval = "await_manager_approval", tax = "calculate_tax")
    )]
    async fn process_payment(
        self: Arc<Self>,
        approval: ApprovalOutcome,
        tax: f64,
    ) -> Result<bool, String> {
        let count = OrderAttempts::inc_payment(&self.order_id);
        PROCESS_PAYMENT_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] process_payment START (execution #{}, tax=${:.2})",
            timestamp(),
            &self.order_id,
            count,
            tax
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
                return Err(OrderError::ManagerRejected { by, reason }.to_string());
                // Non-retryable
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
            return Err(OrderError::InsufficientFunds.to_string());
        }

        println!("[{:.3}]      -> Payment authorized", timestamp());
        Ok(true)
    }

    /// Step 6: Ready for shipping (marker step after payment complete)
    #[step(depends_on = "process_payment")]
    async fn ready_for_shipping(self: Arc<Self>) -> Result<(), String> {
        println!(
            "[{:.3}]   [{}] ready_for_shipping - payment complete",
            timestamp(),
            &self.order_id
        );
        Ok(())
    }

    /// Step 7: Notify Customer (takes label from flow-level child invocation)
    #[step(depends_on = "ready_for_shipping")]
    async fn notify_customer(
        self: Arc<Self>,
        label: ShippingLabel,
    ) -> Result<ShippingLabel, String> {
        let count = OrderAttempts::inc_notify(&self.order_id);
        NOTIFY_CUSTOMER_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "[{:.3}]   [{}] notify_customer START (execution #{})",
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

    /// Main DAG-PARALLEL flow with signals
    #[flow]
    async fn fulfill_order(self: Arc<Self>) -> Result<OrderSummary, ExecutionError> {
        println!(
            "\n[{:.3}] ORDER[{}] Starting DAG-PARALLEL fulfillment",
            timestamp(),
            self.order_id
        );
        let start = std::time::Instant::now();

        // Execute the DAG - steps run in parallel when dependencies allow!
        dag! {
            self.register_validate_customer();
            self.register_check_fraud();
            self.register_reserve_inventory();
            self.register_verify_shipping_address();
            self.register_calculate_tax();
            self.register_await_manager_approval();
            self.register_process_payment();
            self.register_ready_for_shipping()
        }?;

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
            .await?;

        println!(
            "[{:.3}]      -> Label generated: {}",
            timestamp(),
            label.tracking_number
        );

        // Now notify customer with the label
        let label = self.clone().notify_customer(label).await?;

        let duration = start.elapsed();
        ORDER_TIMINGS.insert(self.order_id.clone(), duration.as_secs_f64());
        println!(
            "[{:.3}] ORDER[{}] DAG fulfillment complete in {:.3}s\n",
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
// Main - Multi-Worker with Signals
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let total_start = std::time::Instant::now();

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ Complex Multi-Worker DAG + Signals (SQLite)               ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");
    println!("Scenario: 3 concurrent orders, 4 workers, DAG-PARALLEL execution with SIGNAL-based approvals\n");

    let storage = Arc::new(SqliteExecutionLog::new("data/complex_dag_signals.db").await?);
    // let storage = Arc::new(ergon::storage::InMemoryExecutionLog::new());
    storage.reset().await?;

    // Create signal source for manager approvals
    let signal_source = Arc::new(SimulatedApprovalSource::new());

    let scheduler = Scheduler::new(storage.clone());

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

    // ============================================================
    // PART 1: API Server / Scheduler Process
    // ============================================================
    // In production, this would be an HTTP endpoint that:
    //   POST /api/orders -> schedules workflow -> returns 202 Accepted with task_id
    //
    // The scheduler does NOT wait for completion. It returns immediately.
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 1: Scheduling Orders (API Server)                    ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let mut task_ids = Vec::new();
    for order in &orders {
        let task_id = scheduler.schedule(order.clone(), Uuid::new_v4()).await?;
        println!("   ✓ {} scheduled (task_id: {})", order.order_id, task_id);
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

    println!("\n   → In production: Return HTTP 202 Accepted");
    println!(
        "   → Response body: {{\"task_ids\": [{:?}, ...]}}",
        task_ids[0]
    );
    println!("   → Client polls GET /api/tasks/:id for status\n");

    // ============================================================
    // PART 2: Worker Service (Separate Process)
    // ============================================================
    // In production, workers run in separate pods/containers/services.
    // They continuously poll the shared storage for work.
    //
    // Workers are completely decoupled from the scheduler.
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 2: Starting Workers (Separate Service)               ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

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
    // PART 3: Client Status Monitoring (Demo Only)
    // ============================================================
    // In production, the CLIENT would poll a status API endpoint:
    //   GET /api/tasks/:id -> returns {status: "pending|running|complete|failed"}
    //
    // This demonstrates that workflows actually execute, but in production
    // the scheduler process would NOT do this polling.
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 3: Monitoring Status (Client Would Poll API)         ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");
    println!("   → Simulating client polling GET /api/tasks/:id...\n");

    let timeout_duration = Duration::from_secs(30);
    let wait_result = tokio::time::timeout(timeout_duration, async {
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
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await;

    match wait_result {
        Ok(_) => println!("\nAll flows completed successfully!\n"),
        Err(_) => {
            println!("\n[WARN] Timeout waiting for flows to complete\n");
            let incomplete = storage.get_incomplete_flows().await?;
            println!("Incomplete flows: {}", incomplete.len());
            for inv in &incomplete {
                println!("  - {} ({})", inv.id(), inv.class_name());
            }
        }
    }

    // Shutdown all workers
    for handle in workers {
        handle.await?.shutdown().await;
    }

    // Print summary
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                       Summary                              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Step Execution Counts:");
    println!(
        "  validate_customer:    {}",
        VALIDATE_CUSTOMER_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  check_fraud:          {}",
        CHECK_FRAUD_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  reserve_inventory:    {}",
        RESERVE_INVENTORY_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  await_manager_approval: {} (SIGNAL steps)",
        AWAIT_APPROVAL_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  process_payment:      {}",
        PROCESS_PAYMENT_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  generate_label:       {} (child flow invocations)",
        GENERATE_LABEL_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  notify_customer:      {}",
        NOTIFY_CUSTOMER_COUNT.load(Ordering::Relaxed)
    );

    println!("\nPer-Order Step Attempts:");
    for i in 1..=3 {
        let order_id = format!("ORD-{:03}", i);
        if let Some(attempts) = ORDER_ATTEMPTS.get(&order_id) {
            println!(
                "  {}: validate={}, fraud={}, inventory={}, approval={}, payment={}, label={}, notify={}",
                order_id,
                attempts.validate_customer.load(Ordering::Relaxed),
                attempts.check_fraud.load(Ordering::Relaxed),
                attempts.reserve_inventory.load(Ordering::Relaxed),
                attempts.await_approval.load(Ordering::Relaxed),
                attempts.process_payment.load(Ordering::Relaxed),
                attempts.generate_label.load(Ordering::Relaxed),
                attempts.notify_customer.load(Ordering::Relaxed)
            );
        }
    }

    let total_duration = total_start.elapsed();
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    Execution Timing                        ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\nPer-Order Fulfillment Time:");
    for i in 1..=3 {
        let order_id = format!("ORD-{:03}", i);
        if let Some(timing) = ORDER_TIMINGS.get(&order_id) {
            println!("  {}: {:.3}s", order_id, *timing);
        }
    }
    println!(
        "\n  Total DAG-PARALLEL execution: {:.3}s\n",
        total_duration.as_secs_f64()
    );

    storage.close().await?;
    Ok(())
}

// ➜  ergon git:(main) ✗ cargo run --example multi_worker_with_signals_dag
//     Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.10s
//      Running `target/debug/examples/multi_worker_with_signals_dag`

// ╔════════════════════════════════════════════════════════════╗
// ║ Complex Multi-Worker DAG + Signals (SQLite)               ║
// ╚════════════════════════════════════════════════════════════╝

// Scenario: 3 concurrent orders, 4 workers, DAG-PARALLEL execution with SIGNAL-based approvals

// ╔════════════════════════════════════════════════════════════╗
// ║ PART 1: Scheduling Orders (API Server)                    ║
// ╚════════════════════════════════════════════════════════════╝

//    ✓ ORD-001 scheduled (task_id: a48f89d6-9488-4750-90db-11cad104f645)
//    ✓ ORD-002 scheduled (task_id: 8c3648d4-87c4-4c46-a354-c8a7c956ff38)
//    ✓ ORD-003 scheduled (task_id: ce4ecf77-e68b-4f46-844a-4f9f06c4084c)

//    → In production: Return HTTP 202 Accepted
//    → Response body: {"task_ids": [a48f89d6-9488-4750-90db-11cad104f645, ...]}
//    → Client polls GET /api/tasks/:id for status

// ╔════════════════════════════════════════════════════════════╗
// ║ PART 2: Starting Workers (Separate Service)               ║
// ╚════════════════════════════════════════════════════════════╝

//    ✓ 4 workers started and polling for work

// ╔════════════════════════════════════════════════════════════╗
// ║ PART 3: Monitoring Status (Client Would Poll API)         ║
// ╚════════════════════════════════════════════════════════════╝

//    → Simulating client polling GET /api/tasks/:id...

// [1765440154.748] ORDER[ORD-002] Starting DAG-PARALLEL fulfillment

// [1765440154.749] ORDER[ORD-001] Starting DAG-PARALLEL fulfillment

// [1765440154.750] ORDER[ORD-003] Starting DAG-PARALLEL fulfillment
// [1765440154.751]   [ORD-002] calculate_tax START
// [1765440154.752]   [ORD-003] validate_customer START (execution #1)
// [1765440154.752]   [ORD-001] validate_customer START (execution #1)
// [1765440154.753]   [ORD-003] calculate_tax START
// [1765440154.754]   [ORD-002] validate_customer START (execution #1)
// [1765440154.757]   [ORD-001] calculate_tax START
// [1765440154.852]      -> Tax calculated: $12.00
// [1765440154.854]      -> Customer validated
// [1765440154.854]      -> Tax calculated: $40.00
// [1765440154.854]      -> Customer validated
// [1765440154.855]      -> Customer validated
// [1765440154.855]   [ORD-003] reserve_inventory START (execution #1)
// [1765440154.856]   [ORD-002] reserve_inventory START (execution #1)
// [1765440154.856]   [ORD-002] verify_shipping_address START
// [1765440154.856]   [ORD-003] check_fraud START (execution #1)
// [1765440154.857]   [ORD-002] check_fraud START (execution #1)
// [1765440154.859]      -> Tax calculated: $24.00
// [1765440154.859]   [ORD-003] verify_shipping_address START
// [1765440154.860]   [ORD-001] check_fraud START (execution #1)
// [1765440154.861]   [ORD-001] verify_shipping_address START
// [1765440154.863]   [ORD-001] reserve_inventory START (execution #1)
// [1765440154.957]      -> Address verified
// [1765440154.959]      -> Address verified
// [1765440154.962]      -> Address verified
// [1765440154.977]      -> Inventory reserved
// [1765440154.977]      -> Inventory reserved
// [1765440154.985]      -> Inventory reserved
// [1765440155.008]      -> No fraud detected
// [1765440155.008]      -> No fraud detected
// [1765440155.011]   [ORD-003] await_manager_approval START (execution #1)
// [1765440155.011]      -> No fraud detected
// [1765440155.012]   [ORD-002] await_manager_approval START (execution #1)
// [1765440155.013]   [ORD-001] await_manager_approval START (execution #1)
// [1765440156.744]   [SIGNAL] Manager decision received for 'order_approval_ORD-001'
// [1765440156.745]   [SIGNAL] Manager decision received for 'order_approval_ORD-002'
// [1765440156.745]   [SIGNAL] Manager decision received for 'order_approval_ORD-003'

// [1765440156.859] ORDER[ORD-003] Starting DAG-PARALLEL fulfillment

// [1765440156.860] ORDER[ORD-001] Starting DAG-PARALLEL fulfillment
// [1765440156.860]   [ORD-003] await_manager_approval START (execution #2)
// [1765440156.861]      -> Manager APPROVED by manager@company.com - High-value order approved!
// [1765440156.861]   [ORD-001] await_manager_approval START (execution #2)
// [1765440156.862]      -> Manager APPROVED by manager@company.com - High-value order approved!
// [1765440156.862]   [ORD-001] process_payment START (execution #1, tax=$24.00)
// [1765440156.863]   [ORD-003] process_payment START (execution #1, tax=$40.00)

// [1765440156.864] ORDER[ORD-002] Starting DAG-PARALLEL fulfillment
// [1765440156.865]   [ORD-002] await_manager_approval START (execution #2)
// [1765440156.865]      -> Manager APPROVED by manager@company.com - High-value order approved!
// [1765440156.866]   [ORD-002] process_payment START (execution #1, tax=$12.00)
// [1765440157.063]      -> Payment authorized
// [1765440157.064]      -> Payment authorized
// [1765440157.066]   [ORD-001] generate_shipping_label START (execution #1)
// [1765440157.066]   [ORD-003] generate_shipping_label START (execution #1)
// [1765440157.067]      -> Payment authorized
// [1765440157.069]     CHILD[97841a96]: Generating label for order ORD-001
// [1765440157.070]     CHILD[e8f1eeea]: Generating label for order ORD-003
// [1765440157.072]   [ORD-002] generate_shipping_label START (execution #1)
// [1765440157.074]     CHILD[3c176d47]: Generating label for order ORD-002
// [1765440157.271]     CHILD[97841a96]: Label complete: TRACK-3AAA032B
// [1765440157.271]     CHILD[e8f1eeea]: Label complete: TRACK-7265930B

// [1765440157.274] ORDER[ORD-001] Starting DAG-PARALLEL fulfillment

// [1765440157.274] ORDER[ORD-003] Starting DAG-PARALLEL fulfillment
// [1765440157.275]     CHILD[3c176d47]: Label complete: TRACK-A3A17CD6
// [1765440157.276]   [ORD-003] generate_shipping_label START (execution #2)
// [1765440157.276]   [ORD-001] generate_shipping_label START (execution #2)
// [1765440157.277]      -> Label generated: TRACK-3AAA032B
// [1765440157.279]   [ORD-001] notify_customer START (execution #1)

// [1765440157.279] ORDER[ORD-002] Starting DAG-PARALLEL fulfillment
// [1765440157.280]      -> Label generated: TRACK-7265930B
// [1765440157.281]   [ORD-002] generate_shipping_label START (execution #2)
// [1765440157.282]   [ORD-003] notify_customer START (execution #1)
// [1765440157.284]      -> Label generated: TRACK-A3A17CD6
// [1765440157.286]   [ORD-002] notify_customer START (execution #1)
// [1765440157.380]      -> Customer notified (tracking: TRACK-3AAA032B)
// [1765440157.382] ORDER[ORD-001] DAG fulfillment complete in 0.108s

// [1765440157.383]      -> Customer notified (tracking: TRACK-7265930B)
// [1765440157.383] ORDER[ORD-003] DAG fulfillment complete in 0.109s

// [1765440157.388]      -> Customer notified (tracking: TRACK-A3A17CD6)
// [1765440157.388] ORDER[ORD-002] DAG fulfillment complete in 0.109s

// All flows completed successfully!

// ╔════════════════════════════════════════════════════════════╗
// ║                       Summary                              ║
// ╚════════════════════════════════════════════════════════════╝

// Step Execution Counts:
//   validate_customer:    3
//   check_fraud:          3
//   reserve_inventory:    3
//   await_manager_approval: 6 (SIGNAL steps)
//   process_payment:      3
//   generate_label:       6 (child flow invocations)
//   notify_customer:      3

// Per-Order Step Attempts:
//   ORD-001: validate=1, fraud=1, inventory=1, approval=2, payment=1, label=2, notify=1
//   ORD-002: validate=1, fraud=1, inventory=1, approval=2, payment=1, label=2, notify=1
//   ORD-003: validate=1, fraud=1, inventory=1, approval=2, payment=1, label=2, notify=1

// ╔════════════════════════════════════════════════════════════╗
// ║                    Execution Timing                        ║
// ╚════════════════════════════════════════════════════════════╝

// Per-Order Fulfillment Time:
//   ORD-001: 0.108s
//   ORD-002: 0.109s
//   ORD-003: 0.109s

//   Total DAG-PARALLEL execution: 3.020s
