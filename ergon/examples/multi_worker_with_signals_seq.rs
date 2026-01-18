//! E-commerce order fulfillment with sequential steps and external signals for manager approval.
//!
//! Run with
//!
//! ```not_rust
//! cargo run --example multi_worker_with_signals_seq --features=sqlite
//! ```

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use ergon::core::{FlowType, InvokableFlow};
use ergon::executor::{await_external_signal, InvokeChild, SignalSource, Worker};
use ergon::prelude::*;
use ergon::storage::SqliteExecutionLog;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;

static VALIDATE_CUSTOMER_COUNT: AtomicU32 = AtomicU32::new(0);
static CHECK_FRAUD_COUNT: AtomicU32 = AtomicU32::new(0);
static RESERVE_INVENTORY_COUNT: AtomicU32 = AtomicU32::new(0);
static AWAIT_APPROVAL_COUNT: AtomicU32 = AtomicU32::new(0);
static PROCESS_PAYMENT_COUNT: AtomicU32 = AtomicU32::new(0);
static GENERATE_LABEL_COUNT: AtomicU32 = AtomicU32::new(0);
static NOTIFY_CUSTOMER_COUNT: AtomicU32 = AtomicU32::new(0);

static ORDER_ATTEMPTS: LazyLock<DashMap<String, OrderAttempts>> = LazyLock::new(DashMap::new);
static ORDER_TIMINGS: LazyLock<DashMap<String, f64>> = LazyLock::new(DashMap::new);
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

    fn inc_approval(order_id: &str) -> u32 {
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.await_approval.fetch_add(1, Ordering::Relaxed) + 1;
        }
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .await_approval
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
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
enum OrderError {
    #[error("Network timeout")]
    PaymentNetworkTimeout,
    #[error("Payment gateway unavailable")]
    PaymentGatewayUnavailable,
    #[error("Insufficient funds")]
    InsufficientFunds,
    #[error("Card declined")]
    CardDeclined,
    #[error("Fraud detected")]
    FraudDetected,
    #[error("Database timeout")]
    InventoryDatabaseTimeout,
    #[error("Warehouse system down")]
    WarehouseSystemDown,
    #[error("Out of stock: {product} (requested: {requested})")]
    OutOfStock { product: String, requested: u32 },
    #[error("Invalid product ID")]
    InvalidProductId,
    #[error("Manager rejected by {by} - {reason}")]
    ManagerRejected { by: String, reason: String },
    #[error("Infrastructure error: {0}")]
    Infrastructure(String),
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
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApprovalDecision {
    approved: bool,
    approver: String,
    comments: String,
    timestamp: i64,
}

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

struct SimulatedApprovalSource {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl SimulatedApprovalSource {
    fn new() -> Self {
        Self {
            signals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

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
        println!("{:.3} signal received: {}", timestamp(), signal_name);
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

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderFulfillment {
    order_id: String,
    customer_id: String,
    product_id: String,
    amount: f64,
    quantity: u32,
}

impl OrderFulfillment {
    #[step]
    async fn validate_customer(self: Arc<Self>) -> Result<String, String> {
        let count = OrderAttempts::inc_validate(&self.order_id);
        VALIDATE_CUSTOMER_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "{:.3} {} validate_customer #{}",
            timestamp(),
            &self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        if count == 1 && self.customer_id == "CUST-RETRY" {
            println!("{:.3} transient error, will retry", timestamp());
            return Err("Customer validation timeout".to_string());
        }

        println!("{:.3} customer validated", timestamp());
        Ok(self.customer_id.clone())
    }

    #[step(depends_on = "validate_customer")]
    async fn check_fraud(self: Arc<Self>) -> Result<bool, String> {
        let count = OrderAttempts::inc_fraud(&self.order_id);
        CHECK_FRAUD_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "{:.3} {} check_fraud #{}",
            timestamp(),
            &self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(150)).await;

        if self.customer_id == "CUST-FRAUD" {
            println!("{:.3} fraud detected", timestamp());
            return Err("Fraud detected for customer".to_string());
        }

        println!("{:.3} no fraud detected", timestamp());
        Ok(true)
    }

    #[step]
    async fn reserve_inventory(self: Arc<Self>) -> Result<bool, OrderError> {
        let count = OrderAttempts::inc_reserve(&self.order_id);
        RESERVE_INVENTORY_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "{:.3} {} reserve_inventory #{}",
            timestamp(),
            &self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(120)).await;

        if count == 1 && self.product_id == "PROD-SLOW" {
            println!("{:.3} warehouse system timeout", timestamp());
            return Err(OrderError::WarehouseSystemDown);
        }

        if self.product_id == "PROD-OOS" {
            println!("{:.3} out of stock", timestamp());
            return Err(OrderError::OutOfStock {
                product: self.product_id.clone(),
                requested: self.quantity,
            });
        }

        println!("{:.3} inventory reserved", timestamp());
        Ok(true)
    }

    #[step(depends_on = "check_fraud")]
    async fn await_manager_approval(self: Arc<Self>) -> Result<ApprovalOutcome, OrderError> {
        let count = OrderAttempts::inc_approval(&self.order_id);
        AWAIT_APPROVAL_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "{:.3} {} await_manager_approval #{}",
            timestamp(),
            &self.order_id,
            count
        );

        let decision: ApprovalDecision =
            await_external_signal(&format!("order_approval_{}", self.order_id))
                .await
                .map_err(|e| OrderError::Infrastructure(e.to_string()))?;

        let outcome: ApprovalOutcome = decision.into();

        match &outcome {
            ApprovalOutcome::Approved { by, comment } => {
                println!("{:.3} approved: {} - {}", timestamp(), by, comment);
            }
            ApprovalOutcome::Rejected { by, reason } => {
                println!("{:.3} rejected: {} - {}", timestamp(), by, reason);
            }
        }

        Ok(outcome)
    }

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
            "{:.3} {} process_payment #{}",
            timestamp(),
            &self.order_id,
            count
        );

        match approval {
            ApprovalOutcome::Rejected { by, reason } => {
                println!("{:.3} payment blocked: {} - {}", timestamp(), by, reason);
                return Err(OrderError::ManagerRejected { by, reason });
            }
            ApprovalOutcome::Approved { .. } => {}
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        if self.amount > 10000.0 {
            println!("{:.3} insufficient funds", timestamp());
            return Err(OrderError::InsufficientFunds);
        }

        println!("{:.3} payment authorized", timestamp());
        Ok(true)
    }

    #[step]
    async fn notify_customer(
        self: Arc<Self>,
        label: ShippingLabel,
    ) -> Result<ShippingLabel, OrderError> {
        let count = OrderAttempts::inc_notify(&self.order_id);
        NOTIFY_CUSTOMER_COUNT.fetch_add(1, Ordering::Relaxed);

        println!(
            "{:.3} {} notify_customer #{}",
            timestamp(),
            &self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        println!(
            "{:.3} customer notified: {}",
            timestamp(),
            label.tracking_number
        );
        Ok(label)
    }

    #[flow]
    async fn fulfill_order(self: Arc<Self>) -> Result<OrderSummary, OrderError> {
        println!(
            "\n{:.3} {} starting fulfillment",
            timestamp(),
            self.order_id
        );
        let start = std::time::Instant::now();

        let _customer = self.clone().validate_customer().await?;
        let _fraud = self.clone().check_fraud().await?;
        let _inventory = self.clone().reserve_inventory().await?;

        let approval = self.clone().await_manager_approval().await?;

        let _payment = self.clone().process_payment(approval).await?;

        let count = OrderAttempts::inc_label(&self.order_id);
        GENERATE_LABEL_COUNT.fetch_add(1, Ordering::Relaxed);
        println!(
            "{:.3} {} generate_label #{}",
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
            "{:.3} label generated: {}",
            timestamp(),
            label.tracking_number
        );

        let label = self.clone().notify_customer(label).await?;

        let duration = start.elapsed();
        ORDER_TIMINGS.insert(self.order_id.clone(), duration.as_secs_f64());
        println!(
            "{:.3} {} complete in {:.3}s\n",
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
            "{:.3} generating label {}",
            timestamp(),
            &flow_id.to_string()[..8]
        );

        tokio::time::sleep(Duration::from_millis(200)).await;

        let label = ShippingLabel {
            tracking_number: format!("TRACK-{}", Uuid::new_v4().to_string()[..8].to_uppercase()),
            carrier: "FedEx".to_string(),
            estimated_delivery: "2024-01-15".to_string(),
        };

        println!(
            "{:.3} label complete: {}",
            timestamp(),
            label.tracking_number
        );

        Ok(label)
    }
}

fn timestamp() -> f64 {
    let now = Utc::now();
    now.timestamp() as f64 + (now.timestamp_subsec_millis() as f64 / 1000.0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(SqliteExecutionLog::new("complex_dag_signals.db").await?);
    storage.reset().await?;

    let signal_source = Arc::new(SimulatedApprovalSource::new());

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

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

        let signal_source_clone = signal_source.clone();
        let order_id = order.order_id.clone();
        tokio::spawn(async move {
            signal_source_clone
                .simulate_approval(
                    &format!("order_approval_{}", order_id),
                    Duration::from_secs(2),
                    true,
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

                worker.with_signals(signal_source).start().await
            })
        })
        .collect();

    tokio::time::timeout(Duration::from_secs(30), async {
        storage.wait_for_all(&task_ids).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await
    .ok();

    for handle in workers {
        handle.await?.shutdown().await;
    }

    storage.close().await?;
    Ok(())
}
