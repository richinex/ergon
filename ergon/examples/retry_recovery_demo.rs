//! Retry Policy with Crash Recovery Demo
//!
//! This example demonstrates:
//! - Automatic retry with RetryPolicy for transient errors
//! - Retryable trait to distinguish transient vs permanent errors
//! - Exponential backoff between retry attempts
//! - Step-level resumability with retry policies
//!
//! Flow: validate → charge_payment → reserve_inventory (with retry) → send_confirmation
//! Scenario: reserve_inventory fails with transient error, retries automatically
//! Expected: Inventory reservation retries 3 times, payment runs exactly once
//!
//! Run: cargo run --example retry_recovery_demo

use ergon::core::RetryPolicy;
use ergon::prelude::*;
use ergon::Retryable;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

static PAYMENT_CHARGE_COUNT: AtomicU32 = AtomicU32::new(0);
static INVENTORY_ATTEMPT_COUNT: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Serialize, Deserialize)]
enum InventoryError {
    NetworkTimeout,
    ServiceUnavailable,
    TemporaryOutOfStock,
    ProductNotFound,
    InsufficientStock,
    InvalidWarehouse,
}

impl std::fmt::Display for InventoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InventoryError::NetworkTimeout => write!(f, "Network timeout"),
            InventoryError::ServiceUnavailable => write!(f, "Service unavailable"),
            InventoryError::TemporaryOutOfStock => write!(f, "Temporarily out of stock"),
            InventoryError::ProductNotFound => write!(f, "Product not found"),
            InventoryError::InsufficientStock => write!(f, "Insufficient stock"),
            InventoryError::InvalidWarehouse => write!(f, "Invalid warehouse"),
        }
    }
}

impl std::error::Error for InventoryError {}

impl From<InventoryError> for String {
    fn from(err: InventoryError) -> String {
        err.to_string()
    }
}

impl Retryable for InventoryError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            InventoryError::NetworkTimeout
                | InventoryError::ServiceUnavailable
                | InventoryError::TemporaryOutOfStock
        )
    }
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct OrderProcessor {
    order_id: String,
    amount: f64,
    customer_id: String,
}

impl OrderProcessor {
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, String> {
        println!("[FLOW] Processing order {}", self.order_id);

        // Generate timestamps at flow level for determinism
        let validated_at = chrono::Utc::now().timestamp();
        let charged_at = chrono::Utc::now().timestamp();
        let reserved_at = chrono::Utc::now().timestamp();
        let completed_at = chrono::Utc::now().timestamp();

        let validation = Arc::clone(&self).validate_order(validated_at).await?;
        let payment = Arc::clone(&self)
            .charge_payment(validation, charged_at)
            .await?;
        let inventory = Arc::clone(&self)
            .reserve_inventory(payment, reserved_at)
            .await?;
        let result = Arc::clone(&self)
            .send_confirmation(inventory, completed_at)
            .await?;

        Ok(result)
    }

    #[step]
    async fn validate_order(
        self: Arc<Self>,
        validated_at: i64,
    ) -> Result<ValidationResult, String> {
        println!("  [Step 1/4] Validating order {}", self.order_id);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(ValidationResult {
            order_id: self.order_id.clone(),
            customer_id: self.customer_id.clone(),
            validated_at,
        })
    }

    #[step(inputs(validation = "validate_order"))]
    async fn charge_payment(
        self: Arc<Self>,
        validation: ValidationResult,
        charged_at: i64,
    ) -> Result<PaymentResult, String> {
        println!(
            "  [Step 2/4] Charging payment for order {}",
            validation.order_id
        );

        let count = PAYMENT_CHARGE_COUNT.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "    Charging ${:.2} to customer {} (charge attempt #{})",
            self.amount, self.customer_id, count
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        println!(
            "    Payment successful! Transaction ID: TXN-{}",
            self.order_id
        );

        Ok(PaymentResult {
            transaction_id: format!("TXN-{}", self.order_id),
            amount_charged: self.amount,
            charged_at,
        })
    }

    #[step(inputs(payment = "charge_payment"))]
    async fn reserve_inventory(
        self: Arc<Self>,
        payment: PaymentResult,
        reserved_at: i64,
    ) -> Result<InventoryResult, String> {
        let count = INVENTORY_ATTEMPT_COUNT.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "  [Step 3/4] Reserving inventory for transaction {}",
            payment.transaction_id
        );
        println!(
            "    Payment data loaded from storage: ${:.2} charged at timestamp {}",
            payment.amount_charged, payment.charged_at
        );
        println!("    Inventory reservation attempt {}", count);

        if count < 3 {
            println!("    Service temporarily unavailable (simulated transient error)");
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            return Err("Service temporarily unavailable".to_string());
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
        println!("    Inventory reserved successfully on attempt {}", count);

        Ok(InventoryResult {
            reservation_id: format!("RES-{}", self.order_id),
            reserved_at,
        })
    }

    #[step(inputs(inventory = "reserve_inventory"))]
    async fn send_confirmation(
        self: Arc<Self>,
        inventory: InventoryResult,
        completed_at: i64,
    ) -> Result<OrderResult, String> {
        println!(
            "  [Step 4/4] Sending confirmation email for reservation {}",
            inventory.reservation_id
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(OrderResult {
            order_id: self.order_id.clone(),
            status: "completed".to_string(),
            completed_at,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct ValidationResult {
    order_id: String,
    customer_id: String,
    validated_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct PaymentResult {
    transaction_id: String,
    amount_charged: f64,
    charged_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct InventoryResult {
    reservation_id: String,
    reserved_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct OrderResult {
    order_id: String,
    status: String,
    completed_at: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = "data/test.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);
    // let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = Scheduler::new(storage.clone());

    let order = OrderProcessor {
        order_id: "ORD-67890".to_string(),
        amount: 499.99,
        customer_id: "CUST-002".to_string(),
    };
    let flow_id = Uuid::new_v4();
    scheduler.schedule(order.clone(), flow_id).await?;

    let storage_clone = storage.clone();
    let worker = tokio::spawn(async move {
        let worker = Worker::new(storage_clone.clone(), "worker-auto-retry")
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderProcessor>| flow.process_order())
            .await;
        let handle = worker.start().await;

        tokio::time::sleep(Duration::from_secs(15)).await;

        handle.shutdown().await;
    });

    worker.await?;

    Ok(())
}
