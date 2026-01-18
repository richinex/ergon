//! Custom error types with BoxError type erasure and retry control.
//!
//! Run with:
//! ```not_rust
//! cargo run --example custom_error_boxed --features=sqlite
//! ```

use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use thiserror::Error;

static PAYMENT_ATTEMPTS: AtomicU32 = AtomicU32::new(0);
static SHIPPING_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Clone, Serialize, Deserialize, Error)]
enum PaymentError {
    #[error("Network timeout connecting to payment gateway")]
    NetworkTimeout,

    #[error("Payment gateway temporarily unavailable")]
    GatewayDown,

    #[error("Insufficient funds: available ${available:.2}, required ${required:.2}")]
    InsufficientFunds { available: f64, required: f64 },

    #[error("Invalid card number: {card_number}")]
    InvalidCard { card_number: String },

    #[error("Card expired on {expiry_date}")]
    CardExpired { expiry_date: String },
}

impl ergon::Retryable for PaymentError {
    fn is_retryable(&self) -> bool {
        match self {
            PaymentError::NetworkTimeout => true,
            PaymentError::GatewayDown => true,
            PaymentError::InsufficientFunds { .. } => false,
            PaymentError::InvalidCard { .. } => false,
            PaymentError::CardExpired { .. } => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Error)]
enum ShippingError {
    #[error("Warehouse API timeout")]
    WarehouseTimeout,

    #[error("Item out of stock: {item_id}")]
    OutOfStock { item_id: String },

    #[error("Invalid shipping address: {reason}")]
    InvalidAddress { reason: String },
}

impl ergon::Retryable for ShippingError {
    fn is_retryable(&self) -> bool {
        match self {
            ShippingError::WarehouseTimeout => true,
            ShippingError::OutOfStock { .. } => false,
            ShippingError::InvalidAddress { .. } => false,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct PaymentFlow {
    order_id: String,
    amount: f64,
    simulate_error: String,
}

impl PaymentFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, PaymentError> {
        let attempt = PAYMENT_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;

        match self.simulate_error.as_str() {
            "network_timeout" => {
                if attempt < 3 {
                    return Err(PaymentError::NetworkTimeout);
                }
            }
            "gateway_down" => {
                return Err(PaymentError::GatewayDown);
            }
            "insufficient_funds" => {
                return Err(PaymentError::InsufficientFunds {
                    available: 50.00,
                    required: self.amount,
                });
            }
            "invalid_card" => {
                return Err(PaymentError::InvalidCard {
                    card_number: "**** **** **** 1234".to_string(),
                });
            }
            _ => {}
        }

        Ok(format!("PAY-{}-{}", self.order_id, attempt))
    }
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct ShippingFlow {
    order_id: String,
    item_id: String,
    simulate_error: String,
}

impl ShippingFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ShippingError> {
        let attempt = SHIPPING_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;

        match self.simulate_error.as_str() {
            "warehouse_timeout" => {
                if attempt < 2 {
                    return Err(ShippingError::WarehouseTimeout);
                }
            }
            "out_of_stock" => {
                return Err(ShippingError::OutOfStock {
                    item_id: self.item_id.clone(),
                });
            }
            "invalid_address" => {
                return Err(ShippingError::InvalidAddress {
                    reason: "Missing postal code".to_string(),
                });
            }
            _ => {}
        }

        Ok(format!("TRACK-{}-{}", self.order_id, attempt))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(SqliteExecutionLog::new("data/custom_error_boxed.db").await?);
    storage.reset().await?;

    let worker =
        Worker::new(storage.clone(), "worker-1").with_poll_interval(Duration::from_millis(100));

    worker.register(|f: Arc<PaymentFlow>| f.process()).await;
    worker.register(|f: Arc<ShippingFlow>| f.process()).await;

    let worker_handle = worker.start().await;

    let scheduler = Scheduler::new(storage.clone()).unversioned();
    let mut task_ids = Vec::new();

    PAYMENT_ATTEMPTS.store(0, Ordering::SeqCst);
    let task_id = scheduler
        .schedule(PaymentFlow {
            order_id: "ORD-001".to_string(),
            amount: 99.99,
            simulate_error: "network_timeout".to_string(),
        })
        .await?;
    task_ids.push(("Network timeout (retryable)", task_id));

    PAYMENT_ATTEMPTS.store(0, Ordering::SeqCst);
    let task_id = scheduler
        .schedule(PaymentFlow {
            order_id: "ORD-002".to_string(),
            amount: 150.00,
            simulate_error: "insufficient_funds".to_string(),
        })
        .await?;
    task_ids.push(("Insufficient funds (permanent)", task_id));

    SHIPPING_ATTEMPTS.store(0, Ordering::SeqCst);
    let task_id = scheduler
        .schedule(ShippingFlow {
            order_id: "ORD-003".to_string(),
            item_id: "ITEM-123".to_string(),
            simulate_error: "warehouse_timeout".to_string(),
        })
        .await?;
    task_ids.push(("Warehouse timeout (retryable)", task_id));

    SHIPPING_ATTEMPTS.store(0, Ordering::SeqCst);
    let task_id = scheduler
        .schedule(ShippingFlow {
            order_id: "ORD-004".to_string(),
            item_id: "ITEM-456".to_string(),
            simulate_error: "out_of_stock".to_string(),
        })
        .await?;
    task_ids.push(("Out of stock (permanent)", task_id));

    // Race-condition-free waiting for all tasks
    let task_id_list: Vec<Uuid> = task_ids.iter().map(|(_, id)| *id).collect();
    tokio::time::timeout(Duration::from_secs(15), async {
        storage.wait_for_all(&task_id_list).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await??;

    for (description, task_id) in &task_ids {
        if let Some(task) = storage.get_scheduled_flow(*task_id).await? {
            println!(
                "{}: {:?} (retries: {})",
                task_id, task.status, task.retry_count
            );
            if let Some(error_msg) = &task.error_message {
                println!("  {}: {}", description, error_msg);
            }
        }
    }

    worker_handle.shutdown().await;
    storage.close().await?;

    Ok(())
}
