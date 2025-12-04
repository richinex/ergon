//! Custom Error Types Example
//!
//! This example demonstrates:
//! - Using custom error types instead of generic String errors
//! - Implementing RetryableError trait for fine-grained retry control
//! - Automatic retry for transient errors only (NetworkTimeout, GatewayUnavailable)
//! - Permanent errors that should not retry (InsufficientFunds, OutOfStock, InvalidEmail)
//! - Type-safe error handling with rich context
//! - Compile-time guarantees for error handling
//!
//! ## Scenario
//! An order processing flow with three main steps (inventory check, payment, notification).
//! Each step can fail with specific error types. Some errors are retryable (network issues),
//! while others are permanent (insufficient funds, out of stock). The framework automatically
//! retries only the retryable errors.
//!
//! ## Key Takeaways
//! - Custom error types provide rich context beyond simple strings
//! - RetryableError trait gives fine-grained control over which errors retry
//! - Transient errors (network timeouts, service unavailable) are retryable
//! - Permanent errors (business logic failures) should not retry
//! - Each error type must implement Display, Error, From<T> for String, and RetryableError
//! - The framework respects is_retryable() and only retries when appropriate
//!
//! ## Run with
//! ```bash
//! cargo run --example custom_error_types --features=sqlite
//! ```

use ergon::core::{InvocationStatus, RetryPolicy};
use ergon::executor::{Scheduler, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Custom Error Types
// =============================================================================

// Counter to track payment retry attempts
static PAYMENT_ATTEMPT_COUNT: AtomicU32 = AtomicU32::new(0);

/// Payment processing errors
#[derive(Debug, Clone, Serialize, Deserialize)]
enum PaymentError {
    InsufficientFunds { required: f64, available: f64 },
    CardExpired { expiry_date: String },
    InvalidCardNumber { last_four: String },
    NetworkTimeout,
    GatewayUnavailable,
}

impl fmt::Display for PaymentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PaymentError::InsufficientFunds {
                required,
                available,
            } => {
                write!(
                    f,
                    "Insufficient funds: need ${:.2}, have ${:.2}",
                    required, available
                )
            }
            PaymentError::CardExpired { expiry_date } => {
                write!(f, "Card expired on {}", expiry_date)
            }
            PaymentError::InvalidCardNumber { last_four } => {
                write!(f, "Invalid card number ending in {}", last_four)
            }
            PaymentError::NetworkTimeout => {
                write!(f, "Network timeout connecting to payment gateway")
            }
            PaymentError::GatewayUnavailable => {
                write!(f, "Payment gateway temporarily unavailable")
            }
        }
    }
}

impl std::error::Error for PaymentError {}

impl From<PaymentError> for String {
    fn from(err: PaymentError) -> Self {
        err.to_string()
    }
}

// Implement RetryableError to control which errors should retry
impl RetryableError for PaymentError {
    fn is_retryable(&self) -> bool {
        match self {
            // Transient errors - should retry
            PaymentError::NetworkTimeout => true,
            PaymentError::GatewayUnavailable => true,

            // Permanent errors - should NOT retry
            PaymentError::InsufficientFunds { .. } => false,
            PaymentError::CardExpired { .. } => false,
            PaymentError::InvalidCardNumber { .. } => false,
        }
    }
}

/// Inventory management errors
#[derive(Debug, Clone, Serialize, Deserialize)]
enum InventoryError {
    ProductNotFound {
        product_id: String,
    },
    OutOfStock {
        product_id: String,
        requested: u32,
        available: u32,
    },
    WarehouseUnavailable {
        warehouse_id: String,
    },
    DatabaseConnectionFailed,
}

impl fmt::Display for InventoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InventoryError::ProductNotFound { product_id } => {
                write!(f, "Product not found: {}", product_id)
            }
            InventoryError::OutOfStock {
                product_id,
                requested,
                available,
            } => {
                write!(
                    f,
                    "Product {} out of stock: requested {}, available {}",
                    product_id, requested, available
                )
            }
            InventoryError::WarehouseUnavailable { warehouse_id } => {
                write!(f, "Warehouse {} is unavailable", warehouse_id)
            }
            InventoryError::DatabaseConnectionFailed => {
                write!(f, "Failed to connect to inventory database")
            }
        }
    }
}

impl std::error::Error for InventoryError {}

impl From<InventoryError> for String {
    fn from(err: InventoryError) -> Self {
        err.to_string()
    }
}

impl RetryableError for InventoryError {
    fn is_retryable(&self) -> bool {
        match self {
            // Transient errors
            InventoryError::WarehouseUnavailable { .. } => true,
            InventoryError::DatabaseConnectionFailed => true,

            // Permanent errors
            InventoryError::ProductNotFound { .. } => false,
            InventoryError::OutOfStock { .. } => false,
        }
    }
}

/// Notification errors
#[derive(Debug, Clone, Serialize, Deserialize)]
enum NotificationError {
    InvalidEmailAddress { email: String },
    SmtpConnectionFailed,
    RateLimitExceeded { retry_after_seconds: u32 },
}

impl fmt::Display for NotificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationError::InvalidEmailAddress { email } => {
                write!(f, "Invalid email address: {}", email)
            }
            NotificationError::SmtpConnectionFailed => {
                write!(f, "Failed to connect to SMTP server")
            }
            NotificationError::RateLimitExceeded {
                retry_after_seconds,
            } => {
                write!(
                    f,
                    "Rate limit exceeded, retry after {} seconds",
                    retry_after_seconds
                )
            }
        }
    }
}

impl std::error::Error for NotificationError {}

impl From<NotificationError> for String {
    fn from(err: NotificationError) -> Self {
        err.to_string()
    }
}

impl RetryableError for NotificationError {
    fn is_retryable(&self) -> bool {
        match self {
            NotificationError::SmtpConnectionFailed => true,
            NotificationError::RateLimitExceeded { .. } => true,
            NotificationError::InvalidEmailAddress { .. } => false,
        }
    }
}

// =============================================================================
// Order Flow with Custom Errors
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderFlow {
    order_id: String,
    product_id: String,
    quantity: u32,
    amount: f64,
    customer_email: String,
    simulate_error: Option<String>, // For demonstration
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderResult {
    order_id: String,
    status: String,
}

impl OrderFlow {
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, String> {
        println!("\n[Order {}] Starting order processing", self.order_id);

        // Step 1: Check inventory (returns InventoryError)
        self.clone().check_inventory().await?;

        // Step 2: Process payment (returns PaymentError)
        self.clone().process_payment().await?;

        // Step 3: Send confirmation (returns NotificationError)
        let order_id = self.order_id.clone();
        self.send_notification().await?;

        println!("[Order {}] Completed successfully!", order_id);

        Ok(OrderResult {
            order_id,
            status: "Completed".to_string(),
        })
    }

    #[step]
    async fn check_inventory(self: Arc<Self>) -> Result<(), InventoryError> {
        println!(
            "[Order {}] Checking inventory for product {}",
            self.order_id, self.product_id
        );

        // Simulate various inventory errors
        if let Some(ref error_type) = self.simulate_error {
            match error_type.as_str() {
                "product_not_found" => {
                    return Err(InventoryError::ProductNotFound {
                        product_id: self.product_id.clone(),
                    });
                }
                "out_of_stock" => {
                    return Err(InventoryError::OutOfStock {
                        product_id: self.product_id.clone(),
                        requested: self.quantity,
                        available: self.quantity - 1,
                    });
                }
                "warehouse_unavailable" => {
                    return Err(InventoryError::WarehouseUnavailable {
                        warehouse_id: "WH-001".to_string(),
                    });
                }
                "db_connection_failed" => {
                    return Err(InventoryError::DatabaseConnectionFailed);
                }
                _ => {}
            }
        }

        println!("[Order {}] Inventory check passed", self.order_id);
        Ok(())
    }

    #[step]
    async fn process_payment(self: Arc<Self>) -> Result<String, PaymentError> {
        let attempt = PAYMENT_ATTEMPT_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        println!(
            "[Order {}] Processing payment of ${} (attempt #{})",
            self.order_id, self.amount, attempt
        );

        // Simulate various payment errors
        if let Some(ref error_type) = self.simulate_error {
            match error_type.as_str() {
                "insufficient_funds" => {
                    return Err(PaymentError::InsufficientFunds {
                        required: self.amount,
                        available: self.amount - 10.0,
                    });
                }
                "card_expired" => {
                    return Err(PaymentError::CardExpired {
                        expiry_date: "12/2023".to_string(),
                    });
                }
                "invalid_card" => {
                    return Err(PaymentError::InvalidCardNumber {
                        last_four: "1234".to_string(),
                    });
                }
                "network_timeout" => {
                    // Fail first 2 attempts, succeed on 3rd to demonstrate retry
                    if attempt <= 2 {
                        println!(
                            "[Order {}] Network timeout (retryable) - will retry",
                            self.order_id
                        );
                        return Err(PaymentError::NetworkTimeout);
                    }
                    println!(
                        "[Order {}] Network recovered on attempt #{}",
                        self.order_id, attempt
                    );
                }
                "gateway_unavailable" => {
                    return Err(PaymentError::GatewayUnavailable);
                }
                _ => {}
            }
        }

        let transaction_id = format!("TXN-{}", uuid::Uuid::new_v4());
        println!(
            "[Order {}] Payment processed: {}",
            self.order_id, transaction_id
        );
        Ok(transaction_id)
    }

    #[step]
    async fn send_notification(self: Arc<Self>) -> Result<(), NotificationError> {
        println!(
            "[Order {}] Sending confirmation email to {}",
            self.order_id, self.customer_email
        );

        // Simulate notification errors
        if let Some(ref error_type) = self.simulate_error {
            match error_type.as_str() {
                "invalid_email" => {
                    return Err(NotificationError::InvalidEmailAddress {
                        email: self.customer_email.clone(),
                    });
                }
                "smtp_failed" => {
                    return Err(NotificationError::SmtpConnectionFailed);
                }
                "rate_limit" => {
                    return Err(NotificationError::RateLimitExceeded {
                        retry_after_seconds: 60,
                    });
                }
                _ => {}
            }
        }

        println!("[Order {}] Confirmation email sent", self.order_id);
        Ok(())
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Custom Error Types Example ===\n");

    let storage = Arc::new(SqliteExecutionLog::new("custom_errors.db").await?);
    storage.reset().await?;

    // ==========================================================================
    // Test 1: Successful Order
    // ==========================================================================
    println!("\n--- Test 1: Successful Order ---");

    let order1 = OrderFlow {
        order_id: "ORD-001".to_string(),
        product_id: "PROD-123".to_string(),
        quantity: 2,
        amount: 99.99,
        customer_email: "customer@example.com".to_string(),
        simulate_error: None,
    };

    let flow_id1 = uuid::Uuid::new_v4();
    let instance1 = Executor::new(flow_id1, order1, storage.clone());

    match instance1
        .execute(|f| Box::pin(Arc::new(f.clone()).process_order()))
        .await
    {
        Ok(result) => println!("\nResult: {} - {}", result.order_id, result.status),
        Err(e) => println!("\nOrder failed: {}", e),
    }

    // ==========================================================================
    // Test 2: Inventory Error (Permanent)
    // ==========================================================================
    println!("\n\n--- Test 2: Out of Stock Error (Permanent) ---");

    let order2 = OrderFlow {
        order_id: "ORD-002".to_string(),
        product_id: "PROD-456".to_string(),
        quantity: 5,
        amount: 149.99,
        customer_email: "customer@example.com".to_string(),
        simulate_error: Some("out_of_stock".to_string()),
    };

    let flow_id2 = uuid::Uuid::new_v4();
    let instance2 = Executor::new(flow_id2, order2, storage.clone());

    match instance2
        .execute(|f| Box::pin(Arc::new(f.clone()).process_order()))
        .await
    {
        Ok(result) => println!("\nResult: {} - {}", result.order_id, result.status),
        Err(e) => println!("\nOrder failed: {}", e),
    }

    // ==========================================================================
    // Test 3: Payment Error (Permanent)
    // ==========================================================================
    println!("\n\n--- Test 3: Insufficient Funds Error (Permanent) ---");

    let order3 = OrderFlow {
        order_id: "ORD-003".to_string(),
        product_id: "PROD-789".to_string(),
        quantity: 1,
        amount: 999.99,
        customer_email: "customer@example.com".to_string(),
        simulate_error: Some("insufficient_funds".to_string()),
    };

    let flow_id3 = uuid::Uuid::new_v4();
    let instance3 = Executor::new(flow_id3, order3, storage.clone());

    match instance3
        .execute(|f| Box::pin(Arc::new(f.clone()).process_order()))
        .await
    {
        Ok(result) => println!("\nResult: {} - {}", result.order_id, result.status),
        Err(e) => println!("\nOrder failed: {}", e),
    }

    // ==========================================================================
    // Test 4: Network Error (Retryable with Automatic Retry)
    // ==========================================================================
    println!("\n\n--- Test 4: Network Timeout (Retryable with Automatic Retry) ---");
    println!("Note: Will fail first 2 attempts, then succeed on 3rd retry\n");

    // Reset counter for this test
    PAYMENT_ATTEMPT_COUNT.store(0, Ordering::SeqCst);

    let order4 = OrderFlow {
        order_id: "ORD-004".to_string(),
        product_id: "PROD-999".to_string(),
        quantity: 1,
        amount: 49.99,
        customer_email: "customer@example.com".to_string(),
        simulate_error: Some("network_timeout".to_string()),
    };

    // Use scheduler and worker for automatic retry
    let scheduler = Scheduler::new(storage.clone());
    let flow_id4 = uuid::Uuid::new_v4();
    scheduler.schedule(order4, flow_id4).await?;

    // Start worker to process the flow with retries
    let storage_clone = storage.clone();
    let worker_task = tokio::spawn(async move {
        let worker = Worker::new(storage_clone.clone(), "retry-worker")
            .with_poll_interval(Duration::from_millis(100));

        worker
            .register(|flow: Arc<OrderFlow>| flow.process_order())
            .await;

        let handle = worker.start().await;

        // Wait for flow to complete including retries
        tokio::time::sleep(Duration::from_secs(5)).await;

        handle.shutdown().await;
    });

    worker_task.await?;

    // Check result
    let invocations = storage.get_invocations_for_flow(flow_id4).await?;
    let flow_invocation = invocations.iter().find(|i| i.step() == 0);

    if let Some(flow) = flow_invocation {
        match flow.status() {
            InvocationStatus::Complete => {
                println!(
                    "\nResult: ORD-004 - Completed after {} payment attempts!",
                    PAYMENT_ATTEMPT_COUNT.load(Ordering::SeqCst)
                );
                println!("Automatic retry successfully recovered from transient network errors");
            }
            _ => {
                println!("\nOrder status: {:?}", flow.status());
            }
        }
    }

    storage.close().await?;
    Ok(())
}
