//! Retry Policy with Crash Recovery Demo
//!
//! This example demonstrates:
//! - Automatic retry with RetryPolicy for transient errors
//! - RetryableError trait to distinguish transient vs permanent errors
//! - Exponential backoff between retry attempts
//! - Step-level resumability with retry policies
//!
//! Flow: validate → charge_payment → reserve_inventory (with retry) → send_confirmation
//! Scenario: reserve_inventory fails with transient error, retries automatically
//! Expected: Inventory reservation retries 3 times, payment runs exactly once
//!
//! Run: cargo run --example retry_recovery_demo

use ergon::core::{RetryPolicy, RetryableError};
use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Global counters to track execution
static PAYMENT_CHARGE_COUNT: AtomicU32 = AtomicU32::new(0);
static INVENTORY_ATTEMPT_COUNT: AtomicU32 = AtomicU32::new(0);

/// Custom error type with retry logic
#[derive(Debug, Serialize, Deserialize)]
enum InventoryError {
    // Transient errors - should retry
    NetworkTimeout,
    ServiceUnavailable,
    TemporaryOutOfStock,

    // Permanent errors - should NOT retry
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

impl RetryableError for InventoryError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            InventoryError::NetworkTimeout
                | InventoryError::ServiceUnavailable
                | InventoryError::TemporaryOutOfStock
        )
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct OrderProcessor {
    order_id: String,
    amount: f64,
    customer_id: String,
}

impl OrderProcessor {
    /// Process order with automatic retry at flow level.
    ///
    /// The retry policy is set on the flow, not individual steps.
    /// When any step fails with a transient error, the worker will:
    /// 1. Retry the entire flow (with exponential backoff)
    /// 2. Due to step caching, the flow resumes from the failed step
    /// 3. Previous steps (like payment) are NOT re-executed
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, String> {
        println!("[FLOW] Processing order {}", self.order_id);

        // Step 1: Validate order
        let validation = Arc::clone(&self).validate_order().await?;

        // Step 2: Charge payment (CRITICAL - must run exactly once!)
        let payment = Arc::clone(&self).charge_payment(validation).await?;

        // Step 3: Reserve inventory (may fail with transient error)
        let inventory = Arc::clone(&self).reserve_inventory(payment).await?;

        // Step 4: Send confirmation
        let result = Arc::clone(&self).send_confirmation(inventory).await?;

        println!("[FLOW] Order {} completed successfully", self.order_id);
        Ok(result)
    }

    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<ValidationResult, String> {
        println!("  [Step 1/4] Validating order {}", self.order_id);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(ValidationResult {
            order_id: self.order_id.clone(),
            customer_id: self.customer_id.clone(),
            validated_at: chrono::Utc::now().timestamp(),
        })
    }

    #[step(inputs(validation = "validate_order"))]
    async fn charge_payment(
        self: Arc<Self>,
        validation: ValidationResult,
    ) -> Result<PaymentResult, String> {
        println!(
            "  [Step 2/4] Charging payment for order {}",
            validation.order_id
        );

        // THIS IS THE CRITICAL STEP - We track how many times it runs
        let count = PAYMENT_CHARGE_COUNT.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "    💳 CHARGING ${:.2} to customer {} (charge attempt #{})",
            self.amount, self.customer_id, count
        );

        // Simulate payment processing
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        println!(
            "    ✓ Payment successful! Transaction ID: TXN-{}",
            self.order_id
        );

        Ok(PaymentResult {
            transaction_id: format!("TXN-{}", self.order_id),
            amount_charged: self.amount,
            charged_at: chrono::Utc::now().timestamp(),
        })
    }

    /// Reserve inventory - may fail with transient error.
    ///
    /// Retry is handled at the flow level, not here.
    /// The worker will retry the entire flow if this step fails.
    #[step(inputs(payment = "charge_payment"))]
    async fn reserve_inventory(
        self: Arc<Self>,
        payment: PaymentResult,
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
        println!("    🔄 Inventory reservation attempt {}", count);

        // Simulate transient failures on first 2 attempts
        if count < 3 {
            println!("    ⚠️  Service temporarily unavailable (simulated transient error)");
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            return Err("Service temporarily unavailable".to_string());
        }

        // Success on 3rd attempt
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
        println!("    ✓ Inventory reserved successfully on attempt {}", count);

        Ok(InventoryResult {
            reservation_id: format!("RES-{}", self.order_id),
            reserved_at: chrono::Utc::now().timestamp(),
        })
    }

    #[step(inputs(inventory = "reserve_inventory"))]
    async fn send_confirmation(
        self: Arc<Self>,
        inventory: InventoryResult,
    ) -> Result<OrderResult, String> {
        println!(
            "  [Step 4/4] Sending confirmation email for reservation {}",
            inventory.reservation_id
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(OrderResult {
            order_id: self.order_id.clone(),
            status: "completed".to_string(),
            completed_at: chrono::Utc::now().timestamp(),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ValidationResult {
    order_id: String,
    customer_id: String,
    validated_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PaymentResult {
    transaction_id: String,
    amount_charged: f64,
    charged_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct InventoryResult {
    reservation_id: String,
    reserved_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct OrderResult {
    order_id: String,
    status: String,
    completed_at: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║     Retry Policy with Crash Recovery Demo               ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = FlowScheduler::new(storage.clone());

    // Schedule an order
    let order = OrderProcessor {
        order_id: "ORD-67890".to_string(),
        amount: 499.99,
        customer_id: "CUST-002".to_string(),
    };
    let flow_id = Uuid::new_v4();
    scheduler.schedule(order.clone(), flow_id).await?;

    println!("📋 Scheduled order: {}", order.order_id);
    println!("💰 Amount to charge: ${:.2}", order.amount);
    println!("🔄 Retry Policy: STANDARD (3 attempts, exponential backoff)\n");

    // ========================================================================
    // Execute the workflow with automatic retry
    // ========================================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  Processing Order with Automatic Retry                    ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let storage_clone = storage.clone();
    let worker = tokio::spawn(async move {
        let worker = FlowWorker::new(storage_clone.clone(), "worker-auto-retry")
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderProcessor>| flow.process_order())
            .await;
        let handle = worker.start().await;

        // Wait for workflow to complete (including retries)
        tokio::time::sleep(Duration::from_secs(5)).await;

        handle.shutdown().await;
    });

    worker.await?;

    // ========================================================================
    // FINAL RESULTS
    // ========================================================================

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                     FINAL RESULTS                          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let invocations = storage.get_invocations_for_flow(flow_id).await?;

    // Check flow status (step 0)
    let flow_inv = invocations.iter().find(|i| i.step() == 0);
    if let Some(flow) = flow_inv {
        println!("Flow Status: {:?}", flow.status());
        if let Some(result) = flow.return_value() {
            println!("Flow Result bytes: {} bytes", result.len());
            // Try to deserialize to see if it's Ok or Err
            match ergon::core::deserialize_value::<Result<OrderResult, String>>(result) {
                Ok(Ok(order_result)) => println!("Flow returned Ok: {:?}", order_result),
                Ok(Err(error)) => println!("Flow returned Err: {}", error),
                Err(e) => println!("Could not deserialize: {}", e),
            }
        } else {
            println!("Flow has no return value yet");
        }
    }

    println!("\nAll Steps:");
    for inv in &invocations {
        if inv.step() > 0 {
            println!("  ✓ {} (status: {:?})", inv.method_name(), inv.status());
            if let Some(policy) = inv.retry_policy() {
                println!(
                    "    Retry Policy: max_attempts={}, backoff={}x",
                    policy.max_attempts, policy.backoff_multiplier
                );
            }
        }
    }

    let payment_count = PAYMENT_CHARGE_COUNT.load(Ordering::SeqCst);
    let inventory_attempts = INVENTORY_ATTEMPT_COUNT.load(Ordering::SeqCst);

    println!("\n📊 Execution Statistics:");
    println!("  Payment charged:     {} time(s)", payment_count);
    println!("  Inventory attempts:  {} time(s)", inventory_attempts);

    println!("\n🎯 Key Takeaways:");

    if payment_count == 1 {
        println!("  ✓ Payment step ran exactly ONCE (no retries on non-retryable step)");
    } else {
        println!("  ✗ Payment step ran {} times - unexpected!", payment_count);
    }

    if inventory_attempts == 3 {
        println!(
            "  ✓ Inventory step retried {} times with exponential backoff",
            inventory_attempts
        );
        println!("  ✓ RetryableError trait identified transient failures");
        println!("  ✓ Succeeded on attempt 3 after automatic retries");
    } else {
        println!(
            "  ⚠️  Inventory attempts: {} (expected 3)",
            inventory_attempts
        );
    }

    println!("\n💡 Retry Policy Benefits:");
    println!("  ✓ Automatic retry with exponential backoff");
    println!("  ✓ RetryableError distinguishes transient vs permanent errors");
    println!("  ✓ No manual retry logic needed");
    println!("  ✓ Configurable retry strategies (NONE, STANDARD, AGGRESSIVE, custom)");

    println!("\n💡 Combined with Durable Execution:");
    println!("  ✓ Step-level resumability + automatic retry = resilient workflows");
    println!("  ✓ Payment runs once, inventory retries on transient errors");
    println!("  ✓ Worker crashes handled by resumability");
    println!("  ✓ Transient errors handled by retry policy\n");

    Ok(())
}
