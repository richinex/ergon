//! Crash Recovery and Step-Level Resumability Demo
//!
//! This example demonstrates:
//! - Worker-level automatic retry with crash recovery
//! - Step-level resumability: Flow resumes from failed step after crash
//! - Payment runs exactly once despite multiple retry attempts
//! - No manual idempotency checks or re-scheduling required
//!
//! Flow: validate → charge_payment → reserve_inventory → send_confirmation
//! Scenario: Worker crashes during reserve_inventory
//! Expected: Worker automatically retries, resumes at reserve_inventory, payment NOT re-run
//!
//! Run: cargo run --example crash_recovery_demo

use ergon::core::RetryPolicy;
use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Global counters to track execution
static PAYMENT_CHARGE_COUNT: AtomicU32 = AtomicU32::new(0);
static INVENTORY_RESERVE_COUNT: AtomicU32 = AtomicU32::new(0);

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct OrderProcessor {
    order_id: String,
    amount: f64,
    customer_id: String,
}

impl OrderProcessor {
    /// Process order with automatic retry on failure.
    ///
    /// The retry policy ensures that if reserve_inventory fails,
    /// the worker will automatically retry the flow with exponential backoff.
    /// Due to step caching, the flow resumes from the failed step - payment
    /// is NOT re-executed, ensuring exactly-once semantics.
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, String> {
        println!("[FLOW] Processing order {}", self.order_id);

        // Step 1: Validate order
        let validation = self.clone().validate_order().await?;

        // Step 2: Charge payment (CRITICAL - must run exactly once!)
        let payment = self.clone().charge_payment(validation).await?;

        // Step 3: Reserve inventory (will fail first 2 times, worker auto-retries)
        let inventory = self.clone().reserve_inventory(payment).await?;

        // Step 4: Send confirmation
        let result = self.clone().send_confirmation(inventory).await?;

        println!("[FLOW] Order {} completed successfully", self.order_id);
        Ok(result)
    }

    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<ValidationResult, String> {
        println!("  [Step 1/4] Validating order {}", self.order_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

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
        tokio::time::sleep(Duration::from_millis(200)).await;

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

    #[step(inputs(payment = "charge_payment"))]
    async fn reserve_inventory(
        self: Arc<Self>,
        payment: PaymentResult,
    ) -> Result<InventoryResult, String> {
        let count = INVENTORY_RESERVE_COUNT.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "  [Step 3/4] Reserving inventory (attempt #{}) for transaction {}",
            count, payment.transaction_id
        );
        println!(
            "    Payment data loaded from storage: ${:.2} charged at timestamp {}",
            payment.amount_charged, payment.charged_at
        );

        // Simulate transient failures on first 2 attempts
        // The worker will automatically retry the flow when this returns an error
        if count < 3 {
            println!("    Service temporarily unavailable (simulating transient error)");
            tokio::time::sleep(Duration::from_millis(50)).await;
            return Err("Service temporarily unavailable".to_string());
        }

        // Success on 3rd attempt
        tokio::time::sleep(Duration::from_millis(150)).await;
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

        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(OrderResult {
            order_id: self.order_id.clone(),
            status: "completed".to_string(),
            completed_at: chrono::Utc::now().timestamp(),
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
    println!("\nCrash Recovery & Step Resumability Demo");
    println!("========================================\n");

    // let storage = Arc::new(InMemoryExecutionLog::new());
    let redis_url = "redis://127.0.0.1:6379";

    // Create Redis storage
    println!("Connecting to Redis at {}...", redis_url);
    let storage = Arc::new(RedisExecutionLog::new(redis_url)?);
    let scheduler = FlowScheduler::new(storage.clone());

    // Schedule an order
    let order = OrderProcessor {
        order_id: "ORD-12345".to_string(),
        amount: 299.99,
        customer_id: "CUST-001".to_string(),
    };
    let flow_id = Uuid::new_v4();
    scheduler.schedule(order.clone(), flow_id).await?;

    println!("📋 Scheduled order: {}", order.order_id);
    println!("💰 Amount to charge: ${:.2}", order.amount);
    println!("🔄 Retry Policy: STANDARD (3 attempts, exponential backoff)\n");

    // ========================================================================
    // Worker with Automatic Retry
    // ========================================================================

    println!("Processing Order with Automatic Retry & Crash Recovery");
    println!("======================================================\n");

    let storage_clone = storage.clone();
    let worker = tokio::spawn(async move {
        let worker = FlowWorker::new(storage_clone.clone(), "worker-auto-retry")
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderProcessor>| flow.process_order())
            .await;
        let handle = worker.start().await;

        // Wait for workflow to complete (including retries)
        tokio::time::sleep(Duration::from_secs(10)).await;

        handle.shutdown().await;
    });

    worker.await?;

    // ========================================================================
    // FINAL RESULTS
    // ========================================================================

    println!("\nFINAL RESULTS");
    println!("=============\n");

    let invocations = storage.get_invocations_for_flow(flow_id).await?;
    let flow_invocation = invocations.iter().find(|i| i.step() == 0);

    if let Some(flow) = flow_invocation {
        println!("Flow Status: {:?}", flow.status());
    }

    println!("\nAll Steps:");
    for inv in &invocations {
        if inv.step() > 0 {
            println!("  ✓ {} (completed)", inv.method_name());
        }
    }

    let payment_count = PAYMENT_CHARGE_COUNT.load(Ordering::SeqCst);
    let inventory_count = INVENTORY_RESERVE_COUNT.load(Ordering::SeqCst);

    println!("\n📊 Execution Statistics:");
    println!("  Payment charged:     {} time(s)", payment_count);
    println!("  Inventory reserved:  {} time(s)", inventory_count);

    println!("\nKey Takeaways:");

    if payment_count == 1 {
        println!(
            "  ✓ Payment step ran exactly ONCE despite {} retry attempts",
            inventory_count
        );
        println!("  ✓ Worker automatically retried flow with exponential backoff");
        println!("  ✓ Flow resumed at 'reserve_inventory' step on each retry");
        println!("  ✓ Payment data automatically loaded from storage");
        println!("  ✓ No duplicate charges - customer billed correctly");
    } else {
        println!(
            "  ✗ Payment step ran {} times - duplicate charge!",
            payment_count
        );
    }

    if inventory_count == 3 {
        println!(
            "  ✓ Inventory step retried {} times (as expected)",
            inventory_count
        );
        println!("  ✓ Succeeded on attempt 3 after automatic retries");
    } else {
        println!("  Inventory attempts: {} (expected 3)", inventory_count);
    }

    println!("\nWith Regular Queues:");
    println!("  ✗ Entire task would retry from step 1");
    println!("  ✗ Customer would be charged {} times", inventory_count);
    println!("  ✗ Need 50+ lines of manual idempotency checks");

    println!("\nWith Ergon:");
    println!("  ✓ Automatic worker-level retry with exponential backoff");
    println!("  ✓ Step-level resumability - flow resumes from failed step");
    println!("  ✓ Zero boilerplate idempotency code");
    println!("  ✓ Exactly-once execution guaranteed for idempotent steps");
    println!("  ✓ Durable retry - survives worker crashes\n");

    Ok(())
}
