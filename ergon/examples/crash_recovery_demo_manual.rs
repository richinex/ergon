//! Crash recovery and step-level resumability demo
//!
//! This example demonstrates:
//! - Step-level resumability: Worker crashes mid-flow, another worker resumes at next step
//! - Exactly-once execution: Payment runs exactly once despite worker crash
//! - No manual idempotency checks required
//! - Manual re-scheduling to demonstrate crash recovery
//! - Automatic state loading from storage for resumed steps
//!
//! ## Scenario
//! - Order processing flow: validate -> charge_payment -> reserve_inventory -> send_confirmation
//! - Phase 1: Worker-1 starts processing, completes validate and charge_payment
//! - Crash: Worker dies during reserve_inventory step
//! - Phase 2: Flow is manually re-scheduled, Worker-2 picks it up
//! - Recovery: Worker-2 resumes at reserve_inventory (payment NOT re-run)
//! - Result: Payment charged exactly once, no duplicate charges
//!
//! ## Key Takeaways
//! - Framework provides automatic step-level resumability
//! - Completed steps are never re-executed (exactly-once semantics)
//! - Step inputs automatically loaded from storage on resume
//! - No manual idempotency checks or state management needed
//! - For automatic retry with exponential backoff, see: retry_recovery_demo.rs
//!
//! ## Run with
//! ```bash
//! cargo run --example crash_recovery_demo_manual
//! ```

use ergon::core::InvocationStatus;
use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Global counters to track execution
static PAYMENT_CHARGE_COUNT: AtomicU32 = AtomicU32::new(0);
static INVENTORY_RESERVE_COUNT: AtomicU32 = AtomicU32::new(0);
static INVENTORY_SHOULD_FAIL_ONCE: AtomicU32 = AtomicU32::new(1);

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct OrderProcessor {
    order_id: String,
    amount: f64,
    customer_id: String,
}

impl OrderProcessor {
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, String> {
        println!("[FLOW] Processing order {}", self.order_id);

        // Step 1: Validate order
        let validation = self.clone().validate_order().await?;

        // Step 2: Charge payment (CRITICAL - must run exactly once!)
        let payment = self.clone().charge_payment(validation).await?;

        // Step 3: Reserve inventory (worker will "crash" before this in first run)
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
            "    CHARGING ${:.2} to customer {} (charge attempt #{})",
            self.amount, self.customer_id, count
        );

        // Simulate payment processing
        tokio::time::sleep(Duration::from_millis(200)).await;

        println!(
            "    Payment successful! Transaction ID: TXN-{}",
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

        // Simulate a crash/failure on first attempt (e.g., network timeout, service unavailable)
        let should_fail = INVENTORY_SHOULD_FAIL_ONCE.fetch_sub(
            1.min(INVENTORY_SHOULD_FAIL_ONCE.load(Ordering::SeqCst)),
            Ordering::SeqCst,
        );
        if should_fail > 0 {
            println!("    CRASH: Worker died during inventory reservation!");
            tokio::time::sleep(Duration::from_millis(50)).await;
            return Err("Worker crashed during inventory reservation".to_string());
        }

        tokio::time::sleep(Duration::from_millis(150)).await;

        println!("    Inventory reserved successfully");

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
    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = Scheduler::new(storage.clone());

    // Schedule an order
    let order = OrderProcessor {
        order_id: "ORD-12345".to_string(),
        amount: 299.99,
        customer_id: "CUST-001".to_string(),
    };
    let flow_id = Uuid::new_v4();
    scheduler.schedule(order.clone(), flow_id).await?;

    println!("📋 Scheduled order: {}", order.order_id);
    println!("💰 Amount to charge: ${:.2}\n", order.amount);

    // ========================================================================
    // PHASE 1: First worker processes until crash
    // ========================================================================

    let storage_clone = storage.clone();
    let worker1 = tokio::spawn(async move {
        let worker = Worker::new(storage_clone.clone(), "worker-1")
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderProcessor>| flow.process_order())
            .await;
        let handle = worker.start().await;

        // Let worker process for a bit - it will hit the crash at step 3
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Worker will have encountered the error and stopped processing
        println!("Worker-1 stopped after encountering error\n");
        handle.shutdown().await;
    });

    worker1.await?;

    // Check what happened
    tokio::time::sleep(Duration::from_millis(200)).await;

    let invocations = storage.get_invocations_for_flow(flow_id).await?;

    for inv in &invocations {
        if inv.step() > 0 {
            // Skip flow itself (step 0)
            match inv.status() {
                InvocationStatus::Complete => {
                    println!("  {} (completed)", inv.method_name());
                }
                InvocationStatus::Pending => {
                    println!("  {} (pending)", inv.method_name());
                }
                _ => {}
            }
        }
    }

    let payment_count = PAYMENT_CHARGE_COUNT.load(Ordering::SeqCst);
    println!("\nPayment charged: {} time(s)", payment_count);

    if payment_count == 1 {
        println!("   Good! Customer charged exactly once");
    } else {
        println!("   ERROR! Duplicate charge detected!");
    }

    // ========================================================================
    // PHASE 2: Demonstrate manual retry/recovery (in production: automatic)
    // ========================================================================

    // Re-schedule the same order to demonstrate crash recovery and resumability
    let order_retry = OrderProcessor {
        order_id: "ORD-12345".to_string(),
        amount: 299.99,
        customer_id: "CUST-001".to_string(),
    };
    scheduler.schedule(order_retry, flow_id).await?;

    let storage_clone = storage.clone();
    let worker2 = tokio::spawn(async move {
        let worker = Worker::new(storage_clone.clone(), "worker-2")
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderProcessor>| flow.process_order())
            .await;
        let handle = worker.start().await;

        // Wait for completion
        tokio::time::sleep(Duration::from_millis(800)).await;

        handle.shutdown().await;
    });

    worker2.await?;

    // ========================================================================
    // FINAL RESULTS
    // ========================================================================

    let invocations = storage.get_invocations_for_flow(flow_id).await?;
    let flow_invocation = invocations.iter().find(|i| i.step() == 0);

    if let Some(flow) = flow_invocation {
        println!("Flow Status: {:?}", flow.status());
    }

    for inv in &invocations {
        if inv.step() > 0 {
            println!("  {} (completed)", inv.method_name());
        }
    }

    let payment_count = PAYMENT_CHARGE_COUNT.load(Ordering::SeqCst);
    let inventory_count = INVENTORY_RESERVE_COUNT.load(Ordering::SeqCst);

    println!("Payment charged:     {} time(s)", payment_count);
    println!("Inventory reserved:  {} time(s)", inventory_count);

    Ok(())
}
