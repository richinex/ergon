//! Crash Recovery and Step-Level Resumability Demo
//!
//! This example demonstrates:
//! - Worker-level automatic retry with crash recovery
//! - Step-level resumability: Flow resumes from failed step after crash
//! - Payment runs exactly once despite multiple retry attempts
//! - No manual idempotency checks or re-scheduling required
//! - Exactly-once semantics for critical payment operations
//!
//! ## Scenario
//! - Order processing flow: validate → charge_payment → reserve_inventory → send_confirmation
//! - Worker processes order ORD-12345 for $299.99
//! - Payment step succeeds (charges customer)
//! - Inventory reservation fails twice with "Service temporarily unavailable"
//! - Worker automatically retries flow with exponential backoff (RetryPolicy::STANDARD)
//! - On retry: Flow resumes at reserve_inventory step (payment NOT re-executed)
//! - Inventory succeeds on 3rd attempt, order completes successfully
//!
//! ## Key Takeaways
//! - Payment charged exactly ONCE despite 3 inventory reservation attempts
//! - Flow automatically resumes from failed step (not from beginning)
//! - Payment data loaded from storage on retry (automatic step caching)
//! - Zero manual idempotency code required
//! - Retry policy handles exponential backoff automatically
//! - Contrast with regular queues: Would retry entire flow, charging customer 3 times!
//!
//! ## Prerequisites
//! SQLite database created at /tmp/ergon_benchmark_sqlite.db
//!
//! ## Run with
//! ```bash
//! cargo run --example crash_recovery_demo_sqlite --features=sqlite
//! ```

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

        // Generate timestamps at flow level for determinism
        let validated_at = chrono::Utc::now().timestamp();
        let charged_at = chrono::Utc::now().timestamp();
        let reserved_at = chrono::Utc::now().timestamp();
        let completed_at = chrono::Utc::now().timestamp();

        // Step 1: Validate order
        let validation = self.clone().validate_order(validated_at).await?;

        // Step 2: Charge payment (CRITICAL - must run exactly once!)
        let payment = self.clone().charge_payment(validation, charged_at).await?;

        // Step 3: Reserve inventory (will fail first 2 times, worker auto-retries)
        let inventory = self.clone().reserve_inventory(payment, reserved_at).await?;

        // Step 4: Send confirmation
        let result = self
            .clone()
            .send_confirmation(inventory, completed_at)
            .await?;

        println!("[FLOW] Order {} completed successfully", self.order_id);
        Ok(result)
    }

    #[step]
    async fn validate_order(
        self: Arc<Self>,
        validated_at: i64,
    ) -> Result<ValidationResult, String> {
        println!("  [Step 1/4] Validating order {}", self.order_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

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
            charged_at,
        })
    }

    #[step(inputs(payment = "charge_payment"))]
    async fn reserve_inventory(
        self: Arc<Self>,
        payment: PaymentResult,
        reserved_at: i64,
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

        tokio::time::sleep(Duration::from_millis(100)).await;

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
    let db_path = "/tmp/ergon_benchmark_sqlite.db";
    let _ = std::fs::remove_file(db_path);

    let storage = Arc::new(SqliteExecutionLog::new(db_path).await?);
    let scheduler = Scheduler::new(storage.clone());

    // Schedule an order
    let order = OrderProcessor {
        order_id: "ORD-12345".to_string(),
        amount: 299.99,
        customer_id: "CUST-001".to_string(),
    };
    let flow_id = Uuid::new_v4();
    scheduler.schedule(order.clone(), flow_id).await?;

    println!("Scheduled order: {}", order.order_id);
    println!("Amount to charge: ${:.2}", order.amount);

    // ========================================================================
    // Worker with Automatic Retry
    // ========================================================================

    let storage_clone = storage.clone();
    let worker = tokio::spawn(async move {
        let worker = Worker::new(storage_clone.clone(), "worker-auto-retry")
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
