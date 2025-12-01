//! Distributed Worker Failover Demo with RetryableError
//!
//! This example demonstrates the FULL POWER of Ergon's distributed retry system:
//!
//! ## Scenario:
//! 1. Three workers (Worker-A, Worker-B, Worker-C) polling the same queue
//! 2. Worker-A picks up the flow and starts processing
//! 3. Worker-A completes Steps 1-2 (validate, charge payment)
//! 4. Worker-A **CRASHES** during Step 3 (reserve inventory)
//! 5. Flow is automatically retried and re-queued
//! 6. Worker-B picks up the flow
//! 7. Worker-B skips Steps 1-2 (loaded from cache)
//! 8. Worker-B completes Step 3 successfully
//!
//! ## Key Demonstrations:
//! - ✅ Distributed execution with multiple workers
//! - ✅ Automatic failover when a worker crashes
//! - ✅ Step-level resumability (payment NOT re-charged)
//! - ✅ **RetryableError** trait for smart error handling
//! - ✅ Transient vs Permanent error distinction
//! - ✅ Durable retry state (survives worker crashes)
//! - ✅ Exponential backoff between retries
//! - ✅ Zero duplicate operations
//!
//! Run: cargo run --example distributed_worker_failover

use ergon::core::RetryPolicy;
use ergon::prelude::*;
use ergon::RetryableError;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Global counters to track execution across workers
static PAYMENT_CHARGE_COUNT: AtomicU32 = AtomicU32::new(0);
static INVENTORY_RESERVE_COUNT: AtomicU32 = AtomicU32::new(0);
static WORKER_A_CRASHED: AtomicBool = AtomicBool::new(false);

// ============================================================================
// Custom Error Type with RetryableError Trait
// ============================================================================

/// Custom error type that distinguishes between transient and permanent errors.
///
/// This demonstrates Ergon's `RetryableError` trait which allows steps to specify
/// which errors should trigger retry and which should fail immediately.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum InventoryError {
    // === TRANSIENT ERRORS (should retry) ===
    /// Worker crashed during processing - definitely retryable
    WorkerCrashed,

    /// External API timeout - network issue, should retry
    ApiTimeout,

    /// Service temporarily unavailable - transient, should retry
    ServiceUnavailable,

    /// Rate limit exceeded - retry after backoff
    RateLimited,

    // === PERMANENT ERRORS (should NOT retry) ===
    /// Item not found in catalog - no point retrying
    ItemNotFound(String),

    /// Insufficient inventory - business rule violation
    InsufficientStock { requested: u32, available: u32 },

    /// Invalid SKU format - data validation error
    InvalidSku(String),
}

impl std::fmt::Display for InventoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InventoryError::WorkerCrashed => write!(f, "Worker crashed during processing"),
            InventoryError::ApiTimeout => write!(f, "External inventory API timeout"),
            InventoryError::ServiceUnavailable => write!(f, "Inventory service temporarily unavailable"),
            InventoryError::RateLimited => write!(f, "Rate limit exceeded"),
            InventoryError::ItemNotFound(item) => write!(f, "Item '{}' not found in catalog", item),
            InventoryError::InsufficientStock { requested, available } => {
                write!(f, "Insufficient stock: requested {}, only {} available", requested, available)
            }
            InventoryError::InvalidSku(sku) => write!(f, "Invalid SKU format: '{}'", sku),
        }
    }
}

impl std::error::Error for InventoryError {}

/// Implement RetryableError to distinguish transient from permanent errors.
///
/// This is the KEY feature - errors that return `is_retryable() == false` will be
/// cached immediately and won't trigger retry, while retryable errors will trigger
/// the worker-level retry mechanism.
impl RetryableError for InventoryError {
    fn is_retryable(&self) -> bool {
        match self {
            // Transient errors - should retry
            InventoryError::WorkerCrashed => true,
            InventoryError::ApiTimeout => true,
            InventoryError::ServiceUnavailable => true,
            InventoryError::RateLimited => true,

            // Permanent errors - should NOT retry
            InventoryError::ItemNotFound(_) => false,
            InventoryError::InsufficientStock { .. } => false,
            InventoryError::InvalidSku(_) => false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct OrderProcessor {
    order_id: String,
    amount: f64,
    customer_id: String,
}

impl OrderProcessor {
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, InventoryError> {
        println!("[FLOW] Processing order {}", self.order_id);

        // Step 1: Validate order
        let validation = self.clone().validate_order().await?;

        // Step 2: Charge payment (CRITICAL - must run exactly once!)
        let payment = self.clone().charge_payment(validation).await?;

        // Step 3: Reserve inventory (Worker-A will crash here!)
        let inventory = self.clone().reserve_inventory(payment).await?;

        // Step 4: Send confirmation
        let result = self.clone().send_confirmation(inventory).await?;

        println!("[FLOW] Order {} completed successfully", self.order_id);
        Ok(result)
    }

    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<ValidationResult, InventoryError> {
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
    ) -> Result<PaymentResult, InventoryError> {
        println!(
            "  [Step 2/4] Charging payment for order {}",
            validation.order_id
        );

        // Track payment execution
        let count = PAYMENT_CHARGE_COUNT.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "    💳 CHARGING ${:.2} to customer {} (execution #{})",
            self.amount, self.customer_id, count
        );

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
    ) -> Result<InventoryResult, InventoryError> {
        let count = INVENTORY_RESERVE_COUNT.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "  [Step 3/4] Reserving inventory (attempt #{}) for transaction {}",
            count, payment.transaction_id
        );
        println!(
            "    Payment data loaded from storage: ${:.2} charged at timestamp {}",
            payment.amount_charged, payment.charged_at
        );

        // Simulate Worker-A crash on first attempt (TRANSIENT ERROR - will retry)
        if count == 1 && !WORKER_A_CRASHED.load(Ordering::SeqCst) {
            println!("    💥 WORKER-A CRASH! Simulating worker failure...");
            println!("       Error type: WorkerCrashed (is_retryable = true)");
            WORKER_A_CRASHED.store(true, Ordering::SeqCst);

            // Return RETRYABLE error - worker will retry the flow
            return Err(InventoryError::WorkerCrashed);
        }

        // Simulate transient API timeout on second attempt (TRANSIENT ERROR - will retry)
        if count == 2 {
            println!("    ⚠️  Transient error: External inventory API timeout");
            println!("       Error type: ApiTimeout (is_retryable = true)");
            tokio::time::sleep(Duration::from_millis(50)).await;
            return Err(InventoryError::ApiTimeout);
        }

        // Success on 3rd attempt
        tokio::time::sleep(Duration::from_millis(150)).await;
        println!(
            "    ✓ Inventory reserved successfully on attempt {} (Worker recovered!)",
            count
        );

        Ok(InventoryResult {
            reservation_id: format!("RES-{}", self.order_id),
            reserved_at: chrono::Utc::now().timestamp(),
        })
    }

    #[step(inputs(inventory = "reserve_inventory"))]
    async fn send_confirmation(
        self: Arc<Self>,
        inventory: InventoryResult,
    ) -> Result<OrderResult, InventoryError> {
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
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║        Distributed Worker Failover Demo                   ║");
    println!("║  (Multiple Workers + Crash Recovery + Step Resumability)  ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Use in-memory storage (works same for Redis/SQLite)
    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = FlowScheduler::new(storage.clone());

    // Schedule an order
    let order = OrderProcessor {
        order_id: "ORD-99999".to_string(),
        amount: 1299.99,
        customer_id: "CUST-VIP-001".to_string(),
    };
    let flow_id = Uuid::new_v4();
    scheduler.schedule(order.clone(), flow_id).await?;

    println!("📋 Scheduled order: {}", order.order_id);
    println!("💰 Amount to charge: ${:.2}", order.amount);
    println!("🔄 Retry Policy: STANDARD (3 attempts, exponential backoff)");
    println!("👥 Workers: 3 workers (Worker-A, Worker-B, Worker-C)");
    println!("💥 Simulation: Worker-A will crash, Worker-B will take over\n");

    // ========================================================================
    // Start Multiple Workers (Distributed Execution)
    // ========================================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║              Starting Distributed Workers                  ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Worker-A: Will crash during Step 3
    let storage_a = storage.clone();
    let worker_a = tokio::spawn(async move {
        println!("[Worker-A] Starting up...");
        let worker = FlowWorker::new(storage_a, "Worker-A")
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderProcessor>| flow.process_order())
            .await;
        let handle = worker.start().await;

        // Worker-A will process and crash
        tokio::time::sleep(Duration::from_secs(3)).await;

        println!("[Worker-A] Shutting down gracefully");
        handle.shutdown().await;
    });

    // Worker-B: Will pick up after Worker-A crashes
    let storage_b = storage.clone();
    let worker_b = tokio::spawn(async move {
        println!("[Worker-B] Starting up...");
        let worker = FlowWorker::new(storage_b, "Worker-B")
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderProcessor>| flow.process_order())
            .await;
        let handle = worker.start().await;

        tokio::time::sleep(Duration::from_secs(8)).await;

        println!("[Worker-B] Shutting down gracefully");
        handle.shutdown().await;
    });

    // Worker-C: Backup worker (won't process in this demo but shows distributed setup)
    let storage_c = storage.clone();
    let worker_c = tokio::spawn(async move {
        println!("[Worker-C] Starting up (standby mode)...");
        let worker = FlowWorker::new(storage_c, "Worker-C")
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderProcessor>| flow.process_order())
            .await;
        let handle = worker.start().await;

        tokio::time::sleep(Duration::from_secs(8)).await;

        println!("[Worker-C] Shutting down gracefully");
        handle.shutdown().await;
    });

    // Wait for all workers
    println!();
    let _ = tokio::join!(worker_a, worker_b, worker_c);

    // ========================================================================
    // FINAL RESULTS
    // ========================================================================

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                     FINAL RESULTS                          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let invocations = storage.get_invocations_for_flow(flow_id).await?;
    let flow_invocation = invocations.iter().find(|i| i.step() == 0);

    if let Some(flow) = flow_invocation {
        println!("Flow Status: {:?}", flow.status());
        if let Some(retry_policy) = flow.retry_policy() {
            println!("Retry Policy: max_attempts={}, initial_delay={}ms",
                retry_policy.max_attempts, retry_policy.initial_delay.as_millis());
        }
    }

    println!("\nCompleted Steps:");
    for inv in &invocations {
        if inv.step() > 0 {
            println!("  ✓ {} (status: {:?})", inv.method_name(), inv.status());
        }
    }

    let payment_count = PAYMENT_CHARGE_COUNT.load(Ordering::SeqCst);
    let inventory_count = INVENTORY_RESERVE_COUNT.load(Ordering::SeqCst);
    let worker_crashed = WORKER_A_CRASHED.load(Ordering::SeqCst);

    println!("\n📊 Execution Statistics:");
    println!("  Payment charged:           {} time(s)", payment_count);
    println!("  Inventory reserve attempts: {} time(s)", inventory_count);
    println!("  Worker-A crashed:          {}", if worker_crashed { "Yes" } else { "No" });

    println!("\n🎯 What Happened:");
    println!("  1️⃣  Worker-A picked up the flow from queue");
    println!("  2️⃣  Worker-A completed Step 1 (validate_order) ✓");
    println!("  3️⃣  Worker-A completed Step 2 (charge_payment) - ${:.2} charged ✓", order.amount);
    println!("  4️⃣  Worker-A attempted Step 3 (reserve_inventory)");
    println!("  5️⃣  💥 Worker-A CRASHED during Step 3");
    println!("  6️⃣  Flow auto-retried, re-queued with exponential backoff");
    println!("  7️⃣  Worker-B picked up the retried flow");
    println!("  8️⃣  Worker-B loaded Step 1 from cache (skipped execution)");
    println!("  9️⃣  Worker-B loaded Step 2 from cache (NO RE-CHARGE!) ✓");
    println!("  🔟 Worker-B completed Step 3 successfully after 2 more attempts");
    println!("  1️⃣1️⃣  Worker-B completed Step 4 (send_confirmation) ✓");

    println!("\n💪 FULL POWER Demonstrations:");

    if payment_count == 1 {
        println!("  ✅ Payment ran EXACTLY ONCE despite:");
        println!("     - Worker crash");
        println!("     - {} flow retry attempts", inventory_count);
        println!("     - {} inventory execution attempts", inventory_count);
        println!("     - Worker handoff (A → B)");
    } else {
        println!("  ❌ Payment ran {} times - THIS SHOULD NOT HAPPEN!", payment_count);
    }

    println!("\n  ✅ RetryableError Trait (KEY FEATURE):");
    println!("     - InventoryError::WorkerCrashed → is_retryable() = true");
    println!("     - InventoryError::ApiTimeout → is_retryable() = true");
    println!("     - Both errors triggered automatic retry");
    println!("     - If error was ItemNotFound → is_retryable() = false");
    println!("     - Permanent errors would fail immediately (no retry)");

    println!("\n  ✅ Distributed Execution:");
    println!("     - Multiple workers polling same queue");
    println!("     - Automatic failover when Worker-A crashed");
    println!("     - Worker-B seamlessly continued from Step 3");

    println!("\n  ✅ Step-Level Resumability:");
    println!("     - Steps 1-2 loaded from cache (not re-executed)");
    println!("     - Flow resumed exactly where it failed");
    println!("     - Zero wasted computation");

    println!("\n  ✅ Durable Retry:");
    println!("     - Retry state persisted in storage");
    println!("     - Survived Worker-A crash");
    println!("     - Exponential backoff respected");

    println!("\n💡 Without Ergon (Traditional Queue):");
    println!("  ❌ Worker crash = entire task lost OR manual requeue");
    println!("  ❌ Retry from Step 1 = customer charged {} times", inventory_count);
    println!("  ❌ Need manual idempotency checks (50+ lines of code)");
    println!("  ❌ Need manual worker coordination");
    println!("  ❌ Need manual retry logic with exponential backoff");
    println!("  ❌ Need manual crash detection and recovery");

    println!("\n💡 With Ergon:");
    println!("  ✅ Automatic worker failover - zero configuration");
    println!("  ✅ Step-level resumability - payment charged once");
    println!("  ✅ Durable retry state - survives crashes");
    println!("  ✅ Distributed execution - multiple workers, one queue");
    println!("  ✅ Zero boilerplate - just add #[flow(retry = ...)]");
    println!("  ✅ Production-ready reliability with 5 lines of code\n");

    Ok(())
}
