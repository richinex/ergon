//! Distributed Worker Failover Demo with RetryableError
//!
//! This example demonstrates Ergon's distributed retry system with multiple workers.
//!
//! ## Scenario:
//! 1. Three workers (Worker-A, Worker-B, Worker-C) polling the same queue
//! 2. Worker-A picks up the flow and starts processing
//! 3. Worker-A completes Steps 1-2 (validate, charge payment)
//! 4. Worker-A CRASHES during Step 3 (reserve inventory)
//! 5. Flow is automatically retried and re-queued
//! 6. Worker-B picks up the flow
//! 7. Worker-B skips Steps 1-2 (loaded from cache)
//! 8. Worker-B completes Step 3 successfully
//!
//! ## Key Demonstrations:
//! - Distributed execution with multiple workers
//! - Automatic failover when a worker crashes
//! - Step-level resumability (payment NOT re-charged)
//! - RetryableError trait for smart error handling
//! - Transient vs Permanent error distinction
//! - Durable retry state (survives worker crashes)
//! - Exponential backoff between retries
//! - Zero duplicate operations
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
            InventoryError::ServiceUnavailable => {
                write!(f, "Inventory service temporarily unavailable")
            }
            InventoryError::RateLimited => write!(f, "Rate limit exceeded"),
            InventoryError::ItemNotFound(item) => write!(f, "Item '{}' not found in catalog", item),
            InventoryError::InsufficientStock {
                requested,
                available,
            } => {
                write!(
                    f,
                    "Insufficient stock: requested {}, only {} available",
                    requested, available
                )
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
        // Get worker ID from execution context if available
        // For now, we'll track this via storage metadata
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

        tokio::time::sleep(Duration::from_millis(200)).await;
        println!("    Charging ${:.2} - Execution #{}", self.amount, count);

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

        // Simulate Worker-A crash on first attempt
        // We return an error (which triggers retry) and Worker-A will shutdown
        if count == 1 && !WORKER_A_CRASHED.load(Ordering::SeqCst) {
            WORKER_A_CRASHED.store(true, Ordering::SeqCst);
            println!("    Worker-A crashing! (error + shutdown simulates crash)");
            return Err(InventoryError::WorkerCrashed);
        }

        // Simulate transient API timeout on second attempt (TRANSIENT ERROR - will retry)
        if count == 2 {
            println!("    API timeout - transient error");
            tokio::time::sleep(Duration::from_millis(50)).await;
            return Err(InventoryError::ApiTimeout);
        }

        // Success on 3rd attempt
        tokio::time::sleep(Duration::from_millis(150)).await;
        println!("    Inventory reserved on attempt {}", count);

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
    println!("\nDistributed Worker Failover Demo");
    println!("===================================\n");

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
    let task_id = scheduler.schedule(order.clone(), flow_id).await?;

    println!("Scheduled order: {}", order.order_id);
    println!("Amount to charge: ${:.2}", order.amount);
    println!("Starting workers...\n");

    // Worker-A: Will crash during Step 3 (panic simulates hard crash)
    let storage_a = storage.clone();
    let worker_a = tokio::spawn(async move {
        println!("[Worker-A] Starting up...");
        let worker =
            FlowWorker::new(storage_a, "Worker-A").with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderProcessor>| flow.process_order())
            .await;
        let handle = worker.start().await;

        // Worker-A will execute the flow, which will panic during Step 3
        // The panic happens in the spawned task, simulating a crash
        // Wait long enough for the crash to occur, then shutdown
        tokio::time::sleep(Duration::from_millis(500)).await;

        // After Worker-A crashes (panic in spawned task), shut it down
        // This simulates the worker process dying
        if WORKER_A_CRASHED.load(Ordering::SeqCst) {
            println!("[Worker-A] Detected crash, shutting down immediately!");
            handle.shutdown().await;
        } else {
            tokio::time::sleep(Duration::from_secs(3)).await;
            println!("[Worker-A] Shutting down gracefully");
            handle.shutdown().await;
        }
    });

    // Worker-B: Will pick up after Worker-A crashes
    let storage_b = storage.clone();
    let worker_b = tokio::spawn(async move {
        println!("[Worker-B] Starting up...");
        let worker =
            FlowWorker::new(storage_b, "Worker-B").with_poll_interval(Duration::from_millis(50));

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
        let worker =
            FlowWorker::new(storage_c, "Worker-C").with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderProcessor>| flow.process_order())
            .await;
        let handle = worker.start().await;

        tokio::time::sleep(Duration::from_secs(8)).await;

        println!("[Worker-C] Shutting down gracefully");
        handle.shutdown().await;
    });

    // Wait for all workers
    let _ = tokio::join!(worker_a, worker_b, worker_c);

    // Final results
    println!("\nFinal Results");
    println!("=============\n");

    let invocations = storage.get_invocations_for_flow(flow_id).await?;
    let flow_invocation = invocations.iter().find(|i| i.step() == 0);

    if let Some(flow) = flow_invocation {
        println!("Flow Status: {:?}", flow.status());
    }

    let payment_count = PAYMENT_CHARGE_COUNT.load(Ordering::SeqCst);
    let inventory_count = INVENTORY_RESERVE_COUNT.load(Ordering::SeqCst);

    println!("\nExecution Statistics:");
    println!("  Payment charged: {} time(s)", payment_count);
    println!("  Inventory attempts: {} time(s)", inventory_count);

    // Show which worker handled the flow
    println!("\nWorker Distribution:");
    if let Some(scheduled_flow) = storage.get_scheduled_flow(task_id).await? {
        let worker = scheduled_flow.locked_by.as_deref().unwrap_or("unknown");
        println!("  Order ORD-99999 completed by: {}", worker);
        println!("  (Note: Worker-A crashed, so Worker-B or Worker-C took over)");
    }

    Ok(())
}
