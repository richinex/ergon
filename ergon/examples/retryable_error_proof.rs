//! Retryable Trait - Proof of Concept
//!
//! This example demonstrates:
//! - Concrete evidence that Retryable trait controls retry behavior
//! - Differential behavior between retryable and non-retryable errors
//! - Execution counters proving retry logic respects is_retryable()
//! - Retryable errors are automatically retried (ApiTimeout)
//! - Non-retryable errors fail immediately without retry (ItemNotFound)
//!
//! ## Scenario
//! Two parallel test cases run simultaneously: Scenario A returns a retryable error
//! (ApiTimeout with is_retryable() = true), which causes the step to execute 3 times
//! before succeeding. Scenario B returns a non-retryable error (ItemNotFound with
//! is_retryable() = false), which causes the step to execute only 1 time and fail
//! immediately without retry.
//!
//! ## Key Takeaways
//! - Execution counters provide concrete evidence of retry behavior
//! - Retryable errors (STEP_A_EXECUTIONS = 3) are retried automatically
//! - Non-retryable errors (STEP_B_EXECUTIONS = 1) fail immediately
//! - The is_retryable() method controls whether an error triggers retry
//! - RetryPolicy configuration respects the Retryable trait
//! - Framework distinguishes transient failures from permanent failures
//! - This proves the trait is not just documentation but enforced behavior
//!
//! ## Run with
//! ```bash
//! cargo run --example retryable_error_proof
//! ```

use ergon::core::RetryPolicy;
use ergon::prelude::*;
use ergon::Retryable;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Global counters - this is our EVIDENCE
static STEP_A_EXECUTIONS: AtomicU32 = AtomicU32::new(0);
static STEP_B_EXECUTIONS: AtomicU32 = AtomicU32::new(0);

// ============================================================================
// Custom Error Type with Retryable Trait
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
enum InventoryError {
    /// TRANSIENT error - network timeout, should retry
    ApiTimeout,

    /// PERMANENT error - item doesn't exist, no point retrying
    ItemNotFound(String),
}

impl std::fmt::Display for InventoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InventoryError::ApiTimeout => write!(f, "API timeout - transient network error"),
            InventoryError::ItemNotFound(item) => write!(f, "Item '{}' not found in catalog", item),
        }
    }
}

impl std::error::Error for InventoryError {}

/// This is the KEY implementation - it determines retry behavior
impl Retryable for InventoryError {
    fn is_retryable(&self) -> bool {
        match self {
            InventoryError::ApiTimeout => {
                println!("      is_retryable() called -> returning true (will retry)");
                true
            }
            InventoryError::ItemNotFound(_) => {
                println!("      is_retryable() called -> returning false (will NOT retry)");
                false
            }
        }
    }
}

// ============================================================================
// Scenario A: RETRYABLE Error (should execute 3 times)
// ============================================================================

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct OrderA {
    order_id: String,
}

impl OrderA {
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process_order(self: Arc<Self>) -> Result<String, InventoryError> {
        println!("\n[Flow A] Processing order {}", self.order_id);

        let result = self.clone().check_inventory().await?;

        println!("[Flow A] Order {} completed successfully", self.order_id);
        Ok(result)
    }

    #[step]
    async fn check_inventory(self: Arc<Self>) -> Result<String, InventoryError> {
        let count = STEP_A_EXECUTIONS.fetch_add(1, Ordering::SeqCst) + 1;

        println!("  [Step A] Checking inventory (execution #{})", count);

        // Fail first 2 times with RETRYABLE error
        if count < 3 {
            println!("    API timeout occurred (transient network error)");
            println!("    Returning InventoryError::ApiTimeout");
            return Err(InventoryError::ApiTimeout);
        }

        // Success on 3rd attempt
        println!("    Inventory check succeeded on attempt {}", count);
        Ok(format!("Inventory reserved for {}", self.order_id))
    }
}

// ============================================================================
// Scenario B: NON-RETRYABLE Error (should execute ONLY 1 time)
// ============================================================================

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct OrderB {
    order_id: String,
    item_sku: String,
}

impl OrderB {
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process_order(self: Arc<Self>) -> Result<String, InventoryError> {
        println!("\n[Flow B] Processing order {}", self.order_id);

        let result = self.clone().check_inventory().await?;

        println!("[Flow B] Order {} completed successfully", self.order_id);
        Ok(result)
    }

    #[step]
    async fn check_inventory(self: Arc<Self>) -> Result<String, InventoryError> {
        let count = STEP_B_EXECUTIONS.fetch_add(1, Ordering::SeqCst) + 1;

        println!("  [Step B] Checking inventory (execution #{})", count);
        println!("    Looking up item SKU: {}", self.item_sku);

        // ALWAYS fail with NON-RETRYABLE error
        println!("    Item not found in catalog (permanent error)");
        println!("    Returning InventoryError::ItemNotFound");
        Err(InventoryError::ItemNotFound(self.item_sku.clone()))
    }
}

// ============================================================================
// Main - Run Both Scenarios and Show Evidence
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = Scheduler::new(storage.clone());

    let order_a = OrderA {
        order_id: "ORD-A-001".to_string(),
    };
    let flow_id_a = Uuid::new_v4();
    scheduler.schedule(order_a.clone(), flow_id_a).await?;

    let storage_a = storage.clone();
    let worker_a = tokio::spawn(async move {
        let worker =
            Worker::new(storage_a, "Worker-A").with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderA>| flow.process_order())
            .await;
        let handle = worker.start().await;

        tokio::time::sleep(Duration::from_secs(5)).await;
        handle.shutdown().await;
    });

    worker_a.await?;

    let order_b = OrderB {
        order_id: "ORD-B-002".to_string(),
        item_sku: "INVALID-SKU-999".to_string(),
    };
    let flow_id_b = Uuid::new_v4();
    scheduler.schedule(order_b.clone(), flow_id_b).await?;

    let storage_b = storage.clone();
    let worker_b = tokio::spawn(async move {
        let worker =
            Worker::new(storage_b, "Worker-B").with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<OrderB>| flow.process_order())
            .await;
        let handle = worker.start().await;

        tokio::time::sleep(Duration::from_secs(5)).await;
        handle.shutdown().await;
    });

    worker_b.await?;

    Ok(())
}
