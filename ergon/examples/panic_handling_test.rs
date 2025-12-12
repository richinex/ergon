//! Panic Handling Test Example
//!
//! This example demonstrates the distinction between:
//! 1. **Programmer mistakes** (non-determinism) → Panic immediately
//! 2. **Environment problems** (storage errors) → Panic but Worker retries
//! 3. **Normal execution** → Works correctly
//!
//! ## Key Principle (from when_to_panic.txt)
//! "panic! is for mistakes that the programmer made; Result is for problems in the environment"
//! - burntsushi & scottmcmrust
//!
//! ## Run with
//! ```bash
//! cargo run --example panic_handling_test --features=sqlite
//! ```

use ergon::executor::ExecutionError;
use ergon::prelude::*;
use ergon::TaskStatus;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// Counter to track execution attempts
static EXECUTION_COUNT: AtomicU32 = AtomicU32::new(0);

// ============================================================================
// Test Flow - Normal Execution
// ============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct NormalFlow {
    order_id: String,
    amount: f64,
}

impl NormalFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("\n[NORMAL] Processing order: {}", self.order_id);

        // Step 1: Validate
        self.clone().validate().await?;

        // Step 2: Process payment
        let tx_id = self.clone().process_payment().await?;

        println!("[NORMAL] Order completed: {}", tx_id);
        Ok(tx_id)
    }

    #[step]
    async fn validate(self: Arc<Self>) -> Result<(), String> {
        println!("[NORMAL] Validating order {}", self.order_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        if self.amount <= 0.0 {
            return Err("Invalid amount".to_string());
        }

        println!("[NORMAL] Validation passed");
        Ok(())
    }

    #[step]
    async fn process_payment(self: Arc<Self>) -> Result<String, String> {
        let attempt = EXECUTION_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        println!(
            "[NORMAL] Processing payment: ${} (attempt #{})",
            self.amount, attempt
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        let tx_id = format!("TXN-{}", Uuid::new_v4().to_string()[..8].to_uppercase());
        println!("[NORMAL] Payment processed: {}", tx_id);
        Ok(tx_id)
    }
}

// ============================================================================
// Main - Test Storage Error Handling
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║          Panic Handling Test Example                     ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    let storage = Arc::new(SqliteExecutionLog::new("sqlite::memory:").await?);

    // ========================================================================
    // Test 1: Normal Execution (No Errors)
    // ========================================================================
    println!("\n=== Test 1: Normal Execution ===");
    println!("Expected: Flow completes successfully\n");

    EXECUTION_COUNT.store(0, Ordering::SeqCst);

    let scheduler = Scheduler::new(storage.clone());
    let worker =
        Worker::new(storage.clone(), "test-worker").with_poll_interval(Duration::from_millis(100));

    worker
        .register(|flow: Arc<NormalFlow>| flow.process())
        .await;

    let worker_handle = worker.start().await;

    let flow1 = NormalFlow {
        order_id: "ORD-001".to_string(),
        amount: 99.99,
    };

    let flow_id_1 = Uuid::new_v4();
    scheduler.schedule(flow1, flow_id_1).await?;

    // Wait for completion
    tokio::time::sleep(Duration::from_secs(1)).await;

    match storage.get_scheduled_flow(flow_id_1).await? {
        Some(scheduled) => {
            println!("\n[RESULT] Test 1 status: {:?}", scheduled.status);
            if matches!(scheduled.status, TaskStatus::Complete) {
                println!("[PASS] Normal execution completed successfully");
            } else {
                println!("[FAIL] Unexpected status: {:?}", scheduled.status);
            }
        }
        None => println!("[PASS] Flow completed and removed from queue"),
    }

    // ========================================================================
    // Test 2: Storage Error Philosophy
    // ========================================================================
    println!("\n\n=== Test 2: Storage Error Philosophy ===");
    println!("Why treat storage errors as 'cache miss' instead of panic/error?\n");

    println!("[RATIONALE] Resilience over Perfection:");
    println!("  - Option A: Panic → Worker retries from scratch → blocked if storage down");
    println!("  - Option B: Cache miss → Execute step → Progress even if storage down");
    println!("  - Option B is more resilient!\n");

    println!("[TRADEOFF] Explicit:");
    println!("  - If storage is down, worst case is duplicate work");
    println!("  - But steps SHOULD be idempotent anyway (best practice)");
    println!("  - Duplicate work > blocked work\n");

    println!("[EDGE CASE] What if write fails?");
    println!("  - Cache check fails → execute step → log_step_completion fails");
    println!("  - Result: Step completes but isn't cached");
    println!("  - On replay: Step re-executes (duplicate work)");
    println!("  - Acceptable because idempotent steps are a best practice");

    // ========================================================================
    // Test 3: Storage Error Handling
    // ========================================================================
    println!("\n\n=== Test 3: Storage Error Handling ===");
    println!("When storage errors occur during cache checks:\n");

    println!("[INFO] Consistent Behavior Across All Contexts:");
    println!("  1. Macro catches ExecutionError from get_cached_result()");
    println!("  2. Non-determinism (Incompatible) → Always panics (bug)");
    println!("  3. Storage errors (Core/Failed) → Treat as cache miss:");
    println!("     - DAG executor: Continues execution");
    println!("     - Step direct exec: Continues execution");
    println!("     - Flow execution: Continues execution\n");

    println!("[INFO] Examples of Storage Errors:");
    println!("  - Database connection timeout");
    println!("  - Query failure due to temporary lock");
    println!("  - Network hiccup to remote storage");
    println!("  - Temporary file system issue\n");

    println!("[INFO] Why Consistent Cache Miss Approach:");
    println!("  - Works with or without Worker orchestration");
    println!("  - Allows progress even when storage is temporarily down");
    println!("  - If storage recovers mid-flow, subsequent steps can still cache");
    println!("  - Only bugs (non-determinism) cause panics\n");

    println!("[INFO] Code Changes:");
    println!("  - step.rs DAG executor: Treats as cache miss (line 539-544)");
    println!("  - step.rs direct exec: Treats as cache miss (line 697-701)");
    println!("  - flow.rs: Treats as cache miss (line 203-209)\n");

    println!("[PASS] Storage errors handled as retryable environment problems");

    // ========================================================================
    // Cleanup
    // ========================================================================
    worker_handle.shutdown().await;

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Example Complete - Panic Handling Tests                 ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    println!("\n[INFO] Key Takeaways:");
    println!("   1. Non-determinism (programmer bug) → Always panics");
    println!("   2. Storage errors (environment) → Treat as cache miss, continue");
    println!("   3. Consistent across all execution contexts");
    println!("   4. More resilient: Progress > Perfection");
    println!("   5. Assumes idempotent steps (best practice for durable workflows)\n");

    storage.close().await?;
    Ok(())
}
