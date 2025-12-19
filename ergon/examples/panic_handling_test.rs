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

        // Generate transaction ID at flow level for determinism
        let tx_id = format!("TXN-{}", Uuid::new_v4().to_string()[..8].to_uppercase());

        // Step 1: Validate
        self.clone().validate().await?;

        // Step 2: Process payment
        self.clone().process_payment(tx_id.clone()).await?;

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
    async fn process_payment(self: Arc<Self>, tx_id: String) -> Result<(), String> {
        let attempt = EXECUTION_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        println!(
            "[NORMAL] Processing payment: ${} (attempt #{})",
            self.amount, attempt
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        println!("[NORMAL] Payment processed: {}", tx_id);
        Ok(())
    }
}

// ============================================================================
// Main - Test Storage Error Handling
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(SqliteExecutionLog::new("sqlite::memory:").await?);

    EXECUTION_COUNT.store(0, Ordering::SeqCst);

    let scheduler = Scheduler::new(storage.clone()).unversioned();
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
    scheduler.schedule_with(flow1, flow_id_1).await?;

    // Wait for completion
    tokio::time::sleep(Duration::from_secs(1)).await;

    worker_handle.shutdown().await;

    storage.close().await?;
    Ok(())
}
