//! Non-Determinism Detection Demo
//!
//! Demonstrates how Ergon's hash-based parameter detection catches bugs where
//! external state changes between flow runs.
//!
//! BUGGY VERSION: The discount check is made OUTSIDE of a step.
//! When apply_discount is called with Some("SAVE20") on run 1, then None on run 2,
//! the parameter hash differs and triggers non-determinism detection.

use ergon::prelude::*;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

// Simulates external state that changes between runs
static HAS_DISCOUNT_CODE: AtomicBool = AtomicBool::new(true);
static VALIDATE_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct PaymentProcessor {
    amount: f64,
    // BUGGY: This is set based on external state, not captured in a step!
    discount_code: Option<String>,
}

impl PaymentProcessor {
    #[step]
    async fn apply_discount(self: Arc<Self>) -> Result<f64, String> {
        // BUG: This reads from self.discount_code which was set based on external state!
        // If external state changes between runs, this will have a different value
        match &self.discount_code {
            Some(code) => {
                println!("  [apply_discount] Applying discount code: {}", code);
                Ok(self.amount * 0.8) // 20% off
            }
            None => {
                println!("  [apply_discount] No discount applied");
                Ok(self.amount)
            }
        }
    }

    #[step]
    async fn validate_card(self: Arc<Self>) -> Result<String, String> {
        let attempt = VALIDATE_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            println!("  [validate_card] FAILED - retrying...");
            Err("Card validation failed".to_string())
        } else {
            println!("  [validate_card] OK");
            Ok("validated".to_string())
        }
    }

    #[step(
        depends_on = ["apply_discount", "validate_card"],
        inputs(amount = "apply_discount")
    )]
    async fn send_receipt(self: Arc<Self>, amount: f64) -> Result<String, String> {
        println!("  [send_receipt] Sending confirmation for ${:.2}", amount);
        Ok(format!("Payment of ${:.2} complete", amount))
    }

    #[flow]
    async fn process_payment(self: Arc<Self>) -> Result<String, String> {
        dag! {
            self.register_apply_discount();
            self.register_validate_card();
            self.register_send_receipt()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Non-Determinism Detection Demo");
    println!("===============================\n");

    println!("This shows how hash-based detection catches Option<Some> vs Option<None>.\n");

    let db = "/tmp/nondeterminism_test.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);
    storage.reset().await?;

    // Reset state
    HAS_DISCOUNT_CODE.store(true, Ordering::SeqCst);
    VALIDATE_ATTEMPTS.store(0, Ordering::SeqCst);

    let flow_id = Uuid::new_v4();

    // =========================================================================
    // Run 1: Customer HAS discount code
    // =========================================================================
    println!("Run 1: Customer HAS discount code");
    println!("----------------------------------\n");

    let processor1 = Arc::new(PaymentProcessor {
        amount: 100.0,
        discount_code: if HAS_DISCOUNT_CODE.load(Ordering::SeqCst) {
            Some("SAVE20".to_string())
        } else {
            None
        },
    });

    let executor1 = Executor::new(flow_id, processor1.clone(), storage.clone());
    let result1 = executor1
        .execute(|f| Box::pin(f.clone().process_payment()))
        .await;

    match &result1 {
        FlowOutcome::Completed(r) => println!("  Result: {:?}\n", r),
        FlowOutcome::Suspended(reason) => println!("  Suspended: {:?}\n", reason),
    }

    // Show database state
    println!("Database state after Run 1:");
    let invocations = storage.get_invocations_for_flow(flow_id).await?;
    for inv in &invocations {
        if inv.step() > 0 {
            println!(
                "  step {}: {} (params_hash: 0x{:016x})",
                inv.step(),
                inv.method_name(),
                inv.params_hash()
            );
        }
    }
    println!();

    // =========================================================================
    // Run 2: External state changed - customer NO LONGER has discount
    // =========================================================================
    HAS_DISCOUNT_CODE.store(false, Ordering::SeqCst);

    println!("Run 2: Customer NO LONGER has discount code (state changed!)");
    println!("-------------------------------------------------------------\n");
    println!("       Calling apply_discount with None instead of Some(\"SAVE20\")...\n");

    let processor2 = Arc::new(PaymentProcessor {
        amount: 100.0,
        discount_code: if HAS_DISCOUNT_CODE.load(Ordering::SeqCst) {
            Some("SAVE20".to_string())
        } else {
            None
        },
    });

    // Use catch_unwind to capture the panic
    let executor2 = Executor::new(flow_id, processor2.clone(), storage.clone());
    let result2 =
        std::panic::AssertUnwindSafe(executor2.execute(|f| Box::pin(f.clone().process_payment())))
            .catch_unwind()
            .await;

    // =========================================================================
    // Results
    // =========================================================================
    println!("Detection Result");
    println!("----------------\n");

    match result2 {
        Ok(FlowOutcome::Completed(result)) => {
            println!("  Result: {:?}", result);
            println!("\n[FAIL] Non-determinism was NOT detected.");
            println!("       This should not happen if hash validation is working.");
        }
        Ok(FlowOutcome::Suspended(reason)) => {
            println!("  Suspended: {:?}", reason);
            println!("\n[WARN] Flow suspended, non-determinism may or may not be detected.");
        }
        Err(panic) => {
            let panic_msg = panic
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("Unknown panic");

            println!("[PASS] Non-determinism detected. Flow panicked with:");
            println!("       {}\n", panic_msg);
            println!("       Hash-based detection caught Option<Some> vs Option<None>.");
            println!("       The stored params hash differs from the current params hash.");
        }
    }

    println!("\nKey Insight");
    println!("-----------\n");
    println!("  The bug: discount_code was set from external state OUTSIDE a step.");
    println!("  When state changed between runs, the flow struct had different data.");
    println!("  Ergon's hash validation detected the mismatch and prevented silent corruption.");
    println!("\n  Fix: Capture external state INSIDE a step so it's recorded and replayed.");

    Ok(())
}
