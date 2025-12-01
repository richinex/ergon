//! Built-in Observability Demo
//!
//! This example demonstrates:
//! - Zero-boilerplate observability - no manual logging needed
//! - Query incomplete flows and see exactly where they're stuck
//! - Inspect flow execution history without instrumentation
//! - Compare with regular queues (no visibility)
//!
//! Scenario: Mix of flows - some complete, some stuck, some in progress
//! Goal: Show how easy it is to inspect flow state
//!
//! Run: cargo run --example observability_demo

use ergon::core::InvocationStatus;
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone)]
struct PaymentProcessor {
    order_id: String,
    amount: f64,
    should_fail: bool,
}

impl PaymentProcessor {
    #[flow]
    async fn process_payment(self: Arc<Self>) -> Result<PaymentResult, String> {
        println!("  [Flow] Processing payment for order {}", self.order_id);

        let validation = self.clone().validate_order().await?;
        let authorization = self.clone().authorize_card(validation).await?;
        let capture = self.clone().capture_payment(authorization).await?;
        let confirmation = self.clone().send_confirmation(capture).await?;

        Ok(confirmation)
    }

    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<ValidationResult, String> {
        println!("    [Step 1/4] Validating order {}", self.order_id);
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(ValidationResult {
            order_id: self.order_id.clone(),
            amount: self.amount,
        })
    }

    #[step(inputs(validation = "validate_order"))]
    async fn authorize_card(
        self: Arc<Self>,
        validation: ValidationResult,
    ) -> Result<AuthResult, String> {
        println!(
            "    [Step 2/4] Authorizing card for ${:.2}",
            validation.amount
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Some orders fail at authorization (stuck here)
        if self.should_fail {
            println!("    ✗ Authorization failed for order {}", self.order_id);
            return Err(format!("Card declined for order {}", self.order_id));
        }

        Ok(AuthResult {
            auth_code: format!("AUTH-{}", self.order_id),
            amount: validation.amount,
        })
    }

    #[step(inputs(auth = "authorize_card"))]
    async fn capture_payment(self: Arc<Self>, auth: AuthResult) -> Result<CaptureResult, String> {
        println!("    [Step 3/4] Capturing payment {}", auth.auth_code);
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(CaptureResult {
            transaction_id: format!("TXN-{}", self.order_id),
            captured_amount: auth.amount,
        })
    }

    #[step(inputs(capture = "capture_payment"))]
    async fn send_confirmation(
        self: Arc<Self>,
        capture: CaptureResult,
    ) -> Result<PaymentResult, String> {
        println!(
            "    [Step 4/4] Sending confirmation for {}",
            capture.transaction_id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(PaymentResult {
            order_id: self.order_id.clone(),
            transaction_id: capture.transaction_id,
            status: "completed".to_string(),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ValidationResult {
    order_id: String,
    amount: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct AuthResult {
    auth_code: String,
    amount: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CaptureResult {
    transaction_id: String,
    captured_amount: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PaymentResult {
    order_id: String,
    transaction_id: String,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║         Built-in Observability Demo                     ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = FlowScheduler::new(storage.clone());

    // Schedule mix of orders: some will complete, some will fail at authorization
    let orders = vec![
        ("ORD-001", 99.99, false),  // Will complete
        ("ORD-002", 149.99, true),  // Will fail at step 2
        ("ORD-003", 249.99, false), // Will complete
        ("ORD-004", 399.99, true),  // Will fail at step 2
        ("ORD-005", 79.99, false),  // Will complete
    ];

    println!("📋 Scheduling {} payment orders:\n", orders.len());
    let mut flow_ids = Vec::new();

    for (order_id, amount, should_fail) in &orders {
        let processor = PaymentProcessor {
            order_id: order_id.to_string(),
            amount: *amount,
            should_fail: *should_fail,
        };
        let flow_id = Uuid::new_v4();
        scheduler.schedule(processor, flow_id).await?;
        flow_ids.push((flow_id, order_id.to_string(), *should_fail));

        let status = if *should_fail {
            "(will fail)"
        } else {
            "(will succeed)"
        };
        println!("  • {} - ${:.2} {}", order_id, amount, status);
    }

    // ========================================================================
    // Process orders with a single worker
    // ========================================================================

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  Processing orders...                                      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let storage_clone = storage.clone();
    let worker = tokio::spawn(async move {
        let worker = FlowWorker::new(storage_clone.clone(), "payment-worker")
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<PaymentProcessor>| flow.process_payment())
            .await;
        let handle = worker.start().await;

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(800)).await;

        handle.shutdown().await;
    });

    worker.await?;

    // ========================================================================
    // OBSERVABILITY: Query and inspect flow states
    // ========================================================================

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║    OBSERVABILITY: Zero-Boilerplate Flow Inspection        ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("🔍 Query 1: Get ALL incomplete flows\n");

    let incomplete = storage.get_incomplete_flows().await?;
    println!("   Found {} incomplete flows:", incomplete.len());

    if incomplete.is_empty() {
        println!("   (All flows completed or none were stuck)");
    } else {
        for inv in &incomplete {
            println!("     • Flow {} - {}", inv.id(), inv.class_name());
        }
    }

    println!("\n🔍 Query 2: Detailed inspection of each order\n");

    for (flow_id, order_id, _should_fail) in &flow_ids {
        let invocations = storage.get_invocations_for_flow(*flow_id).await?;

        // Get flow status
        let flow_inv = invocations.iter().find(|i| i.step() == 0);
        let flow_status = flow_inv
            .map(|i| i.status())
            .unwrap_or(InvocationStatus::Pending);

        // Get step details
        let steps: Vec<_> = invocations.iter().filter(|i| i.step() > 0).collect();
        let completed_steps = steps
            .iter()
            .filter(|s| s.status() == InvocationStatus::Complete)
            .count();

        println!(
            "  📦 Order: {} (${:.2})",
            order_id,
            flow_ids
                .iter()
                .find(|(id, _, _)| id == flow_id)
                .map(|(_, _, fail)| if *fail { 149.99 } else { 99.99 })
                .unwrap_or(0.0)
        );
        println!("     Flow ID: {}", flow_id);
        println!("     Status:  {:?}", flow_status);
        println!(
            "     Steps:   {}/{} completed",
            completed_steps,
            steps.len()
        );

        // Show step-by-step progress
        if !steps.is_empty() {
            println!("     Progress:");
            for step in &steps {
                let status_icon = match step.status() {
                    InvocationStatus::Complete => "✓",
                    InvocationStatus::Pending => "⏸",
                    InvocationStatus::WaitingForSignal => "⏳",
                };
                println!(
                    "       {} {} (step {})",
                    status_icon,
                    step.method_name(),
                    step.step()
                );
            }
        }

        // Show where it's stuck
        if flow_status != InvocationStatus::Complete {
            let last_completed = steps
                .iter()
                .filter(|s| s.status() == InvocationStatus::Complete)
                .max_by_key(|s| s.step());

            if let Some(step) = last_completed {
                println!(
                    "     🔴 STUCK after: {} (step {})",
                    step.method_name(),
                    step.step()
                );
            } else {
                println!("     🔴 STUCK at: First step");
            }
        }

        println!();
    }

    // ========================================================================
    // Comparison with Regular Queues
    // ========================================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║              WITH REGULAR QUEUES (No Ergon)                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("❌ Query 1: \"Show me all stuck orders\"");
    println!("   Error: No built-in API. You must:");
    println!("     1. Manually log every step to external DB");
    println!("     2. Write custom query logic");
    println!("     3. Maintain sync between queue and DB");
    println!("     4. Handle stale data");

    println!("\n❌ Query 2: \"Which step did ORD-002 fail at?\"");
    println!("   Error: No execution history. You must:");
    println!("     1. Check application logs (if they exist)");
    println!("     2. grep through log files");
    println!("     3. Hope someone logged the right info");
    println!("     4. No structured data to query");

    println!("\n❌ Query 3: \"How many orders are stuck at authorization?\"");
    println!("   Error: Impossible without custom instrumentation");
    println!("     1. No step-level tracking");
    println!("     2. No queryable state");
    println!("     3. Must build entire observability system yourself");

    // ========================================================================
    // With Ergon - Built-in Observability
    // ========================================================================

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║               WITH ERGON (Zero Boilerplate)                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("✅ Query 1: \"Show me all stuck orders\"");
    println!("   let incomplete = storage.get_incomplete_flows().await?;");
    println!("   → Returns: {} flows", incomplete.len());

    println!("\n✅ Query 2: \"Which step did ORD-002 fail at?\"");
    let ord002_flow = flow_ids
        .iter()
        .find(|(_, id, _)| id == "ORD-002")
        .map(|(id, _, _)| id);
    if let Some(flow_id) = ord002_flow {
        let invocations = storage.get_invocations_for_flow(*flow_id).await?;
        let failed_step = invocations
            .iter()
            .filter(|i| i.step() > 0)
            .filter(|i| i.status() != InvocationStatus::Complete)
            .min_by_key(|i| i.step());

        if let Some(step) = failed_step {
            println!("   let invocations = storage.get_invocations_for_flow(flow_id).await?;");
            println!(
                "   → Answer: Failed at '{}' (step {})",
                step.method_name(),
                step.step()
            );
        }
    }

    println!("\n✅ Query 3: \"How many orders are stuck at authorization?\"");
    let mut stuck_at_auth = 0;
    for (flow_id, _, _) in &flow_ids {
        let invocations = storage.get_invocations_for_flow(*flow_id).await?;
        let steps: Vec<_> = invocations.iter().filter(|i| i.step() > 0).collect();

        // Check if stuck at step 2 (authorize_card)
        if !steps.is_empty()
            && steps
                .iter()
                .any(|s| s.step() == 1 && s.status() == InvocationStatus::Complete)
            && !steps
                .iter()
                .any(|s| s.step() == 2 && s.status() == InvocationStatus::Complete)
        {
            stuck_at_auth += 1;
        }
    }
    println!("   // Query each flow's step history (5 lines of code)");
    println!(
        "   → Answer: {} orders stuck at authorization step",
        stuck_at_auth
    );

    // ========================================================================
    // Summary
    // ========================================================================

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                        SUMMARY                             ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let complete_count = flow_ids.len() - incomplete.len();

    println!("📊 Execution Results:");
    println!("   Total orders:     {}", flow_ids.len());
    println!("   Completed:        {}", complete_count);
    println!("   Failed/Stuck:     {}", incomplete.len());

    println!("\n🎯 Key Takeaways:");
    println!("   ✓ Zero manual logging code written");
    println!("   ✓ Complete execution history automatically tracked");
    println!("   ✓ Query flow state with simple API calls");
    println!("   ✓ Step-level visibility out of the box");
    println!("   ✓ Know exactly where each flow is stuck");

    println!("\n💡 With Regular Queues:");
    println!("   ✗ Must manually log every step");
    println!("   ✗ Build custom tracking infrastructure");
    println!("   ✗ Maintain separate observability system");
    println!("   ✗ 100+ lines of boilerplate per workflow");

    println!("\n💡 With Ergon:");
    println!("   ✓ Automatic execution tracking");
    println!("   ✓ Built-in queryable state");
    println!("   ✓ Zero boilerplate observability");
    println!("   ✓ Production-ready monitoring\n");

    Ok(())
}
