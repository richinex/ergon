//! Built-in Observability Demo
//!
//! This example demonstrates:
//! - Zero-boilerplate observability with no manual logging needed
//! - Querying incomplete flows to see exactly where they are stuck
//! - Inspecting flow execution history without instrumentation
//! - Step-level visibility and progress tracking out of the box
//! - Comparing Ergon's built-in observability vs regular queues
//!
//! ## Scenario
//! Five payment processing orders are scheduled: three will complete successfully,
//! two will fail at the card authorization step. We then query the execution log
//! to inspect the state of each flow, show step-by-step progress, and identify
//! where failed flows are stuck.
//!
//! ## Key Takeaways
//! - No manual logging code required - execution is automatically tracked
//! - Query incomplete flows with simple API: storage.get_incomplete_flows()
//! - Inspect step-by-step progress for any flow with get_invocations_for_flow()
//! - Know exactly which step a flow is stuck at without custom instrumentation
//! - Compare with regular queues where you'd need 100+ lines of boilerplate
//! - Production-ready monitoring and debugging built into the framework
//!
//! ## Run with
//! ```bash
//! cargo run --example observability_demo
//! ```

use ergon::core::InvocationStatus;
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, FlowType)]
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
            println!("    Authorization failed for order {}", self.order_id);
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

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct ValidationResult {
    order_id: String,
    amount: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct AuthResult {
    auth_code: String,
    amount: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct CaptureResult {
    transaction_id: String,
    captured_amount: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct PaymentResult {
    order_id: String,
    transaction_id: String,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    // Schedule mix of orders: some will complete, some will fail at authorization
    let orders = vec![
        ("ORD-001", 99.99, false),  // Will complete
        ("ORD-002", 149.99, true),  // Will fail at step 2
        ("ORD-003", 249.99, false), // Will complete
        ("ORD-004", 399.99, true),  // Will fail at step 2
        ("ORD-005", 79.99, false),  // Will complete
    ];

    let mut flow_ids = Vec::new();

    for (order_id, amount, should_fail) in &orders {
        let processor = PaymentProcessor {
            order_id: order_id.to_string(),
            amount: *amount,
            should_fail: *should_fail,
        };
        let flow_id = Uuid::new_v4();
        scheduler.schedule_with(processor, flow_id).await?;
        flow_ids.push((flow_id, order_id.to_string(), *should_fail));
    }

    let storage_clone = storage.clone();
    let worker = tokio::spawn(async move {
        let worker = Worker::new(storage_clone.clone(), "payment-worker")
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

    // Query and inspect flow states
    println!("\nQuery 1: Get ALL incomplete flows\n");

    let incomplete = storage.get_incomplete_flows().await?;
    println!("   Found {} incomplete flows:", incomplete.len());

    if incomplete.is_empty() {
        println!("   (All flows completed or none were stuck)");
    } else {
        for inv in &incomplete {
            println!("     - Flow {} - {}", inv.id(), inv.class_name());
        }
    }

    println!("\nQuery 2: Detailed inspection of each order\n");

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
            "  Order: {} (${:.2})",
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
                    InvocationStatus::Complete => "[DONE]",
                    InvocationStatus::Pending => "[PEND]",
                    InvocationStatus::WaitingForSignal => "[WAIT]",
                    InvocationStatus::WaitingForTimer => "[TIME]",
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
                    "     STUCK after: {} (step {})",
                    step.method_name(),
                    step.step()
                );
            } else {
                println!("     STUCK at: First step");
            }
        }

        println!();
    }

    Ok(())
}
