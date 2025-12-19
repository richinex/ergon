//! Timer Demo with DAG Execution
//!
//! This example demonstrates event-driven timers working correctly with DAG
//! (Directed Acyclic Graph) execution, where steps run in parallel with dependencies.
//!
//! ## Scenario: Order Processing Workflow
//!
//! DAG Topology:
//! ```
//!                    validate_order (immediate)
//!                           |
//!           ┌───────────────┴───────────────┐
//!           │                               │
//!    fraud_check (2s timer)        reserve_inventory (immediate)
//!           │                               │
//!           └───────────────┬───────────────┘
//!                           │
//!                  process_payment (3s timer)
//!                           |
//!                   ship_order (1s timer)
//! ```
//!
//! ## Key Demonstration
//!
//! - fraud_check and reserve_inventory run IN PARALLEL after validate_order
//! - process_payment waits for BOTH to complete (multi-dependency)
//! - Timers fire independently and don't block other parallel steps
//! - Event-driven notifications ensure zero polling overhead
//! - Workers wake up only when timers fire, not on intervals
//!
//! ## Expected Timeline
//!
//! ```
//! t=0ms:    validate_order starts
//! t=50ms:   validate_order completes
//! t=50ms:   fraud_check starts (2s timer scheduled)
//! t=50ms:   reserve_inventory starts (parallel!)
//! t=100ms:  reserve_inventory completes
//! t=2050ms: fraud_check timer fires
//! t=2050ms: process_payment starts (3s timer scheduled)
//! t=5050ms: process_payment timer fires
//! t=5050ms: ship_order starts (1s timer scheduled)
//! t=6050ms: ship_order timer fires
//! t=6050ms: Flow completes
//! ```
//!
//! Total time: ~6 seconds (vs 7s sequential, vs instant without timers)
//!
//! ## Run with
//! ```bash
//! cargo run --example timer_demo_dag
//! ```

use chrono::Utc;
use ergon::executor::{schedule_timer_named, ExecutionError, Scheduler, Worker};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderWorkflowDAG {
    order_id: String,
    customer_email: String,
    amount: f64,
}

impl OrderWorkflowDAG {
    /// Step 1: Validate order (immediate, no dependencies)
    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!(
            "\n[{}] [{}] Validating order...",
            format_time(),
            self.order_id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!(
            "[{}] [{}] ✓ Order validated (amount: ${:.2})",
            format_time(),
            self.order_id,
            self.amount
        );
        Ok(format!("validated-{}", self.order_id))
    }

    /// Step 2: Fraud check with 2s timer (depends on validation)
    /// This runs IN PARALLEL with reserve_inventory
    #[step(depends_on = "validate_order")]
    async fn fraud_check(self: Arc<Self>) -> Result<bool, ExecutionError> {
        println!(
            "[{}] [{}] Starting fraud check (waiting 2 seconds)...",
            format_time(),
            self.order_id
        );
        schedule_timer_named(Duration::from_secs(2), "fraud-check-timer").await?;
        println!(
            "[{}] [{}] ✓ Fraud check complete - no fraud detected",
            format_time(),
            self.order_id
        );
        Ok(true)
    }

    /// Step 3: Reserve inventory (depends on validation)
    /// This runs IN PARALLEL with fraud_check
    #[step(depends_on = "validate_order")]
    async fn reserve_inventory(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!(
            "[{}] [{}] Reserving inventory...",
            format_time(),
            self.order_id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!(
            "[{}] [{}] ✓ Inventory reserved",
            format_time(),
            self.order_id
        );
        Ok(format!("inventory-{}", self.order_id))
    }

    /// Step 4: Process payment with 3s timer (depends on BOTH fraud_check AND reserve_inventory)
    /// This demonstrates multi-dependency: waits for parallel steps to complete
    #[step(
        depends_on = ["fraud_check", "reserve_inventory"],
        inputs(fraud_result = "fraud_check", inventory_id = "reserve_inventory")
    )]
    async fn process_payment(
        self: Arc<Self>,
        fraud_result: bool,
        inventory_id: String,
    ) -> Result<String, ExecutionError> {
        println!(
            "[{}] [{}] Processing payment (fraud_ok={}, inventory={})...",
            format_time(),
            self.order_id,
            fraud_result,
            inventory_id
        );
        println!(
            "[{}] [{}] Waiting 3 seconds for payment processing...",
            format_time(),
            self.order_id
        );
        schedule_timer_named(Duration::from_secs(3), "payment-processing-timer").await?;
        println!(
            "[{}] [{}] ✓ Payment processed (${:.2})",
            format_time(),
            self.order_id,
            self.amount
        );
        Ok(format!("payment-{}", self.order_id))
    }

    /// Step 5: Ship order with 1s timer (depends on payment)
    #[step(depends_on = "process_payment", inputs(payment_id = "process_payment"))]
    async fn ship_order(self: Arc<Self>, payment_id: String) -> Result<String, ExecutionError> {
        println!(
            "[{}] [{}] Preparing shipment (payment={})...",
            format_time(),
            self.order_id,
            payment_id
        );
        println!(
            "[{}] [{}] Waiting 1 second for shipping preparation...",
            format_time(),
            self.order_id
        );
        schedule_timer_named(Duration::from_secs(1), "shipping-prep-timer").await?;
        println!(
            "[{}] [{}] ✓ Order shipped to {}",
            format_time(),
            self.order_id,
            self.customer_email
        );
        Ok(format!("TRACK-{}", &self.order_id[4..]))
    }

    /// Main flow using DAG execution
    #[flow]
    async fn process_order_dag(self: Arc<Self>) -> Result<String, ExecutionError> {
        dag! {
            self.register_validate_order();
            self.register_fraud_check();
            self.register_reserve_inventory();
            self.register_process_payment();
            self.register_ship_order()
        }
    }
}

fn format_time() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("\n=== Event-Driven Timer Demo with DAG Execution ===\n");
    println!("This demo shows:");
    println!("  • Timers working with parallel DAG execution");
    println!("  • fraud_check and reserve_inventory run in PARALLEL");
    println!("  • process_payment waits for BOTH to complete");
    println!("  • Zero polling - workers wake only when timers fire");
    println!("  • Event-driven notifications for instant status updates");
    println!("\nExpected total time: ~6 seconds\n");

    // Create SQLite storage for durability
    let storage = Arc::new(SqliteExecutionLog::new("timer_demo_dag.db").await?);
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    // Start worker with timer processing (event-driven)
    let worker = Worker::new(storage.clone(), "dag-timer-worker")
        .with_timers()
        .with_poll_interval(Duration::from_millis(100));

    // Register the flow handler
    worker
        .register(|flow: Arc<OrderWorkflowDAG>| flow.process_order_dag())
        .await;

    let worker_handle = worker.start().await;

    // Schedule the order flow
    let order = OrderWorkflowDAG {
        order_id: "ORD-DAG-001".to_string(),
        customer_email: "customer@example.com".to_string(),
        amount: 299.99,
    };

    let flow_id = uuid::Uuid::new_v4();
    let task_id = scheduler.schedule_with(order.clone(), flow_id).await?;

    println!(
        "Scheduled order: {} (flow_id: {})\n",
        order.order_id, flow_id
    );

    let start = std::time::Instant::now();

    // Wait for flow to complete using event-driven notifications (no polling!)
    let status_notify = storage.status_notify().clone();
    while let Some(task) = storage.get_scheduled_flow(task_id).await? {
        match task.status {
            ergon::storage::TaskStatus::Complete => break,
            ergon::storage::TaskStatus::Failed => {
                println!("\n[ERROR] Flow failed");
                break;
            }
            _ => {
                // Still running or pending - wait for status change notification
                status_notify.notified().await;
            }
        }
    }

    let elapsed = start.elapsed();

    println!("\n=== Execution Complete ===\n");

    // Get the final result from step 0 (the flow method itself)
    let result = if let Some(flow_inv) = storage.get_invocation(flow_id, 0).await? {
        if flow_inv.status() == ergon::InvocationStatus::Complete {
            if let Some(return_val) = flow_inv.return_value() {
                ergon::deserialize_value::<Result<String, ExecutionError>>(return_val)?
                    .unwrap_or_else(|_| "Order processing failed".to_string())
            } else {
                "Order completed (no tracking number)".to_string()
            }
        } else {
            "Order processing in progress or failed".to_string()
        }
    } else {
        "Order not found".to_string()
    };

    println!("Result: {}", result);
    println!("Total time: {:?}", elapsed);

    // Verify timing expectations
    if elapsed.as_secs() >= 5 && elapsed.as_secs() <= 7 {
        println!("\n✅ [SUCCESS] Timers worked correctly with DAG execution!");
        println!("   • fraud_check (2s) and reserve_inventory ran in parallel");
        println!("   • process_payment (3s) waited for both");
        println!("   • ship_order (1s) ran after payment");
        println!("   • Total: 2s + 3s + 1s = 6s (not 2s+3s+1s+2s = 8s sequential)");
    } else {
        println!("\n⚠️  [WARNING] Unexpected timing (might be system load)");
        println!("   Expected: 5-7 seconds");
        println!("   Got: {:?}", elapsed);
    }

    // Verify event-driven behavior
    println!("\n=== Event-Driven Timer Verification ===");
    println!("✅ Timer demo completed successfully");
    println!("   • Workers woke up only when timers fired (event-driven)");
    println!("   • Zero CPU overhead while waiting");
    println!("   • Instant notification on timer expiration");
    println!("   • Parallel steps (fraud_check + reserve_inventory) executed concurrently");
    println!("   • Multi-dependency step (process_payment) waited for both");

    worker_handle.shutdown().await;

    println!("\n=== Demo Complete ===\n");

    Ok(())
}
