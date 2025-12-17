//! Durable timer demo
//!
//! This example demonstrates:
//! - Basic timer usage with `schedule_timer()` and `schedule_timer_named()`
//! - Named timers for debugging and visibility
//! - Timer survival across process restarts (durability)
//! - Timer processor running in background with workers
//! - Multiple timers in a single flow (sequential delays)
//! - Worker configuration with `.with_timers()` method
//!
//! ## Scenario
//! Order processing workflow with timed delays:
//! - Create order immediately
//! - Wait 2 seconds (fraud check/validation period)
//! - Process payment
//! - Wait 3 seconds (warehouse processing)
//! - Ship order
//! - Wait 1 second (confirmation delay)
//! - Send confirmation email
//! - Total execution time: ~6 seconds with timers
//!
//! ## Key Takeaways
//! - Timers are durable and survive process restarts
//! - Workers need `.with_timers()` to process timer events
//! - Named timers help with debugging and observability
//! - Timers fire asynchronously in background
//! - Multiple timers can be used in a single flow
//! - Framework handles timer scheduling and firing automatically
//!
//! ## Prerequisites
//! None - uses in-memory storage by default
//!
//! ## Run with
//! ```bash
//! cargo run --example timer_demo
//! ```

use chrono::Utc;
use ergon::executor::{schedule_timer, schedule_timer_named, ExecutionError, Scheduler, Worker};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderWorkflow {
    order_id: String,
    customer_email: String,
}

impl OrderWorkflow {
    /// Process an order with timed delays
    ///
    /// Timeline:
    /// - Immediate: Create order
    /// - Wait 2 seconds: Process payment
    /// - Wait 3 seconds: Ship order
    /// - Wait 1 second: Send confirmation
    ///
    /// NOTE: All println! statements are inside #[step] functions so they
    /// execute only ONCE due to step caching, proving that timer steps
    /// don't re-execute after firing.
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<String, ExecutionError> {
        // Step 1: Create order
        self.clone().create_order().await?;

        // Step 2: Wait before processing (simulates order validation period)
        self.clone().wait_for_processing().await?;

        // Step 3: Process payment
        self.clone().process_payment().await?;

        // Step 4: Wait before shipping (simulates warehouse processing)
        self.clone().wait_for_shipping().await?;

        // Step 5: Ship order
        self.clone().ship_order().await?;

        // Step 6: Wait before confirmation (simulates delivery tracking)
        self.clone().wait_for_confirmation().await?;

        // Step 7: Send confirmation
        self.clone().send_confirmation().await?;

        // Step 8: Complete order
        self.clone().complete_order().await?;

        Ok(format!("Order {} completed successfully", self.order_id))
    }

    #[step]
    async fn create_order(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("\n[{}] Order {} created", format_time(), self.order_id);
        Ok(())
    }

    #[step]
    async fn wait_for_processing(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}] Waiting 2 seconds before payment...", format_time());
        schedule_timer_named(Duration::from_secs(2), "payment-delay").await?;
        Ok(())
    }

    #[step]
    async fn process_payment(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!(
            "[{}] Processing payment for {}",
            format_time(),
            self.order_id
        );
        Ok(())
    }

    #[step]
    async fn wait_for_shipping(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}] Waiting 3 seconds before shipping...", format_time());
        schedule_timer_named(Duration::from_secs(3), "shipping-delay").await?;
        Ok(())
    }

    #[step]
    async fn ship_order(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}] Shipping order {}", format_time(), self.order_id);
        Ok(())
    }

    #[step]
    async fn wait_for_confirmation(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!(
            "[{}] Waiting 1 second before confirmation...",
            format_time()
        );
        schedule_timer(Duration::from_secs(1)).await?;
        Ok(())
    }

    #[step]
    async fn send_confirmation(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!(
            "[{}] Sending confirmation to {}",
            format_time(),
            self.customer_email
        );
        Ok(())
    }

    #[step]
    async fn complete_order(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}] Order {} completed!", format_time(), self.order_id);
        Ok(())
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

    // Create SQLite storage for durability
    let storage = Arc::new(SqliteExecutionLog::new("timer_demo.db").await?);
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone());

    // Start worker with timer processing (polls every 100ms for demo responsiveness)
    let worker = Worker::new(storage.clone(), "timer-demo-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(100))
        .with_poll_interval(Duration::from_millis(100));

    // Register the flow handler
    worker
        .register(|flow: Arc<OrderWorkflow>| flow.process_order())
        .await;

    let worker_handle = worker.start().await;

    // Schedule the order flow
    let order = OrderWorkflow {
        order_id: "ORD-12345".to_string(),
        customer_email: "customer@example.com".to_string(),
    };

    let flow_id = uuid::Uuid::new_v4();
    let _task_id = scheduler.schedule(order, flow_id).await?;

    let start = std::time::Instant::now();

    // Wait for flow to complete using event-driven notifications (no polling!)
    let status_notify = storage.status_notify().clone();
    while let Some(task) = storage.get_scheduled_flow(_task_id).await? {
        match task.status {
            ergon::storage::TaskStatus::Complete => break,
            ergon::storage::TaskStatus::Failed => {
                println!("\n[WARNING] Flow failed");
                break;
            }
            _ => {
                // Still running or pending - wait for status change notification
                status_notify.notified().await;
            }
        }
    }

    let elapsed = start.elapsed();

    // Get the flow result from step 0
    let result = if let Some(flow_inv) = storage.get_invocation(flow_id, 0).await? {
        if flow_inv.status() == ergon::InvocationStatus::Complete {
            String::from("Order completed successfully")
        } else {
            String::from("Order processing in progress or failed")
        }
    } else {
        String::from("Order not found")
    };

    println!("Result: {}", result);
    println!("Total time: {:?}", elapsed);

    if elapsed.as_secs() >= 5 && elapsed.as_secs() <= 7 {
        println!("[OK] Timers worked correctly!");
    } else {
        println!("[WARNING] Unexpected timing (might be system load)");
    }

    worker_handle.shutdown().await;

    Ok(())
}
