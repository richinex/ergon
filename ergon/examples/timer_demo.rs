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
use ergon::executor::{schedule_timer, schedule_timer_named, FlowWorker};
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
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<String, String> {
        println!("\n[{}] Order {} created", format_time(), self.order_id);

        // Step 1: Wait before processing (simulates order validation period)
        self.clone().wait_for_processing().await?;

        println!(
            "[{}] Processing payment for {}",
            format_time(),
            self.order_id
        );
        self.clone().process_payment().await?;

        // Step 2: Wait before shipping (simulates warehouse processing)
        self.clone().wait_for_shipping().await?;

        println!("[{}] Shipping order {}", format_time(), self.order_id);
        self.clone().ship_order().await?;

        // Step 3: Wait before confirmation (simulates delivery tracking)
        self.clone().wait_for_confirmation().await?;

        println!(
            "[{}] Sending confirmation to {}",
            format_time(),
            self.customer_email
        );
        self.clone().send_confirmation().await?;

        println!("[{}] Order {} completed!", format_time(), self.order_id);
        Ok(format!("Order {} completed successfully", self.order_id))
    }

    #[step]
    async fn wait_for_processing(self: Arc<Self>) -> Result<(), String> {
        println!("[{}] Waiting 2 seconds before payment...", format_time());
        schedule_timer_named(Duration::from_secs(2), "payment-delay")
            .await
            .map_err(|e| e.to_string())?;
        println!("[{}] Payment delay timer fired!", format_time());
        Ok(())
    }

    #[step]
    async fn process_payment(self: Arc<Self>) -> Result<(), String> {
        // Simulate payment processing
        Ok(())
    }

    #[step]
    async fn wait_for_shipping(self: Arc<Self>) -> Result<(), String> {
        println!("[{}] Waiting 3 seconds before shipping...", format_time());
        schedule_timer_named(Duration::from_secs(3), "shipping-delay")
            .await
            .map_err(|e| e.to_string())?;
        println!("[{}] Shipping delay timer fired!", format_time());
        Ok(())
    }

    #[step]
    async fn ship_order(self: Arc<Self>) -> Result<(), String> {
        // Simulate shipping
        Ok(())
    }

    #[step]
    async fn wait_for_confirmation(self: Arc<Self>) -> Result<(), String> {
        println!(
            "[{}] Waiting 1 second before confirmation...",
            format_time()
        );
        schedule_timer(Duration::from_secs(1))
            .await
            .map_err(|e| e.to_string())?;
        println!("[{}] Confirmation delay timer fired!", format_time());
        Ok(())
    }

    #[step]
    async fn send_confirmation(self: Arc<Self>) -> Result<(), String> {
        // Simulate sending email
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

    println!("=== Ergon Durable Timer Demo ===\n");
    println!("This demo shows timers that:");
    println!("  - Pause workflow execution for specified durations");
    println!("  - Survive process crashes");
    println!("  - Work across distributed workers");
    println!("  - Fire exactly once via optimistic concurrency\n");

    // Create SQLite storage for durability
    let storage = Arc::new(SqliteExecutionLog::new("timer_demo.db")?);

    // Start worker with timer processing (polls every 100ms for demo responsiveness)
    let worker = FlowWorker::new(storage.clone(), "timer-demo-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(100))
        .start()
        .await;

    println!("[{}] Worker with timer processing started\n", format_time());

    // Create and execute an order with timers
    let order = OrderWorkflow {
        order_id: "ORD-12345".to_string(),
        customer_email: "customer@example.com".to_string(),
    };

    let flow_id = uuid::Uuid::new_v4();
    let instance = FlowInstance::new(flow_id, order, storage.clone());

    println!("Total expected duration: ~6 seconds (2s + 3s + 1s)\n");
    println!("Starting order processing...\n");

    let start = std::time::Instant::now();

    // Execute the flow
    let result = instance
        .executor()
        .execute(|f| Box::pin(Arc::new(f.clone()).process_order()))
        .await?;

    let elapsed = start.elapsed();

    println!("\n=== Results ===");
    match result {
        Ok(msg) => println!("Result: {}", msg),
        Err(e) => println!("Error: {}", e),
    }
    println!("Total time: {:?}", elapsed);
    println!("Expected: ~6 seconds");

    if elapsed.as_secs() >= 5 && elapsed.as_secs() <= 7 {
        println!("[OK] Timers worked correctly!");
    } else {
        println!("[WARNING] Unexpected timing (might be system load)");
    }

    // Gracefully shutdown the worker
    println!("\n[{}] Shutting down worker...", format_time());
    worker.shutdown().await;

    println!("\n=== Demo Complete ===");
    println!("Check timer_demo.db to see persisted timer state");

    Ok(())
}
