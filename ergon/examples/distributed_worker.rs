//! Distributed worker example showing scheduler and workers
//!
//! This example demonstrates:
//! - Scheduling flows for distributed execution
//! - Running workers that pick up and execute flows
//! - Multiple workers processing flows in parallel
//!
//! Run with: cargo run --example distributed_worker

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

/// An order processing flow that validates and processes orders
#[derive(Serialize, Deserialize, Clone)]
struct OrderProcessor {
    order_id: String,
    customer: String,
    amount: f64,
}

impl OrderProcessor {
    /// Main flow that orchestrates order processing
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, String> {
        println!("[Flow {}] Starting order processing", self.order_id);

        // Validate the order
        self.clone().validate_order().await?;

        // Process payment
        self.clone().process_payment().await?;

        // Ship the order
        let order_id = self.order_id.clone();
        self.ship_order().await?;

        Ok(OrderResult {
            order_id,
            status: "Completed".to_string(),
        })
    }

    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<bool, String> {
        println!(
            "[Step] Validating order {} for customer {}",
            self.order_id, self.customer
        );
        tokio::time::sleep(Duration::from_millis(100)).await;

        if self.amount <= 0.0 {
            return Err(format!("Invalid amount: {}", self.amount));
        }

        Ok(true)
    }

    #[step]
    async fn process_payment(self: Arc<Self>) -> Result<String, String> {
        println!("[Step] Processing payment of ${}", self.amount);
        tokio::time::sleep(Duration::from_millis(150)).await;

        Ok(format!("payment-{}", self.order_id))
    }

    #[step]
    async fn ship_order(self: Arc<Self>) -> Result<String, String> {
        println!("[Step] Shipping order {}", self.order_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(format!("tracking-{}", self.order_id))
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct OrderResult {
    order_id: String,
    status: String,
}

/// A simple notification flow
#[derive(Serialize, Deserialize, Clone)]
struct NotificationSender {
    recipient: String,
    message: String,
}

impl NotificationSender {
    #[flow]
    async fn send(self: Arc<Self>) -> Result<String, String> {
        println!("[Flow] Sending notification to {}", self.recipient);

        self.clone().format_message().await?;
        let recipient = self.recipient.clone();
        self.deliver().await?;

        Ok(format!("Sent to {}", recipient))
    }

    #[step]
    async fn format_message(self: Arc<Self>) -> Result<String, String> {
        println!("[Step] Formatting message for {}", self.recipient);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(format!("Dear {}: {}", self.recipient, self.message))
    }

    #[step]
    async fn deliver(self: Arc<Self>) -> Result<bool, String> {
        println!("[Step] Delivering notification to {}", self.recipient);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(true)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Ergon Distributed Worker Example ===\n");

    // Create shared storage
    let storage = Arc::new(SqliteExecutionLog::new("distributed_demo.db")?);
    storage.reset().await?;

    println!("1. Creating scheduler...");
    let scheduler = FlowScheduler::new(storage.clone());

    println!("2. Scheduling flows for distributed execution...\n");

    // Schedule some order processing flows
    for i in 1..=3 {
        let order = OrderProcessor {
            order_id: format!("ORD-{:03}", i),
            customer: format!("Customer {}", i),
            amount: 100.0 * i as f64,
        };

        let flow_id = Uuid::new_v4();
        let task_id = scheduler.schedule(order, flow_id).await?;
        println!(
            "   Scheduled order ORD-{:03} (task_id: {})",
            i,
            task_id.to_string().split('-').next().unwrap_or("")
        );
    }

    // Schedule some notification flows
    for i in 1..=2 {
        let notification = NotificationSender {
            recipient: format!("user{}@example.com", i),
            message: "Your order has been confirmed!".to_string(),
        };

        let flow_id = Uuid::new_v4();
        let task_id = scheduler.schedule(notification, flow_id).await?;
        println!(
            "   Scheduled notification to user{} (task_id: {})",
            i,
            task_id.to_string().split('-').next().unwrap_or("")
        );
    }

    println!("\n3. Starting distributed workers...\n");

    // Start worker 1 (handles both flow types)
    let worker1 =
        FlowWorker::new(storage.clone(), "worker-1").with_poll_interval(Duration::from_millis(100));
    worker1
        .register(|flow: Arc<OrderProcessor>| flow.process_order())
        .await;
    worker1
        .register(|flow: Arc<NotificationSender>| flow.send())
        .await;
    let handle1 = worker1.start().await;
    println!("   Started worker-1");

    // Start worker 2 (handles both flow types)
    let worker2 =
        FlowWorker::new(storage.clone(), "worker-2").with_poll_interval(Duration::from_millis(100));
    worker2
        .register(|flow: Arc<OrderProcessor>| flow.process_order())
        .await;
    worker2
        .register(|flow: Arc<NotificationSender>| flow.send())
        .await;
    let handle2 = worker2.start().await;
    println!("   Started worker-2\n");

    println!("4. Workers processing flows...\n");

    // Let workers process the flows
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("\n5. Shutting down workers...\n");

    handle1.shutdown().await;
    handle2.shutdown().await;

    println!("   All workers stopped");

    println!("\n6. Verifying results...\n");

    // Check flow execution logs
    let incomplete = storage.get_incomplete_flows().await?;
    if incomplete.is_empty() {
        println!("   ✓ All flows completed successfully!");
    } else {
        println!("   ⚠ {} flows incomplete", incomplete.len());
    }

    println!("\n=== Example Complete ===");

    storage.close().await?;
    Ok(())
}
