//! Distributed worker example showing scheduler and workers
//!
//! This example demonstrates:
//! - Scheduling flows for distributed execution
//! - Running multiple workers that poll and execute flows
//! - Parallel processing of flows across workers
//! - Worker coordination and load distribution
//! - Graceful worker shutdown
//!
//! ## Scenario
//! Five order processing flows are scheduled to a shared queue. Two workers
//! (worker-1 and worker-2) poll the queue and process flows in parallel.
//! Each worker picks up flows independently, processes them, and reports
//! completion. The system tracks which worker processed which flow.
//!
//! ## Key Takeaways
//! - FlowScheduler enqueues flows for distributed execution
//! - Multiple FlowWorker instances poll the same queue
//! - Workers coordinate via optimistic concurrency control
//! - Each flow is processed by exactly one worker
//! - Workers can be started and stopped independently
//! - Load is automatically distributed across available workers
//! - Framework handles worker coordination transparently
//!
//! ## Run with
//! ```bash
//! cargo run --example distributed_worker
//! ```

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

/// An order processing flow that validates and processes orders
#[derive(Serialize, Deserialize, Clone, FlowType)]
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

#[derive(Serialize, Deserialize, Debug, FlowType)]
struct OrderResult {
    order_id: String,
    status: String,
}

/// A simple notification flow
#[derive(Serialize, Deserialize, Clone, FlowType)]
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
    // Create shared storage
    let storage = Arc::new(SqliteExecutionLog::new("distributed_demo.db").await?);
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    // Schedule some order processing flows
    for i in 1..=3 {
        let order = OrderProcessor {
            order_id: format!("ORD-{:03}", i),
            customer: format!("Customer {}", i),
            amount: 100.0 * i as f64,
        };

        let flow_id = Uuid::new_v4();
        let _task_id = scheduler.schedule_with(order, flow_id).await?;
    }

    // Schedule some notification flows
    for i in 1..=2 {
        let notification = NotificationSender {
            recipient: format!("user{}@example.com", i),
            message: "Your order has been confirmed!".to_string(),
        };

        let flow_id = Uuid::new_v4();
        let _task_id = scheduler.schedule_with(notification, flow_id).await?;
    }

    // Start worker 1 (handles both flow types)
    let worker1 =
        Worker::new(storage.clone(), "worker-1").with_poll_interval(Duration::from_millis(100));
    worker1
        .register(|flow: Arc<OrderProcessor>| flow.process_order())
        .await;
    worker1
        .register(|flow: Arc<NotificationSender>| flow.send())
        .await;
    let handle1 = worker1.start().await;

    // Start worker 2 (handles both flow types)
    let worker2 =
        Worker::new(storage.clone(), "worker-2").with_poll_interval(Duration::from_millis(100));
    worker2
        .register(|flow: Arc<OrderProcessor>| flow.process_order())
        .await;
    worker2
        .register(|flow: Arc<NotificationSender>| flow.send())
        .await;
    let handle2 = worker2.start().await;

    // Let workers process the flows
    tokio::time::sleep(Duration::from_secs(3)).await;

    handle1.shutdown().await;
    handle2.shutdown().await;

    // Check flow execution logs
    let incomplete = storage.get_incomplete_flows().await?;
    if incomplete.is_empty() {
        println!("All flows completed successfully");
    } else {
        println!("{} flows incomplete", incomplete.len());
    }

    storage.close().await?;
    Ok(())
}
