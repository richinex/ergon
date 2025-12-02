//! Distributed worker example with timer processing
//!
//! This example demonstrates:
//! - Scheduling flows that use durable timers
//! - Multiple workers processing both flows and timers
//! - Timer coordination across distributed workers
//! - Unified worker shutdown (stops both flow and timer processing)
//! - Worker distribution tracking (which worker completed which flow)
//!
//! ## Scenario
//! - 3 order processing flows (each takes ~5 seconds with 2s + 3s timers)
//! - 2 trial expiry flows (each takes ~5 seconds with 5s timer)
//! - 2 workers processing flows in parallel
//! - Expected: ~6 seconds total (with parallelism) vs ~25 seconds (sequential)
//!
//! ## Key Takeaways
//! - Workers with `.with_timers()` process both flows AND timers
//! - Multiple workers coordinate timer firing (no duplicates via optimistic concurrency)
//! - Single `shutdown()` call stops both flow and timer processing
//! - Timers are durable and survive across worker coordination
//! - Framework tracks worker distribution via `ScheduledFlow.locked_by` field
//!
//! ## Prerequisites
//! Start Redis:
//! ```bash
//! docker run -d -p 6379:6379 redis:latest
//! ```
//!
//! Run with: cargo run --example distributed_worker_timer --features=redis

use ergon::executor::{schedule_timer_named, FlowScheduler};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

/// An order processing flow with timed delays
#[derive(Serialize, Deserialize, Clone, FlowType)]
struct TimedOrderProcessor {
    order_id: String,
    customer: String,
    amount: f64,
}

impl TimedOrderProcessor {
    /// Main flow that orchestrates order processing with timed delays
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, String> {
        println!("[{}] Starting order processing", self.order_id);

        // Validate the order
        self.clone().validate_order().await?;

        // Wait for fraud check (simulated with 2 second timer)
        self.clone().wait_for_fraud_check().await?;

        // Process payment
        self.clone().process_payment().await?;

        // Wait for warehouse processing (simulated with 3 second timer)
        self.clone().wait_for_warehouse().await?;

        // Ship the order (last step - no clone needed)
        let order_id = self.order_id.clone();
        self.ship_order().await?;

        println!("[{}] Order completed!", order_id);

        Ok(OrderResult {
            order_id,
            status: "Completed".to_string(),
        })
    }

    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<bool, String> {
        println!(
            "[{}] Validating order for {} (amount: ${})",
            self.order_id, self.customer, self.amount
        );

        if self.amount <= 0.0 {
            return Err(format!("Invalid amount: {}", self.amount));
        }

        Ok(true)
    }

    #[step]
    async fn wait_for_fraud_check(self: Arc<Self>) -> Result<(), String> {
        println!("[{}] Waiting 2s for fraud check...", self.order_id);
        schedule_timer_named(
            Duration::from_secs(2),
            &format!("fraud-check-{}", self.order_id),
        )
        .await
        .map_err(|e| e.to_string())?;
        println!("[{}] Fraud check complete", self.order_id);
        Ok(())
    }

    #[step]
    async fn process_payment(self: Arc<Self>) -> Result<String, String> {
        println!("[{}] Processing payment of ${}", self.order_id, self.amount);
        Ok(format!("payment-{}", self.order_id))
    }

    #[step]
    async fn wait_for_warehouse(self: Arc<Self>) -> Result<(), String> {
        println!("[{}] Waiting 3s for warehouse processing...", self.order_id);
        schedule_timer_named(
            Duration::from_secs(3),
            &format!("warehouse-{}", self.order_id),
        )
        .await
        .map_err(|e| e.to_string())?;
        println!("[{}] Warehouse processing complete", self.order_id);
        Ok(())
    }

    #[step]
    async fn ship_order(self: Arc<Self>) -> Result<String, String> {
        println!("[{}] Shipping order", self.order_id);
        Ok(format!("tracking-{}", self.order_id))
    }
}

#[derive(Serialize, Deserialize, Debug, FlowType)]
struct OrderResult {
    order_id: String,
    status: String,
}

/// A trial expiry notification flow with timer
#[derive(Serialize, Deserialize, Clone, FlowType)]
struct TrialExpiryNotification {
    user_id: String,
    email: String,
}

impl TrialExpiryNotification {
    #[flow]
    async fn send_expiry_notice(self: Arc<Self>) -> Result<String, String> {
        println!("[Trial {}] Starting trial expiry flow", self.user_id);

        // Wait for trial period (simulated with 5 second timer)
        self.clone().wait_for_trial_period().await?;

        // Send expiry notice
        let user_id = self.user_id.clone();
        self.send_notification().await?;

        println!("[Trial {}] Trial expiry flow completed!", user_id);

        Ok(format!("Trial expiry notice sent to {}", user_id))
    }

    #[step]
    async fn wait_for_trial_period(self: Arc<Self>) -> Result<(), String> {
        println!(
            "[Trial {}] Waiting 5s for trial period to expire...",
            self.user_id
        );
        schedule_timer_named(
            Duration::from_secs(5),
            &format!("trial-expiry-{}", self.user_id),
        )
        .await
        .map_err(|e| e.to_string())?;
        println!("[Trial {}] Trial period expired", self.user_id);
        Ok(())
    }

    #[step]
    async fn send_notification(self: Arc<Self>) -> Result<bool, String> {
        println!(
            "[Trial {}] Sending expiry notice to {}",
            self.user_id, self.email
        );
        Ok(true)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nDistributed Worker with Timers Example");
    println!("=======================================\n");

    let redis_url = "redis://127.0.0.1:6379";
    let storage = Arc::new(RedisExecutionLog::new(redis_url)?);
    storage.reset().await?;

    let scheduler = FlowScheduler::new(storage.clone());

    println!("Starting workers with timer processing...\n");

    // Start worker 1 with timer processing enabled
    let worker1 = FlowWorker::new(storage.clone(), "worker-1")
        .with_timers() // Enable timer processing
        .with_timer_interval(Duration::from_millis(100)) // Check timers frequently
        .with_poll_interval(Duration::from_millis(90)); // Slightly faster polling

    worker1
        .register(move |flow: Arc<TimedOrderProcessor>| flow.process_order())
        .await;
    worker1
        .register(move |flow: Arc<TrialExpiryNotification>| flow.send_expiry_notice())
        .await;
    let handle1 = worker1.start().await;
    println!("  Started worker-1");

    // Start worker 2 with timer processing enabled (staggered poll interval)
    let worker2 = FlowWorker::new(storage.clone(), "worker-2")
        .with_timers()
        .with_timer_interval(Duration::from_millis(100))
        .with_poll_interval(Duration::from_millis(110)); // Slightly slower to stagger

    worker2
        .register(move |flow: Arc<TimedOrderProcessor>| flow.process_order())
        .await;
    worker2
        .register(move |flow: Arc<TrialExpiryNotification>| flow.send_expiry_notice())
        .await;
    let handle2 = worker2.start().await;
    println!("  Started worker-2\n");

    // Give both workers time to initialize and start polling
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("Scheduling flows...\n");

    // Track task IDs and their descriptions for worker distribution analysis
    let mut flow_info: Vec<(Uuid, String)> = Vec::new();

    // Schedule order processing flows
    for i in 1..=3 {
        let order = TimedOrderProcessor {
            order_id: format!("ORD-{:03}", i),
            customer: format!("Customer {}", i),
            amount: 100.0 * i as f64,
        };

        let flow_id = Uuid::new_v4();
        let task_id = scheduler.schedule(order, flow_id).await?;
        flow_info.push((task_id, format!("Order ORD-{:03}", i)));
        println!("  Scheduled order ORD-{:03}", i);
    }

    // Schedule trial expiry flows
    for i in 1..=2 {
        let trial = TrialExpiryNotification {
            user_id: format!("user-{}", i),
            email: format!("user{}@example.com", i),
        };

        let flow_id = Uuid::new_v4();
        let task_id = scheduler.schedule(trial, flow_id).await?;
        flow_info.push((task_id, format!("Trial expiry user-{}", i)));
        println!("  Scheduled trial expiry for user-{}", i);
    }

    println!("\nWorkers processing flows and timers...\n");

    // Give workers time to pick up flows from queue
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Wait for all flows to complete with timeout
    let timeout_duration = Duration::from_secs(30);
    match tokio::time::timeout(timeout_duration, async {
        loop {
            let incomplete = storage.get_incomplete_flows().await?;
            if incomplete.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await
    {
        Ok(_) => println!("All flows completed!\n"),
        Err(_) => {
            println!("Timeout waiting for flows to complete\n");
            let incomplete = storage.get_incomplete_flows().await?;
            println!("Incomplete flows: {}", incomplete.len());
            for inv in incomplete {
                println!("  - {} ({})", inv.id(), inv.class_name());
            }
        }
    }
    println!("Shutting down workers...\n");

    handle1.shutdown().await;
    println!("  Worker-1 stopped");

    handle2.shutdown().await;
    println!("  Worker-2 stopped");

    println!("\nVerifying results...\n");

    let incomplete = storage.get_incomplete_flows().await?;
    if incomplete.is_empty() {
        println!("  All flows completed successfully");
    } else {
        println!("  {} flows incomplete", incomplete.len());
        for inv in incomplete {
            println!("    - {} ({})", inv.id(), inv.class_name());
        }
    }

    println!("\nWorker Distribution:\n");

    // Show which worker completed which flow
    for (task_id, description) in flow_info {
        if let Some(scheduled_flow) = storage.get_scheduled_flow(task_id).await? {
            let worker = scheduled_flow.locked_by.as_deref().unwrap_or("unknown");
            println!("  {} -> {}", description, worker);
        }
    }

    Ok(())
}
