//! Distributed worker example with timer processing AND structured tracing
//!
//! This example demonstrates:
//! - Structured tracing with the typestate pattern
//! - Distributed workers with both timers AND tracing enabled
//! - Rich span hierarchy for production debugging
//! - Span fields: worker.id, flow.id, flow.type, task.id
//! - Timer coordination visibility via spans
//!
//! ## What's New in This Example
//! - `.with_structured_tracing()` enables detailed spans
//! - Span hierarchy shows worker loops, timer processing, and flow execution
//! - All operations have structured context (worker_id, flow_id, task_id)
//! - Zero cost for workers without tracing (just remove `.with_structured_tracing()`)
//!
//! ## Scenario
//! - 3 order processing flows (each takes ~5 seconds with 2s + 3s timers)
//! - 2 trial expiry flows (each takes ~5 seconds with 5s timer)
//! - 2 workers processing flows in parallel WITH STRUCTURED TRACING
//! - Expected: ~6 seconds total (with parallelism) vs ~25 seconds (sequential)
//!
//! ## Expected Span Hierarchy
//! ```
//! worker_loop [worker.id="worker-1"]
//! ├── timer_processing [worker.id="worker-1", timers.found=2]
//! └── flow_execution [worker.id="worker-1", flow.id="...", flow.type="...", task.id="..."]
//!     └── (flow steps execute here with full context)
//! ```
//!
//! ## Key Takeaways
//! - Structured tracing is OPT-IN via `.with_structured_tracing()`
//! - Default workers have ZERO overhead (no span creation)
//! - Spans provide rich debugging context in production
//! - Combines seamlessly with timer processing
//! - Follows typestate pattern like timers
//!
//! ## Prerequisites
//! Start Redis:
//! ```bash
//! docker run -d -p 6379:6379 redis:latest
//! ```
//!
//! Run with: cargo run --example distributed_worker_timer_tracing --features=redis

use ergon::executor::{schedule_timer_named, Scheduler};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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
        info!("[{}] Starting order processing", self.order_id);

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

        info!("[{}] Order completed!", order_id);

        Ok(OrderResult {
            order_id,
            status: "Completed".to_string(),
        })
    }

    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<bool, String> {
        info!(
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
        info!("[{}] Waiting 2s for fraud check...", self.order_id);
        schedule_timer_named(
            Duration::from_secs(2),
            &format!("fraud-check-{}", self.order_id),
        )
        .await
        .map_err(|e| e.to_string())?;
        info!("[{}] Fraud check complete", self.order_id);
        Ok(())
    }

    #[step]
    async fn process_payment(self: Arc<Self>) -> Result<String, String> {
        info!("[{}] Processing payment of ${}", self.order_id, self.amount);
        Ok(format!("payment-{}", self.order_id))
    }

    #[step]
    async fn wait_for_warehouse(self: Arc<Self>) -> Result<(), String> {
        info!("[{}] Waiting 3s for warehouse processing...", self.order_id);
        schedule_timer_named(
            Duration::from_secs(3),
            &format!("warehouse-{}", self.order_id),
        )
        .await
        .map_err(|e| e.to_string())?;
        info!("[{}] Warehouse processing complete", self.order_id);
        Ok(())
    }

    #[step]
    async fn ship_order(self: Arc<Self>) -> Result<String, String> {
        info!("[{}] Shipping order", self.order_id);
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
        info!("[Trial {}] Starting trial expiry flow", self.user_id);

        // Wait for trial period (simulated with 5 second timer)
        self.clone().wait_for_trial_period().await?;

        // Send expiry notice
        let user_id = self.user_id.clone();
        self.send_notification().await?;

        info!("[Trial {}] Trial expiry flow completed!", user_id);

        Ok(format!("Trial expiry notice sent to {}", user_id))
    }

    #[step]
    async fn wait_for_trial_period(self: Arc<Self>) -> Result<(), String> {
        info!(
            "[Trial {}] Waiting 5s for trial period to expire...",
            self.user_id
        );
        schedule_timer_named(
            Duration::from_secs(5),
            &format!("trial-expiry-{}", self.user_id),
        )
        .await
        .map_err(|e| e.to_string())?;
        info!("[Trial {}] Trial period expired", self.user_id);
        Ok(())
    }

    #[step]
    async fn send_notification(self: Arc<Self>) -> Result<bool, String> {
        info!(
            "[Trial {}] Sending expiry notice to {}",
            self.user_id, self.email
        );
        Ok(true)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured tracing with span support
    // tracing_subscriber::registry()
    //     .with(
    //         tracing_subscriber::fmt::layer()
    //             .with_target(true)
    //             .with_thread_ids(true)
    //             .with_level(true)
    //             .with_span_events(
    //                 tracing_subscriber::fmt::format::FmtSpan::NEW
    //                     | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
    //             ),
    //     )
    //     .init();
    let level = std::env::var("LOG_LEVEL")
        .unwrap_or_else(|_| "info".to_string())
        .parse::<LevelFilter>()
        .unwrap_or(LevelFilter::INFO);

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        ))
        .with(level)
        .init();

    let redis_url = "redis://127.0.0.1:6379";
    let storage = Arc::new(RedisExecutionLog::new(redis_url).await?);
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    // Start worker 1 with BOTH timer processing AND structured tracing
    let worker1 = Worker::new(storage.clone(), "worker-1")
        .with_timers() // Enable timer processing
        .with_structured_tracing() // ← Enable structured spans
        .with_poll_interval(Duration::from_millis(110));

    worker1
        .register(move |flow: Arc<TimedOrderProcessor>| flow.process_order())
        .await;
    worker1
        .register(move |flow: Arc<TrialExpiryNotification>| flow.send_expiry_notice())
        .await;
    let handle1 = worker1.start().await;

    // Start worker 2 with BOTH timer processing AND structured tracing
    let worker2 = Worker::new(storage.clone(), "worker-2")
        .with_timers()
        .with_structured_tracing() // ← Enable structured spans
        .with_poll_interval(Duration::from_millis(110));

    worker2
        .register(move |flow: Arc<TimedOrderProcessor>| flow.process_order())
        .await;
    worker2
        .register(move |flow: Arc<TrialExpiryNotification>| flow.send_expiry_notice())
        .await;
    let handle2 = worker2.start().await;

    // Give both workers time to initialize and start polling
    tokio::time::sleep(Duration::from_millis(500)).await;

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
        let task_id = scheduler.schedule_with(order, flow_id).await?;
        flow_info.push((task_id, format!("Order ORD-{:03}", i)));
    }

    // Schedule trial expiry flows
    for i in 1..=2 {
        let trial = TrialExpiryNotification {
            user_id: format!("user-{}", i),
            email: format!("user{}@example.com", i),
        };

        let flow_id = Uuid::new_v4();
        let task_id = scheduler.schedule_with(trial, flow_id).await?;
        flow_info.push((task_id, format!("Trial expiry user-{}", i)));
    }

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
        Ok(_) => {}
        Err(_) => {
            let incomplete = storage.get_incomplete_flows().await?;
            warn!("Timeout - {} incomplete flows", incomplete.len());
        }
    }

    handle1.shutdown().await;
    handle2.shutdown().await;

    Ok(())
}
