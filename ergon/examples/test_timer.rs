use ergon::executor::{schedule_timer_named, ExecutionError, Scheduler, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

// =============================================================================
// Domain Types
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PaymentInfo {
    amount: f64,
    currency: String,
    payment_method: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderResult {
    order_id: String,
    status: String,
    total_amount: f64,
    processing_time_ms: u64,
}

// =============================================================================
// Order Flow with Timers
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct TimerOrderFlow {
    order_id: String,
    customer_id: String,
    amount: f64,
}

impl TimerOrderFlow {
    /// Step 1: Validate the order
    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("   [{}] Validating order...", self.order_id);
        sleep(Duration::from_millis(200)).await;
        println!("   [{}] ✓ Order validated", self.order_id);
        Ok(format!("Validated order for customer {}", self.customer_id))
    }

    /// Step 2: Wait for fraud check (2 second timer)
    #[step]
    async fn wait_fraud_check(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!(
            "   [{}] Starting fraud check - waiting 2 seconds...",
            self.order_id
        );
        schedule_timer_named(Duration::from_secs(2), "fraud-check-delay").await?;
        Ok(())
    }

    /// Step 3: Process fraud check result
    #[step]
    async fn process_fraud_check(self: Arc<Self>) -> Result<bool, ExecutionError> {
        println!("   [{}] Fraud check complete", self.order_id);
        sleep(Duration::from_millis(100)).await;
        println!("   [{}] ✓ No fraud detected", self.order_id);
        Ok(true)
    }

    /// Step 4: Reserve inventory
    #[step]
    async fn reserve_inventory(self: Arc<Self>) -> Result<bool, ExecutionError> {
        println!("   [{}] Reserving inventory...", self.order_id);
        sleep(Duration::from_millis(300)).await;
        println!("   [{}] ✓ Inventory reserved", self.order_id);
        Ok(true)
    }

    /// Step 5: Wait for payment processing (3 second timer)
    #[step]
    async fn wait_payment_processing(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!(
            "   [{}] Initiating payment - waiting 3 seconds...",
            self.order_id
        );
        schedule_timer_named(Duration::from_secs(3), "payment-processing").await?;
        Ok(())
    }

    /// Step 6: Confirm payment
    #[step]
    async fn confirm_payment(self: Arc<Self>) -> Result<PaymentInfo, ExecutionError> {
        println!("   [{}] Payment complete", self.order_id);
        sleep(Duration::from_millis(150)).await;
        println!("   [{}] ✓ Payment confirmed", self.order_id);
        Ok(PaymentInfo {
            amount: self.amount,
            currency: "USD".to_string(),
            payment_method: "credit_card".to_string(),
        })
    }

    /// Step 7: Wait before shipping (1 second timer)
    #[step]
    async fn wait_shipping_prep(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!(
            "   [{}] Preparing shipment - waiting 1 second...",
            self.order_id
        );
        schedule_timer_named(Duration::from_secs(1), "shipping-prep").await?;
        Ok(())
    }

    /// Step 8: Ship order
    #[step]
    async fn ship_order(self: Arc<Self>, payment: PaymentInfo) -> Result<String, ExecutionError> {
        println!("   [{}] Shipping order", self.order_id);
        sleep(Duration::from_millis(200)).await;
        println!(
            "   [{}] ✓ Order shipped (paid ${:.2})",
            self.order_id, payment.amount
        );

        // Generate tracking number deterministically from order_id
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.order_id.hash(&mut hasher);
        let hash = hasher.finish();
        Ok(format!("TRACK-{:08X}", (hash % 0xFFFFFFFF) as u32))
    }

    /// Main flow with timer-based steps (sequential execution)
    #[flow]
    async fn process_with_timers(self: Arc<Self>) -> Result<OrderResult, ExecutionError> {
        println!(
            "\nORDER [{}] Starting timer-based processing",
            self.order_id
        );
        let start = std::time::Instant::now();

        // Execute steps sequentially (timers work best with sequential flows)
        self.clone().validate_order().await?;
        self.clone().wait_fraud_check().await?;
        self.clone().process_fraud_check().await?;
        self.clone().reserve_inventory().await?;
        self.clone().wait_payment_processing().await?;
        let payment = self.clone().confirm_payment().await?;
        self.clone().wait_shipping_prep().await?;
        self.clone().ship_order(payment).await?;

        let elapsed = start.elapsed();
        println!("ORDER [{}] Complete in {:?}!", self.order_id, elapsed);

        Ok(OrderResult {
            order_id: self.order_id.clone(),
            status: "shipped".to_string(),
            total_amount: self.amount,
            processing_time_ms: elapsed.as_millis() as u64,
        })
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let storage = Arc::new(ergon::storage::SqliteExecutionLog::new("test_timer.db").await?);
    storage.reset().await?;

    println!("\nErgon Timer Test");
    println!("   This test shows:");
    println!("   - Durable timers in workflow steps");
    println!("   - Multiple timed delays (2s, 3s, 1s)");
    println!("   - Timer status in execution timeline");
    println!("   - Flow suspension and resumption");
    println!("\n   Total expected time: ~6 seconds per order\n");

    sleep(Duration::from_secs(1)).await;

    // Worker with timer processing enabled
    let worker = Worker::new(storage.clone(), "timer-worker")
        .with_timers()
        .with_poll_interval(Duration::from_millis(500));

    worker
        .register(|f: Arc<TimerOrderFlow>| f.process_with_timers())
        .await;

    let _handle = worker.start().await;

    println!("Worker started with timer processing\n");

    sleep(Duration::from_secs(1)).await;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    println!("Scheduling timer-based orders...\n");

    let orders = vec![
        TimerOrderFlow {
            order_id: "ORD-T001".to_string(),
            customer_id: "CUST-001".to_string(),
            amount: 299.99,
        },
        TimerOrderFlow {
            order_id: "ORD-T002".to_string(),
            customer_id: "CUST-002".to_string(),
            amount: 149.99,
        },
    ];

    let mut task_ids = Vec::new();
    for order in orders {
        let id = Uuid::new_v4();
        let task_id = scheduler.schedule_with(order.clone(), id).await?;
        task_ids.push(task_id);
        println!(
            "   Scheduled: {} (${}) - flow_id: {}",
            order.order_id, order.amount, id
        );
        sleep(Duration::from_millis(200)).await;
    }

    println!("\n✅ Orders scheduled!");
    println!("   • See timer execution in console output");
    println!("   • Watch automatic resumption after timers fire");
    println!("   • Each order takes ~6 seconds total\n");

    // Event-driven completion waiting (no polling!)
    let status_notify = storage.status_notify().clone();
    let timeout_duration = Duration::from_secs(30);
    tokio::time::timeout(timeout_duration, async {
        loop {
            let mut all_complete = true;
            for &task_id in &task_ids {
                if let Some(scheduled) = storage.get_scheduled_flow(task_id).await? {
                    if !matches!(
                        scheduled.status,
                        ergon::storage::TaskStatus::Complete | ergon::storage::TaskStatus::Failed
                    ) {
                        all_complete = false;
                        break;
                    }
                }
            }
            if all_complete {
                break;
            }
            // Wait for status change notification instead of polling
            status_notify.notified().await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await
    .ok();

    println!("\n⏳ All orders complete!\n");
    Ok(())
}
