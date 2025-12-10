//! Child Flow Invocation Example - Function Tree Pattern with Signals
//!
//! This example demonstrates how to achieve function tree semantics (like Temporal's
//! child workflows or Restate's nested functions) using Ergon's signal primitives.
//!
//! ## Pattern: Parent Flow → Child Flow Communication
//!
//! 1. Parent flow starts and suspends waiting for child completion signals
//! 2. Orchestrator schedules child flows with parent's flow_id
//! 3. Children execute independently (can crash/retry without replaying parent)
//! 4. Children complete and signal parent with results
//! 5. Parent resumes and continues with child results
//!
//! ## Function Tree Properties Achieved
//!
//! - **Independent Execution**: Child has separate flow_id, own retry policy
//! - **Suspension**: Parent terminates (zero resources) while waiting
//! - **Fault Isolation**: Child failures don't trigger parent replay
//! - **Independent Retry**: Child retries N times before failing to parent
//! - **Type-Safe Results**: Signal data is serialized/deserialized with types
//!
//! ## Scenario
//!
//! An e-commerce order processing system with three independent services:
//! - ParentFlow: Orchestrates the entire order
//! - InventoryService: Validates stock availability (2 second delay)
//! - PaymentService: Processes payment (3 second delay)
//! - ShippingService: Creates shipping label (1 second delay)
//!
//! Each service is a separate flow that can fail/retry independently.
//!
//! ## Run
//!
//! ```bash
//! cargo run --example child_flow_invocation --features=sqlite
//! ```
//!
//! ## Key Takeaways
//!
//! - Ergon has function tree semantics via signals (remote-context side effects)
//! - No new infrastructure needed - signals are already distributed and durable
//! - More flexible than implicit function trees (can signal across any flows)
//! - Simple to understand - explicit schedule + await pattern

use chrono::Utc;
use ergon::executor::{Scheduler, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Domain Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InventoryResult {
    available: bool,
    warehouse: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentResult {
    transaction_id: String,
    charged_amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShippingResult {
    tracking_number: String,
    estimated_delivery: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderResult {
    order_id: String,
    status: String,
    tracking_number: String,
}

// =============================================================================
// Parent Flow - Order Orchestrator
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderFlow {
    order_id: String,
    product_id: String,
    customer_id: String,
    amount: f64,
}

impl OrderFlow {
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, ExecutionError> {
        let parent_flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "\n[{}] PARENT[{}]: Starting order {} processing",
            format_time(),
            parent_flow_id,
            self.order_id
        );

        // Step 1: Wait for inventory check (child flow will signal us)
        println!(
            "[{}] PARENT[{}]: Waiting for inventory service...",
            format_time(),
            parent_flow_id
        );
        let inventory = self.clone().await_inventory_check().await?;
        println!(
            "[{}] PARENT[{}]: Inventory check completed: available={}, warehouse={}",
            format_time(),
            parent_flow_id,
            inventory.available,
            inventory.warehouse
        );

        if !inventory.available {
            return Err(ExecutionError::Failed("Product not in stock".to_string()));
        }

        // Step 2: Wait for payment processing (child flow will signal us)
        println!(
            "[{}] PARENT[{}]: Waiting for payment service...",
            format_time(),
            parent_flow_id
        );
        let payment = self.clone().await_payment_processing().await?;
        println!(
            "[{}] PARENT[{}]: Payment completed: tx_id={}, amount=${}",
            format_time(),
            parent_flow_id,
            payment.transaction_id,
            payment.charged_amount
        );

        // Step 3: Wait for shipping (child flow will signal us)
        println!(
            "[{}] PARENT[{}]: Waiting for shipping service...",
            format_time(),
            parent_flow_id
        );
        let shipping = self.clone().await_shipping().await?;
        println!(
            "[{}] PARENT[{}]: Shipping created: tracking={}",
            format_time(),
            parent_flow_id,
            shipping.tracking_number
        );

        let result = OrderResult {
            order_id: self.order_id.clone(),
            status: "Completed".to_string(),
            tracking_number: shipping.tracking_number,
        };

        println!(
            "\n[{}] PARENT[{}]: Order {} completed successfully!",
            format_time(),
            parent_flow_id,
            self.order_id
        );

        Ok(result)
    }

    #[step]
    async fn await_inventory_check(self: Arc<Self>) -> Result<InventoryResult, ExecutionError> {
        // Suspend waiting for child to signal completion
        let result: InventoryResult = await_external_signal("inventory-check").await?;
        Ok(result)
    }

    #[step]
    async fn await_payment_processing(self: Arc<Self>) -> Result<PaymentResult, ExecutionError> {
        // Suspend waiting for child to signal completion
        let result: PaymentResult = await_external_signal("payment-processing").await?;
        Ok(result)
    }

    #[step]
    async fn await_shipping(self: Arc<Self>) -> Result<ShippingResult, ExecutionError> {
        // Suspend waiting for child to signal completion
        let result: ShippingResult = await_external_signal("shipping-creation").await?;
        Ok(result)
    }
}

// =============================================================================
// Child Flow 1 - Inventory Service
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct InventoryCheckFlow {
    parent_flow_id: Uuid,
    product_id: String,
}

impl InventoryCheckFlow {
    #[flow]
    async fn check_inventory(self: Arc<Self>) -> Result<InventoryResult, ExecutionError> {
        let child_flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{}]   CHILD[inventory-{}]: Checking inventory for product {}",
            format_time(),
            child_flow_id,
            self.product_id
        );

        // Simulate slow inventory check
        self.clone().query_warehouse().await?;

        let result = InventoryResult {
            available: true,
            warehouse: "WAREHOUSE-EAST".to_string(),
        };

        println!(
            "[{}]   CHILD[inventory-{}]: Check complete, signaling parent {}",
            format_time(),
            child_flow_id,
            self.parent_flow_id
        );

        // Signal parent with result
        self.signal_parent(result.clone()).await?;

        Ok(result)
    }

    #[step]
    async fn query_warehouse(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}]     -> Querying warehouse database...", format_time());
        // Simulate database query with timer
        ergon::executor::schedule_timer(Duration::from_secs(2)).await?;
        Ok(())
    }

    #[step]
    async fn signal_parent(self: Arc<Self>, result: InventoryResult) -> Result<(), ExecutionError> {
        println!(
            "[{}]     -> Signaling parent flow {}...",
            format_time(),
            self.parent_flow_id
        );

        // Level 1 API: Clean helper replaces 15 lines of boilerplate
        ergon::executor::signal_parent_flow(self.parent_flow_id, "await_inventory_check", result)
            .await?;

        println!(
            "[{}]     -> Parent flow {} resumed",
            format_time(),
            self.parent_flow_id
        );

        Ok(())
    }
}

// =============================================================================
// Child Flow 2 - Payment Service
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct PaymentProcessingFlow {
    parent_flow_id: Uuid,
    customer_id: String,
    amount: f64,
}

impl PaymentProcessingFlow {
    #[flow]
    async fn process_payment(self: Arc<Self>) -> Result<PaymentResult, ExecutionError> {
        let child_flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{}]   CHILD[payment-{}]: Processing payment for customer {}",
            format_time(),
            child_flow_id,
            self.customer_id
        );

        // Simulate payment processing
        self.clone().charge_customer().await?;

        let result = PaymentResult {
            transaction_id: format!("TXN-{}", Uuid::new_v4()),
            charged_amount: self.amount,
        };

        println!(
            "[{}]   CHILD[payment-{}]: Payment complete, signaling parent {}",
            format_time(),
            child_flow_id,
            self.parent_flow_id
        );

        self.signal_parent(result.clone()).await?;

        Ok(result)
    }

    #[step]
    async fn charge_customer(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!(
            "[{}]     -> Charging ${} to payment gateway...",
            format_time(),
            self.amount
        );
        // Simulate payment gateway call
        ergon::executor::schedule_timer(Duration::from_secs(3)).await?;
        Ok(())
    }

    #[step]
    async fn signal_parent(self: Arc<Self>, result: PaymentResult) -> Result<(), ExecutionError> {
        println!(
            "[{}]     -> Signaling parent flow {}...",
            format_time(),
            self.parent_flow_id
        );

        // Level 1 API: Clean helper replaces 15 lines of boilerplate
        ergon::executor::signal_parent_flow(
            self.parent_flow_id,
            "await_payment_processing",
            result,
        )
        .await?;

        println!(
            "[{}]     -> Parent flow {} resumed",
            format_time(),
            self.parent_flow_id
        );

        Ok(())
    }
}

// =============================================================================
// Child Flow 3 - Shipping Service
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct ShippingFlow {
    parent_flow_id: Uuid,
    order_id: String,
    customer_id: String,
}

impl ShippingFlow {
    #[flow]
    async fn create_shipping(self: Arc<Self>) -> Result<ShippingResult, ExecutionError> {
        let child_flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{}]   CHILD[shipping-{}]: Creating shipping label for order {}",
            format_time(),
            child_flow_id,
            self.order_id
        );

        // Simulate shipping label creation
        self.clone().generate_label().await?;

        let result = ShippingResult {
            tracking_number: format!("TRACK-{}", Uuid::new_v4()),
            estimated_delivery: "2025-12-10".to_string(),
        };

        println!(
            "[{}]   CHILD[shipping-{}]: Label created, signaling parent {}",
            format_time(),
            child_flow_id,
            self.parent_flow_id
        );

        self.signal_parent(result.clone()).await?;

        Ok(result)
    }

    #[step]
    async fn generate_label(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}]     -> Generating shipping label...", format_time());
        // Simulate label generation
        ergon::executor::schedule_timer(Duration::from_secs(1)).await?;
        Ok(())
    }

    #[step]
    async fn signal_parent(self: Arc<Self>, result: ShippingResult) -> Result<(), ExecutionError> {
        println!(
            "[{}]     -> Signaling parent flow {}...",
            format_time(),
            self.parent_flow_id
        );

        // Level 1 API: Clean helper replaces 15 lines of boilerplate
        ergon::executor::signal_parent_flow(self.parent_flow_id, "await_shipping", result).await?;

        println!(
            "[{}]     -> Parent flow {} resumed",
            format_time(),
            self.parent_flow_id
        );

        Ok(())
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

fn format_time() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

// =============================================================================
// Main - Automatic Orchestration
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nChild Flow Invocation - Function Tree Pattern with Signals");
    println!("===========================================================\n");

    let storage = Arc::new(SqliteExecutionLog::new("child_flow_demo.db").await?);
    storage.reset().await?;

    println!("Starting worker...\n");

    // Start worker with timer processing
    let worker = Worker::new(storage.clone(), "order-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(100))
        .with_poll_interval(Duration::from_millis(100));

    // Register all flow types
    worker
        .register(|flow: Arc<OrderFlow>| flow.process_order())
        .await;
    worker
        .register(|flow: Arc<InventoryCheckFlow>| flow.check_inventory())
        .await;
    worker
        .register(|flow: Arc<PaymentProcessingFlow>| flow.process_payment())
        .await;
    worker
        .register(|flow: Arc<ShippingFlow>| flow.create_shipping())
        .await;

    let worker = worker.start().await;

    println!("  Worker started\n");

    // Give worker time to initialize
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("Scheduling flows...\n");

    let scheduler = Scheduler::new(storage.clone());

    // Schedule parent flow
    let parent_flow = OrderFlow {
        order_id: "ORD-001".to_string(),
        product_id: "PROD-12345".to_string(),
        customer_id: "CUST-98765".to_string(),
        amount: 99.99,
    };

    let parent_flow_id = Uuid::new_v4();
    scheduler.schedule(parent_flow, parent_flow_id).await?;
    println!("  Scheduled parent flow: {}", parent_flow_id);

    // Wait for parent to start and suspend on first signal
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Schedule child flow 1: Inventory check
    let inventory_flow = InventoryCheckFlow {
        parent_flow_id,
        product_id: "PROD-12345".to_string(),
    };
    let inv_flow_id = Uuid::new_v4();
    scheduler.schedule(inventory_flow, inv_flow_id).await?;
    println!("  Scheduled inventory child: {}", inv_flow_id);

    // Wait for inventory to complete and parent to suspend again
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Schedule child flow 2: Payment processing
    let payment_flow = PaymentProcessingFlow {
        parent_flow_id,
        customer_id: "CUST-98765".to_string(),
        amount: 99.99,
    };
    let pay_flow_id = Uuid::new_v4();
    scheduler.schedule(payment_flow, pay_flow_id).await?;
    println!("  Scheduled payment child: {}", pay_flow_id);

    // Wait for payment to complete and parent to suspend again
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Schedule child flow 3: Shipping
    let shipping_flow = ShippingFlow {
        parent_flow_id,
        order_id: "ORD-001".to_string(),
        customer_id: "CUST-98765".to_string(),
    };
    let ship_flow_id = Uuid::new_v4();
    scheduler.schedule(shipping_flow, ship_flow_id).await?;
    println!("  Scheduled shipping child: {}", ship_flow_id);

    println!("\nWorker processing flows...\n");

    // Wait for all flows to complete
    let timeout_duration = Duration::from_secs(10);
    match tokio::time::timeout(timeout_duration, async {
        loop {
            let incomplete = storage.get_incomplete_flows().await?;
            if incomplete.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await
    {
        Ok(_) => println!("\nAll flows completed!\n"),
        Err(_) => {
            println!("\nTimeout waiting for flows to complete\n");
            let incomplete = storage.get_incomplete_flows().await?;
            println!("Incomplete flows: {}", incomplete.len());
            for inv in incomplete {
                println!("  - {} ({})", inv.id(), inv.class_name());
            }
        }
    }

    println!("Shutting down worker...\n");
    worker.shutdown().await;

    println!("Verifying results...\n");

    let incomplete = storage.get_incomplete_flows().await?;
    if incomplete.is_empty() {
        println!("  All flows completed successfully");
    } else {
        println!("  {} flows incomplete", incomplete.len());
        for inv in incomplete {
            println!("    - {} ({})", inv.id(), inv.class_name());
        }
    }

    storage.close().await?;
    Ok(())
}
