//! Child Flow Level 2 API - Automatic Parent Signaling
//!
//! This example demonstrates Level 2 of the parent-child flow API evolution:
//! **Child flows automatically signal their parent on return - no explicit signaling needed!**
//!
//! ## Evolution
//!
//! - Level 0: Manual (15 lines of boilerplate)
//! - Level 1: Helper function (`signal_parent_flow()` - 1 line)
//! - Level 2 (THIS EXAMPLE): Automatic - just return `Ok(result)`!
//!
//! ## How It Works
//!
//! 1. Child flow implements `parent_info()` to declare its parent
//! 2. Child flow just returns its result normally
//! 3. Worker automatically detects parent relationship
//! 4. Worker serializes result and signals parent
//! 5. Parent resumes with the result
//!
//! ## Scenario
//!
//! E-commerce order validation:
//! - Parent: OrderValidation orchestrator
//! - Child 1: CheckInventory (validates stock)
//! - Child 2: ValidatePayment (checks payment method)
//!
//! ## Run
//!
//! ```bash
//! cargo run --example child_flow_level2 --features=sqlite
//! ```

use chrono::Utc;
use ergon::core::FlowType;
use ergon::executor::{Scheduler, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Domain Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InventoryStatus {
    in_stock: bool,
    quantity: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentStatus {
    valid: bool,
    payment_method: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidationResult {
    approved: bool,
    inventory_checked: bool,
    payment_validated: bool,
}

// =============================================================================
// Parent Flow - Order Validation Orchestrator
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderValidation {
    order_id: String,
}

impl OrderValidation {
    #[flow]
    async fn validate_order(self: Arc<Self>) -> Result<ValidationResult, ExecutionError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "\n[{}] PARENT[{}]: Validating order {}",
            format_time(),
            flow_id,
            self.order_id
        );

        // Wait for inventory check
        println!(
            "[{}] PARENT[{}]: Waiting for inventory check...",
            format_time(),
            flow_id
        );
        let inventory = self.clone().await_inventory().await?;
        println!(
            "[{}] PARENT[{}]: Inventory: in_stock={}, qty={}",
            format_time(),
            flow_id,
            inventory.in_stock,
            inventory.quantity
        );

        if !inventory.in_stock {
            return Ok(ValidationResult {
                approved: false,
                inventory_checked: true,
                payment_validated: false,
            });
        }

        // Wait for payment validation
        println!(
            "[{}] PARENT[{}]: Waiting for payment validation...",
            format_time(),
            flow_id
        );
        let payment = self.clone().await_payment().await?;
        println!(
            "[{}] PARENT[{}]: Payment: valid={}, method={}",
            format_time(),
            flow_id,
            payment.valid,
            payment.payment_method
        );

        let result = ValidationResult {
            approved: inventory.in_stock && payment.valid,
            inventory_checked: true,
            payment_validated: true,
        };

        println!(
            "\n[{}] PARENT[{}]: Order validation complete: approved={}",
            format_time(),
            flow_id,
            result.approved
        );

        Ok(result)
    }

    #[step]
    async fn await_inventory(self: Arc<Self>) -> Result<InventoryStatus, ExecutionError> {
        let result: InventoryStatus = await_external_signal("inventory-check").await?;
        Ok(result)
    }

    #[step]
    async fn await_payment(self: Arc<Self>) -> Result<PaymentStatus, ExecutionError> {
        let result: PaymentStatus = await_external_signal("payment-validation").await?;
        Ok(result)
    }
}

// =============================================================================
// Child Flow 1 - Inventory Check (Level 2 API - Auto-signaling!)
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct CheckInventory {
    parent_flow_id: Uuid,
    order_id: String,
}

// Manual FlowType implementation to add parent_info()
impl FlowType for CheckInventory {
    fn type_id() -> &'static str {
        "CheckInventory"
    }

    fn parent_info(&self) -> Option<(Uuid, String)> {
        // Declare parent relationship - enables auto-signaling!
        Some((self.parent_flow_id, "await_inventory".to_string()))
    }
}

impl CheckInventory {
    #[flow]
    async fn check(self: Arc<Self>) -> Result<InventoryStatus, ExecutionError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{}]   CHILD[inventory-{}]: Checking inventory for order {}",
            format_time(),
            flow_id,
            self.order_id
        );

        // Simulate inventory check
        self.clone().query_database().await?;

        let result = InventoryStatus {
            in_stock: true,
            quantity: 42,
        };

        println!(
            "[{}]   CHILD[inventory-{}]: Check complete: in_stock={}, qty={}",
            format_time(),
            flow_id,
            result.in_stock,
            result.quantity
        );

        // ✨ LEVEL 2 API: Just return! Worker auto-signals parent!
        Ok(result)
    }

    #[step]
    async fn query_database(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}]     -> Querying inventory database...", format_time());
        ergon::executor::schedule_timer(Duration::from_secs(1)).await?;
        Ok(())
    }
}

// =============================================================================
// Child Flow 2 - Payment Validation (Level 2 API - Auto-signaling!)
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct ValidatePayment {
    parent_flow_id: Uuid,
    order_id: String,
}

impl FlowType for ValidatePayment {
    fn type_id() -> &'static str {
        "ValidatePayment"
    }

    fn parent_info(&self) -> Option<(Uuid, String)> {
        // Declare parent relationship - enables auto-signaling!
        Some((self.parent_flow_id, "await_payment".to_string()))
    }
}

impl ValidatePayment {
    #[flow]
    async fn validate(self: Arc<Self>) -> Result<PaymentStatus, ExecutionError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{}]   CHILD[payment-{}]: Validating payment for order {}",
            format_time(),
            flow_id,
            self.order_id
        );

        // Simulate payment validation
        self.clone().check_payment_gateway().await?;

        let result = PaymentStatus {
            valid: true,
            payment_method: "credit_card".to_string(),
        };

        println!(
            "[{}]   CHILD[payment-{}]: Validation complete: valid={}, method={}",
            format_time(),
            flow_id,
            result.valid,
            result.payment_method
        );

        // ✨ LEVEL 2 API: Just return! Worker auto-signals parent!
        Ok(result)
    }

    #[step]
    async fn check_payment_gateway(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}]     -> Checking payment gateway...", format_time());
        ergon::executor::schedule_timer(Duration::from_millis(500)).await?;
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
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nChild Flow Level 2 API - Automatic Parent Signaling");
    println!("====================================================\n");
    println!("This demonstrates the Level 2 API where child flows");
    println!("automatically signal their parent on completion.\n");
    println!("✨ No explicit signaling code needed - just return Ok(result)!\n");

    let storage = Arc::new(SqliteExecutionLog::new("level2_demo.db").await?);
    storage.reset().await?;

    println!("Starting worker...\n");

    let worker = Worker::new(storage.clone(), "validation-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(100))
        .with_poll_interval(Duration::from_millis(100));

    worker
        .register(|flow: Arc<OrderValidation>| flow.validate_order())
        .await;
    worker
        .register(|flow: Arc<CheckInventory>| flow.check())
        .await;
    worker
        .register(|flow: Arc<ValidatePayment>| flow.validate())
        .await;

    let worker = worker.start().await;

    println!("  Worker started\n");
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("Scheduling flows...\n");

    let scheduler = Scheduler::new(storage.clone());

    // Schedule parent flow
    let parent_flow = OrderValidation {
        order_id: "ORD-12345".to_string(),
    };

    let parent_flow_id = Uuid::new_v4();
    scheduler.schedule(parent_flow, parent_flow_id).await?;
    println!("  Scheduled parent flow: {}", parent_flow_id);

    // Wait for parent to suspend
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Schedule child 1: Inventory check
    let inventory_flow = CheckInventory {
        parent_flow_id,
        order_id: "ORD-12345".to_string(),
    };
    let inv_id = Uuid::new_v4();
    scheduler.schedule(inventory_flow, inv_id).await?;
    println!("  Scheduled inventory child: {}", inv_id);

    // Wait for inventory to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Schedule child 2: Payment validation
    let payment_flow = ValidatePayment {
        parent_flow_id,
        order_id: "ORD-12345".to_string(),
    };
    let pay_id = Uuid::new_v4();
    scheduler.schedule(payment_flow, pay_id).await?;
    println!("  Scheduled payment child: {}", pay_id);

    println!("\nWorker processing flows...\n");

    // Wait for completion
    let timeout_duration = Duration::from_secs(5);
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
        Ok(_) => println!("\n✅ All flows completed!\n"),
        Err(_) => {
            println!("\n❌ Timeout waiting for flows to complete\n");
            let incomplete = storage.get_incomplete_flows().await?;
            println!("Incomplete flows: {}", incomplete.len());
        }
    }

    worker.shutdown().await;

    println!("\n=== Level 2 API Highlights ===\n");
    println!("1. Child flows implement parent_info() to declare parent");
    println!("2. Child flows just return Ok(result) - no signaling code!");
    println!("3. Worker automatically detects parent relationship");
    println!("4. Worker serializes result and signals parent");
    println!("5. Parent resumes with typed result\n");
    println!("Compare to Level 0: 15 lines of boilerplate → 0 lines!");
    println!("Compare to Level 1: 1 line helper → 0 lines!\n");

    storage.close().await?;
    Ok(())
}
