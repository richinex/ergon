//! Child Flow Level 3 API - Token-Based Invocation
//!
//! This example demonstrates Level 3 of the parent-child flow API:
//! **Type-safe, token-based child invocation with automatic type inference**
//!
//! ## Evolution
//!
//! - Level 0: Manual (15 lines of boilerplate)
//! - Level 1: Helper function (`signal_parent_flow()` - 1 line)
//! - Level 2: Automatic signaling (child returns Ok(result))
//! - Level 3 (THIS EXAMPLE): invoke() API - parent directly invokes child!
//!
//! ## Key Features
//!
//! 1. **No parent_flow_id fields** - Parent relationship is implicit
//! 2. **No method name strings** - Uses UUID tokens instead
//! 3. **Type inference** - Compiler knows child's result type
//! 4. **Direct invocation** - Parent calls self.invoke(child)
//! 5. **Impossible to misuse** - Type-safe at compile time
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
//! cargo run --example child_flow_level3 --features=sqlite
//! ```

use chrono::Utc;
use ergon::core::{FlowType, InvokableFlow};
use ergon::executor::{InvokeChild, Worker};
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

        // ✨ LEVEL 3 API: Direct invocation with type inference!
        println!(
            "[{}] PARENT[{}]: Invoking inventory check...",
            format_time(),
            flow_id
        );

        // Type inferred as InventoryStatus (from CheckInventory::Output)
        let inventory = self
            .invoke(CheckInventory {
                order_id: self.order_id.clone(),
            })
            .result()
            .await?;

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

        // ✨ LEVEL 3 API: Second invocation
        println!(
            "[{}] PARENT[{}]: Invoking payment validation...",
            format_time(),
            flow_id
        );

        // Type inferred as PaymentStatus (from ValidatePayment::Output)
        let payment = self
            .invoke(ValidatePayment {
                order_id: self.order_id.clone(),
            })
            .result()
            .await?;

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
}

// =============================================================================
// Child Flow 1 - Inventory Check (Level 3 API)
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct CheckInventory {
    order_id: String,
    // ✨ NO parent_flow_id field!
}

// Manual FlowType implementation
impl FlowType for CheckInventory {
    fn type_id() -> &'static str {
        "CheckInventory"
    }
    // ✨ NO parent_info() implementation!
}

// Declare output type for type inference
impl InvokableFlow for CheckInventory {
    type Output = InventoryStatus;
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

        // ✨ Just return - worker auto-signals parent with token!
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
// Child Flow 2 - Payment Validation (Level 3 API)
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct ValidatePayment {
    order_id: String,
    // ✨ NO parent_flow_id field!
}

impl FlowType for ValidatePayment {
    fn type_id() -> &'static str {
        "ValidatePayment"
    }
    // ✨ NO parent_info() implementation!
}

// Declare output type for type inference
impl InvokableFlow for ValidatePayment {
    type Output = PaymentStatus;
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

        // ✨ Just return - worker auto-signals parent with token!
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
    println!("\nChild Flow Level 3 API - Token-Based Invocation");
    println!("=================================================\n");
    println!("This demonstrates the Level 3 API where:");
    println!("1. Parent directly invokes children (no external scheduling)");
    println!("2. No parent_flow_id fields needed");
    println!("3. No method name strings (uses UUID tokens)");
    println!("4. Full compile-time type inference\n");

    let storage = Arc::new(SqliteExecutionLog::new("level3_demo.db").await?);
    storage.reset().await?;

    println!("Starting worker...\n");

    let worker = Worker::new(storage.clone(), "validation-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(100))
        .with_poll_interval(Duration::from_millis(100));

    // Register all flows - just plain child flows, no wrappers!
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

    println!("Scheduling parent flow...\n");

    let scheduler = ergon::executor::Scheduler::new(storage.clone());

    // Schedule the parent flow - children will be invoked automatically!
    let parent_flow = OrderValidation {
        order_id: "ORD-12345".to_string(),
    };

    let parent_flow_id = Uuid::new_v4();
    scheduler.schedule(parent_flow, parent_flow_id).await?;

    println!("  Scheduled parent flow: {}", parent_flow_id);
    println!("\nWorker processing flows...\n");

    // Wait for completion
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
        Ok(_) => {
            // Allow output to flush from async tasks
            tokio::time::sleep(Duration::from_secs(2)).await;
            println!("\nAll flows completed!\n");
        }
        Err(_) => {
            println!("\nTimeout waiting for flows to complete\n");
            let incomplete = storage.get_incomplete_flows().await?;
            println!("Incomplete flows: {}", incomplete.len());
        }
    }

    worker.shutdown().await;

    println!("\n=== Level 3 API Highlights ===\n");
    println!("1. Parent directly invokes children with self.invoke(child)");
    println!("2. NO parent_flow_id fields in child structs");
    println!("3. NO method name strings (uses UUID tokens)");
    println!("4. Type inference: inventory is InventoryStatus automatically");
    println!("5. Compile-time type safety - wrong type = compile error\n");
    println!("Compare to Level 0: 15 lines of boilerplate → 0 lines!");
    println!("Compare to Level 1: 1 line helper → 0 lines!");
    println!("Compare to Level 2: Still had parent_flow_id field → Gone!\n");

    storage.close().await?;
    Ok(())
}
