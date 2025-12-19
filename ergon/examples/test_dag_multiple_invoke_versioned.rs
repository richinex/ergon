//! Test: Sequential execution with multiple .invoke() calls and Flow Versioning
//!
//! This example demonstrates how versioning works across parent-child flow relationships:
//! - Parent flow is scheduled with a version
//! - Child flows (invoked via .invoke()) are scheduled independently
//! - Each flow (parent and children) can have its own version
//! - Useful for tracking which code version processed each flow in a hierarchy
//!
//! Note: With atomic steps, .invoke() must be at flow level.
//! This means we can't use dag! macro when child results are needed in steps.

use chrono::Utc;
use ergon::executor::{ExecutionError, InvokeChild};
use ergon::prelude::*;
use std::time::Duration;

// =============================================================================
// Parent Flow - Uses DAG execution
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct Order {
    id: String,
}

impl Order {
    #[step]
    async fn validate(self: Arc<Self>) -> Result<(), String> {
        println!("[{}] Step: validate {}", ts(), self.id);
        Ok(())
    }

    #[step]
    async fn finalize_payment(
        self: Arc<Self>,
        result: PaymentResult,
    ) -> Result<PaymentResult, String> {
        println!("[{}] Step: processing payment result: {:?}", ts(), result);
        Ok(result)
    }

    #[step]
    async fn finalize_shipment(
        self: Arc<Self>,
        result: ShipmentResult,
    ) -> Result<ShipmentResult, String> {
        println!("[{}] Step: processing shipment result: {:?}", ts(), result);
        Ok(result)
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<ShipmentResult, ExecutionError> {
        // Validate first
        self.clone()
            .validate()
            .await
            .map_err(ExecutionError::Failed)?;

        // Invoke children at flow level
        println!("[{}] Flow: invoking Payment child", ts());
        let payment_result = self
            .invoke(PaymentFlow {
                order_id: self.id.clone(),
                amount: 99.99,
            })
            .result()
            .await
            .map_err(|e| ExecutionError::Failed(e.to_string()))?;

        println!("[{}] Flow: invoking Shipment child", ts());
        let shipment_result = self
            .invoke(ShipmentFlow {
                order_id: self.id.clone(),
            })
            .result()
            .await
            .map_err(|e| ExecutionError::Failed(e.to_string()))?;

        // Process results in atomic steps
        self.clone()
            .finalize_payment(payment_result)
            .await
            .map_err(ExecutionError::Failed)?;
        self.clone()
            .finalize_shipment(shipment_result)
            .await
            .map_err(ExecutionError::Failed)
    }
}

// =============================================================================
// Child Flow 1 - Payment
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentResult {
    transaction_id: String,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = PaymentResult)]
struct PaymentFlow {
    order_id: String,
    amount: f64,
}

impl PaymentFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<PaymentResult, String> {
        println!("[{}]   CHILD: Processing payment ${:.2}", ts(), self.amount);
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(PaymentResult {
            transaction_id: format!("TXN-{}", &Uuid::new_v4().to_string()[..8]),
        })
    }
}

// =============================================================================
// Child Flow 2 - Shipment
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShipmentResult {
    tracking: String,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = ShipmentResult)]
struct ShipmentFlow {
    order_id: String,
}

impl ShipmentFlow {
    #[flow]
    async fn create(self: Arc<Self>) -> Result<ShipmentResult, String> {
        println!("[{}]   CHILD: Creating shipment", ts());
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(ShipmentResult {
            tracking: format!("TRK-{}", &Uuid::new_v4().to_string()[..8]),
        })
    }
}

// =============================================================================
// Utilities
// =============================================================================

fn ts() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = "/tmp/ergon_test_dag_multiple_invoke_versioned.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);
    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    println!("\n=== Flow Versioning with Child Invocations Demo ===\n");
    println!("This example shows how versioning works when a parent flow invokes children:");
    println!("  • Parent flow (Order) scheduled with version v1.0");
    println!("  • Child flows (Payment, Shipment) are invoked by parent");
    println!("  • Child flows inherit NO version from parent (they're independent)");
    println!("  • Each flow can be tracked separately\n");

    let order = Order {
        id: "ORD-001".into(),
    };

    // Schedule parent flow with version "v1.0"
    let parent_task_id = scheduler.schedule(order).await?;

    println!(
        "✓ Scheduled parent Order with version v1.0 (task_id: {})\n",
        parent_task_id.to_string()[..8].to_uppercase()
    );

    let worker =
        Worker::new(storage.clone(), "worker").with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<Order>| f.process()).await;
    worker.register(|f: Arc<PaymentFlow>| f.process()).await;
    worker.register(|f: Arc<ShipmentFlow>| f.create()).await;

    println!("✓ Worker started\n");

    let handle = worker.start().await;

    // Wait for parent and children to complete
    tokio::time::sleep(Duration::from_secs(5)).await;

    handle.shutdown().await;

    println!("\n=== Flow Version Analysis ===\n");

    // Get parent flow info
    if let Some(parent_flow) = storage.get_scheduled_flow(parent_task_id).await? {
        println!("Parent Flow (Order):");
        println!(
            "  • Task ID: {}",
            parent_task_id.to_string()[..8].to_uppercase()
        );
        println!("  • Flow Type: {}", parent_flow.flow_type);
        println!(
            "  • Version: {}",
            parent_flow.version.as_deref().unwrap_or("unversioned")
        );
        println!("  • Status: {:?}", parent_flow.status);
        println!();
    }

    // Find and display child flows
    // Note: Child flows are created by the parent during execution
    // They won't have versions unless explicitly set
    println!("Child Flows:");
    println!("  Note: Child flows invoked via .invoke() are created WITHOUT versions");
    println!("  unless the parent explicitly schedules them with schedule_with_version().");
    println!("  Currently, .invoke() uses the default schedule() method.\n");

    // In production, you could:
    // 1. Query all flows with parent_flow_id matching the parent
    // 2. Track child flow versions separately
    // 3. Use a convention where child versions inherit from parent

    println!("=== Key Insights ===\n");
    println!("1. Parent-Child Independence:");
    println!("   • Child flows are separate tasks with their own task_ids");
    println!("   • Children do NOT automatically inherit parent's version");
    println!("   • This allows flexibility: parent v1.0 can invoke child v2.0\n");

    println!("2. Version Tracking Strategy:");
    println!("   • Option A: Version only parent flows, track children via parent_flow_id");
    println!("   • Option B: Extend InvokeChild to support version parameter");
    println!("   • Option C: Use flow_type as version (e.g., 'PaymentFlow.v2')\n");

    println!("3. Current Behavior:");
    println!("   • Parent scheduled with schedule_with_version() → has version");
    println!("   • Children invoked with .invoke() → no version (uses default)");
    println!("   • Query parent_flow_id to find all children of a versioned parent\n");

    storage.close().await?;

    println!("✓ Example complete!\n");

    Ok(())
}
