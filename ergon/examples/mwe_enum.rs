//! Ergon Minimal Example - Enum Output Types
//!
//! Demonstrates that child flows can return enums, enabling
//! rich, type-safe result handling with multiple variants.

use chrono::Utc;
use ergon::executor::{InvokeChild, Worker};
use ergon::prelude::*;
use ergon::storage::SqliteExecutionLog;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Domain Types - Rich Enum
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ShippingResult {
    Success { tracking: String, eta_days: u32 },
    Delayed { tracking: String, reason: String, new_eta_days: u32 },
    Failed { reason: String },
}

// =============================================================================
// Parent Flow - Order Processing
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct Order {
    id: String,
    expedited: bool,
}

impl Order {
    #[step]
    async fn validate(self: Arc<Self>) -> Result<(), String> {
        println!("[{}] validating order {}", ts(), self.id);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    #[step(depends_on = "validate")]
    async fn ship(self: Arc<Self>) -> Result<ShippingResult, String> {
        let result = self
            .invoke(Shipment {
                order_id: self.id.clone(),
                expedited: self.expedited,
            })
            .result()
            .await
            .map_err(|e| e.to_string())?;

        // Pattern match on the enum result
        match &result {
            ShippingResult::Success { tracking, eta_days } => {
                println!("[{}] shipped: {} (ETA: {} days)", ts(), tracking, eta_days);
            }
            ShippingResult::Delayed { tracking, reason, new_eta_days } => {
                println!(
                    "[{}] delayed: {} - {} (new ETA: {} days)",
                    ts(),
                    tracking,
                    reason,
                    new_eta_days
                );
            }
            ShippingResult::Failed { reason } => {
                println!("[{}] shipping failed: {}", ts(), reason);
            }
        }

        Ok(result)
    }

    #[flow]
    async fn fulfill(self: Arc<Self>) -> Result<ShippingResult, String> {
        self.clone().validate().await?;
        self.clone().ship().await
    }
}

// =============================================================================
// Child Flow - Shipment (returns enum)
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = ShippingResult)]
struct Shipment {
    order_id: String,
    expedited: bool,
}

impl Shipment {
    #[flow]
    async fn create(self: Arc<Self>) -> Result<ShippingResult, String> {
        println!("[{}] creating shipment for {}", ts(), self.order_id);
        tokio::time::sleep(Duration::from_millis(150)).await;

        let tracking = format!("TRK-{}", &Uuid::new_v4().to_string()[..8]);

        // Simulate different shipping outcomes
        let result = if self.expedited {
            ShippingResult::Success {
                tracking: tracking.clone(),
                eta_days: 1,
            }
        } else {
            // 30% chance of delay (simplified - just checking order_id hash)
            if self.order_id.len() % 3 == 0 {
                ShippingResult::Delayed {
                    tracking: tracking.clone(),
                    reason: "Weather delay".to_string(),
                    new_eta_days: 7,
                }
            } else {
                ShippingResult::Success {
                    tracking: tracking.clone(),
                    eta_days: 3,
                }
            }
        };

        Ok(result)
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
    let db = "/tmp/ergon_mwe_enum.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);
    let scheduler = Scheduler::new(storage.clone());

    println!("\n=== Testing Enum Outputs ===\n");

    // Test 1: Normal shipping (should succeed)
    println!("--- Order 1: Normal Shipping ---");
    let order1 = Order {
        id: "ORD-001".into(),
        expedited: false,
    };
    let task_id = scheduler.schedule(order1, Uuid::new_v4()).await?;
    println!("[{}] scheduled: {}\n", ts(), &task_id.to_string()[..8]);

    // Test 2: Expedited shipping
    println!("--- Order 2: Expedited Shipping ---");
    let order2 = Order {
        id: "ORD-002".into(),
        expedited: true,
    };
    let task_id = scheduler.schedule(order2, Uuid::new_v4()).await?;
    println!("[{}] scheduled: {}\n", ts(), &task_id.to_string()[..8]);

    // Test 3: Order that might get delayed (order_id length % 3 == 0)
    println!("--- Order 3: Potentially Delayed ---");
    let order3 = Order {
        id: "ABC".into(), // length 3 % 3 == 0, will be delayed
        expedited: false,
    };
    let task_id = scheduler.schedule(order3, Uuid::new_v4()).await?;
    println!("[{}] scheduled: {}\n", ts(), &task_id.to_string()[..8]);

    let worker =
        Worker::new(storage.clone(), "worker").with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<Order>| f.fulfill()).await;
    worker.register(|f: Arc<Shipment>| f.create()).await;

    let handle = worker.start().await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    handle.shutdown().await;

    storage.close().await?;

    println!("\n=== All Tests Complete ===");
    Ok(())
}
