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
    Success {
        tracking: String,
        eta_days: u32,
    },
    Delayed {
        tracking: String,
        reason: String,
        new_eta_days: u32,
    },
    Failed {
        reason: String,
    },
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
            ShippingResult::Delayed {
                tracking,
                reason,
                new_eta_days,
            } => {
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

    println!("\n=== Testing Enum Outputs ===\n");

    // ============================================================
    // PART 1: API Server / Scheduler Process
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 1: Scheduling Orders (API Server)                    ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let scheduler = Scheduler::new(storage.clone());
    let mut task_ids = Vec::new();

    // Test 1: Normal shipping
    let order1 = Order {
        id: "ORD-001".into(),
        expedited: false,
    };
    let task_id = scheduler.schedule(order1, Uuid::new_v4()).await?;
    println!(
        "   ✓ ORD-001 scheduled (task_id: {})",
        &task_id.to_string()[..8]
    );
    task_ids.push(task_id);

    // Test 2: Expedited shipping
    let order2 = Order {
        id: "ORD-002".into(),
        expedited: true,
    };
    let task_id = scheduler.schedule(order2, Uuid::new_v4()).await?;
    println!(
        "   ✓ ORD-002 scheduled (task_id: {})",
        &task_id.to_string()[..8]
    );
    task_ids.push(task_id);

    // Test 3: Potentially delayed
    let order3 = Order {
        id: "ABC".into(), // length 3 % 3 == 0, will be delayed
        expedited: false,
    };
    let task_id = scheduler.schedule(order3, Uuid::new_v4()).await?;
    println!(
        "   ✓ ABC scheduled (task_id: {})",
        &task_id.to_string()[..8]
    );
    task_ids.push(task_id);

    println!("\n   → In production: Return HTTP 202 Accepted with task_ids\n");

    // ============================================================
    // PART 2: Worker Service (Separate Process)
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 2: Starting Worker (Separate Service)                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let worker =
        Worker::new(storage.clone(), "worker").with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<Order>| f.fulfill()).await;
    worker.register(|f: Arc<Shipment>| f.create()).await;

    let handle = worker.start().await;

    // ============================================================
    // PART 3: Client Status Monitoring (Demo Only)
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 3: Monitoring Status (Client Would Poll API)         ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let timeout_duration = Duration::from_secs(5);
    let wait_result = tokio::time::timeout(timeout_duration, async {
        loop {
            let mut all_complete = true;
            for &task_id in &task_ids {
                if let Some(scheduled) = storage.get_scheduled_flow(task_id).await? {
                    if !matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                        all_complete = false;
                        break;
                    }
                }
            }
            if all_complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await;

    match wait_result {
        Ok(_) => println!("\n=== All Tests Complete ===\n"),
        Err(_) => println!("\n[WARN] Timeout waiting for tests to complete\n"),
    }

    handle.shutdown().await;
    storage.close().await?;
    Ok(())
}
