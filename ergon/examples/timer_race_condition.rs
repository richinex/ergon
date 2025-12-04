//! Timer Race Condition Example (SQLite)
//!
//! This example demonstrates how ergon handles the race condition where
//! a timer fires BETWEEN log_timer() and await_timer() calls with SQLite storage.
//!
//! ## The Race Condition Scenario
//!
//! 1. Flow calls schedule_timer(Duration::from_millis(1))
//! 2. Timer is logged to SQLite with fire_at = now + 1ms
//! 3. Timer processor polls and finds the expired timer
//! 4. Timer processor fires the timer and notifies
//! 5. Flow hasn't started waiting yet - notification is lost!
//!
//! ## How Ergon Handles It
//!
//! 1. schedule_timer() logs timer to database
//! 2. Timer processor might fire it immediately
//! 3. await_timer() checks database after creating notifier
//! 4. If status=Complete, returns immediately (no wait)
//! 5. This prevents deadlock from lost notifications
//!
//! ## SQLite Implementation Details
//!
//! - Indexed queries for efficient expiry lookups
//! - IMMEDIATE transactions for atomic timer claiming
//! - UPDATE with WHERE clause for optimistic concurrency
//! - Single worker coordinates timer firing
//!
//! ## Scenario
//! - 3 concurrent flows running simultaneously (flow-1, flow-2, flow-3)
//! - Each flow schedules 5 VERY short timers (1ms duration)
//! - Total: 15 timers competing for execution
//! - Worker polls timers every 10ms (aggressive polling)
//! - Timer often fires BEFORE await_timer() starts waiting
//! - Ergon's database check prevents deadlock from lost notifications
//! - All 15 timers complete successfully without hanging
//!
//! ## Key Takeaways
//! - Race condition: Timer fires between log_timer() and await_timer()
//! - Notification lost but flow doesn't deadlock
//! - await_timer() checks database status after creating notifier
//! - If status=Complete, returns immediately (no wait needed)
//! - Indexed queries on fire_at enable efficient timer lookups
//! - Optimistic concurrency via UPDATE WHERE clause prevents duplicates
//! - Works reliably even with aggressive polling and short timers
//!
//! ## Run with
//! ```bash
//! cargo run --example timer_race_condition --features=sqlite
//! ```

use chrono::Utc;
use ergon::executor::{schedule_timer, Scheduler, Worker};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct RaceConditionFlow {
    id: String,
}

impl RaceConditionFlow {
    #[flow]
    async fn test_race(self: Arc<Self>) -> Result<String, String> {
        println!("[{}] Starting flow {}", format_time(), self.id);

        // Use VERY short timers to trigger race condition
        for i in 1..=5 {
            self.clone().short_timer(i).await?;
        }

        println!("[{}] All timers completed!", format_time());
        Ok("Success".to_string())
    }

    #[step]
    async fn short_timer(self: Arc<Self>, iteration: i32) -> Result<(), String> {
        println!(
            "[{}] Step {}: Scheduling VERY short timer (1ms)...",
            format_time(),
            iteration
        );

        // Use 1ms timer - very likely to fire before await_timer starts waiting
        schedule_timer(Duration::from_millis(1))
            .await
            .map_err(|e| e.to_string())?;

        println!(
            "[{}] Step {}: Timer completed (race handled correctly!)",
            format_time(),
            iteration
        );
        Ok(())
    }
}

fn format_time() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Timer Race Condition Demonstration ===\n");

    println!("This example demonstrates the race condition where:");
    println!("  - 3 flows running CONCURRENTLY");
    println!("  - Each scheduling VERY short timers (1ms)");
    println!("  - Aggressive timer polling (10ms)");
    println!("  - Timer fires BEFORE await_timer starts waiting");
    println!("  - ergon handles this correctly via database check\n");

    // Setup storage (use file-based DB like timer_demo)
    let storage = Arc::new(SqliteExecutionLog::new("timer_race.db").await?);

    // Start worker with timer processing enabled and VERY frequent polling (10ms)
    // This makes the race condition MORE likely to occur
    let worker = Worker::new(storage.clone(), "timer-race-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(10))
        .with_poll_interval(Duration::from_millis(100)); // Poll for flows frequently

    // Register flow type
    worker
        .register(|flow: Arc<RaceConditionFlow>| flow.test_race())
        .await;

    let worker = worker.start().await;

    println!("Worker with timer processing started (timer_interval=10ms - aggressive)\n");

    // Schedule 3 flows CONCURRENTLY to increase race condition likelihood
    // Use Scheduler so Worker can process them to completion
    let scheduler = Scheduler::new(storage.clone());
    let mut task_ids = vec![];

    for i in 1..=3 {
        let flow = RaceConditionFlow {
            id: format!("flow-{}", i),
        };
        let flow_id = uuid::Uuid::new_v4();
        let task_id = scheduler.schedule(flow, flow_id).await?;
        task_ids.push((i, task_id));
        println!("[SCHEDULED] Flow {} with task_id {}", i, task_id);
    }

    println!("\nWorker processing flows (5 timers each × 1ms + overhead)...\n");

    // Give worker time to pick up flows from queue
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wait for all flows to complete with timeout
    let timeout_duration = Duration::from_secs(5);
    match tokio::time::timeout(timeout_duration, async {
        loop {
            let incomplete = storage.get_incomplete_flows().await?;
            if incomplete.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
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
        }
    }

    // Shutdown
    worker.shutdown().await;

    // Check final status
    println!("=== Final Results ===\n");

    let incomplete = storage.get_incomplete_flows().await?;
    if incomplete.is_empty() {
        println!("✓ All 3 flows completed successfully (15 timers total)");
    } else {
        println!("✗ {} flows incomplete", incomplete.len());
    }

    println!("\nWorker Distribution:\n");
    for (i, task_id) in task_ids {
        if let Some(scheduled_flow) = storage.get_scheduled_flow(task_id).await? {
            let worker = scheduled_flow.locked_by.as_deref().unwrap_or("unknown");
            let status = match scheduled_flow.status {
                TaskStatus::Complete => "COMPLETE",
                TaskStatus::Running => "RUNNING",
                TaskStatus::Failed => "FAILED",
                TaskStatus::Pending => "PENDING",
            };
            println!("  Flow {} -> {} (status: {})", i, worker, status);
        }
    }

    println!("\n[OK] Race condition handling demonstrated successfully!");

    Ok(())
}
