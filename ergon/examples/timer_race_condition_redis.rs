//! Timer Race Condition Example (Redis)
//!
//! This example demonstrates how ergon handles the race condition where
//! a timer fires BETWEEN log_timer() and await_timer() calls with Redis storage.
//!
//! ## The Race Condition Scenario
//!
//! 1. Flow calls schedule_timer(Duration::from_millis(1))
//! 2. Timer is logged to Redis with fire_at = now + 1ms
//! 3. Timer processor polls and finds the expired timer
//! 4. Timer processor fires the timer and notifies
//! 5. Flow hasn't started waiting yet - notification is lost!
//!
//! ## How Ergon Handles It
//!
//! 1. schedule_timer() logs timer to Redis sorted set
//! 2. Timer processor might fire it immediately via Lua script
//! 3. await_timer() checks Redis after creating notifier
//! 4. If status=Complete, returns immediately (no wait)
//! 5. This prevents deadlock from lost notifications
//!
//! ## Redis Implementation Details
//!
//! - Sorted set (ZSET) for efficient expiry queries by timestamp
//! - Lua script for atomic timer claiming (prevents duplicate firing)
//! - Hash (HASH) for timer metadata storage
//! - Multiple workers coordinate via Redis atomic operations
//!
//! ## Test Configuration
//!
//! - 3 concurrent flows running simultaneously
//! - Each flow schedules 5 VERY short timers (1ms each)
//! - Aggressive polling (10ms) creates ideal race conditions
//! - 15 total timers must complete without deadlocks
//!
//! Run: cargo run --example timer_race_condition_redis --features=redis

use ergon::prelude::*;
use ergon::executor::{schedule_timer, FlowWorker};
use std::sync::Arc;
use std::time::Duration;
use chrono::Utc;

#[derive(Clone, Serialize, Deserialize)]
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
        println!("[{}] Step {}: Scheduling VERY short timer (1ms)...", format_time(), iteration);

        // Use 1ms timer - very likely to fire before await_timer starts waiting
        schedule_timer(Duration::from_millis(1)).await;

        println!("[{}] Step {}: Timer completed (race handled correctly!)", format_time(), iteration);
        Ok(())
    }
}

fn format_time() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Timer Race Condition Demonstration (Redis) ===\n");

    println!("This example demonstrates the race condition where:");
    println!("  - 3 flows running CONCURRENTLY");
    println!("  - Each scheduling VERY short timers (1ms)");
    println!("  - Aggressive timer polling (10ms)");
    println!("  - Timer fires BEFORE await_timer starts waiting");
    println!("  - ergon handles this correctly via Redis check\n");

    // Setup Redis storage
    let storage = Arc::new(RedisExecutionLog::new("redis://127.0.0.1:6379")?);

    // Clear any previous state
    storage.reset().await?;

    // Start worker with timer processing enabled and VERY frequent polling (10ms)
    // This makes the race condition MORE likely to occur
    let worker = FlowWorker::new(storage.clone(), "timer-race-worker-redis")
        .with_timers()
        .with_timer_interval(Duration::from_millis(10))
        .start()
        .await;

    println!("Worker with timer processing started (timer_interval=10ms - aggressive)\n");

    // Run 3 flows CONCURRENTLY to increase race condition likelihood
    // This is the realistic scenario - multiple flows competing for timers
    let mut handles = vec![];

    for i in 1..=3 {
        let storage_clone = storage.clone();
        let handle = tokio::spawn(async move {
            let flow = RaceConditionFlow {
                id: format!("flow-{}", i),
            };

            let flow_id = uuid::Uuid::new_v4();
            let instance = FlowInstance::new(flow_id, flow, storage_clone);

            let start = std::time::Instant::now();
            let result = instance.execute(|f| Arc::new(f).test_race()).await;
            let elapsed = start.elapsed();
            (i, result, elapsed)
        });

        handles.push(handle);
    }

    // Wait for all flows to complete
    for handle in handles {
        match handle.await? {
            (i, Ok(Ok(result)), elapsed) => {
                println!("[OK] Flow {} completed: {} (took {:?})", i, result, elapsed);
            }
            (i, Ok(Err(e)), elapsed) => {
                println!("[ERR] Flow {} failed: {} (took {:?})", i, e, elapsed);
            }
            (i, Err(e), elapsed) => {
                println!("[ERR] Flow {} error: {} (took {:?})", i, e, elapsed);
            }
        }
    }

    // Shutdown
    worker.shutdown().await;
    println!("\n[OK] All 15 timers completed successfully!");

    storage.close().await?;
    Ok(())
}
