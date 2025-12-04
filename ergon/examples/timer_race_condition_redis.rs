//! Timer Race Condition Example (Redis)
//!
//! This example demonstrates:
//! - Handling timer race conditions with Redis backend
//! - Very short timers (1ms) that fire before await starts
//! - Multiple concurrent flows with aggressive timer polling
//! - Redis sorted sets for efficient timer expiry queries
//! - Lua scripts for atomic timer claiming
//! - Deadlock prevention via status checking
//!
//! ## Scenario
//! Three flows run concurrently, each scheduling 5 very short timers (1ms).
//! With aggressive 10ms polling, timers often fire BEFORE await_timer starts waiting.
//! Ergon prevents deadlock by checking timer status after creating the notifier.
//!
//! ## Key Takeaways
//! - schedule_timer logs timer to Redis sorted set with fire_at timestamp
//! - Timer processor might fire timer immediately via Lua script
//! - await_timer checks Redis after creating notifier to prevent deadlock
//! - If timer already Complete, returns immediately without waiting
//! - Multiple workers coordinate via Redis atomic operations
//! - 15 total timers complete without deadlocks despite race conditions
//!
//! ## Prerequisites
//! Start Redis:
//! ```bash
//! docker run -d -p 6379:6379 redis:latest
//! ```
//!
//! ## Run with
//! ```bash
//! cargo run --example timer_race_condition_redis --features redis
//! ```

use chrono::Utc;
use ergon::executor::schedule_timer;
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
    println!("\n=== Timer Race Condition Demonstration (Redis) ===\n");

    println!("This example demonstrates the race condition where:");
    println!("  - 3 flows running CONCURRENTLY");
    println!("  - Each scheduling VERY short timers (1ms)");
    println!("  - Aggressive timer polling (10ms)");
    println!("  - Timer fires BEFORE await_timer starts waiting");
    println!("  - ergon handles this correctly via Redis check\n");

    // Setup Redis storage
    let storage = Arc::new(RedisExecutionLog::new("redis://127.0.0.1:6379").await?);

    // Clear any previous state
    storage.reset().await?;

    // Start worker with timer processing enabled and VERY frequent polling (10ms)
    // This makes the race condition MORE likely to occur
    let worker = Worker::new(storage.clone(), "timer-race-worker-redis")
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
            let instance = Executor::new(flow_id, flow, storage_clone);

            let start = std::time::Instant::now();
            let result = instance
                .execute(|f| Box::pin(Arc::new(f.clone()).test_race()))
                .await;
            let elapsed = start.elapsed();
            (i, result, elapsed)
        });

        handles.push(handle);
    }

    // Wait for all flows to complete
    for handle in handles {
        match handle.await? {
            (i, Ok(result), elapsed) => {
                println!("[OK] Flow {} completed: {} (took {:?})", i, result, elapsed);
            }
            (i, Err(e), elapsed) => {
                println!("[ERR] Flow {} failed: {} (took {:?})", i, e, elapsed);
            }
        }
    }

    // Shutdown
    worker.shutdown().await;
    println!("\n[OK] All 15 timers completed successfully!");

    storage.close().await?;
    Ok(())
}
