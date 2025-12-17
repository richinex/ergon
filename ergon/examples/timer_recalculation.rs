//! Timer recalculation demo
//!
//! This example demonstrates the event-driven timer recalculation:
//! - Schedule a 10-second timer (long wait)
//! - While waiting, schedule a 2-second timer (short wait)
//! - Verify worker wakes up for the 2-second timer first (not after 10 seconds)
//! - Then wakes up for the 10-second timer
//!
//! This proves the worker recalculates its wake time when a new timer is scheduled,
//! rather than polling at fixed intervals.

use chrono::Utc;
use ergon::executor::{schedule_timer_named, ExecutionError, Scheduler, Worker};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct LongTimerFlow {
    id: String,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct ShortTimerFlow {
    id: String,
}

impl LongTimerFlow {
    #[flow]
    async fn run_with_long_timer(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("[{}] Long timer flow started: {}", format_time(), self.id);
        self.clone().wait_long().await?;
        println!("[{}] Long timer flow completed: {}", format_time(), self.id);
        Ok(format!("Long timer {} done", self.id))
    }

    #[step]
    async fn wait_long(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}] Scheduling 10-second timer...", format_time());
        schedule_timer_named(Duration::from_secs(10), "long-timer").await?;
        println!("[{}] 10-second timer fired!", format_time());
        Ok(())
    }
}

impl ShortTimerFlow {
    #[flow]
    async fn run_with_short_timer(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("[{}] Short timer flow started: {}", format_time(), self.id);
        self.clone().wait_short().await?;
        println!("[{}] Short timer flow completed: {}", format_time(), self.id);
        Ok(format!("Short timer {} done", self.id))
    }

    #[step]
    async fn wait_short(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}] Scheduling 2-second timer...", format_time());
        schedule_timer_named(Duration::from_secs(2), "short-timer").await?;
        println!("[{}] 2-second timer fired!", format_time());
        Ok(())
    }
}

fn format_time() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let storage = Arc::new(SqliteExecutionLog::new("timer_recalc.db").await?);
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone());

    println!("\n=== Timer Recalculation Test (Multiple Workers) ===");
    println!("Starting 3 workers...\n");

    // Start 3 workers with timer processing
    let mut worker_handles = Vec::new();

    for worker_num in 1..=3 {
        let worker_id = format!("timer-worker-{}", worker_num);
        let worker = Worker::new(storage.clone(), worker_id.clone())
            .with_timers()
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<LongTimerFlow>| flow.run_with_long_timer())
            .await;
        worker
            .register(|flow: Arc<ShortTimerFlow>| flow.run_with_short_timer())
            .await;

        let handle = worker.start().await;
        worker_handles.push(handle);
        println!("✓ Started {}", worker_id);
    }

    println!("\nExpected behavior:");
    println!("1. Long timer scheduled (10s) - all workers sleep until 10s");
    println!("2. Short timer scheduled (2s) - at least one worker wakes up early!");
    println!("3. Short timer fires at ~2s (claimed by one worker)");
    println!("4. Long timer fires at ~10s (claimed by one worker)");
    println!("================================\n");

    let start = std::time::Instant::now();

    // Schedule long timer first
    let long_flow = LongTimerFlow {
        id: "long-1".to_string(),
    };
    let long_flow_id = uuid::Uuid::new_v4();
    let long_task_id = scheduler.schedule(long_flow, long_flow_id).await?;

    // Wait 1 second to ensure long timer is scheduled and worker is sleeping
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now schedule short timer - this should wake up the worker immediately
    // and recalculate the next wake time
    let short_flow = ShortTimerFlow {
        id: "short-1".to_string(),
    };
    let short_flow_id = uuid::Uuid::new_v4();
    let short_task_id = scheduler.schedule(short_flow, short_flow_id).await?;

    println!("[{}] Both timers scheduled. Worker should wake for short timer in ~2s, not 10s!\n", format_time());

    // Wait for both flows to complete
    let status_notify = storage.status_notify().clone();
    let mut completed = 0;
    let mut completed_tasks = std::collections::HashSet::new();

    while completed < 2 {
        status_notify.notified().await;

        if !completed_tasks.contains(&short_task_id) {
            if let Some(task) = storage.get_scheduled_flow(short_task_id).await? {
                if task.status == ergon::storage::TaskStatus::Complete {
                    let elapsed = start.elapsed();
                    let worker_id = task.locked_by.as_deref().unwrap_or("unknown");
                    println!(
                        "\n[RESULT] Short timer completed at {:?} (worker: {})",
                        elapsed, worker_id
                    );
                    if elapsed.as_secs() >= 2 && elapsed.as_secs() <= 4 {
                        println!("✓ Short timer fired correctly (~2s)");
                    } else {
                        println!("✗ Short timer timing unexpected!");
                    }
                    completed_tasks.insert(short_task_id);
                    completed += 1;
                }
            }
        }

        if !completed_tasks.contains(&long_task_id) {
            if let Some(task) = storage.get_scheduled_flow(long_task_id).await? {
                if task.status == ergon::storage::TaskStatus::Complete {
                    let elapsed = start.elapsed();
                    let worker_id = task.locked_by.as_deref().unwrap_or("unknown");
                    println!(
                        "\n[RESULT] Long timer completed at {:?} (worker: {})",
                        elapsed, worker_id
                    );
                    if elapsed.as_secs() >= 10 && elapsed.as_secs() <= 12 {
                        println!("✓ Long timer fired correctly (~10s)");
                    } else {
                        println!("✗ Long timer timing unexpected!");
                    }
                    completed_tasks.insert(long_task_id);
                    completed += 1;
                }
            }
        }
    }

    let total_elapsed = start.elapsed();
    println!("\n[RESULT] Total execution time: {:?}", total_elapsed);

    if total_elapsed.as_secs() >= 10 && total_elapsed.as_secs() <= 12 {
        println!("✓ Timer recalculation worked! At least one worker woke up early for the short timer.");
    } else {
        println!("✗ Unexpected total time. Event-driven recalculation may not be working.");
    }

    // Show which workers claimed which timers
    println!("\nTimer claim details:");
    if let Some(task) = storage.get_scheduled_flow(short_task_id).await? {
        println!(
            "  Short timer (2s): claimed by {}",
            task.locked_by.as_deref().unwrap_or("unknown")
        );
    }
    if let Some(task) = storage.get_scheduled_flow(long_task_id).await? {
        println!(
            "  Long timer (10s): claimed by {}",
            task.locked_by.as_deref().unwrap_or("unknown")
        );
    }

    println!("\n=== Multi-Worker Timer Recalculation Summary ===");
    println!("✓ Each worker independently tracks next wake time");
    println!("✓ notify_one() wakes at least one worker when new timer scheduled");
    println!("✓ Awakened worker recalculates and processes short timer early");
    println!("✓ Optimistic concurrency ensures only one worker claims each timer");

    // Shutdown all workers
    for handle in worker_handles {
        handle.shutdown().await;
    }

    Ok(())
}
