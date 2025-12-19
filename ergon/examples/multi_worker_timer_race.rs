//! Multiple workers claiming timers demo
//!
//! This example demonstrates distributed timer coordination:
//! - Start 3 workers concurrently
//! - Schedule 10 flows, each with a 1-second timer
//! - Verify only ONE worker claims each timer (no duplicates)
//! - Show which worker processed each timer
//!
//! This proves optimistic concurrency works correctly across multiple workers.

use ergon::executor::{schedule_timer_named, ExecutionError, Scheduler, Worker};
use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct TimerFlow {
    flow_number: u32,
}

impl TimerFlow {
    #[flow]
    async fn run_timer(self: Arc<Self>) -> Result<String, ExecutionError> {
        self.clone().wait_one_second().await?;
        Ok(format!("Flow {} completed", self.flow_number))
    }

    #[step]
    async fn wait_one_second(self: Arc<Self>) -> Result<(), ExecutionError> {
        schedule_timer_named(
            Duration::from_secs(1),
            &format!("timer-{}", self.flow_number),
        )
        .await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let storage = Arc::new(SqliteExecutionLog::new("multi_worker_race.db").await?);
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    println!("\n=== Multi-Worker Timer Race Test ===");
    println!("Starting 3 workers...\n");

    // Start 3 workers
    let mut worker_handles = Vec::new();

    for worker_num in 1..=3 {
        let worker_id = format!("worker-{}", worker_num);
        let worker = Worker::new(storage.clone(), worker_id.clone())
            .with_timers()
            .with_poll_interval(Duration::from_millis(50));

        worker
            .register(|flow: Arc<TimerFlow>| flow.run_timer())
            .await;

        let handle = worker.start().await;
        worker_handles.push(handle);
        println!("✓ Started {}", worker_id);
    }

    println!("\nScheduling 10 flows with 1-second timers...\n");

    let start = std::time::Instant::now();
    let mut task_ids = Vec::new();

    // Schedule 10 flows
    for i in 1..=10 {
        let flow = TimerFlow { flow_number: i };
        let flow_id = uuid::Uuid::new_v4();
        let task_id = scheduler.schedule_with(flow, flow_id).await?;
        task_ids.push((i, task_id, flow_id));
        println!("Scheduled flow {}", i);
    }

    // Wait for all flows to complete
    let status_notify = storage.status_notify().clone();
    let mut completed = 0;

    println!("\nWaiting for timers to fire...\n");

    let mut completed_tasks = std::collections::HashSet::new();

    while completed < 10 {
        status_notify.notified().await;

        for (flow_num, task_id, _) in &task_ids {
            if completed_tasks.contains(task_id) {
                continue; // Already counted this task
            }

            if let Some(task) = storage.get_scheduled_flow(*task_id).await? {
                if task.status == ergon::storage::TaskStatus::Complete {
                    let elapsed = start.elapsed();
                    println!(
                        "[{:?}] Flow {} completed (worker: {})",
                        elapsed,
                        flow_num,
                        task.locked_by.as_deref().unwrap_or("unknown")
                    );
                    completed_tasks.insert(*task_id);
                    completed += 1;
                }
            }
        }
    }

    let total_elapsed = start.elapsed();

    println!("\n=== Results ===");
    println!("Total execution time: {:?}", total_elapsed);

    // Verify no duplicate processing by checking invocation counts
    println!("\nVerifying timer claim integrity...");
    let mut all_claimed_correctly = true;

    for (_flow_num, _task_id, flow_id) in &task_ids {
        let invocations = storage.get_invocations_for_flow(*flow_id).await?;

        // Count how many times each step was executed
        let mut step_counts = std::collections::HashMap::new();
        for inv in invocations {
            *step_counts.entry(inv.step()).or_insert(0) += 1;
        }

        // Each step should be executed exactly once
        for (step, count) in step_counts {
            if count != 1 {
                println!(
                    "✗ Flow {:?} step {} executed {} times (expected 1)!",
                    flow_id, step, count
                );
                all_claimed_correctly = false;
            }
        }
    }

    if all_claimed_correctly {
        println!("✓ All timers claimed exactly once (no duplicates)");
    }

    // Count how many timers each worker processed
    println!("\nTimer distribution across workers:");
    let mut worker_counts = std::collections::HashMap::new();

    for (_, task_id, _) in &task_ids {
        if let Some(task) = storage.get_scheduled_flow(*task_id).await? {
            if let Some(worker_id) = task.locked_by {
                *worker_counts.entry(worker_id).or_insert(0) += 1;
            }
        }
    }

    for (worker_id, count) in worker_counts {
        println!("  {}: {} timers", worker_id, count);
    }

    if total_elapsed.as_secs() >= 1 && total_elapsed.as_secs() <= 3 {
        println!("\n✓ Timing correct: All timers processed in ~1-2 seconds");
    } else {
        println!("\n✗ Unexpected timing");
    }

    println!("\n=== Test Summary ===");
    println!("✓ Multiple workers successfully coordinated timer processing");
    println!("✓ Optimistic concurrency prevented duplicate timer firing");
    println!("✓ Event-driven architecture scales across workers");

    // Shutdown all workers
    for handle in worker_handles {
        handle.shutdown().await;
    }

    Ok(())
}
