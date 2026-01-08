//! Graceful Shutdown Demonstration
//!
//! Workers track background flow execution tasks and wait for them to
//! complete during shutdown, ensuring no work is lost.
//!
//! Scenario:
//! - 10 long-running flows (each takes 3 seconds)
//! - 2 workers processing flows in parallel
//! - Shutdown triggered while flows are still executing
//! - Workers wait for all background tasks to complete before exiting
//!
//! Key Features:
//! - Background task tracking prevents tasks from being dropped
//! - Shutdown waits for in-flight flows to complete
//! - No work is lost during worker shutdown
//! - Clean resource cleanup

use ergon::core::{FlowType, InvokableFlow};
use ergon::executor::{Scheduler, Worker};
use ergon::prelude::*;
use ergon::storage::{SqliteExecutionLog, TaskStatus};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

// =============================================================================
// Long Running Task Flow
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct LongRunningTask {
    task_id: String,
    duration_secs: u64,
}

impl FlowType for LongRunningTask {
    fn type_id() -> &'static str {
        "LongRunningTask"
    }
}

impl InvokableFlow for LongRunningTask {
    type Output = TaskResult;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TaskResult {
    task_id: String,
    status: String,
}

impl LongRunningTask {
    #[flow]
    async fn execute(self: Arc<Self>) -> Result<TaskResult, String> {
        println!(
            "[{}] Starting long-running task ({}s)",
            self.task_id, self.duration_secs
        );

        // Simulate multiple steps of long work
        self.clone().step_1().await?;
        self.clone().step_2().await?;
        self.clone().step_3().await?;

        println!("[{}] Task completed!", self.task_id);
        Ok(TaskResult {
            task_id: self.task_id.clone(),
            status: "completed".to_string(),
        })
    }

    #[step]
    async fn step_1(self: Arc<Self>) -> Result<String, String> {
        println!("[{}] Step 1: Initializing...", self.task_id);
        tokio::time::sleep(Duration::from_secs(self.duration_secs / 3)).await;
        println!("[{}] Step 1: Complete", self.task_id);
        Ok("step1_done".to_string())
    }

    #[step]
    async fn step_2(self: Arc<Self>) -> Result<String, String> {
        println!("[{}] Step 2: Processing...", self.task_id);
        tokio::time::sleep(Duration::from_secs(self.duration_secs / 3)).await;
        println!("[{}] Step 2: Complete", self.task_id);
        Ok("step2_done".to_string())
    }

    #[step]
    async fn step_3(self: Arc<Self>) -> Result<String, String> {
        println!("[{}] Step 3: Finalizing...", self.task_id);
        tokio::time::sleep(Duration::from_secs(self.duration_secs / 3)).await;
        println!("[{}] Step 3: Complete", self.task_id);
        Ok("step3_done".to_string())
    }
}

// =============================================================================
// Main Demo
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Graceful Shutdown Demonstration ===\n");

    // Initialize storage
    let storage = Arc::new(SqliteExecutionLog::new("graceful_shutdown_demo.db").await?);

    // Start 2 workers
    println!("Starting workers...");
    let worker1 =
        Worker::new(storage.clone(), "worker-1").with_poll_interval(Duration::from_millis(100));
    worker1
        .register(|f: Arc<LongRunningTask>| f.execute())
        .await;
    let handle1 = worker1.start().await;

    let worker2 =
        Worker::new(storage.clone(), "worker-2").with_poll_interval(Duration::from_millis(100));
    worker2
        .register(|f: Arc<LongRunningTask>| f.execute())
        .await;
    let handle2 = worker2.start().await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create scheduler and schedule tasks
    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");
    let num_tasks = 10;
    let task_duration = 3;

    println!(
        "Scheduling {} tasks with {}s duration each...\n",
        num_tasks, task_duration
    );
    let start_time = Instant::now();
    let mut task_ids = Vec::new();

    for i in 1..=num_tasks {
        let task = LongRunningTask {
            task_id: format!("TASK-{:03}", i),
            duration_secs: task_duration,
        };
        let task_id = scheduler.schedule(task).await?;
        task_ids.push(task_id);
    }

    // Give workers time to pick up tasks
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Trigger shutdown while tasks are still running
    println!("\n=== Triggering shutdown while tasks are still running ===");
    let shutdown_start = Instant::now();

    handle1.shutdown().await;
    handle2.shutdown().await;

    let shutdown_duration = shutdown_start.elapsed();
    let total_duration = start_time.elapsed();

    // Check results
    println!("\n=== Results ===");
    let mut completed_count = 0;
    let mut failed_count = 0;
    let mut other_count = 0;

    for task_id in &task_ids {
        if let Some(task) = storage.get_scheduled_flow(*task_id).await? {
            match task.status {
                TaskStatus::Complete => completed_count += 1,
                TaskStatus::Failed => failed_count += 1,
                _ => other_count += 1,
            }
        }
    }

    println!("Shutdown duration: {:.2}s", shutdown_duration.as_secs_f64());
    println!("Total duration: {:.2}s", total_duration.as_secs_f64());
    println!("Tasks completed: {}/{}", completed_count, num_tasks);
    println!("Tasks failed: {}/{}", failed_count, num_tasks);
    println!("Tasks in other states: {}/{}", other_count, num_tasks);

    Ok(())
}
