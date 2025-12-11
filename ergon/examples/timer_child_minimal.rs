//! Minimal Timer + Child Flow + Error Test
//!
//! Tests if timers in child flows correctly propagate errors to parent.

use ergon::core::{FlowType, InvokableFlow, RetryableError};
use ergon::executor::{schedule_timer_named, InvokeChild, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// =============================================================================
// Custom Error
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TaskError {
    Timeout,
    Failed(String),
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskError::Timeout => write!(f, "Task timed out"),
            TaskError::Failed(msg) => write!(f, "Task failed: {}", msg),
        }
    }
}

impl std::error::Error for TaskError {}

impl From<TaskError> for String {
    fn from(err: TaskError) -> Self {
        err.to_string()
    }
}

impl RetryableError for TaskError {
    fn is_retryable(&self) -> bool {
        match self {
            TaskError::Timeout => true,
            TaskError::Failed(_) => false, // Non-retryable
        }
    }
}

impl From<ExecutionError> for TaskError {
    fn from(e: ExecutionError) -> Self {
        match e {
            ExecutionError::Suspended(_) => TaskError::Timeout,
            ExecutionError::NonRetryable(msg) => TaskError::Failed(msg),
            _ => TaskError::Timeout,
        }
    }
}

// =============================================================================
// Child Flow with Timer
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct ChildTask {
    should_fail: bool,
}

impl FlowType for ChildTask {
    fn type_id() -> &'static str {
        "ChildTask"
    }
}

impl InvokableFlow for ChildTask {
    type Output = String;
}

impl ChildTask {
    #[flow]
    async fn execute(self: Arc<Self>) -> Result<String, TaskError> {
        println!("  [CHILD] Starting task...");

        // Step with timer
        self.clone().wait_step().await?;

        println!("  [CHILD] Timer completed, checking if should fail...");

        if self.should_fail {
            println!("  [CHILD] Returning error!");
            return Err(TaskError::Failed("Simulated failure".to_string()));
        }

        println!("  [CHILD] Task succeeded!");
        Ok("Success".to_string())
    }

    #[step]
    async fn wait_step(self: Arc<Self>) -> Result<(), TaskError> {
        println!("  [CHILD] Waiting 2 seconds...");
        schedule_timer_named(Duration::from_secs(2), "child-wait")
            .await
            .map_err(TaskError::from)?;
        println!("  [CHILD] Wait complete!");
        Ok(())
    }
}

// =============================================================================
// Parent Flow
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct ParentTask {
    test_name: String,
    child_should_fail: bool,
}

impl ParentTask {
    #[flow]
    async fn run(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("\n[PARENT] {} - Starting...", self.test_name);

        let result = self
            .invoke(ChildTask {
                should_fail: self.child_should_fail,
            })
            .result()
            .await;

        match result {
            Ok(msg) => {
                println!("[PARENT] {} - Child succeeded: {}", self.test_name, msg);
                Ok(format!("Parent: child succeeded with {}", msg))
            }
            Err(e) => {
                println!("[PARENT] {} - Child failed: {}", self.test_name, e);
                Err(e) // Return ExecutionError as-is to preserve retryability
            }
        }
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nMinimal Timer + Child Flow + Error Test");
    println!("========================================\n");

    let storage = Arc::new(SqliteExecutionLog::new("timer_child_minimal.db").await?);
    storage.reset().await?;

    let worker = Worker::new(storage.clone(), "test-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(50))
        .with_poll_interval(Duration::from_millis(50));

    worker
        .register(|flow: Arc<ParentTask>| flow.run())
        .await;
    worker
        .register(|flow: Arc<ChildTask>| flow.execute())
        .await;

    let worker = worker.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let scheduler = ergon::executor::Scheduler::new(storage.clone());

    // Test 1: Child succeeds
    println!("=== Test 1: Child Succeeds ===");
    let task1 = ParentTask {
        test_name: "Test1".to_string(),
        child_should_fail: false,
    };
    let task_id_1 = scheduler.schedule(task1, Uuid::new_v4()).await?;

    // Wait for completion
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            println!("[TIMEOUT] Test 1 did not complete");
            break;
        }
        match storage.get_scheduled_flow(task_id_1).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                    println!("Test 1 final status: {:?}\n", scheduled.status);
                    break;
                }
            }
            None => break,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Test 2: Child fails with non-retryable error
    println!("=== Test 2: Child Fails (Non-Retryable) ===");
    let task2 = ParentTask {
        test_name: "Test2".to_string(),
        child_should_fail: true,
    };
    let task_id_2 = scheduler.schedule(task2, Uuid::new_v4()).await?;

    // Wait for completion
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            println!("[TIMEOUT] Test 2 did not complete");
            break;
        }
        match storage.get_scheduled_flow(task_id_2).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                    println!("Test 2 final status: {:?}\n", scheduled.status);
                    break;
                }
            }
            None => break,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("=== All Tests Complete ===\n");

    worker.shutdown().await;
    storage.close().await?;

    Ok(())
}
