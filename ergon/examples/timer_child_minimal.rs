//! Minimal Timer + Child Flow + Error Test
//!
//! Tests if timers in child flows correctly propagate errors to parent.

use ergon::core::{FlowType, InvokableFlow};
use ergon::executor::{schedule_timer_named, InvokeChild, Worker};
use ergon::prelude::*;
use ergon::Retryable;
use ergon::TaskStatus;
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

impl Retryable for TaskError {
    fn is_retryable(&self) -> bool {
        match self {
            TaskError::Timeout => true,
            TaskError::Failed(_) => false,
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
        // Step only handles suspension
        self.clone().wait_step().await?;

        // All logging and business logic AFTER the step
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
        // Minimal step: only the suspension point
        schedule_timer_named(Duration::from_secs(2), "child-wait")
            .await
            .map_err(|_| TaskError::Timeout)
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
    async fn run(self: Arc<Self>) -> Result<String, TaskError> {
        // Invoke child and wait for result
        let child_result = self
            .invoke(ChildTask {
                should_fail: self.child_should_fail,
            })
            .result()
            .await;

        // All logging AFTER the await
        match child_result {
            Ok(result) => {
                println!("[PARENT] {} - Child succeeded: {}", self.test_name, result);
                Ok(format!("Parent: child succeeded with {}", result))
            }
            Err(e) => {
                println!("[PARENT] {} - Child failed: {}", self.test_name, e);
                Err(TaskError::Failed(e.to_string()))
            }
        }
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(SqliteExecutionLog::new("timer_child_minimal.db").await?);
    storage.reset().await?;

    let worker = Worker::new(storage.clone(), "test-worker")
        .with_timers()
        .with_poll_interval(Duration::from_millis(50));

    worker.register(|flow: Arc<ParentTask>| flow.run()).await;
    worker.register(|flow: Arc<ChildTask>| flow.execute()).await;

    let worker = worker.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let scheduler = ergon::executor::Scheduler::new(storage.clone()).with_version("v1.0");

    // Test 1: Child should succeed
    println!("=== Scheduling Test1 (child should succeed) ===");
    let task1 = ParentTask {
        test_name: "Test1".to_string(),
        child_should_fail: false,
    };
    let task_id_1 = scheduler.schedule_with(task1, Uuid::new_v4()).await?;

    // Use storage's race-condition-free wait_for_completion (no polling!)
    let status1 = tokio::time::timeout(
        Duration::from_secs(10),
        storage.wait_for_completion(task_id_1),
    )
    .await
    .ok()
    .and_then(Result::ok);
    println!("=== Test1 final status: {:?} ===\n", status1);

    // Test 2: Child should fail
    println!("=== Scheduling Test2 (child should fail) ===");
    let task2 = ParentTask {
        test_name: "Test2".to_string(),
        child_should_fail: true,
    };
    let task_id_2 = scheduler.schedule_with(task2, Uuid::new_v4()).await?;

    // Use storage's race-condition-free wait_for_completion (no polling!)
    let status2 = tokio::time::timeout(
        Duration::from_secs(10),
        storage.wait_for_completion(task_id_2),
    )
    .await
    .ok()
    .and_then(Result::ok);
    println!("=== Test2 final status: {:?} ===\n", status2);

    // Verify results
    println!("=== Summary ===");
    println!(
        "Test1 (should succeed): {:?} - {}",
        status1,
        if matches!(status1, Some(TaskStatus::Complete)) {
            "PASS"
        } else {
            "FAIL"
        }
    );
    println!(
        "Test2 (should fail): {:?} - {}",
        status2,
        if matches!(status2, Some(TaskStatus::Failed)) {
            "PASS"
        } else {
            "FAIL"
        }
    );

    worker.shutdown().await;
    storage.close().await?;

    Ok(())
}
// ```

// ## Changes Made

// 1. **Removed logging before suspension points** - no more "Starting task..." or "Waiting 2 seconds..." that would duplicate on replay

// 2. **Step is minimal** - `wait_step` contains only the timer, nothing else

// 3. **All logging after suspension** - both child and parent log only after their respective awaits complete

// ## Expected Output
// ```
// === Scheduling Test1 (child should succeed) ===
//   [CHILD] Timer completed, checking if should fail...
//   [CHILD] Task succeeded!
// [PARENT] Test1 - Child succeeded: Success
// === Test1 final status: Some(Complete) ===

// === Scheduling Test2 (child should fail) ===
//   [CHILD] Timer completed, checking if should fail...
//   [CHILD] Returning error!
// [PARENT] Test2 - Child failed: timer_child_minimal::TaskError: Task failed: Simulated failure
// === Test2 final status: Some(Failed) ===

// === Summary ===
// Test1 (should succeed): Some(Complete) - PASS
// Test2 (should fail): Some(Failed) - PASS
