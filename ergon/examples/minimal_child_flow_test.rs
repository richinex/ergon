//! Minimal Child Flow Test - No Retries, No DAG
//!
//! This is the MINIMAL test case to understand flow restart behavior.
//!
//! Flow structure:
//! - Parent has ONE step that invokes a child
//! - NO retries
//! - NO DAG
//! - NO errors
//!
//! Expected behavior:
//! 1. Parent starts → invokes child → suspends
//! 2. Child executes and completes
//! 3. Parent resumes → gets cached child result → completes
//!
//! Expected counts:
//! - Parent starts: 2 (initial invocation + resume after child)
//! - Parent step: 2 (invoke + get cached result)
//! - Child executions: 1 (actual work)

use chrono::Utc;
use ergon::core::{FlowType, InvokableFlow};
use ergon::executor::{ExecutionError, InvokeChild, Worker};
use ergon::prelude::*;
use ergon::storage::SqliteExecutionLog;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

static PARENT_START_COUNT: AtomicU32 = AtomicU32::new(0);
static PARENT_STEP_COUNT: AtomicU32 = AtomicU32::new(0);
static CHILD_EXEC_COUNT: AtomicU32 = AtomicU32::new(0);

fn timestamp() -> f64 {
    Utc::now().timestamp_millis() as f64 / 1000.0
}

// =============================================================================
// Child Flow
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChildTask {
    task_id: String,
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
    async fn execute(self: Arc<Self>) -> Result<String, ExecutionError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        let count = CHILD_EXEC_COUNT.fetch_add(1, Ordering::Relaxed) + 1;

        println!(
            "[{:.3}]   CHILD[{}] flow_id={}: Executing (execution #{})",
            timestamp(),
            self.task_id,
            &flow_id.to_string()[..8],
            count
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        println!(
            "[{:.3}]   CHILD[{}]: Complete - returning result",
            timestamp(),
            self.task_id
        );

        Ok(format!("RESULT-{}", self.task_id))
    }
}

// =============================================================================
// Parent Flow with ONE step
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ParentFlow {
    flow_id: String,
}

impl FlowType for ParentFlow {
    fn type_id() -> &'static str {
        "ParentFlow"
    }
}

impl ParentFlow {
    /// Process child result atomically
    #[step]
    async fn process_child_result(
        self: Arc<Self>,
        result: String,
    ) -> Result<String, ExecutionError> {
        let count = PARENT_STEP_COUNT.fetch_add(1, Ordering::Relaxed) + 1;

        println!(
            "[{:.3}]   [{}] process_child_result (attempt #{})",
            timestamp(),
            self.flow_id,
            count
        );

        println!(
            "[{:.3}]   [{}] Received child result: {}",
            timestamp(),
            self.flow_id,
            result
        );

        Ok(result)
    }

    #[flow]
    async fn run(self: Arc<Self>) -> Result<String, ExecutionError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        let count = PARENT_START_COUNT.fetch_add(1, Ordering::Relaxed) + 1;

        println!(
            "[{:.3}]   [{}] Starting parent flow (attempt #{})",
            timestamp(),
            self.flow_id,
            count
        );

        // Invoke child at flow level
        let result = self
            .invoke(ChildTask {
                task_id: format!("CHILD-{}", self.flow_id),
            })
            .result()
            .await?;

        println!(
            "\n[{:.3}] PARENT[{}] flow_id={}: START #{} ================================",
            timestamp(),
            self.flow_id,
            &flow_id.to_string()[..8],
            count
        );

        // Process result in atomic step
        let result = self.clone().process_child_result(result).await?;

        println!(
            "[{:.3}] PARENT[{}] flow_id={}: COMPLETE ================================\n",
            timestamp(),
            self.flow_id,
            &flow_id.to_string()[..8]
        );

        Ok(result)
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(SqliteExecutionLog::new(":memory:").await?);
    storage.reset().await?;

    let worker =
        Worker::new(storage.clone(), "test-worker").with_poll_interval(Duration::from_millis(50));

    worker.register(|flow: Arc<ParentFlow>| flow.run()).await;
    worker.register(|flow: Arc<ChildTask>| flow.execute()).await;

    let handle = worker.start().await;

    let scheduler = ergon::executor::Scheduler::new(storage);
    let parent = ParentFlow {
        flow_id: "TEST".to_string(),
    };

    scheduler.schedule(parent, uuid::Uuid::new_v4()).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;
    handle.shutdown().await;

    Ok(())
}
