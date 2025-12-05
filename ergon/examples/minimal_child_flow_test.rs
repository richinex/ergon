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
    /// Single step that invokes child
    #[step]
    async fn invoke_child_task(self: Arc<Self>) -> Result<String, ExecutionError> {
        let count = PARENT_STEP_COUNT.fetch_add(1, Ordering::Relaxed) + 1;

        println!(
            "[{:.3}]   [{}] invoke_child_task (attempt #{})",
            timestamp(),
            self.flow_id,
            count
        );

        let result = self
            .invoke(ChildTask {
                task_id: format!("CHILD-{}", self.flow_id),
            })
            .result()
            .await?;

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
            "\n[{:.3}] PARENT[{}] flow_id={}: START #{} ================================",
            timestamp(),
            self.flow_id,
            &flow_id.to_string()[..8],
            count
        );

        let result = self.clone().invoke_child_task().await?;

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
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║           Minimal Child Flow Test (SQLite)               ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");
    println!("MINIMAL test: ONE step, NO retries, NO DAG, NO errors\n");

    let storage = Arc::new(SqliteExecutionLog::new(":memory:").await?);
    storage.reset().await?;

    let worker = Worker::new(storage.clone(), "test-worker")
        .with_poll_interval(Duration::from_millis(50));

    worker
        .register(|flow: Arc<ParentFlow>| flow.run())
        .await;
    worker
        .register(|flow: Arc<ChildTask>| flow.execute())
        .await;

    let handle = worker.start().await;

    let scheduler = ergon::executor::Scheduler::new(storage);
    let parent = ParentFlow {
        flow_id: "TEST".to_string(),
    };

    scheduler.schedule(parent, uuid::Uuid::new_v4()).await?;

    tokio::time::sleep(Duration::from_secs(5)).await;
    handle.shutdown().await;

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║                         Results                           ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");
    println!("Parent start count:    {}", PARENT_START_COUNT.load(Ordering::Relaxed));
    println!("Parent step count:     {}", PARENT_STEP_COUNT.load(Ordering::Relaxed));
    println!("Child executions:      {}", CHILD_EXEC_COUNT.load(Ordering::Relaxed));

    println!("\nExpected (minimal case: parent invokes child, no retries):");
    println!("  Parent starts:    2 (initial + resume after child)");
    println!("  Parent step:      2 (invoke child + get cached result)");
    println!("  Child executions: 1 (actual work)");

    let parent_count = PARENT_START_COUNT.load(Ordering::Relaxed);
    let parent_step = PARENT_STEP_COUNT.load(Ordering::Relaxed);
    let child_count = CHILD_EXEC_COUNT.load(Ordering::Relaxed);

    println!("\nAnalysis:");
    if parent_count == 2 && parent_step == 2 && child_count == 1 {
        println!("✅ PERFECT: This is the MINIMAL expected behavior");
        println!("   - Parent suspends when invoking child (start #1)");
        println!("   - Parent resumes when child completes (start #2)");
        println!("   - Child executes once");
        println!("   - Parent step runs twice (invoke + cached result)");
    } else {
        println!("⚠️  UNEXPECTED BEHAVIOR:");
        if parent_count != 2 {
            println!("   - Parent starts: {} (expected 2)", parent_count);
        }
        if parent_step != 2 {
            println!("   - Parent step: {} (expected 2)", parent_step);
        }
        if child_count != 1 {
            println!("   - Child executions: {} (expected 1)", child_count);
        }
    }

    Ok(())
}
