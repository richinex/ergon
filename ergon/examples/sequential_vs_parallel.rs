//! Ergon Execution Model: Parallel vs Sequential
//!
//! - No depends_on: steps run in PARALLEL
//! - Explicit depends_on: steps run in declared order
//!
//! There is NO auto-chaining. Dependencies must be explicit.

use ergon::executor::{ExecutionError, Executor, FlowOutcome};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct Workflow {
    id: String,
}

impl Workflow {
    // =========================================================================
    // SCENARIO 1: Parallel (No depends_on = all run concurrently)
    // =========================================================================

    #[step]
    async fn par_step1(self: Arc<Self>) -> Result<String, String> {
        println!("[Parallel] Step 1 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Parallel] Step 1 finished");
        Ok("step1".to_string())
    }

    #[step]
    async fn par_step2(self: Arc<Self>) -> Result<String, String> {
        println!("[Parallel] Step 2 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Parallel] Step 2 finished");
        Ok("step2".to_string())
    }

    #[step]
    async fn par_step3(self: Arc<Self>) -> Result<String, String> {
        println!("[Parallel] Step 3 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Parallel] Step 3 finished");
        Ok("step3".to_string())
    }

    #[flow]
    async fn run_parallel(self: Arc<Self>) -> Result<String, ExecutionError> {
        dag! {
            self.register_par_step1();
            self.register_par_step2();
            self.register_par_step3()
        }
    }

    // =========================================================================
    // SCENARIO 2: Sequential (Explicit depends_on chains steps)
    // =========================================================================

    #[step]
    async fn seq_step1(self: Arc<Self>) -> Result<String, String> {
        println!("[Sequential] Step 1 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Sequential] Step 1 finished");
        Ok("step1".to_string())
    }

    #[step(depends_on = "seq_step1")]
    async fn seq_step2(self: Arc<Self>) -> Result<String, String> {
        println!("[Sequential] Step 2 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Sequential] Step 2 finished");
        Ok("step2".to_string())
    }

    #[step(depends_on = "seq_step2")]
    async fn seq_step3(self: Arc<Self>) -> Result<String, String> {
        println!("[Sequential] Step 3 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Sequential] Step 3 finished");
        Ok("step3".to_string())
    }

    #[flow]
    async fn run_sequential(self: Arc<Self>) -> Result<String, ExecutionError> {
        dag! {
            self.register_seq_step1();
            self.register_seq_step2();
            self.register_seq_step3()
        }
    }

    // =========================================================================
    // SCENARIO 3: DAG (Explicit depends_on enables fan-out/fan-in)
    //
    // root ──┬── branch1 ──┬── merge
    //        └── branch2 ──┘
    // =========================================================================

    #[step]
    async fn dag_root(self: Arc<Self>) -> Result<String, String> {
        println!("[DAG] Root starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[DAG] Root finished");
        Ok("root".to_string())
    }

    #[step(depends_on = "dag_root")]
    async fn dag_branch1(self: Arc<Self>) -> Result<i32, String> {
        println!("[DAG] Branch 1 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[DAG] Branch 1 finished");
        Ok(10)
    }

    #[step(depends_on = "dag_root")]
    async fn dag_branch2(self: Arc<Self>) -> Result<i32, String> {
        println!("[DAG] Branch 2 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[DAG] Branch 2 finished");
        Ok(20)
    }

    #[step(
        depends_on = ["dag_branch1", "dag_branch2"],
        inputs(a = "dag_branch1", b = "dag_branch2")
    )]
    async fn dag_merge(self: Arc<Self>, a: i32, b: i32) -> Result<String, String> {
        println!("[DAG] Merge starting (a={}, b={})", a, b);
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[DAG] Merge finished: {}", a + b);
        Ok(format!("merged: {}", a + b))
    }

    #[flow]
    async fn run_dag(self: Arc<Self>) -> Result<String, ExecutionError> {
        dag! {
            self.register_dag_root();
            self.register_dag_branch1();
            self.register_dag_branch2();
            self.register_dag_merge()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Scenario 1: Parallel
    println!("\n=== SCENARIO 1: Parallel (No depends_on) ===\n");

    let storage1 = Arc::new(InMemoryExecutionLog::new());
    storage1.reset().await?;
    let workflow1 = Arc::new(Workflow {
        id: "parallel".into(),
    });

    let start1 = Instant::now();
    let executor1 = Executor::new(Uuid::new_v4(), workflow1.clone(), storage1);
    let result1 = match executor1
        .execute(|w| Box::pin(w.clone().run_parallel()))
        .await
    {
        FlowOutcome::Completed(r) => r,
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    };
    let elapsed1 = start1.elapsed();

    println!("\nResult: {:?}", result1);
    println!("Time: {:?}", elapsed1);

    // Scenario 2: Sequential
    println!("\n=== SCENARIO 2: Sequential (Explicit depends_on) ===\n");

    let storage2 = Arc::new(InMemoryExecutionLog::new());
    storage2.reset().await?;
    let workflow2 = Arc::new(Workflow {
        id: "sequential".into(),
    });

    let start2 = Instant::now();
    let executor2 = Executor::new(Uuid::new_v4(), workflow2.clone(), storage2);
    let result2 = match executor2
        .execute(|w| Box::pin(w.clone().run_sequential()))
        .await
    {
        FlowOutcome::Completed(r) => r,
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    };
    let elapsed2 = start2.elapsed();

    println!("\nResult: {:?}", result2);
    println!("Time: {:?}", elapsed2);

    // Scenario 3: DAG
    println!("\n=== SCENARIO 3: DAG (Fan-out/Fan-in) ===\n");

    let storage3 = Arc::new(InMemoryExecutionLog::new());
    storage3.reset().await?;
    let workflow3 = Arc::new(Workflow { id: "dag".into() });

    let start3 = Instant::now();
    let executor3 = Executor::new(Uuid::new_v4(), workflow3.clone(), storage3);
    let result3 = match executor3.execute(|w| Box::pin(w.clone().run_dag())).await {
        FlowOutcome::Completed(r) => r,
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    };
    let elapsed3 = start3.elapsed();

    println!("\nResult: {:?}", result3);
    println!("Time: {:?}", elapsed3);

    // Summary
    println!("\n=== Summary ===\n");
    println!(
        "| {:<10} | {:<12} | {:<10} |",
        "Scenario", "depends_on", "Time"
    );
    println!("|{:-<12}|{:-<14}|{:-<12}|", "", "", "");
    println!(
        "| {:<10} | {:<12} | {:>10.2?} |",
        "Parallel", "None", elapsed1
    );
    println!(
        "| {:<10} | {:<12} | {:>10.2?} |",
        "Sequential", "Chained", elapsed2
    );
    println!(
        "| {:<10} | {:<12} | {:>10.2?} |",
        "DAG", "Fan-out", elapsed3
    );

    println!("\nKey: depends_on is explicit. No auto-chaining.");

    Ok(())
}
