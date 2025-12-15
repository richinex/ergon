//! Ergon Execution Model: Parallel vs Sequential vs DAG
//!
//! This example demonstrates three execution patterns for the SAME work:
//! - Scenario 1: Pure parallel (no coordination)
//! - Scenario 2: Pure sequential (full chain)
//! - Scenario 3: DAG (smart parallelization with coordination)
//!
//! Key principle: Dependencies must be EXPLICIT via depends_on.
//! There is NO auto-chaining.

use ergon::executor::{ExecutionError, Executor, FlowOutcome};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
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
    // SCENARIO 3: DAG (Smart parallelization with coordination)
    //
    // Topology:  step1 ──┬── step2
    //                     └── step3
    //
    // Step 1 runs first, then steps 2 & 3 run in parallel
    // =========================================================================

    #[step]
    async fn dag_step1(self: Arc<Self>) -> Result<String, String> {
        println!("[DAG] Step 1 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[DAG] Step 1 finished");
        Ok("step1".to_string())
    }

    #[step(depends_on = "dag_step1")]
    async fn dag_step2(self: Arc<Self>) -> Result<String, String> {
        println!("[DAG] Step 2 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[DAG] Step 2 finished");
        Ok("step2".to_string())
    }

    #[step(depends_on = "dag_step1")]
    async fn dag_step3(self: Arc<Self>) -> Result<String, String> {
        println!("[DAG] Step 3 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[DAG] Step 3 finished");
        Ok("step3".to_string())
    }

    #[flow]
    async fn run_dag(self: Arc<Self>) -> Result<String, ExecutionError> {
        dag! {
            self.register_dag_step1();
            self.register_dag_step2();
            self.register_dag_step3()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Scenario 1: Parallel

    let storage1 = Arc::new(InMemoryExecutionLog::new());
    storage1.reset().await?;
    let workflow1 = Arc::new(Workflow {
        id: "parallel".into(),
    });

    let executor1 = Executor::new(Uuid::new_v4(), workflow1.clone(), storage1);
    match executor1
        .execute(|w| Box::pin(w.clone().run_parallel()))
        .await
    {
        FlowOutcome::Completed(_) => {},
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    };

    // Scenario 2: Sequential

    let storage2 = Arc::new(InMemoryExecutionLog::new());
    storage2.reset().await?;
    let workflow2 = Arc::new(Workflow {
        id: "sequential".into(),
    });

    let executor2 = Executor::new(Uuid::new_v4(), workflow2.clone(), storage2);
    match executor2
        .execute(|w| Box::pin(w.clone().run_sequential()))
        .await
    {
        FlowOutcome::Completed(_) => {},
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    };

    // Scenario 3: DAG

    let storage3 = Arc::new(InMemoryExecutionLog::new());
    storage3.reset().await?;
    let workflow3 = Arc::new(Workflow { id: "dag".into() });

    let executor3 = Executor::new(Uuid::new_v4(), workflow3.clone(), storage3);
    match executor3.execute(|w| Box::pin(w.clone().run_dag())).await {
        FlowOutcome::Completed(_) => {},
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    };

    Ok(())
}
