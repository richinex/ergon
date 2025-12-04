//! Comprehensive Example: Sequential by Default vs Explicit Parallelism
//!
//! This example demonstrates the new execution model:
//! 1. Steps WITHOUT depends_on run SEQUENTIALLY (auto-chained)
//! 2. Steps WITH explicit depends_on enable PARALLEL execution

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
    fn new(id: String) -> Self {
        Self { id }
    }

    // =========================================================================
    // SCENARIO 1: Sequential Execution (No depends_on)
    // =========================================================================

    #[step]
    async fn seq_step1(self: Arc<Self>) -> Result<String, String> {
        println!("[Sequential] Step 1 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Sequential] Step 1 finished");
        Ok("step1".to_string())
    }

    #[step] // Auto-depends on seq_step1
    async fn seq_step2(self: Arc<Self>) -> Result<String, String> {
        println!("[Sequential] Step 2 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Sequential] Step 2 finished");
        Ok("step2".to_string())
    }

    #[step] // Auto-depends on seq_step2
    async fn seq_step3(self: Arc<Self>) -> Result<String, String> {
        println!("[Sequential] Step 3 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Sequential] Step 3 finished");
        Ok("step3".to_string())
    }

    #[flow]
    async fn run_sequential(self: Arc<Self>) -> Result<String, String> {
        dag! {
            self.register_seq_step1();
            self.register_seq_step2(); // Waits for step1
            self.register_seq_step3() // Waits for step2
        }
    }

    // =========================================================================
    // SCENARIO 2: Parallel Execution (Explicit depends_on)
    // =========================================================================

    #[step]
    async fn par_root(self: Arc<Self>) -> Result<String, String> {
        println!("[Parallel] Root starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Parallel] Root finished");
        Ok("root".to_string())
    }

    #[step(depends_on = "par_root")] // Explicit: depends on root
    async fn par_branch1(self: Arc<Self>) -> Result<String, String> {
        println!("[Parallel] Branch 1 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Parallel] Branch 1 finished");
        Ok("branch1".to_string())
    }

    #[step(depends_on = "par_root")] // Explicit: ALSO depends on root (parallel!)
    async fn par_branch2(self: Arc<Self>) -> Result<String, String> {
        println!("[Parallel] Branch 2 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Parallel] Branch 2 finished");
        Ok("branch2".to_string())
    }

    #[step(depends_on = ["par_branch1", "par_branch2"])] // Waits for both
    async fn par_merge(self: Arc<Self>) -> Result<String, String> {
        println!("[Parallel] Merge starting (after both branches)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Parallel] Merge finished");
        Ok("merged".to_string())
    }

    #[flow]
    async fn run_parallel(self: Arc<Self>) -> Result<String, String> {
        dag! {
            self.register_par_root();
            self.register_par_branch1(); // Parallel with branch2
            self.register_par_branch2(); // Parallel with branch1
            self.register_par_merge() // Waits for both branches
        }
    }

    // =========================================================================
    // SCENARIO 3: Using inputs for Data Wiring
    // =========================================================================

    #[step]
    async fn fetch_data(self: Arc<Self>) -> Result<i32, String> {
        println!("[Inputs] Fetching data");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Inputs] Data fetched: 42");
        Ok(42)
    }

    #[step(depends_on = "fetch_data", inputs(value = "fetch_data"))]
    async fn process_data(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!("[Inputs] Processing data: {}", value);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = value * 2;
        println!("[Inputs] Processed result: {}", result);
        Ok(result)
    }

    #[step(depends_on = "process_data", inputs(value = "process_data"))]
    async fn save_result(self: Arc<Self>, value: i32) -> Result<String, String> {
        println!("[Inputs] Saving result: {}", value);
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[Inputs] Result saved");
        Ok(format!("Saved: {}", value))
    }

    #[flow]
    async fn run_with_inputs(self: Arc<Self>) -> Result<String, String> {
        dag! {
            self.register_fetch_data();
            self.register_process_data();
            self.register_save_result()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // =========================================================================
    // Test 1: Sequential Execution
    // =========================================================================
    println!("\n=== SCENARIO 1: Sequential Execution (Default) ===\n");
    println!("Steps run one after another in registration order.\n");

    let storage1 = Arc::new(InMemoryExecutionLog::new());
    storage1.reset().await?;
    let workflow1 = Arc::new(Workflow::new("seq".to_string()));
    let flow_id1 = Uuid::new_v4();

    let start1 = Instant::now();
    let instance1 = Executor::new(flow_id1, Arc::clone(&workflow1), Arc::clone(&storage1));
    let result1 = instance1
        .execute(|f| Box::pin(f.clone().run_sequential()))
        .await;
    let elapsed1 = start1.elapsed();

    println!("\nResult: {:?}", result1);
    println!(
        "Time: {:?} (expect ~150ms for 3 sequential steps)",
        elapsed1
    );

    // =========================================================================
    // Test 2: Parallel Execution
    // =========================================================================
    println!("\n=== SCENARIO 2: Parallel Execution (Explicit) ===\n");
    println!("Branch1 and Branch2 run in parallel after Root.\n");

    let storage2 = Arc::new(InMemoryExecutionLog::new());
    storage2.reset().await?;
    let workflow2 = Arc::new(Workflow::new("par".to_string()));
    let flow_id2 = Uuid::new_v4();

    let start2 = Instant::now();
    let instance2 = Executor::new(flow_id2, Arc::clone(&workflow2), Arc::clone(&storage2));
    let result2 = instance2
        .execute(|f| Box::pin(f.clone().run_parallel()))
        .await;
    let elapsed2 = start2.elapsed();

    println!("\nResult: {:?}", result2);
    println!(
        "Time: {:?} (expect ~150ms: root(50) + parallel_branches(50) + merge(50))",
        elapsed2
    );

    // =========================================================================
    // Test 3: Data Wiring with inputs
    // =========================================================================
    println!("\n=== SCENARIO 3: Data Wiring with inputs ===\n");
    println!("Steps pass data through inputs attribute.\n");

    let storage3 = Arc::new(InMemoryExecutionLog::new());
    storage3.reset().await?;
    let workflow3 = Arc::new(Workflow::new("inputs".to_string()));
    let flow_id3 = Uuid::new_v4();

    let instance3 = Executor::new(flow_id3, Arc::clone(&workflow3), Arc::clone(&storage3));
    let result3 = instance3
        .execute(|f| Box::pin(f.clone().run_with_inputs()))
        .await?;

    println!("\nResult: {:?}", result3);

    println!("\n=== Summary ===\n");
    println!("1. Without depends_on: Sequential (safe default)");
    println!("2. With explicit depends_on: Parallel (opt-in)");
    println!("3. Use inputs for data wiring between steps");

    Ok(())
}
