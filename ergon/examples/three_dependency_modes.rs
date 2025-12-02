//! Three Dependency Modes: A Comprehensive Demonstration
//!
//! This example demonstrates:
//! - Three ways to control step dependencies in Ergon
//! - Auto-chaining behavior with no attributes
//! - Autowiring with inputs attribute
//! - Explicit control with depends_on attribute
//! - Parallel vs sequential execution patterns
//! - Fan-out and fan-in DAG patterns
//!
//! ## Scenario
//! Three demonstrations show different dependency modes: Mode 1 (auto-chain for sequential),
//! Mode 2 (inputs for data-driven parallelism), Mode 3 (depends_on for explicit DAG control).
//! Each mode enables different execution patterns and use cases.
//!
//! ## Dependency Mode Summary
//!
//! | Attribute           | Depends On                          |
//! |---------------------|-------------------------------------|
//! | (none)              | Previous registered step            |
//! | inputs(...)         | Input source steps ONLY             |
//! | depends_on = "..."  | Explicitly named steps ONLY         |
//!
//! ## Key Takeaways
//! - No attributes: Auto-chains to previous step (safe sequential default)
//! - inputs: Auto-depends on data sources only (enables parallelism)
//! - depends_on: Explicit dependencies only (fine-grained DAG control)
//! - inputs breaks auto-chain by providing explicit dependencies
//! - Multiple steps with same parent via depends_on run in parallel
//! - All three modes can be combined for complex workflows
//!
//! ## Run with
//! ```bash
//! cargo run --example three_dependency_modes
//! ```

use ergon::Ergon;
use ergon::{flow, step};
use ergon::{ExecutionLog, InMemoryExecutionLog};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThreeModes {
    id: String,
}

impl ThreeModes {
    fn new(id: String) -> Self {
        Self { id }
    }

    #[step]
    async fn mode1_step1(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 1] Step 1: No dependencies → runs first");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(1)
    }

    #[step]
    async fn mode1_step2(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 1] Step 2: Auto-chains → waits for step 1");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(2)
    }

    #[step]
    async fn mode1_step3(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 1] Step 3: Auto-chains → waits for step 2");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(3)
    }

    #[flow]
    async fn demo_mode1(self: Arc<Self>) -> Result<i32, String> {
        self.register_mode1_step1();
        self.register_mode1_step2();
        self.register_mode1_step3()
    }

    #[step]
    async fn mode2_root(self: Arc<Self>) -> Result<i32, String> {
        println!("\n[Mode 2] Root: No dependencies → runs first");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(10)
    }

    #[step(inputs(value = "mode2_root"))]
    async fn mode2_branch1(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!("[Mode 2] Branch 1: inputs(root) → depends on root ONLY");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value + 1)
    }

    #[step(inputs(value = "mode2_root"))]
    async fn mode2_branch2(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!("[Mode 2] Branch 2: inputs(root) → depends on root ONLY (parallel!)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value * 2)
    }

    #[step(inputs(a = "mode2_branch1", b = "mode2_branch2"))]
    async fn mode2_merge(self: Arc<Self>, a: i32, b: i32) -> Result<i32, String> {
        println!("[Mode 2] Merge: inputs(branch1, branch2) → waits for both");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(a + b)
    }

    #[flow]
    async fn demo_mode2(self: Arc<Self>) -> Result<i32, String> {
        self.register_mode2_root();
        self.register_mode2_branch1();
        self.register_mode2_branch2();
        self.register_mode2_merge()
    }

    #[step]
    async fn mode3_init(self: Arc<Self>) -> Result<(), String> {
        println!("\n[Mode 3] Init: No dependencies → runs first");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[step(depends_on = "mode3_init")]
    async fn mode3_task1(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 3] Task 1: depends_on(init) → waits for init");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(1)
    }

    #[step(depends_on = "mode3_init")]
    async fn mode3_task2(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 3] Task 2: depends_on(init) → waits for init (parallel!)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(2)
    }

    #[step(depends_on = ["mode3_task1", "mode3_task2"])]
    async fn mode3_finalize(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 3] Finalize: depends_on(task1, task2) → waits for both");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(3)
    }

    #[flow]
    async fn demo_mode3(self: Arc<Self>) -> Result<i32, String> {
        self.register_mode3_init();
        self.register_mode3_task1();
        self.register_mode3_task2();
        self.register_mode3_finalize()
    }

    #[step]
    async fn comparison_step1(self: Arc<Self>) -> Result<i32, String> {
        println!("\n[Comparison] Step 1: Root step");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(100)
    }

    #[step]
    async fn comparison_step2(self: Arc<Self>) -> Result<i32, String> {
        println!("[Comparison] Step 2: Auto-chains to step1");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(200)
    }

    #[step(inputs(value = "comparison_step1"))]
    async fn comparison_step3(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!("[Comparison] Step 3: inputs(step1) → depends on step1 ONLY");
        println!("              (Runs in PARALLEL with step2!)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value * 3)
    }

    #[flow]
    async fn demo_comparison(self: Arc<Self>) -> Result<i32, String> {
        self.register_comparison_step1();
        self.register_comparison_step2();
        self.register_comparison_step3()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nThree Dependency Modes Demonstration");
    println!("====================================\n");

    let storage = Arc::new(InMemoryExecutionLog::new());

    println!("-----------------------------------------------------------");
    println!("│ MODE 1: No Attributes → Auto-Chain to Previous            │");
    println!("└────────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let demo1 = Arc::new(ThreeModes::new("mode1".to_string()));
    let instance1 = Ergon::new_flow(Arc::clone(&demo1), Uuid::new_v4(), Arc::clone(&storage));
    let result1 = instance1.execute(|f| f.demo_mode1()).await;
    println!("\nResult: {:?}", result1);
    println!("Execution: step1 → step2 → step3 (sequential)\n");

    println!("-----------------------------------------------------------");
    println!("│ MODE 2: inputs(...) → Auto-Depend on Input Sources        │");
    println!("└────────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let demo2 = Arc::new(ThreeModes::new("mode2".to_string()));
    let instance2 = Ergon::new_flow(Arc::clone(&demo2), Uuid::new_v4(), Arc::clone(&storage));
    let result2 = instance2.execute(|f| f.demo_mode2()).await;
    println!("\nResult: {:?}", result2);
    println!("Execution: root → (branch1 || branch2) → merge\n");

    println!("-----------------------------------------------------------");
    println!("│ MODE 3: depends_on → Explicit Dependencies                │");
    println!("└────────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let demo3 = Arc::new(ThreeModes::new("mode3".to_string()));
    let instance3 = Ergon::new_flow(Arc::clone(&demo3), Uuid::new_v4(), Arc::clone(&storage));
    let result3 = instance3.execute(|f| f.demo_mode3()).await;
    println!("\nResult: {:?}", result3);
    println!("Execution: init → (task1 || task2) → finalize\n");

    println!("-----------------------------------------------------------");
    println!("│ KEY INSIGHT: inputs(...) Does NOT Auto-Chain!             │");
    println!("└────────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let comparison = Arc::new(ThreeModes::new("comparison".to_string()));
    let instance4 = Ergon::new_flow(
        Arc::clone(&comparison),
        Uuid::new_v4(),
        Arc::clone(&storage),
    );
    let result4 = instance4.execute(|f| f.demo_comparison()).await;
    println!("\nResult: {:?}", result4);
    println!("Execution: step1 → (step2 || step3) [parallel!]\n");

    Ok(())
}
