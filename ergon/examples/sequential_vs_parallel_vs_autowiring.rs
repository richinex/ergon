//! Complete Comparison: Sequential vs Parallel vs Autowiring
//!
//! This example demonstrates:
//! - Four distinct execution patterns in Ergon
//! - How dependencies determine execution order (sequential vs parallel)
//! - The difference between explicit control flow and data flow
//! - Why inputs attribute alone doesn't force parallelism
//!
//! ## Scenario
//! We compare four patterns side-by-side:
//! 1. Sequential execution with no attributes (auto-chaining)
//! 2. Parallel execution with explicit depends_on (control dependencies)
//! 3. Sequential execution with inputs (data dependencies, different parents)
//! 4. Parallel execution with inputs (data dependencies, same parent)
//!
//! ## Key Takeaways
//! - Steps run in PARALLEL when they depend on the SAME parent(s)
//! - Steps run SEQUENTIALLY when they depend on DIFFERENT parents
//! - The inputs attribute creates dependencies but doesn't force parallelism
//! - Parallelism is determined by the dependency graph structure, not the attribute type
//! - Auto-chaining (no attributes) creates sequential execution
//! - Multiple steps depending on the same parent execute in parallel
//!
//! ## Run with
//! ```bash
//! cargo run --example sequential_vs_parallel_vs_autowiring
//! ```

use ergon::Ergon;
use ergon::{flow, step};
use ergon::{ExecutionLog, InMemoryExecutionLog};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AllPatterns {
    id: String,
}

impl AllPatterns {
    fn new(id: String) -> Self {
        Self { id }
    }

    #[step]
    async fn seq_step1(self: Arc<Self>) -> Result<(), String> {
        println!("[Sequential] Step 1 (no attributes)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[step]
    async fn seq_step2(self: Arc<Self>) -> Result<(), String> {
        println!("[Sequential] Step 2 (auto-chains to step1)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[step]
    async fn seq_step3(self: Arc<Self>) -> Result<(), String> {
        println!("[Sequential] Step 3 (auto-chains to step2)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[flow]
    async fn pattern1_sequential(self: Arc<Self>) -> Result<(), String> {
        self.register_seq_step1();
        self.register_seq_step2();
        self.register_seq_step3()
    }

    #[step]
    async fn par_explicit_root(self: Arc<Self>) -> Result<(), String> {
        println!("\n[Parallel-Explicit] Root");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[step(depends_on = "par_explicit_root")]
    async fn par_explicit_branch1(self: Arc<Self>) -> Result<(), String> {
        println!("[Parallel-Explicit] Branch 1 (depends_on root)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[step(depends_on = "par_explicit_root")]
    async fn par_explicit_branch2(self: Arc<Self>) -> Result<(), String> {
        println!("[Parallel-Explicit] Branch 2 (depends_on root)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[step(depends_on = ["par_explicit_branch1", "par_explicit_branch2"])]
    async fn par_explicit_merge(self: Arc<Self>) -> Result<(), String> {
        println!("[Parallel-Explicit] Merge (waits for both branches)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[flow]
    async fn pattern2_parallel_explicit(self: Arc<Self>) -> Result<(), String> {
        self.register_par_explicit_root();
        self.register_par_explicit_branch1();
        self.register_par_explicit_branch2();
        self.register_par_explicit_merge()
    }

    #[step]
    async fn seq_auto_step1(self: Arc<Self>) -> Result<i32, String> {
        println!("\n[Sequential-Autowiring] Step 1 (returns 100)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(100)
    }

    #[step(inputs(value = "seq_auto_step1"))]
    async fn seq_auto_step2(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!(
            "[Sequential-Autowiring] Step 2 (inputs from step1: {})",
            value
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value * 2) // 200
    }

    #[step(inputs(value = "seq_auto_step2"))]
    async fn seq_auto_step3(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!(
            "[Sequential-Autowiring] Step 3 (inputs from step2: {})",
            value
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value + 50) // 250
    }

    #[flow]
    async fn pattern3_sequential_autowiring(self: Arc<Self>) -> Result<i32, String> {
        self.register_seq_auto_step1();
        self.register_seq_auto_step2();
        self.register_seq_auto_step3()
    }

    #[step]
    async fn par_auto_root(self: Arc<Self>) -> Result<i32, String> {
        println!("\n[Parallel-Autowiring] Root (returns 10)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(10)
    }

    #[step(inputs(value = "par_auto_root"))]
    async fn par_auto_branch1(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!(
            "[Parallel-Autowiring] Branch 1 (inputs from root: {})",
            value
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value + 1) // 11
    }

    #[step(inputs(value = "par_auto_root"))]
    async fn par_auto_branch2(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!(
            "[Parallel-Autowiring] Branch 2 (inputs from root: {})",
            value
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value * 2) // 20
    }

    #[step(inputs(a = "par_auto_branch1", b = "par_auto_branch2"))]
    async fn par_auto_merge(self: Arc<Self>, a: i32, b: i32) -> Result<i32, String> {
        println!(
            "[Parallel-Autowiring] Merge (inputs from both: a={}, b={})",
            a, b
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(a + b) // 31
    }

    #[flow]
    async fn pattern4_parallel_autowiring(self: Arc<Self>) -> Result<i32, String> {
        self.register_par_auto_root();
        self.register_par_auto_branch1();
        self.register_par_auto_branch2();
        self.register_par_auto_merge()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=============================================================");
    println!("  Complete Comparison: All Execution Patterns");
    println!("=============================================================\n");

    let storage = Arc::new(InMemoryExecutionLog::new());

    println!("-------------------------------------------------------------");
    println!(" PATTERN 1: SEQUENTIAL (No Attributes)");
    println!(" Each step auto-chains to the previous one");
    println!("-------------------------------------------------------------");

    storage.reset().await?;
    let p1 = Arc::new(AllPatterns::new("p1".to_string()));
    let instance1 = Ergon::new_flow(Arc::clone(&p1), Uuid::new_v4(), Arc::clone(&storage));
    let result1 = instance1.execute(|f| f.pattern1_sequential()).await;

    println!("\nExecution: step1 → step2 → step3");
    println!("Result: {:?}\n", result1);

    println!("-------------------------------------------------------------");
    println!(" PATTERN 2: PARALLEL with depends_on (Explicit Control)");
    println!(" Both branches explicitly depend on root");
    println!("-------------------------------------------------------------");

    storage.reset().await?;
    let p2 = Arc::new(AllPatterns::new("p2".to_string()));
    let instance2 = Ergon::new_flow(Arc::clone(&p2), Uuid::new_v4(), Arc::clone(&storage));
    let result2 = instance2.execute(|f| f.pattern2_parallel_explicit()).await;

    println!("\nExecution: root → (branch1 || branch2) → merge");
    println!("Result: {:?}\n", result2);

    println!("-------------------------------------------------------------");
    println!(" PATTERN 3: SEQUENTIAL with inputs (Autowiring)");
    println!(" Each step inputs from PREVIOUS step only - Sequential!");
    println!("-------------------------------------------------------------");

    storage.reset().await?;
    let p3 = Arc::new(AllPatterns::new("p3".to_string()));
    let instance3 = Ergon::new_flow(Arc::clone(&p3), Uuid::new_v4(), Arc::clone(&storage));
    let result3 = instance3
        .execute(|f| f.pattern3_sequential_autowiring())
        .await;

    println!("\nExecution: step1 → step2(inputs step1) → step3(inputs step2)");
    println!("Data flow: 100 → 200 → 250");
    println!("Result: {:?}\n", result3);

    println!("-------------------------------------------------------------");
    println!(" PATTERN 4: PARALLEL with inputs (Autowiring)");
    println!(" Multiple steps input from SAME parent - Parallel!");
    println!("-------------------------------------------------------------");

    storage.reset().await?;
    let p4 = Arc::new(AllPatterns::new("p4".to_string()));
    let instance4 = Ergon::new_flow(Arc::clone(&p4), Uuid::new_v4(), Arc::clone(&storage));
    let result4 = instance4
        .execute(|f| f.pattern4_parallel_autowiring())
        .await;

    println!("\nExecution: root → (branch1(inputs root) || branch2(inputs root)) → merge");
    println!("Data flow: 10 → (11, 20) → 31");
    println!("Result: {:?}\n", result4);

    Ok(())
}
