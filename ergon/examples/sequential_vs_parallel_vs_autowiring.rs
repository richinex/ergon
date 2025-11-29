//! Complete Comparison: Sequential vs Parallel vs Autowiring
//!
//! This example demonstrates ALL execution patterns:
//!
//! 1. SEQUENTIAL (no attributes)
//!    - Auto-chains to previous step
//!    - No data passing
//!
//! 2. PARALLEL with depends_on
//!    - Explicit dependencies on same parent
//!    - No data passing (control dependencies only)
//!
//! 3. SEQUENTIAL with inputs (autowiring)
//!    - Each step inputs from PREVIOUS step only
//!    - Data flows linearly: step1 → step2(inputs step1) → step3(inputs step2)
//!    - Still sequential! (each depends on different parent)
//!
//! 4. PARALLEL with inputs (autowiring)
//!    - Multiple steps input from SAME parent
//!    - Data flows in parallel: root → [branch1(inputs root) || branch2(inputs root)]
//!
//! KEY INSIGHT: inputs doesn't force parallelism - it depends on the dependency pattern!

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

    // =========================================================================
    // PATTERN 1: SEQUENTIAL (No Attributes)
    // =========================================================================

    #[step]
    async fn seq_step1(self: Arc<Self>) -> Result<(), String> {
        println!("[Sequential] Step 1 (no attributes)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[step]  // Auto-chains to seq_step1
    async fn seq_step2(self: Arc<Self>) -> Result<(), String> {
        println!("[Sequential] Step 2 (auto-chains to step1)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[step]  // Auto-chains to seq_step2
    async fn seq_step3(self: Arc<Self>) -> Result<(), String> {
        println!("[Sequential] Step 3 (auto-chains to step2)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    #[flow]
    async fn pattern1_sequential(self: Arc<Self>) -> Result<(), String> {
        self.register_seq_step1();
        self.register_seq_step2();  // Waits for step1
        self.register_seq_step3()   // Waits for step2
    }

    // =========================================================================
    // PATTERN 2: PARALLEL with depends_on (Explicit Control)
    // =========================================================================

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

    #[step(depends_on = "par_explicit_root")]  // Same parent → PARALLEL!
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
        self.register_par_explicit_branch1();  // Parallel with branch2
        self.register_par_explicit_branch2();  // Parallel with branch1
        self.register_par_explicit_merge()     // Waits for both
    }

    // =========================================================================
    // PATTERN 3: SEQUENTIAL with inputs (Autowiring, Still Sequential!)
    // =========================================================================

    #[step]
    async fn seq_auto_step1(self: Arc<Self>) -> Result<i32, String> {
        println!("\n[Sequential-Autowiring] Step 1 (returns 100)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(100)
    }

    // inputs from step1 ONLY → depends ONLY on step1
    #[step(inputs(value = "seq_auto_step1"))]
    async fn seq_auto_step2(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!("[Sequential-Autowiring] Step 2 (inputs from step1: {})", value);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value * 2)  // 200
    }

    // inputs from step2 ONLY → depends ONLY on step2 → Still Sequential!
    #[step(inputs(value = "seq_auto_step2"))]
    async fn seq_auto_step3(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!("[Sequential-Autowiring] Step 3 (inputs from step2: {})", value);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value + 50)  // 250
    }

    #[flow]
    async fn pattern3_sequential_autowiring(self: Arc<Self>) -> Result<i32, String> {
        self.register_seq_auto_step1();
        self.register_seq_auto_step2();  // Depends on step1 (via inputs)
        self.register_seq_auto_step3()   // Depends on step2 (via inputs) → Sequential!
    }

    // =========================================================================
    // PATTERN 4: PARALLEL with inputs (Autowiring with Same Parent)
    // =========================================================================

    #[step]
    async fn par_auto_root(self: Arc<Self>) -> Result<i32, String> {
        println!("\n[Parallel-Autowiring] Root (returns 10)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(10)
    }

    // inputs from root → depends on root
    #[step(inputs(value = "par_auto_root"))]
    async fn par_auto_branch1(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!("[Parallel-Autowiring] Branch 1 (inputs from root: {})", value);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value + 1)  // 11
    }

    // ALSO inputs from root → ALSO depends on root → PARALLEL with branch1!
    #[step(inputs(value = "par_auto_root"))]
    async fn par_auto_branch2(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!("[Parallel-Autowiring] Branch 2 (inputs from root: {})", value);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value * 2)  // 20
    }

    // inputs from BOTH branches → depends on both
    #[step(inputs(a = "par_auto_branch1", b = "par_auto_branch2"))]
    async fn par_auto_merge(self: Arc<Self>, a: i32, b: i32) -> Result<i32, String> {
        println!("[Parallel-Autowiring] Merge (inputs from both: a={}, b={})", a, b);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(a + b)  // 31
    }

    #[flow]
    async fn pattern4_parallel_autowiring(self: Arc<Self>) -> Result<i32, String> {
        self.register_par_auto_root();
        self.register_par_auto_branch1();  // Depends on root (via inputs)
        self.register_par_auto_branch2();  // ALSO depends on root → PARALLEL!
        self.register_par_auto_merge()     // Depends on both branches
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Complete Comparison: All Execution Patterns             ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let storage = Arc::new(InMemoryExecutionLog::new());

    // =========================================================================
    // PATTERN 1: Sequential (No Attributes)
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ PATTERN 1: SEQUENTIAL (No Attributes)                    │");
    println!("│ Each step auto-chains to the previous one                │");
    println!("└───────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let p1 = Arc::new(AllPatterns::new("p1".to_string()));
    let instance1 = Ergon::new_flow(Arc::clone(&p1), Uuid::new_v4(), Arc::clone(&storage));
    let result1 = instance1.execute(|f| f.pattern1_sequential()).await;

    println!("\nExecution: step1 → step2 → step3");
    println!("Result: {:?}\n", result1);

    // =========================================================================
    // PATTERN 2: Parallel with depends_on
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ PATTERN 2: PARALLEL with depends_on (Explicit Control)   │");
    println!("│ Both branches explicitly depend on root                  │");
    println!("└───────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let p2 = Arc::new(AllPatterns::new("p2".to_string()));
    let instance2 = Ergon::new_flow(Arc::clone(&p2), Uuid::new_v4(), Arc::clone(&storage));
    let result2 = instance2.execute(|f| f.pattern2_parallel_explicit()).await;

    println!("\nExecution: root → (branch1 || branch2) → merge");
    println!("Result: {:?}\n", result2);

    // =========================================================================
    // PATTERN 3: Sequential with inputs (Autowiring)
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ PATTERN 3: SEQUENTIAL with inputs (Autowiring)           │");
    println!("│ Each step inputs from PREVIOUS step only → Sequential!   │");
    println!("└───────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let p3 = Arc::new(AllPatterns::new("p3".to_string()));
    let instance3 = Ergon::new_flow(Arc::clone(&p3), Uuid::new_v4(), Arc::clone(&storage));
    let result3 = instance3.execute(|f| f.pattern3_sequential_autowiring()).await;

    println!("\nExecution: step1 → step2(inputs step1) → step3(inputs step2)");
    println!("Data flow: 100 → 200 → 250");
    println!("Result: {:?}\n", result3);

    // =========================================================================
    // PATTERN 4: Parallel with inputs (Autowiring)
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ PATTERN 4: PARALLEL with inputs (Autowiring)             │");
    println!("│ Multiple steps input from SAME parent → Parallel!        │");
    println!("└───────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let p4 = Arc::new(AllPatterns::new("p4".to_string()));
    let instance4 = Ergon::new_flow(Arc::clone(&p4), Uuid::new_v4(), Arc::clone(&storage));
    let result4 = instance4.execute(|f| f.pattern4_parallel_autowiring()).await;

    println!("\nExecution: root → (branch1(inputs root) || branch2(inputs root)) → merge");
    println!("Data flow: 10 → (11, 20) → 31");
    println!("Result: {:?}\n", result4);

    // =========================================================================
    // KEY INSIGHT
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ KEY INSIGHT: inputs Doesn't Force Parallelism!           │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Sequential with inputs:");
    println!("  step1 → step2(inputs step1) → step3(inputs step2)");
    println!("  ✓ Each step depends on DIFFERENT parent → Sequential\n");

    println!("Parallel with inputs:");
    println!("  root → branch1(inputs root), branch2(inputs root)");
    println!("  ✓ Both steps depend on SAME parent → Parallel!\n");

    // =========================================================================
    // SUMMARY TABLE
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ SUMMARY: When Does Parallelism Occur?                    │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("┌────────────────────────┬─────────────────┬──────────────┐");
    println!("│ Pattern                │ Dependencies    │ Execution    │");
    println!("├────────────────────────┼─────────────────┼──────────────┤");
    println!("│ No attributes          │ Auto-chain      │ Sequential   │");
    println!("│ depends_on same parent │ Same parent     │ Parallel     │");
    println!("│ inputs diff parents    │ Different       │ Sequential   │");
    println!("│ inputs same parent     │ Same parent     │ Parallel     │");
    println!("└────────────────────────┴─────────────────┴──────────────┘\n");

    println!("Rule: Steps run in PARALLEL if they depend on the SAME parent(s)");
    println!("      Steps run SEQUENTIALLY if they depend on DIFFERENT parents\n");

    // =========================================================================
    // CODE COMPARISON
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ CODE COMPARISON                                           │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Sequential (no attributes):");
    println!("  #[step]");
    println!("  async fn step1() {{ }}\n");
    println!("  #[step]  // Auto-chains");
    println!("  async fn step2() {{ }}\n");

    println!("Parallel (explicit depends_on):");
    println!("  #[step(depends_on = \"root\")]");
    println!("  async fn branch1() {{ }}\n");
    println!("  #[step(depends_on = \"root\")]  // Parallel!");
    println!("  async fn branch2() {{ }}\n");

    println!("Sequential (inputs autowiring):");
    println!("  #[step(inputs(v = \"step1\"))]");
    println!("  async fn step2(v: i32) {{ }}\n");
    println!("  #[step(inputs(v = \"step2\"))]  // Sequential!");
    println!("  async fn step3(v: i32) {{ }}\n");

    println!("Parallel (inputs autowiring):");
    println!("  #[step(inputs(v = \"root\"))]");
    println!("  async fn branch1(v: i32) {{ }}\n");
    println!("  #[step(inputs(v = \"root\"))]  // Parallel!");
    println!("  async fn branch2(v: i32) {{ }}\n");

    Ok(())
}
