//! Three Dependency Modes: A Comprehensive Demonstration
//!
//! This example clearly demonstrates the three ways to control dependencies:
//!
//! | Attribute          | Behavior                             | Use Case                              |
//! |--------------------|--------------------------------------|---------------------------------------|
//! | No depends_on      | Auto-chains to previous step         | Simple sequential pipelines           |
//! | inputs(...)        | Auto-depends on input sources        | Data transformations with parallelism |
//! | depends_on = "..." | Explicit dependencies, no auto-chain | Fine-grained control, complex DAGs    |

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

    // =========================================================================
    // MODE 1: No Attributes (Auto-Chain to Previous)
    // =========================================================================

    #[step]
    async fn mode1_step1(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 1] Step 1: No dependencies → runs first");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(1)
    }

    #[step] // No attributes → auto-chains to mode1_step1
    async fn mode1_step2(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 1] Step 2: Auto-chains → waits for step 1");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(2)
    }

    #[step] // No attributes → auto-chains to mode1_step2
    async fn mode1_step3(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 1] Step 3: Auto-chains → waits for step 2");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(3)
    }

    #[flow]
    async fn demo_mode1(self: Arc<Self>) -> Result<i32, String> {
        self.register_mode1_step1();
        self.register_mode1_step2(); // Auto-chains to step1
        self.register_mode1_step3() // Auto-chains to step2
    }

    // =========================================================================
    // MODE 2: inputs(...) Only (Auto-Depend on Input Sources)
    // =========================================================================

    #[step]
    async fn mode2_root(self: Arc<Self>) -> Result<i32, String> {
        println!("\n[Mode 2] Root: No dependencies → runs first");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(10)
    }

    // Uses inputs → depends ONLY on mode2_root (NOT on previous step!)
    #[step(inputs(value = "mode2_root"))]
    async fn mode2_branch1(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!("[Mode 2] Branch 1: inputs(root) → depends on root ONLY");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value + 1)
    }

    // Also uses inputs → ALSO depends only on mode2_root
    // This means branch1 and branch2 run IN PARALLEL!
    #[step(inputs(value = "mode2_root"))]
    async fn mode2_branch2(self: Arc<Self>, value: i32) -> Result<i32, String> {
        println!("[Mode 2] Branch 2: inputs(root) → depends on root ONLY (parallel!)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(value * 2)
    }

    // Depends on BOTH branches (fan-in merge)
    #[step(inputs(a = "mode2_branch1", b = "mode2_branch2"))]
    async fn mode2_merge(self: Arc<Self>, a: i32, b: i32) -> Result<i32, String> {
        println!("[Mode 2] Merge: inputs(branch1, branch2) → waits for both");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(a + b)
    }

    #[flow]
    async fn demo_mode2(self: Arc<Self>) -> Result<i32, String> {
        self.register_mode2_root();
        self.register_mode2_branch1(); // Depends on root (via inputs)
        self.register_mode2_branch2(); // Also depends on root → PARALLEL!
        self.register_mode2_merge() // Depends on both branches
    }

    // =========================================================================
    // MODE 3: depends_on Explicit (No Auto-Chain)
    // =========================================================================

    #[step]
    async fn mode3_init(self: Arc<Self>) -> Result<(), String> {
        println!("\n[Mode 3] Init: No dependencies → runs first");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    // Explicit depends_on → does NOT auto-chain to previous
    #[step(depends_on = "mode3_init")]
    async fn mode3_task1(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 3] Task 1: depends_on(init) → waits for init");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(1)
    }

    // Also explicit depends_on init → runs in PARALLEL with task1
    #[step(depends_on = "mode3_init")]
    async fn mode3_task2(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 3] Task 2: depends_on(init) → waits for init (parallel!)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(2)
    }

    // Explicit depends on BOTH tasks
    #[step(depends_on = ["mode3_task1", "mode3_task2"])]
    async fn mode3_finalize(self: Arc<Self>) -> Result<i32, String> {
        println!("[Mode 3] Finalize: depends_on(task1, task2) → waits for both");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(3)
    }

    #[flow]
    async fn demo_mode3(self: Arc<Self>) -> Result<i32, String> {
        self.register_mode3_init();
        self.register_mode3_task1(); // Explicit: depends on init
        self.register_mode3_task2(); // Explicit: also depends on init → PARALLEL
        self.register_mode3_finalize() // Explicit: depends on both tasks
    }

    // =========================================================================
    // COMPARISON: Demonstrating the Key Difference
    // =========================================================================

    #[step]
    async fn comparison_step1(self: Arc<Self>) -> Result<i32, String> {
        println!("\n[Comparison] Step 1: Root step");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(100)
    }

    #[step] // No attributes → auto-chains to step1
    async fn comparison_step2(self: Arc<Self>) -> Result<i32, String> {
        println!("[Comparison] Step 2: Auto-chains to step1");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(200)
    }

    // inputs(step1) → depends ONLY on step1, NOT on step2!
    // This means step2 and step3 can run IN PARALLEL
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
        self.register_comparison_step2(); // Auto-chains to step1
        self.register_comparison_step3() // Depends on step1 (NOT step2!) → PARALLEL!
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  Three Dependency Modes Demonstration                     ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let storage = Arc::new(InMemoryExecutionLog::new());

    // =========================================================================
    // MODE 1: Auto-Chain (No Attributes)
    // =========================================================================
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ MODE 1: No Attributes → Auto-Chain to Previous            │");
    println!("└────────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let demo1 = Arc::new(ThreeModes::new("mode1".to_string()));
    let instance1 = Ergon::new_flow(Arc::clone(&demo1), Uuid::new_v4(), Arc::clone(&storage));
    let result1 = instance1.execute(|f| f.demo_mode1()).await;
    println!("\nResult: {:?}", result1);
    println!("Execution: step1 → step2 → step3 (sequential)\n");

    // =========================================================================
    // MODE 2: inputs(...) → Auto-Depend on Input Sources
    // =========================================================================
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ MODE 2: inputs(...) → Auto-Depend on Input Sources        │");
    println!("└────────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let demo2 = Arc::new(ThreeModes::new("mode2".to_string()));
    let instance2 = Ergon::new_flow(Arc::clone(&demo2), Uuid::new_v4(), Arc::clone(&storage));
    let result2 = instance2.execute(|f| f.demo_mode2()).await;
    println!("\nResult: {:?}", result2);
    println!("Execution: root → (branch1 || branch2) → merge\n");

    // =========================================================================
    // MODE 3: depends_on → Explicit Dependencies
    // =========================================================================
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ MODE 3: depends_on → Explicit Dependencies                │");
    println!("└────────────────────────────────────────────────────────────┘");

    storage.reset().await?;
    let demo3 = Arc::new(ThreeModes::new("mode3".to_string()));
    let instance3 = Ergon::new_flow(Arc::clone(&demo3), Uuid::new_v4(), Arc::clone(&storage));
    let result3 = instance3.execute(|f| f.demo_mode3()).await;
    println!("\nResult: {:?}", result3);
    println!("Execution: init → (task1 || task2) → finalize\n");

    // =========================================================================
    // KEY INSIGHT: inputs vs Auto-Chain
    // =========================================================================
    println!("┌────────────────────────────────────────────────────────────┐");
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
    println!("Why? step2 auto-chains to step1, step3 uses inputs(step1)");
    println!("Both depend on step1, NOT on each other → parallel!\n");

    // =========================================================================
    // SUMMARY TABLE
    // =========================================================================
    println!("┌────────────────────────────────────────────────────────────┐");
    println!("│ SUMMARY: Three Dependency Modes                           │");
    println!("└────────────────────────────────────────────────────────────┘\n");
    println!("┌─────────────────────┬─────────────────────────────────────┐");
    println!("│ Attribute           │ Depends On                          │");
    println!("├─────────────────────┼─────────────────────────────────────┤");
    println!("│ (none)              │ Previous registered step            │");
    println!("│ inputs(...)         │ Input source steps ONLY             │");
    println!("│ depends_on = \"...\"  │ Explicitly named steps ONLY         │");
    println!("└─────────────────────┴─────────────────────────────────────┘\n");

    println!("Key Points:");
    println!("• No attributes → Sequential (safe default)");
    println!("• inputs → Depends on data sources (enables parallelism)");
    println!("• depends_on → Explicit control (fine-grained DAGs)");
    println!("• All three can be combined for complex workflows!\n");

    Ok(())
}
