//! Settling the Truth: Auto-Chaining vs Parallelism
//!
//! This example definitively shows what ACTUALLY happens with:
//! 1. Auto-chaining behavior
//! 2. Multiple inputs
//! 3. depends_on = [] to disable auto-chaining
//! 4. How to achieve true parallelism

use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct TestFlow {
    id: String,
}

impl TestFlow {
    fn new(id: String) -> Self {
        Self { id }
    }

    // =========================================================================
    // CASE 1: Two Steps with NO depends_on, Merge with inputs
    // QUESTION: Do extract_a and extract_b run in parallel?
    // =========================================================================

    #[step]
    async fn case1_extract_a(self: Arc<Self>) -> Result<String, String> {
        println!("[CASE 1] Extract A starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 1] Extract A finished");
        Ok("data_a".to_string())
    }

    // NO depends_on, NO inputs → Will this auto-chain to extract_a?
    #[step]
    async fn case1_extract_b(self: Arc<Self>) -> Result<String, String> {
        println!("[CASE 1] Extract B starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 1] Extract B finished");
        Ok("data_b".to_string())
    }

    // Has inputs from BOTH → Does this affect extract_a and extract_b?
    #[step(inputs(a = "case1_extract_a", b = "case1_extract_b"))]
    async fn case1_merge(self: Arc<Self>, a: String, b: String) -> Result<String, String> {
        println!("[CASE 1] Merge starting (has both inputs)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 1] Merge finished");
        Ok(format!("{} + {}", a, b))
    }

    #[flow]
    async fn run_case1(self: Arc<Self>) -> Result<String, String> {
        self.register_case1_extract_a(); // 1st registration: no deps
        self.register_case1_extract_b(); // 2nd registration: auto-chain?
        self.register_case1_merge() // 3rd registration: inputs(a, b)
    }

    // =========================================================================
    // CASE 2: Two Steps with EXPLICIT depends_on to common parent
    // QUESTION: Do extract_a and extract_b run in parallel now?
    // =========================================================================

    #[step]
    async fn case2_root(self: Arc<Self>) -> Result<(), String> {
        println!("\n[CASE 2] Root starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 2] Root finished");
        Ok(())
    }

    // Explicit depends_on → Should NOT auto-chain
    #[step(depends_on = "case2_root")]
    async fn case2_extract_a(self: Arc<Self>) -> Result<String, String> {
        println!("[CASE 2] Extract A starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 2] Extract A finished");
        Ok("data_a".to_string())
    }

    // ALSO explicit depends_on same parent → Should run in parallel with A
    #[step(depends_on = "case2_root")]
    async fn case2_extract_b(self: Arc<Self>) -> Result<String, String> {
        println!("[CASE 2] Extract B starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 2] Extract B finished");
        Ok("data_b".to_string())
    }

    #[step(inputs(a = "case2_extract_a", b = "case2_extract_b"))]
    async fn case2_merge(self: Arc<Self>, a: String, b: String) -> Result<String, String> {
        println!("[CASE 2] Merge starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 2] Merge finished");
        Ok(format!("{} + {}", a, b))
    }

    #[flow]
    async fn run_case2(self: Arc<Self>) -> Result<String, String> {
        self.register_case2_root();
        self.register_case2_extract_a(); // depends_on = "root"
        self.register_case2_extract_b(); // depends_on = "root"
        self.register_case2_merge() // inputs(a, b)
    }

    // =========================================================================
    // CASE 3: Using inputs on extract steps (does this help?)
    // QUESTION: If extracts use inputs from root, are they parallel?
    // =========================================================================

    #[step]
    async fn case3_root(self: Arc<Self>) -> Result<String, String> {
        println!("\n[CASE 3] Root starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 3] Root finished");
        Ok("config".to_string())
    }

    // Uses inputs from root → auto-depends on root
    #[step(inputs(config = "case3_root"))]
    async fn case3_extract_a(self: Arc<Self>, config: String) -> Result<String, String> {
        println!("[CASE 3] Extract A starting (config: {})", config);
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 3] Extract A finished");
        Ok("data_a".to_string())
    }

    // ALSO uses inputs from root → also auto-depends on root
    #[step(inputs(config = "case3_root"))]
    async fn case3_extract_b(self: Arc<Self>, config: String) -> Result<String, String> {
        println!("[CASE 3] Extract B starting (config: {})", config);
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 3] Extract B finished");
        Ok("data_b".to_string())
    }

    #[step(inputs(a = "case3_extract_a", b = "case3_extract_b"))]
    async fn case3_merge(self: Arc<Self>, a: String, b: String) -> Result<String, String> {
        println!("[CASE 3] Merge starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 3] Merge finished");
        Ok(format!("{} + {}", a, b))
    }

    #[flow]
    async fn run_case3(self: Arc<Self>) -> Result<String, String> {
        self.register_case3_root();
        self.register_case3_extract_a(); // inputs(config = "root")
        self.register_case3_extract_b(); // inputs(config = "root")
        self.register_case3_merge() // inputs(a, b)
    }

    // =========================================================================
    // CASE 4: Using depends_on = [] to disable auto-chaining
    // QUESTION: Can we get parallelism without a common parent?
    // =========================================================================

    #[step]
    async fn case4_extract_a(self: Arc<Self>) -> Result<String, String> {
        println!("\n[CASE 4] Extract A starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 4] Extract A finished");
        Ok("data_a".to_string())
    }

    // Uses depends_on = [] to explicitly disable auto-chaining
    #[step(depends_on = [])]
    async fn case4_extract_b(self: Arc<Self>) -> Result<String, String> {
        println!("[CASE 4] Extract B starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 4] Extract B finished");
        Ok("data_b".to_string())
    }

    #[step(inputs(a = "case4_extract_a", b = "case4_extract_b"))]
    async fn case4_merge(self: Arc<Self>, a: String, b: String) -> Result<String, String> {
        println!("[CASE 4] Merge starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 4] Merge finished");
        Ok(format!("{} + {}", a, b))
    }

    #[flow]
    async fn run_case4(self: Arc<Self>) -> Result<String, String> {
        self.register_case4_extract_a();
        self.register_case4_extract_b(); // depends_on = [] → NO auto-chain!
        self.register_case4_merge() // inputs(a, b)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  SETTLING THE TRUTH: Auto-Chaining vs Parallelism        ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let storage = Arc::new(InMemoryExecutionLog::new());

    // =========================================================================
    // CASE 1: Two steps, no depends_on, merge with inputs
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ CASE 1: Two Steps with NO depends_on                     │");
    println!("│ Question: Does auto-chaining make them sequential?       │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Code:");
    println!("  #[step]");
    println!("  async fn extract_a() -> String {{ ... }}");
    println!();
    println!("  #[step]  // ← No depends_on, no inputs");
    println!("  async fn extract_b() -> String {{ ... }}");
    println!();
    println!("  #[step(inputs(a = \"extract_a\", b = \"extract_b\"))]");
    println!("  async fn merge(a: String, b: String) {{ ... }}\n");

    storage.reset().await?;
    let test1 = Arc::new(TestFlow::new("case1".to_string()));
    let start1 = Instant::now();
    let instance1 = Ergon::new_flow(Arc::clone(&test1), Uuid::new_v4(), Arc::clone(&storage));
    let result1 = instance1
        .executor()
        .execute(|f| Box::pin(f.clone().run_case1()))
        .await;
    let elapsed1 = start1.elapsed();

    println!("\nResult: {:?}", result1);
    println!("Time: {:?}", elapsed1);

    if elapsed1.as_millis() > 180 {
        println!("\n✓ SEQUENTIAL: ~200ms (100 + 100 + 50)");
        println!("  extract_a runs, THEN extract_b runs, THEN merge");
        println!("  → Auto-chaining made extract_b depend on extract_a!");
    } else {
        println!("\n✓ PARALLEL: ~150ms (max(100,100) + 50)");
        println!("  extract_a and extract_b run in parallel!");
    }

    // =========================================================================
    // CASE 2: Explicit depends_on to common parent
    // =========================================================================
    println!("\n┌───────────────────────────────────────────────────────────┐");
    println!("│ CASE 2: Explicit depends_on to Common Parent             │");
    println!("│ Question: Does this create parallelism?                  │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Code:");
    println!("  #[step]");
    println!("  async fn root() {{ ... }}");
    println!();
    println!("  #[step(depends_on = \"root\")]");
    println!("  async fn extract_a() -> String {{ ... }}");
    println!();
    println!("  #[step(depends_on = \"root\")]  // ← Same parent!");
    println!("  async fn extract_b() -> String {{ ... }}");
    println!();
    println!("  #[step(inputs(a = \"extract_a\", b = \"extract_b\"))]");
    println!("  async fn merge(a: String, b: String) {{ ... }}\n");

    storage.reset().await?;
    let test2 = Arc::new(TestFlow::new("case2".to_string()));
    let start2 = Instant::now();
    let instance2 = Ergon::new_flow(Arc::clone(&test2), Uuid::new_v4(), Arc::clone(&storage));
    let result2 = instance2
        .executor()
        .execute(|f| Box::pin(f.clone().run_case2()))
        .await;
    let elapsed2 = start2.elapsed();

    println!("\nResult: {:?}", result2);
    println!("Time: {:?}", elapsed2);

    if elapsed2.as_millis() > 230 {
        println!("\n✗ SEQUENTIAL: ~250ms");
        println!("  Something went wrong!");
    } else {
        println!("\n✓ PARALLEL: ~200ms (50 + max(100,100) + 50)");
        println!("  root runs, THEN extract_a and extract_b run in PARALLEL, THEN merge");
        println!("  → Explicit depends_on breaks auto-chaining!");
    }

    // =========================================================================
    // CASE 3: Using inputs from common parent
    // =========================================================================
    println!("\n┌───────────────────────────────────────────────────────────┐");
    println!("│ CASE 3: inputs from Common Parent (Autowiring)           │");
    println!("│ Question: Does inputs create parallelism like depends_on?│");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Code:");
    println!("  #[step]");
    println!("  async fn root() -> String {{ ... }}");
    println!();
    println!("  #[step(inputs(config = \"root\"))]");
    println!("  async fn extract_a(config: String) -> String {{ ... }}");
    println!();
    println!("  #[step(inputs(config = \"root\"))]  // ← Same parent via inputs!");
    println!("  async fn extract_b(config: String) -> String {{ ... }}");
    println!();
    println!("  #[step(inputs(a = \"extract_a\", b = \"extract_b\"))]");
    println!("  async fn merge(a: String, b: String) {{ ... }}\n");

    storage.reset().await?;
    let test3 = Arc::new(TestFlow::new("case3".to_string()));
    let start3 = Instant::now();
    let instance3 = Ergon::new_flow(Arc::clone(&test3), Uuid::new_v4(), Arc::clone(&storage));
    let result3 = instance3
        .executor()
        .execute(|f| Box::pin(f.clone().run_case3()))
        .await;
    let elapsed3 = start3.elapsed();

    println!("\nResult: {:?}", result3);
    println!("Time: {:?}", elapsed3);

    if elapsed3.as_millis() > 230 {
        println!("\n✗ SEQUENTIAL: ~250ms");
        println!("  Something went wrong!");
    } else {
        println!("\n✓ PARALLEL: ~200ms (50 + max(100,100) + 50)");
        println!("  root runs, THEN extract_a and extract_b run in PARALLEL, THEN merge");
        println!("  → inputs also breaks auto-chaining (provides explicit dependency)!");
    }

    // =========================================================================
    // CASE 4: Using depends_on = [] to disable auto-chaining
    // =========================================================================
    println!("\n┌───────────────────────────────────────────────────────────┐");
    println!("│ CASE 4: Using depends_on = [] (Disable Auto-Chaining)    │");
    println!("│ Question: Can we get parallelism without a common parent?│");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Code:");
    println!("  #[step]");
    println!("  async fn extract_a() -> String {{ ... }}");
    println!();
    println!("  #[step(depends_on = [])]  // ← Explicitly disable auto-chain!");
    println!("  async fn extract_b() -> String {{ ... }}");
    println!();
    println!("  #[step(inputs(a = \"extract_a\", b = \"extract_b\"))]");
    println!("  async fn merge(a: String, b: String) {{ ... }}\n");

    storage.reset().await?;
    let test4 = Arc::new(TestFlow::new("case4".to_string()));
    let start4 = Instant::now();
    let instance4 = Ergon::new_flow(Arc::clone(&test4), Uuid::new_v4(), Arc::clone(&storage));
    let result4 = instance4
        .executor()
        .execute(|f| Box::pin(f.clone().run_case4()))
        .await;
    let elapsed4 = start4.elapsed();

    println!("\nResult: {:?}", result4);
    println!("Time: {:?}", elapsed4);

    if elapsed4.as_millis() > 180 {
        println!("\n✗ SEQUENTIAL: ~200ms");
        println!("  Something went wrong!");
    } else {
        println!("\n✓ PARALLEL: ~150ms (max(100,100) + 50)");
        println!("  extract_a and extract_b run in PARALLEL, THEN merge");
        println!("  → depends_on = [] explicitly disables auto-chaining!");
    }

    // =========================================================================
    // FINAL VERDICT
    // =========================================================================
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  FINAL VERDICT                                            ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    println!("┌───────────────────────────────┬─────────────────────────┐");
    println!("│ Pattern                       │ Result                  │");
    println!("├───────────────────────────────┼─────────────────────────┤");
    println!("│ No depends_on, no inputs      │ Auto-chains → Sequential│");
    println!("│ depends_on = \"same_parent\"    │ Parallel (breaks chain) │");
    println!("│ inputs(x = \"same_parent\")     │ Parallel (breaks chain) │");
    println!("│ depends_on = []               │ Parallel (no auto-chain)│");
    println!("└───────────────────────────────┴─────────────────────────┘\n");

    println!("THE TRUTH:");
    println!("• Auto-chaining ONLY happens when step has NO dependencies");
    println!("• Both depends_on AND inputs provide explicit dependencies");
    println!("• Explicit dependencies BREAK the auto-chain");
    println!("• depends_on = [] explicitly disables auto-chaining");
    println!("• Multiple steps with SAME parent (via depends_on OR inputs) = PARALLEL");
    println!("• Multiple independent steps (via depends_on = []) = PARALLEL");
    println!("• inputs doesn't force sequential OR parallel - it depends on the parent!");

    Ok(())
}
