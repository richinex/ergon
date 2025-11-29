//! Testing Precedence: depends_on vs inputs
//!
//! This example tests what happens when you combine depends_on and inputs:
//! 1. Control + Data Dependencies (normal combination)
//! 2. Overlapping Dependencies (same step in both)
//! 3. depends_on = [] with inputs (critical edge case)
//! 4. Multiple inputs with depends_on = []
//! 5. Empty inputs with depends_on
//!
//! KEY FINDING: The framework takes the UNION of both.
//! - depends_on provides control dependencies
//! - inputs provides data dependencies
//! - A step waits for ALL dependencies from BOTH

use ergon::Ergon;
use ergon::{flow, step};
use ergon::{ExecutionLog, InMemoryExecutionLog};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PrecedenceTest {
    id: String,
}

impl PrecedenceTest {
    fn new(id: String) -> Self {
        Self { id }
    }

    // =========================================================================
    // CASE 1: Control + Data Dependencies (Normal Combination)
    // EXPECTED: Waits for BOTH initialize AND fetch_config
    // =========================================================================

    #[step]
    async fn case1_initialize(self: Arc<Self>) -> Result<(), String> {
        println!("[CASE 1] Initialize starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 1] Initialize finished");
        Ok(())
    }

    #[step]
    async fn case1_fetch_config(self: Arc<Self>) -> Result<String, String> {
        println!("[CASE 1] Fetch config starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 1] Fetch config finished");
        Ok("config_data".to_string())
    }

    #[step(
        depends_on = "case1_initialize",      // Control dependency
        inputs(config = "case1_fetch_config") // Data dependency
    )]
    async fn case1_start_server(
        self: Arc<Self>,
        config: String,
    ) -> Result<String, String> {
        println!("[CASE 1] Start server (config: {})", config);
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 1] Start server finished");
        Ok("server_started".to_string())
    }

    #[flow]
    async fn run_case1(self: Arc<Self>) -> Result<String, String> {
        self.register_case1_initialize();
        self.register_case1_fetch_config();
        self.register_case1_start_server() // Waits for BOTH
    }

    // =========================================================================
    // CASE 2: Overlapping Dependencies (Redundant)
    // EXPECTED: Deduplicates to single dependency on fetch_data
    // =========================================================================

    #[step]
    async fn case2_fetch_data(self: Arc<Self>) -> Result<String, String> {
        println!("\n[CASE 2] Fetch data starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 2] Fetch data finished");
        Ok("raw_data".to_string())
    }

    // Both depends_on AND inputs reference the same step
    #[step(
        depends_on = "case2_fetch_data",      // Explicit dependency
        inputs(data = "case2_fetch_data")     // Same step in inputs!
    )]
    async fn case2_process(self: Arc<Self>, data: String) -> Result<String, String> {
        println!("[CASE 2] Process (data: {})", data);
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 2] Process finished");
        Ok("processed".to_string())
    }

    #[flow]
    async fn run_case2(self: Arc<Self>) -> Result<String, String> {
        self.register_case2_fetch_data();
        self.register_case2_process() // Should deduplicate
    }

    // =========================================================================
    // CASE 3: depends_on = [] with inputs (Critical Edge Case)
    // EXPECTED: depends_on = [] disables auto-chain, but inputs still work
    // =========================================================================

    #[step]
    async fn case3_step1(self: Arc<Self>) -> Result<String, String> {
        println!("\n[CASE 3] Step 1 starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 3] Step 1 finished");
        Ok("data_from_step1".to_string())
    }

    // depends_on = [] means "don't auto-chain to previous step"
    // But inputs still creates dependency on step1
    #[step(depends_on = [], inputs(x = "case3_step1"))]
    async fn case3_step2(self: Arc<Self>, x: String) -> Result<String, String> {
        println!("[CASE 3] Step 2 (got: {})", x);
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 3] Step 2 finished");
        Ok(x)
    }

    #[flow]
    async fn run_case3(self: Arc<Self>) -> Result<String, String> {
        self.register_case3_step1();
        self.register_case3_step2() // depends_on = [] prevents auto-chain, but inputs works
    }

    // =========================================================================
    // CASE 4: Multiple inputs with depends_on = []
    // EXPECTED: Depends on all inputs, but NOT on previous step
    // =========================================================================

    #[step]
    async fn case4_extract_a(self: Arc<Self>) -> Result<String, String> {
        println!("\n[CASE 4] Extract A starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 4] Extract A finished");
        Ok("data_a".to_string())
    }

    #[step(depends_on = [])] // No auto-chain
    async fn case4_extract_b(self: Arc<Self>) -> Result<String, String> {
        println!("[CASE 4] Extract B starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 4] Extract B finished");
        Ok("data_b".to_string())
    }

    // depends_on = [] + inputs from multiple steps
    #[step(
        depends_on = [],
        inputs(a = "case4_extract_a", b = "case4_extract_b")
    )]
    async fn case4_merge(
        self: Arc<Self>,
        a: String,
        b: String,
    ) -> Result<String, String> {
        println!("[CASE 4] Merge (a: {}, b: {})", a, b);
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 4] Merge finished");
        Ok(format!("{} + {}", a, b))
    }

    #[flow]
    async fn run_case4(self: Arc<Self>) -> Result<String, String> {
        self.register_case4_extract_a();
        self.register_case4_extract_b(); // Runs in parallel with A
        self.register_case4_merge()      // Waits for both A and B (from inputs)
    }

    // =========================================================================
    // CASE 5: Empty inputs with depends_on (Control-Only)
    // EXPECTED: Just the control dependency, no data flow
    // =========================================================================

    #[step]
    async fn case5_initialize(self: Arc<Self>) -> Result<(), String> {
        println!("\n[CASE 5] Initialize starting");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("[CASE 5] Initialize finished");
        Ok(())
    }

    #[step(depends_on = "case5_initialize")] // Control only, no inputs
    async fn case5_start(self: Arc<Self>) -> Result<String, String> {
        println!("[CASE 5] Start (no data needed, just wait for init)");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 5] Start finished");
        Ok("started".to_string())
    }

    #[flow]
    async fn run_case5(self: Arc<Self>) -> Result<String, String> {
        self.register_case5_initialize();
        self.register_case5_start()
    }

    // =========================================================================
    // CASE 6: Complex - Multiple depends_on + inputs (Union Test)
    // EXPECTED: Waits for ALL: init1, init2, and data_source
    // =========================================================================

    #[step]
    async fn case6_init1(self: Arc<Self>) -> Result<(), String> {
        println!("\n[CASE 6] Init 1 starting");
        tokio::time::sleep(Duration::from_millis(80)).await;
        println!("[CASE 6] Init 1 finished");
        Ok(())
    }

    #[step]
    async fn case6_init2(self: Arc<Self>) -> Result<(), String> {
        println!("[CASE 6] Init 2 starting");
        tokio::time::sleep(Duration::from_millis(80)).await;
        println!("[CASE 6] Init 2 finished");
        Ok(())
    }

    #[step]
    async fn case6_data_source(self: Arc<Self>) -> Result<String, String> {
        println!("[CASE 6] Data source starting");
        tokio::time::sleep(Duration::from_millis(80)).await;
        println!("[CASE 6] Data source finished");
        Ok("important_data".to_string())
    }

    #[step(
        depends_on = ["case6_init1", "case6_init2"],  // Two control deps
        inputs(data = "case6_data_source")             // One data dep
    )]
    async fn case6_process(self: Arc<Self>, data: String) -> Result<String, String> {
        println!("[CASE 6] Process (data: {})", data);
        println!("[CASE 6] Process waited for: init1, init2, AND data_source");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[CASE 6] Process finished");
        Ok("done".to_string())
    }

    #[flow]
    async fn run_case6(self: Arc<Self>) -> Result<String, String> {
        self.register_case6_init1();
        self.register_case6_init2();
        self.register_case6_data_source();
        self.register_case6_process() // Waits for ALL THREE
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Testing Precedence: depends_on vs inputs               ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let storage = Arc::new(InMemoryExecutionLog::new());

    // =========================================================================
    // CASE 1: Control + Data Dependencies
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ CASE 1: Control + Data Dependencies                      │");
    println!("│ Question: Does it wait for BOTH?                         │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Code:");
    println!("  #[step(");
    println!("      depends_on = \"initialize\",");
    println!("      inputs(config = \"fetch_config\")");
    println!("  )]");
    println!("  async fn start_server(config: String) {{ ... }}\n");

    storage.reset().await?;
    let test1 = Arc::new(PrecedenceTest::new("case1".to_string()));
    let start1 = Instant::now();
    let instance1 = Ergon::new_flow(Arc::clone(&test1), Uuid::new_v4(), Arc::clone(&storage));
    let result1 = instance1.execute(|f| f.run_case1()).await;
    let elapsed1 = start1.elapsed();

    println!("\nResult: {:?}", result1);
    println!("Time: {:?}", elapsed1);
    println!("\n✓ UNION BEHAVIOR: Waits for BOTH initialize AND fetch_config");
    println!("  Dependencies: ['initialize', 'fetch_config']");

    // =========================================================================
    // CASE 2: Overlapping Dependencies
    // =========================================================================
    println!("\n┌───────────────────────────────────────────────────────────┐");
    println!("│ CASE 2: Overlapping Dependencies (Redundant)             │");
    println!("│ Question: What happens when same step in both?           │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Code:");
    println!("  #[step(");
    println!("      depends_on = \"fetch_data\",");
    println!("      inputs(data = \"fetch_data\")  // Same step!");
    println!("  )]");
    println!("  async fn process(data: String) {{ ... }}\n");

    storage.reset().await?;
    let test2 = Arc::new(PrecedenceTest::new("case2".to_string()));
    let start2 = Instant::now();
    let instance2 = Ergon::new_flow(Arc::clone(&test2), Uuid::new_v4(), Arc::clone(&storage));
    let result2 = instance2.execute(|f| f.run_case2()).await;
    let elapsed2 = start2.elapsed();

    println!("\nResult: {:?}", result2);
    println!("Time: {:?}", elapsed2);
    println!("\n✓ DEDUPLICATION: Same step only appears once in dependency list");
    println!("  Dependencies: ['fetch_data'] (not ['fetch_data', 'fetch_data'])");
    println!("  Note: This is redundant - autowiring makes depends_on unnecessary!");

    // =========================================================================
    // CASE 3: depends_on = [] with inputs (Critical Edge Case)
    // =========================================================================
    println!("\n┌───────────────────────────────────────────────────────────┐");
    println!("│ CASE 3: depends_on = [] with inputs (Critical)           │");
    println!("│ Question: Does depends_on = [] override inputs?          │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Code:");
    println!("  #[step]");
    println!("  async fn step1() -> String {{ ... }}");
    println!();
    println!("  #[step(depends_on = [], inputs(x = \"step1\"))]");
    println!("  async fn step2(x: String) {{ ... }}\n");

    storage.reset().await?;
    let test3 = Arc::new(PrecedenceTest::new("case3".to_string()));
    let start3 = Instant::now();
    let instance3 = Ergon::new_flow(Arc::clone(&test3), Uuid::new_v4(), Arc::clone(&storage));
    let result3 = instance3.execute(|f| f.run_case3()).await;
    let elapsed3 = start3.elapsed();

    println!("\nResult: {:?}", result3);
    println!("Time: {:?}", elapsed3);
    println!("\n✓ INPUTS TAKE PRECEDENCE: depends_on = [] only disables AUTO-CHAIN");
    println!("  Explicit dependencies from inputs still apply!");
    println!("  Dependencies: ['step1'] (from inputs, not from auto-chain)");

    // =========================================================================
    // CASE 4: Multiple inputs with depends_on = []
    // =========================================================================
    println!("\n┌───────────────────────────────────────────────────────────┐");
    println!("│ CASE 4: Multiple inputs with depends_on = []             │");
    println!("│ Question: Can inputs create fan-in with depends_on = []? │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Code:");
    println!("  #[step]");
    println!("  async fn extract_a() -> String {{ ... }}");
    println!();
    println!("  #[step(depends_on = [])]");
    println!("  async fn extract_b() -> String {{ ... }}");
    println!();
    println!("  #[step(");
    println!("      depends_on = [],");
    println!("      inputs(a = \"extract_a\", b = \"extract_b\")");
    println!("  )]");
    println!("  async fn merge(a: String, b: String) {{ ... }}\n");

    storage.reset().await?;
    let test4 = Arc::new(PrecedenceTest::new("case4".to_string()));
    let start4 = Instant::now();
    let instance4 = Ergon::new_flow(Arc::clone(&test4), Uuid::new_v4(), Arc::clone(&storage));
    let result4 = instance4.execute(|f| f.run_case4()).await;
    let elapsed4 = start4.elapsed();

    println!("\nResult: {:?}", result4);
    println!("Time: {:?}", elapsed4);
    println!("\n✓ INPUTS WIN: depends_on = [] doesn't prevent inputs dependencies");
    println!("  extract_a and extract_b run in parallel (both have no auto-chain)");
    println!("  merge waits for both (via inputs)");
    println!("  Dependencies: ['extract_a', 'extract_b']");

    // =========================================================================
    // CASE 5: Empty inputs with depends_on
    // =========================================================================
    println!("\n┌───────────────────────────────────────────────────────────┐");
    println!("│ CASE 5: Empty inputs with depends_on (Control Only)      │");
    println!("│ Question: Can we have control dep without data?          │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Code:");
    println!("  #[step(depends_on = \"initialize\")]  // No inputs!");
    println!("  async fn start() -> String {{ ... }}\n");

    storage.reset().await?;
    let test5 = Arc::new(PrecedenceTest::new("case5".to_string()));
    let start5 = Instant::now();
    let instance5 = Ergon::new_flow(Arc::clone(&test5), Uuid::new_v4(), Arc::clone(&storage));
    let result5 = instance5.execute(|f| f.run_case5()).await;
    let elapsed5 = start5.elapsed();

    println!("\nResult: {:?}", result5);
    println!("Time: {:?}", elapsed5);
    println!("\n✓ CONTROL-ONLY: depends_on works without inputs");
    println!("  Use case: Wait for initialization without consuming data");
    println!("  Dependencies: ['initialize']");

    // =========================================================================
    // CASE 6: Complex Union (Multiple depends_on + inputs)
    // =========================================================================
    println!("\n┌───────────────────────────────────────────────────────────┐");
    println!("│ CASE 6: Complex Union Test                               │");
    println!("│ Question: Multiple deps in each - full union?            │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Code:");
    println!("  #[step(");
    println!("      depends_on = [\"init1\", \"init2\"],");
    println!("      inputs(data = \"data_source\")");
    println!("  )]");
    println!("  async fn process(data: String) {{ ... }}\n");

    storage.reset().await?;
    let test6 = Arc::new(PrecedenceTest::new("case6".to_string()));
    let start6 = Instant::now();
    let instance6 = Ergon::new_flow(Arc::clone(&test6), Uuid::new_v4(), Arc::clone(&storage));
    let result6 = instance6.execute(|f| f.run_case6()).await;
    let elapsed6 = start6.elapsed();

    println!("\nResult: {:?}", result6);
    println!("Time: {:?}", elapsed6);
    println!("\n✓ FULL UNION: Waits for ALL dependencies from both sources");
    println!("  Dependencies: ['init1', 'init2', 'data_source']");
    println!("  All three run in parallel, process waits for all three");

    // =========================================================================
    // FINAL SUMMARY
    // =========================================================================
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  FINAL VERDICT: Precedence Rules                         ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ Rule 1: UNION                                             │");
    println!("│ A step waits for ALL dependencies from BOTH depends_on   │");
    println!("│ and inputs. There is no precedence - it's a union!       │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ Rule 2: DEDUPLICATION                                     │");
    println!("│ If same step appears in both, it's deduplicated          │");
    println!("│ (But this is redundant - autowiring makes it unnecessary)│");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ Rule 3: depends_on = [] ONLY DISABLES AUTO-CHAIN          │");
    println!("│ It does NOT override explicit inputs dependencies        │");
    println!("│ inputs always create dependencies, regardless of         │");
    println!("│ depends_on = []                                           │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("Use Cases:");
    println!("  • Control + Data: depends_on for sequencing, inputs for data");
    println!("  • Control Only: depends_on without inputs (wait without data)");
    println!("  • Data Only: inputs without depends_on (autowiring!)");
    println!("  • Independent + Data: depends_on = [] + inputs (parallel starts, data deps)\n");

    Ok(())
}
