//! Complex DAG: Push Ergon to the Limit
//!
//! A computation DAG with multiple fan-out/fan-in points and cross-branch dependencies.
//!
//! ```text
//!                           ┌─── mul_2 ────────────────────────────────────────────┐
//!                           │    (20)                                               │
//!         ┌── fetch_a ──────┤                                                       │
//!         │    (10)         │                      ┌── cross_mul ───┐               │
//!         │                 └─── mul_3 ────────────┤     (750)      │               │
//!         │                      (30)              │                │               │
//!         │                                        │                ├── aggregate ──┼── final
//! start ──┤                                        │                │    (1650)     │  (1670)
//!         │                 ┌─── square ───────────┤                │               │
//!         │                 │    (25)              │                │               │
//!         └── fetch_b ──────┤                      └── cross_add ───┘               │
//!              (5)          │                           (775)                       │
//!                           └─── cube ─────────────────────────────────────────────┘
//!                                (125)
//! ```
//!
//! Expected: final = 1670
//! Critical path: fetch → mul/square → cross_mul → cross_add → aggregate → final
//! Expected time: ~300ms (6 levels × 50ms)
//! Sequential would be: ~500ms (10 steps × 50ms)

use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct ComplexDag {
    id: String,
}

impl ComplexDag {
    // =========================================================================
    // Level 0: Start
    // =========================================================================

    #[step]
    async fn start(self: Arc<Self>) -> Result<(), String> {
        println!("[L0] start");
        Ok(())
    }

    // =========================================================================
    // Level 1: Fetch (fan-out from start)
    // =========================================================================

    #[step(depends_on = "start")]
    async fn fetch_a(self: Arc<Self>) -> Result<i64, String> {
        println!("[L1] fetch_a starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[L1] fetch_a = 10");
        Ok(10)
    }

    #[step(depends_on = "start")]
    async fn fetch_b(self: Arc<Self>) -> Result<i64, String> {
        println!("[L1] fetch_b starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[L1] fetch_b = 5");
        Ok(5)
    }

    // =========================================================================
    // Level 2: Compute (fan-out from fetch_a and fetch_b)
    // =========================================================================

    #[step(depends_on = "fetch_a", inputs(a = "fetch_a"))]
    async fn mul_2(self: Arc<Self>, a: i64) -> Result<i64, String> {
        println!("[L2] mul_2 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = a * 2;
        println!("[L2] mul_2 = {} × 2 = {}", a, result);
        Ok(result)
    }

    #[step(depends_on = "fetch_a", inputs(a = "fetch_a"))]
    async fn mul_3(self: Arc<Self>, a: i64) -> Result<i64, String> {
        println!("[L2] mul_3 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = a * 3;
        println!("[L2] mul_3 = {} × 3 = {}", a, result);
        Ok(result)
    }

    #[step(depends_on = "fetch_b", inputs(b = "fetch_b"))]
    async fn square(self: Arc<Self>, b: i64) -> Result<i64, String> {
        println!("[L2] square starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = b * b;
        println!("[L2] square = {}² = {}", b, result);
        Ok(result)
    }

    #[step(depends_on = "fetch_b", inputs(b = "fetch_b"))]
    async fn cube(self: Arc<Self>, b: i64) -> Result<i64, String> {
        println!("[L2] cube starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = b * b * b;
        println!("[L2] cube = {}³ = {}", b, result);
        Ok(result)
    }

    // =========================================================================
    // Level 3: Cross-branch multiplication (fan-in from mul_3, square)
    // =========================================================================

    #[step(
        depends_on = ["mul_3", "square"],
        inputs(m = "mul_3", s = "square")
    )]
    async fn cross_mul(self: Arc<Self>, m: i64, s: i64) -> Result<i64, String> {
        println!("[L3] cross_mul starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = m * s;
        println!("[L3] cross_mul = {} × {} = {}", m, s, result);
        Ok(result)
    }

    // =========================================================================
    // Level 4: Cross-branch addition (fan-in from cross_mul, square)
    // =========================================================================

    #[step(
        depends_on = ["cross_mul", "square"],
        inputs(cm = "cross_mul", s = "square")
    )]
    async fn cross_add(self: Arc<Self>, cm: i64, s: i64) -> Result<i64, String> {
        println!("[L4] cross_add starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = cm + s;
        println!("[L4] cross_add = {} + {} = {}", cm, s, result);
        Ok(result)
    }

    // =========================================================================
    // Level 5: Aggregate (fan-in from cross_mul, cross_add, cube)
    // =========================================================================

    #[step(
        depends_on = ["cross_mul", "cross_add", "cube"],
        inputs(cm = "cross_mul", ca = "cross_add", c = "cube")
    )]
    async fn aggregate(self: Arc<Self>, cm: i64, ca: i64, c: i64) -> Result<i64, String> {
        println!("[L5] aggregate starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = cm + ca + c;
        println!("[L5] aggregate = {} + {} + {} = {}", cm, ca, c, result);
        Ok(result)
    }

    // =========================================================================
    // Level 6: Final (fan-in from mul_2, aggregate)
    // =========================================================================

    #[step(
        depends_on = ["mul_2", "aggregate"],
        inputs(m2 = "mul_2", agg = "aggregate")
    )]
    async fn final_result(self: Arc<Self>, m2: i64, agg: i64) -> Result<i64, String> {
        println!("[L6] final starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = m2 + agg;
        println!("[L6] final = {} + {} = {}", m2, agg, result);
        Ok(result)
    }

    #[flow]
    async fn run(self: Arc<Self>) -> Result<i64, String> {
        dag! {
            self.register_start();
            self.register_fetch_a();
            self.register_fetch_b();
            self.register_mul_2();
            self.register_mul_3();
            self.register_square();
            self.register_cube();
            self.register_cross_mul();
            self.register_cross_add();
            self.register_aggregate();
            self.register_final_result()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════════════════╗");
    println!("║              Complex DAG: Fan-out / Fan-in / Cross-branch             ║");
    println!("╚═══════════════════════════════════════════════════════════════════════╝\n");

    println!("DAG Structure:");
    println!("                          ┌─── mul_2 ────────────────────────────────────────────┐");
    println!("                          │    (20)                                               │");
    println!("        ┌── fetch_a ──────┤                                                       │");
    println!("        │    (10)         │                      ┌── cross_mul ───┐               │");
    println!("        │                 └─── mul_3 ────────────┤     (750)      │               │");
    println!("        │                      (30)              │                │               │");
    println!("        │                                        │                ├── aggregate ──┼── final");
    println!("start ──┤                                        │                │    (1650)     │  (1670)");
    println!("        │                 ┌─── square ───────────┤                │               │");
    println!("        │                 │    (25)              │                │               │");
    println!("        └── fetch_b ──────┤                      └── cross_add ───┘               │");
    println!("             (5)          │                           (775)                       │");
    println!("                          └─── cube ─────────────────────────────────────────────┘");
    println!("                               (125)\n");

    println!("Expected: final = 1670");
    println!("Critical path: 6 levels × 50ms = ~300ms");
    println!("Sequential would be: 10 steps × 50ms = ~500ms\n");
    println!("═══════════════════════════════════════════════════════════════════════════\n");

    let storage = Arc::new(InMemoryExecutionLog::new());
    storage.reset().await?;
    let workflow = Arc::new(ComplexDag {
        id: "complex".into(),
    });

    let start = Instant::now();
    let executor = Executor::new(Uuid::new_v4(), workflow.clone(), storage);
    let result = match executor.execute(|w| Box::pin(w.clone().run())).await {
        FlowOutcome::Completed(r) => r,
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    };
    let elapsed = start.elapsed();

    println!("\n═══════════════════════════════════════════════════════════════════════════");
    println!("\nResult: {:?}", result);
    println!("Time: {:?}", elapsed);

    let expected = 1670i64;
    let correct = result.as_ref().map(|&v| v == expected).unwrap_or(false);

    println!("\n┌─────────────────────────────────────────┐");
    println!("│ Verification                            │");
    println!("├─────────────────────────────────────────┤");
    println!("│ Expected: {:<28} │", expected);
    println!(
        "│ Got:      {:<28} │",
        result
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or("ERROR".into())
    );
    println!(
        "│ Status:   {:<28} │",
        if correct { "✅ CORRECT" } else { "❌ WRONG" }
    );
    println!("├─────────────────────────────────────────┤");
    println!("│ Timing                                  │");
    println!("├─────────────────────────────────────────┤");
    println!("│ Actual:     {:>10.2?}                 │", elapsed);
    println!("│ Sequential: {:>10}                 │", "~500ms");
    println!(
        "│ Speedup:    {:>10.2}x                │",
        500.0 / elapsed.as_millis() as f64
    );
    println!("└─────────────────────────────────────────┘");

    Ok(())
}
// ```

// **Expected output:**
// ```
// ╔═══════════════════════════════════════════════════════════════════════╗
// ║              Complex DAG: Fan-out / Fan-in / Cross-branch             ║
// ╚═══════════════════════════════════════════════════════════════════════╝

// [L0] start
// [L1] fetch_a starting
// [L1] fetch_b starting        ← parallel
// [L1] fetch_a = 10
// [L1] fetch_b = 5
// [L2] mul_2 starting
// [L2] mul_3 starting
// [L2] square starting
// [L2] cube starting           ← all 4 parallel
// [L2] mul_2 = 10 × 2 = 20
// [L2] mul_3 = 10 × 3 = 30
// [L2] square = 5² = 25
// [L2] cube = 5³ = 125
// [L3] cross_mul starting
// [L3] cross_mul = 30 × 25 = 750
// [L4] cross_add starting
// [L4] cross_add = 750 + 25 = 775
// [L5] aggregate starting
// [L5] aggregate = 750 + 775 + 125 = 1650
// [L6] final starting
// [L6] final = 20 + 1650 = 1670

// ┌─────────────────────────────────────────┐
// │ Verification                            │
// ├─────────────────────────────────────────┤
// │ Expected: 1670                          │
// │ Got:      1670                          │
// │ Status:   ✅ CORRECT                    │
// ├─────────────────────────────────────────┤
// │ Timing                                  │
// ├─────────────────────────────────────────┤
// │ Actual:     ~300ms                      │
// │ Sequential: ~500ms                      │
// │ Speedup:    ~1.67x                      │
// └─────────────────────────────────────────┘
