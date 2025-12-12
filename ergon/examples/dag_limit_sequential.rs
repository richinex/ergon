//! Complex DAG: Sequential Execution
//!
//! A computation DAG executed sequentially (one step at a time).
//! This demonstrates the difference between parallel DAG execution and sequential steps.
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
//! Sequential execution: 10 steps × 50ms = ~500ms

use ergon::executor::{ExecutionError, Executor, FlowOutcome};
use ergon::prelude::*;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct ComplexDagSequential {
    id: String,
}

impl ComplexDagSequential {
    // =========================================================================
    // Level 0: Start
    // =========================================================================

    #[step]
    async fn start(self: Arc<Self>) -> Result<(), String> {
        println!("[L0] start");
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    // =========================================================================
    // Level 1: Fetch (sequential instead of parallel)
    // =========================================================================

    #[step(depends_on = "start")]
    async fn fetch_a(self: Arc<Self>) -> Result<i64, String> {
        println!("[L1] fetch_a starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[L1] fetch_a = 10");
        Ok(10)
    }

    #[step(depends_on = "fetch_a")]
    async fn fetch_b(self: Arc<Self>) -> Result<i64, String> {
        println!("[L1] fetch_b starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("[L1] fetch_b = 5");
        Ok(5)
    }

    // =========================================================================
    // Level 2: Compute (sequential instead of parallel)
    // =========================================================================

    #[step(depends_on = "fetch_a", inputs(a = "fetch_a"))]
    async fn mul_2(self: Arc<Self>, a: i64) -> Result<i64, String> {
        println!("[L2] mul_2 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = a * 2;
        println!("[L2] mul_2 = {} × 2 = {}", a, result);
        Ok(result)
    }

    #[step(depends_on = "mul_2", inputs(a = "fetch_a"))]
    async fn mul_3(self: Arc<Self>, a: i64) -> Result<i64, String> {
        println!("[L2] mul_3 starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = a * 3;
        println!("[L2] mul_3 = {} × 3 = {}", a, result);
        Ok(result)
    }

    #[step(depends_on = "mul_3", inputs(b = "fetch_b"))]
    async fn square(self: Arc<Self>, b: i64) -> Result<i64, String> {
        println!("[L2] square starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = b * b;
        println!("[L2] square = {}² = {}", b, result);
        Ok(result)
    }

    #[step(depends_on = "square", inputs(b = "fetch_b"))]
    async fn cube(self: Arc<Self>, b: i64) -> Result<i64, String> {
        println!("[L2] cube starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = b * b * b;
        println!("[L2] cube = {}³ = {}", b, result);
        Ok(result)
    }

    // =========================================================================
    // Level 3: Cross-branch multiplication
    // =========================================================================

    #[step(depends_on = "cube", inputs(m = "mul_3", s = "square"))]
    async fn cross_mul(self: Arc<Self>, m: i64, s: i64) -> Result<i64, String> {
        println!("[L3] cross_mul starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = m * s;
        println!("[L3] cross_mul = {} × {} = {}", m, s, result);
        Ok(result)
    }

    // =========================================================================
    // Level 4: Cross-branch addition
    // =========================================================================

    #[step(depends_on = "cross_mul", inputs(cm = "cross_mul", s = "square"))]
    async fn cross_add(self: Arc<Self>, cm: i64, s: i64) -> Result<i64, String> {
        println!("[L4] cross_add starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = cm + s;
        println!("[L4] cross_add = {} + {} = {}", cm, s, result);
        Ok(result)
    }

    // =========================================================================
    // Level 5: Aggregate
    // =========================================================================

    #[step(
        depends_on = "cross_add",
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
    // Level 6: Final
    // =========================================================================

    #[step(depends_on = "aggregate", inputs(m2 = "mul_2", agg = "aggregate"))]
    async fn final_result(self: Arc<Self>, m2: i64, agg: i64) -> Result<i64, String> {
        println!("[L6] final starting");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = m2 + agg;
        println!("[L6] final = {} + {} = {}", m2, agg, result);
        Ok(result)
    }

    #[flow]
    async fn run(self: Arc<Self>) -> Result<i64, ExecutionError> {
        // Sequential execution using dag! macro, but dependencies force sequential order
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
    let storage = Arc::new(InMemoryExecutionLog::new());
    storage.reset().await?;
    let workflow = Arc::new(ComplexDagSequential {
        id: "complex_sequential".into(),
    });

    let start = Instant::now();
    let executor = Executor::new(Uuid::new_v4(), workflow.clone(), storage);
    let result = match executor.execute(|w| Box::pin(w.clone().run())).await {
        FlowOutcome::Completed(r) => r,
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    };
    let elapsed = start.elapsed();

    println!("Result: {:?}", result);
    println!("Time: {:?}", elapsed);

    let expected = 1670i64;
    let correct = result.as_ref().map(|&v| v == expected).unwrap_or(false);

    println!("Expected: {}", expected);
    println!(
        "Got:      {}",
        result
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or("ERROR".into())
    );
    println!("Status:   {}", if correct { "CORRECT" } else { "WRONG" });
    println!("Sequential: {:?}", elapsed);

    Ok(())
}

// **Expected output:**
// ```
// ╔═══════════════════════════════════════════════════════════════════════╗
// ║           Complex DAG: SEQUENTIAL Execution (one at a time)          ║
// ╚═══════════════════════════════════════════════════════════════════════╝
//
// [L0] start
// [L1] fetch_a starting
// [L1] fetch_a = 10
// [L1] fetch_b starting        ← waits for fetch_a (sequential)
// [L1] fetch_b = 5
// [L2] mul_2 starting
// [L2] mul_2 = 10 × 2 = 20
// [L2] mul_3 starting          ← waits for mul_2 (sequential)
// [L2] mul_3 = 10 × 3 = 30
// [L2] square starting
// [L2] square = 5² = 25
// [L2] cube starting           ← waits for square (sequential)
// [L2] cube = 5³ = 125
// [L3] cross_mul starting
// [L3] cross_mul = 30 × 25 = 750
// [L4] cross_add starting
// [L4] cross_add = 750 + 25 = 775
// [L5] aggregate starting
// [L5] aggregate = 750 + 775 + 125 = 1650
// [L6] final starting
// [L6] final = 20 + 1650 = 1670
//
// ┌─────────────────────────────────────────┐
// │ Verification                            │
// ├─────────────────────────────────────────┤
// │ Expected: 1670                          │
// │ Got:      1670                          │
// │ Status:   CORRECT                    │
// ├─────────────────────────────────────────┤
// │ Timing                                  │
// ├─────────────────────────────────────────┤
// │ Sequential: ~500ms                      │
// └─────────────────────────────────────────┘
//
