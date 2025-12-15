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

use ergon::executor::{ExecutionError, Executor, FlowOutcome};
use ergon::prelude::*;
use std::time::Duration;

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
    async fn run(self: Arc<Self>) -> Result<i64, ExecutionError> {
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
    let workflow = Arc::new(ComplexDag {
        id: "complex".into(),
    });

    let executor = Executor::new(Uuid::new_v4(), workflow.clone(), storage);
    match executor.execute(|w| Box::pin(w.clone().run())).await {
        FlowOutcome::Completed(_) => {}
        FlowOutcome::Suspended(reason) => return Err(format!("Suspended: {:?}", reason).into()),
    };

    Ok(())
}
