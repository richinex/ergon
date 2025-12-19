//! Durable Fibonacci (Happy Path)
//!
//! Demonstrates how Ergon steps act as "Checkpoints".
//!
//! Flow:
//! 1. Calculate 0..25  -> Save State (F24, F25) to DB
//! 2. Calculate 26..50 -> Load State -> Save State (F49, F50) to DB
//! 3. Calculate 51..75 -> Load State -> Save State (F74, F75) to DB
//! 4. Calculate 76..100 -> Final Result
//!
//! Run with:
//! cargo run --example fibonacci_happy_path --features=sqlite

use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

// Signal to exit the process when done
static DONE_NOTIFIER: std::sync::LazyLock<Arc<Notify>> =
    std::sync::LazyLock::new(|| Arc::new(Notify::new()));

// =============================================================================
// DOMAIN LOGIC
// =============================================================================

/// The "Memoized" State.
/// This is what gets written to the database between steps.
/// It contains ONLY what is needed to resume calculation.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FibCheckpoint {
    index: u32,
    prev: u128, // F(n-1)
    curr: u128, // F(n)
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct FibonacciFlow;

impl FibonacciFlow {
    /// Pure logic helper: Computes `count` iterations from a starting point
    fn compute_next_chunk(start: FibCheckpoint, count: u32) -> FibCheckpoint {
        let mut prev = start.prev;
        let mut curr = start.curr;
        let start_idx = start.index;

        for _ in 0..count {
            let next = prev + curr;
            prev = curr;
            curr = next;
        }

        FibCheckpoint {
            index: start_idx + count,
            prev,
            curr,
        }
    }

    /// Step 1: 0 -> 25
    #[step]
    async fn chunk_1(self: Arc<Self>) -> Result<FibCheckpoint, String> {
        println!("   [Step 1] Starting from 0...");

        // Initial State: F(0)=0, F(1)=1. We start at index 1.
        let start = FibCheckpoint {
            index: 1,
            prev: 0,
            curr: 1,
        };

        // Compute 24 more to get to index 25
        let result = Self::compute_next_chunk(start, 24);

        println!(
            "   [Step 1] Saved Checkpoint: F({}) = {}",
            result.index, result.curr
        );
        Ok(result)
    }

    /// Step 2: 26 -> 50
    #[step(depends_on = "chunk_1")]
    async fn chunk_2(self: Arc<Self>, prev_state: FibCheckpoint) -> Result<FibCheckpoint, String> {
        println!("   [Step 2] Resuming from F({})...", prev_state.index);

        let result = Self::compute_next_chunk(prev_state, 25);

        println!(
            "   [Step 2] Saved Checkpoint: F({}) = {}",
            result.index, result.curr
        );
        Ok(result)
    }

    /// Step 3: 51 -> 75
    #[step(depends_on = "chunk_2")]
    async fn chunk_3(self: Arc<Self>, prev_state: FibCheckpoint) -> Result<FibCheckpoint, String> {
        println!("   [Step 3] Resuming from F({})...", prev_state.index);

        let result = Self::compute_next_chunk(prev_state, 25);

        println!(
            "   [Step 3] Saved Checkpoint: F({}) = {}",
            result.index, result.curr
        );
        Ok(result)
    }

    /// Step 4: 76 -> 100
    #[step(depends_on = "chunk_3")]
    async fn chunk_4(self: Arc<Self>, prev_state: FibCheckpoint) -> Result<u128, String> {
        println!("   [Step 4] Resuming from F({})...", prev_state.index);

        let result = Self::compute_next_chunk(prev_state, 25);

        println!("   [Step 4] FINAL RESULT: F({})", result.index);
        Ok(result.curr)
    }

    /// Main Orchestrator
    #[flow]
    async fn run(self: Arc<Self>) -> Result<u128, String> {
        // 1. Run Chunk 1 -> Returns Checkpoint A
        let cp_a = self.clone().chunk_1().await?;

        // 2. Run Chunk 2 -> Takes Checkpoint A, Returns Checkpoint B
        let cp_b = self.clone().chunk_2(cp_a).await?;

        // 3. Run Chunk 3 -> Takes Checkpoint B, Returns Checkpoint C
        let cp_c = self.clone().chunk_3(cp_b).await?;

        // 4. Run Chunk 4 -> Takes Checkpoint C, Returns Final u128
        let final_val = self.clone().chunk_4(cp_c).await?;

        DONE_NOTIFIER.notify_one();
        Ok(final_val)
    }
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use In-Memory for speed
    let storage = Arc::new(ergon::storage::InMemoryExecutionLog::default());
    let scheduler = Scheduler::new(storage.clone()).unversioned();

    // Schedule the flow
    scheduler.schedule(FibonacciFlow).await?;

    // Start Worker
    let worker = Worker::new(storage, "calc-worker").with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<FibonacciFlow>| f.run()).await;
    let handle = worker.start().await;

    // Wait for completion
    DONE_NOTIFIER.notified().await;
    handle.shutdown().await;

    Ok(())
}
