//! Sequential Multi-Worker Test - No DAG
//!
//! This example tests flow restart behavior with:
//! - Multiple workers (3 workers processing concurrently)
//! - Multiple orders (3 orders processed in parallel)
//! - Sequential steps (no DAG parallelism within each order)
//! - Child flow invocation
//! - Retry logic
//!
//! This isolates whether the restart issue is DAG-specific or a multi-worker problem.

use chrono::Utc;
use dashmap::DashMap;
use ergon::core::{FlowType, InvokableFlow, RetryableError};
use ergon::executor::{ExecutionError, InvokeChild, Worker};
use ergon::prelude::*;
use ergon::storage::SqliteExecutionLog;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

// Global counters for totals
static PARENT_START_COUNT: AtomicU32 = AtomicU32::new(0);
static CHILD_EXEC_COUNT: AtomicU32 = AtomicU32::new(0);

// Per-order attempt tracking
static ORDER_ATTEMPTS: LazyLock<DashMap<String, OrderAttempts>> = LazyLock::new(DashMap::new);

#[derive(Default)]
struct OrderAttempts {
    step1: AtomicU32,
    step2: AtomicU32,
    step3: AtomicU32,
}

impl OrderAttempts {
    fn inc_step1(order_id: &str) -> u32 {
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.step1.fetch_add(1, Ordering::Relaxed) + 1;
        }
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .step1
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    fn inc_step2(order_id: &str) -> u32 {
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.step2.fetch_add(1, Ordering::Relaxed) + 1;
        }
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .step2
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }

    fn inc_step3(order_id: &str) -> u32 {
        if let Some(attempts) = ORDER_ATTEMPTS.get(order_id) {
            return attempts.step3.fetch_add(1, Ordering::Relaxed) + 1;
        }
        ORDER_ATTEMPTS
            .entry(order_id.to_string())
            .or_default()
            .step3
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }
}

fn timestamp() -> f64 {
    Utc::now().timestamp_millis() as f64 / 1000.0
}

// =============================================================================
// Custom Error Type (Retryable)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ProcessError {
    TransientFailure,
    PermanentFailure,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcessError::TransientFailure => write!(f, "Transient failure (will retry)"),
            ProcessError::PermanentFailure => write!(f, "Permanent failure (no retry)"),
        }
    }
}

impl std::error::Error for ProcessError {}

impl From<ProcessError> for ExecutionError {
    fn from(e: ProcessError) -> Self {
        ExecutionError::Failed(e.to_string())
    }
}

impl RetryableError for ProcessError {
    fn is_retryable(&self) -> bool {
        matches!(self, ProcessError::TransientFailure)
    }
}

// =============================================================================
// Child Flow
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GenerateLabel {
    order_id: String,
}

impl FlowType for GenerateLabel {
    fn type_id() -> &'static str {
        "GenerateLabel"
    }
}

impl InvokableFlow for GenerateLabel {
    type Output = String;
}

impl GenerateLabel {
    #[flow]
    async fn generate(self: Arc<Self>) -> Result<String, ExecutionError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        let count = CHILD_EXEC_COUNT.fetch_add(1, Ordering::Relaxed) + 1;

        println!(
            "[{:.3}]     CHILD[{}] flow_id={}: Generating label (execution #{})",
            timestamp(),
            self.order_id,
            &flow_id.to_string()[..8],
            count
        );

        tokio::time::sleep(Duration::from_millis(200)).await;

        let label = format!("LABEL-{}", uuid::Uuid::new_v4().to_string()[..8].to_uppercase());

        println!(
            "[{:.3}]     CHILD[{}]: Generated {}",
            timestamp(),
            self.order_id,
            label
        );

        Ok(label)
    }
}

// =============================================================================
// Parent Flow with Sequential Steps
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderProcessing {
    order_id: String,
    simulate_step1_retry: bool,
    simulate_step2_retry: bool,
}

impl FlowType for OrderProcessing {
    fn type_id() -> &'static str {
        "OrderProcessing"
    }
}

impl OrderProcessing {
    /// Step 1: Validate Order
    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<String, ProcessError> {
        let count = OrderAttempts::inc_step1(&self.order_id);

        println!(
            "[{:.3}]   [{}] Step 1: validate_order (attempt #{})",
            timestamp(),
            self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate transient error on first attempt (per-order tracking!)
        if self.simulate_step1_retry && count == 1 {
            println!(
                "[{:.3}]      -> Transient error, will retry",
                timestamp()
            );
            return Err(ProcessError::TransientFailure);
        }

        println!("[{:.3}]      -> Order validated", timestamp());
        Ok(format!("validated-{}", self.order_id))
    }

    /// Step 2: Check Inventory
    #[step]
    async fn check_inventory(self: Arc<Self>) -> Result<String, ProcessError> {
        let count = OrderAttempts::inc_step2(&self.order_id);

        println!(
            "[{:.3}]   [{}] Step 2: check_inventory (attempt #{})",
            timestamp(),
            self.order_id,
            count
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate transient error on first attempt (per-order tracking!)
        if self.simulate_step2_retry && count == 1 {
            println!(
                "[{:.3}]      -> Warehouse timeout, will retry",
                timestamp()
            );
            return Err(ProcessError::TransientFailure);
        }

        println!("[{:.3}]      -> Inventory available", timestamp());
        Ok(format!("inventory-ok-{}", self.order_id))
    }

    /// Step 3: Generate Label (CHILD FLOW)
    #[step]
    async fn generate_shipping_label(self: Arc<Self>) -> Result<String, ExecutionError> {
        let count = OrderAttempts::inc_step3(&self.order_id);

        println!(
            "[{:.3}]   [{}] Step 3: generate_shipping_label (attempt #{}) - INVOKING CHILD",
            timestamp(),
            self.order_id,
            count
        );

        // Invoke child flow
        let label = self
            .invoke(GenerateLabel {
                order_id: self.order_id.clone(),
            })
            .result()
            .await?;

        println!(
            "[{:.3}]      -> Label received: {}",
            timestamp(),
            label
        );

        Ok(label)
    }

    /// Main flow - runs steps SEQUENTIALLY (no DAG)
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        let count = PARENT_START_COUNT.fetch_add(1, Ordering::Relaxed) + 1;

        println!(
            "\n[{:.3}] PARENT[{}] flow_id={}: START #{} ================================",
            timestamp(),
            self.order_id,
            &flow_id.to_string()[..8],
            count
        );

        // Sequential execution - each step completes before next starts
        println!(
            "[{:.3}] PARENT[{}]: Executing steps sequentially...",
            timestamp(),
            self.order_id
        );

        let _validation = self.clone().validate_order().await?;
        let _inventory = self.clone().check_inventory().await?;
        let label = self.clone().generate_shipping_label().await?;

        println!(
            "[{:.3}] PARENT[{}] flow_id={}: COMPLETE ================================\n",
            timestamp(),
            self.order_id,
            &flow_id.to_string()[..8]
        );

        Ok(format!("Order {} processed with label: {}", self.order_id, label))
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║      Sequential Multi-Worker Test (SQLite)               ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");
    println!("Testing: 3 workers, 3 orders, sequential steps, NO DAG\n");

    // Setup storage
    let storage = Arc::new(SqliteExecutionLog::new(":memory:").await?);
    storage.reset().await?;

    // Schedule 3 orders
    let scheduler = ergon::executor::Scheduler::new(storage.clone());

    println!("Scheduling orders...");
    for i in 1..=3 {
        let order = OrderProcessing {
            order_id: format!("ORD-{:03}", i),
            simulate_step1_retry: i % 2 == 1,  // Orders 1, 3 retry step 1
            simulate_step2_retry: i == 2,      // Order 2 retries step 2
        };
        scheduler.schedule(order, uuid::Uuid::new_v4()).await?;
        println!("   - ORD-{:03} scheduled", i);
    }

    println!("\nStarting 3 workers...\n");

    // Spawn 3 workers
    let workers = (1..=3)
        .map(|i| {
            let storage_clone = storage.clone();
            let worker_name = format!("worker-{}", i);

            tokio::spawn(async move {
                let worker = Worker::new(storage_clone, &worker_name)
                    .with_poll_interval(Duration::from_millis(50));

                worker
                    .register(|flow: Arc<OrderProcessing>| flow.process())
                    .await;
                worker
                    .register(|flow: Arc<GenerateLabel>| flow.generate())
                    .await;

                let handle = worker.start().await;
                tokio::time::sleep(Duration::from_secs(30)).await;
                handle.shutdown().await;
            })
        })
        .collect::<Vec<_>>();

    // Wait for all workers
    for worker in workers {
        worker.await?;
    }

    // Print results
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║                         Results                           ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    println!("Total Counts (3 orders across 3 workers):");
    println!("  Parent starts:       {}", PARENT_START_COUNT.load(Ordering::Relaxed));
    println!("  Child executions:    {}", CHILD_EXEC_COUNT.load(Ordering::Relaxed));

    println!("\nPer-Order Step Attempts:");
    for i in 1..=3 {
        let order_id = format!("ORD-{:03}", i);
        if let Some(attempts) = ORDER_ATTEMPTS.get(&order_id) {
            println!("  {}: step1={}, step2={}, step3={}",
                order_id,
                attempts.step1.load(Ordering::Relaxed),
                attempts.step2.load(Ordering::Relaxed),
                attempts.step3.load(Ordering::Relaxed)
            );
        }
    }

    println!("\nExpected (with per-order retry tracking):");
    println!("  ORD-001: 3 starts (step1 retry + child invocation)");
    println!("    - step1: 2 (fail + retry), step2: 1, step3: 2 (invoke + cached)");
    println!("  ORD-002: 3 starts (step2 retry + child invocation)");
    println!("    - step1: 1, step2: 2 (fail + retry), step3: 2 (invoke + cached)");
    println!("  ORD-003: 3 starts (step1 retry + child invocation)");
    println!("    - step1: 2 (fail + retry), step2: 1, step3: 2 (invoke + cached)");
    println!("  Total parent starts: 9");
    println!("  Total child execs: 3");

    let total_starts = PARENT_START_COUNT.load(Ordering::Relaxed);
    let total_children = CHILD_EXEC_COUNT.load(Ordering::Relaxed);

    println!("\nAnalysis:");
    if total_starts == 9 && total_children == 3 {
        println!("✅ PERFECT: Retries working correctly with per-order tracking!");
    } else {
        println!("   Parent starts: {} (expected 9)", total_starts);
        println!("   Child execs: {} (expected 3)", total_children);
        if total_starts < 9 {
            println!("   (Some orders may not have completed - increase timeout)");
        }
    }

    println!("\nKey Observations:");
    println!("  - Per-order counters (DashMap) enable correct retry logic");
    println!("  - Multiple workers process orders concurrently");
    println!("  - Each order has independent retry state");
    println!("  - Sequential steps, NO DAG complexity");

    Ok(())
}
