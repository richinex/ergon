//! Multi-Worker Stress Test with Deterministic Assertions
//!
//! Run with:
//! cargo run --example multi_worker_stress --features=sqlite
//!
//! Scenario:
//! - 4 Concurrent Workers
//! - 30 Concurrent Flows
//! - Single SQLite Database (High Contention)
//! - Mathematical Failure Injection

use dashmap::DashMap;
use ergon::core::InvokableFlow;
use ergon::executor::InvokeChild;
use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::Notify;

// =============================================================================
// GLOBAL METRICS ( The Source of Truth )
// =============================================================================

// We use Atomics to track exactly how many times each step actually ran.
static STEP_VALIDATE: AtomicU32 = AtomicU32::new(0);
static STEP_FRAUD: AtomicU32 = AtomicU32::new(0);
static STEP_INVENTORY: AtomicU32 = AtomicU32::new(0);
static STEP_PAYMENT: AtomicU32 = AtomicU32::new(0);
static STEP_LABEL: AtomicU32 = AtomicU32::new(0);
static STEP_NOTIFY: AtomicU32 = AtomicU32::new(0);

// Track completions to know when to stop the test
static COMPLETED_FLOWS: AtomicU32 = AtomicU32::new(0);
static COMPLETION_NOTIFIER: LazyLock<Arc<Notify>> = LazyLock::new(|| Arc::new(Notify::new()));

// Per-order attempt tracking (to simulate transient failures)
static ORDER_ATTEMPTS: LazyLock<DashMap<String, u32>> = LazyLock::new(DashMap::new);

fn get_attempt(key: &str) -> u32 {
    let mut entry = ORDER_ATTEMPTS.entry(key.to_string()).or_insert(0);
    *entry += 1;
    *entry
}

// =============================================================================
// DOMAIN LOGIC
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShippingLabel {
    tracking: String,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderFlow {
    id: u32, // Numeric ID for deterministic math
    order_ref: String,
}

impl OrderFlow {
    // 1. Validate: Fails once if ID % 3 == 0
    #[step]
    async fn validate(self: Arc<Self>) -> Result<(), String> {
        let attempt = get_attempt(&format!("{}-val", self.order_ref));
        STEP_VALIDATE.fetch_add(1, Ordering::Relaxed);

        // Deterministic Failure Logic
        if self.id % 3 == 0 && attempt == 1 {
            return Err("Simulated Network Blip".to_string());
        }
        Ok(())
    }

    // 2. Fraud: Never fails
    #[step]
    async fn fraud_check(self: Arc<Self>) -> Result<(), String> {
        STEP_FRAUD.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    // 3. Inventory: Fails once if ID % 3 == 1
    #[step]
    async fn reserve_inventory(self: Arc<Self>) -> Result<(), String> {
        let attempt = get_attempt(&format!("{}-inv", self.order_ref));
        STEP_INVENTORY.fetch_add(1, Ordering::Relaxed);

        if self.id % 3 == 1 && attempt == 1 {
            return Err("Simulated Deadlock".to_string());
        }
        Ok(())
    }

    // 4. Payment: Never fails
    #[step]
    async fn process_payment(self: Arc<Self>) -> Result<(), String> {
        STEP_PAYMENT.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    // 5. Notify: Never fails
    #[step]
    async fn notify(self: Arc<Self>, _label: ShippingLabel) -> Result<(), String> {
        STEP_NOTIFY.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    // MAIN FLOW
    #[flow]
    async fn run_order(self: Arc<Self>) -> Result<(), String> {
        // Run steps
        self.clone().validate().await?;
        self.clone().fraud_check().await?;
        // Simulate DB contention delay
        tokio::time::sleep(Duration::from_millis(10)).await;
        self.clone()
            .reserve_inventory()
            .await
            .map_err(|e| e.to_string())?;
        self.clone()
            .process_payment()
            .await
            .map_err(|e| e.to_string())?;

        // Invoke Child
        let label = self
            .invoke(LabelFlow { parent_id: self.id })
            .result()
            .await
            .map_err(|e| e.to_string())?;

        self.clone().notify(label).await?;

        // Signal completion
        let current = COMPLETED_FLOWS.fetch_add(1, Ordering::Relaxed);
        if current + 1 >= 30 {
            // 30 is total orders
            COMPLETION_NOTIFIER.notify_one();
        }
        Ok(())
    }
}

// CHILD FLOW
#[derive(Clone, Serialize, Deserialize, FlowType)]
struct LabelFlow {
    parent_id: u32,
}

impl InvokableFlow for LabelFlow {
    type Output = ShippingLabel;
}

impl LabelFlow {
    #[flow]
    async fn generate(self: Arc<Self>) -> Result<ShippingLabel, String> {
        STEP_LABEL.fetch_add(1, Ordering::Relaxed);
        // Simulate work
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(ShippingLabel {
            tracking: format!("TRK-{}", self.parent_id),
        })
    }
}

// =============================================================================
// MAIN EXECUTION
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("STARTING MULTI-WORKER STRESS TEST");
    println!("   - Workers: 4");
    println!("   - Orders:  30");
    println!("   - DB:      InMemory (Max Concurrency)");

    // 1. Setup Database
    // let db_path = "/tmp/ergon_stress_test.db";
    // let _ = std::fs::remove_file(db_path);
    // let storage = Arc::new(SqliteExecutionLog::new(db_path).await?);
    let storage = Arc::new(InMemoryExecutionLog::default());
    let scheduler = Scheduler::new(storage.clone()).unversioned();

    // 2. Schedule 30 Orders (Deterministic Batch)
    // Group 0 (Mod 0): 10 orders -> Fail Validate
    // Group 1 (Mod 1): 10 orders -> Fail Inventory
    // Group 2 (Mod 2): 10 orders -> Clean
    for i in 0..30 {
        let flow = OrderFlow {
            id: i,
            order_ref: format!("ORD-{:02}", i),
        };
        scheduler.schedule(flow).await?;
    }

    println!("Scheduled 30 flows.");

    // 3. Spawn 4 Workers
    let mut handles = Vec::new();
    for i in 0..4 {
        let s = storage.clone();
        let w = Worker::new(s, format!("worker-{}", i))
            .with_poll_interval(Duration::from_millis(50)) // Aggressive polling
            .with_max_concurrent_flows(5); // Backpressure

        w.register(|f: Arc<OrderFlow>| f.run_order()).await;
        w.register(|f: Arc<LabelFlow>| f.generate()).await;

        handles.push(w.start().await);
    }

    println!("4 Workers running. High contention imminent...");
    println!("Waiting for 30 completions...\n");

    // 4. Wait for completion (Timeout safety)
    tokio::time::timeout(Duration::from_secs(30), COMPLETION_NOTIFIER.notified())
        .await
        .ok();

    // Shutdown workers
    for h in handles {
        h.shutdown().await;
    }

    // Calculate expected counts based on failure injection logic:
    // - 10 orders (id % 3 == 0): fail validate once -> 2 validate attempts each = 20 total
    // - 10 orders (id % 3 == 1): fail inventory once -> 2 inventory attempts each = 20 total
    // - 10 orders (id % 3 == 2): no failures -> 1 attempt each = 10 total
    // Expected validate: 10 (group 0 first) + 10 (group 0 retry) + 10 (group 1) + 10 (group 2) = 40
    // Expected inventory: 10 (group 1 first) + 10 (group 1 retry) + 10 (group 0) + 10 (group 2) = 40
    // All others: 30 (no retries)

    let actual_validate = STEP_VALIDATE.load(Ordering::Relaxed);
    let actual_fraud = STEP_FRAUD.load(Ordering::Relaxed);
    let actual_inventory = STEP_INVENTORY.load(Ordering::Relaxed);
    let actual_payment = STEP_PAYMENT.load(Ordering::Relaxed);
    let actual_label = STEP_LABEL.load(Ordering::Relaxed);
    let actual_notify = STEP_NOTIFY.load(Ordering::Relaxed);

    println!("FINAL STATISTICS REPORT");
    println!("---------------------------------------------------");
    println!("Step          | Expected | Actual | Status");
    println!("--------------|----------|--------|-------");
    println!(
        "Validate      | 40       | {:<6} | {}",
        actual_validate,
        if actual_validate == 40 { "OK" } else { "FAIL" }
    );
    println!(
        "Fraud Check   | 30       | {:<6} | {}",
        actual_fraud,
        if actual_fraud == 30 { "OK" } else { "FAIL" }
    );
    println!(
        "Inventory     | 40       | {:<6} | {}",
        actual_inventory,
        if actual_inventory == 40 { "OK" } else { "FAIL" }
    );
    println!(
        "Payment       | 30       | {:<6} | {}",
        actual_payment,
        if actual_payment == 30 { "OK" } else { "FAIL" }
    );
    println!(
        "Child Flow    | 30       | {:<6} | {}",
        actual_label,
        if actual_label == 30 { "OK" } else { "FAIL" }
    );
    println!(
        "Notify        | 30       | {:<6} | {}",
        actual_notify,
        if actual_notify == 30 { "OK" } else { "FAIL" }
    );
    println!("---------------------------------------------------");

    if actual_validate == 40
        && actual_fraud == 30
        && actual_inventory == 40
        && actual_payment == 30
        && actual_label == 30
        && actual_notify == 30
    {
        println!("SUCCESS: SYSTEM IS DETERMINISTIC UNDER LOAD");
    } else {
        println!("FAILURE: NON-DETERMINISTIC BEHAVIOR DETECTED");
    }

    storage.close().await?;
    Ok(())
}
