//! Custom Retry Policy Example
//!
//! Demonstrates how to create and use custom retry policies at the flow level.
//!
//! # Custom Policy Shown
//!
//! **QUICK_RETRY**: Fast retries for low-latency operations
//! - 5 attempts, 50ms initial delay, 1.2x backoff, max 1s delay
//!
//! The flow attempts:
//! - Attempt 1: API call fails
//! - Attempt 2: API call fails
//! - Attempt 3: API call succeeds, DB write fails
//! - Attempt 4: API cached, DB write succeeds ✓
//!
//! Run: cargo run --example custom_retry_policy

use ergon::core::RetryPolicy;
use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// CUSTOM RETRY POLICIES
// =============================================================================

/// Quick retry for low-latency API calls
/// Fast retries with minimal backoff
const QUICK_RETRY: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_delay: Duration::from_millis(50),
    max_delay: Duration::from_secs(1),
    backoff_multiplier: 1.2,
};

// =============================================================================
// TRACKING COUNTERS
// =============================================================================

static API_ATTEMPTS: AtomicU32 = AtomicU32::new(0);
static DB_ATTEMPTS: AtomicU32 = AtomicU32::new(0);
static BATCH_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

// =============================================================================
// WORKFLOW WITH CUSTOM RETRY POLICY
// =============================================================================

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct OrderProcessor {
    order_id: String,
}

impl OrderProcessor {
    /// Flow using custom QUICK_RETRY policy
    #[flow(retry = QUICK_RETRY)]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, String> {
        println!("[FLOW] Processing order {}", self.order_id);

        // Step 1: Call external API (will fail and retry)
        let api_result = Arc::clone(&self).call_external_api().await?;

        // Step 2: Write to database (will fail and retry with different policy)
        let db_result = Arc::clone(&self).write_to_database(api_result).await?;

        // Step 3: Process batch (will succeed immediately)
        let batch_result = Arc::clone(&self).process_batch_job(db_result).await?;

        println!("[FLOW] Order {} completed successfully", self.order_id);
        Ok(batch_result)
    }

    #[step]
    async fn call_external_api(self: Arc<Self>) -> Result<String, String> {
        let attempt = API_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        println!("  [Step 1/3] API call attempt #{}", attempt);

        // Fail first 2 times (succeed on 3rd)
        if attempt < 3 {
            println!("    Network timeout (simulated)");
            return Err("Network timeout".to_string());
        }

        println!("    API call succeeded on attempt {}", attempt);
        Ok(format!("API-DATA-{}", self.order_id))
    }

    #[step(inputs(api_data = "call_external_api"))]
    async fn write_to_database(self: Arc<Self>, api_data: String) -> Result<String, String> {
        let attempt = DB_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        println!(
            "  [Step 2/3] DB write attempt #{} (data: {})",
            attempt, api_data
        );

        // Fail first time (succeed on 2nd)
        if attempt < 2 {
            println!("    Lock conflict (simulated)");
            return Err("Database lock conflict".to_string());
        }

        println!("    DB write succeeded on attempt {}", attempt);
        Ok(format!("DB-REC-{}", self.order_id))
    }

    #[step(inputs(db_record = "write_to_database"))]
    async fn process_batch_job(self: Arc<Self>, db_record: String) -> Result<OrderResult, String> {
        let attempt = BATCH_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        println!(
            "  [Step 3/3] Batch processing attempt #{} (record: {})",
            attempt, db_record
        );

        // Succeed immediately
        println!("    Batch processing succeeded on attempt {}", attempt);
        Ok(OrderResult {
            order_id: self.order_id.clone(),
            status: "completed".to_string(),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct OrderResult {
    order_id: String,
    status: String,
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nCustom Retry Policy Example");
    println!("============================");
    println!("Policy: QUICK_RETRY (5 attempts, 50ms initial, 1.2x backoff)\n");

    let db = "data/test_custom_retry.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);
    let scheduler = Scheduler::new(storage.clone());

    let order = OrderProcessor {
        order_id: "ORD-12345".to_string(),
    };
    let flow_id = Uuid::new_v4();
    let task_id = scheduler.schedule(order.clone(), flow_id).await?;

    println!("Scheduled order: {}\n", order.order_id);

    let worker = Worker::new(storage.clone(), "custom-retry-worker")
        .with_poll_interval(Duration::from_millis(50));

    worker
        .register(|flow: Arc<OrderProcessor>| flow.process_order())
        .await;
    let worker_handle = worker.start().await;

    // Wait for flow to complete (with timeout)
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(30) {
            println!("[TIMEOUT] Flow did not complete within 30 seconds");
            break;
        }

        match storage.get_scheduled_flow(task_id).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete) {
                    break;
                } else if matches!(scheduled.status, TaskStatus::Failed) {
                    println!("[ERROR] Flow failed");
                    break;
                }
            }
            None => {
                break;
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    worker_handle.shutdown().await;

    let invocations = storage.get_invocations_for_flow(flow_id).await?;
    let flow_inv = invocations.iter().find(|i| i.step() == 0);

    println!("\n=== Results ===\n");

    if let Some(flow) = flow_inv {
        println!("Flow status: {:?}", flow.status());
        if let Some(result) = flow.return_value() {
            match ergon::core::deserialize_value::<Result<OrderResult, String>>(result) {
                Ok(Ok(order_result)) => println!("Flow result: {:?}", order_result),
                Ok(Err(error)) => println!("Flow error: {}", error),
                Err(e) => println!("Deserialization error: {}", e),
            }
        }
    }

    let api_attempts = API_ATTEMPTS.load(Ordering::SeqCst);
    let db_attempts = DB_ATTEMPTS.load(Ordering::SeqCst);
    let batch_attempts = BATCH_ATTEMPTS.load(Ordering::SeqCst);

    println!("\nStep execution counts:");
    println!("  API calls: {} (expected: 3)", api_attempts);
    println!("  DB writes: {} (expected: 2)", db_attempts);
    println!("  Batch processing: {} (expected: 1)", batch_attempts);

    println!("\n=== How to Create Custom Retry Policies ===\n");
    println!("Define a const RetryPolicy:");
    println!("  const MY_POLICY: RetryPolicy = RetryPolicy {{");
    println!("      max_attempts: 5,");
    println!("      initial_delay: Duration::from_millis(100),");
    println!("      max_delay: Duration::from_secs(10),");
    println!("      backoff_multiplier: 2.0,");
    println!("  }};");

    println!("\nUse it in your flow:");
    println!("  #[flow(retry = MY_POLICY)]");
    println!("  async fn my_flow(self: Arc<Self>) -> Result<T, E> {{ ... }}");

    println!("\nPredefined policies:");
    println!("  - RetryPolicy::NONE (no retries)");
    println!("  - RetryPolicy::STANDARD (3 attempts, 1s initial)");
    println!("  - RetryPolicy::AGGRESSIVE (10 attempts, 100ms initial)");

    Ok(())
}

// ➜  ergon git:(modularize-worker) ✗ cargo run --example custom_retry_policy
//     Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.08s
//      Running `target/debug/examples/custom_retry_policy`

// Custom Retry Policy Example
// ============================
// Policy: QUICK_RETRY (5 attempts, 50ms initial, 1.2x backoff)

// Scheduled order: ORD-12345

// [FLOW] Processing order ORD-12345
//   [Step 1/3] API call attempt #1
//     Network timeout (simulated)
// [FLOW] Processing order ORD-12345
//   [Step 1/3] API call attempt #2
//     Network timeout (simulated)
// [FLOW] Processing order ORD-12345
//   [Step 1/3] API call attempt #3
//     Network timeout (simulated)
// [FLOW] Processing order ORD-12345
//   [Step 1/3] API call attempt #4
//     API call succeeded on attempt 4
//   [Step 2/3] DB write attempt #1 (data: API-DATA-ORD-12345)
//     Lock conflict (simulated)
// [FLOW] Processing order ORD-12345
//   [Step 2/3] DB write attempt #2 (data: API-DATA-ORD-12345)
//     Lock conflict (simulated)
// [ERROR] Flow failed

// === Results ===

// Flow status: Pending

// Step execution counts:
//   API calls: 4
//   DB writes: 2
//   Batch processing: 0

// === How to Create Custom Retry Policies ===

// Define a const RetryPolicy:
//   const MY_POLICY: RetryPolicy = RetryPolicy {
//       max_attempts: 5,
//       initial_delay: Duration::from_millis(100),
//       max_delay: Duration::from_secs(10),
//       backoff_multiplier: 2.0,
//   };

// Use it in your flow:
//   #[flow(retry = MY_POLICY)]
//   async fn my_flow(self: Arc<Self>) -> Result<T, E> { ... }

// Predefined policies:
//   - RetryPolicy::NONE (no retries)
//   - RetryPolicy::STANDARD (3 attempts, 1s initial)
//   - RetryPolicy::AGGRESSIVE (10 attempts, 100ms initial)
// ➜  ergon git:(modularize-worker) ✗
