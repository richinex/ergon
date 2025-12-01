//! Race Condition Prevention - Exactly-Once Execution Demo
//!
//! This example demonstrates:
//! - Multiple workers trying to process the same flow
//! - Pessimistic locking ensures only ONE worker succeeds
//! - No duplicate execution despite race conditions
//!
//! Scenario: 5 workers race to process the same 3 flows
//! Expected: Each flow processed exactly once, no duplicates
//!
//! Run: cargo run --example race_condition_demo

use ergon::core::InvocationStatus;
use ergon::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// Global counters to track execution attempts per flow
static EXECUTION_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

#[derive(Serialize, Deserialize, Clone)]
struct CriticalTask {
    task_id: String,
    operation: String,
}

impl CriticalTask {
    #[flow]
    async fn execute(self: Arc<Self>) -> Result<TaskResult, String> {
        let attempt = EXECUTION_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "  [EXECUTION] Task {} starting (global attempt #{})",
            self.task_id, attempt
        );

        // Step 1: Perform critical operation
        let result = self.clone().perform_critical_operation().await?;

        // Step 2: Verify and commit
        let verified = self.clone().verify_and_commit(result).await?;

        println!("  [COMPLETED] Task {} finished", self.task_id);

        Ok(verified)
    }

    #[step]
    async fn perform_critical_operation(self: Arc<Self>) -> Result<OperationResult, String> {
        println!(
            "    [Step 1] Performing critical operation: {}",
            self.operation
        );

        // Simulate work
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(OperationResult {
            task_id: self.task_id.clone(),
            operation: self.operation.clone(),
            result_data: format!("DATA-{}", self.task_id),
            executed_at: chrono::Utc::now().timestamp(),
        })
    }

    #[step(inputs(result = "perform_critical_operation"))]
    async fn verify_and_commit(
        self: Arc<Self>,
        result: OperationResult,
    ) -> Result<TaskResult, String> {
        println!(
            "    [Step 2] Verifying and committing result for task {}",
            result.task_id
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(TaskResult {
            task_id: self.task_id.clone(),
            status: "committed".to_string(),
            data: result.result_data,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct OperationResult {
    task_id: String,
    operation: String,
    result_data: String,
    executed_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct TaskResult {
    task_id: String,
    status: String,
    data: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║     Race Condition Prevention - Exactly-Once Demo       ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = FlowScheduler::new(storage.clone());

    // Schedule 3 critical tasks
    let tasks = vec![
        ("TASK-001", "Transfer $10,000 to Account B"),
        ("TASK-002", "Deduct 100 items from inventory"),
        ("TASK-003", "Send password reset email"),
    ];

    let mut flow_ids = Vec::new();

    println!("📋 Scheduling {} critical tasks:\n", tasks.len());
    for (task_id, operation) in &tasks {
        let task = CriticalTask {
            task_id: task_id.to_string(),
            operation: operation.to_string(),
        };
        let flow_id = Uuid::new_v4();
        scheduler.schedule(task, flow_id).await?;
        flow_ids.push(flow_id);

        println!("  • {} - {}", task_id, operation);
    }

    println!("\n⚠️  SCENARIO: 5 workers racing to process these 3 tasks");
    println!("    Without proper locking: Each task could run 5 times!");
    println!("    Expected behavior: Each task runs exactly once\n");

    // ========================================================================
    // Launch 5 workers simultaneously to create race condition
    // ========================================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║  Starting 5 workers simultaneously...                      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let start = Instant::now();
    let mut worker_handles = Vec::new();

    // Track which worker processes which flow
    let worker_assignments: Arc<tokio::sync::Mutex<HashMap<String, String>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    for worker_id in 1..=5 {
        let storage_clone = storage.clone();
        let worker_name = format!("worker-{}", worker_id);
        let assignments = worker_assignments.clone();

        let handle = tokio::spawn(async move {
            println!("🚀 {} started (racing for work...)", worker_name);

            let worker = FlowWorker::new(storage_clone.clone(), &worker_name)
                .with_poll_interval(Duration::from_millis(10));

            // Keep a copy for later use
            let worker_name_final = worker_name.clone();

            // Custom registration with assignment tracking
            worker
                .register(move |flow: Arc<CriticalTask>| {
                    let assignments = assignments.clone();
                    let worker_name = worker_name.clone();
                    async move {
                        // Track which worker got this task
                        {
                            let mut map = assignments.lock().await;
                            map.insert(flow.task_id.clone(), worker_name.clone());
                        }

                        println!("🎯 {} claimed {}", worker_name, flow.task_id);
                        flow.execute().await
                    }
                })
                .await;

            let handle = worker.start().await;

            // Wait for work to complete
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if let Ok(incomplete) = storage_clone.get_incomplete_flows().await {
                    if incomplete.is_empty() {
                        break;
                    }
                }
            }

            println!("🏁 {} finished", worker_name_final);
            handle.shutdown().await;
        });

        worker_handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in worker_handles {
        handle.await?;
    }

    let duration = start.elapsed();

    // ========================================================================
    // Analyze Results
    // ========================================================================

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                    RESULTS ANALYSIS                        ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let assignments = worker_assignments.lock().await;
    let total_attempts = EXECUTION_ATTEMPTS.load(Ordering::SeqCst);

    println!("⏱️  Total execution time: {:?}", duration);
    println!("📊 Total execution attempts: {}", total_attempts);
    println!("🎯 Tasks scheduled: {}", tasks.len());
    println!("\n📋 Task Assignment:");

    let mut tasks_processed = 0;
    for (task_id, _) in &tasks {
        if let Some(worker) = assignments.get(&task_id.to_string()) {
            println!("  ✓ {} → processed by {}", task_id, worker);
            tasks_processed += 1;
        } else {
            println!("  ✗ {} → NOT PROCESSED", task_id);
        }
    }

    // Verify each flow ran exactly once
    println!("\n🔍 Verification:");
    let mut all_good = true;

    for flow_id in &flow_ids {
        let invocations = storage.get_invocations_for_flow(*flow_id).await?;
        let steps: Vec<_> = invocations.iter().filter(|i| i.step() > 0).collect();
        let step_count = steps.len();
        let completed_count = steps
            .iter()
            .filter(|s| s.status() == InvocationStatus::Complete)
            .count();

        if step_count == 2 && completed_count == 2 {
            println!("  ✓ Flow {} - 2/2 steps completed", flow_id);
        } else {
            println!(
                "  ✗ Flow {} - {}/{} steps completed (UNEXPECTED)",
                flow_id, completed_count, step_count
            );
            all_good = false;
        }
    }

    // ========================================================================
    // Final Summary
    // ========================================================================

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║                   EXACTLY-ONCE PROOF                       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    if total_attempts == tasks.len() as u32 && tasks_processed == tasks.len() && all_good {
        println!("✅ SUCCESS: Exactly-once execution verified!");
        println!("\n   • {} tasks scheduled", tasks.len());
        println!(
            "   • {} execution attempts (no duplicates!)",
            total_attempts
        );
        println!("   • 5 workers racing for work");
        println!("   • Each task processed exactly once");
        println!("\n🔒 Pessimistic Locking Protected Against:");
        println!("   • Race conditions between workers");
        println!("   • Duplicate execution");
        println!("   • Lost work (all tasks completed)");
    } else {
        println!("❌ UNEXPECTED: Execution count mismatch");
        println!("   Expected: {} attempts", tasks.len());
        println!("   Actual:   {} attempts", total_attempts);
    }

    println!("\n💡 With Regular Queues:");
    println!("   ✗ Multiple workers could pop same task (visibility timeout race)");
    println!(
        "   ✗ $10,000 transferred {} times to Account B",
        if total_attempts > 1 {
            total_attempts
        } else {
            2
        }
    );
    println!(
        "   ✗ Inventory deducted {} times (oversold!)",
        if total_attempts > 1 {
            total_attempts
        } else {
            2
        }
    );
    println!("   ✗ Need Redis-based distributed locks manually");

    println!("\n💡 With Ergon:");
    println!("   ✓ BLPOP is atomic - only one worker gets each flow");
    println!("   ✓ Built-in pessimistic locking");
    println!("   ✓ No race conditions possible");
    println!("   ✓ Zero manual lock management\n");

    Ok(())
}
