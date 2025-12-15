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

use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

static EXECUTION_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

#[derive(Serialize, Deserialize, Clone, FlowType)]
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

        // Generate timestamp at flow level for determinism
        let executed_at = chrono::Utc::now().timestamp();

        let result = self.clone().perform_critical_operation(executed_at).await?;
        let verified = self.clone().verify_and_commit(result).await?;

        println!("  [COMPLETED] Task {} finished", self.task_id);

        Ok(verified)
    }

    #[step]
    async fn perform_critical_operation(
        self: Arc<Self>,
        executed_at: i64,
    ) -> Result<OperationResult, String> {
        println!(
            "    [Step 1] Performing critical operation: {}",
            self.operation
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(OperationResult {
            task_id: self.task_id.clone(),
            operation: self.operation.clone(),
            result_data: format!("DATA-{}", self.task_id),
            executed_at,
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

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct OperationResult {
    task_id: String,
    operation: String,
    result_data: String,
    executed_at: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct TaskResult {
    task_id: String,
    status: String,
    data: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = Scheduler::new(storage.clone());

    let tasks = vec![
        ("TASK-001", "Transfer $10,000 to Account B"),
        ("TASK-002", "Deduct 100 items from inventory"),
        ("TASK-003", "Send password reset email"),
    ];

    for (task_id, operation) in &tasks {
        let task = CriticalTask {
            task_id: task_id.to_string(),
            operation: operation.to_string(),
        };
        let flow_id = Uuid::new_v4();
        scheduler.schedule(task, flow_id).await?;
    }

    let mut worker_handles = Vec::new();

    for worker_id in 1..=5 {
        let storage_clone = storage.clone();
        let worker_name = format!("worker-{}", worker_id);

        let handle = tokio::spawn(async move {
            let worker = Worker::new(storage_clone.clone(), &worker_name)
                .with_poll_interval(Duration::from_millis(10));

            worker
                .register(|flow: Arc<CriticalTask>| async move { flow.execute().await })
                .await;

            let handle = worker.start().await;

            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if let Ok(incomplete) = storage_clone.get_incomplete_flows().await {
                    if incomplete.is_empty() {
                        break;
                    }
                }
            }

            handle.shutdown().await;
        });

        worker_handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in worker_handles {
        handle.await?;
    }

    Ok(())
}
