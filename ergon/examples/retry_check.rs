//! Retry Policy Timing Check
//!
//! Validates retry policy exponential backoff timing with actual vs expected delay comparison.
//!
//! Run: cargo run --example retry_check --features=sqlite

use chrono::{DateTime, Utc};
use ergon::core::RetryPolicy;
use ergon::prelude::*;
use ergon::TaskStatus;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

static ATTEMPT_COUNT: AtomicU32 = AtomicU32::new(0);
static TIMESTAMPS: std::sync::Mutex<Vec<DateTime<Utc>>> = std::sync::Mutex::new(Vec::new());

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct RetryCheckFlow {
    id: String,
}

impl RetryCheckFlow {
    #[flow(retry = RetryPolicy::STANDARD)]
    async fn process(self: Arc<Self>) -> Result<String, String> {
        let timestamp = Utc::now();
        self.clone().failing_step(timestamp).await
    }

    #[step]
    async fn failing_step(self: Arc<Self>, now: DateTime<Utc>) -> Result<String, String> {
        let attempt = ATTEMPT_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        TIMESTAMPS.lock().unwrap().push(now);

        println!("\n[Attempt #{}]", attempt);
        println!("Time: {}", now.format("%H:%M:%S%.3f"));

        if attempt > 1 {
            let timestamps = TIMESTAMPS.lock().unwrap();
            if timestamps.len() >= 2 {
                let prev = timestamps[timestamps.len() - 2];
                let current = timestamps[timestamps.len() - 1];
                let actual_delay = (current - prev).num_milliseconds();

                let retry_num = attempt - 2;
                let expected_delay_secs = 1.0 * 2.0_f64.powi(retry_num as i32);
                let expected_delay_ms = (expected_delay_secs * 1000.0) as i64;

                println!("Expected delay: ~{:.1}s", expected_delay_secs);
                println!("Actual delay:   {:.3}s", actual_delay as f64 / 1000.0);

                let diff_ms = (actual_delay - expected_delay_ms).abs();
                let tolerance_ms = 200;

                if diff_ms <= tolerance_ms {
                    println!("Status: OK (within {}ms tolerance)", tolerance_ms);
                } else {
                    println!(
                        "Status: WARN - Differs by {}ms (tolerance: {}ms)",
                        diff_ms, tolerance_ms
                    );
                }
            }
        }

        if attempt < 3 {
            println!("Result: FAILED (will retry)");
            return Err(format!("Simulated failure on attempt {}", attempt));
        }

        println!("Result: SUCCESS");
        Ok(format!("Completed after {} attempts", attempt))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = "/tmp/ergon_retry_check.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);
    let scheduler = Scheduler::new(storage.clone()).unversioned();

    let flow = RetryCheckFlow {
        id: "retry-test-001".to_string(),
    };

    let task_id = scheduler.schedule(flow.clone()).await?;

    let worker =
        Worker::new(storage.clone(), "retry-checker").with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<RetryCheckFlow>| f.process()).await;

    let worker_handle = worker.start().await;

    loop {
        match storage.get_scheduled_flow(task_id).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                    break;
                }
            }
            None => break,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    worker_handle.shutdown().await;

    storage.close().await?;

    Ok(())
}
