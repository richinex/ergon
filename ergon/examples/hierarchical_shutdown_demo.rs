//! Hierarchical Cancellation with Worker CancellationTokens
//!
//! This example demonstrates:
//! - Accessing worker CancellationTokens via `handle.cancellation_token()`
//! - Creating child tokens for custom background tasks
//! - Hierarchical cancellation: shutting down a worker automatically cancels child tasks
//! - Coordinating worker lifecycle with custom application logic
//!
//! ## Scenario
//! We start two workers (processing and notification workers). Each worker has
//! custom monitoring tasks that run alongside it. When we shut down a worker,
//! its monitoring task automatically stops via hierarchical token cancellation.
//!
//! ## Key Takeaways
//! - Workers create their own CancellationToken internally
//! - Access the token via `handle.cancellation_token()` for advanced patterns
//! - Create child tokens to coordinate custom tasks with worker lifecycle
//! - Shutting down a worker automatically cancels all its child tokens
//! - Use this pattern to run application-specific tasks tied to worker lifetime
//!
//! ## Run with
//! ```bash
//! cargo run --example hierarchical_shutdown_demo
//! ```

use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Global execution counters
static DATA_TRANSFORM_COUNT: AtomicU32 = AtomicU32::new(0);
static NOTIFICATION_SEND_COUNT: AtomicU32 = AtomicU32::new(0);

// Custom task metrics
static PROCESSING_MONITOR_TICKS: AtomicU32 = AtomicU32::new(0);
static NOTIFICATION_MONITOR_TICKS: AtomicU32 = AtomicU32::new(0);

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct DataProcessor {
    data_id: u32,
    processing_time_ms: u64,
}

impl DataProcessor {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, String> {
        println!("[DataProcessor] Processing data {}", self.data_id);
        self.transform().await
    }

    #[step]
    async fn transform(self: Arc<Self>) -> Result<String, String> {
        DATA_TRANSFORM_COUNT.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(self.processing_time_ms)).await;
        Ok(format!("Processed data {}", self.data_id))
    }
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct NotificationJob {
    job_id: u32,
}

impl NotificationJob {
    #[flow]
    async fn execute(self: Arc<Self>) -> Result<String, String> {
        println!("[NotificationJob] Executing job {}", self.job_id);
        self.send_notification().await
    }

    #[step]
    async fn send_notification(self: Arc<Self>) -> Result<String, String> {
        NOTIFICATION_SEND_COUNT.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(format!("Notification {} sent", self.job_id))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║       Hierarchical Cancellation with Worker Tokens        ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let storage = Arc::new(SqliteExecutionLog::new("hierarchical_demo.db").await?);
    storage.reset().await?;

    // ============================================================
    // PART 1: Scheduling Flows
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 1: Scheduling Flows                                  ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let scheduler = Scheduler::new(storage.clone());

    let mut task_ids = Vec::new();

    // Schedule data processing flows
    for i in 1..=5 {
        let flow = DataProcessor {
            data_id: i,
            processing_time_ms: 200,
        };
        let task_id = scheduler.schedule(flow, Uuid::new_v4()).await?;
        println!("   ✓ DataProcessor {} scheduled", i);
        task_ids.push(task_id);
    }

    // Schedule notification flows
    for i in 1..=3 {
        let flow = NotificationJob { job_id: i };
        let task_id = scheduler.schedule(flow, Uuid::new_v4()).await?;
        println!("   ✓ NotificationJob {} scheduled", i);
        task_ids.push(task_id);
    }

    println!("\n   → {} total flows scheduled\n", task_ids.len());

    // ============================================================
    // PART 2: Starting Workers with Hierarchical Tasks
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 2: Starting Workers with Hierarchical Tasks          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Start worker 1 - processes both flow types
    let worker1 =
        Worker::new(storage.clone(), "worker-1").with_poll_interval(Duration::from_millis(100));
    worker1
        .register(|flow: Arc<DataProcessor>| flow.process())
        .await;
    worker1
        .register(|flow: Arc<NotificationJob>| flow.execute())
        .await;
    let worker1_handle = worker1.start().await;
    println!("   ✓ Started worker-1");

    // ACCESS the worker's CancellationToken and create a child token
    let worker1_token = worker1_handle.cancellation_token();
    let worker1_child_token = worker1_token.child_token();

    // Spawn a custom monitoring task tied to this worker's lifetime
    let worker1_monitor = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = worker1_child_token.cancelled() => {
                    println!("   [Monitor] Worker-1 cancelled, stopping monitor task");
                    break;
                }
                _ = interval.tick() => {
                    let count = PROCESSING_MONITOR_TICKS.fetch_add(1, Ordering::Relaxed);
                    println!("   [Monitor] Worker-1 active (tick {})", count + 1);
                }
            }
        }
    });

    // Start worker 2 - processes both flow types
    let worker2 =
        Worker::new(storage.clone(), "worker-2").with_poll_interval(Duration::from_millis(100));
    worker2
        .register(|flow: Arc<DataProcessor>| flow.process())
        .await;
    worker2
        .register(|flow: Arc<NotificationJob>| flow.execute())
        .await;
    let worker2_handle = worker2.start().await;
    println!("   ✓ Started worker-2");

    // ACCESS the worker's CancellationToken and create a child token
    let worker2_token = worker2_handle.cancellation_token();
    let worker2_child_token = worker2_token.child_token();

    // Spawn a custom monitoring task tied to this worker's lifetime
    let worker2_monitor = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = worker2_child_token.cancelled() => {
                    println!("   [Monitor] Worker-2 cancelled, stopping monitor task");
                    break;
                }
                _ = interval.tick() => {
                    let count = NOTIFICATION_MONITOR_TICKS.fetch_add(1, Ordering::Relaxed);
                    println!("   [Monitor] Worker-2 active (tick {})", count + 1);
                }
            }
        }
    });

    println!("\n   → Workers started with hierarchical monitoring tasks\n");

    // ============================================================
    // PART 3: Workers and Monitors Running
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 3: Workers and Monitors Running                      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");
    println!("   Workers processing flows...");
    println!("   Monitor tasks tracking worker health...");
    println!("   Waiting for all flows to complete...\n");

    // Wait for all flows to actually complete (not just arbitrary sleep)
    let timeout_duration = Duration::from_secs(30);
    let wait_result = tokio::time::timeout(timeout_duration, async {
        loop {
            let mut all_complete = true;
            for &task_id in &task_ids {
                // Check if flow is complete
                if let Some(scheduled) = storage.get_scheduled_flow(task_id).await? {
                    if !matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                        all_complete = false;
                        break;
                    }
                }
            }
            if all_complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await;

    match wait_result {
        Ok(_) => println!("\n   ✓ All flows completed successfully\n"),
        Err(_) => {
            println!("\n   ⚠ Timeout waiting for flows to complete");
            let incomplete = storage.get_incomplete_flows().await?;
            println!("   Incomplete flows: {}", incomplete.len());
            for inv in &incomplete {
                println!("     - {} ({})", inv.id(), inv.class_name());
            }
            println!();
        }
    }

    // ============================================================
    // PART 4: Hierarchical Shutdown
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 4: Hierarchical Shutdown                             ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");
    println!("   → Shutting down worker-1...");

    // Shutdown worker 1
    // This cancels its internal CancellationToken, which cascades to worker1_child_token
    worker1_handle.shutdown().await;

    // Wait for the monitor task to stop (it should stop automatically)
    worker1_monitor.await?;
    println!("   ✓ Worker-1 and monitor stopped\n");

    println!("   → Shutting down worker-2...");

    // Shutdown worker 2
    // This cancels its internal CancellationToken, which cascades to worker2_child_token
    worker2_handle.shutdown().await;

    // Wait for the monitor task to stop (it should stop automatically)
    worker2_monitor.await?;
    println!("   ✓ Worker-2 and monitor stopped\n");

    println!("=== Summary ===\n");

    println!("Worker Flow Execution:");
    println!(
        "  data_transform:     {}",
        DATA_TRANSFORM_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  send_notification:  {}",
        NOTIFICATION_SEND_COUNT.load(Ordering::Relaxed)
    );

    println!("\nCustom Monitor Tasks (via child tokens):");
    println!(
        "  worker-1 monitor:  {} ticks",
        PROCESSING_MONITOR_TICKS.load(Ordering::Relaxed)
    );
    println!(
        "  worker-2 monitor:  {} ticks",
        NOTIFICATION_MONITOR_TICKS.load(Ordering::Relaxed)
    );

    let incomplete = storage.get_incomplete_flows().await?;
    let completed_count = 8 - incomplete.len();

    println!("\nFlow Completion:");
    println!("  Completed:  {}/8 flows", completed_count);
    if !incomplete.is_empty() {
        println!(
            "  Incomplete: {} flows (interrupted by shutdown)",
            incomplete.len()
        );
    }

    println!("\nHierarchical Cancellation Demonstrated:");
    println!("  ✓ Worker shutdown automatically cancelled child monitor tasks");
    println!("  ✓ No manual cancellation needed for custom tasks");
    println!("  ✓ Pattern: handle.cancellation_token().child_token()\n");

    storage.close().await?;
    Ok(())
}
