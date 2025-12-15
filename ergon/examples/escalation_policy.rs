//! Durable Escalation Policy (Dead Man's Switch)
//!
//! Scenario: A critical server is down.
//! 1. Notify L1 Support -> Wait 2s.
//! 2. Check if Resolved -> If no, Notify L2 -> Wait 2s.
//! 3. Check if Resolved -> If no, Notify VP.
//!
//! Key Concepts:
//! - **Durable Timers**: Sleep for days without RAM/CPU usage.
//! - **Step Identity**: Using 'stage' args to ensure identical steps run multiple times.
//!
//! Run with:
//! cargo run --example escalation_policy

use async_trait::async_trait;
use ergon::executor::{schedule_timer_named, SignalSource, Worker};
use ergon::prelude::*;
use ergon::storage::InMemoryExecutionLog;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};

// Track completions to exit cleanly
static COMPLETED_COUNT: AtomicU32 = AtomicU32::new(0);
static DONE_NOTIFIER: std::sync::LazyLock<Arc<Notify>> =
    std::sync::LazyLock::new(|| Arc::new(Notify::new()));

// =============================================================================
// SIGNAL INFRASTRUCTURE (The Pager System)
// =============================================================================

#[derive(Clone)]
struct PagerSystem {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl PagerSystem {
    fn new() -> Self {
        Self {
            signals: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl SignalSource for PagerSystem {
    async fn poll_for_signal(&self, signal_name: &str) -> Option<Vec<u8>> {
        let map = self.signals.read().await;
        map.get(signal_name).cloned()
    }

    async fn consume_signal(&self, signal_name: &str) {
        let mut map = self.signals.write().await;
        map.remove(signal_name);
    }
}

// =============================================================================
// DOMAIN LOGIC
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct IncidentFlow {
    incident_id: String,
    server_name: String,
}

impl IncidentFlow {
    #[step]
    async fn notify_tier_1(self: Arc<Self>) -> Result<(), String> {
        println!(
            "   [BOT] Tier 1 Paged (SMS/Email): Server '{}' is down!",
            self.server_name
        );
        Ok(())
    }

    #[step]
    async fn notify_tier_2(self: Arc<Self>) -> Result<(), String> {
        println!("   [BOT] Tier 1 didn't respond! Paging Tier 2 (Phone Call)!");
        Ok(())
    }

    #[step]
    async fn notify_vp(self: Arc<Self>) -> Result<(), String> {
        println!("   [BOT] Tier 2 didn't respond! Paging VP of Engineering (Siren)!");
        Ok(())
    }

    /// Reusable Step: Wait for human.
    /// FIX: Added `stage` argument. This makes the step ID unique per stage
    /// (e.g., "wait_L1" vs "wait_L2"), preventing the framework from skipping the second wait.
    #[step]
    async fn wait_for_human(self: Arc<Self>, seconds: u64, stage: String) -> Result<(), String> {
        println!(
            "   [BOT] [{}] Waiting {} seconds for response...",
            stage, seconds
        );

        // We also use stage in the timer name so logs are clear
        let timer_name = format!("timer_{}_{}", self.incident_id, stage);

        schedule_timer_named(Duration::from_secs(seconds), &timer_name)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Reusable Step: Check status.
    /// FIX: Added `stage` argument so the result of the first check isn't cached/reused for the second.
    #[step]
    async fn check_resolution_status(
        self: Arc<Self>,
        signal_name: String,
        stage: String,
    ) -> Result<bool, String> {
        // In a real app, this would check the DB or SignalSource.
        // For this demo, we simulate resolution based on the incident ID suffix.

        if self.incident_id.ends_with("-FIXED") {
            println!(
                "   [BOT] [{}] ACK Received via '{}'! Cancelling escalation.",
                stage, signal_name
            );
            return Ok(true);
        }

        println!("   [BOT] [{}] No ACK received yet.", stage);
        Ok(false)
    }

    /// Helper to mark global completion
    fn mark_complete() {
        let count = COMPLETED_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= 2 {
            DONE_NOTIFIER.notify_one();
        }
    }

    #[flow]
    async fn run_policy(self: Arc<Self>) -> Result<String, String> {
        let result = self.clone().execute_logic().await;

        Self::mark_complete();
        result
    }

    async fn execute_logic(self: Arc<Self>) -> Result<String, String> {
        let signal_tag = format!("{}_ack", self.incident_id);

        // --- LEVEL 1 ---
        self.clone().notify_tier_1().await?;

        // Wait 2 seconds for L1
        self.clone().wait_for_human(2, "L1".to_string()).await?;

        // Check if resolved
        if self
            .clone()
            .check_resolution_status(signal_tag.clone(), "L1_Check".to_string())
            .await?
        {
            return Ok("Resolved by Tier 1".to_string());
        }

        // --- LEVEL 2 ---
        self.clone().notify_tier_2().await?;

        // Wait 2 seconds for L2 (Now a distinct step from L1 wait)
        self.clone().wait_for_human(2, "L2".to_string()).await?;

        // Check if resolved (Distinct step from L1 check)
        if self
            .clone()
            .check_resolution_status(signal_tag, "L2_Check".to_string())
            .await?
        {
            return Ok("Resolved by Tier 2".to_string());
        }

        // --- LEVEL 3 ---
        self.clone().notify_vp().await?;

        Ok("Escalated to VP".to_string())
    }
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use InMemory for speed, but works identically with SQLite
    let storage = Arc::new(InMemoryExecutionLog::default());
    let scheduler = ergon::executor::Scheduler::new(storage.clone());
    let pager = Arc::new(PagerSystem::new());

    // 1. Scenario: Everyone ignores it (Full Escalation)
    let inc1 = IncidentFlow {
        incident_id: "INC-001-IGNORE".to_string(),
        server_name: "DB-Production-1".to_string(),
    };
    scheduler.schedule(inc1, uuid::Uuid::new_v4()).await?;

    // 2. Scenario: Tier 2 fixes it (Partial Escalation)
    // The -FIXED suffix simulates a successful resolution in our mock logic
    let inc2 = IncidentFlow {
        incident_id: "INC-002-FIXED".to_string(),
        server_name: "Cache-Cluster-A".to_string(),
    };
    scheduler.schedule(inc2, uuid::Uuid::new_v4()).await?;

    let worker = Worker::new(storage, "ops-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(100))
        .with_signals(pager.clone())
        .with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<IncidentFlow>| f.run_policy()).await;
    let handle = worker.start().await;

    // Wait for the done signal (Should take approx 4-5 seconds due to timers)
    DONE_NOTIFIER.notified().await;

    handle.shutdown().await;
    Ok(())
}
