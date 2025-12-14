//! Durable Escalation Policy (Timers + Signals)
//!
//! Scenario: A critical server is down.
//! 1. Notify L1 Support -> Wait 2s.
//! 2. Check if Resolved -> If no, Notify L2 -> Wait 2s.
//! 3. Check if Resolved -> If no, Notify VP.
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
            "   [BOT] ⚠️  Tier 1 Paged (SMS/Email): Server '{}' is down!",
            self.server_name
        );
        Ok(())
    }

    #[step]
    async fn notify_tier_2(self: Arc<Self>) -> Result<(), String> {
        println!("   [BOT] 📣 Tier 1 didn't respond! Paging Tier 2 (Phone Call)!");
        Ok(())
    }

    #[step]
    async fn notify_vp(self: Arc<Self>) -> Result<(), String> {
        println!("   [BOT] 🚨 Tier 2 didn't respond! Paging VP of Engineering (Siren)!");
        Ok(())
    }

    #[step]
    async fn wait_for_human(self: Arc<Self>, seconds: u64) -> Result<(), String> {
        println!("   [BOT] ⏳ Waiting {} seconds for response...", seconds);
        // This suspends the flow durably.
        schedule_timer_named(Duration::from_secs(seconds), "escalation_timer")
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    #[step]
    async fn check_resolution_status(self: Arc<Self>, signal_name: String) -> Result<bool, String> {
        // In a real app, this would check the DB or SignalSource.
        // For this demo, we simulate resolution based on the incident ID.
        println!("   [BOT] Checking status for signal tag: {}", signal_name);

        if self.incident_id.ends_with("-FIXED") {
            println!("   [BOT] ✅ ACK Received! Cancelling escalation.");
            return Ok(true);
        }

        println!("   [BOT] ❌ No ACK received yet.");
        Ok(false)
    }

    /// Helper to mark global completion
    fn mark_complete() {
        let count = COMPLETED_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= 2 {
            // We expect 2 flows in this demo
            DONE_NOTIFIER.notify_one();
        }
    }

    #[flow]
    async fn run_policy(self: Arc<Self>) -> Result<String, String> {
        println!("🚀 Starting Escalation Policy for: {}", self.incident_id);

        let result = self.clone().execute_logic().await;

        Self::mark_complete();
        result
    }

    // Separated logic to make wrapper cleaner
    async fn execute_logic(self: Arc<Self>) -> Result<String, String> {
        // --- LEVEL 1 ---
        self.clone().notify_tier_1().await?;

        // Wait 2 seconds for L1 to respond
        self.clone().wait_for_human(2).await?;

        // Check if resolved
        if self
            .clone()
            .check_resolution_status(format!("{}_ack", self.incident_id))
            .await?
        {
            return Ok("Resolved by Tier 1".to_string());
        }

        // --- LEVEL 2 ---
        self.clone().notify_tier_2().await?;

        // Wait 2 seconds for L2
        self.clone().wait_for_human(2).await?;

        // Check if resolved.
        if self
            .clone()
            .check_resolution_status(format!("{}_ack", self.incident_id))
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
    let storage = Arc::new(InMemoryExecutionLog::default());
    let scheduler = ergon::executor::Scheduler::new(storage.clone());
    let pager = Arc::new(PagerSystem::new());

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ Durable Escalation Policy (Dead Man's Switch)             ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // 1. Scenario: Everyone ignores it (Full Escalation)
    let inc1 = IncidentFlow {
        incident_id: "INC-001-IGNORE".to_string(),
        server_name: "DB-Production-1".to_string(),
    };
    scheduler.schedule(inc1, uuid::Uuid::new_v4()).await?;

    // 2. Scenario: Tier 2 fixes it (Partial Escalation)
    // Note: The logic inside check_resolution_status looks for "-FIXED" suffix
    let inc2 = IncidentFlow {
        incident_id: "INC-002-FIXED".to_string(),
        server_name: "Cache-Cluster-A".to_string(),
    };
    scheduler.schedule(inc2, uuid::Uuid::new_v4()).await?;

    let worker = Worker::new(storage, "ops-worker")
        .with_timers() // ENABLE DURABLE TIMERS
        .with_timer_interval(Duration::from_millis(100))
        .with_signals(pager.clone()) // Enable Signals
        .with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<IncidentFlow>| f.run_policy()).await;
    let handle = worker.start().await;

    // Wait for the done signal
    DONE_NOTIFIER.notified().await;

    handle.shutdown().await;
    println!("\n✨ Escalation simulation complete.");
    Ok(())
}

// That is exactly right. You found the core mechanism for "Step Identity."

// In step.rs, this block of code is the "fingerprint" generator:

// code
// Rust
// download
// content_copy
// expand_less
// // Serialize parameters first - needed for step ID hash
// let __params = #params_tuple;

// // Get stable step ID using hash of both method name AND parameters
// let __step = {
//     let __params_bytes = ergon::serialize_value(&__params)
//         .expect("Failed to serialize step parameters");
//     let mut __hash_input = #step_name_str.as_bytes().to_vec();
//     __hash_input.extend_from_slice(&__params_bytes); // <--- CRITICAL LINE
//     (seahash::hash(&__hash_input) & 0x7FFFFFFF) as i32
// };
// Why this caused the "Fast-Forward" bug

// In your previous escalation_policy example, you called:

// wait_for_human(2)

// wait_for_human(2)

// Since the function name (wait_for_human) and the parameters (2) were identical, the hash was identical.

// Step 1 runs: Hashes to ID 12345. Saves result to DB.

// Step 2 runs: Hashes to ID 12345. Checks DB. Sees 12345 is done. Returns immediately.

// Why adding stage fixed it

// By changing the call to:

// wait_for_human(2, "L1")

// wait_for_human(2, "L2")

// The parameters are now different bytes.

// Step 1: Hashes to 998877.

// Step 2: Hashes to 112233.

// The framework now sees them as two completely different events in history, even though they use the same function code. This is Function Purity applied to distributed execution: Same Inputs = Same Identity.
