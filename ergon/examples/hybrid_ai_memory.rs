//! Hybrid AI/Human Content Moderation Pipeline (In-Memory Edition)
//!
//! Scenario:
//! 1. Analyze text toxicity (Simulated AI).
//! 2. If Score < 20: Auto-Approve.
//! 3. If Score > 80: Auto-Reject.
//! 4. If Score 20-80: Suspend flow and wait for Human Signal.
//! 5. Finally: Spawn a child flow to archive the decision.
//!
//! Run with:
//! ```
//! cargo run --release --example hybrid_ai_memory
//! ```

use async_trait::async_trait;
use ergon::core::InvokableFlow;
use ergon::executor::{await_external_signal, InvokeChild, SignalSource, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};

// Completion tracking for the demo
static COMPLETED_COUNT: AtomicU32 = AtomicU32::new(0);
static DONE_NOTIFIER: std::sync::LazyLock<Arc<Notify>> =
    std::sync::LazyLock::new(|| Arc::new(Notify::new()));

// =============================================================================
// INFRASTRUCTURE: The Moderator Dashboard (Signal Source)
// =============================================================================

#[derive(Clone)]
struct ModeratorDashboard {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl ModeratorDashboard {
    fn new() -> Self {
        Self {
            signals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // Called by the "Frontend" (Main fn) when a human clicks Approve/Reject
    async fn submit_review(&self, content_id: &str, approved: bool) {
        println!(
            "   [DASHBOARD] Human Moderator reviewed {}: {}",
            content_id,
            if approved { "APPROVED" } else { "REJECTED" }
        );
        let decision = HumanDecision { approved };
        let data = ergon::core::serialize_value(&decision).unwrap();

        let mut map = self.signals.write().await;
        // Signal name convention: "review:<content_id>"
        map.insert(format!("review:{}", content_id), data);
    }
}

#[async_trait]
impl SignalSource for ModeratorDashboard {
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
// DATA TYPES
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ModerationStatus {
    AutoApproved,
    AutoRejected,
    HumanApproved,
    HumanRejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HumanDecision {
    approved: bool,
}

// =============================================================================
// CHILD FLOW: Audit Archiver
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct AuditLogFlow {
    content_id: String,
    status: ModerationStatus,
    score: u32,
}

impl InvokableFlow for AuditLogFlow {
    type Output = ();
}

impl AuditLogFlow {
    #[flow]
    async fn run_audit(self: Arc<Self>) -> Result<(), String> {
        // Simulate DB write delay
        tokio::time::sleep(Duration::from_millis(50)).await;
        println!(
            "   [AUDIT] Archived decision for {}: {:?} (Score: {})",
            self.content_id, self.status, self.score
        );
        Ok(())
    }
}

// =============================================================================
// PARENT FLOW: Content Pipeline
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct ContentFlow {
    content_id: String,
    text: String,
}

impl ContentFlow {
    // 1. AI Step: Deterministically calculate score based on text hash
    #[step]
    async fn ai_analyze(self: Arc<Self>) -> Result<u32, String> {
        println!("   [AI] Analyzing content: \"{}\"", self.text);
        tokio::time::sleep(Duration::from_millis(100)).await; // Simulate GPU work

        // Simple deterministic hash for "Toxicity Score"
        let sum: u32 = self.text.bytes().map(|b| b as u32).sum();
        let score = sum % 100;

        println!("   [AI] Score calculated: {}/100", score);
        Ok(score)
    }

    // 2. Human Step: Wait for signal (Only called if ambiguous)
    #[step]
    async fn wait_for_human(self: Arc<Self>) -> Result<HumanDecision, String> {
        println!("   [FLOW] Score ambiguous. Suspending for Human Review...");

        let signal_name = format!("review:{}", self.content_id);

        // This blocks the flow (durably) until dashboard sends signal
        let decision: HumanDecision = await_external_signal(&signal_name)
            .await
            .map_err(|e| format!("Signal error: {}", e))?;

        Ok(decision)
    }

    // MAIN ORCHESTRATOR
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, String> {
        println!("Processing upload: {}", self.content_id);

        let score = self.clone().ai_analyze().await?;
        let final_status: ModerationStatus;

        // Dynamic Branching Logic
        if score < 20 {
            println!("   [FLOW] Safe content. Auto-Approving.");
            final_status = ModerationStatus::AutoApproved;
        } else if score > 80 {
            println!("   [FLOW] Toxic content. Auto-Rejecting.");
            final_status = ModerationStatus::AutoRejected;
        } else {
            // Ambiguous: We need a human
            // Note: If we crash here, we resume waiting for the human.
            let decision = self.clone().wait_for_human().await?;
            if decision.approved {
                final_status = ModerationStatus::HumanApproved;
            } else {
                final_status = ModerationStatus::HumanRejected;
            }
        }

        // Spawn Child Flow for Auditing (Fire and Await)
        self.invoke(AuditLogFlow {
            content_id: self.content_id.clone(),
            status: final_status.clone(),
            score,
        })
        .result()
        .await
        .map_err(|e| e.to_string())?;

        // Global counter for demo exit
        let count = COMPLETED_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= 3 {
            DONE_NOTIFIER.notify_one();
        }

        Ok(format!("Finished with status: {:?}", final_status))
    }
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Setup In-Memory Storage
    println!("Using in-memory storage");

    let storage = Arc::new(ergon::storage::InMemoryExecutionLog::new());

    let scheduler = ergon::executor::Scheduler::new(storage.clone());
    let dashboard = Arc::new(ModeratorDashboard::new());

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ Trust & Safety Pipeline (In-Memory Backed)                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // 2. Schedule the 3 Flows
    // Safe
    let flow_approve = ContentFlow {
        content_id: "POST-SAFE".to_string(),
        text: "\x05".to_string(),
    };
    scheduler
        .schedule(flow_approve, uuid::Uuid::new_v4())
        .await?;

    // Toxic
    let flow_reject = ContentFlow {
        content_id: "POST-TOXIC".to_string(),
        text: "a".to_string(),
    };
    scheduler
        .schedule(flow_reject, uuid::Uuid::new_v4())
        .await?;

    // Ambiguous (Requires Human)
    let flow_review = ContentFlow {
        content_id: "POST-AMBIGUOUS".to_string(),
        text: "Rust".to_string(),
    };
    scheduler
        .schedule(flow_review, uuid::Uuid::new_v4())
        .await?;

    // 3. Start the Worker
    let worker = Worker::new(storage.clone(), "content-worker")
        .with_poll_interval(Duration::from_millis(200))
        .with_signals(dashboard.clone());

    worker.register(|f: Arc<ContentFlow>| f.process()).await;
    worker.register(|f: Arc<AuditLogFlow>| f.run_audit()).await;

    let handle = worker.start().await;

    // 4. Simulate Human Interaction (The "Frontend")
    tokio::spawn(async move {
        // Wait for the AI to finish analysis and suspend
        tokio::time::sleep(Duration::from_secs(2)).await;

        println!("\n   [SYSTEM] Notification: POST-AMBIGUOUS requires review.");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Human clicks "Approve"
        dashboard.submit_review("POST-AMBIGUOUS", true).await;
    });

    // 5. Wait until the Database says "No more work"
    println!("\n   [MAIN] Monitoring database for completion...");

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let pending_work = storage.get_incomplete_flows().await?;

        if pending_work.is_empty() {
            println!("   [MAIN] All flows completed successfully.");
            break;
        } else {
            println!("   [MAIN] Still running: {} flows", pending_work.len());
        }
    }

    handle.shutdown().await;
    println!("\nModeration pipeline complete.");
    Ok(())
}
