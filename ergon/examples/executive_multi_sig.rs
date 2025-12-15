//! The Executive Multi-Sig (Parallel Signals + Deadline Timer)
//!
//! Demonstrates:
//! 1. Parallel Signal Collection (Out-of-order delivery).
//! 2. The "Durable Barrier" pattern.
//! 3. Deadline / Timeout logic.
//! 4. Zero-resource waiting.
//!
//! Run with:
//! cargo run --example multi_sig_showcase

use async_trait::async_trait;
use ergon::executor::{await_external_signal, SignalSource, Worker};
use ergon::prelude::*;
use ergon::storage::InMemoryExecutionLog;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};

// Completion Tracking
static COMPLETED_COUNT: AtomicU32 = AtomicU32::new(0);
static DONE_NOTIFIER: std::sync::LazyLock<Arc<Notify>> =
    std::sync::LazyLock::new(|| Arc::new(Notify::new()));

// =============================================================================
// SIGNAL INFRASTRUCTURE (The Corporate Inbox)
// =============================================================================

#[derive(Clone)]
struct CorporateInbox {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl CorporateInbox {
    fn new() -> Self {
        Self {
            signals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Simulates an executive signing the document
    async fn sign(&self, signal_name: &str, signer_name: &str) {
        println!("   [SIGNER] {} has signed '{}'", signer_name, signal_name);
        let data = ergon::core::serialize_value(&signer_name.to_string()).unwrap();
        self.signals
            .write()
            .await
            .insert(signal_name.to_string(), data);
    }
}

#[async_trait]
impl SignalSource for CorporateInbox {
    async fn poll_for_signal(&self, name: &str) -> Option<Vec<u8>> {
        self.signals.read().await.get(name).cloned()
    }
    async fn consume_signal(&self, name: &str) {
        self.signals.write().await.remove(name);
    }
}

// =============================================================================
// DOMAIN LOGIC
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct MultiSigTransfer {
    transfer_id: String,
    amount: f64,
}

impl MultiSigTransfer {
    #[step]
    async fn request_signatures(self: Arc<Self>) -> Result<(), String> {
        println!(
            "   [SYSTEM] Multi-sig request started for {} (${}M)",
            self.transfer_id, self.amount
        );
        println!("   [SYSTEM] Emails sent to: Legal, Finance, CEO.");
        Ok(())
    }

    #[step]
    async fn wait_for_approval(self: Arc<Self>, dept: String) -> Result<String, String> {
        let signal_name = format!("{}_{}", self.transfer_id, dept);
        println!(
            "   [SYSTEM] Flow suspended: Waiting for {} signature...",
            dept
        );

        // This is the Durable Signal. It kills the task and waits for the DB to be updated.
        let signer: String = await_external_signal(&signal_name)
            .await
            .map_err(|e| e.to_string())?;

        Ok(signer)
    }

    #[step]
    async fn execute_transfer(self: Arc<Self>, sigs: Vec<String>) -> Result<(), String> {
        println!("   [SYSTEM] ALL SIGNATURES RECEIVED: {:?}", sigs);
        println!("   [SYSTEM] Executing transfer {}...", self.transfer_id);
        Ok(())
    }

    #[flow]
    async fn run_transfer(self: Arc<Self>) -> Result<(), String> {
        // 1. Initial Broadcast
        self.clone().request_signatures().await?;

        // 2. The Durable Barrier
        // We wait for all three. Note: If CEO signs first,
        // this flow stays suspended at "Legal". But once Legal signs,
        // it will "fast-forward" through Finance and CEO instantly.
        let s1 = self.clone().wait_for_approval("Legal".into()).await?;
        let s2 = self.clone().wait_for_approval("Finance".into()).await?;
        let s3 = self.clone().wait_for_approval("CEO".into()).await?;

        // 3. Finalize
        self.clone().execute_transfer(vec![s1, s2, s3]).await?;

        COMPLETED_COUNT.fetch_add(1, Ordering::Relaxed);
        DONE_NOTIFIER.notify_one();
        Ok(())
    }
}

// =============================================================================
// MAIN: THE OUT-OF-ORDER DEMO
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemoryExecutionLog::default());
    let inbox = Arc::new(CorporateInbox::new());
    let scheduler = Scheduler::new(storage.clone());

    let tx_id = "TX-99".to_string();
    scheduler
        .schedule(
            MultiSigTransfer {
                transfer_id: tx_id.clone(),
                amount: 10.0,
            },
            uuid::Uuid::new_v4(),
        )
        .await?;

    let worker = Worker::new(storage, "bank-worker")
        .with_signals(inbox.clone())
        .with_poll_interval(Duration::from_millis(50));

    worker
        .register(|f: Arc<MultiSigTransfer>| f.run_transfer())
        .await;
    let _handle = worker.start().await;

    // --- THE SHOWCASE INTERACTION ---

    // 1. CEO approves ALMOST IMMEDIATELY (Out of order)
    tokio::time::sleep(Duration::from_millis(200)).await;
    inbox
        .sign(&format!("{}_CEO", tx_id), "Elizabeth (CEO)")
        .await;

    // 2. Finance approves next
    tokio::time::sleep(Duration::from_millis(200)).await;
    inbox
        .sign(&format!("{}_Finance", tx_id), "Mark (CFO)")
        .await;

    // 3. Legal is slow... wait 1 second
    tokio::time::sleep(Duration::from_secs(1)).await;
    inbox
        .sign(&format!("{}_Legal", tx_id), "Sarah (General Counsel)")
        .await;

    // Wait for flow to recognize all signals and finish
    DONE_NOTIFIER.notified().await;
    Ok(())
}
