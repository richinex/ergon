//! External Signal with Stdin Example
//!
//! This example demonstrates using stdin for external signals.
//! This is the SAME code as external_signal_abstraction.rs, but with
//! StdinSignalSource instead of SimulatedUserInputSource.
//!
//! ## What Changed
//! ONLY ONE LINE changed from external_signal_abstraction.rs:
//! ```rust
//! // Before (simulated):
//! // let signal_source = Arc::new(SimulatedUserInputSource::new());
//!
//! // After (stdin):
//! let signal_source = Arc::new(StdinSignalSource::new());
//! ```
//!
//! ## Worker Integration
//! The Worker automatically handles signal processing:
//! ```rust
//! let worker = Worker::new(storage, "worker-1")
//!     .with_signals(signal_source)  // Automatic signal delivery!
//!     .start()
//!     .await;
//! ```
//!
//! ## Usage
//! Run the example, then send signals via stdin:
//! ```bash
//! cargo run --example external_signal_stdin --features=sqlite
//!
//! # When prompted, enter signals in format:
//! # signal_name approved approver_email comments
//! manager_approval_DOC-001 true manager@company.com Looks good
//! legal_review_DOC-001 true legal@company.com Approved
//! ```

use async_trait::async_trait;
use ergon::executor::{await_external_signal, SignalSource};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::RwLock;
use uuid::Uuid;

// ============================================================================
// Domain Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApprovalDecision {
    approved: bool,
    approver: String,
    comments: String,
    timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DocumentSubmission {
    document_id: String,
    title: String,
    author: String,
    content: String,
}

// ============================================================================
// Signal Source Abstraction
// ============================================================================
// We use ergon::executor::SignalSource trait for signal source abstraction

// ============================================================================
// Stdin Signal Source - Reads user input from terminal
// ============================================================================

/// Reads approval decisions from stdin.
/// Input format: signal_name approved approver comments
/// Example: manager_approval_DOC-001 true manager@company.com Looks good
struct StdinSignalSource {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl StdinSignalSource {
    fn new() -> Self {
        let signals = Arc::new(RwLock::new(HashMap::new()));

        // Spawn background task to read from stdin
        let signals_clone = signals.clone();
        tokio::spawn(async move {
            let stdin = tokio::io::stdin();
            let mut reader = BufReader::new(stdin);
            let mut line = String::new();

            println!("\n[STDIN] Waiting for approval signals...");
            println!("[STDIN] Format: signal_name approved approver comments");
            println!(
                "[STDIN] Example: manager_approval_DOC-001 true manager@company.com Looks good\n"
            );

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 4 {
                            let signal_name = parts[0];
                            let approved = parts[1].parse::<bool>().unwrap_or(false);
                            let approver = parts[2].to_string();
                            let comments = parts[3..].join(" ");

                            let decision = ApprovalDecision {
                                approved,
                                approver,
                                comments,
                                timestamp: chrono::Utc::now().timestamp(),
                            };

                            if let Ok(data) = ergon::core::serialize_value(&decision) {
                                let mut signals = signals_clone.write().await;
                                signals.insert(signal_name.to_string(), data);
                                println!("[STDIN] Signal '{}' received", signal_name);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[ERROR] Failed to read stdin: {}", e);
                        break;
                    }
                }
            }
        });

        Self { signals }
    }
}

#[async_trait]
impl SignalSource for StdinSignalSource {
    async fn poll_for_signal(&self, signal_name: &str) -> Option<Vec<u8>> {
        let signals = self.signals.read().await;
        signals.get(signal_name).cloned()
    }

    async fn consume_signal(&self, signal_name: &str) {
        let mut signals = self.signals.write().await;
        signals.remove(signal_name);
    }
}

// ============================================================================
// Document Approval Workflow (IDENTICAL to external_signal_abstraction.rs)
// ============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct DocumentApprovalFlow {
    submission: DocumentSubmission,
}

impl DocumentApprovalFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, String> {
        println!("\n[FLOW] Processing document: {}", self.submission.title);
        println!("       Author: {}", self.submission.author);

        self.clone().validate_document().await?;
        self.clone().await_manager_approval().await?;
        self.clone().await_legal_review().await?;
        self.clone().publish_document().await?;

        Ok(format!(
            "Document '{}' approved and published",
            self.submission.title
        ))
    }

    #[step]
    async fn validate_document(self: Arc<Self>) -> Result<(), String> {
        println!("       [STEP] Validating document format...");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("       [OK] Validation passed");
        Ok(())
    }

    #[step]
    async fn await_manager_approval(self: Arc<Self>) -> Result<(), String> {
        println!("       [STEP] Awaiting manager approval...");
        println!(
            "       [HINT] Enter: manager_approval_{} true manager@company.com Looks good",
            self.submission.document_id
        );

        let decision: ApprovalDecision =
            await_external_signal(&format!("manager_approval_{}", self.submission.document_id))
                .await
                .map_err(|e| format!("Failed to wait for approval: {}", e))?;

        if !decision.approved {
            return Err(format!("Manager rejected: {}", decision.comments));
        }

        println!(
            "       [OK] Manager approved by {} - {}",
            decision.approver, decision.comments
        );
        Ok(())
    }

    #[step]
    async fn await_legal_review(self: Arc<Self>) -> Result<(), String> {
        println!("       [STEP] Awaiting legal review...");
        println!(
            "       [HINT] Enter: legal_review_{} true legal@company.com Approved",
            self.submission.document_id
        );

        let decision: ApprovalDecision =
            await_external_signal(&format!("legal_review_{}", self.submission.document_id))
                .await
                .map_err(|e| format!("Failed to wait for legal review: {}", e))?;

        if !decision.approved {
            return Err(format!("Legal rejected: {}", decision.comments));
        }

        println!(
            "       [OK] Legal approved by {} - {}",
            decision.approver, decision.comments
        );
        Ok(())
    }

    #[step]
    async fn publish_document(self: Arc<Self>) -> Result<(), String> {
        println!("       [STEP] Publishing document...");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("       [OK] Document published");
        Ok(())
    }
}

// ============================================================================
// Main - Signal processing is now handled by Worker
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║     External Signal with Stdin Example                   ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let storage = Arc::new(SqliteExecutionLog::new("sqlite::memory:").await?);

    // Use StdinSignalSource (instead of SimulatedUserInputSource)
    let signal_source = Arc::new(StdinSignalSource::new());

    // Start worker with signal processing - Worker automatically handles signal delivery
    let worker = Worker::new(storage.clone(), "approval-worker");
    worker
        .register(|flow: Arc<DocumentApprovalFlow>| flow.process())
        .await;
    let worker = worker.with_signals(signal_source.clone()).start().await;

    let scheduler = Scheduler::new(storage.clone());

    println!("=== Scheduling Document for Approval ===\n");

    let doc1 = DocumentSubmission {
        document_id: "DOC-001".to_string(),
        title: "Q4 Financial Report".to_string(),
        author: "john.doe@company.com".to_string(),
        content: "Financial summary for Q4...".to_string(),
    };

    let flow1 = DocumentApprovalFlow {
        submission: doc1.clone(),
    };

    let flow_id = Uuid::new_v4();
    scheduler.schedule(flow1, flow_id).await?;

    // Keep running until user terminates
    println!("\n[INFO] Flow is waiting for your approval signals via stdin");
    println!("[INFO] Press Ctrl+C to exit\n");

    tokio::signal::ctrl_c().await?;

    worker.shutdown().await;
    storage.close().await?;

    println!("\n[INFO] Shutdown complete");
    Ok(())
}
