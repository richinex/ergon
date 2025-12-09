//! External Signal Abstraction Example
//!
//! This example demonstrates:
//! - Using external signals to suspend and resume flows
//! - Abstracting signal sources for easy replacement (HTTP, stdin, message queues, etc.)
//! - Built-in Worker integration for automatic signal processing (no manual polling!)
//! - Modeling signal outcomes (approved/rejected) vs errors
//!
//! ## Scenario
//! A document approval workflow that waits for external approval signals.
//! The signal source implements `ergon::executor::SignalSource`, making it easy
//! to swap between implementations (testing, CLI input, HTTP, message queues).
//!
//! ## Worker Integration
//! ```rust
//! let worker = Worker::new(storage, "worker-1")
//!     .with_signals(signal_source)  // Automatic signal processing!
//!     .start()
//!     .await;
//! ```
//!
//! ## Run with
//! ```bash
//! cargo run --example external_signal_abstraction --features=sqlite
//! ```

use async_trait::async_trait;
use ergon::executor::{await_external_signal, SignalSource};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
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

/// Outcome of an approval step (both approved and rejected are valid outcomes)
#[derive(Clone, Debug, Serialize, Deserialize)]
enum ApprovalOutcome {
    Approved { by: String, comment: String },
    Rejected { by: String, reason: String },
}

impl From<ApprovalDecision> for ApprovalOutcome {
    fn from(decision: ApprovalDecision) -> Self {
        if decision.approved {
            ApprovalOutcome::Approved {
                by: decision.approver,
                comment: decision.comments,
            }
        } else {
            ApprovalOutcome::Rejected {
                by: decision.approver,
                reason: decision.comments,
            }
        }
    }
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
//
// We use ergon::executor::SignalSource trait which allows you to easily
// swap between different signal sources:
// - MockSignalSource: For testing
// - UserInputSource: For manual CLI input
// - HttpSignalSource: For REST API endpoints
// - MessageQueueSource: For RabbitMQ/Kafka/etc.

// ============================================================================
// Simulated User Input Source (demonstrates realistic usage)
// ============================================================================

/// Simulates user input with random decisions after a delay.
/// In a real app, this would read from stdin, HTTP request, or GUI.
struct SimulatedUserInputSource {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl SimulatedUserInputSource {
    fn new() -> Self {
        Self {
            signals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Simulate user making a decision after a delay
    async fn simulate_user_decision(&self, signal_name: &str, delay: Duration, approve: bool) {
        tokio::time::sleep(delay).await;

        let decision = ApprovalDecision {
            approved: approve,
            approver: "manager@company.com".to_string(),
            comments: if approve {
                "Looks good, approved!".to_string()
            } else {
                "Needs revision".to_string()
            },
            timestamp: chrono::Utc::now().timestamp(),
        };

        let data = ergon::core::serialize_value(&decision).unwrap();
        let mut signals = self.signals.write().await;
        signals.insert(signal_name.to_string(), data);
        println!("  [INPUT] User decision received for '{}'", signal_name);
    }
}

#[async_trait]
impl SignalSource for SimulatedUserInputSource {
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
// Document Approval Workflow
// ============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct DocumentApprovalFlow {
    submission: DocumentSubmission,
}

impl DocumentApprovalFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("\n[FLOW] Processing document: {}", self.submission.title);
        println!("       Author: {}", self.submission.author);

        // Step 1: Validate document
        self.clone()
            .validate_document()
            .await
            .map_err(ExecutionError::Failed)?;

        // Step 2: Wait for manager approval (SUSPENDS HERE!)
        // Step returns OUTCOME (success), not error
        let manager_outcome = self
            .clone()
            .await_manager_approval()
            .await
            .map_err(ExecutionError::Failed)?;

        // Flow decides what rejection MEANS (permanent failure)
        match manager_outcome {
            ApprovalOutcome::Approved { .. } => {
                // Continue to next step
            }
            ApprovalOutcome::Rejected { by, reason } => {
                // Manager rejection is a permanent business decision - use NonRetryable
                println!("       [COMPLETE] Document rejected (permanent)");
                return Err(ExecutionError::NonRetryable(format!(
                    "Document rejected by {} - {}",
                    by, reason
                )));
            }
        }

        // Step 3: Wait for legal review (SUSPENDS AGAIN!)
        self.clone()
            .await_legal_review()
            .await
            .map_err(ExecutionError::Failed)?;

        // Step 4: Publish document
        self.clone()
            .publish_document()
            .await
            .map_err(ExecutionError::Failed)?;

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
    async fn await_manager_approval(self: Arc<Self>) -> Result<ApprovalOutcome, String> {
        println!("       [SUSPEND] Waiting for manager approval...");

        // This SUSPENDS the flow until signal arrives!
        let decision: ApprovalDecision =
            await_external_signal(&format!("manager_approval_{}", self.submission.document_id))
                .await
                .map_err(|e| format!("Failed to wait for approval: {}", e))?;

        // Convert to outcome - BOTH approved and rejected are successful outcomes
        let outcome: ApprovalOutcome = decision.into();

        // Log the outcome
        match &outcome {
            ApprovalOutcome::Approved { by, comment } => {
                println!("       [OK] Manager approved by {} - {}", by, comment);
            }
            ApprovalOutcome::Rejected { by, reason } => {
                println!("       [REJECTED] Manager rejected by {} - {}", by, reason);
            }
        }

        // Step succeeds with the outcome (cached for replay)
        Ok(outcome)
    }

    #[step]
    async fn await_legal_review(self: Arc<Self>) -> Result<(), String> {
        println!("       [SUSPEND] Waiting for legal review...");

        // Another suspension point!
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
// Main Example
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║     External Signal Abstraction Example                  ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Setup storage
    let storage = Arc::new(SqliteExecutionLog::new("sqlite::memory:").await?);

    // Choose your signal source implementation
    // Option 1: Mock (for testing)
    let signal_source = Arc::new(SimulatedUserInputSource::new());

    // Option 2: Real user input (you could implement this)
    // let signal_source = Arc::new(StdinSignalSource::new());

    // Option 3: HTTP endpoint (you could implement this)
    // let signal_source = Arc::new(HttpSignalSource::new("http://localhost:8080"));

    // Start worker with signal processing
    // The worker automatically handles signal delivery to waiting flows
    let worker = Worker::new(storage.clone(), "approval-worker");
    worker
        .register(|flow: Arc<DocumentApprovalFlow>| flow.process())
        .await;
    let worker = worker.with_signals(signal_source.clone()).start().await;

    // Create scheduler
    let scheduler = Scheduler::new(storage.clone());

    println!("=== Test 1: Approved Document ===\n");

    let doc1 = DocumentSubmission {
        document_id: "DOC-001".to_string(),
        title: "Q4 Financial Report".to_string(),
        author: "john.doe@company.com".to_string(),
        content: "Financial summary for Q4...".to_string(),
    };

    let flow1 = DocumentApprovalFlow {
        submission: doc1.clone(),
    };

    let flow_id_1 = Uuid::new_v4();
    let task_id_1 = scheduler.schedule(flow1, flow_id_1).await?;

    // Simulate manager approving after 1 second
    let signal_source_clone = signal_source.clone();
    let doc_id = doc1.document_id.clone();
    tokio::spawn(async move {
        signal_source_clone
            .simulate_user_decision(
                &format!("manager_approval_{}", doc_id),
                Duration::from_secs(1),
                true, // approve
            )
            .await;
    });

    // Simulate legal approving after 2 seconds
    let signal_source_clone = signal_source.clone();
    let doc_id = doc1.document_id.clone();
    tokio::spawn(async move {
        signal_source_clone
            .simulate_user_decision(
                &format!("legal_review_{}", doc_id),
                Duration::from_secs(2),
                true, // approve
            )
            .await;
    });

    // Wait for flow to complete by polling task status
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            println!("[WARN] Timeout waiting for Test 1 to complete");
            break;
        }
        match storage.get_scheduled_flow(task_id_1).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                    break;
                }
            }
            None => break, // Flow completed and removed from queue
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("\n=== Test 2: Rejected by Manager ===\n");

    let doc2 = DocumentSubmission {
        document_id: "DOC-002".to_string(),
        title: "Policy Update Draft".to_string(),
        author: "jane.smith@company.com".to_string(),
        content: "Proposed policy changes...".to_string(),
    };

    let flow2 = DocumentApprovalFlow {
        submission: doc2.clone(),
    };

    let flow_id_2 = Uuid::new_v4();
    let task_id_2 = scheduler.schedule(flow2, flow_id_2).await?;

    // Simulate manager rejecting after 1 second
    // Key insight: Rejection is an OUTCOME (step succeeds), not an error
    // The step returns Ok(ApprovalOutcome::Rejected) and is CACHED
    // The flow then returns NonRetryable error (permanent failure)
    // Result: Flow fails immediately with no retries (signal consumed only once)
    let signal_source_clone = signal_source.clone();
    let doc_id = doc2.document_id.clone();
    tokio::spawn(async move {
        signal_source_clone
            .simulate_user_decision(
                &format!("manager_approval_{}", doc_id),
                Duration::from_secs(1),
                false, // reject
            )
            .await;
    });

    // Wait for flow to fail with NonRetryable error
    // Expected time: ~1-2 seconds (no retries, signal consumed once)
    // We wait up to 10 seconds to be safe
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(20) {
            println!("\n[TIMEOUT] Test 2 did not complete within 20 seconds");
            if let Ok(Some(scheduled)) = storage.get_scheduled_flow(task_id_2).await {
                println!(
                    "[INFO] Final status: {:?}, retry_count: {}",
                    scheduled.status, scheduled.retry_count
                );
            }
            break;
        }
        match storage.get_scheduled_flow(task_id_2).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Failed) {
                    println!("\n[COMPLETE] Test 2 failed as expected: Manager rejection is permanent (NonRetryable)");
                    println!("           Signal step succeeded and was cached (no re-suspension on retry)");
                    break;
                } else if matches!(scheduled.status, TaskStatus::Complete) {
                    println!("\n[ERROR] Test 2 completed unexpectedly (should have failed with NonRetryable)");
                    break;
                }
            }
            None => {
                println!("\n[COMPLETE] Test 2 flow removed from queue");
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Shutdown
    worker.shutdown().await;

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Example Complete - Flows suspended and resumed via      ║");
    println!("║  external signals using abstracted signal sources!       ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    println!("\n[INFO] To implement a real signal source:");
    println!("   1. Implement the ergon::executor::SignalSource trait");
    println!("   2. Replace SimulatedUserInputSource with your implementation");
    println!("   3. Add it to Worker with .with_signals(your_source)");
    println!("   4. Examples:");
    println!("      - StdinSignalSource: Read from terminal input");
    println!("      - HttpSignalSource: Poll HTTP endpoint or webhook");
    println!("      - RedisSignalSource: Use Redis pub/sub");
    println!("      - KafkaSignalSource: Consume from Kafka topic");
    println!("\n   The Worker automatically handles signal delivery - no manual polling needed!\n");

    storage.close().await?;
    Ok(())
}
