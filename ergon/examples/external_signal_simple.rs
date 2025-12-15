//! Simple External Signal Example
//!
//! This example demonstrates using external signals with custom error types.
//! Users define their own error types and convert ExecutionError at framework boundaries.
//!
//! ## Key Points
//!
//! - Steps use custom error types (DocumentError)
//! - `await_external_signal()` returns `ExecutionError` which is converted at the boundary
//! - Worker automatically handles signal delivery via `.with_signals()`
//! - Signal outcomes (approved/rejected) are modeled as DATA, not errors
//!
//! ## Run with
//! ```bash
//! cargo run --example external_signal_simple --features=sqlite
//! ```

use async_trait::async_trait;
use ergon::executor::{await_external_signal, SignalSource};
use ergon::prelude::*;
use ergon::{Retryable, TaskStatus};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

// ============================================================================
// Custom Error Type
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
enum DocumentError {
    /// Document was rejected by manager (permanent business decision)
    ManagerRejection { by: String, reason: String },
    /// Document was rejected by legal team (permanent business decision)
    LegalRejection { by: String, reason: String },
    /// Infrastructure or framework error (retryable)
    Infrastructure(String),
}

impl std::fmt::Display for DocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DocumentError::ManagerRejection { by, reason } => {
                write!(f, "Document rejected by {} - {}", by, reason)
            }
            DocumentError::LegalRejection { by, reason } => {
                write!(f, "Legal rejected by {} - {}", by, reason)
            }
            DocumentError::Infrastructure(msg) => {
                write!(f, "Infrastructure error: {}", msg)
            }
        }
    }
}

impl std::error::Error for DocumentError {}

impl From<DocumentError> for String {
    fn from(err: DocumentError) -> Self {
        err.to_string()
    }
}

impl Retryable for DocumentError {
    fn is_retryable(&self) -> bool {
        match self {
            // Business rejections are permanent
            DocumentError::ManagerRejection { .. } => false,
            DocumentError::LegalRejection { .. } => false,
            // Infrastructure errors are retryable
            DocumentError::Infrastructure(_) => true,
        }
    }
}

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
    async fn process(self: Arc<Self>) -> Result<String, DocumentError> {
        println!("\n[FLOW] Processing document: {}", self.submission.title);
        println!("       Author: {}", self.submission.author);

        // Step 1: Validate document
        self.clone().validate_document().await?;

        // Step 2: Wait for manager approval (SUSPENDS HERE!)
        // Step returns OUTCOME (success), not error
        let manager_outcome = self.clone().await_manager_approval().await?;

        // Flow decides what rejection MEANS (permanent failure)
        match manager_outcome {
            ApprovalOutcome::Approved { .. } => {
                // Continue to next step
            }
            ApprovalOutcome::Rejected { by, reason } => {
                // Manager rejection is a permanent business decision
                println!("       [COMPLETE] Document rejected (permanent)");
                return Err(DocumentError::ManagerRejection { by, reason });
            }
        }

        // Step 3: Wait for legal review (SUSPENDS AGAIN!)
        self.clone().await_legal_review().await?;

        // Step 4: Publish document
        self.clone().publish_document().await?;

        Ok(format!(
            "Document '{}' approved and published",
            self.submission.title
        ))
    }

    #[step]
    async fn validate_document(self: Arc<Self>) -> Result<(), DocumentError> {
        println!("       [STEP] Validating document format...");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("       [OK] Validation passed");
        Ok(())
    }

    #[step]
    async fn await_manager_approval(self: Arc<Self>) -> Result<ApprovalOutcome, DocumentError> {
        println!("       [STEP] Awaiting manager approval...");

        // This may suspend the flow until signal arrives (or return immediately if cached)
        // await_external_signal returns ExecutionError, convert at the boundary
        let decision: ApprovalDecision =
            await_external_signal(&format!("manager_approval_{}", self.submission.document_id))
                .await
                .map_err(|e| DocumentError::Infrastructure(e.to_string()))?;

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
    async fn await_legal_review(self: Arc<Self>) -> Result<(), DocumentError> {
        println!("       [STEP] Awaiting legal review...");

        // Another suspension point (or immediate return if cached)
        let decision: ApprovalDecision =
            await_external_signal(&format!("legal_review_{}", self.submission.document_id))
                .await
                .map_err(|e| DocumentError::Infrastructure(e.to_string()))?;

        if !decision.approved {
            return Err(DocumentError::LegalRejection {
                by: decision.approver,
                reason: decision.comments,
            });
        }

        println!(
            "       [OK] Legal approved by {} - {}",
            decision.approver, decision.comments
        );
        Ok(())
    }

    #[step]
    async fn publish_document(self: Arc<Self>) -> Result<(), DocumentError> {
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
    // Setup storage
    let storage = Arc::new(SqliteExecutionLog::new("sqlite::memory:").await?);
    let signal_source = Arc::new(SimulatedUserInputSource::new());

    // Start worker with signal processing
    let worker = Worker::new(storage.clone(), "approval-worker");
    worker
        .register(|flow: Arc<DocumentApprovalFlow>| flow.process())
        .await;
    let worker = worker.with_signals(signal_source.clone()).start().await;

    let scheduler = Scheduler::new(storage.clone());

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
            break;
        }
        match storage.get_scheduled_flow(task_id_1).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                    break;
                }
            }
            None => break,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

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
    // The flow then returns DocumentError::ManagerRejection (permanent failure)
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

    // Wait for flow to fail with permanent error
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(20) {
            break;
        }
        match storage.get_scheduled_flow(task_id_2).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Failed)
                    || matches!(scheduled.status, TaskStatus::Complete)
                {
                    break;
                }
            }
            None => break,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    worker.shutdown().await;
    storage.close().await?;
    Ok(())
}
