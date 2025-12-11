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

impl RetryableError for DocumentError {
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
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║     Simple External Signal Example                       ║");
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
                    println!(
                        "\n[COMPLETE] Test 2 failed as expected: Manager rejection is permanent"
                    );
                    println!("           Signal step succeeded and was cached (no re-suspension on retry)");
                    break;
                } else if matches!(scheduled.status, TaskStatus::Complete) {
                    println!("\n[ERROR] Test 2 completed unexpectedly (should have failed)");
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

// Output
// ergon git:(modularize-worker) ✗ cargo run --example external_signal_simple
//    Compiling ergon v0.1.0 (/home/richinex/Documents/devs/rust_projects/ergon/ergon)
//     Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.34s
//      Running `target/debug/examples/external_signal_simple`

// ╔═══════════════════════════════════════════════════════════╗
// ║     Simple External Signal Example                       ║
// ╚═══════════════════════════════════════════════════════════╝

// === Test 1: Approved Document ===

// [FLOW] Processing document: Q4 Financial Report
//        Author: john.doe@company.com
//        [STEP] Validating document format...
//        [OK] Validation passed
//        [SUSPEND] Waiting for manager approval...
//   [INPUT] User decision received for 'manager_approval_DOC-001'
//   [INPUT] User decision received for 'legal_review_DOC-001'

// [FLOW] Processing document: Q4 Financial Report
//        Author: john.doe@company.com
//        [SUSPEND] Waiting for manager approval...
//        [OK] Manager approved by manager@company.com - Looks good, approved!
//        [SUSPEND] Waiting for legal review...

// [FLOW] Processing document: Q4 Financial Report
//        Author: john.doe@company.com
//        [SUSPEND] Waiting for legal review...
//        [OK] Legal approved by manager@company.com - Looks good, approved!
//        [STEP] Publishing document...
//        [OK] Document published

// === Test 2: Rejected by Manager ===

//   [INPUT] User decision received for 'manager_approval_DOC-002'

// [FLOW] Processing document: Policy Update Draft
//        Author: jane.smith@company.com
//        [STEP] Validating document format...
//        [OK] Validation passed
//        [SUSPEND] Waiting for manager approval...

// [FLOW] Processing document: Policy Update Draft
//        Author: jane.smith@company.com
//        [SUSPEND] Waiting for manager approval...
//        [REJECTED] Manager rejected by manager@company.com - Needs revision
//        [COMPLETE] Document rejected (permanent)

// [COMPLETE] Test 2 failed as expected: Manager rejection is permanent
//            Signal step succeeded and was cached (no re-suspension on retry)

// ╔═══════════════════════════════════════════════════════════╗
// ║  Example Complete - Flows suspended and resumed via      ║
// ║  external signals using abstracted signal sources!       ║
// ╚═══════════════════════════════════════════════════════════╝

// [INFO] To implement a real signal source:
//    1. Implement the ergon::executor::SignalSource trait
//    2. Replace SimulatedUserInputSource with your implementation
//    3. Add it to Worker with .with_signals(your_source)
//    4. Examples:
//       - StdinSignalSource: Read from terminal input
//       - HttpSignalSource: Poll HTTP endpoint or webhook
//       - RedisSignalSource: Use Redis pub/sub
//       - KafkaSignalSource: Consume from Kafka topic

//    The Worker automatically handles signal delivery - no manual polling needed!
