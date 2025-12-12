//! External Signal Abstraction Example - FlowError Variant
//!
//! This example demonstrates using **FlowError** for error handling.
//!
//! ## Pattern Shown: FlowError
//!
//! This variant uses `Result<T, FlowError>` in flows and steps:
//! - **Required** when using `dag!` macro or `inputs()` feature
//! - Preserves error type information via serialization
//! - Allows downcasting back to original error types
//! - Custom errors auto-convert via `IntoFlowError` trait
//!
//! ```rust
//! #[flow]
//! async fn process(self: Arc<Self>) -> Result<String, FlowError> {
//!     self.validate().await?;  // DocumentError auto-converts to FlowError
//!     Ok("done")
//! }
//!
//! #[step]
//! async fn validate(self: Arc<Self>) -> Result<(), FlowError> {
//!     if problem {
//!         return Err(DocumentError::Invalid.into_flow_error());
//!     }
//!     Ok(())
//! }
//! ```
//!
//! ## Compare With
//!
//! See `external_signal_abstraction.rs` for the custom error variant.
//!
//! ## Run with
//! ```bash
//! cargo run --example external_signal_abstraction_flowerror --features=sqlite
//! ```

use async_trait::async_trait;
use ergon::executor::{await_external_signal, SignalSource};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

// Global execution counters
static VALIDATE_COUNT: AtomicU32 = AtomicU32::new(0);
static MANAGER_APPROVAL_COUNT: AtomicU32 = AtomicU32::new(0);
static LEGAL_REVIEW_COUNT: AtomicU32 = AtomicU32::new(0);
static PUBLISH_COUNT: AtomicU32 = AtomicU32::new(0);

// ============================================================================
// Custom Error Type
// ============================================================================

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
enum DocumentError {
    /// Document was rejected by manager (permanent business decision)
    #[error("Document rejected by {by} - {reason}")]
    ManagerRejection { by: String, reason: String },
    /// Document was rejected by legal team (permanent business decision)
    #[error("Legal rejected by {by} - {reason}")]
    LegalRejection { by: String, reason: String },
    /// Infrastructure or framework error (retryable)
    #[error("Infrastructure error: {0}")]
    Infrastructure(String),
}

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
    async fn process(self: Arc<Self>) -> Result<String, FlowError> {
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
                return Err(DocumentError::ManagerRejection { by, reason }.into_flow_error());
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
    async fn validate_document(self: Arc<Self>) -> Result<(), FlowError> {
        VALIDATE_COUNT.fetch_add(1, Ordering::Relaxed);
        println!("       [STEP] Validating document format...");
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("       [OK] Validation passed");
        Ok(())
    }

    #[step]
    async fn await_manager_approval(self: Arc<Self>) -> Result<ApprovalOutcome, FlowError> {
        // This may suspend the flow until signal arrives (or return immediately if cached)
        let decision: ApprovalDecision =
            await_external_signal(&format!("manager_approval_{}", self.submission.document_id))
                .await?; // ExecutionError auto-converts to FlowError

        // Count and log AFTER signal received (only once per flow, not on replay)
        MANAGER_APPROVAL_COUNT.fetch_add(1, Ordering::Relaxed);
        println!("       [RECEIVED] Manager approval signal received");

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
    async fn await_legal_review(self: Arc<Self>) -> Result<(), FlowError> {
        // Another suspension point (or immediate return if cached)
        let decision: ApprovalDecision =
            await_external_signal(&format!("legal_review_{}", self.submission.document_id))
                .await?; // ExecutionError auto-converts to FlowError

        // Count and log AFTER signal received (only once per flow, not on replay)
        LEGAL_REVIEW_COUNT.fetch_add(1, Ordering::Relaxed);

        if !decision.approved {
            println!(
                "       [REJECTED] Legal rejected by {} - {}",
                decision.approver, decision.comments
            );
            return Err(DocumentError::LegalRejection {
                by: decision.approver,
                reason: decision.comments,
            }
            .into_flow_error());
        }

        println!(
            "       [OK] Legal approved by {} - {}",
            decision.approver, decision.comments
        );
        Ok(())
    }

    #[step]
    async fn publish_document(self: Arc<Self>) -> Result<(), FlowError> {
        PUBLISH_COUNT.fetch_add(1, Ordering::Relaxed);
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

    let storage = Arc::new(SqliteExecutionLog::new("sqlite::memory:").await?);
    let signal_source = Arc::new(SimulatedUserInputSource::new());

    // ============================================================
    // PART 1: API Server / Scheduler Process
    // ============================================================
    // In production, this would be an HTTP endpoint that:
    //   POST /api/documents -> schedules workflow -> returns 202 Accepted with task_id
    //
    // The scheduler does NOT wait for completion. It returns immediately.
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 1: Scheduling Documents (API Server)                 ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

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
    println!("   ✓ DOC-001 scheduled (task_id: {})", task_id_1);

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
    println!("   ✓ DOC-002 scheduled (task_id: {})", task_id_2);

    println!("\n   → In production: Return HTTP 202 Accepted");
    println!(
        "   → Response body: {{\"task_ids\": [\"{}\", ...]}}",
        &task_id_1.to_string()[..8]
    );
    println!("   → Client polls GET /api/tasks/:id for status\n");

    // ============================================================
    // PART 2: Worker Service (Separate Process)
    // ============================================================
    // In production, workers run in separate pods/containers/services.
    // They continuously poll the shared storage for work.
    //
    // Workers are completely decoupled from the scheduler.
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 2: Starting Worker (Separate Service)                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let worker = Worker::new(storage.clone(), "document-processor");
    worker
        .register(|flow: Arc<DocumentApprovalFlow>| flow.process())
        .await;
    let worker = worker.with_signals(signal_source.clone()).start().await;
    println!("   ✓ Worker started with signal processing enabled\n");

    // Simulate manager approving DOC-001 after 1 second
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

    // Simulate legal approving DOC-001 after 2 seconds
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

    // Wait for Test 1 (DOC-001) to complete
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            println!("\n[TIMEOUT] Test 1 did not complete within 10 seconds");
            if let Ok(Some(scheduled)) = storage.get_scheduled_flow(task_id_1).await {
                println!(
                    "[INFO] Final status: {:?}, retry_count: {}",
                    scheduled.status, scheduled.retry_count
                );
            }
            break;
        }
        match storage.get_scheduled_flow(task_id_1).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete) {
                    println!("\n[COMPLETE] Test 1 completed successfully");
                    break;
                } else if matches!(scheduled.status, TaskStatus::Failed) {
                    println!("\n[ERROR] Test 1 failed unexpectedly");
                    break;
                }
            }
            None => {
                println!("\n[COMPLETE] Test 1 flow removed from queue (completed)");
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Simulate manager rejecting DOC-002 after 1 second
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

    println!("\n=== Summary ===\n");

    println!("Step Execution Counts:");
    println!(
        "  validate_document:      {}",
        VALIDATE_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  await_manager_approval: {} (SIGNAL steps)",
        MANAGER_APPROVAL_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  await_legal_review:     {} (SIGNAL steps)",
        LEGAL_REVIEW_COUNT.load(Ordering::Relaxed)
    );
    println!(
        "  publish_document:       {}",
        PUBLISH_COUNT.load(Ordering::Relaxed)
    );

    println!("\nFlow Results:");
    match storage.get_scheduled_flow(task_id_1).await? {
        Some(scheduled) => println!("  DOC-001: {:?}", scheduled.status),
        None => println!("  DOC-001: Complete (removed from queue)"),
    }
    match storage.get_scheduled_flow(task_id_2).await? {
        Some(scheduled) => println!("  DOC-002: {:?}", scheduled.status),
        None => println!("  DOC-002: Complete (removed from queue)"),
    }

    storage.close().await?;
    Ok(())
}
