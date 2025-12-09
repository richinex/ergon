//! External Signals with Custom Error Types
//!
//! This example demonstrates:
//! - Using external signals to suspend and resume flows
//! - Properly modeling signal outcomes (approved/rejected) as DATA, not errors
//! - Using custom error types with `RetryableError` trait
//! - Converting `ExecutionError` to custom errors using `From` trait
//! - Distinguishing between retryable failures and permanent rejections
//! - Worker integration with `.with_signals()`
//!
//! ## Key Principles
//!
//! ### Outcomes vs Errors
//! - **Outcome**: Manager rejects a loan → `Ok(LoanDecision::Rejected)` (signal step succeeds!)
//! - **Error**: Network timeout, validation failure → `Err(LoanError::...)`
//!
//! ### Custom Errors with Signals
//! - Define custom error types with `RetryableError` trait
//! - Implement `From<ExecutionError> for YourError` to convert framework errors
//! - The step macro now checks `suspend_reason` BEFORE caching, so suspension works correctly
//! - Use `is_retryable()` to control retry behavior for business logic errors
//!
//! ### Error Classification
//! - **Infrastructure errors**: Network, database, etc. → `is_retryable() = true`
//! - **Business rejections**: Low credit, invalid data → `is_retryable() = false`
//! - **Signal errors**: Suspension-related → `is_retryable() = true` (handled by framework)
//!
//! ## How It Works
//!
//! 1. `await_external_signal()` returns `ExecutionError::Failed("Flow suspended")`
//! 2. Convert to `LoanError::SignalError` using `From<ExecutionError>`
//! 3. Step macro checks `has_suspend_reason()` BEFORE checking `is_retryable()`
//! 4. Suspension is detected and handled correctly regardless of `is_retryable()` value
//! 5. Other errors follow normal retry logic based on `is_retryable()`
//!
//! ## Run with
//! ```bash
//! cargo run --example external_signal_custom_error --features=sqlite
//! ```

use async_trait::async_trait;
use chrono::Utc;
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

use ergon::core::RetryableError;

/// Custom error type for loan processing
#[derive(Debug, Clone, Serialize, Deserialize)]
enum LoanError {
    /// Infrastructure errors (network, database, etc.) - retryable
    Infrastructure(String),
    /// Business logic errors (low credit score, etc.) - non-retryable
    BusinessRejection(String),
    /// Signal-related errors - handled by suspension mechanism
    SignalError(String),
}

impl std::fmt::Display for LoanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoanError::Infrastructure(msg) => write!(f, "Infrastructure error: {}", msg),
            LoanError::BusinessRejection(msg) => write!(f, "Business rejection: {}", msg),
            LoanError::SignalError(msg) => write!(f, "Signal error: {}", msg),
        }
    }
}

impl std::error::Error for LoanError {}

impl RetryableError for LoanError {
    fn is_retryable(&self) -> bool {
        match self {
            LoanError::Infrastructure(_) => true,  // Infrastructure errors should retry
            LoanError::BusinessRejection(_) => false,  // Business decisions are permanent
            LoanError::SignalError(_) => true,  // Signal errors are handled by suspension
        }
    }
}

impl From<ExecutionError> for LoanError {
    fn from(e: ExecutionError) -> Self {
        // Convert ExecutionError to LoanError
        // The step macro now checks suspend_reason before caching, so we can safely convert
        match e {
            ExecutionError::Core(msg) => LoanError::Infrastructure(msg),
            ExecutionError::Failed(msg) => LoanError::Infrastructure(msg),
            ExecutionError::Suspended(msg) => LoanError::SignalError(msg),
            ExecutionError::Incompatible(msg) => LoanError::Infrastructure(format!("Non-determinism: {}", msg)),
            ExecutionError::NonRetryable(msg) => LoanError::BusinessRejection(msg),
            _ => LoanError::Infrastructure("Unknown error".to_string()),
        }
    }
}

// ============================================================================
// Domain Types - Signal Outcomes (DATA, not errors!)
// ============================================================================

/// External signal data from manager
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManagerDecisionSignal {
    approved: bool,
    manager_id: String,
    comments: String,
    timestamp: i64,
}

/// Loan decision outcome - BOTH approved and rejected are valid outcomes
#[derive(Clone, Debug, Serialize, Deserialize)]
enum LoanDecision {
    Approved {
        amount: f64,
        interest_rate: f64,
        term_months: u32,
        approved_by: String,
    },
    Rejected {
        reason: String,
        rejected_by: String,
    },
    RequiresAdditionalReview {
        reason: String,
        reviewer: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LoanApplication {
    application_id: String,
    applicant_name: String,
    requested_amount: f64,
    credit_score: u32,
    annual_income: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreditCheckResult {
    score: u32,
    outstanding_debt: f64,
    payment_history: String,
}

// ============================================================================
// Signal Source Implementation
// ============================================================================

/// Simulated signal source for testing
struct MockLoanSignalSource {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl MockLoanSignalSource {
    fn new() -> Self {
        Self {
            signals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Simulate manager making a decision after a delay
    async fn simulate_manager_decision(
        &self,
        signal_name: &str,
        delay: Duration,
        approved: bool,
        comments: &str,
    ) {
        tokio::time::sleep(delay).await;

        let decision = ManagerDecisionSignal {
            approved,
            manager_id: "manager@bank.com".to_string(),
            comments: comments.to_string(),
            timestamp: Utc::now().timestamp(),
        };

        let data = ergon::core::serialize_value(&decision).unwrap();
        let mut signals = self.signals.write().await;
        signals.insert(signal_name.to_string(), data);
        println!(
            "  [SIGNAL] Manager decision received for '{}'",
            signal_name
        );
    }
}

#[async_trait]
impl SignalSource for MockLoanSignalSource {
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
// Loan Application Flow
// ============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct LoanApplicationFlow {
    application: LoanApplication,
}

impl LoanApplicationFlow {
    #[step]
    async fn validate_application(self: Arc<Self>) -> Result<(), LoanError> {
        println!(
            "[{}] Validating application {}",
            ts(),
            self.application.application_id
        );
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Validate requested amount
        if self.application.requested_amount <= 0.0 {
            return Err(LoanError::BusinessRejection("Amount must be greater than zero".to_string()));
        }

        if self.application.requested_amount > 1_000_000.0 {
            return Err(LoanError::BusinessRejection("Amount exceeds maximum loan limit".to_string()));
        }

        println!("[{}] Application validation passed", ts());
        Ok(())
    }

    #[step]
    async fn check_credit(self: Arc<Self>) -> Result<CreditCheckResult, LoanError> {
        println!(
            "[{}] Running credit check for {}",
            ts(),
            self.application.applicant_name
        );
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Simulate credit check service
        let result = CreditCheckResult {
            score: self.application.credit_score,
            outstanding_debt: 15000.0,
            payment_history: "Good".to_string(),
        };

        // Check eligibility based on credit score
        if result.score < 600 {
            return Err(LoanError::BusinessRejection(format!(
                "Credit score {} is below minimum requirement of 600",
                result.score
            )));
        }

        println!("[{}] Credit check passed: score {}", ts(), result.score);
        Ok(result)
    }

    #[step]
    async fn await_manager_approval(self: Arc<Self>) -> Result<LoanDecision, LoanError> {
        println!(
            "[{}] Awaiting manager approval for {}...",
            ts(),
            self.application.application_id
        );

        let signal_name = format!("loan_approval_{}", self.application.application_id);

        // Await external signal (suspends flow until signal arrives)
        // With the updated step macro, suspension is detected via suspend_reason
        // So we can safely convert ExecutionError to LoanError using From
        let decision: ManagerDecisionSignal = await_external_signal(&signal_name)
            .await
            .map_err(LoanError::from)?;

        // Convert signal to outcome - BOTH approved and rejected are successful outcomes!
        let outcome = if decision.approved {
            // Calculate terms based on credit score and amount
            let base_rate = 5.0;
            let credit_adjustment = (750 - self.application.credit_score as i32) as f64 * 0.01;
            let interest_rate = (base_rate + credit_adjustment).max(3.5);

            LoanDecision::Approved {
                amount: self.application.requested_amount,
                interest_rate,
                term_months: 60,
                approved_by: decision.manager_id,
            }
        } else {
            LoanDecision::Rejected {
                reason: decision.comments,
                rejected_by: decision.manager_id,
            }
        };

        // Log the outcome
        match &outcome {
            LoanDecision::Approved {
                amount,
                interest_rate,
                approved_by,
                ..
            } => {
                println!(
                    "[{}] Manager approved: ${:.2} at {:.2}% by {}",
                    ts(),
                    amount,
                    interest_rate,
                    approved_by
                );
            }
            LoanDecision::Rejected {
                reason,
                rejected_by,
            } => {
                println!(
                    "[{}] Manager rejected by {}: {}",
                    ts(),
                    rejected_by,
                    reason
                );
            }
            LoanDecision::RequiresAdditionalReview { reason, reviewer } => {
                println!(
                    "[{}] Additional review required by {}: {}",
                    ts(),
                    reviewer,
                    reason
                );
            }
        }

        // Step succeeds with the outcome (cached for replay)
        Ok(outcome)
    }

    #[step]
    async fn finalize_loan(self: Arc<Self>, decision: LoanDecision) -> Result<String, String> {
        println!("[{}] Finalizing loan...", ts());
        tokio::time::sleep(Duration::from_millis(100)).await;

        match decision {
            LoanDecision::Approved {
                amount,
                interest_rate,
                term_months,
                ..
            } => {
                let loan_id = format!("LOAN-{}", &Uuid::new_v4().to_string()[..8]);
                println!(
                    "[{}] Loan finalized: {} - ${:.2} at {:.2}% for {} months",
                    ts(),
                    loan_id,
                    amount,
                    interest_rate,
                    term_months
                );
                Ok(loan_id)
            }
            _ => {
                // This shouldn't happen if flow logic is correct
                Err("Cannot finalize non-approved loan".to_string())
            }
        }
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!(
            "\n[FLOW] Processing loan application: {}",
            self.application.application_id
        );
        println!("       Applicant: {}", self.application.applicant_name);
        println!(
            "       Requested: ${:.2}",
            self.application.requested_amount
        );

        // Step 1: Validate application
        self.clone()
            .validate_application()
            .await
            .map_err(|e| match e {
                LoanError::BusinessRejection(msg) => ExecutionError::NonRetryable(msg),
                LoanError::Infrastructure(msg) => ExecutionError::Failed(msg),
                LoanError::SignalError(msg) => ExecutionError::Failed(msg),
            })?;

        // Step 2: Credit check
        self.clone()
            .check_credit()
            .await
            .map_err(|e| match e {
                LoanError::BusinessRejection(msg) => ExecutionError::NonRetryable(msg),
                LoanError::Infrastructure(msg) => ExecutionError::Failed(msg),
                LoanError::SignalError(msg) => ExecutionError::Failed(msg),
            })?;

        // Step 3: Wait for manager approval (SUSPENDS HERE!)
        // Step returns OUTCOME (success), not error
        let decision = self.clone()
            .await_manager_approval()
            .await
            .map_err(|e| match e {
                LoanError::BusinessRejection(msg) => ExecutionError::NonRetryable(msg),
                LoanError::Infrastructure(msg) => ExecutionError::Failed(msg),
                LoanError::SignalError(msg) => ExecutionError::Failed(msg),
            })?;

        // Flow decides what each outcome MEANS
        match decision.clone() {
            LoanDecision::Approved { .. } => {
                // Continue to finalization
                let loan_id = self.clone()
                    .finalize_loan(decision)
                    .await
                    .map_err(|e| ExecutionError::Failed(e))?;
                println!("[FLOW] Loan approved and finalized: {}\n", loan_id);
                Ok(loan_id)
            }
            LoanDecision::Rejected {
                reason,
                rejected_by,
            } => {
                // Manager rejection is a permanent business decision
                // Use NonRetryable for permanent failures
                println!("[FLOW] Loan rejected (permanent)\n");
                Err(ExecutionError::NonRetryable(format!(
                    "Loan rejected by {}: {}",
                    rejected_by, reason
                )))
            }
            LoanDecision::RequiresAdditionalReview { reason, reviewer } => {
                // Additional review needed - could implement another signal wait here
                println!("[FLOW] Additional review required\n");
                Err(ExecutionError::NonRetryable(format!(
                    "Additional review required by {}: {}",
                    reviewer, reason
                )))
            }
        }
    }
}

// ============================================================================
// Utilities
// ============================================================================

fn ts() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

// ============================================================================
// Main Example
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║   External Signals with Custom Error Types Example       ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // Setup storage
    let storage = Arc::new(SqliteExecutionLog::new("sqlite::memory:").await?);

    // Create signal source
    let signal_source = Arc::new(MockLoanSignalSource::new());

    // Start worker with signal processing
    let worker = Worker::new(storage.clone(), "loan-processor");
    worker
        .register(|flow: Arc<LoanApplicationFlow>| flow.process())
        .await;
    let worker = worker
        .with_signals(signal_source.clone())
        .start()
        .await;

    // Create scheduler
    let scheduler = Scheduler::new(storage.clone());

    // ========================================================================
    // Test 1: Approved Loan (Good Credit)
    // ========================================================================
    println!("=== Test 1: Approved Loan (Good Credit) ===\n");

    let app1 = LoanApplication {
        application_id: "APP-001".to_string(),
        applicant_name: "Alice Johnson".to_string(),
        requested_amount: 50000.0,
        credit_score: 750,
        annual_income: 80000.0,
    };

    let flow1 = LoanApplicationFlow {
        application: app1.clone(),
    };

    let flow_id_1 = Uuid::new_v4();
    let task_id_1 = scheduler.schedule(flow1, flow_id_1).await?;

    // Simulate manager approving after 1 second
    let signal_source_clone = signal_source.clone();
    let app_id = app1.application_id.clone();
    tokio::spawn(async move {
        signal_source_clone
            .simulate_manager_decision(
                &format!("loan_approval_{}", app_id),
                Duration::from_secs(1),
                true,
                "Excellent credit, approved!",
            )
            .await;
    });

    // Wait for completion
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            println!("[WARN] Timeout waiting for Test 1");
            break;
        }
        match storage.get_scheduled_flow(task_id_1).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete) {
                    break;
                }
            }
            None => break,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ========================================================================
    // Test 2: Rejected Loan (Business Decision)
    // ========================================================================
    println!("\n=== Test 2: Rejected Loan (Business Decision) ===\n");

    let app2 = LoanApplication {
        application_id: "APP-002".to_string(),
        applicant_name: "Bob Smith".to_string(),
        requested_amount: 100000.0,
        credit_score: 720,
        annual_income: 60000.0,
    };

    let flow2 = LoanApplicationFlow {
        application: app2.clone(),
    };

    let flow_id_2 = Uuid::new_v4();
    let task_id_2 = scheduler.schedule(flow2, flow_id_2).await?;

    // Simulate manager rejecting after 1 second
    let signal_source_clone = signal_source.clone();
    let app_id = app2.application_id.clone();
    tokio::spawn(async move {
        signal_source_clone
            .simulate_manager_decision(
                &format!("loan_approval_{}", app_id),
                Duration::from_secs(1),
                false,
                "Debt-to-income ratio too high",
            )
            .await;
    });

    // Wait for failure with NonRetryable error
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(20) {
            println!("\n[TIMEOUT] Test 2 did not complete");
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
                }
            }
            None => {
                println!("\n[COMPLETE] Test 2 flow removed from queue");
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ========================================================================
    // Test 3: Ineligible Applicant (Low Credit - Error)
    // ========================================================================
    println!("\n=== Test 3: Ineligible Applicant (Low Credit - Error) ===\n");

    let app3 = LoanApplication {
        application_id: "APP-003".to_string(),
        applicant_name: "Charlie Brown".to_string(),
        requested_amount: 30000.0,
        credit_score: 550, // Below minimum
        annual_income: 50000.0,
    };

    let flow3 = LoanApplicationFlow {
        application: app3.clone(),
    };

    let flow_id_3 = Uuid::new_v4();
    let task_id_3 = scheduler.schedule(flow3, flow_id_3).await?;

    // This should fail at credit check step (before waiting for signal)
    tokio::time::sleep(Duration::from_millis(500)).await;

    match storage.get_scheduled_flow(task_id_3).await? {
        Some(scheduled) => {
            println!(
                "[INFO] Test 3 status: {:?} (failed at credit check)",
                scheduled.status
            );
        }
        None => {
            println!("[INFO] Test 3 flow removed from queue");
        }
    }

    // ========================================================================
    // Test 4: Invalid Application (Error)
    // ========================================================================
    println!("\n=== Test 4: Invalid Application (Error) ===\n");

    let app4 = LoanApplication {
        application_id: "APP-004".to_string(),
        applicant_name: "Diana Prince".to_string(),
        requested_amount: -5000.0, // Invalid amount
        credit_score: 700,
        annual_income: 70000.0,
    };

    let flow4 = LoanApplicationFlow {
        application: app4.clone(),
    };

    let flow_id_4 = Uuid::new_v4();
    let task_id_4 = scheduler.schedule(flow4, flow_id_4).await?;

    // This should fail at validation step
    tokio::time::sleep(Duration::from_millis(500)).await;

    match storage.get_scheduled_flow(task_id_4).await? {
        Some(scheduled) => {
            println!(
                "[INFO] Test 4 status: {:?} (failed at validation)",
                scheduled.status
            );
        }
        None => {
            println!("[INFO] Test 4 flow removed from queue");
        }
    }

    // Shutdown
    tokio::time::sleep(Duration::from_secs(1)).await;
    worker.shutdown().await;

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Example Complete - Demonstrates:                        ║");
    println!("║  • Custom error types for domain-specific failures       ║");
    println!("║  • Signal outcomes as DATA (approved/rejected)           ║");
    println!("║  • Worker integration with .with_signals()               ║");
    println!("║  • Proper error vs outcome distinction                   ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    println!("\n[INFO] Key Takeaways:");
    println!("   1. Signal outcomes (approved/rejected) are Ok(LoanDecision)");
    println!("   2. Technical failures are Err(LoanError)");
    println!("   3. Flow decides what outcomes mean (rejection = BusinessRejection error)");
    println!("   4. RetryableError::is_retryable() controls retry behavior");
    println!("   5. Custom errors provide rich context for error handling\n");

    storage.close().await?;
    Ok(())
}
