//! External Signals with Custom Error Types
//!
//! This example demonstrates:
//! - Using external signals to suspend and resume flows
//! - Properly modeling signal outcomes (approved/rejected) as DATA, not errors
//! - Using custom error types with `Retryable` trait
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
//! - Define custom error types with `Retryable` trait
//! - Convert ExecutionError at framework boundaries using `.map_err()`
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
//! 1. `await_external_signal()` suspends via `Poll::Pending` (never returns)
//! 2. Worker detects suspension via timeout and `take_suspend_reason()`
//! 3. Suspension is completely invisible to user code - no error returned
//! 4. Business errors (LoanError) are handled normally via `.map_err()` conversions
//! 5. Errors follow normal retry logic based on `is_retryable()`
//!
//! ## Run with
//! ```bash
//! cargo run --example external_signal_custom_error --features=sqlite
//! ```

use async_trait::async_trait;
use chrono::Utc;
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

/// Custom error type for loan processing
///
/// Note: Suspension (waiting for signals/timers) is NOT an error - it's handled
/// via Poll::Pending and is completely invisible to user code. This enum only
/// contains actual business/infrastructure errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum LoanError {
    /// Infrastructure errors (network, database, etc.) - retryable
    Infrastructure(String),
    /// Business logic errors (low credit score, etc.) - non-retryable
    BusinessRejection(String),
}

impl std::fmt::Display for LoanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoanError::Infrastructure(msg) => write!(f, "Infrastructure error: {}", msg),
            LoanError::BusinessRejection(msg) => write!(f, "Business rejection: {}", msg),
        }
    }
}

impl std::error::Error for LoanError {}

impl Retryable for LoanError {
    fn is_retryable(&self) -> bool {
        match self {
            LoanError::Infrastructure(_) => true, // Infrastructure errors should retry
            LoanError::BusinessRejection(_) => false, // Business decisions are permanent
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
        println!("  [SIGNAL] Manager decision received for '{}'", signal_name);
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
            return Err(LoanError::BusinessRejection(
                "Amount must be greater than zero".to_string(),
            ));
        }

        if self.application.requested_amount > 1_000_000.0 {
            return Err(LoanError::BusinessRejection(
                "Amount exceeds maximum loan limit".to_string(),
            ));
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

        // Await external signal (suspends flow via Poll::Pending until signal arrives)
        // Convert framework errors (ExecutionError) to our domain error (LoanError)
        let decision: ManagerDecisionSignal = await_external_signal(&signal_name)
            .await
            .map_err(|e| LoanError::Infrastructure(e.to_string()))?;

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
                println!("[{}] Manager rejected by {}: {}", ts(), rejected_by, reason);
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
    async fn finalize_loan(
        self: Arc<Self>,
        decision: LoanDecision,
        loan_id: String,
    ) -> Result<String, String> {
        println!("[{}] Finalizing loan...", ts());
        tokio::time::sleep(Duration::from_millis(100)).await;

        match decision {
            LoanDecision::Approved {
                amount,
                interest_rate,
                term_months,
                ..
            } => {
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
    async fn process(self: Arc<Self>) -> Result<String, LoanError> {
        println!(
            "\n[FLOW] Processing loan application: {}",
            self.application.application_id
        );
        println!("       Applicant: {}", self.application.applicant_name);
        println!(
            "       Requested: ${:.2}",
            self.application.requested_amount
        );

        // Generate loan ID at flow level for determinism
        let loan_id = format!("LOAN-{}", &Uuid::new_v4().to_string()[..8]);

        // Step 1: Validate application
        self.clone().validate_application().await?;

        // Step 2: Credit check
        self.clone().check_credit().await?;

        // Step 3: Wait for manager approval (SUSPENDS HERE!)
        // Step returns OUTCOME (success), not error
        let decision = self.clone().await_manager_approval().await?;

        // Flow decides what each outcome MEANS
        match decision.clone() {
            LoanDecision::Approved { .. } => {
                // Continue to finalization
                let loan_id = self
                    .clone()
                    .finalize_loan(decision, loan_id)
                    .await
                    .map_err(LoanError::Infrastructure)?;
                println!("[FLOW] Loan approved and finalized: {}\n", loan_id);
                Ok(loan_id)
            }
            LoanDecision::Rejected {
                reason,
                rejected_by,
            } => {
                // Manager rejection is a permanent business decision
                // LoanError::BusinessRejection is non-retryable via is_retryable()
                println!("[FLOW] Loan rejected (permanent)\n");
                Err(LoanError::BusinessRejection(format!(
                    "Loan rejected by {}: {}",
                    rejected_by, reason
                )))
            }
            LoanDecision::RequiresAdditionalReview { reason, reviewer } => {
                // Additional review needed - could implement another signal wait here
                println!("[FLOW] Additional review required\n");
                Err(LoanError::BusinessRejection(format!(
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
    // Setup storage
    let storage = Arc::new(SqliteExecutionLog::new("sqlite::memory:").await?);

    // Create signal source
    let signal_source = Arc::new(MockLoanSignalSource::new());

    // Start worker with signal processing
    let worker = Worker::new(storage.clone(), "loan-processor");
    worker
        .register(|flow: Arc<LoanApplicationFlow>| flow.process())
        .await;
    let worker = worker.with_signals(signal_source.clone()).start().await;

    // Create scheduler
    let scheduler = Scheduler::new(storage.clone());

    // ========================================================================
    // Test 1: Approved Loan (Good Credit)
    // ========================================================================

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

    // Wait for failure with permanent error
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(20) {
            break;
        }
        match storage.get_scheduled_flow(task_id_2).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Failed) {
                    break;
                }
            }
            None => {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ========================================================================
    // Test 3: Ineligible Applicant (Low Credit - Error)
    // ========================================================================

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

    storage.close().await?;
    Ok(())
}
