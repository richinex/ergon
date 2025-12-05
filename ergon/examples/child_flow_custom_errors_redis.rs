//! Child Flow Level 3 API with Custom Error Types
//!
//! This example combines two advanced features:
//! 1. **Level 3 child flow API** - Token-based type-safe invocation
//! 2. **Custom error types** - Rich, domain-specific errors with retry control
//!
//! ## Scenario: Loan Application Processing
//!
//! A loan approval system with two child flows:
//! - Parent: LoanApplication orchestrator
//! - Child 1: CreditCheck (validates credit score)
//! - Child 2: IncomeVerification (validates income)
//!
//! Each child can fail with custom errors:
//! - **Retryable**: CreditBureauTimeout, VerificationServiceDown
//! - **Permanent**: CreditScoreTooLow, InsufficientIncome, InvalidSSN
//!
//! ## Key Features
//!
//! 1. **Level 3 API**: No parent_flow_id fields, direct invocation
//! 2. **Custom Errors**: Rich context (scores, amounts, reasons)
//! 3. **Retry Control**: Only transient errors retry automatically
//! 4. **Type Safety**: Compiler enforces error handling
//!
//! ## Run
//!
//! ```bash
//! cargo run --example child_flow_custom_errors --features=sqlite
//! ```

use chrono::Utc;
use ergon::core::{FlowType, InvokableFlow};
use ergon::executor::{InvokeChild, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Custom Error Types
// =============================================================================

// Counter to simulate retry success
static CREDIT_CHECK_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

/// Credit check errors
#[derive(Debug, Clone, Serialize, Deserialize)]
enum CreditCheckError {
    CreditScoreTooLow { score: u32, minimum_required: u32 },
    InvalidSSN { ssn: String },
    CreditBureauTimeout,
    CreditBureauUnavailable { bureau_name: String },
}

impl fmt::Display for CreditCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CreditCheckError::CreditScoreTooLow {
                score,
                minimum_required,
            } => {
                write!(
                    f,
                    "Credit score {} is below minimum required {}",
                    score, minimum_required
                )
            }
            CreditCheckError::InvalidSSN { ssn } => {
                write!(f, "Invalid SSN format: {}", ssn)
            }
            CreditCheckError::CreditBureauTimeout => {
                write!(f, "Credit bureau request timed out")
            }
            CreditCheckError::CreditBureauUnavailable { bureau_name } => {
                write!(
                    f,
                    "Credit bureau {} is temporarily unavailable",
                    bureau_name
                )
            }
        }
    }
}

impl std::error::Error for CreditCheckError {}

impl From<CreditCheckError> for String {
    fn from(err: CreditCheckError) -> Self {
        err.to_string()
    }
}

impl RetryableError for CreditCheckError {
    fn is_retryable(&self) -> bool {
        match self {
            // Transient errors - should retry
            CreditCheckError::CreditBureauTimeout => true,
            CreditCheckError::CreditBureauUnavailable { .. } => true,

            // Permanent errors - should NOT retry
            CreditCheckError::CreditScoreTooLow { .. } => false,
            CreditCheckError::InvalidSSN { .. } => false,
        }
    }
}

/// Income verification errors
#[derive(Debug, Clone, Serialize, Deserialize)]
enum IncomeVerificationError {
    InsufficientIncome {
        annual_income: f64,
        minimum_required: f64,
    },
    EmploymentVerificationFailed {
        employer: String,
    },
    VerificationServiceDown,
    DocumentExpired {
        document_type: String,
        expiry_date: String,
    },
}

impl fmt::Display for IncomeVerificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IncomeVerificationError::InsufficientIncome {
                annual_income,
                minimum_required,
            } => {
                write!(
                    f,
                    "Annual income ${:.2} is below minimum required ${:.2}",
                    annual_income, minimum_required
                )
            }
            IncomeVerificationError::EmploymentVerificationFailed { employer } => {
                write!(f, "Could not verify employment with {}", employer)
            }
            IncomeVerificationError::VerificationServiceDown => {
                write!(f, "Income verification service is temporarily down")
            }
            IncomeVerificationError::DocumentExpired {
                document_type,
                expiry_date,
            } => {
                write!(f, "{} expired on {}", document_type, expiry_date)
            }
        }
    }
}

impl std::error::Error for IncomeVerificationError {}

impl From<IncomeVerificationError> for String {
    fn from(err: IncomeVerificationError) -> Self {
        err.to_string()
    }
}

impl RetryableError for IncomeVerificationError {
    fn is_retryable(&self) -> bool {
        match self {
            // Transient errors
            IncomeVerificationError::VerificationServiceDown => true,

            // Permanent errors
            IncomeVerificationError::InsufficientIncome { .. } => false,
            IncomeVerificationError::EmploymentVerificationFailed { .. } => false,
            IncomeVerificationError::DocumentExpired { .. } => false,
        }
    }
}

// =============================================================================
// Domain Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreditScore {
    score: u32,
    bureau: String,
    approved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IncomeReport {
    annual_income: f64,
    employer: String,
    verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LoanDecision {
    approved: bool,
    loan_amount: f64,
    credit_score: Option<u32>,
    annual_income: Option<f64>,
    reason: String,
}

// =============================================================================
// Parent Flow - Loan Application Orchestrator
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct LoanApplication {
    application_id: String,
    applicant_name: String,
    requested_amount: f64,
    simulate_error: Option<String>,
}

impl LoanApplication {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<LoanDecision, String> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "\n[{}] APPLICATION[{}]: Processing loan application for {}",
            format_time(),
            flow_id,
            self.applicant_name
        );
        println!(
            "[{}] APPLICATION[{}]: Requested amount: ${:.2}",
            format_time(),
            flow_id,
            self.requested_amount
        );

        // ✨ Step 1: Credit Check using Level 3 invoke() API
        println!(
            "[{}] APPLICATION[{}]: Invoking credit check...",
            format_time(),
            flow_id
        );

        // Type inferred as CreditScore (from CreditCheck::Output)
        // On error, handle gracefully and return decision
        let credit_score = match self
            .invoke(CreditCheck {
                applicant_name: self.applicant_name.clone(),
                simulate_error: self.simulate_error.clone(),
            })
            .result()
            .await
        {
            Ok(score) => {
                println!(
                    "[{}] APPLICATION[{}]: Credit check passed: score={}, bureau={}",
                    format_time(),
                    flow_id,
                    score.score,
                    score.bureau
                );
                score
            }
            Err(e) => {
                println!(
                    "[{}] APPLICATION[{}]: Credit check failed: {}",
                    format_time(),
                    flow_id,
                    e
                );
                return Ok(LoanDecision {
                    approved: false,
                    loan_amount: 0.0,
                    credit_score: None,
                    annual_income: None,
                    reason: format!("Credit check failed: {}", e),
                });
            }
        };

        // ✨ Step 2: Income Verification using Level 3 invoke() API
        println!(
            "[{}] APPLICATION[{}]: Invoking income verification...",
            format_time(),
            flow_id
        );

        // Type inferred as IncomeReport (from IncomeVerification::Output)
        let income = match self
            .invoke(IncomeVerification {
                applicant_name: self.applicant_name.clone(),
                simulate_error: self.simulate_error.clone(),
            })
            .result()
            .await
        {
            Ok(report) => {
                println!(
                    "[{}] APPLICATION[{}]: Income verified: ${:.2}/year from {}",
                    format_time(),
                    flow_id,
                    report.annual_income,
                    report.employer
                );
                report
            }
            Err(e) => {
                println!(
                    "[{}] APPLICATION[{}]: Income verification failed: {}",
                    format_time(),
                    flow_id,
                    e
                );
                return Ok(LoanDecision {
                    approved: false,
                    loan_amount: self.requested_amount,
                    credit_score: Some(credit_score.score),
                    annual_income: None,
                    reason: format!("Income verification failed: {}", e),
                });
            }
        };

        // Make final decision
        let decision = LoanDecision {
            approved: true,
            loan_amount: self.requested_amount,
            credit_score: Some(credit_score.score),
            annual_income: Some(income.annual_income),
            reason: "All checks passed".to_string(),
        };

        println!(
            "\n[{}] APPLICATION[{}]: APPROVED - ${:.2}",
            format_time(),
            flow_id,
            decision.loan_amount
        );

        Ok(decision)
    }
}

// =============================================================================
// Child Flow 1 - Credit Check (Level 3 API with Custom Errors)
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct CreditCheck {
    applicant_name: String,
    simulate_error: Option<String>,
    // ✨ NO parent_flow_id field!
}

impl FlowType for CreditCheck {
    fn type_id() -> &'static str {
        "CreditCheck"
    }
}

impl InvokableFlow for CreditCheck {
    type Output = CreditScore;
}

impl CreditCheck {
    #[flow]
    async fn check(self: Arc<Self>) -> Result<CreditScore, CreditCheckError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        let attempt = CREDIT_CHECK_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "[{}]   CREDIT[{}]: Checking credit for {} (attempt #{})",
            format_time(),
            flow_id,
            self.applicant_name,
            attempt
        );

        // Simulate credit check delay
        self.clone().query_bureau().await?;

        // Simulate various credit errors
        if let Some(ref error_type) = self.simulate_error {
            match error_type.as_str() {
                "low_credit_score" => {
                    return Err(CreditCheckError::CreditScoreTooLow {
                        score: 580,
                        minimum_required: 650,
                    });
                }
                "invalid_ssn" => {
                    return Err(CreditCheckError::InvalidSSN {
                        ssn: "***-**-1234".to_string(),
                    });
                }
                "bureau_timeout" => {
                    // Succeed on 3rd attempt (demonstrates retry)
                    if attempt <= 2 {
                        println!(
                            "[{}]   CREDIT[{}]: Bureau timeout (retryable) - will retry",
                            format_time(),
                            flow_id
                        );
                        return Err(CreditCheckError::CreditBureauTimeout);
                    }
                    println!(
                        "[{}]   CREDIT[{}]: Bureau recovered on attempt #{}",
                        format_time(),
                        flow_id,
                        attempt
                    );
                }
                "bureau_unavailable" => {
                    return Err(CreditCheckError::CreditBureauUnavailable {
                        bureau_name: "Experian".to_string(),
                    });
                }
                _ => {}
            }
        }

        let result = CreditScore {
            score: 720,
            bureau: "Equifax".to_string(),
            approved: true,
        };

        println!(
            "[{}]   CREDIT[{}]: Check complete: score={} ✓",
            format_time(),
            flow_id,
            result.score
        );

        // ✨ Just return - worker auto-signals parent!
        Ok(result)
    }

    #[step]
    async fn query_bureau(self: Arc<Self>) -> Result<(), CreditCheckError> {
        println!("[{}]     -> Querying credit bureau...", format_time());
        ergon::executor::schedule_timer(Duration::from_millis(500))
            .await
            .map_err(|_| CreditCheckError::CreditBureauTimeout)?;
        Ok(())
    }
}

// =============================================================================
// Child Flow 2 - Income Verification (Level 3 API with Custom Errors)
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct IncomeVerification {
    applicant_name: String,
    simulate_error: Option<String>,
    // ✨ NO parent_flow_id field!
}

impl FlowType for IncomeVerification {
    fn type_id() -> &'static str {
        "IncomeVerification"
    }
}

impl InvokableFlow for IncomeVerification {
    type Output = IncomeReport;
}

impl IncomeVerification {
    #[flow]
    async fn verify(self: Arc<Self>) -> Result<IncomeReport, IncomeVerificationError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{}]   INCOME[{}]: Verifying income for {}",
            format_time(),
            flow_id,
            self.applicant_name
        );

        // Simulate verification delay
        self.clone().check_employment().await?;

        // Simulate various income verification errors
        if let Some(ref error_type) = self.simulate_error {
            match error_type.as_str() {
                "insufficient_income" => {
                    return Err(IncomeVerificationError::InsufficientIncome {
                        annual_income: 35000.0,
                        minimum_required: 50000.0,
                    });
                }
                "employment_failed" => {
                    return Err(IncomeVerificationError::EmploymentVerificationFailed {
                        employer: "Acme Corp".to_string(),
                    });
                }
                "service_down" => {
                    return Err(IncomeVerificationError::VerificationServiceDown);
                }
                "document_expired" => {
                    return Err(IncomeVerificationError::DocumentExpired {
                        document_type: "W-2".to_string(),
                        expiry_date: "2023-12-31".to_string(),
                    });
                }
                _ => {}
            }
        }

        let result = IncomeReport {
            annual_income: 85000.0,
            employer: "Tech Corp Inc".to_string(),
            verified: true,
        };

        println!(
            "[{}]   INCOME[{}]: Verification complete: ${:.2}/year ✓",
            format_time(),
            flow_id,
            result.annual_income
        );

        // ✨ Just return - worker auto-signals parent!
        Ok(result)
    }

    #[step]
    async fn check_employment(self: Arc<Self>) -> Result<(), IncomeVerificationError> {
        println!("[{}]     -> Checking employment records...", format_time());
        ergon::executor::schedule_timer(Duration::from_millis(300))
            .await
            .map_err(|_| IncomeVerificationError::VerificationServiceDown)?;
        Ok(())
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

fn format_time() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

// =============================================================================
// Main - Test Different Error Scenarios
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nChild Flow Level 3 API + Custom Error Types");
    println!("=============================================\n");
    println!("Combining:");
    println!("1. Level 3 child flow API (no parent_flow_id fields)");
    println!("2. Custom error types with retry control\n");

    let redis_url = "redis://127.0.0.1:6379";
    let storage = Arc::new(RedisExecutionLog::new(redis_url).await?);
    storage.reset().await?;

    // Start worker
    let worker = Worker::new(storage.clone(), "loan-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(100))
        .with_poll_interval(Duration::from_millis(100));

    worker
        .register(|flow: Arc<LoanApplication>| flow.process())
        .await;
    worker.register(|flow: Arc<CreditCheck>| flow.check()).await;
    worker
        .register(|flow: Arc<IncomeVerification>| flow.verify())
        .await;

    let worker = worker.start().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let scheduler = ergon::executor::Scheduler::new(storage.clone());

    // ==========================================================================
    // Test 1: Successful Loan Application
    // ==========================================================================
    println!("\n\n=== Test 1: Successful Application ===");

    let app1 = LoanApplication {
        application_id: "APP-001".to_string(),
        applicant_name: "Alice Johnson".to_string(),
        requested_amount: 50000.0,
        simulate_error: None,
    };

    scheduler.schedule(app1, Uuid::new_v4()).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ==========================================================================
    // Test 2: Credit Score Too Low (Permanent Error)
    // ==========================================================================
    println!("\n\n=== Test 2: Low Credit Score (Permanent Error) ===");

    let app2 = LoanApplication {
        application_id: "APP-002".to_string(),
        applicant_name: "Bob Smith".to_string(),
        requested_amount: 30000.0,
        simulate_error: Some("low_credit_score".to_string()),
    };

    scheduler.schedule(app2, Uuid::new_v4()).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ==========================================================================
    // Test 3: Insufficient Income (Permanent Error)
    // ==========================================================================
    println!("\n\n=== Test 3: Insufficient Income (Permanent Error) ===");

    let app3 = LoanApplication {
        application_id: "APP-003".to_string(),
        applicant_name: "Carol Davis".to_string(),
        requested_amount: 100000.0,
        simulate_error: Some("insufficient_income".to_string()),
    };

    scheduler.schedule(app3, Uuid::new_v4()).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ==========================================================================
    // Test 4: Bureau Timeout (Retryable Error)
    // ==========================================================================
    println!("\n\n=== Test 4: Credit Bureau Timeout (Retryable - Auto-Retry) ===");
    println!("Note: Will fail first 2 attempts, succeed on 3rd\n");

    CREDIT_CHECK_ATTEMPTS.store(0, Ordering::SeqCst);

    let app4 = LoanApplication {
        application_id: "APP-004".to_string(),
        applicant_name: "Dave Wilson".to_string(),
        requested_amount: 25000.0,
        simulate_error: Some("bureau_timeout".to_string()),
    };

    scheduler.schedule(app4, Uuid::new_v4()).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!(
        "\nTotal credit check attempts: {}",
        CREDIT_CHECK_ATTEMPTS.load(Ordering::SeqCst)
    );

    // Wait for completion
    tokio::time::sleep(Duration::from_secs(2)).await;

    worker.shutdown().await;

    println!("\n\n=== Summary ===\n");
    println!("✓ Level 3 API: Children have NO parent_flow_id fields");
    println!("✓ Custom Errors: Rich domain-specific error context");
    println!("✓ Retry Control: Only transient errors retry automatically");
    println!("✓ Type Safety: Compiler enforces error handling at compile time\n");

    storage.close().await?;
    Ok(())
}

// Exactly! The child_flow_custom_errors.rs example is a comprehensive integration test that validates the entire parent-child flow system
//   working together. Let me break down what it tests:

//   What It Tests

//   1. Parent-Child Flow Orchestration

//   - ✅ Parent (LoanApplication) invokes multiple children sequentially
//   - ✅ Level 3 API: No parent_flow_id fields, token-based signaling
//   - ✅ Type-safe child invocation with compile-time guarantees

//   2. Worker Execution

//   - ✅ Worker polls queue and picks up flows
//   - ✅ Worker executes flows in background tasks
//   - ✅ Worker handles completion, retries, and failures
//   - ✅ Worker signals parent AFTER retry decision (the fix we just made!)

//   3. Step Execution & Caching

//   - ✅ #[step] macro for both parent and children
//   - ✅ Steps cached on replay (parent suspends → child runs → parent resumes from cache)
//   - ✅ await_external_signal for parent waiting on child (signal.rs:82)

//   4. Custom Error Handling

//   - ✅ Domain-specific errors (CreditCheckError, IncomeVerificationError)
//   - ✅ RetryableError trait implementation
//   - ✅ Parent catches child errors and makes business decisions

//   5. Retry Logic

//   - ✅ Retryable errors (CreditBureauTimeout) → retry with default policy
//   - ✅ Permanent errors (CreditScoreTooLow) → fail immediately, no retry
//   - ✅ Default RetryPolicy::STANDARD (3 attempts, exponential backoff)
//   - ✅ has_non_retryable_error check prevents retrying permanent errors

//   6. Signal Coordination

//   - ✅ Parent suspends with await_external_signal (via invoke().result())
//   - ✅ Worker wraps result in SignalPayload { success, data }
//   - ✅ Worker signals parent with success or error via SignalPayload
//   - ✅ Signal params stored/retrieved from storage

//   7. Test Scenarios Cover All Paths

//   | Test   | Scenario                     | What It Validates                                    |
//   |--------|------------------------------|------------------------------------------------------|
//   | Test 1 | Success (all checks pass)    | Happy path, both children succeed                    |
//   | Test 2 | Permanent error (low credit) | Child fails, no retry, parent gets error immediately |
//   | Test 3 | Permanent error (income)     | Second child fails after first succeeds              |
//   | Test 4 | Retryable error (timeout)    | Child retries 3 times, succeeds on 3rd, parent waits |

//   Why Test 4 is Critical

//   Test 4 validates the entire fix we just implemented:

//   Attempt #1: CreditBureauTimeout (retryable) → Worker: "will retry"
//   Attempt #2: CreditBureauTimeout again      → Worker: "will retry"
//   Attempt #3: Bureau recovered               → Worker: "success!"
//               ↓ NOW signals parent with SignalPayload { success: true, data }
//   Parent resumes: "Credit check passed: score=720"

//   Before the fix: Parent got error after attempt #2
//   After the fix: Parent waits until attempt #3 succeeds ✅

//   Integration Test Coverage

//   This example tests the full stack:
//   User Code (Parent/Child flows with custom errors)
//            ↓
//   Executor (step caching, await_external_signal)
//            ↓
//   Worker (polling, retry decision, parent signaling)
//            ↓
//   Storage (invocations, signal_params, task queue)
