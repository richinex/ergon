//! Child Flow with Timers and Custom Error Types
//!
//! This example combines three advanced features:
//! 1. **Level 3 child flow API** - Token-based type-safe invocation
//! 2. **Custom error types** - Rich, domain-specific errors with retry control
//! 3. **Durable timers** - Time-based delays and timeouts
//!
//! ## Scenario: Loan Application Processing with SLA
//!
//! A loan approval system with service-level agreements (SLAs):
//! - Parent: LoanApplication orchestrator with 30s total timeout
//! - Child 1: CreditCheck (3s processing delay, can timeout)
//! - Child 2: IncomeVerification (2s processing delay, can timeout)
//! - Child 3: FraudCheck (4s processing delay, can timeout)
//!
//! Each child has realistic delays and can fail with custom errors:
//! - **Retryable**: ServiceTimeout, BureauUnavailable, NetworkError
//! - **Permanent**: CreditScoreTooLow, InsufficientIncome, FraudDetected, InvalidSSN
//!
//! ## Key Features
//!
//! 1. **Timers in child flows**: Simulate real-world processing delays
//! 2. **Timeout handling**: Parent enforces SLA timeouts on children
//! 3. **Custom errors**: Rich context with retry control
//! 4. **Type safety**: Compiler enforces error handling
//! 5. **Durable execution**: Timers survive worker restarts
//!
//! ## Run
//!
//! ```bash
//! cargo run --example child_flow_timers --features=sqlite
//! ```

use chrono::Utc;
use ergon::core::{FlowType, InvokableFlow};
use ergon::executor::{schedule_timer_named, InvokeChild, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// =============================================================================
// Custom Error Types
// =============================================================================

/// Credit check errors
#[derive(Debug, Clone, Serialize, Deserialize)]
enum CreditCheckError {
    CreditScoreTooLow { score: u32, minimum_required: u32 },
    InvalidSSN { ssn: String },
    CreditBureauTimeout,
    CreditBureauUnavailable { bureau_name: String },
    ProcessingDelay,
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
            CreditCheckError::ProcessingDelay => {
                write!(f, "Processing delayed - bureau overloaded")
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
            CreditCheckError::ProcessingDelay => true,

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
    ProcessingTimeout,
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
            IncomeVerificationError::ProcessingTimeout => {
                write!(f, "Verification processing timed out")
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
            IncomeVerificationError::ProcessingTimeout => true,

            // Permanent errors
            IncomeVerificationError::InsufficientIncome { .. } => false,
            IncomeVerificationError::EmploymentVerificationFailed { .. } => false,
            IncomeVerificationError::DocumentExpired { .. } => false,
        }
    }
}

/// Fraud check errors
#[derive(Debug, Clone, Serialize, Deserialize)]
enum FraudCheckError {
    FraudDetected { risk_score: u32, reason: String },
    SuspiciousActivity { activity_type: String },
    FraudServiceTimeout,
    FraudServiceUnavailable,
}

impl fmt::Display for FraudCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FraudCheckError::FraudDetected { risk_score, reason } => {
                write!(f, "Fraud detected (risk score: {}): {}", risk_score, reason)
            }
            FraudCheckError::SuspiciousActivity { activity_type } => {
                write!(f, "Suspicious activity detected: {}", activity_type)
            }
            FraudCheckError::FraudServiceTimeout => {
                write!(f, "Fraud check service timed out")
            }
            FraudCheckError::FraudServiceUnavailable => {
                write!(f, "Fraud check service is unavailable")
            }
        }
    }
}

impl std::error::Error for FraudCheckError {}

impl From<FraudCheckError> for String {
    fn from(err: FraudCheckError) -> Self {
        err.to_string()
    }
}

impl RetryableError for FraudCheckError {
    fn is_retryable(&self) -> bool {
        match self {
            // Transient errors
            FraudCheckError::FraudServiceTimeout => true,
            FraudCheckError::FraudServiceUnavailable => true,

            // Permanent errors
            FraudCheckError::FraudDetected { .. } => false,
            FraudCheckError::SuspiciousActivity { .. } => false,
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
struct FraudReport {
    risk_score: u32,
    cleared: bool,
    checked_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LoanDecision {
    approved: bool,
    loan_amount: f64,
    credit_score: Option<u32>,
    annual_income: Option<f64>,
    fraud_cleared: Option<bool>,
    reason: String,
    processing_time_ms: u64,
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

        let start_time = std::time::Instant::now();

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

        // ✨ Step 1: Credit Check (with 3s timer inside)
        println!(
            "[{}] APPLICATION[{}]: Invoking credit check...",
            format_time(),
            flow_id
        );

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
                    "[{}] APPLICATION[{}]: ✓ Credit check passed: score={}, bureau={}",
                    format_time(),
                    flow_id,
                    score.score,
                    score.bureau
                );
                score
            }
            Err(e) => {
                println!(
                    "[{}] APPLICATION[{}]: ✗ Credit check failed: {}",
                    format_time(),
                    flow_id,
                    e
                );
                return Ok(LoanDecision {
                    approved: false,
                    loan_amount: 0.0,
                    credit_score: None,
                    annual_income: None,
                    fraud_cleared: None,
                    reason: format!("Credit check failed: {}", e),
                    processing_time_ms: start_time.elapsed().as_millis() as u64,
                });
            }
        };

        // ✨ Step 2: Income Verification (with 2s timer inside)
        println!(
            "[{}] APPLICATION[{}]: Invoking income verification...",
            format_time(),
            flow_id
        );

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
                    "[{}] APPLICATION[{}]: ✓ Income verified: ${:.2}/year from {}",
                    format_time(),
                    flow_id,
                    report.annual_income,
                    report.employer
                );
                report
            }
            Err(e) => {
                println!(
                    "[{}] APPLICATION[{}]: ✗ Income verification failed: {}",
                    format_time(),
                    flow_id,
                    e
                );
                return Ok(LoanDecision {
                    approved: false,
                    loan_amount: self.requested_amount,
                    credit_score: Some(credit_score.score),
                    annual_income: None,
                    fraud_cleared: None,
                    reason: format!("Income verification failed: {}", e),
                    processing_time_ms: start_time.elapsed().as_millis() as u64,
                });
            }
        };

        // ✨ Step 3: Fraud Check (with 4s timer inside)
        println!(
            "[{}] APPLICATION[{}]: Invoking fraud check...",
            format_time(),
            flow_id
        );

        let fraud = match self
            .invoke(FraudCheck {
                applicant_name: self.applicant_name.clone(),
                simulate_error: self.simulate_error.clone(),
            })
            .result()
            .await
        {
            Ok(report) => {
                println!(
                    "[{}] APPLICATION[{}]: ✓ Fraud check passed: risk_score={}",
                    format_time(),
                    flow_id,
                    report.risk_score
                );
                report
            }
            Err(e) => {
                println!(
                    "[{}] APPLICATION[{}]: ✗ Fraud check failed: {}",
                    format_time(),
                    flow_id,
                    e
                );
                return Ok(LoanDecision {
                    approved: false,
                    loan_amount: self.requested_amount,
                    credit_score: Some(credit_score.score),
                    annual_income: Some(income.annual_income),
                    fraud_cleared: None,
                    reason: format!("Fraud check failed: {}", e),
                    processing_time_ms: start_time.elapsed().as_millis() as u64,
                });
            }
        };

        // All checks passed - approve loan
        let decision = LoanDecision {
            approved: true,
            loan_amount: self.requested_amount,
            credit_score: Some(credit_score.score),
            annual_income: Some(income.annual_income),
            fraud_cleared: Some(fraud.cleared),
            reason: "All checks passed".to_string(),
            processing_time_ms: start_time.elapsed().as_millis() as u64,
        };

        println!(
            "\n[{}] APPLICATION[{}]: ✓ APPROVED - ${:.2} (processed in {}ms)",
            format_time(),
            flow_id,
            decision.loan_amount,
            decision.processing_time_ms
        );

        Ok(decision)
    }
}

// =============================================================================
// Child Flow 1 - Credit Check (with Timer)
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct CreditCheck {
    applicant_name: String,
    simulate_error: Option<String>,
}

impl FlowType for CreditCheck {
    fn type_id() -> &'static str {
        "CreditCheck"
    }
}

impl InvokableFlow for CreditCheck {
    type Output = CreditScore;
}

static CREDIT_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

impl CreditCheck {
    #[flow]
    async fn check(self: Arc<Self>) -> Result<CreditScore, CreditCheckError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        let attempt = CREDIT_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "[{}]   CREDIT[{}]: Checking credit for {} (attempt #{})",
            format_time(),
            flow_id,
            self.applicant_name,
            attempt
        );

        // ✨ Wait for credit bureau processing (3 seconds)
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
                    if attempt <= 2 {
                        println!(
                            "[{}]   CREDIT[{}]: Bureau timeout (retryable) - will retry",
                            format_time(),
                            flow_id
                        );
                        return Err(CreditCheckError::CreditBureauTimeout);
                    }
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

        Ok(result)
    }

    #[step]
    async fn query_bureau(self: Arc<Self>) -> Result<(), CreditCheckError> {
        println!(
            "[{}]     -> Querying credit bureau (3s delay)...",
            format_time()
        );

        // ✨ Timer: Simulate credit bureau processing time
        schedule_timer_named(Duration::from_secs(3), "credit-bureau-query")
            .await
            .map_err(|_| CreditCheckError::CreditBureauTimeout)?;

        println!("[{}]     -> Bureau query complete", format_time());
        Ok(())
    }
}

// =============================================================================
// Child Flow 2 - Income Verification (with Timer)
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct IncomeVerification {
    applicant_name: String,
    simulate_error: Option<String>,
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

        // ✨ Wait for employment verification (2 seconds)
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

        Ok(result)
    }

    #[step]
    async fn check_employment(self: Arc<Self>) -> Result<(), IncomeVerificationError> {
        println!(
            "[{}]     -> Checking employment records (2s delay)...",
            format_time()
        );

        // ✨ Timer: Simulate employment verification time
        schedule_timer_named(Duration::from_secs(2), "employment-check")
            .await
            .map_err(|_| IncomeVerificationError::ProcessingTimeout)?;

        println!("[{}]     -> Employment check complete", format_time());
        Ok(())
    }
}

// =============================================================================
// Child Flow 3 - Fraud Check (with Timer)
// =============================================================================

#[derive(Clone, Serialize, Deserialize)]
struct FraudCheck {
    applicant_name: String,
    simulate_error: Option<String>,
}

impl FlowType for FraudCheck {
    fn type_id() -> &'static str {
        "FraudCheck"
    }
}

impl InvokableFlow for FraudCheck {
    type Output = FraudReport;
}

impl FraudCheck {
    #[flow]
    async fn scan(self: Arc<Self>) -> Result<FraudReport, FraudCheckError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{}]   FRAUD[{}]: Scanning for fraud: {}",
            format_time(),
            flow_id,
            self.applicant_name
        );

        // ✨ Wait for fraud analysis (4 seconds)
        self.clone().analyze_patterns().await?;

        // Simulate fraud errors
        if let Some(ref error_type) = self.simulate_error {
            match error_type.as_str() {
                "fraud_detected" => {
                    return Err(FraudCheckError::FraudDetected {
                        risk_score: 85,
                        reason: "Multiple applications from same IP".to_string(),
                    });
                }
                "suspicious_activity" => {
                    return Err(FraudCheckError::SuspiciousActivity {
                        activity_type: "Rapid application submissions".to_string(),
                    });
                }
                "service_timeout" => {
                    return Err(FraudCheckError::FraudServiceTimeout);
                }
                _ => {}
            }
        }

        let result = FraudReport {
            risk_score: 12,
            cleared: true,
            checked_at: Utc::now().to_rfc3339(),
        };

        println!(
            "[{}]   FRAUD[{}]: Scan complete: risk_score={} ✓",
            format_time(),
            flow_id,
            result.risk_score
        );

        Ok(result)
    }

    #[step]
    async fn analyze_patterns(self: Arc<Self>) -> Result<(), FraudCheckError> {
        println!(
            "[{}]     -> Analyzing fraud patterns (4s delay)...",
            format_time()
        );

        // ✨ Timer: Simulate fraud analysis time
        schedule_timer_named(Duration::from_secs(4), "fraud-analysis")
            .await
            .map_err(|_| FraudCheckError::FraudServiceTimeout)?;

        println!("[{}]     -> Fraud analysis complete", format_time());
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
// Main - Test Different Scenarios
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nChild Flow with Timers + Custom Error Types");
    println!("=============================================\n");
    println!("Combining:");
    println!("1. Level 3 child flow API (no parent_flow_id fields)");
    println!("2. Custom error types with retry control");
    println!("3. Durable timers for realistic delays\n");

    let storage = Arc::new(SqliteExecutionLog::new("child_timers.db").await?);
    storage.reset().await?;

    // Start worker with timer processing enabled
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
    worker.register(|flow: Arc<FraudCheck>| flow.scan()).await;

    let worker = worker.start().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let scheduler = ergon::executor::Scheduler::new(storage.clone());

    // ==========================================================================
    // Test 1: Successful Loan Application (all timers complete)
    // ==========================================================================
    println!("\n=== Test 1: Successful Application (3s + 2s + 4s = ~9s total) ===");

    let app1 = LoanApplication {
        application_id: "APP-001".to_string(),
        applicant_name: "Alice Johnson".to_string(),
        requested_amount: 50000.0,
        simulate_error: None,
    };

    scheduler.schedule(app1, Uuid::new_v4()).await?;
    tokio::time::sleep(Duration::from_secs(15)).await; // Wait for all 3 children + buffer

    // ==========================================================================
    // Test 2: Credit Score Too Low (Permanent Error - No Retry)
    // ==========================================================================
    println!("\n\n=== Test 2: Low Credit Score (Permanent Error - Fails Fast) ===");

    let app2 = LoanApplication {
        application_id: "APP-002".to_string(),
        applicant_name: "Bob Smith".to_string(),
        requested_amount: 30000.0,
        simulate_error: Some("low_credit_score".to_string()),
    };

    scheduler.schedule(app2, Uuid::new_v4()).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ==========================================================================
    // Test 3: Bureau Timeout (Retryable - Auto-Retry)
    // ==========================================================================
    println!("\n\n=== Test 3: Credit Bureau Timeout (Retryable - Auto-Retry) ===");
    println!("Note: Will fail first 2 attempts, succeed on 3rd\n");

    CREDIT_ATTEMPTS.store(0, Ordering::SeqCst);

    let app3 = LoanApplication {
        application_id: "APP-003".to_string(),
        applicant_name: "Carol Davis".to_string(),
        requested_amount: 25000.0,
        simulate_error: Some("bureau_timeout".to_string()),
    };

    scheduler.schedule(app3, Uuid::new_v4()).await?;
    tokio::time::sleep(Duration::from_secs(15)).await;

    println!(
        "\nTotal credit check attempts: {}",
        CREDIT_ATTEMPTS.load(Ordering::SeqCst)
    );

    // ==========================================================================
    // Test 4: Fraud Detected (Permanent Error after all timers)
    // ==========================================================================
    println!("\n\n=== Test 4: Fraud Detected (Permanent - After All Checks) ===");

    let app4 = LoanApplication {
        application_id: "APP-004".to_string(),
        applicant_name: "Dave Wilson".to_string(),
        requested_amount: 100000.0,
        simulate_error: Some("fraud_detected".to_string()),
    };

    scheduler.schedule(app4, Uuid::new_v4()).await?;
    tokio::time::sleep(Duration::from_secs(12)).await;

    // Wait for completion
    tokio::time::sleep(Duration::from_secs(2)).await;

    worker.shutdown().await;

    println!("\n\n=== Summary ===\n");
    println!("✓ Level 3 API: Children have NO parent_flow_id fields");
    println!("✓ Timers: Each child has realistic processing delays");
    println!("  - Credit check: 3s timer");
    println!("  - Income verification: 2s timer");
    println!("  - Fraud check: 4s timer");
    println!("✓ Custom Errors: Rich domain-specific error context");
    println!("✓ Retry Control: Only transient errors retry automatically");
    println!("✓ Durable Execution: Timers survive worker restarts\n");

    storage.close().await?;
    Ok(())
}
