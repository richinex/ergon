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
use dashmap::DashMap;
use ergon::core::{FlowType, InvokableFlow};
use ergon::executor::{schedule_timer_named, InvokeChild, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
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

impl From<ExecutionError> for CreditCheckError {
    fn from(e: ExecutionError) -> Self {
        match e {
            // Timer suspension or timeout
            ExecutionError::Suspended(_) => CreditCheckError::CreditBureauTimeout,
            // Framework errors map to retryable infrastructure issues
            ExecutionError::Core(_) | ExecutionError::Failed(_) => {
                CreditCheckError::ProcessingDelay
            }
            // Non-retryable errors
            ExecutionError::NonRetryable(msg) => {
                if msg.contains("SSN") {
                    CreditCheckError::InvalidSSN { ssn: msg }
                } else {
                    CreditCheckError::CreditScoreTooLow {
                        score: 0,
                        minimum_required: 600,
                    }
                }
            }
            _ => CreditCheckError::ProcessingDelay,
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

impl From<ExecutionError> for IncomeVerificationError {
    fn from(e: ExecutionError) -> Self {
        match e {
            // Timer suspension or timeout
            ExecutionError::Suspended(_) => IncomeVerificationError::ProcessingTimeout,
            // Framework errors map to retryable infrastructure issues
            ExecutionError::Core(_) | ExecutionError::Failed(_) => {
                IncomeVerificationError::VerificationServiceDown
            }
            // Non-retryable errors
            ExecutionError::NonRetryable(msg) => {
                if msg.contains("income") {
                    IncomeVerificationError::InsufficientIncome {
                        annual_income: 0.0,
                        minimum_required: 50000.0,
                    }
                } else if msg.contains("employment") {
                    IncomeVerificationError::EmploymentVerificationFailed {
                        employer: "Unknown".to_string(),
                    }
                } else {
                    IncomeVerificationError::DocumentExpired {
                        document_type: "Unknown".to_string(),
                        expiry_date: "Unknown".to_string(),
                    }
                }
            }
            _ => IncomeVerificationError::VerificationServiceDown,
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

impl From<ExecutionError> for FraudCheckError {
    fn from(e: ExecutionError) -> Self {
        match e {
            // Timer suspension or timeout
            ExecutionError::Suspended(_) => FraudCheckError::FraudServiceTimeout,
            // Framework errors map to retryable infrastructure issues
            ExecutionError::Core(_) | ExecutionError::Failed(_) => {
                FraudCheckError::FraudServiceUnavailable
            }
            // Non-retryable errors
            ExecutionError::NonRetryable(msg) => {
                if msg.contains("fraud") || msg.contains("suspicious") {
                    FraudCheckError::FraudDetected {
                        risk_score: 100,
                        reason: msg,
                    }
                } else {
                    FraudCheckError::SuspiciousActivity {
                        activity_type: msg,
                    }
                }
            }
            _ => FraudCheckError::FraudServiceUnavailable,
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
                // Credit check failed - decline loan
                let reason = format!("Credit check failed: {}", e);
                println!(
                    "\n[{}] APPLICATION[{}]: ✗ DECLINED - {}",
                    format_time(),
                    flow_id,
                    reason
                );
                return Ok(LoanDecision {
                    approved: false,
                    loan_amount: 0.0,
                    credit_score: None,
                    annual_income: None,
                    fraud_cleared: None,
                    reason,
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
                // Income verification failed - decline loan
                let reason = format!("Income verification failed: {}", e);
                println!(
                    "\n[{}] APPLICATION[{}]: ✗ DECLINED - {}",
                    format_time(),
                    flow_id,
                    reason
                );
                return Ok(LoanDecision {
                    approved: false,
                    loan_amount: self.requested_amount,
                    credit_score: Some(credit_score.score),
                    annual_income: None,
                    fraud_cleared: None,
                    reason,
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
                // Fraud check failed - decline loan
                let reason = format!("Fraud check failed: {}", e);
                println!(
                    "\n[{}] APPLICATION[{}]: ✗ DECLINED - {}",
                    format_time(),
                    flow_id,
                    reason
                );
                return Ok(LoanDecision {
                    approved: false,
                    loan_amount: self.requested_amount,
                    credit_score: Some(credit_score.score),
                    annual_income: Some(income.annual_income),
                    fraud_cleared: None,
                    reason,
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
// Per-Applicant Attempt Tracking
// =============================================================================

/// Per-applicant attempt counters (each applicant tracks its own retry attempts)
static APPLICANT_ATTEMPTS: LazyLock<DashMap<String, AtomicU32>> = LazyLock::new(DashMap::new);

/// Increment and return credit check attempt counter for an applicant
fn inc_credit_attempt(applicant_name: &str) -> u32 {
    // Fast path: use read lock if entry exists
    if let Some(counter) = APPLICANT_ATTEMPTS.get(applicant_name) {
        return counter.fetch_add(1, Ordering::Relaxed) + 1;
    }

    // Slow path: create entry with write lock only on first access
    APPLICANT_ATTEMPTS
        .entry(applicant_name.to_string())
        .or_insert_with(|| AtomicU32::new(0))
        .fetch_add(1, Ordering::Relaxed)
        + 1
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

impl CreditCheck {
    #[flow]
    async fn check(self: Arc<Self>) -> Result<CreditScore, CreditCheckError> {
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{}]   CREDIT[{}]: Checking credit for {}",
            format_time(),
            flow_id,
            self.applicant_name
        );

        // ✨ Wait for credit bureau processing (3 seconds)
        let attempt = self.clone().query_bureau().await?;

        // Simulate various credit errors
        if let Some(ref error_type) = self.simulate_error {
            match error_type.as_str() {
                "low_credit_score" => {
                    println!(
                        "[{}]   CREDIT[{}]: ✗ Credit score too low - score=580, required=650",
                        format_time(),
                        flow_id
                    );
                    return Err(CreditCheckError::CreditScoreTooLow {
                        score: 580,
                        minimum_required: 650,
                    });
                }
                "invalid_ssn" => {
                    println!(
                        "[{}]   CREDIT[{}]: ✗ Invalid SSN",
                        format_time(),
                        flow_id
                    );
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
                    println!(
                        "[{}]   CREDIT[{}]: ✗ Bureau unavailable - Experian",
                        format_time(),
                        flow_id
                    );
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
    async fn query_bureau(self: Arc<Self>) -> Result<u32, CreditCheckError> {
        // Increment counter INSIDE the cached step (only runs once per actual execution)
        let attempt = inc_credit_attempt(&self.applicant_name);

        println!(
            "[{}]     -> Querying credit bureau (attempt #{}, 3s delay)...",
            format_time(),
            attempt
        );

        // ✨ Timer: Simulate credit bureau processing time
        schedule_timer_named(Duration::from_secs(3), "credit-bureau-query")
            .await
            .map_err(CreditCheckError::from)?;

        println!(
            "[{}]     -> Bureau query complete (attempt #{})",
            format_time(),
            attempt
        );
        Ok(attempt)
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
                    println!(
                        "[{}]   INCOME[{}]: ✗ Insufficient income - $35000/year, required=$50000/year",
                        format_time(),
                        flow_id
                    );
                    return Err(IncomeVerificationError::InsufficientIncome {
                        annual_income: 35000.0,
                        minimum_required: 50000.0,
                    });
                }
                "employment_failed" => {
                    println!(
                        "[{}]   INCOME[{}]: ✗ Employment verification failed - Acme Corp",
                        format_time(),
                        flow_id
                    );
                    return Err(IncomeVerificationError::EmploymentVerificationFailed {
                        employer: "Acme Corp".to_string(),
                    });
                }
                "service_down" => {
                    println!(
                        "[{}]   INCOME[{}]: ✗ Verification service down (retryable)",
                        format_time(),
                        flow_id
                    );
                    return Err(IncomeVerificationError::VerificationServiceDown);
                }
                "document_expired" => {
                    println!(
                        "[{}]   INCOME[{}]: ✗ Document expired - W-2 expired on 2023-12-31",
                        format_time(),
                        flow_id
                    );
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
            .map_err(IncomeVerificationError::from)?;

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
                    println!(
                        "[{}]   FRAUD[{}]: ✗ FRAUD DETECTED - risk_score=95",
                        format_time(),
                        flow_id
                    );
                    return Err(FraudCheckError::FraudDetected {
                        risk_score: 95,
                        reason: "Multiple applications from same IP".to_string(),
                    });
                }
                "suspicious_activity" => {
                    println!(
                        "[{}]   FRAUD[{}]: ✗ Suspicious activity detected",
                        format_time(),
                        flow_id
                    );
                    return Err(FraudCheckError::SuspiciousActivity {
                        activity_type: "Rapid application submissions".to_string(),
                    });
                }
                "service_timeout" => {
                    println!(
                        "[{}]   FRAUD[{}]: ✗ Service timeout",
                        format_time(),
                        flow_id
                    );
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
            .map_err(FraudCheckError::from)?;

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
    println!("\n=== Test 1: Successful Application ===\n");

    let app1 = LoanApplication {
        application_id: "APP-001".to_string(),
        applicant_name: "Alice Johnson".to_string(),
        requested_amount: 50000.0,
        simulate_error: None,
    };

    let task_id_1 = scheduler.schedule(app1, Uuid::new_v4()).await?;

    // Wait for flow to complete (with timeout)
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(20) {
            println!("\n[TIMEOUT] Test 1 did not complete within 20 seconds");
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

    // ==========================================================================
    // Test 2: Credit Score Too Low (Permanent Error - No Retry)
    // ==========================================================================
    println!("\n\n=== Test 2: Low Credit Score ===\n");

    let app2 = LoanApplication {
        application_id: "APP-002".to_string(),
        applicant_name: "Bob Smith".to_string(),
        requested_amount: 30000.0,
        simulate_error: Some("low_credit_score".to_string()),
    };

    let task_id_2 = scheduler.schedule(app2, Uuid::new_v4()).await?;

    // Wait for flow to complete (with timeout)
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            println!("\n[TIMEOUT] Test 2 did not complete within 10 seconds");
            break;
        }
        match storage.get_scheduled_flow(task_id_2).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                    break;
                }
            }
            None => break,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ==========================================================================
    // Test 3: Bureau Timeout (Retryable - Auto-Retry)
    // ==========================================================================
    println!("\n\n=== Test 3: Credit Bureau Timeout ===\n");

    let app3 = LoanApplication {
        application_id: "APP-003".to_string(),
        applicant_name: "Carol Davis".to_string(),
        requested_amount: 25000.0,
        simulate_error: Some("bureau_timeout".to_string()),
    };

    let task_id_3 = scheduler.schedule(app3, Uuid::new_v4()).await?;

    // Wait for flow to complete (with timeout)
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(25) {
            println!("\n[TIMEOUT] Test 3 did not complete within 25 seconds");
            break;
        }
        match storage.get_scheduled_flow(task_id_3).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                    break;
                }
            }
            None => break,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Show per-applicant attempt counts
    if let Some(attempts) = APPLICANT_ATTEMPTS.get("Carol Davis") {
        println!(
            "\nCarol Davis credit check attempts: {}",
            attempts.load(Ordering::SeqCst)
        );
    }

    // ==========================================================================
    // Test 4: Fraud Detected (Permanent Error after all timers)
    // ==========================================================================
    println!("\n\n=== Test 4: Fraud Detected ===\n");

    let app4 = LoanApplication {
        application_id: "APP-004".to_string(),
        applicant_name: "Dave Wilson".to_string(),
        requested_amount: 100000.0,
        simulate_error: Some("fraud_detected".to_string()),
    };

    let task_id_4 = scheduler.schedule(app4, Uuid::new_v4()).await?;

    // Wait for flow to complete (with timeout - needs longer for all child flow timers)
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(30) {
            println!("\n[TIMEOUT] Test 4 did not complete within 30 seconds");
            break;
        }
        match storage.get_scheduled_flow(task_id_4).await? {
            Some(scheduled) => {
                if matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                    break;
                }
            }
            None => break,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("\n=== All Tests Complete ===\n");

    // Give worker time to finish any pending work
    tokio::time::sleep(Duration::from_millis(500)).await;

    worker.shutdown().await;

    storage.close().await?;
    Ok(())
}

//   ergon git:(modularize-worker) ✗ cargo run --example child_flow_timers
//    Compiling ergon v0.1.0 (/home/richinex/Documents/devs/rust_projects/ergon/ergon)
//     Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.24s
//      Running `target/debug/examples/child_flow_timers`

// Child Flow with Timers + Custom Error Types
// =============================================

// Combining:
// 1. Level 3 child flow API (no parent_flow_id fields)
// 2. Custom error types with retry control
// 3. Durable timers for realistic delays

// === Test 1: Successful Application ===

// [09:04:43.349] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Processing loan application for Alice Johnson
// [09:04:43.349] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Requested amount: $50000.00
// [09:04:43.349] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Invoking credit check...
// [09:04:43.351] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: ✗ Credit check failed: execution failed: Flow suspended for signal
// [09:04:43.454]   CREDIT[17f573c7-ff03-538b-a7a5-5653e0091d40]: Checking credit for Alice Johnson (attempt #1)
// [09:04:43.455]     -> Querying credit bureau (3s delay)...
// [09:04:47.465]   CREDIT[17f573c7-ff03-538b-a7a5-5653e0091d40]: Checking credit for Alice Johnson (attempt #2)
// [09:04:47.466]   CREDIT[17f573c7-ff03-538b-a7a5-5653e0091d40]: Check complete: score=720 ✓

// [09:04:47.571] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Processing loan application for Alice Johnson
// [09:04:47.571] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Requested amount: $50000.00
// [09:04:47.571] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Invoking credit check...
// [09:04:47.573] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: ✓ Credit check passed: score=720, bureau=Equifax
// [09:04:47.573] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Invoking income verification...
// [09:04:47.574] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: ✗ Income verification failed: execution failed: Flow suspended for signal
// [09:04:47.675]   INCOME[7bb305bc-ef33-57a4-820d-4b04b3b0e7c3]: Verifying income for Alice Johnson
// [09:04:47.676]     -> Checking employment records (2s delay)...
// [09:04:50.687]   INCOME[7bb305bc-ef33-57a4-820d-4b04b3b0e7c3]: Verifying income for Alice Johnson
// [09:04:50.687]   INCOME[7bb305bc-ef33-57a4-820d-4b04b3b0e7c3]: Verification complete: $85000.00/year ✓

// [09:04:50.793] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Processing loan application for Alice Johnson
// [09:04:50.793] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Requested amount: $50000.00
// [09:04:50.793] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Invoking credit check...
// [09:04:50.794] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: ✓ Credit check passed: score=720, bureau=Equifax
// [09:04:50.794] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Invoking income verification...
// [09:04:50.795] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: ✓ Income verified: $85000.00/year from Tech Corp Inc
// [09:04:50.795] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Invoking fraud check...
// [09:04:50.797] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: ✗ Fraud check failed: execution failed: Flow suspended for signal
// [09:04:50.897]   FRAUD[cd1f56b8-4044-5e5b-a43f-a07a9f2b89d4]: Scanning for fraud: Alice Johnson
// [09:04:50.898]     -> Analyzing fraud patterns (4s delay)...
// [09:04:55.913]   FRAUD[cd1f56b8-4044-5e5b-a43f-a07a9f2b89d4]: Scanning for fraud: Alice Johnson
// [09:04:55.913]   FRAUD[cd1f56b8-4044-5e5b-a43f-a07a9f2b89d4]: Scan complete: risk_score=12 ✓

// [09:04:56.017] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Processing loan application for Alice Johnson
// [09:04:56.017] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Requested amount: $50000.00
// [09:04:56.017] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Invoking credit check...
// [09:04:56.018] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: ✓ Credit check passed: score=720, bureau=Equifax
// [09:04:56.018] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Invoking income verification...
// [09:04:56.019] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: ✓ Income verified: $85000.00/year from Tech Corp Inc
// [09:04:56.019] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: Invoking fraud check...
// [09:04:56.020] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: ✓ Fraud check passed: risk_score=12

// [09:04:56.020] APPLICATION[64ce6852-91d7-4a8b-8cbe-138ffdfe2768]: ✓ APPROVED - $50000.00 (processed in 2ms)

// === Test 2: Low Credit Score ===

// [09:04:58.350] APPLICATION[c0b6444e-80aa-44d7-90bd-8b784fbfdfc7]: Processing loan application for Bob Smith
// [09:04:58.350] APPLICATION[c0b6444e-80aa-44d7-90bd-8b784fbfdfc7]: Requested amount: $30000.00
// [09:04:58.350] APPLICATION[c0b6444e-80aa-44d7-90bd-8b784fbfdfc7]: Invoking credit check...
// [09:04:58.351] APPLICATION[c0b6444e-80aa-44d7-90bd-8b784fbfdfc7]: ✗ Credit check failed: execution failed: Flow suspended for signal
// [09:04:58.455]   CREDIT[bc308160-e503-5654-9bf3-93f977302c03]: Checking credit for Bob Smith (attempt #3)
// [09:04:58.456]     -> Querying credit bureau (3s delay)...
// [09:05:02.465]   CREDIT[bc308160-e503-5654-9bf3-93f977302c03]: Checking credit for Bob Smith (attempt #4)

// [09:05:02.571] APPLICATION[c0b6444e-80aa-44d7-90bd-8b784fbfdfc7]: Processing loan application for Bob Smith
// [09:05:02.571] APPLICATION[c0b6444e-80aa-44d7-90bd-8b784fbfdfc7]: Requested amount: $30000.00
// [09:05:02.571] APPLICATION[c0b6444e-80aa-44d7-90bd-8b784fbfdfc7]: Invoking credit check...
// [09:05:02.572] APPLICATION[c0b6444e-80aa-44d7-90bd-8b784fbfdfc7]: ✗ Credit check failed: execution failed: execution failed: Credit score 580 is below minimum required 650

// === Test 3: Credit Bureau Timeout ===

// [09:05:03.352] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Processing loan application for Carol Davis
// [09:05:03.352] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Requested amount: $25000.00
// [09:05:03.352] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Invoking credit check...
// [09:05:03.353] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: ✗ Credit check failed: execution failed: Flow suspended for signal
// [09:05:03.457]   CREDIT[1a5a83d5-fbf6-5a9c-80c7-761131f0231f]: Checking credit for Carol Davis (attempt #1)
// [09:05:03.457]     -> Querying credit bureau (3s delay)...
// [09:05:07.466]   CREDIT[1a5a83d5-fbf6-5a9c-80c7-761131f0231f]: Checking credit for Carol Davis (attempt #2)
// [09:05:07.467]   CREDIT[1a5a83d5-fbf6-5a9c-80c7-761131f0231f]: Bureau timeout (retryable) - will retry
// [09:05:09.366]   CREDIT[1a5a83d5-fbf6-5a9c-80c7-761131f0231f]: Checking credit for Carol Davis (attempt #3)
// [09:05:09.367]   CREDIT[1a5a83d5-fbf6-5a9c-80c7-761131f0231f]: Check complete: score=720 ✓

// [09:05:09.472] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Processing loan application for Carol Davis
// [09:05:09.472] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Requested amount: $25000.00
// [09:05:09.472] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Invoking credit check...
// [09:05:09.474] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: ✓ Credit check passed: score=720, bureau=Equifax
// [09:05:09.474] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Invoking income verification...
// [09:05:09.475] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: ✗ Income verification failed: execution failed: Flow suspended for signal
// [09:05:09.576]   INCOME[8e4452b8-5520-516b-aaf3-7300d24beefa]: Verifying income for Carol Davis
// [09:05:09.577]     -> Checking employment records (2s delay)...
// [09:05:12.587]   INCOME[8e4452b8-5520-516b-aaf3-7300d24beefa]: Verifying income for Carol Davis
// [09:05:12.587]   INCOME[8e4452b8-5520-516b-aaf3-7300d24beefa]: Verification complete: $85000.00/year ✓

// [09:05:12.692] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Processing loan application for Carol Davis
// [09:05:12.692] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Requested amount: $25000.00
// [09:05:12.692] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Invoking credit check...
// [09:05:12.692] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: ✓ Credit check passed: score=720, bureau=Equifax
// [09:05:12.692] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Invoking income verification...
// [09:05:12.695] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: ✓ Income verified: $85000.00/year from Tech Corp Inc
// [09:05:12.695] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Invoking fraud check...
// [09:05:12.696] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: ✗ Fraud check failed: execution failed: Flow suspended for signal
// [09:05:12.798]   FRAUD[8b15ec0d-2c37-5442-b3d3-d0d29737f61e]: Scanning for fraud: Carol Davis
// [09:05:12.799]     -> Analyzing fraud patterns (4s delay)...
// [09:05:17.810]   FRAUD[8b15ec0d-2c37-5442-b3d3-d0d29737f61e]: Scanning for fraud: Carol Davis
// [09:05:17.810]   FRAUD[8b15ec0d-2c37-5442-b3d3-d0d29737f61e]: Scan complete: risk_score=12 ✓

// [09:05:17.916] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Processing loan application for Carol Davis
// [09:05:17.916] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Requested amount: $25000.00
// [09:05:17.916] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Invoking credit check...
// [09:05:17.917] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: ✓ Credit check passed: score=720, bureau=Equifax
// [09:05:17.917] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Invoking income verification...
// [09:05:17.917] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: ✓ Income verified: $85000.00/year from Tech Corp Inc
// [09:05:17.917] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: Invoking fraud check...
// [09:05:17.918] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: ✓ Fraud check passed: risk_score=12

// [09:05:17.918] APPLICATION[d4278863-6731-47c7-8298-33194d134054]: ✓ APPROVED - $25000.00 (processed in 2ms)

// Total credit check attempts: 3

// === Test 4: Fraud Detected ===

// [09:05:18.354] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Processing loan application for Dave Wilson
// [09:05:18.354] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Requested amount: $100000.00
// [09:05:18.354] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Invoking credit check...
// [09:05:18.354] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: ✗ Credit check failed: execution failed: Flow suspended for signal
// [09:05:18.459]   CREDIT[a6c8f9b0-dfbf-5222-ba77-16260abef5d7]: Checking credit for Dave Wilson (attempt #4)
// [09:05:18.460]     -> Querying credit bureau (3s delay)...
// [09:05:22.467]   CREDIT[a6c8f9b0-dfbf-5222-ba77-16260abef5d7]: Checking credit for Dave Wilson (attempt #5)
// [09:05:22.468]   CREDIT[a6c8f9b0-dfbf-5222-ba77-16260abef5d7]: Check complete: score=720 ✓

// [09:05:22.573] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Processing loan application for Dave Wilson
// [09:05:22.573] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Requested amount: $100000.00
// [09:05:22.573] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Invoking credit check...
// [09:05:22.575] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: ✓ Credit check passed: score=720, bureau=Equifax
// [09:05:22.575] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Invoking income verification...
// [09:05:22.576] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: ✗ Income verification failed: execution failed: Flow suspended for signal
// [09:05:22.678]   INCOME[fed643c5-6b35-599b-9712-5824c8483f60]: Verifying income for Dave Wilson
// [09:05:22.679]     -> Checking employment records (2s delay)...
// [09:05:25.687]   INCOME[fed643c5-6b35-599b-9712-5824c8483f60]: Verifying income for Dave Wilson
// [09:05:25.688]   INCOME[fed643c5-6b35-599b-9712-5824c8483f60]: Verification complete: $85000.00/year ✓

// [09:05:25.792] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Processing loan application for Dave Wilson
// [09:05:25.792] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Requested amount: $100000.00
// [09:05:25.792] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Invoking credit check...
// [09:05:25.793] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: ✓ Credit check passed: score=720, bureau=Equifax
// [09:05:25.793] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Invoking income verification...
// [09:05:25.794] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: ✓ Income verified: $85000.00/year from Tech Corp Inc
// [09:05:25.794] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Invoking fraud check...
// [09:05:25.795] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: ✗ Fraud check failed: execution failed: Flow suspended for signal
// [09:05:25.898]   FRAUD[ff826256-45c7-574c-b519-f48207239759]: Scanning for fraud: Dave Wilson
// [09:05:25.899]     -> Analyzing fraud patterns (4s delay)...
// [09:05:30.911]   FRAUD[ff826256-45c7-574c-b519-f48207239759]: Scanning for fraud: Dave Wilson

// [09:05:31.016] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Processing loan application for Dave Wilson
// [09:05:31.016] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Requested amount: $100000.00
// [09:05:31.016] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Invoking credit check...
// [09:05:31.017] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: ✓ Credit check passed: score=720, bureau=Equifax
// [09:05:31.017] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Invoking income verification...
// [09:05:31.017] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: ✓ Income verified: $85000.00/year from Tech Corp Inc
// [09:05:31.017] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: Invoking fraud check...
// [09:05:31.018] APPLICATION[8913c60b-ac84-4f2d-aca1-a748d4775221]: ✗ Fraud check failed: execution failed: execution failed: Fraud detected (risk score: 85): Multiple applications from same IP
