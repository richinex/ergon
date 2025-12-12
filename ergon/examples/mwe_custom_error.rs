//! Ergon Example - Custom Error Types
//!
//! Demonstrates using custom error enums instead of String for type-safe
//! error handling. Shows how rich error types can carry context and enable
//! sophisticated error recovery strategies.

use chrono::Utc;
use ergon::executor::{InvokeChild, Worker};
use ergon::prelude::*;
use ergon::storage::SqliteExecutionLog;
use ergon::TaskStatus;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

// =============================================================================
// Custom Error Type - Rich Enum with Context
// =============================================================================

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
enum PaymentError {
    #[error("Insufficient funds: ${available:.2} available, ${required:.2} required")]
    InsufficientFunds {
        available: f64,
        required: f64,
    },
    #[error("Network error: {message} (retry after {retry_after_ms}ms)")]
    NetworkError {
        message: String,
        retry_after_ms: u64,
    },
    #[error("Fraud detected: {reason} (risk score: {risk_score:.2})")]
    FraudDetected {
        reason: String,
        risk_score: f32,
    },
    #[error("Invalid card: {field} is invalid")]
    InvalidCard {
        field: String,
    },
    #[error("Service unavailable: {service}")]
    ServiceUnavailable {
        service: String,
    },
}

impl ergon::Retryable for PaymentError {
    fn is_retryable(&self) -> bool {
        match self {
            // Transient errors - retryable
            PaymentError::NetworkError { .. } => true,
            PaymentError::ServiceUnavailable { .. } => true,
            // Permanent errors - non-retryable
            PaymentError::InsufficientFunds { .. } => false,
            PaymentError::FraudDetected { .. } => false,
            PaymentError::InvalidCard { .. } => false,
        }
    }
}

// =============================================================================
// Domain Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentResult {
    transaction_id: String,
    amount: f64,
    status: String,
}

// =============================================================================
// Parent Flow - Payment Order
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct PaymentOrder {
    order_id: String,
    amount: f64,
    card_number: String,
}

impl PaymentOrder {
    #[step]
    async fn validate_card(self: Arc<Self>) -> Result<(), PaymentError> {
        println!("[{}] validating card for {}", ts(), self.order_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate card validation
        if self.card_number.len() != 16 {
            return Err(PaymentError::InvalidCard {
                field: "card_number".to_string(),
            });
        }

        Ok(())
    }

    #[step(depends_on = "validate_card")]
    async fn check_fraud(self: Arc<Self>) -> Result<(), PaymentError> {
        println!("[{}] checking fraud for {}", ts(), self.order_id);
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Simulate fraud check - high amounts are suspicious
        if self.amount > 10000.0 {
            return Err(PaymentError::FraudDetected {
                reason: "Unusually high transaction amount".to_string(),
                risk_score: 0.95,
            });
        }

        Ok(())
    }

    #[step(depends_on = "check_fraud")]
    async fn process_payment(self: Arc<Self>) -> Result<PaymentResult, PaymentError> {
        println!("[{}] processing payment for {}", ts(), self.order_id);

        // Invoke the payment processor child flow
        let result = self
            .invoke(PaymentProcessor {
                order_id: self.order_id.clone(),
                amount: self.amount,
                card_number: self.card_number.clone(),
            })
            .result()
            .await
            .map_err(|e| PaymentError::ServiceUnavailable {
                service: format!("PaymentProcessor: {}", e),
            })?;

        println!(
            "[{}] payment processed: {} - ${}",
            ts(),
            result.transaction_id,
            result.amount
        );

        Ok(result)
    }

    #[flow]
    async fn execute(self: Arc<Self>) -> Result<PaymentResult, PaymentError> {
        self.clone().validate_card().await?;
        self.clone().check_fraud().await?;
        self.clone().process_payment().await
    }
}

// =============================================================================
// Child Flow - Payment Processor
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = PaymentResult)]
struct PaymentProcessor {
    order_id: String,
    amount: f64,
    card_number: String,
}

impl PaymentProcessor {
    #[step]
    async fn check_balance(self: Arc<Self>) -> Result<(), PaymentError> {
        println!("[{}] checking balance for order {}", ts(), self.order_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate balance check based on amount
        let available = 5000.0; // Simulated balance
        if self.amount > available {
            return Err(PaymentError::InsufficientFunds {
                available,
                required: self.amount,
            });
        }

        Ok(())
    }

    #[step(depends_on = "check_balance")]
    async fn authorize(self: Arc<Self>) -> Result<String, PaymentError> {
        println!("[{}] authorizing payment for {}", ts(), self.order_id);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Simulate network error for specific card patterns
        if self.card_number.starts_with("4000") {
            return Err(PaymentError::NetworkError {
                message: "Connection timeout to payment gateway".to_string(),
                retry_after_ms: 5000,
            });
        }

        let auth_code = format!("AUTH-{}", &Uuid::new_v4().to_string()[..8]);
        println!("[{}] authorized: {}", ts(), auth_code);

        Ok(auth_code)
    }

    #[step(depends_on = "authorize")]
    async fn capture(self: Arc<Self>) -> Result<PaymentResult, PaymentError> {
        println!("[{}] capturing payment for {}", ts(), self.order_id);
        tokio::time::sleep(Duration::from_millis(150)).await;

        let transaction_id = format!("TXN-{}", &Uuid::new_v4().to_string()[..8]);

        Ok(PaymentResult {
            transaction_id,
            amount: self.amount,
            status: "captured".to_string(),
        })
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<PaymentResult, PaymentError> {
        self.clone().check_balance().await?;
        self.clone().authorize().await?;
        self.clone().capture().await
    }
}

// =============================================================================
// Utilities
// =============================================================================

fn ts() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

// =============================================================================
// Main - Demonstrates Various Error Scenarios
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = "/tmp/ergon_mwe_custom_error.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);

    println!("\n=== Testing Custom Error Types ===\n");

    // ============================================================
    // PART 1: API Server / Scheduler Process
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 1: Scheduling Payment Orders (API Server)            ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let scheduler = Scheduler::new(storage.clone());
    let mut task_ids = Vec::new();

    // Test 1: Successful payment
    let order1 = PaymentOrder {
        order_id: "ORD-001".into(),
        amount: 100.0,
        card_number: "1234567890123456".into(),
    };
    let task_id = scheduler.schedule(order1, Uuid::new_v4()).await?;
    println!(
        "   ✓ ORD-001 scheduled (task_id: {})",
        &task_id.to_string()[..8]
    );
    task_ids.push(task_id);

    // Test 2: Invalid card
    let order2 = PaymentOrder {
        order_id: "ORD-002".into(),
        amount: 200.0,
        card_number: "123".into(), // Too short
    };
    let task_id = scheduler.schedule(order2, Uuid::new_v4()).await?;
    println!(
        "   ✓ ORD-002 scheduled (task_id: {})",
        &task_id.to_string()[..8]
    );
    task_ids.push(task_id);

    // Test 3: Fraud detection
    let order3 = PaymentOrder {
        order_id: "ORD-003".into(),
        amount: 15000.0, // High amount triggers fraud
        card_number: "1234567890123456".into(),
    };
    let task_id = scheduler.schedule(order3, Uuid::new_v4()).await?;
    println!(
        "   ✓ ORD-003 scheduled (task_id: {})",
        &task_id.to_string()[..8]
    );
    task_ids.push(task_id);

    // Test 4: Insufficient funds
    let order4 = PaymentOrder {
        order_id: "ORD-004".into(),
        amount: 8000.0, // Exceeds simulated balance of 5000
        card_number: "1234567890123456".into(),
    };
    let task_id = scheduler.schedule(order4, Uuid::new_v4()).await?;
    println!(
        "   ✓ ORD-004 scheduled (task_id: {})",
        &task_id.to_string()[..8]
    );
    task_ids.push(task_id);

    // Test 5: Network error
    let order5 = PaymentOrder {
        order_id: "ORD-005".into(),
        amount: 300.0,
        card_number: "4000123456789012".into(), // 4000 prefix triggers network error
    };
    let task_id = scheduler.schedule(order5, Uuid::new_v4()).await?;
    println!(
        "   ✓ ORD-005 scheduled (task_id: {})",
        &task_id.to_string()[..8]
    );
    task_ids.push(task_id);

    println!("\n   → In production: Return HTTP 202 Accepted with task_ids\n");

    // ============================================================
    // PART 2: Worker Service (Separate Process)
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 2: Starting Worker (Separate Service)                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let worker =
        Worker::new(storage.clone(), "worker").with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<PaymentOrder>| f.execute()).await;
    worker
        .register(|f: Arc<PaymentProcessor>| f.process())
        .await;

    let handle = worker.start().await;
    println!("   ✓ Worker started and polling for work\n");

    // ============================================================
    // PART 3: Client Status Monitoring (Demo Only)
    // ============================================================

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PART 3: Monitoring Status (Client Would Poll API)         ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let timeout_duration = Duration::from_secs(5);
    let wait_result = tokio::time::timeout(timeout_duration, async {
        loop {
            let mut all_complete = true;
            for &task_id in &task_ids {
                if let Some(scheduled) = storage.get_scheduled_flow(task_id).await? {
                    if !matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                        all_complete = false;
                        break;
                    }
                }
            }
            if all_complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await;

    match wait_result {
        Ok(_) => println!("\n=== All Tests Complete ===\n"),
        Err(_) => println!("\n[WARN] Timeout waiting for tests to complete\n"),
    }

    handle.shutdown().await;
    storage.close().await?;

    println!("Note: Check the execution log to see how different error types");
    println!("are stored and can be used for error recovery strategies.");

    Ok(())
}
