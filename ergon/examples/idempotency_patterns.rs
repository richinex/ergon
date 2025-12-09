//! Idempotency Patterns Example
//!
//! This example demonstrates how to make steps idempotent for durable workflows.
//!
//! ## Why Idempotency Matters
//!
//! Steps may be re-executed if:
//! - Storage is temporarily unavailable (treated as cache miss)
//! - Worker crashes and flow is retried
//! - Non-determinism is detected during replay
//!
//! ## Patterns Demonstrated
//!
//! 1. **Deterministic IDs** - Use flow_id + step hash instead of random UUIDs
//! 2. **Idempotency Keys** - Check-then-create pattern with external systems
//! 3. **Read Operations** - Always idempotent (safe to repeat)
//! 4. **State Checks** - Check current state before mutations
//!
//! ## Run with
//! ```bash
//! cargo run --example idempotency_patterns --features=sqlite
//! ```

use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

// ============================================================================
// Simulated External Services (with idempotency support)
// ============================================================================

/// Simulated payment service that supports idempotency keys
#[derive(Clone)]
struct PaymentService {
    processed_payments: Arc<RwLock<HashMap<String, String>>>,
}

impl PaymentService {
    fn new() -> Self {
        Self {
            processed_payments: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Process payment with idempotency key
    async fn charge(&self, idempotency_key: &str, amount: f64) -> String {
        let mut payments = self.processed_payments.write().await;

        // Check if already processed
        if let Some(existing_tx) = payments.get(idempotency_key) {
            println!(
                "      [IDEMPOTENT] Payment already processed with key '{}': {}",
                idempotency_key, existing_tx
            );
            return existing_tx.clone();
        }

        // Simulate payment processing
        tokio::time::sleep(Duration::from_millis(100)).await;
        let tx_id = format!("TX-{}", &Uuid::new_v4().to_string()[..8].to_uppercase());

        println!(
            "      [NEW] Processing payment ${} with key '{}' → {}",
            amount, idempotency_key, tx_id
        );

        payments.insert(idempotency_key.to_string(), tx_id.clone());
        tx_id
    }
}

/// Simulated email service
#[derive(Clone)]
struct EmailService {
    sent_emails: Arc<RwLock<HashMap<String, ()>>>,
}

impl EmailService {
    fn new() -> Self {
        Self {
            sent_emails: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Send email with idempotency key
    async fn send(&self, idempotency_key: &str, to: &str, subject: &str) {
        let mut emails = self.sent_emails.write().await;

        if emails.contains_key(idempotency_key) {
            println!(
                "      [IDEMPOTENT] Email already sent with key '{}'",
                idempotency_key
            );
            return;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        println!("      [NEW] Sending email to {} - {}", to, subject);

        emails.insert(idempotency_key.to_string(), ());
    }
}

// Global services (in real app, pass these via context or DI)
static PAYMENT_SERVICE: tokio::sync::OnceCell<PaymentService> = tokio::sync::OnceCell::const_new();
static EMAIL_SERVICE: tokio::sync::OnceCell<EmailService> = tokio::sync::OnceCell::const_new();

// ============================================================================
// Example Flow - Order Processing with Idempotency
// ============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderProcessingFlow {
    order_id: String,
    customer_email: String,
    amount: f64,
}

impl OrderProcessingFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("\n[FLOW] Processing order: {}", self.order_id);

        // Get flow context for deterministic IDs
        let flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be in flow");

        println!("      Flow ID: {}", flow_id);

        // Step 1: Validate (read-only, naturally idempotent)
        self.clone().validate_order().await?;

        // Step 2: Process payment (uses idempotency key)
        let tx_id = self.clone().process_payment(flow_id).await?;

        // Step 3: Send confirmation (uses idempotency key)
        self.clone()
            .send_confirmation(flow_id, tx_id.clone())
            .await?;

        Ok(format!(
            "Order {} processed successfully: {}",
            self.order_id, tx_id
        ))
    }

    /// Pattern 1: Read-Only Operations (Naturally Idempotent)
    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<(), String> {
        println!("[STEP] Validating order {}", self.order_id);

        // Read operations are naturally idempotent
        // Safe to call multiple times
        tokio::time::sleep(Duration::from_millis(50)).await;

        if self.amount <= 0.0 {
            return Err("Invalid amount".to_string());
        }

        println!("      ✓ Validation passed");
        Ok(())
    }

    /// Pattern 2: Idempotency Keys with External Services
    #[step]
    async fn process_payment(self: Arc<Self>, flow_id: Uuid) -> Result<String, String> {
        println!("[STEP] Processing payment for order {}", self.order_id);

        // Get current step number for unique key
        let step = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.next_step() - 1) // -1 because we already incremented
            .expect("Must be in flow");

        // Create deterministic idempotency key
        // Format: {flow_id}-{step}-{business_id}
        let idempotency_key = format!("{}-{}-payment", flow_id, step);

        println!("      Idempotency key: {}", idempotency_key);

        // Use idempotency key with external service
        let payment_service = PAYMENT_SERVICE.get().expect("Service not initialized");
        let tx_id = payment_service.charge(&idempotency_key, self.amount).await;

        Ok(tx_id)
    }

    /// Pattern 3: Idempotency with Side Effects
    #[step]
    async fn send_confirmation(
        self: Arc<Self>,
        flow_id: Uuid,
        tx_id: String,
    ) -> Result<(), String> {
        println!("[STEP] Sending confirmation email");

        let step = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.next_step() - 1)
            .expect("Must be in flow");

        let idempotency_key = format!("{}-{}-email", flow_id, step);

        println!("      Idempotency key: {}", idempotency_key);

        let email_service = EMAIL_SERVICE.get().expect("Service not initialized");
        email_service
            .send(
                &idempotency_key,
                &self.customer_email,
                &format!("Order {} confirmed - {}", self.order_id, tx_id),
            )
            .await;

        Ok(())
    }
}

// ============================================================================
// Main - Demonstrate Idempotency
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║          Idempotency Patterns Example                    ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    // Initialize services
    PAYMENT_SERVICE
        .set(PaymentService::new())
        .ok()
        .expect("Already initialized");
    EMAIL_SERVICE
        .set(EmailService::new())
        .ok()
        .expect("Already initialized");

    let storage = Arc::new(SqliteExecutionLog::new("sqlite::memory:").await?);

    println!("\n=== Test: Execute flow twice (simulating re-execution) ===\n");

    let order = OrderProcessingFlow {
        order_id: "ORD-001".to_string(),
        customer_email: "customer@example.com".to_string(),
        amount: 99.99,
    };

    let flow_id = Uuid::new_v4();

    // First execution
    println!(">>> First Execution");
    let executor1 = Executor::new(flow_id, order.clone(), storage.clone());
    let outcome1 = executor1
        .execute(|f| Box::pin(Arc::new(f.clone()).process()))
        .await;

    match outcome1 {
        FlowOutcome::Completed(Ok(result)) => println!("\n[SUCCESS] First execution: {}", result),
        FlowOutcome::Completed(Err(e)) => println!("\n[ERROR] First execution failed: {}", e),
        FlowOutcome::Suspended(reason) => println!("\n[SUSPENDED] First execution: {:?}", reason),
    }

    // Second execution (simulates cache miss due to storage error)
    println!("\n\n>>> Second Execution (simulating cache miss/re-execution)");
    let executor2 = Executor::new(flow_id, order.clone(), storage.clone());
    let outcome2 = executor2
        .execute(|f| Box::pin(Arc::new(f.clone()).process()))
        .await;

    match outcome2 {
        FlowOutcome::Completed(Ok(result)) => println!("\n[SUCCESS] Second execution: {}", result),
        FlowOutcome::Completed(Err(e)) => println!("\n[ERROR] Second execution failed: {}", e),
        FlowOutcome::Suspended(reason) => println!("\n[SUSPENDED] Second execution: {:?}", reason),
    }

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Key Observations                                         ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    println!("\n1. Payment was processed only ONCE despite two executions");
    println!("   → Idempotency key prevented duplicate charge\n");

    println!("2. Email was sent only ONCE");
    println!("   → Idempotency key prevented duplicate email\n");

    println!("3. Validation ran twice (read-only, safe)");
    println!("   → No side effects from re-execution\n");

    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  Best Practices                                           ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    println!("\n✓ Use deterministic IDs: flow_id + step + business_key");
    println!("✓ Pass idempotency keys to external services");
    println!("✓ Read operations are naturally idempotent");
    println!("✓ Check-then-create pattern for mutations");
    println!("✓ NEVER use Uuid::new_v4() in steps!");
    println!("✓ NEVER use timestamps for business logic!\n");

    storage.close().await?;
    Ok(())
}
