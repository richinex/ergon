//! Nested flows example - parent flow actually spawns and awaits child flows
//!
//! Run with:
//! ```bash
//! cargo run --example nested_flows_real
//! ```

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// PARENT FLOW: Actually spawns child flows
// ============================================================================

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct OrderProcessor {
    order_id: String,
    items: Vec<String>,
    total_amount: f64,
}

impl OrderProcessor {
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, String> {
        println!("\n[PARENT] Processing order: {}", self.order_id);

        let validation = self.clone().validate_order().await?;
        let payment = self.clone().process_payment(validation).await?;
        let inventory = self.clone().reserve_inventory(payment).await?;
        let result = self.clone().finalize_order(inventory).await?;

        println!("[PARENT] Order {} completed!", self.order_id);
        Ok(result)
    }

    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<ValidationResult, String> {
        println!("  [Step] Validating order {}", self.order_id);
        tokio::time::sleep(Duration::from_millis(50)).await;

        if self.items.is_empty() || self.total_amount <= 0.0 {
            return Err("Invalid order".to_string());
        }

        Ok(ValidationResult {
            order_id: self.order_id.clone(),
        })
    }

    #[step(inputs(validation = "validate_order"))]
    async fn process_payment(
        self: Arc<Self>,
        validation: ValidationResult,
    ) -> Result<PaymentResult, String> {
        println!(
            "  [Step] Spawning child payment flow for {}",
            validation.order_id
        );

        // Create and execute child flow inline
        let child = PaymentFlow {
            transaction_id: format!("TXN-{}", self.order_id),
            amount: self.total_amount,
        };

        // Execute child flow directly (it has its own steps)
        let result = Arc::new(child).process().await?;

        println!("  [Step] Child payment completed: {:?}", result);
        Ok(result)
    }

    #[step(inputs(payment = "process_payment"))]
    async fn reserve_inventory(
        self: Arc<Self>,
        payment: PaymentResult,
    ) -> Result<InventoryResult, String> {
        println!(
            "  [Step] Spawning child inventory flow (payment: {})",
            payment.transaction_id
        );

        // Create and execute child flow inline
        let child = InventoryFlow {
            reservation_id: format!("RES-{}", self.order_id),
            items: self.items.clone(),
        };

        let result = Arc::new(child).process().await?;

        println!("  [Step] Child inventory completed: {:?}", result);
        Ok(result)
    }

    #[step(inputs(inventory = "reserve_inventory"))]
    async fn finalize_order(
        self: Arc<Self>,
        inventory: InventoryResult,
    ) -> Result<OrderResult, String> {
        println!(
            "  [Step] Finalizing order (reservation: {})",
            inventory.reservation_id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(OrderResult {
            order_id: self.order_id.clone(),
            status: "completed".to_string(),
            items_count: self.items.len(),
        })
    }
}

// ============================================================================
// CHILD FLOW: Payment processing
// ============================================================================

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct PaymentFlow {
    transaction_id: String,
    amount: f64,
}

impl PaymentFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<PaymentResult, String> {
        println!("    [CHILD-Payment] Starting: {}", self.transaction_id);

        let auth = self.clone().authorize().await?;
        let result = self.clone().capture(auth).await?;

        println!("    [CHILD-Payment] Done: {}", self.transaction_id);
        Ok(result)
    }

    #[step]
    async fn authorize(self: Arc<Self>) -> Result<String, String> {
        println!("      [Step] Authorizing ${:.2}", self.amount);
        tokio::time::sleep(Duration::from_millis(30)).await;
        Ok(format!("AUTH-{}", self.transaction_id))
    }

    #[step(inputs(auth_code = "authorize"))]
    async fn capture(self: Arc<Self>, auth_code: String) -> Result<PaymentResult, String> {
        println!("      [Step] Capturing payment (auth: {})", auth_code);
        tokio::time::sleep(Duration::from_millis(30)).await;

        Ok(PaymentResult {
            transaction_id: self.transaction_id.clone(),
            status: "captured".to_string(),
        })
    }
}

// ============================================================================
// CHILD FLOW: Inventory management
// ============================================================================

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct InventoryFlow {
    reservation_id: String,
    items: Vec<String>,
}

impl InventoryFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<InventoryResult, String> {
        println!("    [CHILD-Inventory] Starting: {}", self.reservation_id);

        let checked = self.clone().check_stock().await?;
        let result = self.clone().reserve(checked).await?;

        println!("    [CHILD-Inventory] Done: {}", self.reservation_id);
        Ok(result)
    }

    #[step]
    async fn check_stock(self: Arc<Self>) -> Result<Vec<String>, String> {
        println!("      [Step] Checking stock for {} items", self.items.len());
        tokio::time::sleep(Duration::from_millis(30)).await;
        Ok(self.items.clone())
    }

    #[step(inputs(items = "check_stock"))]
    async fn reserve(self: Arc<Self>, items: Vec<String>) -> Result<InventoryResult, String> {
        println!("      [Step] Reserving {} items", items.len());
        tokio::time::sleep(Duration::from_millis(30)).await;

        Ok(InventoryResult {
            reservation_id: self.reservation_id.clone(),
            status: "reserved".to_string(),
        })
    }
}

// ============================================================================
// Data structures
// ============================================================================

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct ValidationResult {
    order_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct PaymentResult {
    transaction_id: String,
    status: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct InventoryResult {
    reservation_id: String,
    status: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct OrderResult {
    order_id: String,
    status: String,
    items_count: usize,
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

    // Schedule parent flow only - it will spawn children
    let order = OrderProcessor {
        order_id: "ORD-001".to_string(),
        items: vec!["Widget".to_string(), "Gadget".to_string()],
        total_amount: 99.99,
    };
    scheduler
        .schedule_with(order.clone(), Uuid::new_v4())
        .await?;

    // Worker only needs to handle the parent flow type
    // Child flows are executed inline, not scheduled separately
    let worker = Worker::new(storage.clone(), "worker-1");
    worker
        .register(|flow: Arc<OrderProcessor>| flow.process_order())
        .await;

    let handle = worker.start().await;

    // Wait for completion
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Ok(flows) = storage.get_incomplete_flows().await {
            if flows.is_empty() {
                break;
            }
        }
    }

    handle.shutdown().await;

    Ok(())
}
