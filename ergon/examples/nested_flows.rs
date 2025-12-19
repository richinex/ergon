//! Nested flows and composition example
//!
//! This example demonstrates:
//! - Flows that schedule other flows (nested/sub-workflows)
//! - Steps that call helper methods (composition without tracking)
//! - Hierarchical workflow orchestration
//! - Parent-child flow relationships
//! - Helper methods vs steps (what gets tracked)
//!
//! ## Scenario
//! - Parent flow: OrderProcessor orchestrates the entire order
//!   - Step 1: Validate order (uses helper methods for validation)
//!   - Step 2: Process payment (conceptually schedules child payment flow)
//!   - Step 3: Reserve inventory (conceptually schedules child inventory flow)
//!   - Step 4: Finalize order
//! - Child flow: PaymentFlow processes payment independently
//!   - Authorize -> Capture -> Confirm
//! - Child flow: InventoryFlow manages inventory independently
//!   - Check stock -> Reserve items -> Confirm reservation
//!
//! ## Key Takeaways
//! - Parent flows can orchestrate multiple child flows
//! - Helper methods provide composition without creating tracked steps
//! - Child flows run independently and can be scheduled dynamically
//! - The `inputs` attribute enables data flow between steps in any flow
//! - One worker can handle multiple flow types simultaneously
//!
//! ## Run with
//! ```bash
//! cargo run --example nested_flows
//! ```

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// PARENT FLOW: Orchestrates the entire order processing
// ============================================================================

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct OrderProcessor {
    order_id: String,
    items: Vec<String>,
    total_amount: f64,
    customer_id: String,
}

impl OrderProcessor {
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<OrderResult, String> {
        println!("\n[PARENT FLOW] Processing order: {}", self.order_id);

        // Generate non-deterministic values at flow level
        let validated_at = chrono::Utc::now().timestamp();
        let transaction_id = format!("TXN-{}", uuid::Uuid::new_v4());
        let reservation_id = format!("RES-{}", uuid::Uuid::new_v4());

        // Step 1: Validate order
        let validation = self.clone().validate_order(validated_at).await?;

        // Step 2: Process payment (this will schedule a CHILD FLOW!)
        let payment_result = self
            .clone()
            .process_payment(validation, transaction_id)
            .await?;

        // Step 3: Reserve inventory (this will schedule another CHILD FLOW!)
        let inventory_result = self
            .clone()
            .reserve_inventory(payment_result, reservation_id)
            .await?;

        // Step 4: Finalize order
        let result = self.clone().finalize_order(inventory_result).await?;

        println!("[PARENT FLOW] Order {} completed!", self.order_id);
        Ok(result)
    }

    #[step]
    async fn validate_order(
        self: Arc<Self>,
        validated_at: i64,
    ) -> Result<ValidationResult, String> {
        println!("  [Step] Validating order {}", self.order_id);

        // Call helper method (not tracked as separate step)
        let items_valid = self.validate_items();
        let amount_valid = self.validate_amount();

        tokio::time::sleep(Duration::from_millis(100)).await;

        if !items_valid || !amount_valid {
            return Err("Order validation failed".to_string());
        }

        Ok(ValidationResult {
            order_id: self.order_id.clone(),
            validated_at,
        })
    }

    #[step(inputs(validation = "validate_order"))]
    async fn process_payment(
        self: Arc<Self>,
        validation: ValidationResult,
        transaction_id: String,
    ) -> Result<PaymentInfo, String> {
        println!(
            "  [Step] Processing payment for order {}",
            validation.order_id
        );

        // Here we would schedule a CHILD FLOW for payment processing
        // In a real scenario, you'd use the scheduler to create a new flow
        println!("    [Child Flow] Payment flow would be scheduled here");
        println!("    [Child Flow] Amount: ${:.2}", self.total_amount);

        // Simulate payment processing
        tokio::time::sleep(Duration::from_millis(200)).await;

        Ok(PaymentInfo {
            transaction_id,
            amount: self.total_amount,
            status: "completed".to_string(),
        })
    }

    #[step(inputs(payment = "process_payment"))]
    async fn reserve_inventory(
        self: Arc<Self>,
        payment: PaymentInfo,
        reservation_id: String,
    ) -> Result<InventoryReservation, String> {
        println!(
            "  [Step] Reserving inventory (payment: {})",
            payment.transaction_id
        );

        // Schedule another CHILD FLOW for inventory management
        println!("    [Child Flow] Inventory flow would be scheduled here");
        println!("    [Child Flow] Items: {:?}", self.items);

        tokio::time::sleep(Duration::from_millis(150)).await;

        Ok(InventoryReservation {
            reservation_id,
            items: self.items.clone(),
        })
    }

    #[step(inputs(inventory = "reserve_inventory"))]
    async fn finalize_order(
        self: Arc<Self>,
        inventory: InventoryReservation,
    ) -> Result<OrderResult, String> {
        println!(
            "  [Step] Finalizing order (reservation: {})",
            inventory.reservation_id
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(OrderResult {
            order_id: self.order_id.clone(),
            status: "completed".to_string(),
            items_count: self.items.len(),
        })
    }

    // ========================================================================
    // HELPER METHODS (not tracked as steps)
    // ========================================================================

    fn validate_items(&self) -> bool {
        println!("    [Helper] Validating {} items", self.items.len());
        !self.items.is_empty()
    }

    fn validate_amount(&self) -> bool {
        println!("    [Helper] Validating amount: ${:.2}", self.total_amount);
        self.total_amount > 0.0
    }
}

// ============================================================================
// CHILD FLOW: Independent payment processing
// ============================================================================

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct PaymentFlow {
    transaction_id: String,
    amount: f64,
    payment_method: String,
}

impl PaymentFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<PaymentResult, String> {
        println!(
            "\n  [CHILD FLOW - Payment] Processing: {}",
            self.transaction_id
        );

        // Generate non-deterministic values at flow level
        let auth_code = format!("AUTH-{}", uuid::Uuid::new_v4());
        let capture_id = format!("CAP-{}", uuid::Uuid::new_v4());

        let auth = self.clone().authorize_payment(auth_code).await?;
        let capture = self.clone().capture_payment(auth, capture_id).await?;
        let result = self.clone().confirm_payment(capture).await?;

        println!(
            "  [CHILD FLOW - Payment] Completed: {}",
            self.transaction_id
        );
        Ok(result)
    }

    #[step]
    async fn authorize_payment(self: Arc<Self>, auth_code: String) -> Result<AuthResult, String> {
        println!(
            "    [Step] Authorizing ${:.2} via {}",
            self.amount, self.payment_method
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(AuthResult {
            auth_code,
            authorized_amount: self.amount,
        })
    }

    #[step(inputs(auth = "authorize_payment"))]
    async fn capture_payment(
        self: Arc<Self>,
        auth: AuthResult,
        capture_id: String,
    ) -> Result<CaptureResult, String> {
        println!("    [Step] Capturing payment (auth: {})", auth.auth_code);
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(CaptureResult {
            capture_id,
            captured_amount: auth.authorized_amount,
        })
    }

    #[step(inputs(capture = "capture_payment"))]
    async fn confirm_payment(
        self: Arc<Self>,
        capture: CaptureResult,
    ) -> Result<PaymentResult, String> {
        println!(
            "    [Step] Confirming payment (capture: {})",
            capture.capture_id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(PaymentResult {
            transaction_id: self.transaction_id.clone(),
            status: "confirmed".to_string(),
        })
    }
}

// ============================================================================
// CHILD FLOW: Independent inventory management
// ============================================================================

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct InventoryFlow {
    reservation_id: String,
    items: Vec<String>,
}

impl InventoryFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<InventoryResult, String> {
        println!(
            "\n  [CHILD FLOW - Inventory] Processing: {}",
            self.reservation_id
        );

        let check = self.clone().check_stock().await?;
        let reserve = self.clone().reserve_items(check).await?;
        let result = self.clone().confirm_reservation(reserve).await?;

        println!(
            "  [CHILD FLOW - Inventory] Completed: {}",
            self.reservation_id
        );
        Ok(result)
    }

    #[step]
    async fn check_stock(self: Arc<Self>) -> Result<StockCheck, String> {
        println!("    [Step] Checking stock for {} items", self.items.len());
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Use helper method to validate each item
        for item in &self.items {
            if !self.is_item_available(item) {
                return Err(format!("Item not available: {}", item));
            }
        }

        Ok(StockCheck {
            all_available: true,
            items: self.items.clone(),
        })
    }

    #[step(inputs(stock = "check_stock"))]
    async fn reserve_items(self: Arc<Self>, stock: StockCheck) -> Result<ReservationData, String> {
        println!("    [Step] Reserving {} items", stock.items.len());
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(ReservationData {
            reservation_id: self.reservation_id.clone(),
            items: stock.items,
        })
    }

    #[step(inputs(reservation = "reserve_items"))]
    async fn confirm_reservation(
        self: Arc<Self>,
        reservation: ReservationData,
    ) -> Result<InventoryResult, String> {
        println!(
            "    [Step] Confirming reservation: {}",
            reservation.reservation_id
        );
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(InventoryResult {
            reservation_id: reservation.reservation_id,
            status: "confirmed".to_string(),
        })
    }

    // Helper method (not a step)
    fn is_item_available(&self, item: &str) -> bool {
        println!("      [Helper] Checking availability: {}", item);
        true // Simplified - always available
    }
}

// ============================================================================
// Data structures
// ============================================================================

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct ValidationResult {
    order_id: String,
    validated_at: i64,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct PaymentInfo {
    transaction_id: String,
    amount: f64,
    status: String,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct InventoryReservation {
    reservation_id: String,
    items: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct OrderResult {
    order_id: String,
    status: String,
    items_count: usize,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct AuthResult {
    auth_code: String,
    authorized_amount: f64,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct CaptureResult {
    capture_id: String,
    captured_amount: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct PaymentResult {
    transaction_id: String,
    status: String,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct StockCheck {
    all_available: bool,
    items: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct ReservationData {
    reservation_id: String,
    items: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, FlowType)]
struct InventoryResult {
    reservation_id: String,
    status: String,
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = Scheduler::new(storage.clone()).unversioned();

    // Schedule PARENT flow (order processing)
    let order = OrderProcessor {
        order_id: "ORD-12345".to_string(),
        items: vec![
            "ITEM-A".to_string(),
            "ITEM-B".to_string(),
            "ITEM-C".to_string(),
        ],
        total_amount: 299.99,
        customer_id: "CUST-001".to_string(),
    };
    scheduler.schedule(order).await?;

    // Schedule CHILD flows (these run independently)
    let payment = PaymentFlow {
        transaction_id: "PAY-001".to_string(),
        amount: 299.99,
        payment_method: "credit_card".to_string(),
    };
    scheduler.schedule(payment).await?;

    let inventory = InventoryFlow {
        reservation_id: "INV-001".to_string(),
        items: vec!["ITEM-A".to_string(), "ITEM-B".to_string()],
    };
    scheduler.schedule(inventory).await?;

    // Start worker
    let worker = Worker::new(storage.clone(), "orchestrator");
    worker
        .register(|flow: Arc<OrderProcessor>| flow.process_order())
        .await;
    worker
        .register(|flow: Arc<PaymentFlow>| flow.process())
        .await;
    worker
        .register(|flow: Arc<InventoryFlow>| flow.process())
        .await;

    let worker_handle = worker.start().await;

    // Wait for completion
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Ok(incomplete) = storage.get_incomplete_flows().await {
            if incomplete.is_empty() {
                break;
            }
        }
    }

    worker_handle.shutdown().await;

    Ok(())
}
