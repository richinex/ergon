//! Sequential vs Parallel with Autowiring
//!
//! This example demonstrates how `inputs` autowiring creates cleaner,
//! more maintainable code compared to explicit `depends_on` declarations.
//!
//! Autowiring Benefits:
//! - No duplication between depends_on and inputs
//! - Dependencies automatically match data flow
//! - Refactoring is easier (rename step in one place)
//! - More readable: see data dependencies directly in function signature

use ergon::Ergon;
use ergon::{flow, step};
use ergon::{ExecutionLog, InMemoryExecutionLog};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// ============================================================================
// Data Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Customer {
    id: String,
    name: String,
    credit_score: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Product {
    id: String,
    name: String,
    price: f64,
    stock: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentAuth {
    transaction_id: String,
    authorized: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderResult {
    order_id: String,
    customer_name: String,
    product_name: String,
    total_price: f64,
}

// ============================================================================
// BEFORE: Without Autowiring (Verbose)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderFlowOld {
    customer_id: String,
    product_id: String,
}

impl OrderFlowOld {
    fn new(customer_id: String, product_id: String) -> Self {
        Self {
            customer_id,
            product_id,
        }
    }

    #[step]
    async fn fetch_customer(self: Arc<Self>) -> Result<Customer, String> {
        println!("[OLD] Fetching customer: {}", self.customer_id);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(Customer {
            id: self.customer_id.clone(),
            name: "Alice".to_string(),
            credit_score: 750,
        })
    }

    // VERBOSE: Must specify both depends_on AND inputs
    #[step(depends_on = "fetch_customer", inputs(customer = "fetch_customer"))]
    async fn validate_credit(
        self: Arc<Self>,
        customer: Customer,
    ) -> Result<bool, String> {
        println!("[OLD] Validating credit for: {}", customer.name);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(customer.credit_score > 600)
    }

    #[step(depends_on = "fetch_customer")]
    async fn fetch_product(self: Arc<Self>) -> Result<Product, String> {
        println!("[OLD] Fetching product: {}", self.product_id);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(Product {
            id: self.product_id.clone(),
            name: "Widget".to_string(),
            price: 99.99,
            stock: 10,
        })
    }

    // VERBOSE: Long depends_on list AND inputs list
    #[step(
        depends_on = ["validate_credit", "fetch_product"],
        inputs(credit_ok = "validate_credit", product = "fetch_product")
    )]
    async fn authorize_payment(
        self: Arc<Self>,
        credit_ok: bool,
        product: Product,
    ) -> Result<PaymentAuth, String> {
        println!("[OLD] Authorizing payment for: {}", product.name);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(PaymentAuth {
            transaction_id: "TXN-001".to_string(),
            authorized: credit_ok && product.stock > 0,
        })
    }

    // VERBOSE: Three dependencies duplicated in depends_on AND inputs
    #[step(
        depends_on = ["fetch_customer", "fetch_product", "authorize_payment"],
        inputs(
            customer = "fetch_customer",
            product = "fetch_product",
            auth = "authorize_payment"
        )
    )]
    async fn finalize_order(
        self: Arc<Self>,
        customer: Customer,
        product: Product,
        auth: PaymentAuth,
    ) -> Result<OrderResult, String> {
        println!("[OLD] Finalizing order");
        tokio::time::sleep(Duration::from_millis(50)).await;

        if !auth.authorized {
            return Err("Payment not authorized".to_string());
        }

        Ok(OrderResult {
            order_id: "ORD-001".to_string(),
            customer_name: customer.name,
            product_name: product.name,
            total_price: product.price,
        })
    }

    #[flow]
    async fn process_old(self: Arc<Self>) -> Result<OrderResult, String> {
        self.register_fetch_customer();
        self.register_validate_credit();
        self.register_fetch_product();
        self.register_authorize_payment();
        self.register_finalize_order()
    }
}

// ============================================================================
// AFTER: With Autowiring (Clean!)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderFlowNew {
    customer_id: String,
    product_id: String,
}

impl OrderFlowNew {
    fn new(customer_id: String, product_id: String) -> Self {
        Self {
            customer_id,
            product_id,
        }
    }

    #[step]
    async fn fetch_customer(self: Arc<Self>) -> Result<Customer, String> {
        println!("[NEW] Fetching customer: {}", self.customer_id);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(Customer {
            id: self.customer_id.clone(),
            name: "Alice".to_string(),
            credit_score: 750,
        })
    }

    // CLEAN: Only inputs needed! Auto-depends on fetch_customer
    #[step(inputs(customer = "fetch_customer"))]
    async fn validate_credit(
        self: Arc<Self>,
        customer: Customer,
    ) -> Result<bool, String> {
        println!("[NEW] Validating credit for: {}", customer.name);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(customer.credit_score > 600)
    }

    // PARALLEL: Also depends on fetch_customer (via explicit depends_on)
    // Runs in parallel with validate_credit!
    #[step(depends_on = "fetch_customer")]
    async fn fetch_product(self: Arc<Self>) -> Result<Product, String> {
        println!("[NEW] Fetching product: {} (parallel!)", self.product_id);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(Product {
            id: self.product_id.clone(),
            name: "Widget".to_string(),
            price: 99.99,
            stock: 10,
        })
    }

    // CLEAN: Just inputs! Auto-depends on validate_credit AND fetch_product
    #[step(inputs(credit_ok = "validate_credit", product = "fetch_product"))]
    async fn authorize_payment(
        self: Arc<Self>,
        credit_ok: bool,
        product: Product,
    ) -> Result<PaymentAuth, String> {
        println!("[NEW] Authorizing payment for: {}", product.name);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(PaymentAuth {
            transaction_id: "TXN-001".to_string(),
            authorized: credit_ok && product.stock > 0,
        })
    }

    // CLEAN: Just inputs! Auto-depends on ALL three steps
    #[step(inputs(
        customer = "fetch_customer",
        product = "fetch_product",
        auth = "authorize_payment"
    ))]
    async fn finalize_order(
        self: Arc<Self>,
        customer: Customer,
        product: Product,
        auth: PaymentAuth,
    ) -> Result<OrderResult, String> {
        println!("[NEW] Finalizing order");
        tokio::time::sleep(Duration::from_millis(50)).await;

        if !auth.authorized {
            return Err("Payment not authorized".to_string());
        }

        Ok(OrderResult {
            order_id: "ORD-001".to_string(),
            customer_name: customer.name,
            product_name: product.name,
            total_price: product.price,
        })
    }

    #[flow]
    async fn process_new(self: Arc<Self>) -> Result<OrderResult, String> {
        self.register_fetch_customer();
        self.register_validate_credit();  // Auto-depends on fetch_customer
        self.register_fetch_product();    // Parallel with validate_credit
        self.register_authorize_payment(); // Auto-depends on both above
        self.register_finalize_order()    // Auto-depends on all three
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Sequential vs Parallel with Autowiring                  ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let storage = Arc::new(InMemoryExecutionLog::new());

    // =========================================================================
    // OLD WAY: Verbose with Duplicated Dependencies
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ OLD WAY: Explicit depends_on + inputs (Verbose)          │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    storage.reset().await?;
    let old_flow = Arc::new(OrderFlowOld::new(
        "CUST-001".to_string(),
        "PROD-123".to_string(),
    ));
    let instance1 = Ergon::new_flow(Arc::clone(&old_flow), Uuid::new_v4(), Arc::clone(&storage));
    let result1 = instance1.execute(|f| f.process_old()).await;

    println!("\nResult: {:?}\n", result1);
    println!("Code Example:");
    println!("  #[step(depends_on = \"fetch_customer\", inputs(customer = \"fetch_customer\"))]");
    println!("  async fn validate_credit(customer: Customer) -> Result<bool>");
    println!("\n  Problem: 'fetch_customer' appears TWICE! 💢\n");

    // =========================================================================
    // NEW WAY: Clean with Autowiring
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ NEW WAY: Autowiring with inputs (Clean!)                 │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    storage.reset().await?;
    let new_flow = Arc::new(OrderFlowNew::new(
        "CUST-001".to_string(),
        "PROD-123".to_string(),
    ));
    let instance2 = Ergon::new_flow(Arc::clone(&new_flow), Uuid::new_v4(), Arc::clone(&storage));
    let result2 = instance2.execute(|f| f.process_new()).await;

    println!("\nResult: {:?}\n", result2);
    println!("Code Example:");
    println!("  #[step(inputs(customer = \"fetch_customer\"))]");
    println!("  async fn validate_credit(customer: Customer) -> Result<bool>");
    println!("\n  Benefit: 'fetch_customer' appears ONCE! ✓\n");

    // =========================================================================
    // COMPARISON
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ COMPARISON: Lines of Code Saved                          │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("OLD (finalize_order):");
    println!("  #[step(");
    println!("      depends_on = [\"fetch_customer\", \"fetch_product\", \"authorize_payment\"],");
    println!("      inputs(");
    println!("          customer = \"fetch_customer\",");
    println!("          product = \"fetch_product\",");
    println!("          auth = \"authorize_payment\"");
    println!("      )");
    println!("  )]");
    println!("  Total: 7 lines, 6 step name references\n");

    println!("NEW (finalize_order):");
    println!("  #[step(inputs(");
    println!("      customer = \"fetch_customer\",");
    println!("      product = \"fetch_product\",");
    println!("      auth = \"authorize_payment\"");
    println!("  ))]");
    println!("  Total: 5 lines, 3 step name references\n");

    println!("Savings: 28% fewer lines, 50% fewer step name references!\n");

    // =========================================================================
    // KEY BENEFITS
    // =========================================================================
    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ KEY BENEFITS OF AUTOWIRING                                │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("✓ DRY Principle: Step names appear only once");
    println!("✓ Type Safety: Dependencies match data flow");
    println!("✓ Easier Refactoring: Rename step in one place");
    println!("✓ More Readable: See data deps in function signature");
    println!("✓ Less Error-Prone: Can't forget to update depends_on");
    println!("✓ Natural Parallelism: Same parent = parallel execution\n");

    println!("┌───────────────────────────────────────────────────────────┐");
    println!("│ EXECUTION GRAPH                                           │");
    println!("└───────────────────────────────────────────────────────────┘\n");

    println!("  fetch_customer");
    println!("       ├─────────────┬──────────────┐");
    println!("       │             │              │");
    println!("  validate_credit  fetch_product   │ (parallel)");
    println!("       └──────┬──────┘              │");
    println!("              │                     │");
    println!("       authorize_payment ───────────┘");
    println!("              │");
    println!("       finalize_order\n");

    println!("Note: validate_credit and fetch_product run in PARALLEL");
    println!("      because they both depend on fetch_customer!\n");

    Ok(())
}
