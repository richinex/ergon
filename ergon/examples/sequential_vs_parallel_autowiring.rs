//! Sequential vs parallel execution with autowiring
//!
//! This example demonstrates:
//! - How `inputs` autowiring creates cleaner, more maintainable code
//! - Comparison between explicit `depends_on` and automatic dependency inference
//! - DRY principle: avoiding duplication of step names
//! - Natural parallelism when multiple steps depend on the same parent
//! - Type-safe data flow with automatic dependency tracking
//!
//! ## Scenario
//! Order processing flow with both approaches:
//! - Old way: Explicit depends_on + inputs (verbose, duplicated step names)
//! - New way: Autowiring with inputs only (clean, DRY principle)
//!
//! Flow structure:
//! - fetch_customer (root)
//! - validate_credit + fetch_product (parallel - both depend on fetch_customer)
//! - authorize_payment (depends on validate_credit + fetch_product)
//! - finalize_order (depends on customer + product + auth)
//!
//! ## Key Takeaways
//! - `inputs` automatically adds referenced steps to dependencies (no depends_on duplication)
//! - Steps with same parent dependency execute in parallel naturally
//! - Autowiring makes refactoring easier (rename step in one place)
//! - Type safety prevents mismatched dependencies and data flow
//! - Code is 28% shorter and 50% fewer step name references with autowiring
//!
//! ## Run with
//! ```bash
//! cargo run --example sequential_vs_parallel_autowiring
//! ```

use ergon::Ergon;
use ergon::{flow, step};
use ergon::{ExecutionLog, InMemoryExecutionLog};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

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

    #[step(depends_on = "fetch_customer", inputs(customer = "fetch_customer"))]
    async fn validate_credit(self: Arc<Self>, customer: Customer) -> Result<bool, String> {
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

    #[step(inputs(customer = "fetch_customer"))]
    async fn validate_credit(self: Arc<Self>, customer: Customer) -> Result<bool, String> {
        println!("[NEW] Validating credit for: {}", customer.name);
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(customer.credit_score > 600)
    }

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
        self.register_validate_credit();
        self.register_fetch_product();
        self.register_authorize_payment();
        self.register_finalize_order()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nSequential vs Parallel with Autowiring");
    println!("=======================================\n");

    let storage = Arc::new(InMemoryExecutionLog::new());

    println!("OLD WAY: Explicit depends_on + inputs (Verbose)");
    println!("================================================\n");

    storage.reset().await?;
    let old_flow = Arc::new(OrderFlowOld::new(
        "CUST-001".to_string(),
        "PROD-123".to_string(),
    ));
    let instance1 = Ergon::new_flow(Arc::clone(&old_flow), Uuid::new_v4(), Arc::clone(&storage));
    let result1 = instance1.execute(|f| f.process_old()).await;

    println!("\nResult: {:?}\n", result1);

    println!("NEW WAY: Autowiring with inputs (Clean!)");
    println!("=========================================\n");

    storage.reset().await?;
    let new_flow = Arc::new(OrderFlowNew::new(
        "CUST-001".to_string(),
        "PROD-123".to_string(),
    ));
    let instance2 = Ergon::new_flow(Arc::clone(&new_flow), Uuid::new_v4(), Arc::clone(&storage));
    let result2 = instance2.execute(|f| f.process_new()).await;

    println!("\nResult: {:?}\n", result2);

    Ok(())
}
