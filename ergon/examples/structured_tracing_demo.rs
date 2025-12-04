//! Structured Tracing Demo with Typestate Pattern
//!
//! This example demonstrates:
//! - Zero-cost tracing abstraction using the typestate pattern
//! - Enabling structured tracing with .with_structured_tracing()
//! - Automatic span hierarchy for flow and step execution
//! - Structured fields for debugging production issues
//! - Combining tracing with timer processing
//! - Compile-time enforcement of tracing configuration
//!
//! ## Scenario
//! An order processing flow with three steps (validate, process payment, fulfill).
//! We demonstrate three tracing modes: basic (no structured spans), structured
//! (with span hierarchy), and full spans (with entry/exit events). The typestate
//! pattern ensures tracing configuration is known at compile time.
//!
//! ## Key Takeaways
//! - Typestate pattern provides zero-cost abstraction for tracing
//! - with_structured_tracing() enables automatic span creation
//! - Each flow and step gets its own span with structured fields
//! - Span hierarchy shows parent-child relationships in traces
//! - Structured fields include flow_id, step_name, class_name
//! - Configuration is compile-time checked via type system
//! - No runtime overhead when tracing is disabled
//!
//! ## Run with
//! ```bash
//! # Basic (no structured spans)
//! cargo run --example structured_tracing_demo
//!
//! # With structured spans
//! cargo run --example structured_tracing_demo -- --structured
//!
//! # With full span events (entry/exit)
//! cargo run --example structured_tracing_demo -- --structured --full-spans
//! ```

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct OrderFlow {
    order_id: String,
    amount: f64,
    customer: String,
}

impl OrderFlow {
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<String, String> {
        info!(
            "Processing order {} for customer {}",
            self.order_id, self.customer
        );

        let validation = self.clone().validate_order().await?;
        let payment = self.clone().process_payment(validation).await?;
        let fulfillment = self.clone().fulfill_order(payment).await?;

        Ok(fulfillment)
    }

    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<String, String> {
        info!("Validating order {}", self.order_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        if self.amount <= 0.0 {
            return Err("Invalid amount".to_string());
        }

        Ok(format!("Validated-{}", self.order_id))
    }

    #[step(inputs(validation = "validate_order"))]
    async fn process_payment(self: Arc<Self>, validation: String) -> Result<String, String> {
        info!("Processing payment for {}", validation);
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Simulate occasional payment failure
        if self.amount > 10000.0 {
            warn!("Payment declined for high amount: ${}", self.amount);
            return Err("Payment declined".to_string());
        }

        Ok(format!("Payment-{}", self.order_id))
    }

    #[step(inputs(payment = "process_payment"))]
    async fn fulfill_order(self: Arc<Self>, payment: String) -> Result<String, String> {
        info!("Fulfilling order with {}", payment);
        tokio::time::sleep(Duration::from_millis(150)).await;

        Ok(format!("Fulfilled-{}", self.order_id))
    }
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct ShippingFlow {
    order_id: String,
    address: String,
}

impl ShippingFlow {
    #[flow]
    async fn ship_order(self: Arc<Self>) -> Result<String, String> {
        info!("Shipping order {} to {}", self.order_id, self.address);

        let label = self.clone().generate_label().await?;
        let tracking = self.clone().ship_package(label).await?;

        Ok(tracking)
    }

    #[step]
    async fn generate_label(self: Arc<Self>) -> Result<String, String> {
        info!("Generating shipping label");
        tokio::time::sleep(Duration::from_millis(80)).await;
        Ok(format!("Label-{}", self.order_id))
    }

    #[step(inputs(label = "generate_label"))]
    async fn ship_package(self: Arc<Self>, label: String) -> Result<String, String> {
        info!("Shipping package with {}", label);
        tokio::time::sleep(Duration::from_millis(120)).await;
        Ok(format!("Tracking-{}", self.order_id))
    }
}

fn init_tracing(structured: bool, full_spans: bool) {
    if structured {
        // Initialize with structured tracing and span events
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_target(true)
            .with_thread_ids(true)
            .with_level(true);

        let fmt_layer = if full_spans {
            fmt_layer.with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        } else {
            fmt_layer.with_span_events(
                tracing_subscriber::fmt::format::FmtSpan::NEW
                    | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
            )
        };

        tracing_subscriber::registry().with(fmt_layer).init();

        info!("Initialized with structured tracing (spans enabled)");
    } else {
        // Initialize basic tracing (no span events)
        tracing_subscriber::fmt()
            .with_target(false)
            .with_thread_ids(false)
            .with_level(true)
            .init();

        info!("Initialized with basic tracing (no structured spans)");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line args
    let args: Vec<String> = std::env::args().collect();
    let structured = args.contains(&"--structured".to_string());
    let full_spans = args.contains(&"--full-spans".to_string());

    // Initialize tracing based on flags
    init_tracing(structured, full_spans);

    // Create storage
    let storage = Arc::new(SqliteExecutionLog::in_memory().await?);

    println!("\n=== Structured Tracing Demo ===\n");

    if structured {
        println!("✓ Structured tracing ENABLED");
        println!("  - Worker loop spans will be created");
        println!("  - Flow execution spans with structured fields");
        println!("  - Timer processing spans (if timers enabled)");
        println!("  - Rich debugging context for production\n");

        // Create worker with structured tracing
        let worker = Worker::new(storage.clone(), "worker-1")
            .with_structured_tracing() // Enable structured spans
            .with_poll_interval(Duration::from_millis(100));

        worker
            .register(|flow: Arc<OrderFlow>| flow.process_order())
            .await;
        worker
            .register(|flow: Arc<ShippingFlow>| flow.ship_order())
            .await;

        let handle = worker.start().await;

        println!("Worker started with STRUCTURED TRACING\n");
        println!("Expected span hierarchy:");
        println!("  worker_loop [worker.id=\"worker-1\"]");
        println!("  └─ flow_execution [flow.id, flow.type, task.id]");
        println!("     └─ (your flow steps execute here)\n");

        // Schedule some flows
        let scheduler = Scheduler::new(storage.clone());

        info!("Scheduling flows...");
        let order1 = OrderFlow {
            order_id: "ORD-001".to_string(),
            amount: 99.99,
            customer: "Alice".to_string(),
        };
        scheduler.schedule(order1, Uuid::new_v4()).await?;

        let order2 = OrderFlow {
            order_id: "ORD-002".to_string(),
            amount: 150.50,
            customer: "Bob".to_string(),
        };
        scheduler.schedule(order2, Uuid::new_v4()).await?;

        let shipping = ShippingFlow {
            order_id: "ORD-001".to_string(),
            address: "123 Main St".to_string(),
        };
        scheduler.schedule(shipping, Uuid::new_v4()).await?;

        // Wait for flows to complete
        tokio::time::sleep(Duration::from_secs(2)).await;

        handle.shutdown().await;
        info!("Worker shut down gracefully");
    } else {
        println!("✗ Structured tracing DISABLED (default)");
        println!("  - Using basic log-style tracing only");
        println!("  - Zero overhead - no span creation");
        println!("  - Simple info/debug/warn/error messages\n");

        // Create worker WITHOUT structured tracing (default)
        let worker =
            Worker::new(storage.clone(), "worker-1").with_poll_interval(Duration::from_millis(100));

        worker
            .register(|flow: Arc<OrderFlow>| flow.process_order())
            .await;
        worker
            .register(|flow: Arc<ShippingFlow>| flow.ship_order())
            .await;

        let handle = worker.start().await;

        println!("Worker started with BASIC TRACING (zero-cost)\n");

        // Schedule some flows
        let scheduler = Scheduler::new(storage.clone());

        info!("Scheduling flows...");
        let order1 = OrderFlow {
            order_id: "ORD-001".to_string(),
            amount: 99.99,
            customer: "Alice".to_string(),
        };
        scheduler.schedule(order1, Uuid::new_v4()).await?;

        let order2 = OrderFlow {
            order_id: "ORD-002".to_string(),
            amount: 150.50,
            customer: "Bob".to_string(),
        };
        scheduler.schedule(order2, Uuid::new_v4()).await?;

        let shipping = ShippingFlow {
            order_id: "ORD-001".to_string(),
            address: "123 Main St".to_string(),
        };
        scheduler.schedule(shipping, Uuid::new_v4()).await?;

        // Wait for flows to complete
        tokio::time::sleep(Duration::from_secs(2)).await;

        handle.shutdown().await;
        info!("Worker shut down gracefully");
    }

    println!("\n=== Demo Complete ===\n");
    println!("Key Takeaways:");
    println!("1. Structured tracing is OPT-IN via .with_structured_tracing()");
    println!("2. Default workers have ZERO overhead (no span creation)");
    println!("3. Structured spans provide rich context: worker.id, flow.id, flow.type, task.id");
    println!("4. Follows typestate pattern just like timer processing");
    println!("5. Can be combined: .with_timers().with_structured_tracing()");
    println!("\nTry running with --structured flag to see the difference!");

    Ok(())
}
