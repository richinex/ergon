//! Axum + Ergon: Durable Order Processing API
//!
//! This example demonstrates:
//! - Integrating Ergon with Axum web framework
//! - Using SQLite as durable backend storage
//! - Building REST API endpoints for flow management
//! - Flows surviving server restarts and resuming automatically
//! - Querying flow status and results via HTTP
//! - Retrying failed flows through API
//!
//! ## Scenario
//! A REST API exposes three endpoints: POST /orders (submit order), GET /orders/:id
//! (check status), and POST /orders/:id/retry (retry failed order). Each order goes
//! through a 4-step flow: validate, charge payment, fulfill, notify. If the server
//! crashes mid-flow, the flow automatically resumes from the last completed step on
//! restart thanks to SQLite persistence.
//!
//! ## Key Takeaways
//! - Ergon integrates seamlessly with Axum using shared Arc state
//! - Scheduler enqueues flows, Worker processes them asynchronously
//! - SQLite backend provides durability across server restarts
//! - API returns immediately with flow_id while processing continues in background
//! - get_invocations_for_flow enables status querying
//! - Manual retry endpoint demonstrates programmatic flow restart
//! - Each step is idempotent and crash-safe
//!
//! ## Run with
//! ```bash
//! cargo run --example axum_order_processing --features sqlite
//! ```
//!
//! ## Testing
//! ```bash
//! # Submit an order
//! curl -X POST http://localhost:3000/orders \
//!   -H "Content-Type: application/json" \
//!   -d '{"item": "laptop", "quantity": 2, "customer_email": "user@example.com"}'
//!
//! # Check status (use flow_id from above)
//! curl http://localhost:3000/orders/{flow_id}
//!
//! # Retry a failed order
//! curl -X POST http://localhost:3000/orders/{flow_id}/retry
//! ```

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

// ===== Domain Types =====

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct OrderRequest {
    item: String,
    quantity: u32,
    customer_email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct OrderFlow {
    order_id: Uuid,
    item: String,
    quantity: u32,
    customer_email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct ValidationResult {
    order_id: Uuid,
    available: bool,
    price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct PaymentResult {
    order_id: Uuid,
    transaction_id: String,
    amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct FulfillmentResult {
    order_id: Uuid,
    tracking_number: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FlowType)]
struct OrderComplete {
    order_id: Uuid,
    transaction_id: String,
    tracking_number: String,
    notification_sent: bool,
}

// ===== Ergon Flow Implementation =====

impl OrderFlow {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<OrderComplete, String> {
        // Step 1: Validate order
        let validation = self.clone().validate_order().await?;

        // Step 2: Charge payment
        let payment = self.clone().charge_payment(validation).await?;

        // Step 3: Fulfill order
        let fulfillment = self.clone().fulfill_order(payment.clone()).await?;

        // Step 4: Send notification
        let notification = self
            .clone()
            .send_notification(payment.clone(), fulfillment.clone())
            .await?;

        Ok(OrderComplete {
            order_id: self.order_id,
            transaction_id: payment.transaction_id,
            tracking_number: fulfillment.tracking_number,
            notification_sent: notification,
        })
    }

    #[step]
    async fn validate_order(self: Arc<Self>) -> Result<ValidationResult, String> {
        // Simulate inventory and pricing check
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Check if item is in stock
        let available = self.quantity <= 10; // Max 10 items per order
        let price = match self.item.as_str() {
            "laptop" => 999.99,
            "mouse" => 29.99,
            "keyboard" => 79.99,
            _ => return Err(format!("Unknown item: {}", self.item)),
        };

        if !available {
            return Err(format!(
                "Item {} not available in quantity {}",
                self.item, self.quantity
            ));
        }

        Ok(ValidationResult {
            order_id: self.order_id,
            available,
            price: price * self.quantity as f64,
        })
    }

    #[step(inputs(validation = "validate_order"))]
    async fn charge_payment(
        self: Arc<Self>,
        validation: ValidationResult,
    ) -> Result<PaymentResult, String> {
        // Simulate payment gateway call
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Simulate occasional payment failures (10% failure rate)
        let random_val = (self.order_id.as_u128() % 10) as u32;
        if random_val == 0 {
            return Err("Payment gateway timeout - will retry".to_string());
        }

        Ok(PaymentResult {
            order_id: self.order_id,
            transaction_id: format!("txn_{}", Uuid::new_v4()),
            amount: validation.price,
        })
    }

    #[step(inputs(payment = "charge_payment"))]
    async fn fulfill_order(
        self: Arc<Self>,
        payment: PaymentResult,
    ) -> Result<FulfillmentResult, String> {
        // Simulate warehouse fulfillment
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        Ok(FulfillmentResult {
            order_id: self.order_id,
            tracking_number: format!("TRACK{}", &payment.transaction_id[4..12]),
        })
    }

    #[step(inputs(payment = "charge_payment", fulfillment = "fulfill_order"))]
    async fn send_notification(
        self: Arc<Self>,
        payment: PaymentResult,
        fulfillment: FulfillmentResult,
    ) -> Result<bool, String> {
        // Simulate sending email notification
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        println!(
            "[EMAIL] Sent to {}: Order {} confirmed. Amount: ${:.2}, Tracking: {}",
            self.customer_email, self.order_id, payment.amount, fulfillment.tracking_number
        );

        Ok(true)
    }
}

// ===== API Types =====

#[derive(Serialize, FlowType)]
struct OrderSubmitted {
    flow_id: Uuid,
    message: String,
}

#[derive(Serialize, FlowType)]
struct OrderStatus {
    flow_id: Uuid,
    status: String,
    result: Option<OrderComplete>,
    error: Option<String>,
}

// ===== Application State =====

#[derive(Clone, FlowType)]
struct AppState {
    storage: Arc<SqliteExecutionLog>,
    scheduler: Arc<Scheduler<SqliteExecutionLog>>,
}

// ===== API Handlers =====

async fn submit_order(
    State(state): State<AppState>,
    Json(request): Json<OrderRequest>,
) -> Result<Json<OrderSubmitted>, AppError> {
    let flow_id = Uuid::new_v4();

    let order = OrderFlow {
        order_id: flow_id,
        item: request.item.clone(),
        quantity: request.quantity,
        customer_email: request.customer_email.clone(),
    };

    // Schedule the order flow for execution
    state
        .scheduler
        .schedule(order, flow_id)
        .await
        .map_err(|e| AppError::InternalError(e.to_string()))?;

    Ok(Json(OrderSubmitted {
        flow_id,
        message: format!(
            "Order submitted successfully. Processing {} x{}",
            request.item, request.quantity
        ),
    }))
}

async fn get_order_status(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
) -> Result<Json<OrderStatus>, AppError> {
    // Get the latest invocation for this flow
    let latest = state
        .storage
        .get_latest_invocation(flow_id)
        .await
        .map_err(|e| AppError::InternalError(e.to_string()))?;

    match latest {
        None => Err(AppError::NotFound(format!("Order {} not found", flow_id))),
        Some(inv) => {
            let status_str = match inv.status() {
                InvocationStatus::Pending => "processing".to_string(),
                InvocationStatus::WaitingForSignal => "waiting".to_string(),
                InvocationStatus::WaitingForTimer => "waiting_for_timer".to_string(),
                InvocationStatus::Complete => "complete".to_string(),
            };

            let result = if inv.status() == InvocationStatus::Complete {
                inv.return_value()
                    .and_then(|bytes| serde_json::from_slice::<OrderComplete>(bytes).ok())
            } else {
                None
            };

            // Check if there's an error cached
            let has_error = state
                .storage
                .has_non_retryable_error(flow_id)
                .await
                .unwrap_or(false);

            let error = if has_error {
                Some("Order processing failed - check logs for details".to_string())
            } else {
                None
            };

            Ok(Json(OrderStatus {
                flow_id,
                status: status_str,
                result,
                error,
            }))
        }
    }
}

async fn retry_order(
    State(state): State<AppState>,
    Path(flow_id): Path<Uuid>,
) -> Result<Json<OrderSubmitted>, AppError> {
    // Get the stored flow data
    let invocations = state
        .storage
        .get_invocations_for_flow(flow_id)
        .await
        .map_err(|e| AppError::InternalError(e.to_string()))?;

    if invocations.is_empty() {
        return Err(AppError::NotFound(format!("Order {} not found", flow_id)));
    }

    // Get the first invocation to extract the original order data
    let first_inv = &invocations[0];
    let order: OrderFlow = serde_json::from_slice(first_inv.parameters())
        .map_err(|e| AppError::InternalError(format!("Failed to deserialize order: {}", e)))?;

    // Schedule retry with the same flow_id
    state
        .scheduler
        .schedule(order, flow_id)
        .await
        .map_err(|e| AppError::InternalError(e.to_string()))?;

    Ok(Json(OrderSubmitted {
        flow_id,
        message: "Order retry scheduled successfully".to_string(),
    }))
}

async fn health_check() -> &'static str {
    "OK"
}

// ===== Error Handling =====

enum AppError {
    NotFound(String),
    InternalError(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            AppError::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

// ===== Main =====

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("\nStarting Order Processing API with Ergon...\n");

    // Setup storage
    let db_path = "data/ergon_axum_orders.db";
    let storage = Arc::new(SqliteExecutionLog::new(db_path).await?);

    // Setup flow scheduler
    let scheduler = Arc::new(Scheduler::new(storage.clone()));

    // Create application state
    let state = AppState {
        storage: storage.clone(),
        scheduler,
    };

    // Start background workers to process flows
    println!("Starting 2 background workers...");
    for worker_id in 0..2 {
        let storage_clone = storage.clone();
        tokio::spawn(async move {
            let worker = Worker::new(storage_clone, format!("worker-{}", worker_id))
                .with_poll_interval(tokio::time::Duration::from_millis(100));

            worker.register(|flow: Arc<OrderFlow>| flow.process()).await;

            let handle = worker.start().await;

            // Workers run indefinitely
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    println!("Worker {} shutting down...", worker_id);
                    handle.shutdown().await;
                }
            }
        });
    }

    // Build the router
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/orders", post(submit_order))
        .route("/orders/{id}", get(get_order_status))
        .route("/orders/{id}/retry", post(retry_order))
        .with_state(state);

    // Start the server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("Server running on http://localhost:3000");
    println!("\nAPI Endpoints:");
    println!("  POST   /orders          - Submit a new order");
    println!("  GET    /orders/:id      - Get order status");
    println!("  POST   /orders/:id/retry - Retry a failed order");
    println!("  GET    /health          - Health check");
    println!("\nTry:");
    println!(
        r#"  curl -X POST http://localhost:3000/orders -H "Content-Type: application/json" -d '{{"item": "laptop", "quantity": 2, "customer_email": "user@example.com"}}'"#
    );
    println!("\nPress Ctrl+C to shutdown\n");

    // Setup graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    println!("Server shutdown complete");

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");
    println!("\nShutdown signal received, stopping server...");
}
