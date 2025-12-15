//! Sequential Database Writes - E-commerce Order Processing
//!
//! This example demonstrates a real-world scenario where a workflow writes
//! analytics events to a separate database. Each step in the order processing
//! pipeline writes tracking events sequentially.
//!
//! ## Scenario
//! An e-commerce order goes through multiple stages:
//! 1. Validate payment
//! 2. Reserve inventory
//! 3. Create shipment
//! 4. Send notification
//!
//! Each stage writes an analytics event to a separate SQLite database.
//! The workflow uses child flows and sequential execution.
//!
//! ## Architecture
//! - **Ergon Database**: Tracks workflow execution state (SQLite)
//! - **Analytics Database**: Stores business events (separate SQLite)
//!
//! ## Run with
//! ```bash
//! cargo run --example write_db_sequential --features sqlite
//! ```

use ergon::executor::{InvokeChild, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// =============================================================================
// Analytics Database Schema
// =============================================================================

async fn setup_analytics_db(db_path: &str) -> Result<SqlitePool, Box<dyn std::error::Error>> {
    let connect_options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true);

    let pool = SqlitePool::connect_with(connect_options).await?;

    // Create analytics events table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS analytics_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            order_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            metadata TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(&pool)
    .await?;

    Ok(pool)
}

async fn write_analytics_event(
    pool: &SqlitePool,
    event_type: &str,
    order_id: &str,
    metadata: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let timestamp = chrono::Utc::now().to_rfc3339();

    let result = sqlx::query(
        r#"
        INSERT INTO analytics_events (event_type, order_id, timestamp, metadata)
        VALUES (?, ?, ?, ?)
        "#,
    )
    .bind(event_type)
    .bind(order_id)
    .bind(timestamp)
    .bind(metadata)
    .execute(pool)
    .await?;

    Ok(result.last_insert_rowid())
}

// =============================================================================
// Parent Flow - Order Processing
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct OrderProcessing {
    order_id: String,
    amount: f64,
    items: Vec<String>,
    customer_email: String,
    analytics_db_path: String,
}

impl OrderProcessing {
    #[step]
    async fn validate_payment(self: Arc<Self>) -> Result<(), String> {
        println!("[Step 1] Validating payment for order {}", self.order_id);
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Write analytics event
        let pool = SqlitePool::connect(&format!("sqlite:{}", self.analytics_db_path))
            .await
            .map_err(|e| e.to_string())?;

        write_analytics_event(
            &pool,
            "payment_validated",
            &self.order_id,
            &format!("amount=${:.2}", self.amount),
        )
        .await
        .map_err(|e| e.to_string())?;

        pool.close().await;
        println!("[Step 1] Payment validated: ${:.2}", self.amount);
        Ok(())
    }

    #[step(depends_on = "validate_payment")]
    async fn reserve_inventory(self: Arc<Self>) -> Result<(), String> {
        println!("[Step 2] Reserving inventory for order {}", self.order_id);
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Write analytics event
        let pool = SqlitePool::connect(&format!("sqlite:{}", self.analytics_db_path))
            .await
            .map_err(|e| e.to_string())?;

        write_analytics_event(
            &pool,
            "inventory_reserved",
            &self.order_id,
            &format!("items={}", self.items.join(", ")),
        )
        .await
        .map_err(|e| e.to_string())?;

        pool.close().await;
        println!("[Step 2] Inventory reserved: {} items", self.items.len());
        Ok(())
    }

    #[step(depends_on = "reserve_inventory")]
    async fn process_shipment(self: Arc<Self>, result: String) -> Result<String, String> {
        println!(
            "[Step 3] Processing shipment result for order {}",
            self.order_id
        );
        println!("[Step 3] Shipment created: {}", result);
        Ok(result)
    }

    #[step(depends_on = "reserve_inventory")]
    async fn process_notification(self: Arc<Self>) -> Result<(), String> {
        println!("[Step 4] Notification processing complete");
        println!("[Step 4] Notification sent");
        Ok(())
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<(), String> {
        // Sequential execution: each step waits for the previous
        self.clone().validate_payment().await?;
        self.clone().reserve_inventory().await?;

        // Invoke child flow at flow level
        println!("[Flow] Creating shipment for order {}", self.order_id);
        let shipment_result = self
            .invoke(ShipmentCreation {
                order_id: self.order_id.clone(),
                items: self.items.clone(),
                analytics_db_path: self.analytics_db_path.clone(),
            })
            .result()
            .await
            .map_err(|e| e.to_string())?;

        // Process shipment result in atomic step
        self.clone().process_shipment(shipment_result).await?;

        // Invoke notification child flow at flow level
        println!("[Flow] Sending notification to {}", self.customer_email);
        self.invoke(NotificationSender {
            order_id: self.order_id.clone(),
            customer_email: self.customer_email.clone(),
            analytics_db_path: self.analytics_db_path.clone(),
        })
        .result()
        .await
        .map_err(|e| e.to_string())?;

        // Process notification in atomic step
        self.clone().process_notification().await?;
        Ok(())
    }
}

// =============================================================================
// Child Flow 1 - Shipment Creation
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = String)]
struct ShipmentCreation {
    order_id: String,
    items: Vec<String>,
    analytics_db_path: String,
}

impl ShipmentCreation {
    #[flow]
    async fn create(self: Arc<Self>) -> Result<String, String> {
        println!("  [Child Flow] Creating shipment for {}", self.order_id);
        tokio::time::sleep(Duration::from_millis(200)).await;

        let tracking_number = format!("TRK-{}", &Uuid::new_v4().to_string()[..8]);

        // Write analytics event
        let pool = SqlitePool::connect(&format!("sqlite:{}", self.analytics_db_path))
            .await
            .map_err(|e| e.to_string())?;

        write_analytics_event(
            &pool,
            "shipment_created",
            &self.order_id,
            &format!("tracking={}", tracking_number),
        )
        .await
        .map_err(|e| e.to_string())?;

        pool.close().await;
        Ok(tracking_number)
    }
}

// =============================================================================
// Child Flow 2 - Notification Sender
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NotificationResult {
    sent: bool,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = NotificationResult)]
struct NotificationSender {
    order_id: String,
    customer_email: String,
    analytics_db_path: String,
}

impl NotificationSender {
    #[flow]
    async fn send(self: Arc<Self>) -> Result<NotificationResult, String> {
        println!("  [Child Flow] Sending email to {}", self.customer_email);
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Write analytics event
        let pool = SqlitePool::connect(&format!("sqlite:{}", self.analytics_db_path))
            .await
            .map_err(|e| e.to_string())?;

        write_analytics_event(
            &pool,
            "notification_sent",
            &self.order_id,
            &format!("email={}", self.customer_email),
        )
        .await
        .map_err(|e| e.to_string())?;

        pool.close().await;
        Ok(NotificationResult { sent: true })
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup databases
    let ergon_db = "data/ergon_write_db_sequential.db";
    let analytics_db = "data/analytics_sequential.db";

    let _ = std::fs::remove_file(ergon_db);
    let _ = std::fs::remove_file(analytics_db);

    let analytics_pool = setup_analytics_db(analytics_db).await?;
    let storage = Arc::new(SqliteExecutionLog::new(ergon_db).await?);

    // Create order
    let order_id = format!("ORD-{}", &Uuid::new_v4().to_string()[..8]);

    let order = OrderProcessing {
        order_id: order_id.clone(),
        amount: 299.99,
        items: vec!["Laptop".into(), "Mouse".into(), "Keyboard".into()],
        customer_email: "customer@example.com".into(),
        analytics_db_path: analytics_db.to_string(),
    };

    // Schedule and process
    let scheduler = Scheduler::new(storage.clone());
    scheduler.schedule(order, Uuid::new_v4()).await?;

    // Start worker
    let worker =
        Worker::new(storage.clone(), "worker-1").with_poll_interval(Duration::from_millis(50));

    worker.register(|f: Arc<OrderProcessing>| f.process()).await;
    worker.register(|f: Arc<ShipmentCreation>| f.create()).await;
    worker.register(|f: Arc<NotificationSender>| f.send()).await;

    let handle = worker.start().await;

    // Wait for completion (sequential takes longer than parallel)
    tokio::time::sleep(Duration::from_secs(4)).await;
    handle.shutdown().await;

    // Cleanup
    storage.close().await?;
    analytics_pool.close().await;

    Ok(())
}
