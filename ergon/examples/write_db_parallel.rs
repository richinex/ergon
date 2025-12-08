//! Parallel Database Writes - E-commerce Order Processing
//!
//! This example demonstrates a real-world scenario where a workflow writes
//! analytics events to a separate database. Independent operations run in
//! parallel for better throughput.
//!
//! ## Scenario
//! An e-commerce order goes through multiple stages:
//! 1. Validate payment
//! 2. Reserve inventory
//! 3. Create shipment + Update analytics (parallel)
//! 4. Send notification
//!
//! Multiple analytics operations run concurrently when they don't depend on each other.
//!
//! ## Architecture
//! - **Ergon Database**: Tracks workflow execution state (SQLite)
//! - **Analytics Database**: Stores business events (separate SQLite)
//!
//! ## Run with
//! ```bash
//! cargo run --example write_db_parallel --features sqlite
//! ```

use ergon::executor::{InvokeChild, Worker};
use ergon::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};
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

async fn query_analytics_events(
    pool: &SqlitePool,
    order_id: &str,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let rows = sqlx::query(
        r#"
        SELECT event_type, metadata
        FROM analytics_events
        WHERE order_id = ?
        ORDER BY created_at ASC
        "#,
    )
    .bind(order_id)
    .fetch_all(pool)
    .await?;

    let events = rows
        .into_iter()
        .map(|row| {
            let event_type: String = row.get("event_type");
            let metadata: String = row.get("metadata");
            (event_type, metadata)
        })
        .collect();

    Ok(events)
}

// =============================================================================
// Parent Flow - Order Processing (Parallel)
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
        println!("[Step 1] ✅ Payment validated: ${:.2}", self.amount);
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
        println!("[Step 2] ✅ Inventory reserved: {} items", self.items.len());
        Ok(())
    }

    // These three operations run in PARALLEL after inventory is reserved
    #[step(depends_on = "reserve_inventory")]
    async fn create_shipment(self: Arc<Self>) -> Result<String, String> {
        println!("[Step 3a] Creating shipment for order {} (parallel)", self.order_id);

        let result = self
            .invoke(ShipmentCreation {
                order_id: self.order_id.clone(),
                items: self.items.clone(),
                analytics_db_path: self.analytics_db_path.clone(),
            })
            .result()
            .await
            .map_err(|e| e.to_string())?;

        println!("[Step 3a] ✅ Shipment created: {}", result);
        Ok(result)
    }

    #[step(depends_on = "reserve_inventory")]
    async fn write_revenue_analytics(self: Arc<Self>) -> Result<(), String> {
        println!("[Step 3b] Writing revenue analytics (parallel)");

        self.invoke(RevenueAnalytics {
            order_id: self.order_id.clone(),
            amount: self.amount,
            analytics_db_path: self.analytics_db_path.clone(),
        })
        .result()
        .await
        .map_err(|e| e.to_string())?;

        println!("[Step 3b] ✅ Revenue analytics written");
        Ok(())
    }

    #[step(depends_on = "reserve_inventory")]
    async fn write_inventory_analytics(self: Arc<Self>) -> Result<(), String> {
        println!("[Step 3c] Writing inventory analytics (parallel)");

        self.invoke(InventoryAnalytics {
            order_id: self.order_id.clone(),
            items: self.items.clone(),
            analytics_db_path: self.analytics_db_path.clone(),
        })
        .result()
        .await
        .map_err(|e| e.to_string())?;

        println!("[Step 3c] ✅ Inventory analytics written");
        Ok(())
    }

    // Notification waits for all parallel operations to complete
    #[step(depends_on = ["create_shipment", "write_revenue_analytics", "write_inventory_analytics"])]
    async fn send_notification(self: Arc<Self>) -> Result<(), String> {
        println!(
            "[Step 4] Sending notification to {} (after parallel ops)",
            self.customer_email
        );

        self.invoke(NotificationSender {
            order_id: self.order_id.clone(),
            customer_email: self.customer_email.clone(),
            analytics_db_path: self.analytics_db_path.clone(),
        })
        .result()
        .await
        .map_err(|e| e.to_string())?;

        println!("[Step 4] ✅ Notification sent");
        Ok(())
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<(), String> {
        // Parallel execution using DAG
        dag! {
            self.register_validate_payment();
            self.register_reserve_inventory();
            // These three run in parallel:
            self.register_create_shipment();
            self.register_write_revenue_analytics();
            self.register_write_inventory_analytics();
            // Waits for all parallel ops:
            self.register_send_notification()
        }
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
// Child Flow 2 - Revenue Analytics
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnalyticsResult {
    recorded: bool,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = AnalyticsResult)]
struct RevenueAnalytics {
    order_id: String,
    amount: f64,
    analytics_db_path: String,
}

impl RevenueAnalytics {
    #[flow]
    async fn write(self: Arc<Self>) -> Result<AnalyticsResult, String> {
        println!("  [Child Flow] Writing revenue analytics");
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Write analytics event
        let pool = SqlitePool::connect(&format!("sqlite:{}", self.analytics_db_path))
            .await
            .map_err(|e| e.to_string())?;

        write_analytics_event(
            &pool,
            "revenue_tracked",
            &self.order_id,
            &format!("revenue=${:.2}", self.amount),
        )
        .await
        .map_err(|e| e.to_string())?;

        pool.close().await;
        Ok(AnalyticsResult { recorded: true })
    }
}

// =============================================================================
// Child Flow 3 - Inventory Analytics
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
#[invokable(output = AnalyticsResult)]
struct InventoryAnalytics {
    order_id: String,
    items: Vec<String>,
    analytics_db_path: String,
}

impl InventoryAnalytics {
    #[flow]
    async fn write(self: Arc<Self>) -> Result<AnalyticsResult, String> {
        println!("  [Child Flow] Writing inventory analytics");
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Write analytics event
        let pool = SqlitePool::connect(&format!("sqlite:{}", self.analytics_db_path))
            .await
            .map_err(|e| e.to_string())?;

        write_analytics_event(
            &pool,
            "inventory_tracked",
            &self.order_id,
            &format!("items_count={}", self.items.len()),
        )
        .await
        .map_err(|e| e.to_string())?;

        pool.close().await;
        Ok(AnalyticsResult { recorded: true })
    }
}

// =============================================================================
// Child Flow 4 - Notification Sender
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
        println!(
            "  [Child Flow] Sending email to {}",
            self.customer_email
        );
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
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║      Parallel DB Writes - E-commerce Order Processing        ║");
    println!("╚═══════════════════════════════════════════════════════════════╝\n");

    // Setup databases
    let ergon_db = "data/ergon_write_db_parallel.db";
    let analytics_db = "data/analytics_parallel.db";

    let _ = std::fs::remove_file(ergon_db);
    let _ = std::fs::remove_file(analytics_db);

    println!("📦 Setting up databases...");
    let analytics_pool = setup_analytics_db(analytics_db).await?;
    let storage = Arc::new(SqliteExecutionLog::new(ergon_db).await?);
    println!("✅ Databases ready\n");

    println!("═══════════════════════════════════════════════════════════════\n");

    // Create order
    let order_id = format!("ORD-{}", &Uuid::new_v4().to_string()[..8]);
    println!("🛒 Processing order: {}\n", order_id);

    let order = OrderProcessing {
        order_id: order_id.clone(),
        amount: 299.99,
        items: vec!["Laptop".into(), "Mouse".into(), "Keyboard".into()],
        customer_email: "customer@example.com".into(),
        analytics_db_path: analytics_db.to_string(),
    };

    // Schedule and process
    let scheduler = Scheduler::new(storage.clone());
    let task_id = scheduler.schedule(order, Uuid::new_v4()).await?;
    println!("📋 Scheduled task: {}\n", &task_id.to_string()[..8]);

    // Start worker
    let worker =
        Worker::new(storage.clone(), "worker-1").with_poll_interval(Duration::from_millis(50));

    worker
        .register(|f: Arc<OrderProcessing>| f.process())
        .await;
    worker
        .register(|f: Arc<ShipmentCreation>| f.create())
        .await;
    worker
        .register(|f: Arc<RevenueAnalytics>| f.write())
        .await;
    worker
        .register(|f: Arc<InventoryAnalytics>| f.write())
        .await;
    worker
        .register(|f: Arc<NotificationSender>| f.send())
        .await;

    let handle = worker.start().await;

    // Wait for completion
    tokio::time::sleep(Duration::from_secs(4)).await;
    handle.shutdown().await;

    println!("\n═══════════════════════════════════════════════════════════════\n");

    // Query and verify analytics events
    println!("📊 Querying Analytics Database...\n");
    let events = query_analytics_events(&analytics_pool, &order_id).await?;

    println!("┌─────────────────────────────────────────────────────────────┐");
    println!("│ Analytics Events (in order)                                 │");
    println!("├─────────────────────────────────────────────────────────────┤");
    for (i, (event_type, metadata)) in events.iter().enumerate() {
        println!("│ {}. {:<30} │ {}", i + 1, event_type, metadata);
    }
    println!("└─────────────────────────────────────────────────────────────┘\n");

    // Verify expected events (order may vary for parallel writes)
    let expected_events = vec![
        "payment_validated",
        "inventory_reserved",
        "shipment_created",
        "revenue_tracked",
        "inventory_tracked",
        "notification_sent",
    ];

    let actual_events: Vec<String> = events.iter().map(|(e, _)| e.clone()).collect();
    let all_present = expected_events
        .iter()
        .all(|e| actual_events.contains(&e.to_string()));

    println!("┌─────────────────────────────────────────────────────────────┐");
    println!("│ Verification                                                │");
    println!("├─────────────────────────────────────────────────────────────┤");
    println!("│ Expected events: 6                                          │");
    println!("│ Found events:    {}                                          │", events.len());
    println!(
        "│ Status:          {:<42} │",
        if all_present && events.len() == 6 {
            "✅ All events written correctly"
        } else {
            "❌ Missing or extra events"
        }
    );
    println!("└─────────────────────────────────────────────────────────────┘\n");

    println!("💡 Key Insight:");
    println!("   Parallel execution runs independent analytics writes concurrently.");
    println!("   Steps 3a, 3b, and 3c execute simultaneously after step 2 completes.");
    println!("   The order of parallel events in the DB may vary between runs.\n");

    // Cleanup
    storage.close().await?;
    analytics_pool.close().await;

    Ok(())
}
