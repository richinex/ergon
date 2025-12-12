//! Child Flow Clean API Example
//!
//! This example demonstrates the progression of parent-child flow APIs:
//!
//! ## Level 0 (Manual - see child_flow_invocation.rs)
//! - 15 lines of boilerplate per child
//! - Manual storage manipulation
//! - Error-prone (method name typos, wrong serialization format)
//!
//! ## Level 1 (Helper Function - THIS EXAMPLE)
//! - `signal_parent_flow()` reduces to 1 line
//! - Still explicit but much cleaner
//! - Demonstrates the helper API
//!
//! ## Scenario
//!
//! A simple data processing pipeline:
//! - Parent: DataPipeline orchestrator
//! - Child 1: FetchDataFlow (fetches raw data)
//! - Child 2: TransformDataFlow (transforms the data)
//!
//! ## Run
//!
//! ```bash
//! cargo run --example child_flow_clean_api --features=sqlite
//! ```

use chrono::Utc;
use ergon::executor::ExecutionError;
use ergon::prelude::*;
use std::time::Duration;

// =============================================================================
// Domain Types
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawData {
    records: Vec<String>,
    count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransformedData {
    processed_records: Vec<String>,
    transformation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PipelineResult {
    total_records: usize,
    transformation_applied: String,
    status: String,
}

// =============================================================================
// Parent Flow - Data Pipeline Orchestrator
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct DataPipeline {
    source: String,
}

impl DataPipeline {
    #[flow]
    async fn run_pipeline(self: Arc<Self>) -> Result<PipelineResult, ExecutionError> {
        let parent_flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "\n[{}] PARENT[{}]: Starting pipeline for source: {}",
            format_time(),
            parent_flow_id,
            self.source
        );

        // Step 1: Fetch data (wait for child)
        println!(
            "[{}] PARENT[{}]: Waiting for data fetch...",
            format_time(),
            parent_flow_id
        );
        let raw_data = self.clone().await_fetch().await?;
        println!(
            "[{}] PARENT[{}]: Received {} records",
            format_time(),
            parent_flow_id,
            raw_data.count
        );

        // Step 2: Transform data (wait for child)
        println!(
            "[{}] PARENT[{}]: Waiting for transformation...",
            format_time(),
            parent_flow_id
        );
        let transformed = self.clone().await_transform().await?;
        println!(
            "[{}] PARENT[{}]: Data transformed using: {}",
            format_time(),
            parent_flow_id,
            transformed.transformation
        );

        let result = PipelineResult {
            total_records: transformed.processed_records.len(),
            transformation_applied: transformed.transformation,
            status: "Completed".to_string(),
        };

        println!(
            "\n[{}] PARENT[{}]: Pipeline completed successfully!",
            format_time(),
            parent_flow_id
        );

        Ok(result)
    }

    #[step]
    async fn await_fetch(self: Arc<Self>) -> Result<RawData, ExecutionError> {
        let result: RawData = await_external_signal("data-fetch").await?;
        Ok(result)
    }

    #[step]
    async fn await_transform(self: Arc<Self>) -> Result<TransformedData, ExecutionError> {
        let result: TransformedData = await_external_signal("data-transform").await?;
        Ok(result)
    }
}

// =============================================================================
// Child Flow 1 - Data Fetch Service (Level 1 API)
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct FetchDataFlow {
    parent_flow_id: Uuid,
    source: String,
}

impl FetchDataFlow {
    #[flow]
    async fn fetch_data(self: Arc<Self>) -> Result<RawData, ExecutionError> {
        let child_flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{}]   CHILD[fetch-{}]: Fetching data from {}",
            format_time(),
            child_flow_id,
            self.source
        );

        // Simulate data fetch
        self.clone().perform_fetch().await?;

        let result = RawData {
            records: vec![
                "record1".to_string(),
                "record2".to_string(),
                "record3".to_string(),
            ],
            count: 3,
        };

        println!(
            "[{}]   CHILD[fetch-{}]: Fetch complete, signaling parent",
            format_time(),
            child_flow_id
        );

        // ✨ LEVEL 1 API: Clean helper replaces 15 lines of boilerplate
        ergon::executor::signal_parent_flow(self.parent_flow_id, "await_fetch", result.clone())
            .await?;

        println!(
            "[{}]   CHILD[fetch-{}]: Parent signaled successfully",
            format_time(),
            child_flow_id
        );

        Ok(result)
    }

    #[step]
    async fn perform_fetch(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}]     -> Querying data source...", format_time());
        ergon::executor::schedule_timer(Duration::from_secs(1)).await?;
        Ok(())
    }
}

// =============================================================================
// Child Flow 2 - Data Transform Service (Level 1 API)
// =============================================================================

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct TransformDataFlow {
    parent_flow_id: Uuid,
}

impl TransformDataFlow {
    #[flow]
    async fn transform_data(self: Arc<Self>) -> Result<TransformedData, ExecutionError> {
        let child_flow_id = ergon::EXECUTION_CONTEXT
            .try_with(|ctx| ctx.id)
            .expect("Must be called within flow");

        println!(
            "[{}]   CHILD[transform-{}]: Transforming data",
            format_time(),
            child_flow_id
        );

        // Simulate transformation
        self.clone().apply_transformation().await?;

        let result = TransformedData {
            processed_records: vec![
                "TRANSFORMED_RECORD1".to_string(),
                "TRANSFORMED_RECORD2".to_string(),
                "TRANSFORMED_RECORD3".to_string(),
            ],
            transformation: "uppercase".to_string(),
        };

        println!(
            "[{}]   CHILD[transform-{}]: Transform complete, signaling parent",
            format_time(),
            child_flow_id
        );

        // ✨ LEVEL 1 API: Clean, simple, one line
        ergon::executor::signal_parent_flow(self.parent_flow_id, "await_transform", result.clone())
            .await?;

        println!(
            "[{}]   CHILD[transform-{}]: Parent signaled successfully",
            format_time(),
            child_flow_id
        );

        Ok(result)
    }

    #[step]
    async fn apply_transformation(self: Arc<Self>) -> Result<(), ExecutionError> {
        println!("[{}]     -> Applying transformation...", format_time());
        ergon::executor::schedule_timer(Duration::from_millis(500)).await?;
        Ok(())
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

fn format_time() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\nChild Flow Clean API - Level 1 Helper Example");
    println!("==============================================\n");
    println!("This example demonstrates the signal_parent_flow() helper API");
    println!("which reduces boilerplate from 15 lines to 1 line per child.\n");

    let storage = Arc::new(SqliteExecutionLog::new("clean_api_demo.db").await?);
    storage.reset().await?;

    println!("Starting worker...\n");

    let worker = Worker::new(storage.clone(), "data-worker")
        .with_timers()
        .with_timer_interval(Duration::from_millis(100))
        .with_poll_interval(Duration::from_millis(100));

    worker
        .register(|flow: Arc<DataPipeline>| flow.run_pipeline())
        .await;
    worker
        .register(|flow: Arc<FetchDataFlow>| flow.fetch_data())
        .await;
    worker
        .register(|flow: Arc<TransformDataFlow>| flow.transform_data())
        .await;

    let worker = worker.start().await;

    println!("  Worker started\n");

    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("Scheduling flows...\n");

    let scheduler = Scheduler::new(storage.clone());

    // Schedule parent flow
    let parent_flow = DataPipeline {
        source: "database".to_string(),
    };

    let parent_flow_id = Uuid::new_v4();
    scheduler.schedule(parent_flow, parent_flow_id).await?;
    println!("  Scheduled parent flow: {}", parent_flow_id);

    // Wait for parent to suspend
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Schedule child 1: Fetch
    let fetch_flow = FetchDataFlow {
        parent_flow_id,
        source: "database".to_string(),
    };
    let fetch_id = Uuid::new_v4();
    scheduler.schedule(fetch_flow, fetch_id).await?;
    println!("  Scheduled fetch child: {}", fetch_id);

    // Wait for fetch to complete and parent to suspend again
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Schedule child 2: Transform
    let transform_flow = TransformDataFlow { parent_flow_id };
    let transform_id = Uuid::new_v4();
    scheduler.schedule(transform_flow, transform_id).await?;
    println!("  Scheduled transform child: {}", transform_id);

    println!("\nWorker processing flows...\n");

    // Wait for completion
    let timeout_duration = Duration::from_secs(5);
    match tokio::time::timeout(timeout_duration, async {
        loop {
            let incomplete = storage.get_incomplete_flows().await?;
            if incomplete.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
    .await
    {
        Ok(_) => println!("\nAll flows completed!\n"),
        Err(_) => {
            println!("\nTimeout waiting for flows to complete\n");
            let incomplete = storage.get_incomplete_flows().await?;
            println!("Incomplete flows: {}", incomplete.len());
        }
    }

    worker.shutdown().await;

    println!("\n=== API Comparison ===\n");
    println!("Level 0 (Manual):");
    println!("  15 lines per child: get storage, serialize, find parent,");
    println!("  store params, resume flow, handle errors\n");
    println!("Level 1 (Helper - THIS EXAMPLE):");
    println!("  1 line: signal_parent_flow(parent_id, method_name, result)\n");
    println!("Level 2 (Future - Implicit):");
    println!("  0 lines: Just return Ok(result) - auto-signals parent\n");
    println!("Level 3 (Future - invoke() API):");
    println!("  Parent: self.invoke(Child{{...}}).result().await?");
    println!("  Child: Just return Ok(result)\n");

    storage.close().await?;
    Ok(())
}
