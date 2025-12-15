//! Complete Redis distributed execution demo
//!
//! This example demonstrates a full distributed workflow by running both
//! scheduler and workers in a single demo. It shows:
//! - Scheduling multiple flows with data dependencies (inputs)
//! - Multiple workers processing jobs concurrently
//! - Real-time progress updates
//! - Final status reporting
//!
//! Prerequisites:
//! ```bash
//! docker run -d -p 6379:6379 redis:latest
//! ```
//!
//! Run with: cargo run --example distributed_redis_demo --features redis

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct DataPipeline {
    pipeline_id: String,
    batch_size: u32,
}

impl DataPipeline {
    #[flow]
    async fn process(self: Arc<Self>) -> Result<ProcessingResult, String> {
        println!(
            "[Pipeline {}] Starting data processing (batch size: {})",
            self.pipeline_id, self.batch_size
        );

        // Generate timestamp at flow level for determinism
        let transformed_at = chrono::Utc::now().timestamp();

        // Step 1: Load raw data
        let raw_data = self.clone().load_data().await?;

        // Step 2: Validate and transform (uses input from load_data)
        let validated = self
            .clone()
            .validate_and_transform(raw_data, transformed_at)
            .await?;

        // Step 3: Aggregate results (uses input from validate_and_transform)
        let result = self.clone().aggregate(validated).await?;

        println!(
            "[Pipeline {}] Completed with {} records",
            self.pipeline_id, result.total_records
        );
        Ok(result)
    }

    #[step]
    async fn load_data(self: Arc<Self>) -> Result<DataBatch, String> {
        println!("  [Load] Reading {} records from source", self.batch_size);
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(DataBatch {
            records: self.batch_size,
            source: format!("source_{}", self.pipeline_id),
        })
    }

    #[step(inputs(batch = "load_data"))]
    async fn validate_and_transform(
        self: Arc<Self>,
        batch: DataBatch,
        transformed_at: i64,
    ) -> Result<ValidatedBatch, String> {
        println!(
            "  [Transform] Validating and transforming {} records from {}",
            batch.records, batch.source
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(ValidatedBatch {
            valid_records: batch.records - 2, // Simulate 2 invalid records
            transformed_at,
        })
    }

    #[step(inputs(validated = "validate_and_transform"))]
    async fn aggregate(
        self: Arc<Self>,
        validated: ValidatedBatch,
    ) -> Result<ProcessingResult, String> {
        println!(
            "  [Aggregate] Aggregating {} valid records (transformed at: {})",
            validated.valid_records, validated.transformed_at
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(ProcessingResult {
            total_records: validated.valid_records,
            status: "Complete".to_string(),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct DataBatch {
    records: u32,
    source: String,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct ValidatedBatch {
    valid_records: u32,
    transformed_at: i64,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct ProcessingResult {
    total_records: u32,
    status: String,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct NotificationTask {
    task_id: String,
    recipient_count: u32,
}

impl NotificationTask {
    #[flow]
    async fn send(self: Arc<Self>) -> Result<NotificationResult, String> {
        println!(
            "[Notification {}] Preparing to send to {} recipients",
            self.task_id, self.recipient_count
        );

        // Generate deterministic batch_id at flow level using task_id
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.task_id.hash(&mut hasher);
        self.recipient_count.hash(&mut hasher);
        let batch_id = format!("batch_{:x}", hasher.finish());

        // Step 1: Prepare recipients
        let recipients = self.clone().prepare_recipients().await?;

        // Step 2: Send notifications (uses input from prepare_recipients)
        let result = self.clone().dispatch(recipients, batch_id).await?;

        println!(
            "[Notification {}] Sent successfully (ID: {})",
            self.task_id, result.batch_id
        );
        Ok(result)
    }

    #[step]
    async fn prepare_recipients(self: Arc<Self>) -> Result<RecipientList, String> {
        println!(
            "  [Prepare] Building recipient list for {} users",
            self.recipient_count
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(RecipientList {
            count: self.recipient_count,
            list_id: format!("list_{}", self.task_id),
        })
    }

    #[step(inputs(recipients = "prepare_recipients"))]
    async fn dispatch(
        self: Arc<Self>,
        recipients: RecipientList,
        batch_id: String,
    ) -> Result<NotificationResult, String> {
        println!(
            "  [Dispatch] Sending to {} recipients (list: {})",
            recipients.count, recipients.list_id
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        Ok(NotificationResult {
            batch_id,
            sent_count: recipients.count,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct RecipientList {
    count: u32,
    list_id: String,
}

#[derive(Serialize, Deserialize, Clone, FlowType)]
struct NotificationResult {
    batch_id: String,
    sent_count: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = "redis://127.0.0.1:6379";

    // Create Redis storage
    let storage = Arc::new(RedisExecutionLog::new(redis_url).await?);

    // Clear any previous data
    storage.reset().await?;

    let scheduler = Scheduler::new(storage.clone());

    // Schedule data pipeline jobs
    for i in 1..=5 {
        let pipeline = DataPipeline {
            pipeline_id: format!("PIPELINE-{:03}", i),
            batch_size: i * 100,
        };

        let flow_id = Uuid::new_v4();
        scheduler.schedule(pipeline, flow_id).await?;
    }

    // Schedule notification tasks
    for i in 1..=3 {
        let notification = NotificationTask {
            task_id: format!("NOTIF-{:03}", i),
            recipient_count: i * 50,
        };

        let flow_id = Uuid::new_v4();
        scheduler.schedule(notification, flow_id).await?;
    }

    // Start 3 workers concurrently
    let mut worker_handles = vec![];

    for i in 1..=3 {
        let storage_clone = storage.clone();
        let worker_id = format!("worker-{}", i);

        let handle = tokio::spawn(async move {
            let worker = Worker::new(storage_clone.clone(), &worker_id)
                .with_poll_interval(Duration::from_millis(50));

            // Register flow types
            worker
                .register(|flow: Arc<DataPipeline>| flow.process())
                .await;
            worker
                .register(|flow: Arc<NotificationTask>| flow.send())
                .await;

            let worker_handle = worker.start().await;

            // Run for 15 seconds
            tokio::time::sleep(Duration::from_secs(15)).await;

            // Shutdown gracefully
            worker_handle.shutdown().await;
        });

        worker_handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in worker_handles {
        handle.await?;
    }

    Ok(())
}
