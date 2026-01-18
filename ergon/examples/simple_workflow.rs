use ergon::executor::{ExecutionError, Worker};
use ergon::prelude::*;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct DataPipeline {
    id: String,
    value: i32,
}

impl DataPipeline {
    #[step]
    async fn validate(self: Arc<Self>) -> Result<i32, ExecutionError> {
        println!("{} Validating: {}", self.id, self.value);
        Ok(self.value)
    }

    #[step]
    async fn transform(self: Arc<Self>, input: i32) -> Result<i32, ExecutionError> {
        println!("{} Transforming: {} -> {}", self.id, input, input * 2);
        Ok(input * 2)
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<i32, ExecutionError> {
        let validated = self.clone().validate().await?;
        self.transform(validated).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::SqliteExecutionLog::new("simple_workflow.db").await?);
    storage.reset().await?;
    let worker = Worker::new(storage.clone(), "worker");
    worker.register(|f: Arc<DataPipeline>| f.process()).await;
    let worker_handle = worker.start().await;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");
    let task_id = scheduler
        .schedule_with(
            DataPipeline {
                id: "data_001".into(),
                value: 42,
            },
            Uuid::new_v4(),
        )
        .await?;

    // Race-condition-free completion waiting (no polling!)
    storage.wait_for_completion(task_id).await?;
    println!("Pipeline complete!");
    worker_handle.shutdown().await;
    Ok(())
}
