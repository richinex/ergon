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

    let scheduler = Scheduler::new(storage.clone());
    let task_id = scheduler
        .schedule(
            DataPipeline {
                id: "data_001".into(),
                value: 42,
            },
            Uuid::new_v4(),
        )
        .await?;

    // Event-driven completion waiting
    let notify = storage.status_notify().clone();
    loop {
        if let Some(task) = storage.get_scheduled_flow(task_id).await? {
            if matches!(
                task.status,
                ergon::storage::TaskStatus::Complete | ergon::storage::TaskStatus::Failed
            ) {
                break;
            }
        }
        notify.notified().await;
    }
    println!("Pipeline complete!");
    worker_handle.shutdown().await;
    Ok(())
}
