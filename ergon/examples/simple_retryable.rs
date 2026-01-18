use ergon::executor::Worker;
use ergon::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, Error)]
enum ApiError {
    #[error("Timeout")]
    Timeout,
    #[error("Invalid API key")]
    InvalidApiKey,
}

impl ergon::Retryable for ApiError {
    fn is_retryable(&self) -> bool {
        matches!(self, ApiError::Timeout)
    }
}

static ATTEMPTS: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct ApiCall {
    id: String,
}

impl ApiCall {
    #[step]
    async fn call_api(self: Arc<Self>) -> Result<String, ApiError> {
        let attempt = ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        println!("{} Step attempt #{}", self.id, attempt);

        if attempt < 3 {
            println!("Timeout error (retryable) - will retry");
            return Err(ApiError::Timeout);
        }
        if attempt == 3 {
            println!("InvalidApiKey error (non-retryable) - stops!");
            return Err(ApiError::InvalidApiKey);
        }
        println!("Success on attempt {}", attempt);
        Ok("API response".into())
    }

    #[flow]
    async fn execute(self: Arc<Self>) -> Result<String, ApiError> {
        self.call_api().await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::SqliteExecutionLog::new("retryable.db").await?);
    storage.reset().await?;
    let worker = Worker::new(storage.clone(), "worker");
    worker.register(|f: Arc<ApiCall>| f.execute()).await;
    let worker_handle = worker.start().await;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");
    let task_id = scheduler
        .schedule_with(
            ApiCall {
                id: "API_001".into(),
            },
            Uuid::new_v4(),
        )
        .await?;

    // Race-condition-free completion waiting
    let final_status = storage.wait_for_completion(task_id).await?;
    println!(
        "Final status: {:?}, Attempts: {}",
        final_status,
        ATTEMPTS.load(Ordering::SeqCst)
    );
    worker_handle.shutdown().await;
    Ok(())
}
