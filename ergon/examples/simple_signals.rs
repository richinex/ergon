use async_trait::async_trait;
use ergon::executor::{await_external_signal, ExecutionError, SignalSource, Worker};
use ergon::prelude::*;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone, Serialize, Deserialize)]
struct SignalData {
    message: String,
}

struct SimpleSignalSource {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl SimpleSignalSource {
    fn new() -> Self {
        Self {
            signals: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    async fn send(&self, name: &str, msg: String) {
        self.signals.write().await.insert(
            name.to_string(),
            ergon::core::serialize_value(&SignalData { message: msg }).unwrap(),
        );
    }
}

#[async_trait]
impl SignalSource for SimpleSignalSource {
    async fn poll_for_signal(&self, name: &str) -> Option<Vec<u8>> {
        self.signals.read().await.get(name).cloned()
    }
    async fn consume_signal(&self, name: &str) {
        self.signals.write().await.remove(name);
    }
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct Task {
    id: String,
}

impl Task {
    #[flow]
    async fn run(self: Arc<Self>) -> Result<String, ExecutionError> {
        println!("{} Waiting for signal...", self.id);
        let data: SignalData = await_external_signal(&format!("sig_{}", self.id)).await?;
        println!("{} Received: {}", self.id, data.message);
        Ok(data.message)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(ergon::storage::SqliteExecutionLog::new("simple_signals.db").await?);
    storage.reset().await?;
    let sig = Arc::new(SimpleSignalSource::new());
    let worker = Worker::new(storage.clone(), "worker");
    worker.register(|f: Arc<Task>| f.run()).await;
    let worker_handle = worker.with_signals(sig.clone()).start().await;

    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");
    let task_id = scheduler
        .schedule_with(Task { id: "W1".into() }, Uuid::new_v4())
        .await?;

    // Send signal after task suspends, then wait for completion
    tokio::spawn({
        let sig = sig.clone();
        async move {
            sig.send("sig_W1", "Approved".to_string()).await;
        }
    });

    // Race-condition-free completion waiting
    storage.wait_for_completion(task_id).await?;
    println!("Signal task complete!");
    worker_handle.shutdown().await;
    Ok(())
}
