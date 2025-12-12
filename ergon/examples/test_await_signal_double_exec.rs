//! Test: Does await_external_signal cause step body to execute twice?

use async_trait::async_trait;
use chrono::Utc;
use ergon::executor::{await_external_signal, ExecutionError, SignalSource};
use ergon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct TestFlow {
    id: String,
}

impl TestFlow {
    #[step]
    async fn process_signal(self: Arc<Self>, signal_data: String) -> Result<String, String> {
        eprintln!("[{}] Step: processing signal: {}", ts(), signal_data);
        Ok(signal_data)
    }

    #[flow]
    async fn process(self: Arc<Self>) -> Result<String, ExecutionError> {
        eprintln!("[{}] FLOW: Starting", ts());

        // Await signal at flow level
        let signal_data: String = await_external_signal("test_signal")
            .await
            .map_err(|e| ExecutionError::Failed(e.to_string()))?;

        // Process in atomic step
        let result = self
            .clone()
            .process_signal(signal_data)
            .await
            .map_err(ExecutionError::Failed)?;

        eprintln!("[{}] FLOW: Complete", ts());
        Ok(result)
    }
}

fn ts() -> String {
    Utc::now().format("%H:%M:%S%.3f").to_string()
}

// Simple in-memory signal source
struct MemorySignalSource {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl MemorySignalSource {
    fn new() -> Self {
        Self {
            signals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn send_signal(&self, name: &str, data: impl serde::Serialize) {
        let bytes = ergon::serialize_value(&data).unwrap();
        self.signals.write().await.insert(name.to_string(), bytes);
    }
}

#[async_trait]
impl SignalSource for MemorySignalSource {
    async fn poll_for_signal(&self, signal_name: &str) -> Option<Vec<u8>> {
        self.signals.read().await.get(signal_name).cloned()
    }

    async fn consume_signal(&self, signal_name: &str) {
        self.signals.write().await.remove(signal_name);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = "/tmp/ergon_test_await_signal.db";
    let _ = std::fs::remove_file(db);

    let storage = Arc::new(SqliteExecutionLog::new(db).await?);
    let scheduler = Scheduler::new(storage.clone());
    let signal_source = Arc::new(MemorySignalSource::new());

    eprintln!("\n=== Test: await_external_signal Double Execution ===\n");

    let flow = TestFlow {
        id: "TEST-001".into(),
    };
    let task_id = scheduler.schedule(flow, Uuid::new_v4()).await?;
    eprintln!("[{}] Scheduled: {}\n", ts(), &task_id.to_string()[..8]);

    let worker = Worker::new(storage.clone(), "worker")
        .with_poll_interval(Duration::from_millis(50))
        .with_signals(signal_source.clone());

    worker.register(|f: Arc<TestFlow>| f.process()).await;

    let handle = worker.start().await;

    // Wait for flow to suspend
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send signal
    eprintln!("[{}] Sending signal\n", ts());
    signal_source.send_signal("test_signal", "Hello!").await;

    // Wait for completion
    tokio::time::sleep(Duration::from_secs(2)).await;
    handle.shutdown().await;

    storage.close().await?;

    eprintln!("\n=== Test Complete ===");
    Ok(())
}
