//! External signals for human-in-the-loop workflows.
//!
//! Shows how flows can suspend while waiting for external events (approval,
//! payment confirmation, user input, etc.). The flow consumes zero resources
//! while waiting and resumes automatically when the signal arrives.
//!
//! Run with
//!
//! ```not_rust
//! cargo run --example mini_signal
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ergon::executor::{await_external_signal, SignalSource, Worker};
use ergon::prelude::*;
use ergon::storage::InMemoryExecutionLog;
use tokio::sync::RwLock;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = Arc::new(InMemoryExecutionLog::default());
    let scheduler = Scheduler::new(storage.clone());
    let signals = Arc::new(SimpleSignals::new());

    let task_id = scheduler
        .schedule(Document { id: "DOC-001".into() }, Uuid::new_v4())
        .await?;

    let worker = Worker::new(storage.clone(), "worker").with_signals(signals.clone());
    worker.register(|f: Arc<Document>| f.publish()).await;
    let handle = worker.start().await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    signals
        .send("approval-DOC-001", Approval { approved: true })
        .await;

    loop {
        if let Some(scheduled) = storage.get_scheduled_flow(task_id).await? {
            if matches!(scheduled.status, TaskStatus::Complete | TaskStatus::Failed) {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    handle.shutdown().await;
    Ok(())
}

struct SimpleSignals {
    signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl SimpleSignals {
    fn new() -> Self {
        Self {
            signals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn send(&self, name: &str, value: Approval) {
        let data = ergon::core::serialize_value(&value).unwrap();
        self.signals.write().await.insert(name.to_string(), data);
    }
}

#[async_trait]
impl SignalSource for SimpleSignals {
    async fn poll_for_signal(&self, name: &str) -> Option<Vec<u8>> {
        self.signals.read().await.get(name).cloned()
    }

    async fn consume_signal(&self, name: &str) {
        self.signals.write().await.remove(name);
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Approval {
    approved: bool,
}

#[derive(Clone, Serialize, Deserialize, FlowType)]
struct Document {
    id: String,
}

impl Document {
    #[step]
    async fn await_approval(self: Arc<Self>) -> Result<(), String> {
        let token = format!("approval-{}", self.id);

        let decision: Approval = await_external_signal(&token)
            .await
            .map_err(|e| e.to_string())?;

        if decision.approved {
            println!("Approved");
            Ok(())
        } else {
            Err("Rejected".into())
        }
    }

    #[flow]
    async fn publish(self: Arc<Self>) -> Result<(), String> {
        println!("Waiting for approval...");
        self.await_approval().await?;
        println!("Published");
        Ok(())
    }
}

// Output:
// Waiting for approval...
// Approved
// Published
