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
use ergon::TaskStatus;
use tokio::sync::RwLock;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "/tmp/ergon_mini_signal.db";
    let _ = std::fs::remove_file(db_path);

    let storage = Arc::new(SqliteExecutionLog::new(db_path).await?);
    let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");
    let signals = Arc::new(SimpleSignals::new());

    let task_id = scheduler
        .schedule_with(
            Document {
                id: "DOC-001".into(),
            },
            Uuid::new_v4(),
        )
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
    storage.close().await?;
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
    async fn process_approval(self: Arc<Self>, decision: Approval) -> Result<(), String> {
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

        let token = format!("approval-{}", self.id);
        let decision: Approval = await_external_signal(&token)
            .await
            .map_err(|e| e.to_string())?;

        // Process decision in atomic step
        self.process_approval(decision).await?;

        println!("Published");
        Ok(())
    }
}
