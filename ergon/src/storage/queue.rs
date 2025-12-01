//! Queue types for distributed flow execution.
//!
//! This module provides types for scheduling and managing flows in a distributed
//! execution environment. Flows can be enqueued for later execution by workers.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Status of a scheduled flow in the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Flow is waiting to be picked up by a worker.
    Pending,
    /// Flow is currently being executed by a worker.
    Running,
    /// Flow has completed successfully.
    Complete,
    /// Flow execution failed.
    Failed,
}

impl TaskStatus {
    /// Returns the string representation of the task status.
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "PENDING",
            TaskStatus::Running => "RUNNING",
            TaskStatus::Complete => "COMPLETE",
            TaskStatus::Failed => "FAILED",
        }
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PENDING" => Ok(TaskStatus::Pending),
            "RUNNING" => Ok(TaskStatus::Running),
            "COMPLETE" => Ok(TaskStatus::Complete),
            "FAILED" => Ok(TaskStatus::Failed),
            _ => Err(format!("unknown task status: {}", s)),
        }
    }
}

/// A flow scheduled for distributed execution.
///
/// This represents a flow that has been serialized and queued for execution
/// by a worker. The flow data is stored as serialized bytes and will be
/// deserialized by the worker that picks it up.
#[derive(Debug, Clone)]
pub struct ScheduledFlow {
    /// Unique identifier for this scheduled task.
    pub task_id: Uuid,
    /// The flow's execution ID.
    pub flow_id: Uuid,
    /// The fully qualified type name of the flow (e.g., "myapp::OrderProcessor").
    pub flow_type: String,
    /// Serialized flow instance data.
    pub flow_data: Vec<u8>,
    /// Current status of this task.
    pub status: TaskStatus,
    /// Worker ID that locked this task (if running).
    pub locked_by: Option<String>,
    /// When this task was created.
    pub created_at: DateTime<Utc>,
    /// When this task was last updated.
    pub updated_at: DateTime<Utc>,
}

impl ScheduledFlow {
    /// Creates a new scheduled flow in pending status.
    pub fn new(flow_id: Uuid, flow_type: String, flow_data: Vec<u8>) -> Self {
        let now = Utc::now();
        Self {
            task_id: Uuid::new_v4(),
            flow_id,
            flow_type,
            flow_data,
            status: TaskStatus::Pending,
            locked_by: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Returns true if this task is currently locked by a worker.
    pub fn is_locked(&self) -> bool {
        self.locked_by.is_some()
    }

    /// Returns true if this task has completed (successfully or with failure).
    pub fn is_finished(&self) -> bool {
        matches!(self.status, TaskStatus::Complete | TaskStatus::Failed)
    }
}
