//! Storage layer for the ergon durable execution engine.
//!
//! This module provides a trait-based interface for persisting execution state
//! with multiple backend implementations:
//!
//! - [`SqliteExecutionLog`]: Persistent SQLite-based storage with connection pooling
//! - [`InMemoryExecutionLog`]: Fast in-memory storage for testing and development
//! - [`RedisExecutionLog`]: Redis-based storage for true distributed execution
//!
//! # Example
//!
//! ```no_run
//! use ergon::storage::{ExecutionLog, InMemoryExecutionLog};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let log = InMemoryExecutionLog::new();
//! // Use the log for storing execution state
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use std::time::Duration;
use uuid::Uuid;

mod error;
mod params;
mod queue;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "redis")]
pub mod redis;

pub mod memory;

// Re-export public types
pub use error::{Result, StorageError};
pub use memory::InMemoryExecutionLog;
pub use params::InvocationStartParams;
pub use queue::{ScheduledFlow, TaskStatus};

#[cfg(feature = "sqlite")]
pub use sqlite::{PoolConfig, SqliteExecutionLog};

#[cfg(feature = "redis")]
pub use redis::RedisExecutionLog;

use crate::core::Invocation;

/// Trait for execution log storage backends.
///
/// This trait defines the async interface for persisting and retrieving
/// flow execution state. Implementations must be thread-safe.
///
/// Using `async_trait` allows truly async storage backends (e.g., async
/// database drivers) without forcing blocking calls in async contexts.
#[async_trait]
pub trait ExecutionLog: Send + Sync {
    /// Log the start of a step invocation.
    /// The params_hash is computed internally from the parameters bytes.
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()>;

    /// Log the completion of a step invocation.
    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation>;

    /// Get a specific invocation by flow ID and step number.
    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>>;

    /// Get the latest invocation for a flow.
    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>>;

    /// Get all invocations for a flow.
    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>>;

    /// Get all incomplete flows (flows that haven't completed).
    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>>;

    /// Reset the execution log (delete all entries).
    async fn reset(&self) -> Result<()>;

    /// Close the execution log.
    async fn close(&self) -> Result<()>;

    // ===== Distributed Queue Operations =====
    // These methods are optional and only implemented by storage backends
    // that support distributed execution via task queues.

    /// Enqueue a flow for distributed execution.
    ///
    /// This method schedules a flow to be executed by a worker. The flow is
    /// serialized and stored in the queue with pending status.
    ///
    /// # Default Implementation
    ///
    /// Returns `StorageError::Unsupported` by default. Storage backends that
    /// support distributed execution should override this method.
    async fn enqueue_flow(&self, flow: ScheduledFlow) -> Result<Uuid> {
        let _ = flow;
        Err(StorageError::Unsupported(
            "flow queue not implemented for this storage backend".to_string(),
        ))
    }

    /// Dequeue a flow for execution by a worker.
    ///
    /// This method atomically finds a pending flow and locks it for execution
    /// by the specified worker. Uses pessimistic locking to prevent multiple
    /// workers from executing the same flow.
    ///
    /// # Arguments
    ///
    /// * `worker_id` - Unique identifier for the worker requesting work
    ///
    /// # Returns
    ///
    /// Returns `Some(ScheduledFlow)` if a flow was successfully locked, or
    /// `None` if no pending flows are available.
    ///
    /// # Default Implementation
    ///
    /// Returns `StorageError::Unsupported` by default.
    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<ScheduledFlow>> {
        let _ = worker_id;
        Err(StorageError::Unsupported(
            "flow queue not implemented for this storage backend".to_string(),
        ))
    }

    /// Mark a scheduled flow as complete.
    ///
    /// This method updates the status of a scheduled flow after execution
    /// completes, either successfully or with failure.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The unique identifier of the scheduled task
    /// * `status` - Final status (Complete or Failed)
    ///
    /// # Default Implementation
    ///
    /// Returns `StorageError::Unsupported` by default.
    async fn complete_flow(&self, task_id: Uuid, status: TaskStatus) -> Result<()> {
        let _ = (task_id, status);
        Err(StorageError::Unsupported(
            "flow queue not implemented for this storage backend".to_string(),
        ))
    }

    /// Get the current status of a scheduled flow.
    ///
    /// # Default Implementation
    ///
    /// Returns `StorageError::Unsupported` by default.
    async fn get_scheduled_flow(&self, task_id: Uuid) -> Result<Option<ScheduledFlow>> {
        let _ = task_id;
        Err(StorageError::Unsupported(
            "flow queue not implemented for this storage backend".to_string(),
        ))
    }

    /// Retry a failed flow with exponential backoff.
    ///
    /// This method re-schedules a failed flow for retry, incrementing the retry
    /// count and setting a delay based on exponential backoff.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The unique identifier of the failed task
    /// * `error_message` - Error message from the failed execution
    /// * `delay` - Delay before retrying (for exponential backoff)
    ///
    /// # Default Implementation
    ///
    /// Returns `StorageError::Unsupported` by default.
    async fn retry_flow(&self, task_id: Uuid, error_message: String, delay: Duration) -> Result<()> {
        let _ = (task_id, error_message, delay);
        Err(StorageError::Unsupported(
            "flow queue not implemented for this storage backend".to_string(),
        ))
    }
}

// Implement ExecutionLog for Box<dyn ExecutionLog> to allow type-erased storage
#[async_trait]
impl ExecutionLog for Box<dyn ExecutionLog> {
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()> {
        (**self).log_invocation_start(params).await
    }

    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation> {
        (**self)
            .log_invocation_completion(id, step, return_value)
            .await
    }

    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>> {
        (**self).get_invocation(id, step).await
    }

    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>> {
        (**self).get_latest_invocation(id).await
    }

    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>> {
        (**self).get_invocations_for_flow(id).await
    }

    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>> {
        (**self).get_incomplete_flows().await
    }

    async fn reset(&self) -> Result<()> {
        (**self).reset().await
    }

    async fn close(&self) -> Result<()> {
        (**self).close().await
    }

    async fn enqueue_flow(&self, flow: ScheduledFlow) -> Result<Uuid> {
        (**self).enqueue_flow(flow).await
    }

    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<ScheduledFlow>> {
        (**self).dequeue_flow(worker_id).await
    }

    async fn complete_flow(&self, task_id: Uuid, status: TaskStatus) -> Result<()> {
        (**self).complete_flow(task_id, status).await
    }

    async fn get_scheduled_flow(&self, task_id: Uuid) -> Result<Option<ScheduledFlow>> {
        (**self).get_scheduled_flow(task_id).await
    }

    async fn retry_flow(&self, task_id: Uuid, error_message: String, delay: Duration) -> Result<()> {
        (**self).retry_flow(task_id, error_message, delay).await
    }
}
