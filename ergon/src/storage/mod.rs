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
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

mod error;
mod params;
mod queue;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "postgres")]
pub mod postgres;

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

#[cfg(feature = "postgres")]
pub use postgres::{PoolConfig as PgPoolConfig, PostgresExecutionLog};

#[cfg(feature = "redis")]
pub use redis::RedisExecutionLog;

use crate::core::Invocation;
use chrono::{DateTime, Utc};

/// Information about a timer that needs to fire.
#[derive(Debug, Clone)]
pub struct TimerInfo {
    pub flow_id: Uuid,
    pub step: i32,
    pub fire_at: DateTime<Utc>,
    pub timer_name: Option<String>,
}

/// Information about a flow waiting for a signal.
#[derive(Debug, Clone)]
pub struct SignalInfo {
    pub flow_id: Uuid,
    pub step: i32,
    pub signal_name: Option<String>,
}

/// Trait for execution log storage backends.
///
/// This trait defines the async interface for persisting and retrieving
/// flow execution state. Implementations must be thread-safe.
///
/// Using `async_trait` allows truly async storage backends (e.g., async
/// database drivers) without forcing blocking calls in async contexts.
///
/// # Automatic Maintenance
///
/// The trait includes maintenance methods that are called automatically
/// by workers when using the `Worker` type:
///
/// - **`move_ready_delayed_tasks()`**: Called every 1 second to move delayed
///   tasks to the pending queue (primarily for Redis distributed scheduling)
/// - **`recover_stale_locks()`**: Called every 60 seconds to recover flows
///   from crashed workers (distributed systems only)
/// - **`cleanup_completed()`**: Should be called periodically (e.g., daily)
///   to remove old completed flow data
///
/// Most backends use the default no-op implementations where maintenance
/// isn't needed (e.g., SQLite handles scheduling atomically, single-node
/// systems don't need crash recovery).
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

    /// Check if a flow has any non-retryable errors.
    ///
    /// This method scans all invocations for the given flow and returns true if
    /// any step has cached an error marked as non-retryable (is_retryable = Some(false)).
    ///
    /// # Returns
    ///
    /// Returns `true` if any step has a permanent error, `false` otherwise.
    async fn has_non_retryable_error(&self, flow_id: Uuid) -> Result<bool>;

    /// Update the is_retryable flag for a specific invocation.
    ///
    /// This method is called after an error is cached to mark whether the error
    /// is retryable or permanent.
    ///
    /// # Arguments
    ///
    /// * `id` - The flow ID
    /// * `step` - The step number
    /// * `is_retryable` - Whether the cached error is retryable (true = retryable, false = permanent)
    async fn update_is_retryable(&self, id: Uuid, step: i32, is_retryable: bool) -> Result<()>;

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

    /// Get the current status of a scheduled flow by task_id.
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
    async fn retry_flow(
        &self,
        task_id: Uuid,
        error_message: String,
        delay: Duration,
    ) -> Result<()> {
        let _ = (task_id, error_message, delay);
        Err(StorageError::Unsupported(
            "flow queue not implemented for this storage backend".to_string(),
        ))
    }

    // ===== Durable Timer Operations =====
    // These methods support durable timers that survive worker crashes.

    /// Get all timers that should have fired by `now`.
    ///
    /// Returns timers with status=WAITING_FOR_TIMER and timer_fire_at <= now.
    /// Used by TimerProcessor to find expired timers.
    ///
    /// # Default Implementation
    ///
    /// Returns an empty vector by default. Storage backends that support
    /// timers should override this method.
    async fn get_expired_timers(&self, now: DateTime<Utc>) -> Result<Vec<TimerInfo>> {
        let _ = now;
        Ok(Vec::new())
    }

    /// Atomically claim an expired timer using optimistic concurrency.
    ///
    /// Updates status from WAITING_FOR_TIMER to COMPLETE only if the
    /// status is still WAITING_FOR_TIMER (prevents duplicate firing).
    ///
    /// # Returns
    ///
    /// Returns `true` if we successfully claimed the timer, `false` if
    /// another worker already claimed it.
    ///
    /// # Default Implementation
    ///
    /// Returns `false` by default.
    async fn claim_timer(&self, flow_id: Uuid, step: i32) -> Result<bool> {
        let _ = (flow_id, step);
        Ok(false)
    }

    /// Log a timer that should fire at the specified time.
    ///
    /// Called by schedule_timer() to persist the timer to storage.
    /// Updates the invocation with WAITING_FOR_TIMER status and fire_at time.
    ///
    /// # Default Implementation
    ///
    /// Returns `StorageError::Unsupported` by default.
    async fn log_timer(
        &self,
        flow_id: Uuid,
        step: i32,
        fire_at: DateTime<Utc>,
        timer_name: Option<&str>,
    ) -> Result<()> {
        let _ = (flow_id, step, fire_at, timer_name);
        Err(StorageError::Unsupported(
            "timers not implemented for this storage backend".to_string(),
        ))
    }

    /// Re-enqueue a suspended flow back to the pending queue.
    ///
    /// Called when a timer fires or signal arrives to resume a suspended flow.
    /// The flow's task should be found and re-enqueued to the pending queue.
    ///
    /// # Arguments
    ///
    /// * `flow_id` - The flow ID to resume
    ///
    /// # Returns
    ///
    /// - `Ok(true)` - Flow was successfully resumed (was in SUSPENDED state)
    /// - `Ok(false)` - Flow was not resumed (not in SUSPENDED state - may be RUNNING, COMPLETE, or FAILED)
    /// - `Err(...)` - Storage error (connection failure, etc.)
    ///
    /// Returning `false` is NOT an error - it indicates the flow wasn't in a resumable state.
    /// This is expected when signals/timers race with flow execution.
    ///
    /// # Default Implementation
    ///
    /// Returns `StorageError::Unsupported` by default.
    async fn resume_flow(&self, flow_id: Uuid) -> Result<bool> {
        let _ = flow_id;
        Err(StorageError::Unsupported(
            "flow resume not implemented for this storage backend".to_string(),
        ))
    }

    /// Returns a reference to the work notification handle.
    ///
    /// Workers should use this notify to wait for work instead of sleeping.
    /// The storage backend signals this notify when new work becomes available.
    ///
    /// # Default Implementation
    ///
    /// Returns `None` by default for storage backends that don't support notifications.
    fn work_notify(&self) -> Option<&Arc<tokio::sync::Notify>> {
        None
    }

    // ===== External Signal Operations =====
    // These methods support durable external signals that survive crashes.

    /// Log a signal that the flow is waiting for.
    ///
    /// Called by await_external_signal() to persist the signal wait to storage.
    /// Updates the invocation with WAITING_FOR_SIGNAL status and signal name.
    ///
    /// # Default Implementation
    ///
    /// Returns `StorageError::Unsupported` by default.
    async fn log_signal(&self, flow_id: Uuid, step: i32, signal_name: &str) -> Result<()> {
        let _ = (flow_id, step, signal_name);
        Err(StorageError::Unsupported(
            "signals not implemented for this storage backend".to_string(),
        ))
    }

    /// Store signal parameters for a waiting flow.
    ///
    /// Called when a signal arrives for a flow that is (or will be)
    /// waiting for an external signal. Parameters are persisted so they
    /// survive worker crashes.
    ///
    /// # Default Implementation
    ///
    /// Returns `StorageError::Unsupported` by default.
    async fn store_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
        params: &[u8],
    ) -> Result<()> {
        let _ = (flow_id, step, signal_name, params);
        Err(StorageError::Unsupported(
            "signals not implemented for this storage backend".to_string(),
        ))
    }

    /// Retrieve signal parameters for a waiting flow.
    ///
    /// Returns the parameters if they exist, or None if no signal has
    /// arrived yet.
    ///
    /// # Default Implementation
    ///
    /// Returns None by default.
    async fn get_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
    ) -> Result<Option<Vec<u8>>> {
        let _ = (flow_id, step, signal_name);
        Ok(None)
    }

    /// Remove signal parameters after they've been consumed.
    ///
    /// Called after a flow successfully resumes from a signal to clean up
    /// the stored parameters.
    ///
    /// # Default Implementation
    ///
    /// Returns Ok by default (no-op).
    async fn remove_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
    ) -> Result<()> {
        let _ = (flow_id, step, signal_name);
        Ok(())
    }

    /// Get all flows currently waiting for signals.
    ///
    /// Returns flows with status=WAITING_FOR_SIGNAL.
    /// Used by examples and tools to find flows that need signal resumption.
    ///
    /// # Default Implementation
    ///
    /// Returns an empty vector by default. Storage backends that support
    /// signals should override this method.
    async fn get_waiting_signals(&self) -> Result<Vec<SignalInfo>> {
        Ok(Vec::new())
    }

    // ===== Step-Child Mapping Operations =====
    // These methods support the framework fix for #[step] + invoke() interaction.
    // When a step contains a child flow invocation, we need to track which child
    // step corresponds to the parent step, so on replay we can skip the step body
    // and return the child's cached result directly.

    /// Store a mapping from a parent step to its child invocation step.
    ///
    /// This is called when `invoke().result()` is invoked within a `#[step]` wrapper.
    /// It records that the parent step is waiting for a child invocation at child_step.
    ///
    /// # Default Implementation
    ///
    /// Returns Ok by default (no-op). Storage backends that support child flows
    /// should override this method.
    async fn store_step_child_mapping(
        &self,
        flow_id: Uuid,
        parent_step: i32,
        child_step: i32,
    ) -> Result<()> {
        let _ = (flow_id, parent_step, child_step);
        Ok(())
    }

    /// Get the child step for a parent step, if one exists.
    ///
    /// Returns the child step number if the parent step has a pending child invocation,
    /// or None if the parent step has no child.
    ///
    /// # Default Implementation
    ///
    /// Returns None by default.
    async fn get_child_step_for_parent(
        &self,
        flow_id: Uuid,
        parent_step: i32,
    ) -> Result<Option<i32>> {
        let _ = (flow_id, parent_step);
        Ok(None)
    }

    // ===== Cleanup Operations =====
    // Periodic cleanup to prevent unbounded growth of completed flow data.

    /// Clean up completed flows older than the specified duration.
    ///
    /// This method should be called periodically (e.g., daily) to remove
    /// old completed flow data and prevent unbounded storage growth.
    ///
    /// # Arguments
    ///
    /// * `older_than` - Delete flows completed more than this duration ago
    ///
    /// # Returns
    ///
    /// Returns the number of rows deleted.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Clean up flows completed more than 7 days ago
    /// let deleted = storage.cleanup_completed(Duration::from_secs(7 * 24 * 3600)).await?;
    /// info!("Cleaned up {} old completed flows", deleted);
    /// ```
    ///
    /// # Default Implementation
    ///
    /// Returns 0 by default (no-op).
    async fn cleanup_completed(&self, older_than: Duration) -> Result<u64> {
        let _ = older_than;
        Ok(0)
    }

    /// Moves ready delayed tasks from delayed queue to pending queue.
    ///
    /// This method is called automatically by workers to move tasks from a
    /// delayed execution queue to the pending queue once their scheduled time
    /// has arrived. This is primarily used by distributed backends like Redis.
    ///
    /// # Implementation Notes
    ///
    /// - **Redis**: Moves tasks from `ergon:queue:delayed` (sorted set) to
    ///   `ergon:queue:pending` (list) when `score <= now`
    /// - **SQLite**: No-op (scheduling handled atomically in dequeue query)
    /// - **In-Memory**: No-op (no separate delayed queue)
    ///
    /// # Returns
    ///
    /// The number of tasks moved to the pending queue.
    ///
    /// # Default Implementation
    ///
    /// Returns 0 by default (no-op). Override for distributed backends that
    /// maintain separate delayed task queues.
    async fn move_ready_delayed_tasks(&self) -> Result<u64> {
        Ok(0) // No-op by default
    }

    /// Recovers flows locked by crashed workers.
    ///
    /// This method is called automatically by workers to detect and recover
    /// flows that have been in the `Running` state for too long, indicating
    /// the worker that was processing them likely crashed.
    ///
    /// # Implementation Notes
    ///
    /// - **Redis**: Checks `ergon:running` sorted set for flows with
    ///   `start_time < (now - stale_timeout)` and resets them to pending
    /// - **SQLite**: No-op (single-node, no distributed lock management needed)
    /// - **In-Memory**: No-op (single-process, no crash recovery needed)
    ///
    /// # Returns
    ///
    /// The number of stale locks recovered and flows reset to pending.
    ///
    /// # Default Implementation
    ///
    /// Returns 0 by default (no-op). Override for distributed backends that
    /// need to handle worker crashes across multiple machines.
    async fn recover_stale_locks(&self) -> Result<u64> {
        Ok(0) // No-op by default
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

    async fn has_non_retryable_error(&self, flow_id: Uuid) -> Result<bool> {
        (**self).has_non_retryable_error(flow_id).await
    }

    async fn update_is_retryable(&self, id: Uuid, step: i32, is_retryable: bool) -> Result<()> {
        (**self).update_is_retryable(id, step, is_retryable).await
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

    async fn retry_flow(
        &self,
        task_id: Uuid,
        error_message: String,
        delay: Duration,
    ) -> Result<()> {
        (**self).retry_flow(task_id, error_message, delay).await
    }

    async fn get_expired_timers(&self, now: DateTime<Utc>) -> Result<Vec<TimerInfo>> {
        (**self).get_expired_timers(now).await
    }

    async fn claim_timer(&self, flow_id: Uuid, step: i32) -> Result<bool> {
        (**self).claim_timer(flow_id, step).await
    }

    async fn log_timer(
        &self,
        flow_id: Uuid,
        step: i32,
        fire_at: DateTime<Utc>,
        timer_name: Option<&str>,
    ) -> Result<()> {
        (**self).log_timer(flow_id, step, fire_at, timer_name).await
    }

    async fn resume_flow(&self, flow_id: Uuid) -> Result<bool> {
        (**self).resume_flow(flow_id).await
    }

    async fn log_signal(&self, flow_id: Uuid, step: i32, signal_name: &str) -> Result<()> {
        (**self).log_signal(flow_id, step, signal_name).await
    }

    async fn store_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
        params: &[u8],
    ) -> Result<()> {
        (**self)
            .store_signal_params(flow_id, step, signal_name, params)
            .await
    }

    async fn get_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
    ) -> Result<Option<Vec<u8>>> {
        (**self).get_signal_params(flow_id, step, signal_name).await
    }

    async fn remove_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
    ) -> Result<()> {
        (**self)
            .remove_signal_params(flow_id, step, signal_name)
            .await
    }

    async fn get_waiting_signals(&self) -> Result<Vec<SignalInfo>> {
        (**self).get_waiting_signals().await
    }

    async fn store_step_child_mapping(
        &self,
        flow_id: Uuid,
        parent_step: i32,
        child_step: i32,
    ) -> Result<()> {
        (**self)
            .store_step_child_mapping(flow_id, parent_step, child_step)
            .await
    }

    async fn get_child_step_for_parent(
        &self,
        flow_id: Uuid,
        parent_step: i32,
    ) -> Result<Option<i32>> {
        (**self)
            .get_child_step_for_parent(flow_id, parent_step)
            .await
    }

    async fn cleanup_completed(&self, older_than: Duration) -> Result<u64> {
        (**self).cleanup_completed(older_than).await
    }

    async fn move_ready_delayed_tasks(&self) -> Result<u64> {
        (**self).move_ready_delayed_tasks().await
    }

    async fn recover_stale_locks(&self) -> Result<u64> {
        (**self).recover_stale_locks().await
    }
}
