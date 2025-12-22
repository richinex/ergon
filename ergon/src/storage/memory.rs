use super::{
    error::Result, error::StorageError, params::InvocationStartParams, ExecutionLog,
    TimerNotificationSource, WorkNotificationSource,
};
use crate::core::{hash_params, Invocation, InvocationStatus};
use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};
use uuid::Uuid;

/// In-memory execution log using DashMap for concurrent access.
///
/// This implementation uses DashMap, a lock-free concurrent HashMap,
/// to provide thread-safe in-memory storage without the single-write
/// limitation of SQLite's shared cache mode. This is suitable for
/// both sequential and parallel DAG execution.
///
/// **Distributed Execution Support:**
/// This storage backend also implements the distributed queue methods,
/// allowing workers to be tested in-memory without requiring SQLite.
/// Note that this is only suitable for single-process testing, as the
/// queue is not shared across processes.
///
/// Reference: https://github.com/xacrimon/dashmap
/// DashMap provides concurrent access through sharding, eliminating
/// the bottleneck of a single mutex.
pub struct InMemoryExecutionLog {
    /// Concurrent storage for invocations keyed by (flow_id, step)
    invocations: dashmap::DashMap<(Uuid, i32), Invocation>,
    /// Concurrent storage for scheduled flows keyed by task_id
    flow_queue: dashmap::DashMap<Uuid, super::ScheduledFlow>,
    /// Concurrent storage for suspension results (signals and timers) keyed by (flow_id, step, suspension_key)
    suspension_params: dashmap::DashMap<(Uuid, i32, String), Vec<u8>>,
    /// Index mapping flow_id to task_id for fast lookup during resume_flow
    flow_task_map: dashmap::DashMap<Uuid, Uuid>,
    /// Channel for pending flows (mimics Redis Stream)
    pending_tx: mpsc::UnboundedSender<Uuid>,
    pending_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Uuid>>>,
    /// Notification mechanism to wake up workers when new work arrives
    work_notify: Arc<Notify>,
    /// Notification mechanism for flow status changes (completion, failure, etc.)
    status_notify: Arc<Notify>,
    /// Notification mechanism for timer changes (new timer scheduled, timer fired)
    timer_notify: Arc<Notify>,
}

impl InMemoryExecutionLog {
    /// Creates a new in-memory execution log.
    pub fn new() -> Self {
        let (pending_tx, pending_rx) = mpsc::unbounded_channel();
        Self {
            invocations: dashmap::DashMap::new(),
            flow_queue: dashmap::DashMap::new(),
            suspension_params: dashmap::DashMap::new(),
            flow_task_map: dashmap::DashMap::new(),
            pending_tx,
            pending_rx: Arc::new(tokio::sync::Mutex::new(pending_rx)),
            work_notify: Arc::new(Notify::new()),
            status_notify: Arc::new(Notify::new()),
            timer_notify: Arc::new(Notify::new()),
        }
    }

    /// Returns a reference to the status notification handle.
    ///
    /// Callers can use this to wait for flow status changes (completion, failure, etc.)
    /// instead of polling. The notification is triggered whenever any flow status changes.
    pub fn status_notify(&self) -> &Arc<Notify> {
        &self.status_notify
    }

    /// Returns a reference to the timer notification handle.
    ///
    /// Callers can use this to wait for timer events (new timer scheduled, timer claimed/fired)
    /// instead of polling. This enables event-driven timer processing.
    pub fn timer_notify(&self) -> &Arc<Notify> {
        &self.timer_notify
    }
}

impl Default for InMemoryExecutionLog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionLog for InMemoryExecutionLog {
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()> {
        // Destructure params
        let InvocationStartParams {
            id,
            step,
            class_name,
            method_name,
            delay,
            status,
            parameters,
            retry_policy,
        } = params;

        let params_hash = hash_params(parameters);
        let delay_ms = delay.map(|d| d.as_millis() as i64);

        let invocation = Invocation::new(
            id,
            step,
            Utc::now(),
            class_name.to_string(),
            method_name.to_string(),
            status,
            1,
            parameters.to_vec(),
            params_hash,
            None,
            delay_ms,
            retry_policy,
            None, // is_retryable (None = not an error yet)
        );

        let key = (id, step);

        // Check if invocation already exists and skip if Complete or WaitingForSignal
        // This prevents overwriting cached results or signal state during replay
        if let Some(existing) = self.invocations.get(&key) {
            if existing.status() == InvocationStatus::Complete {
                return Ok(());
            }
            if existing.status() == InvocationStatus::WaitingForSignal {
                return Ok(());
            }
        }

        self.invocations.insert(key, invocation);
        Ok(())
    }

    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation> {
        let key = (id, step);
        if let Some(mut entry) = self.invocations.get_mut(&key) {
            let delay_ms = entry.delay().map(|d| d.as_millis() as i64);

            let invocation = Invocation::new(
                entry.id(),
                entry.step(),
                entry.timestamp(),
                entry.class_name().to_string(),
                entry.method_name().to_string(),
                InvocationStatus::Complete,
                entry.attempts(),
                entry.parameters().to_vec(),
                entry.params_hash(),
                Some(return_value.to_vec()),
                delay_ms,
                entry.retry_policy(), // Preserve retry policy
                entry.is_retryable(), // Preserve is_retryable
            );
            *entry = invocation.clone();
            Ok(invocation)
        } else {
            Err(StorageError::InvocationNotFound { id, step })
        }
    }

    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>> {
        let key = (id, step);
        Ok(self
            .invocations
            .get(&key)
            .map(|entry| entry.value().clone()))
    }

    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>> {
        let invocations = self.get_invocations_for_flow(id).await?;
        Ok(invocations.into_iter().max_by_key(|inv| inv.step()))
    }

    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>> {
        let mut invocations: Vec<Invocation> = self
            .invocations
            .iter()
            .filter(|entry| entry.key().0 == id)
            .map(|entry| entry.value().clone())
            .collect();

        invocations.sort_by_key(|inv| inv.step());
        Ok(invocations)
    }

    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>> {
        let invocations: Vec<Invocation> = self
            .invocations
            .iter()
            .filter(|entry| entry.value().status() != InvocationStatus::Complete)
            .map(|entry| entry.value().clone())
            .collect();

        // Also check for flows in the queue that haven't started yet
        // This ensures we don't miss flows that are scheduled but not yet picked up by workers
        let mut flow_ids_with_invocations: std::collections::HashSet<Uuid> =
            invocations.iter().map(|inv| inv.id()).collect();

        // Add placeholder invocations for flows in queue that have no invocations yet
        let mut all_incomplete = invocations;
        for entry in self.flow_queue.iter() {
            let flow = entry.value();
            // Only consider pending/running flows
            if matches!(
                flow.status,
                super::TaskStatus::Pending | super::TaskStatus::Running
            ) {
                // If this flow has no invocations yet, create a placeholder
                if !flow_ids_with_invocations.contains(&flow.flow_id) {
                    flow_ids_with_invocations.insert(flow.flow_id);
                    // Create a placeholder invocation for this queued flow
                    let placeholder = Invocation::new(
                        flow.flow_id,
                        -1, // Placeholder step number
                        flow.created_at,
                        flow.flow_type.clone(),
                        "queued".to_string(),
                        InvocationStatus::Pending,
                        0,
                        vec![],
                        0,
                        None,
                        None,
                        None,
                        None,
                    );
                    all_incomplete.push(placeholder);
                }
            }
        }

        Ok(all_incomplete)
    }

    async fn has_non_retryable_error(&self, flow_id: Uuid) -> Result<bool> {
        // Check if any step in the flow has is_retryable = Some(false)
        let has_non_retryable = self
            .invocations
            .iter()
            .filter(|entry| entry.key().0 == flow_id)
            .any(|entry| entry.value().is_retryable() == Some(false));

        Ok(has_non_retryable)
    }

    async fn update_is_retryable(&self, id: Uuid, step: i32, is_retryable: bool) -> Result<()> {
        let key = (id, step);
        if let Some(mut entry) = self.invocations.get_mut(&key) {
            entry.set_is_retryable(Some(is_retryable));
            Ok(())
        } else {
            Err(StorageError::InvocationNotFound { id, step })
        }
    }

    async fn reset(&self) -> Result<()> {
        self.invocations.clear();
        self.flow_queue.clear();
        self.suspension_params.clear();
        self.flow_task_map.clear();

        // Drain the channel by receiving all pending messages
        let mut rx = self.pending_rx.lock().await;
        while rx.try_recv().is_ok() {
            // Discard all messages
        }

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    // ===== Distributed Queue Operations =====

    async fn enqueue_flow(&self, flow: super::ScheduledFlow) -> Result<Uuid> {
        let task_id = flow.task_id;
        let flow_id = flow.flow_id;

        // Store metadata in DashMap
        self.flow_queue.insert(task_id, flow);
        self.flow_task_map.insert(flow_id, task_id);

        // Send to channel (mimics Redis Stream XADD)
        // UnboundedSender::send never fails unless receiver is dropped
        self.pending_tx
            .send(task_id)
            .map_err(|_| StorageError::Connection("pending channel closed".to_string()))?;

        // Wake up one waiting worker
        self.work_notify.notify_one();

        Ok(task_id)
    }

    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<super::ScheduledFlow>> {
        // Receive task_id from channel (mimics Redis Stream XREADGROUP)
        // This provides proper FIFO ordering and avoids DashMap iteration deadlocks
        let mut rx = self.pending_rx.lock().await;

        // Try to receive with a short timeout to avoid blocking forever
        let task_id =
            match tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await {
                Ok(Some(id)) => id,
                Ok(None) => return Ok(None), // Channel closed
                Err(_) => return Ok(None),   // Timeout - no pending flows
            };

        // Release lock immediately after receiving
        drop(rx);

        // Look up flow metadata in DashMap
        if let Some(mut entry) = self.flow_queue.get_mut(&task_id) {
            let now = Utc::now();

            // Check if flow is still pending and ready to execute
            if entry.status == super::TaskStatus::Pending {
                // Check if scheduled_for time has passed (or is None)
                let is_ready = entry.scheduled_for.is_none_or(|scheduled| scheduled <= now);

                if is_ready {
                    // Atomically claim the flow
                    entry.status = super::TaskStatus::Running;
                    entry.locked_by = Some(worker_id.to_string());
                    entry.updated_at = Utc::now();

                    // Return claimed flow
                    return Ok(Some(entry.clone()));
                } else {
                    // Flow is delayed - put it back in the channel for later
                    let _ = self.pending_tx.send(task_id);
                    return Ok(None);
                }
            }
        }

        // Flow was already claimed or doesn't exist - return None
        Ok(None)
    }

    async fn complete_flow(&self, task_id: Uuid, status: super::TaskStatus) -> Result<()> {
        if let Some(mut entry) = self.flow_queue.get_mut(&task_id) {
            entry.status = status;
            entry.updated_at = Utc::now();

            // Notify any waiters that a flow status changed
            self.status_notify.notify_waiters();

            Ok(())
        } else {
            Err(StorageError::ScheduledFlowNotFound(task_id))
        }
    }

    async fn get_scheduled_flow(&self, task_id: Uuid) -> Result<Option<super::ScheduledFlow>> {
        Ok(self.flow_queue.get(&task_id).map(|entry| entry.clone()))
    }

    async fn retry_flow(
        &self,
        task_id: Uuid,
        error_message: String,
        delay: std::time::Duration,
    ) -> Result<()> {
        if let Some(mut entry) = self.flow_queue.get_mut(&task_id) {
            // Increment retry count
            entry.retry_count += 1;
            // Set error message
            entry.error_message = Some(error_message);
            // Set status back to pending
            entry.status = super::TaskStatus::Pending;
            // Clear the lock
            entry.locked_by = None;
            // Set scheduled_for timestamp (current time + delay)
            let chrono_delay = chrono::Duration::from_std(delay).map_err(|e| {
                StorageError::InvalidParameter(format!("Invalid delay duration: {}", e))
            })?;
            entry.scheduled_for = Some(Utc::now() + chrono_delay);
            // Update timestamp
            entry.updated_at = Utc::now();

            // Re-enqueue to channel (mimics Redis re-adding to stream)
            self.pending_tx
                .send(task_id)
                .map_err(|_| StorageError::Connection("pending channel closed".to_string()))?;

            // Notify workers that new work is available (or will be available soon)
            self.work_notify.notify_one();

            Ok(())
        } else {
            Err(StorageError::ScheduledFlowNotFound(task_id))
        }
    }

    async fn get_expired_timers(
        &self,
        now: chrono::DateTime<Utc>,
    ) -> Result<Vec<super::TimerInfo>> {
        let mut timers = Vec::new();

        for entry in self.invocations.iter() {
            let (flow_id, step) = *entry.key();
            let inv = entry.value();

            // Check if this is a timer waiting to fire
            if inv.status() == InvocationStatus::WaitingForTimer {
                if let Some(fire_at) = inv.timer_fire_at() {
                    if fire_at <= now {
                        timers.push(super::TimerInfo {
                            flow_id,
                            step,
                            fire_at,
                            timer_name: inv.timer_name().map(|s| s.to_string()),
                        });
                    }
                }
            }
        }

        // Sort by fire_at (oldest first)
        timers.sort_by_key(|t| t.fire_at);

        Ok(timers)
    }

    async fn claim_timer(&self, flow_id: Uuid, step: i32) -> Result<bool> {
        let key = (flow_id, step);

        if let Some(mut entry) = self.invocations.get_mut(&key) {
            // Check if still waiting for timer
            if entry.status() == InvocationStatus::WaitingForTimer {
                // Claim it by marking as complete
                entry.set_status(InvocationStatus::Complete);
                entry.set_return_value(
                    crate::core::serialize_value(&()).map_err(|_| StorageError::Serialization)?,
                );

                // Notify timer processor that timer was claimed (may need to recalculate next wake time)
                self.timer_notify.notify_one();

                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn get_next_timer_fire_time(&self) -> Result<Option<chrono::DateTime<Utc>>> {
        let mut next_fire_time: Option<chrono::DateTime<Utc>> = None;

        for entry in self.invocations.iter() {
            let inv = entry.value();

            // Check if this is a timer waiting to fire
            if inv.status() == InvocationStatus::WaitingForTimer {
                if let Some(fire_at) = inv.timer_fire_at() {
                    match next_fire_time {
                        None => next_fire_time = Some(fire_at),
                        Some(current_earliest) if fire_at < current_earliest => {
                            next_fire_time = Some(fire_at);
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(next_fire_time)
    }

    async fn log_timer(
        &self,
        flow_id: Uuid,
        step: i32,
        fire_at: chrono::DateTime<Utc>,
        timer_name: Option<&str>,
    ) -> Result<()> {
        let key = (flow_id, step);

        if let Some(mut entry) = self.invocations.get_mut(&key) {
            entry.set_status(InvocationStatus::WaitingForTimer);
            entry.set_timer_fire_at(Some(fire_at));
            entry.set_timer_name(timer_name.map(|s| s.to_string()));

            // Notify timer processor that a new timer was scheduled
            self.timer_notify.notify_one();

            Ok(())
        } else {
            Err(StorageError::InvocationNotFound { id: flow_id, step })
        }
    }

    async fn log_signal(&self, flow_id: Uuid, step: i32, signal_name: &str) -> Result<()> {
        let key = (flow_id, step);

        if let Some(mut entry) = self.invocations.get_mut(&key) {
            entry.set_status(InvocationStatus::WaitingForSignal);
            entry.set_timer_name(Some(signal_name.to_string()));
            Ok(())
        } else {
            Err(StorageError::InvocationNotFound { id: flow_id, step })
        }
    }

    async fn store_suspension_result(
        &self,
        flow_id: Uuid,
        step: i32,
        suspension_key: &str,
        result: &[u8],
    ) -> Result<()> {
        let key = (flow_id, step, suspension_key.to_string());
        self.suspension_params.insert(key, result.to_vec());
        Ok(())
    }

    async fn get_suspension_result(
        &self,
        flow_id: Uuid,
        step: i32,
        suspension_key: &str,
    ) -> Result<Option<Vec<u8>>> {
        let key = (flow_id, step, suspension_key.to_string());
        Ok(self.suspension_params.get(&key).map(|v| v.clone()))
    }

    async fn remove_suspension_result(
        &self,
        flow_id: Uuid,
        step: i32,
        suspension_key: &str,
    ) -> Result<()> {
        let key = (flow_id, step, suspension_key.to_string());
        self.suspension_params.remove(&key);
        Ok(())
    }

    async fn store_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
        params: &[u8],
    ) -> Result<()> {
        self.store_suspension_result(flow_id, step, signal_name, params)
            .await
    }

    async fn get_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
    ) -> Result<Option<Vec<u8>>> {
        self.get_suspension_result(flow_id, step, signal_name).await
    }

    async fn remove_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
    ) -> Result<()> {
        self.remove_suspension_result(flow_id, step, signal_name)
            .await
    }

    async fn get_waiting_signals(&self) -> Result<Vec<super::SignalInfo>> {
        let mut signals = Vec::new();

        for entry in self.invocations.iter() {
            let (flow_id, step) = *entry.key();
            let inv = entry.value();

            // Check if this invocation is waiting for a signal
            if inv.status() == InvocationStatus::WaitingForSignal {
                signals.push(super::SignalInfo {
                    flow_id,
                    step,
                    signal_name: inv.timer_name().map(|s| s.to_string()),
                });
            }
        }

        Ok(signals)
    }

    async fn cleanup_completed(&self, older_than: std::time::Duration) -> Result<u64> {
        let chrono_duration = chrono::Duration::from_std(older_than)
            .map_err(|e| StorageError::InvalidParameter(format!("Invalid duration: {}", e)))?;
        let cutoff = Utc::now() - chrono_duration;

        // Count and remove completed invocations older than cutoff
        let mut deleted = 0u64;
        self.invocations.retain(|_key, inv| {
            if inv.status() == InvocationStatus::Complete && inv.timestamp() < cutoff {
                deleted += 1;
                false // Remove this entry
            } else {
                true // Keep this entry
            }
        });

        // Cleanup old suspension parameters (signals and timers)
        self.suspension_params.clear(); // In-memory can just clear all suspension params

        // Cleanup old completed flows from queue
        self.flow_queue.retain(|_key, flow| {
            !(flow.status == super::TaskStatus::Complete
                || flow.status == super::TaskStatus::Failed)
                || flow.updated_at >= cutoff
        });

        Ok(deleted)
    }

    async fn resume_flow(&self, flow_id: Uuid) -> Result<bool> {
        // O(1) lookup using flow_id -> task_id index
        let Some(task_id) = self.flow_task_map.get(&flow_id).map(|v| *v) else {
            return Ok(false);
        };

        // Atomically check and update the flow status
        // ONLY resume flows that are SUSPENDED (waiting for signals/child flows)
        if let Some(mut entry) = self.flow_queue.get_mut(&task_id) {
            if entry.status == super::TaskStatus::Suspended {
                entry.status = super::TaskStatus::Pending;
                entry.locked_by = None;
                entry.updated_at = Utc::now();

                // Re-enqueue to channel (mimics Redis re-adding to stream)
                // This is the critical fix for signals!
                self.pending_tx
                    .send(task_id)
                    .map_err(|_| StorageError::Connection("pending channel closed".to_string()))?;

                // Wake up one waiting worker since we just made a flow available
                self.work_notify.notify_one();

                return Ok(true);
            }
        }

        Ok(false)
    }
}

// Implement notification source traits for type-safe access
impl WorkNotificationSource for InMemoryExecutionLog {
    fn work_notify(&self) -> &Arc<Notify> {
        &self.work_notify
    }
}

impl TimerNotificationSource for InMemoryExecutionLog {
    fn timer_notify(&self) -> &Arc<Notify> {
        &self.timer_notify
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::TaskStatus;

    #[tokio::test]
    async fn test_enqueue_and_get_flow() {
        let log = InMemoryExecutionLog::new();
        let flow_id = Uuid::new_v4();
        let flow = super::super::ScheduledFlow::new(flow_id, "TestFlow".to_string(), vec![1, 2, 3]);
        let task_id = flow.task_id;

        // Enqueue
        let returned_id = log.enqueue_flow(flow).await.unwrap();
        assert_eq!(returned_id, task_id);

        // Get
        let retrieved = log.get_scheduled_flow(task_id).await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.flow_id, flow_id);
        assert_eq!(retrieved.flow_type, "TestFlow");
        assert_eq!(retrieved.status, TaskStatus::Pending);
    }

    #[tokio::test]
    async fn test_dequeue_locks_flow() {
        let log = InMemoryExecutionLog::new();
        let flow_id = Uuid::new_v4();
        let flow = super::super::ScheduledFlow::new(flow_id, "TestFlow".to_string(), vec![1, 2, 3]);
        let task_id = flow.task_id;

        log.enqueue_flow(flow).await.unwrap();

        // Dequeue
        let dequeued = log.dequeue_flow("worker-1").await.unwrap();
        assert!(dequeued.is_some());
        let dequeued = dequeued.unwrap();
        assert_eq!(dequeued.task_id, task_id);
        assert_eq!(dequeued.status, TaskStatus::Running);
        assert_eq!(dequeued.locked_by, Some("worker-1".to_string()));

        // Second dequeue should return None (already locked)
        let second = log.dequeue_flow("worker-2").await.unwrap();
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn test_dequeue_fifo_order() {
        let log = InMemoryExecutionLog::new();

        // Enqueue 3 flows
        let flow1 = super::super::ScheduledFlow::new(Uuid::new_v4(), "Flow1".to_string(), vec![1]);
        let task1 = flow1.task_id;
        log.enqueue_flow(flow1).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let flow2 = super::super::ScheduledFlow::new(Uuid::new_v4(), "Flow2".to_string(), vec![2]);
        log.enqueue_flow(flow2).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let flow3 = super::super::ScheduledFlow::new(Uuid::new_v4(), "Flow3".to_string(), vec![3]);
        log.enqueue_flow(flow3).await.unwrap();

        // Dequeue should return oldest first
        let first = log.dequeue_flow("worker-1").await.unwrap().unwrap();
        assert_eq!(first.task_id, task1);
    }

    #[tokio::test]
    async fn test_complete_flow() {
        let log = InMemoryExecutionLog::new();
        let flow =
            super::super::ScheduledFlow::new(Uuid::new_v4(), "TestFlow".to_string(), vec![1, 2, 3]);
        let task_id = flow.task_id;

        log.enqueue_flow(flow).await.unwrap();
        log.dequeue_flow("worker-1").await.unwrap();

        // Mark as complete
        log.complete_flow(task_id, TaskStatus::Complete)
            .await
            .unwrap();

        // Verify status updated
        let flow = log.get_scheduled_flow(task_id).await.unwrap().unwrap();
        assert_eq!(flow.status, TaskStatus::Complete);
    }

    #[tokio::test]
    async fn test_complete_nonexistent_flow() {
        let log = InMemoryExecutionLog::new();
        let fake_id = Uuid::new_v4();

        let result = log.complete_flow(fake_id, TaskStatus::Complete).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            StorageError::ScheduledFlowNotFound(_)
        ));
    }

    #[tokio::test]
    async fn test_concurrent_dequeue() {
        let log = std::sync::Arc::new(InMemoryExecutionLog::new());

        // Enqueue 10 flows
        for i in 0..10 {
            let flow = super::super::ScheduledFlow::new(
                Uuid::new_v4(),
                format!("Flow{}", i),
                vec![i as u8],
            );
            log.enqueue_flow(flow).await.unwrap();
        }

        // Spawn 3 workers concurrently
        let mut handles = vec![];
        for worker_id in 1..=3 {
            let log_clone = log.clone();
            let handle = tokio::spawn(async move {
                let mut count = 0;
                for _ in 0..5 {
                    if let Some(_flow) = log_clone
                        .dequeue_flow(&format!("worker-{}", worker_id))
                        .await
                        .unwrap()
                    {
                        count += 1;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                }
                count
            });
            handles.push(handle);
        }

        // Wait for all workers
        let mut total = 0;
        for handle in handles {
            total += handle.await.unwrap();
        }

        // All 10 flows should be dequeued exactly once
        assert_eq!(total, 10);
    }
}
