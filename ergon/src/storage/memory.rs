use super::{error::Result, error::StorageError, params::InvocationStartParams, ExecutionLog};
use crate::core::{hash_params, Invocation, InvocationStatus};
use async_trait::async_trait;
use chrono::Utc;
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
    /// Concurrent storage for signal parameters keyed by (flow_id, step)
    signal_params: dashmap::DashMap<(Uuid, i32), Vec<u8>>,
}

impl InMemoryExecutionLog {
    /// Creates a new in-memory execution log.
    pub fn new() -> Self {
        Self {
            invocations: dashmap::DashMap::new(),
            flow_queue: dashmap::DashMap::new(),
            signal_params: dashmap::DashMap::new(),
        }
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

        Ok(invocations)
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
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    // ===== Distributed Queue Operations =====

    async fn enqueue_flow(&self, flow: super::ScheduledFlow) -> Result<Uuid> {
        let task_id = flow.task_id;
        self.flow_queue.insert(task_id, flow);
        Ok(task_id)
    }

    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<super::ScheduledFlow>> {
        // Retry loop to handle race conditions when multiple workers compete
        // for the same flow. This ensures we find the next oldest flow if the
        // first candidate is claimed by another worker.
        loop {
            // Find the oldest pending flow that is ready to execute
            // Note: DashMap doesn't have built-in ordering, so we need to scan
            let mut oldest: Option<(Uuid, super::ScheduledFlow)> = None;
            let now = Utc::now();

            for entry in self.flow_queue.iter() {
                let flow = entry.value();
                // Only consider pending flows that are ready to execute
                if flow.status == super::TaskStatus::Pending {
                    // Check if scheduled_for time has passed (or is None)
                    let is_ready = flow.scheduled_for.is_none_or(|scheduled| scheduled <= now);

                    if is_ready {
                        if let Some((_, ref current_oldest)) = oldest {
                            if flow.created_at < current_oldest.created_at {
                                oldest = Some((*entry.key(), flow.clone()));
                            }
                        } else {
                            oldest = Some((*entry.key(), flow.clone()));
                        }
                    }
                }
            }

            // If no pending flows found, return None
            let Some((task_id, mut flow)) = oldest else {
                return Ok(None);
            };

            // Try to atomically lock the flow
            if let Some(mut entry) = self.flow_queue.get_mut(&task_id) {
                // Double-check it's still pending (another thread might have taken it)
                if entry.status == super::TaskStatus::Pending {
                    // Successfully locked it
                    entry.status = super::TaskStatus::Running;
                    entry.locked_by = Some(worker_id.to_string());
                    entry.updated_at = Utc::now();

                    // Return the updated flow
                    flow.status = super::TaskStatus::Running;
                    flow.locked_by = Some(worker_id.to_string());
                    flow.updated_at = Utc::now();
                    return Ok(Some(flow));
                }
            }

            // Flow was claimed by another worker, retry to find the next oldest
            continue;
        }
    }

    async fn complete_flow(&self, task_id: Uuid, status: super::TaskStatus) -> Result<()> {
        if let Some(mut entry) = self.flow_queue.get_mut(&task_id) {
            entry.status = status;
            entry.updated_at = Utc::now();
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
            entry.scheduled_for = Some(Utc::now() + chrono::Duration::from_std(delay).unwrap());
            // Update timestamp
            entry.updated_at = Utc::now();
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
                return Ok(true);
            }
        }

        Ok(false)
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
            Ok(())
        } else {
            Err(StorageError::InvocationNotFound { id: flow_id, step })
        }
    }

    async fn store_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        params: &[u8],
    ) -> Result<()> {
        let key = (flow_id, step);
        self.signal_params.insert(key, params.to_vec());
        Ok(())
    }

    async fn get_signal_params(&self, flow_id: Uuid, step: i32) -> Result<Option<Vec<u8>>> {
        let key = (flow_id, step);
        Ok(self.signal_params.get(&key).map(|v| v.clone()))
    }

    async fn remove_signal_params(&self, flow_id: Uuid, step: i32) -> Result<()> {
        let key = (flow_id, step);
        self.signal_params.remove(&key);
        Ok(())
    }

    async fn cleanup_completed(&self, older_than: std::time::Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::from_std(older_than).unwrap();

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

        // Cleanup old signal parameters
        self.signal_params.clear(); // In-memory can just clear all signal params

        // Cleanup old completed flows from queue
        self.flow_queue.retain(|_key, flow| {
            !(flow.status == super::TaskStatus::Complete
                || flow.status == super::TaskStatus::Failed)
                || flow.updated_at >= cutoff
        });

        Ok(deleted)
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
