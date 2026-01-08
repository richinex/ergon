//! Durable timer implementation.
//!
//! This module hides the complexity of:
//! - Timer persistence and recovery
//! - Timer processor coordination
//! - Fire time calculation and storage
//!
//! Following the same pattern as signal.rs, this module provides
//! a simple API for scheduling durable timers that survive crashes.

use super::context::EXECUTION_CONTEXT;
use super::error::{ExecutionError, Result, SuspendReason};
use crate::core::InvocationStatus;
use crate::storage::ExecutionLog;
use chrono::{Duration as ChronoDuration, Utc};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

// ============================================================================
// Timer Typestates (moved from worker.rs)
// ============================================================================

/// Typestate: Worker without timer processing
pub struct WithoutTimers;

/// Typestate: Worker with timer processing enabled
pub struct WithTimers {
    pub timer_poll_interval: Duration,
}

// ============================================================================
// Timer Processing Trait
// ============================================================================

/// Trait that defines timer processing behavior based on type state.
///
/// This trait uses the typestate pattern to provide different behaviors
/// for workers with and without timer processing enabled.
#[async_trait::async_trait]
pub trait TimerProcessing: Send + Sync {
    async fn process_timers<S: ExecutionLog>(&self, storage: &Arc<S>, worker_id: &str);
    fn timer_poll_interval(&self) -> Duration;
}

#[async_trait::async_trait]
impl TimerProcessing for WithoutTimers {
    async fn process_timers<S: ExecutionLog>(&self, _storage: &Arc<S>, _worker_id: &str) {
        // No-op: timer processing disabled
    }

    fn timer_poll_interval(&self) -> Duration {
        // Return a very long interval since timers are disabled
        Duration::from_secs(3600)
    }
}

#[async_trait::async_trait]
impl TimerProcessing for WithTimers {
    fn timer_poll_interval(&self) -> Duration {
        self.timer_poll_interval
    }

    async fn process_timers<S: ExecutionLog>(&self, storage: &Arc<S>, _worker_id: &str) {
        // Fetch expired timers and process them
        match storage.get_expired_timers(Utc::now()).await {
            Ok(timers) => {
                if !timers.is_empty() {
                    debug!("Processing {} expired timers", timers.len());
                }

                for timer in timers {
                    // Try to claim the timer (optimistic concurrency)
                    match storage.claim_timer(timer.flow_id, timer.step).await {
                        Ok(true) => {
                            // Successfully claimed - resume the flow
                            info!(
                                "Timer fired: flow={} step={} name={:?}",
                                timer.flow_id, timer.step, timer.timer_name
                            );

                            // Store the timer fired marker for caching (like signals do)
                            // This prevents re-execution of the timer step on resume
                            // NOTE: The actual step result will be stored when the step completes
                            // For now, we just mark that the timer has fired
                            let timer_key = timer.timer_name.as_deref().unwrap_or("");

                            // Use SuspensionPayload (same structure as signals, but for timers)
                            // Store success=true with empty data to indicate timer fired
                            // The step will execute and store its actual result
                            use crate::executor::child_flow::SuspensionPayload;
                            let payload = SuspensionPayload {
                                success: true,
                                data: vec![], // Empty - timer doesn't carry data, just marks delay completion
                                is_retryable: None,
                            };
                            let result = crate::core::serialize_value(&payload).unwrap_or_default();

                            if let Err(e) = storage
                                .store_suspension_result(
                                    timer.flow_id,
                                    timer.step,
                                    timer_key,
                                    &result,
                                )
                                .await
                            {
                                warn!(
                                    "Failed to store timer result (will still resume): flow={} step={} key={:?} error={}",
                                    timer.flow_id, timer.step, timer_key, e
                                );
                            }

                            // Resume the flow by re-enqueuing it
                            match storage.resume_flow(timer.flow_id).await {
                                Ok(true) => debug!("Resumed flow after timer: {}", timer.flow_id),
                                Ok(false) => debug!(
                                    "Flow {} not in SUSPENDED state after timer (may have already resumed)",
                                    timer.flow_id
                                ),
                                Err(e) => warn!(
                                    "Failed to resume flow after timer: flow={} step={} error={}",
                                    timer.flow_id, timer.step, e
                                ),
                            }
                        }
                        Ok(false) => {
                            // Another worker already claimed it
                            debug!(
                                "Timer already fired by another worker: flow={} step={}",
                                timer.flow_id, timer.step
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to claim timer (will retry): flow={} step={} error={}",
                                timer.flow_id, timer.step, e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to fetch expired timers: {}", e);
            }
        }
    }
}

// ============================================================================
// Timer Scheduling API
// ============================================================================

/// Schedules a durable timer that pauses workflow execution.
///
/// The timer is persisted to storage and will fire even if the worker
/// crashes. When the timer fires, execution resumes from this point.
///
/// # Guarantees
/// - **Durable**: Timer survives worker crashes
/// - **Eventually Fires**: If timer missed while worker was down, fires immediately on recovery
/// - **Idempotent**: If timer fires multiple times, only first firing proceeds (step caching)
///
/// # Errors
/// Returns an error if storage operations fail (network down, disk full, database locked).
///
/// # Example
/// ```ignore
/// #[step]
/// async fn wait_five_minutes(&self) -> Result<(), ExecutionError> {
///     schedule_timer(Duration::from_secs(300)).await?;
///     Ok(())
/// }
/// ```
pub async fn schedule_timer(duration: Duration) -> Result<()> {
    schedule_timer_impl(duration, None).await
}

/// Schedules a named timer for debugging and observability.
///
/// Named timers appear in logs and can be queried for monitoring.
///
/// # Errors
/// Returns an error if storage operations fail.
///
/// # Example
/// ```ignore
/// #[step]
/// async fn wait_for_trial_expiry(&self) -> Result<(), ExecutionError> {
///     schedule_timer_named(Duration::from_secs(14 * 24 * 3600), "trial-expiry").await?;
///     Ok(())
/// }
/// ```
pub async fn schedule_timer_named(duration: Duration, name: &str) -> Result<()> {
    schedule_timer_impl(duration, Some(name)).await
}

/// Internal implementation that handles timer scheduling.
async fn schedule_timer_impl(duration: Duration, name: Option<&str>) -> Result<()> {
    let ctx = EXECUTION_CONTEXT
        .try_with(|c| c.clone())
        .expect("schedule_timer called outside execution context");

    // Get the step number from enclosing step (set by #[step] macro)
    // Steps now use hash-based IDs, not counters
    let current_step = ctx
        .get_enclosing_step()
        .expect("schedule_timer called but no enclosing step set");

    // Check if we're resuming from a timer
    let existing_inv = ctx
        .storage
        .get_invocation(ctx.id, current_step)
        .await
        .map_err(ExecutionError::from)?;

    if let Some(inv) = existing_inv {
        if inv.status() == InvocationStatus::Complete {
            // Timer already fired and step completed - we're resuming
            return Ok(());
        }

        if inv.status() == InvocationStatus::WaitingForTimer {
            // We're waiting for this timer - check if it's fired and has a cached result
            let timer_key = name.unwrap_or("");

            if let Some(result_bytes) = ctx
                .storage
                .get_suspension_result(ctx.id, current_step, timer_key)
                .await
                .map_err(ExecutionError::from)?
            {
                // Timer fired! Deserialize SuspensionPayload (consistent with signals)
                use crate::core::deserialize_value;
                use crate::executor::child_flow::SuspensionPayload;

                let payload: SuspensionPayload = deserialize_value(&result_bytes).map_err(|e| {
                    ExecutionError::Failed(format!("Failed to deserialize timer payload: {}", e))
                })?;

                // Clean up suspension result so it isn't re-delivered on retry
                ctx.storage
                    .remove_suspension_result(ctx.id, current_step, timer_key)
                    .await
                    .map_err(ExecutionError::from)?;

                // Timer fired successfully (payload.success should be true, data is empty)
                if payload.success {
                    return Ok(());
                } else {
                    // This shouldn't happen for timers, but handle it gracefully
                    return Err(ExecutionError::Failed("Timer marked as failed".to_string()));
                }
            }

            // Timer not fired yet - suspend the flow
            // Set suspension in context for worker to detect
            let reason = SuspendReason::Timer {
                flow_id: ctx.id,
                step: current_step,
            };
            ctx.set_suspend_reason(reason);

            // Suspension is control flow, not an error. Return Poll::Pending.
            // Worker will poll this future, get Poll::Pending, and check ctx.take_suspend_reason()
            // to determine the suspension reason. This makes suspension invisible to user code.
            return std::future::pending::<Result<()>>().await;
        }
    }

    // First time - calculate fire time and log timer
    let now = Utc::now();
    let fire_at = ChronoDuration::from_std(duration)
        .map(|d| now + d)
        .unwrap_or_else(|_| now + ChronoDuration::MAX);

    ctx.storage
        .log_timer(ctx.id, current_step, fire_at, name)
        .await
        .map_err(ExecutionError::from)?;

    // Suspend the flow - it will be resumed when the timer fires
    // Set suspension in context for worker to detect
    let reason = SuspendReason::Timer {
        flow_id: ctx.id,
        step: current_step,
    };
    ctx.set_suspend_reason(reason);

    // Suspension is control flow, not an error. Return Poll::Pending.
    // Worker will poll this future, get Poll::Pending, and check ctx.take_suspend_reason()
    // to determine the suspension reason. This makes suspension invisible to user code.
    std::future::pending::<Result<()>>().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::serialize_value;
    use crate::executor::child_flow::SuspensionPayload;
    
    use crate::storage::{InMemoryExecutionLog, InvocationStartParams};
    use uuid::Uuid;

    // =========================================================================
    // TimerProcessing Trait Tests
    // =========================================================================

    #[test]
    fn test_without_timers_poll_interval() {
        let without_timers = WithoutTimers;
        assert_eq!(
            without_timers.timer_poll_interval(),
            Duration::from_secs(3600)
        );
    }

    #[tokio::test]
    async fn test_without_timers_process_is_noop() {
        let without_timers = WithoutTimers;
        let storage = Arc::new(InMemoryExecutionLog::new());

        // Should not panic or do anything
        without_timers.process_timers(&storage, "test-worker").await;
    }

    #[test]
    fn test_with_timers_poll_interval() {
        let with_timers = WithTimers {
            timer_poll_interval: Duration::from_millis(100),
        };

        assert_eq!(
            with_timers.timer_poll_interval(),
            Duration::from_millis(100)
        );
    }

    #[tokio::test]
    async fn test_with_timers_process_no_expired() {
        let with_timers = WithTimers {
            timer_poll_interval: Duration::from_millis(100),
        };
        let storage = Arc::new(InMemoryExecutionLog::new());

        // Should complete without error when no timers are expired
        with_timers.process_timers(&storage, "test-worker").await;
    }

    #[tokio::test]
    async fn test_with_timers_process_expired_timer() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();

        // Setup: Create a timer that has already expired
        let past_time = Utc::now() - ChronoDuration::seconds(10);
        storage
            .log_timer(flow_id, 1, past_time, Some("test-timer"))
            .await
            .unwrap();

        let with_timers = WithTimers {
            timer_poll_interval: Duration::from_millis(100),
        };

        // Process timers
        with_timers.process_timers(&storage, "test-worker").await;

        // Verify timer was claimed and result stored
        let result = storage
            .get_suspension_result(flow_id, 1, "test-timer")
            .await
            .unwrap();
        assert!(result.is_some());

        // Verify it's a valid SuspensionPayload
        let payload: SuspensionPayload = crate::core::deserialize_value(&result.unwrap()).unwrap();
        assert!(payload.success);
        assert!(payload.data.is_empty()); // Timers have empty data
    }

    #[tokio::test]
    async fn test_with_timers_claim_timer_success() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();

        // Create an expired timer
        let past_time = Utc::now() - ChronoDuration::seconds(5);
        storage
            .log_timer(flow_id, 1, past_time, None)
            .await
            .unwrap();

        // First claim should succeed
        let claimed = storage.claim_timer(flow_id, 1).await.unwrap();
        assert!(claimed);

        // Second claim should fail (already claimed)
        let claimed_again = storage.claim_timer(flow_id, 1).await.unwrap();
        assert!(!claimed_again);
    }

    #[tokio::test]
    async fn test_with_timers_multiple_expired() {
        let storage = Arc::new(InMemoryExecutionLog::new());

        let flow1 = Uuid::new_v4();
        let flow2 = Uuid::new_v4();

        // Create two expired timers
        let past_time = Utc::now() - ChronoDuration::seconds(10);
        storage
            .log_timer(flow1, 1, past_time, Some("timer1"))
            .await
            .unwrap();
        storage
            .log_timer(flow2, 1, past_time, Some("timer2"))
            .await
            .unwrap();

        let with_timers = WithTimers {
            timer_poll_interval: Duration::from_millis(100),
        };

        // Process timers
        with_timers.process_timers(&storage, "test-worker").await;

        // Both should have results
        let result1 = storage
            .get_suspension_result(flow1, 1, "timer1")
            .await
            .unwrap();
        let result2 = storage
            .get_suspension_result(flow2, 1, "timer2")
            .await
            .unwrap();

        assert!(result1.is_some());
        assert!(result2.is_some());
    }

    #[tokio::test]
    async fn test_with_timers_not_yet_expired() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();

        // Create a timer that hasn't expired yet
        let future_time = Utc::now() + ChronoDuration::seconds(60);
        storage
            .log_timer(flow_id, 1, future_time, Some("future-timer"))
            .await
            .unwrap();

        let with_timers = WithTimers {
            timer_poll_interval: Duration::from_millis(100),
        };

        // Process timers
        with_timers.process_timers(&storage, "test-worker").await;

        // Should not have a result (not expired)
        let result = storage
            .get_suspension_result(flow_id, 1, "future-timer")
            .await
            .unwrap();
        assert!(result.is_none());
    }

    // =========================================================================
    // Timer Storage Tests
    // =========================================================================

    #[tokio::test]
    async fn test_log_timer_unnamed() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();
        let fire_at = Utc::now() + ChronoDuration::seconds(30);

        let result = storage.log_timer(flow_id, 1, fire_at, None).await;
        assert!(result.is_ok());

        // Verify timer was logged
        let inv = storage.get_invocation(flow_id, 1).await.unwrap().unwrap();
        assert_eq!(inv.status(), InvocationStatus::WaitingForTimer);
    }

    #[tokio::test]
    async fn test_log_timer_named() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();
        let fire_at = Utc::now() + ChronoDuration::seconds(30);

        let result = storage
            .log_timer(flow_id, 1, fire_at, Some("my-timer"))
            .await;
        assert!(result.is_ok());

        let inv = storage.get_invocation(flow_id, 1).await.unwrap().unwrap();
        assert_eq!(inv.status(), InvocationStatus::WaitingForTimer);
        assert_eq!(inv.timer_name(), Some("my-timer"));
    }

    #[tokio::test]
    async fn test_get_expired_timers_none() {
        let storage = Arc::new(InMemoryExecutionLog::new());

        let expired = storage.get_expired_timers(Utc::now()).await.unwrap();
        assert!(expired.is_empty());
    }

    #[tokio::test]
    async fn test_get_expired_timers_some() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();

        // Create an expired timer
        let past_time = Utc::now() - ChronoDuration::seconds(10);
        storage
            .log_timer(flow_id, 1, past_time, None)
            .await
            .unwrap();

        let expired = storage.get_expired_timers(Utc::now()).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].flow_id, flow_id);
        assert_eq!(expired[0].step, 1);
    }

    #[tokio::test]
    async fn test_get_expired_timers_filters_future() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow1 = Uuid::new_v4();
        let flow2 = Uuid::new_v4();

        // One expired, one future
        let past_time = Utc::now() - ChronoDuration::seconds(10);
        let future_time = Utc::now() + ChronoDuration::seconds(60);

        storage.log_timer(flow1, 1, past_time, None).await.unwrap();
        storage
            .log_timer(flow2, 1, future_time, None)
            .await
            .unwrap();

        let expired = storage.get_expired_timers(Utc::now()).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].flow_id, flow1);
    }

    #[tokio::test]
    async fn test_claim_timer_updates_status() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();

        // Create timer
        let past_time = Utc::now() - ChronoDuration::seconds(5);
        storage
            .log_timer(flow_id, 1, past_time, None)
            .await
            .unwrap();

        // Claim it
        storage.claim_timer(flow_id, 1).await.unwrap();

        // After claiming, it should not appear in expired timers
        let expired = storage.get_expired_timers(Utc::now()).await.unwrap();
        assert!(expired.is_empty());
    }

    #[tokio::test]
    async fn test_get_next_timer_fire_time_none() {
        let storage = Arc::new(InMemoryExecutionLog::new());

        let next = storage.get_next_timer_fire_time().await.unwrap();
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn test_get_next_timer_fire_time_some() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();
        let fire_at = Utc::now() + ChronoDuration::seconds(30);

        storage.log_timer(flow_id, 1, fire_at, None).await.unwrap();

        let next = storage.get_next_timer_fire_time().await.unwrap();
        assert!(next.is_some());
    }

    #[tokio::test]
    async fn test_get_next_timer_fire_time_returns_earliest() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow1 = Uuid::new_v4();
        let flow2 = Uuid::new_v4();

        let early_time = Utc::now() + ChronoDuration::seconds(10);
        let late_time = Utc::now() + ChronoDuration::seconds(60);

        storage.log_timer(flow1, 1, late_time, None).await.unwrap();
        storage.log_timer(flow2, 1, early_time, None).await.unwrap();

        let next = storage.get_next_timer_fire_time().await.unwrap();
        assert!(next.is_some());
        // Should return the earlier time (within a small tolerance for timing)
        let diff = (next.unwrap() - early_time).num_milliseconds().abs();
        assert!(diff < 1000, "Expected earliest timer, got diff {}ms", diff);
    }

    // =========================================================================
    // SuspensionPayload Tests (Timer-specific)
    // =========================================================================

    #[tokio::test]
    async fn test_timer_suspension_payload_structure() {
        let payload = SuspensionPayload {
            success: true,
            data: vec![], // Timers have empty data
            is_retryable: None,
        };

        let serialized = serialize_value(&payload).unwrap();
        let deserialized: SuspensionPayload = crate::core::deserialize_value(&serialized).unwrap();

        assert!(deserialized.success);
        assert!(deserialized.data.is_empty());
        assert!(deserialized.is_retryable.is_none());
    }

    // =========================================================================
    // Integration Tests
    // =========================================================================

    #[tokio::test]
    async fn test_timer_workflow_end_to_end() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();

        // Step 1: Schedule a timer that's already expired
        let past_time = Utc::now() - ChronoDuration::seconds(1);
        storage
            .log_timer(flow_id, 1, past_time, Some("test"))
            .await
            .unwrap();

        // Step 2: Worker processes timers
        let with_timers = WithTimers {
            timer_poll_interval: Duration::from_millis(100),
        };
        with_timers.process_timers(&storage, "test-worker").await;

        // Step 3: Verify timer fired
        let result = storage
            .get_suspension_result(flow_id, 1, "test")
            .await
            .unwrap();
        assert!(result.is_some());

        let payload: SuspensionPayload = crate::core::deserialize_value(&result.unwrap()).unwrap();
        assert!(payload.success);
        assert!(payload.data.is_empty());
    }

    #[tokio::test]
    async fn test_concurrent_timer_claiming() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();

        // Create an expired timer
        let past_time = Utc::now() - ChronoDuration::seconds(10);
        storage
            .log_timer(flow_id, 1, past_time, None)
            .await
            .unwrap();

        // Simulate two workers trying to claim the same timer
        let storage1 = storage.clone();
        let storage2 = storage.clone();

        let handle1 = tokio::spawn(async move { storage1.claim_timer(flow_id, 1).await });

        let handle2 = tokio::spawn(async move { storage2.claim_timer(flow_id, 1).await });

        let result1 = handle1.await.unwrap().unwrap();
        let result2 = handle2.await.unwrap().unwrap();

        // Exactly one should succeed
        assert!(result1 != result2, "One should succeed, one should fail");
        assert!(result1 || result2, "At least one should succeed");
    }

    #[tokio::test]
    async fn test_timer_with_completed_invocation() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let flow_id = Uuid::new_v4();

        // Create a completed invocation (simulating step that already finished)
        let params = InvocationStartParams {
            id: flow_id,
            step: 1,
            class_name: "TestFlow",
            method_name: "test_step()",
            status: InvocationStatus::Complete,
            parameters: &serialize_value(&()).unwrap(),
            retry_policy: None,
        };
        storage.log_invocation_start(params).await.unwrap();

        // Timer should not interfere with completed step
        let fire_at = Utc::now() + ChronoDuration::seconds(30);
        let result = storage.log_timer(flow_id, 1, fire_at, None).await;

        // This should succeed (timer logged but step is already complete)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_timer_processing_with_multiple_workers() {
        let storage = Arc::new(InMemoryExecutionLog::new());

        // Create multiple expired timers
        for i in 0..5 {
            let flow_id = Uuid::new_v4();
            let past_time = Utc::now() - ChronoDuration::seconds(10);
            storage
                .log_timer(flow_id, i, past_time, None)
                .await
                .unwrap();
        }

        // Create two workers
        let with_timers = WithTimers {
            timer_poll_interval: Duration::from_millis(100),
        };

        let storage1 = storage.clone();
        let storage2 = storage.clone();

        // Both workers process timers concurrently
        let handle1 = tokio::spawn(async move {
            with_timers.process_timers(&storage1, "worker-1").await;
        });

        let handle2 = tokio::spawn(async move {
            let with_timers2 = WithTimers {
                timer_poll_interval: Duration::from_millis(100),
            };
            with_timers2.process_timers(&storage2, "worker-2").await;
        });

        handle1.await.unwrap();
        handle2.await.unwrap();

        // All timers should be claimed (no duplicates due to optimistic locking)
        let remaining_expired = storage.get_expired_timers(Utc::now()).await.unwrap();
        assert!(remaining_expired.is_empty(), "All timers should be claimed");
    }
}
