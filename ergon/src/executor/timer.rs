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

                            // Store the timer result for caching (like signals do)
                            // This prevents re-execution of the timer step on resume
                            let timer_key = timer.timer_name.as_deref().unwrap_or("");
                            let result =
                                crate::core::serialize_value(&Ok::<(), super::ExecutionError>(()))
                                    .unwrap_or_default();

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
                // Timer fired and result is cached! Deserialize and return
                use crate::core::deserialize_value;
                let result: Result<()> = deserialize_value(&result_bytes).map_err(|e| {
                    ExecutionError::Failed(format!("Failed to deserialize timer result: {}", e))
                })?;

                // Clean up suspension result so it isn't re-delivered on retry
                ctx.storage
                    .remove_suspension_result(ctx.id, current_step, timer_key)
                    .await
                    .map_err(ExecutionError::from)?;

                return result;
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
