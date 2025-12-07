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
use chrono::{Duration as ChronoDuration, Utc};
use std::time::Duration;

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

    // Get the step number that was just allocated by the step macro
    let current_step = ctx
        .last_allocated_step()
        .expect("schedule_timer called but no step allocated");

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
            // We're waiting for this timer - check if it's fired
            if inv.is_timer_expired() {
                // Timer fired - just return Ok and let step macro log completion
                // with the correct Result<(), ExecutionError> type
                return Ok(());
            }

            // Timer not fired yet - suspend the flow
            // Set suspension in context (authoritative - survives error type conversion)
            let reason = SuspendReason::Timer {
                flow_id: ctx.id,
                step: current_step,
            };
            ctx.set_suspend_reason(reason);
            // Return error to prevent step caching; worker checks context to detect suspension
            return Err(ExecutionError::Failed(
                "Flow suspended for timer".to_string(),
            ));
        }
    }

    // First time - calculate fire time and log timer
    let fire_at = ChronoDuration::from_std(duration)
        .map(|d| Utc::now() + d)
        .unwrap_or_else(|_| Utc::now() + ChronoDuration::MAX);

    ctx.storage
        .log_timer(ctx.id, current_step, fire_at, name)
        .await
        .map_err(ExecutionError::from)?;

    // Suspend the flow - it will be resumed when the timer fires
    // Set suspension in context (authoritative - survives error type conversion)
    let reason = SuspendReason::Timer {
        flow_id: ctx.id,
        step: current_step,
    };
    ctx.set_suspend_reason(reason);
    // Return error to prevent step caching; worker checks context to detect suspension
    Err(ExecutionError::Failed(
        "Flow suspended for timer".to_string(),
    ))
}
