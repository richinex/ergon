//! Durable timer implementation.
//!
//! This module hides the complexity of:
//! - Timer persistence and recovery
//! - Timer processor coordination
//! - Fire time calculation and storage
//!
//! Following the same pattern as signal.rs, this module provides
//! a simple API for scheduling durable timers that survive crashes.

use super::context::{ExecutionContext, EXECUTION_CONTEXT};
use super::error::Result;
use crate::core::InvocationStatus;
use chrono::{Duration as ChronoDuration, Utc};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use uuid::Uuid;

lazy_static::lazy_static! {
    /// Notifiers for timers that have fired.
    /// Maps (flow_id, step) -> Notify for waking waiting flows.
    pub(super) static ref TIMER_NOTIFIERS: DashMap<(Uuid, i32), Arc<Notify>> = DashMap::new();
}

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
///     schedule_timer_named(Duration::from_days(14), "trial-expiry").await?;
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
        .map_err(super::error::ExecutionError::Storage)?;

    if let Some(inv) = existing_inv {
        if inv.status() == InvocationStatus::Complete {
            // Timer already fired and step completed - we're resuming
            return Ok(());
        }

        if inv.status() == InvocationStatus::WaitingForTimer {
            // We're waiting for this timer - check if it's fired
            if inv.is_timer_expired() {
                // Timer fired while worker was down - mark it complete explicitly
                ctx.complete_timer(current_step).await?;
                return Ok(());
            }

            // Timer not fired yet - wait for notification
            ctx.await_timer(current_step).await?;
            return Ok(());
        }
    }

    // First time - calculate fire time and log timer
    let fire_at = ChronoDuration::from_std(duration)
        .map(|d| Utc::now() + d)
        .unwrap_or_else(|_| Utc::now() + ChronoDuration::MAX);

    ctx.storage
        .log_timer(ctx.id, current_step, fire_at, name)
        .await
        .map_err(super::error::ExecutionError::Storage)?;

    // Wait for timer to fire
    // Note: There's a potential race condition here - the timer might fire between
    // logging and awaiting. The await_timer method handles this by checking the database.
    ctx.await_timer(current_step).await
}

impl ExecutionContext {
    /// Complete a timer that has already fired.
    ///
    /// This is called when replaying a step that was waiting for a timer
    /// that fired while the worker was down. It explicitly marks the timer
    /// step as complete to ensure proper state transitions.
    pub(super) async fn complete_timer(&self, step: i32) -> Result<()> {
        self.storage
            .log_invocation_completion(self.id, step, &crate::core::serialize_value(&())?)
            .await
            .map(|_| ())
            .map_err(super::error::ExecutionError::Storage)
    }

    /// Wait for a timer to fire.
    ///
    /// This method blocks until the timer processor fires the timer
    /// and notifies us via the TIMER_NOTIFIERS map.
    ///
    /// Uses the defensive notified() pattern to avoid missing notifications.
    pub(super) async fn await_timer(&self, step: i32) -> Result<()> {
        let key = (self.id, step);

        // Defensive pattern: Register for notification FIRST
        let notifier = TIMER_NOTIFIERS
            .entry(key)
            .or_insert_with(|| Arc::new(Notify::new()))
            .value()
            .clone();

        let notified = notifier.notified();

        // THEN check if already complete
        if let Ok(Some(inv)) = self.storage.get_invocation(self.id, step).await {
            if inv.status() == InvocationStatus::Complete {
                // Timer already fired, no need to wait
                TIMER_NOTIFIERS.remove(&key);
                return Ok(());
            }
        }

        // Wait for timer to fire (no timeout - worker shutdown handles cleanup)
        notified.await;

        // Clean up notifier
        TIMER_NOTIFIERS.remove(&key);

        Ok(())
    }
}
