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
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use uuid::Uuid;

lazy_static::lazy_static! {
    /// Notifiers for timers that have fired.
    /// Maps (flow_id, step) -> Notify for waking waiting flows.
    pub(super) static ref TIMER_NOTIFIERS: Mutex<HashMap<(Uuid, i32), Arc<Notify>>> =
        Mutex::new(HashMap::new());
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
/// # Example
/// ```ignore
/// #[step]
/// async fn wait_five_minutes(&self) {
///     schedule_timer(Duration::from_secs(300)).await;
/// }
/// ```
pub async fn schedule_timer(duration: Duration) {
    schedule_timer_impl(duration, None).await
}

/// Schedules a named timer for debugging and observability.
///
/// Named timers appear in logs and can be queried for monitoring.
///
/// # Example
/// ```ignore
/// #[step]
/// async fn wait_for_trial_expiry(&self) {
///     schedule_timer_named(Duration::from_days(14), "trial-expiry").await;
/// }
/// ```
pub async fn schedule_timer_named(duration: Duration, name: &str) {
    schedule_timer_impl(duration, Some(name)).await
}

/// Internal implementation that handles timer scheduling.
async fn schedule_timer_impl(duration: Duration, name: Option<&str>) {
    let ctx = EXECUTION_CONTEXT
        .try_with(|c| c.clone())
        .expect("schedule_timer called outside execution context");

    // Get current step - the step macro already incremented it, so we need the previous value
    // The step macro calls next_step() which returns the old value and increments
    // So if the step was logged with step=N, the counter is now N+1, we need to use N
    let current_step = ctx.step_counter.load(Ordering::SeqCst) - 1;

    // Check if we're resuming from a timer
    let existing_inv = ctx
        .storage
        .get_invocation(ctx.id, current_step)
        .await
        .ok()
        .flatten();

    if let Some(inv) = existing_inv {
        if inv.status() == InvocationStatus::Complete {
            // Timer already fired and step completed - we're resuming
            return;
        }

        if inv.status() == InvocationStatus::WaitingForTimer {
            // We're waiting for this timer - check if it's fired
            if inv.is_timer_expired() {
                // Timer fired, but we haven't processed it yet
                // This can happen during replay - just continue
                return;
            }

            // Timer not fired yet - wait for notification
            ctx.await_timer(current_step).await.unwrap();
            return;
        }
    }

    // First time - calculate fire time and log timer
    let fire_at = Utc::now() + ChronoDuration::from_std(duration).unwrap();

    ctx.storage
        .log_timer(ctx.id, current_step, fire_at, name)
        .await
        .expect("Failed to log timer");

    // Wait for timer to fire
    // Note: There's a potential race condition here - the timer might fire between
    // logging and awaiting. The await_timer method handles this by checking the database.
    ctx.await_timer(current_step).await.unwrap();
}

impl<S: crate::storage::ExecutionLog> ExecutionContext<S> {
    /// Wait for a timer to fire.
    ///
    /// This method blocks until the timer processor fires the timer
    /// and notifies us via the TIMER_NOTIFIERS map.
    pub(super) async fn await_timer(&self, step: i32) -> Result<()> {
        let key = (self.id, step);

        let notifier = {
            let mut notifiers = TIMER_NOTIFIERS
                .lock()
                .expect("TIMER_NOTIFIERS Mutex poisoned");
            notifiers
                .entry(key)
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone()
        };

        // Check if timer already fired (handles race condition)
        if let Ok(Some(inv)) = self.storage.get_invocation(self.id, step).await {
            if inv.status() == InvocationStatus::Complete {
                // Timer already fired, no need to wait
                return Ok(());
            }
        }

        // Wait for timer to fire
        notifier.notified().await;

        // Clean up notifier
        {
            let mut notifiers = TIMER_NOTIFIERS
                .lock()
                .expect("TIMER_NOTIFIERS Mutex poisoned");
            notifiers.remove(&key);
        }

        Ok(())
    }
}
