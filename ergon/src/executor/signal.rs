//! External signal handling module.
//!
//! This module hides the complexity of:
//! - External signal coordination (wait/resume mechanism)
//! - StepFuture wrapping for await_external_signal
//! - Global state management for waiting flows
//!
//! Following Parnas's information hiding principle, this module encapsulates
//! decisions about how signals are coordinated across flow executions.

use super::context::{ExecutionContext, CALL_TYPE, EXECUTION_CONTEXT};
use super::error::{ExecutionError, Result};
use crate::core::{deserialize_value, CallType, InvocationStatus};
use dashmap::DashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use uuid::Uuid;

lazy_static::lazy_static! {
    pub(super) static ref WAIT_NOTIFIERS: DashMap<Uuid, Arc<Notify>> = DashMap::new();
    pub(super) static ref RESUME_PARAMS: DashMap<Uuid, Vec<u8>> = DashMap::new();
}

/// Wrapper type for step futures that return Option<R>.
/// This allows steps to safely return None in Await mode without using unsafe code.
///
/// When used in normal flow execution (not through await_external_signal), this Future resolves to R.
/// When used through await_external_signal, the Option<R> inner value is exposed for special handling.
pub struct StepFuture<Fut> {
    pub(crate) inner: Fut,
}

/// Builder for configuring signal timeouts.
///
/// Allows per-signal timeout configuration with explicit opt-out.
pub struct SignalBuilder<Fut, R>
where
    Fut: std::future::Future<Output = Option<R>>,
{
    future: StepFuture<Fut>,
    timeout: Option<Duration>,
}

impl<Fut, R> SignalBuilder<Fut, R>
where
    Fut: std::future::Future<Output = Option<R>>,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Create a new signal builder.
    pub fn new(future: StepFuture<Fut>) -> Self {
        Self {
            future,
            timeout: Some(Duration::from_secs(3600 * 24 * 7)), // Default: 7 days
        }
    }

    /// Set a custom timeout for this signal.
    ///
    /// # Example
    /// ```ignore
    /// await_external_signal(self.confirm_email())
    ///     .with_timeout(Duration::from_hours(48))
    ///     .await?;
    /// ```
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Disable timeout for this signal (wait indefinitely).
    ///
    /// Use this for critical flows that must wait for external events
    /// regardless of how long it takes.
    ///
    /// # Example
    /// ```ignore
    /// await_external_signal(self.legal_review())
    ///     .no_timeout()
    ///     .await?;
    /// ```
    pub fn no_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// Execute the signal wait with the configured timeout.
    pub async fn await_signal(self) -> Result<R> {
        if let Some(timeout) = self.timeout {
            match tokio::time::timeout(timeout, await_external_signal_impl(self.future)).await {
                Ok(result) => result,
                Err(_) => Err(ExecutionError::SignalTimeout {
                    message: format!("Signal timed out after {:?}", timeout),
                }),
            }
        } else {
            await_external_signal_impl(self.future).await
        }
    }
}

impl<Fut> StepFuture<Fut> {
    pub fn new(f: Fut) -> Self {
        Self { inner: f }
    }
}

// Implement Future for StepFuture so it can be awaited directly in flows
impl<Fut, R> std::future::Future for StepFuture<Fut>
where
    Fut: std::future::Future<Output = Option<R>>,
{
    type Output = R;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // SAFETY: We're projecting from Pin<&mut Self> to Pin<&mut Fut>
        // This is safe because StepFuture is structurally equivalent to Fut and
        // we never move out of `inner` after it's pinned.
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };

        match inner.poll(cx) {
            std::task::Poll::Ready(Some(value)) => std::task::Poll::Ready(value),
            std::task::Poll::Ready(None) => {
                panic!("Step returned None outside of await_external_signal context")
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// Awaits an external signal before continuing flow execution.
///
/// Returns a builder that allows configuring timeout behavior.
/// By default, signals timeout after 7 days.
///
/// # Example - Basic usage (default 7-day timeout)
/// ```ignore
/// let confirmed_at = await_external_signal(self.confirm_email())
///     .await_signal().await?;
/// ```
///
/// # Example - Custom timeout
/// ```ignore
/// let confirmed_at = await_external_signal(self.confirm_email())
///     .with_timeout(Duration::from_hours(48))
///     .await_signal().await?;
/// ```
///
/// # Example - No timeout for critical flows
/// ```ignore
/// let approval = await_external_signal(self.legal_review())
///     .no_timeout()
///     .await_signal().await?;
/// ```
pub fn await_external_signal<Fut, R>(step_future: StepFuture<Fut>) -> SignalBuilder<Fut, R>
where
    Fut: std::future::Future<Output = Option<R>>,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    SignalBuilder::new(step_future)
}

/// Internal implementation of signal waiting.
///
/// This function is used when a step needs to wait for an external event
/// (like email confirmation, payment webhook, manual approval, etc.)
/// before continuing. It:
///
/// 1. Logs the step as `WAITING_FOR_SIGNAL` in the database
/// 2. Pauses execution until `signal_resume()` is called
/// 3. Returns the value provided in the resume signal
///
/// # Panics
/// - Panics if called outside of an execution context (i.e., not within a flow)
/// - Panics if the step returns None in Resume mode (framework error)
///
/// # Errors
/// Returns an error if storage operations fail or signal parameters cannot be deserialized.
async fn await_external_signal_impl<Fut, R>(step_future: StepFuture<Fut>) -> Result<R>
where
    Fut: std::future::Future<Output = Option<R>>,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let ctx = EXECUTION_CONTEXT.try_with(|c| c.clone()).expect(
        "BUG: await_external_signal called outside execution context - this is a framework error",
    );

    // Get the current step number (peek without incrementing)
    let current_step = ctx.step_counter.load(Ordering::SeqCst);

    // Check if this step is already waiting for a signal
    let existing_inv = ctx
        .storage
        .get_invocation(ctx.id, current_step)
        .await
        .ok()
        .flatten();

    if let Some(inv) = existing_inv {
        if inv.status() == InvocationStatus::WaitingForSignal {
            // We're resuming - execute the step
            let result = CALL_TYPE.scope(CallType::Resume, step_future.inner).await;
            // In Resume mode, step should return Some(value)
            return result.ok_or_else(|| {
                ExecutionError::Failed(
                    "BUG: Step returned None in Resume mode - step implementation error"
                        .to_string(),
                )
            });
        }
    }

    // First time calling this await - set up waiting state
    CALL_TYPE
        .scope(CallType::Await, async {
            // Execute the step - it will return None in Await mode
            let result = step_future.inner.await;

            match result {
                None => {
                    // Step is awaiting - wait for external signal
                    ctx.await_signal().await
                }
                Some(value) => {
                    // Step completed immediately (shouldn't happen in Await mode)
                    Ok(value)
                }
            }
        })
        .await
}

impl ExecutionContext {
    /// Wait for an external signal to resume flow execution.
    ///
    /// This method is called internally by `await_external_signal` to block
    /// until a signal is received via `signal_resume`.
    ///
    /// Uses the defensive notified() pattern to avoid missing signals.
    /// Signal parameters are persisted to storage for crash recovery.
    pub(super) async fn await_signal<R: for<'de> serde::Deserialize<'de>>(&self) -> Result<R> {
        let current_step = self.step_counter.load(std::sync::atomic::Ordering::SeqCst);

        // Defensive pattern: Register for notification FIRST (like timer.rs)
        let notifier = WAIT_NOTIFIERS
            .entry(self.id)
            .or_insert_with(|| Arc::new(Notify::new()))
            .value()
            .clone();

        // Register for notification BEFORE checking if signal already arrived
        let notified = notifier.notified();

        // Check if signal parameters already exist in storage (signal arrived before we started waiting)
        if let Ok(Some(params)) = self.storage.get_signal_params(self.id, current_step).await {
            // Signal already arrived - clean up and return immediately
            WAIT_NOTIFIERS.remove(&self.id);
            RESUME_PARAMS.remove(&self.id);
            let _ = self.storage.remove_signal_params(self.id, current_step).await;
            let result: R = deserialize_value(&params)?;
            return Ok(result);
        }

        // Also check in-memory cache for backwards compatibility
        if let Some((_, params)) = RESUME_PARAMS.remove(&self.id) {
            // Signal already arrived - clean up and return immediately
            WAIT_NOTIFIERS.remove(&self.id);
            let _ = self.storage.remove_signal_params(self.id, current_step).await;
            let result: R = deserialize_value(&params)?;
            return Ok(result);
        }

        // Wait for signal notification
        notified.await;

        // Signal arrived - try storage first, then in-memory
        let params = if let Ok(Some(params)) = self.storage.get_signal_params(self.id, current_step).await {
            let _ = self.storage.remove_signal_params(self.id, current_step).await;
            params
        } else {
            RESUME_PARAMS
                .remove(&self.id)
                .map(|(_, v)| v)
                .ok_or_else(|| ExecutionError::Failed("No resume parameters found".to_string()))?
        };

        // Clean up notifier
        WAIT_NOTIFIERS.remove(&self.id);

        let result: R = deserialize_value(&params)?;
        Ok(result)
    }
}
