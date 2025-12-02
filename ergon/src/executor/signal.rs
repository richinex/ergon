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
/// This function is used when a step needs to wait for an external event
/// (like email confirmation, payment webhook, manual approval, etc.)
/// before continuing. It:
///
/// 1. Logs the step as `WAITING_FOR_SIGNAL` in the database
/// 2. Pauses execution until `signal_resume()` is called
/// 3. Returns the value provided in the resume signal
///
/// # Example
/// ```ignore
/// #[flow]
/// async fn signup_user(&self, email: &str) {
///     self.send_confirmation_email(email).await;
///
///     // Flow pauses here until user clicks confirmation link
///     let confirmed_at = await_external_signal(self.confirm_email(Utc::now())).await;
///
///     self.activate_account().await;
/// }
/// ```
///
/// # Panics
/// - Panics if called outside of an execution context (i.e., not within a flow)
/// - Panics if the step returns None in Resume mode (framework error)
///
/// # Errors
/// Returns an error if storage operations fail or signal parameters cannot be deserialized.
pub async fn await_external_signal<Fut, R>(step_future: StepFuture<Fut>) -> Result<R>
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
    pub(super) async fn await_signal<R: for<'de> serde::Deserialize<'de>>(&self) -> Result<R> {
        let notifier = WAIT_NOTIFIERS
            .entry(self.id)
            .or_insert_with(|| Arc::new(Notify::new()))
            .value()
            .clone();

        notifier.notified().await;

        let params = RESUME_PARAMS
            .remove(&self.id)
            .map(|(_, v)| v)
            .ok_or_else(|| ExecutionError::Failed("No resume parameters found".to_string()))?;

        let result: R = deserialize_value(&params)?;
        Ok(result)
    }
}
