//! External signal handling module.
//!
//! This module provides a simple API for awaiting external signals,
//! following the same pattern as timer.rs.
//!
//! # Example
//!
//! ```ignore
//! #[step]
//! async fn wait_for_approval(self: Arc<Self>) -> Result<String, ExecutionError> {
//!     let approval: String = await_external_signal("approval").await?;
//!     println!("Received approval: {}", approval);
//!     Ok(approval)
//! }
//! ```

use super::context::EXECUTION_CONTEXT;
use super::error::{ExecutionError, Result, SuspendReason};
use crate::core::{deserialize_value, InvocationStatus};

/// Wrapper type for step futures (for backwards compatibility with step macro).
///
/// This is used internally by the #[step] macro and should not be used directly.
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
                panic!("Step returned None outside of signal context (this should not happen with the new signal API)")
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// Awaits an external signal with the given name.
///
/// The signal is persisted to storage and will resume even if the worker
/// crashes. When the signal arrives, execution resumes from this point.
///
/// # Guarantees
/// - **Durable**: Signal survives worker crashes
/// - **Type-safe**: Signal data is deserialized to the expected type
/// - **Idempotent**: If signal arrives multiple times, only first arrival proceeds (step caching)
///
/// # Errors
/// Returns an error if storage operations fail or signal data cannot be deserialized.
///
/// # Example
/// ```ignore
/// #[step]
/// async fn wait_for_payment(self: Arc<Self>) -> Result<PaymentDetails, ExecutionError> {
///     let payment: PaymentDetails = await_external_signal("payment-received").await?;
///     Ok(payment)
/// }
/// ```
pub async fn await_external_signal<T>(signal_name: &str) -> Result<T>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let ctx = EXECUTION_CONTEXT
        .try_with(|c| c.clone())
        .expect("await_external_signal called outside execution context");

    // Get the step number that was just allocated by the step macro
    let current_step = ctx
        .last_allocated_step()
        .expect("await_external_signal called but no step allocated");

    // Check if we're resuming from a signal
    let existing_inv = ctx
        .storage
        .get_invocation(ctx.id, current_step)
        .await
        .map_err(ExecutionError::from)?;

    if let Some(inv) = existing_inv {
        if inv.status() == InvocationStatus::Complete {
            // Signal already received and step completed - we're resuming
            if let Some(bytes) = inv.return_value() {
                let result: T = deserialize_value(bytes)?;
                return Ok(result);
            }
            // If no return value stored, this is an error
            return Err(ExecutionError::Failed(
                "Signal step completed but no return value found".to_string(),
            ));
        }

        if inv.status() == InvocationStatus::WaitingForSignal {
            // We're waiting for this signal - check if it's arrived
            if let Some(params) = ctx.storage.get_signal_params(ctx.id, current_step).await? {
                // Signal arrived! Deserialize and return the data
                let result: T = deserialize_value(&params)?;
                // Clean up signal params
                ctx.storage
                    .remove_signal_params(ctx.id, current_step)
                    .await?;
                return Ok(result);
            }

            // Signal not arrived yet - suspend the flow
            // Set suspension in context (authoritative - survives error type conversion)
            let reason = SuspendReason::Signal {
                flow_id: ctx.id,
                step: current_step,
                signal_name: signal_name.to_string(),
            };
            ctx.set_suspend_reason(reason);
            // Return error to prevent step caching; worker checks context to detect suspension
            return Err(ExecutionError::Failed(
                "Flow suspended for signal".to_string(),
            ));
        }
    }

    // First time - log signal wait
    ctx.storage
        .log_signal(ctx.id, current_step, signal_name)
        .await
        .map_err(ExecutionError::from)?;

    // Suspend the flow - it will be resumed when the signal arrives
    // Set suspension in context (authoritative - survives error type conversion)
    let reason = SuspendReason::Signal {
        flow_id: ctx.id,
        step: current_step,
        signal_name: signal_name.to_string(),
    };
    ctx.set_suspend_reason(reason);
    // Return error to prevent step caching; worker checks context to detect suspension
    Err(ExecutionError::Failed(
        "Flow suspended for signal".to_string(),
    ))
}
