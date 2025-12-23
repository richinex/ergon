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
use crate::storage::ExecutionLog;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};
use uuid::Uuid;

// ============================================================================
// Signal Typestates (moved from worker.rs)
// ============================================================================

/// Typestate: Worker without signal processing
pub struct WithoutSignals;

/// Typestate: Worker with signal processing enabled
pub struct WithSignals<Src> {
    pub signal_source: Arc<Src>,
    pub signal_poll_interval: Duration,
}

// ============================================================================
// Signal Processing Trait
// ============================================================================

/// Trait that defines signal processing behavior based on type state.
///
/// This trait uses the typestate pattern to provide different behaviors
/// for workers with and without signal processing enabled.
#[async_trait::async_trait]
pub trait SignalProcessing: Send + Sync {
    async fn process_signals<S: ExecutionLog>(&self, storage: &Arc<S>, worker_id: &str);
    fn signal_poll_interval(&self) -> Duration;
}

#[async_trait::async_trait]
impl SignalProcessing for WithoutSignals {
    async fn process_signals<S: ExecutionLog>(&self, _storage: &Arc<S>, _worker_id: &str) {
        // No-op: signal processing disabled
    }

    fn signal_poll_interval(&self) -> Duration {
        // Return a very long interval since signals are disabled
        Duration::from_secs(3600)
    }
}

#[async_trait::async_trait]
impl<Src> SignalProcessing for WithSignals<Src>
where
    Src: SignalSource + 'static,
{
    fn signal_poll_interval(&self) -> Duration {
        self.signal_poll_interval
    }

    async fn process_signals<S: ExecutionLog>(&self, storage: &Arc<S>, _worker_id: &str) {
        // Get all flows waiting for signals
        let waiting_signals = match storage.get_waiting_signals().await {
            Ok(signals) => signals,
            Err(e) => {
                warn!("Failed to fetch waiting signals: {}", e);
                return;
            }
        };

        if !waiting_signals.is_empty() {
            debug!("Processing {} waiting signals", waiting_signals.len());
        }

        for signal_info in waiting_signals {
            let Some(signal_name) = &signal_info.signal_name else {
                continue;
            };

            // Check if signal source has the signal
            if let Some(signal_data) = self.signal_source.poll_for_signal(signal_name).await {
                // Store signal params
                match storage
                    .store_suspension_result(
                        signal_info.flow_id,
                        signal_info.step,
                        signal_info.signal_name.as_deref().unwrap_or(""),
                        &signal_data,
                    )
                    .await
                {
                    Ok(_) => {
                        // Consume signal so we don't re-process
                        self.signal_source.consume_signal(signal_name).await;

                        // Try to resume flow
                        match storage.resume_flow(signal_info.flow_id).await {
                            Ok(true) => {
                                debug!(
                                    "Signal '{}' delivered to flow {}",
                                    signal_name, signal_info.flow_id
                                );
                            }
                            Ok(false) => {
                                debug!(
                                    "Signal '{}' stored for flow {} (will resume when suspended)",
                                    signal_name, signal_info.flow_id
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to resume flow {} after signal '{}': {}",
                                    signal_info.flow_id, signal_name, e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to store signal params for '{}': {}", signal_name, e);
                    }
                }
            }
        }
    }
}

// ============================================================================
// Signal Source Trait
// ============================================================================

/// Trait for external signal sources that can be integrated with the Worker.
///
/// Implement this trait to provide custom signal sources (HTTP webhooks,
/// stdin, message queues, etc.). The Worker will automatically poll for
/// signals and resume flows.
///
/// # Example
///
/// ```ignore
/// struct HttpSignalSource {
///     signals: Arc<RwLock<HashMap<String, Vec<u8>>>>,
/// }
///
/// #[async_trait]
/// impl SignalSource for HttpSignalSource {
///     async fn poll_for_signal(&self, signal_name: &str) -> Option<Vec<u8>> {
///         self.signals.write().await.remove(signal_name)
///     }
///
///     async fn consume_signal(&self, _signal_name: &str) {
///         // Already consumed in poll_for_signal
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait SignalSource: Send + Sync {
    /// Poll for a signal by name.
    ///
    /// Returns Some(signal_data) if the signal is available, None otherwise.
    async fn poll_for_signal(&self, signal_name: &str) -> Option<Vec<u8>>;

    /// Mark a signal as consumed after processing.
    ///
    /// This prevents the signal from being re-processed.
    async fn consume_signal(&self, signal_name: &str);
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
    T: serde::Serialize + DeserializeOwned,
{
    let ctx = EXECUTION_CONTEXT
        .try_with(|c| c.clone())
        .expect("await_external_signal called outside execution context");

    // Get the step number from enclosing step (set by #[step] macro)
    // Steps now use hash-based IDs, not counters
    let current_step = ctx
        .get_enclosing_step()
        .expect("await_external_signal called but no enclosing step set");

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
            if let Some(params) = ctx
                .storage
                .get_suspension_result(ctx.id, current_step, signal_name)
                .await?
            {
                // Signal arrived! Deserialize and return the data
                let result: T = deserialize_value(&params)?;
                // DO NOT mark invocation as complete here - let the step macro do it!
                // The step might return an error after reading the signal,
                // and we don't want to cache it as Complete if it fails.
                // Clean up signal params so they aren't re-delivered on retry
                ctx.storage
                    .remove_suspension_result(ctx.id, current_step, signal_name)
                    .await?;
                return Ok(result);
            }

            // Signal not arrived yet - suspend the flow
            // Set suspension in context for worker to detect
            let reason = SuspendReason::Signal {
                flow_id: ctx.id,
                step: current_step,
                signal_name: signal_name.to_string(),
            };
            ctx.set_suspend_reason(reason);

            // Suspension is control flow, not an error. Return Poll::Pending.
            // Worker will poll this future, get Poll::Pending, and check ctx.take_suspend_reason()
            // to determine the suspension reason. This makes suspension invisible to user code.
            return std::future::pending::<Result<T>>().await;
        }
    }

    // First time - log signal wait
    ctx.storage
        .log_signal(ctx.id, current_step, signal_name)
        .await
        .map_err(ExecutionError::from)?;

    // Suspend the flow - it will be resumed when the signal arrives
    // Set suspension in context for worker to detect
    let reason = SuspendReason::Signal {
        flow_id: ctx.id,
        step: current_step,
        signal_name: signal_name.to_string(),
    };
    ctx.set_suspend_reason(reason);

    // Suspension is control flow, not an error. Return Poll::Pending.
    // Worker will poll this future, get Poll::Pending, and check ctx.take_suspend_reason()
    // to determine the suspension reason. This makes suspension invisible to user code.
    std::future::pending::<Result<T>>().await
}

/// Level 1 API: Explicitly signals a parent flow from within a child flow.
///
/// Use this when you manually schedule child flows and need explicit control
/// over when and how to signal the parent. For simpler cases, consider using
/// the Level 3 `invoke()` API which handles parent signaling automatically.
///
/// # API Levels Comparison
///
/// - **Level 1 (this function)**: Manual scheduling + explicit signaling
///   - Parent schedules child with `Scheduler::schedule()`
///   - Child calls `signal_parent_flow()` explicitly
///   - More control, more boilerplate
///
/// - **Level 3 (`invoke()` API)**: Automatic scheduling + automatic signaling
///   - Parent calls `self.invoke(child).result().await?`
///   - Child just returns `Ok(result)` - worker handles signaling
///   - Less boilerplate, more convenience
///
/// # How It Works
///
/// This function encapsulates the low-level operations needed for parent-child
/// flow communication:
/// - Finding the parent's waiting step by method name
/// - Serializing the result
/// - Storing signal parameters
/// - Resuming the parent flow
///
/// # Arguments
///
/// * `parent_flow_id` - The UUID of the parent flow to signal
/// * `parent_method_name` - The name of the method the parent is waiting on (without parentheses)
/// * `result` - The typed result to send to the parent
///
/// # Errors
///
/// Returns `ExecutionError` if:
/// - Not called within a flow context
/// - Parent flow not found
/// - Parent not waiting for the specified signal
/// - Serialization fails
/// - Storage operations fail
///
/// # Example
///
/// ```ignore
/// use ergon::prelude::*;
/// use ergon::executor::signal_parent_flow;
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct InventoryResult {
///     available: bool,
///     warehouse: String,
/// }
///
/// impl MyFlow {
///     #[step]
///     async fn signal_parent(
///         self: Arc<Self>,
///         result: InventoryResult,
///     ) -> Result<(), ExecutionError> {
///         // Level 1 API: Explicitly signal parent
///         signal_parent_flow(
///             self.parent_flow_id,
///             "await_inventory_check",
///             result
///         ).await
///     }
/// }
/// ```
pub async fn signal_parent_flow<T>(
    parent_flow_id: Uuid,
    parent_method_name: &str,
    result: T,
) -> Result<()>
where
    T: serde::Serialize,
{
    let storage = EXECUTION_CONTEXT
        .try_with(|ctx| ctx.storage.clone())
        .expect("signal_parent_flow must be called within flow context");

    // Serialize result using JSON (same format as ergon internals)
    let result_bytes = crate::core::serialize_value(&result)?;

    // Find parent's waiting step
    let invocations = storage.get_invocations_for_flow(parent_flow_id).await?;

    // Construct full method name with parentheses (how ergon stores method names)
    let full_method_name = format!("{}()", parent_method_name);

    let waiting_step = invocations
        .iter()
        .find(|inv| {
            inv.status() == crate::core::InvocationStatus::WaitingForSignal
                && inv.method_name() == full_method_name
        })
        .ok_or_else(|| {
            ExecutionError::Failed(format!(
                "Parent flow {} not waiting for signal at method {}",
                parent_flow_id, full_method_name
            ))
        })?;

    // Store signal data
    storage
        .store_suspension_result(
            parent_flow_id,
            waiting_step.step(),
            waiting_step.timer_name().unwrap_or(""),
            &result_bytes,
        )
        .await?;

    // Resume parent flow - may return false if parent isn't suspended yet (race condition)
    // The worker's handle_suspended_flow will check for pending signals and resume
    match storage.resume_flow(parent_flow_id).await? {
        true => debug!("Resumed parent flow {}", parent_flow_id),
        false => debug!(
            "Parent flow {} not in SUSPENDED state (will resume when it suspends)",
            parent_flow_id
        ),
    }

    Ok(())
}
