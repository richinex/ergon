//! Flow execution outcome handling.
//!
//! This module hides the design decisions around:
//! - Retry policies and when to retry failed flows
//! - Parent signaling when child flows complete (Level 3 API)
//! - Suspension handling for timer/signal workflows
//! - Error handling and flow completion
//!
//! Following Parnas's principle: These decisions can change independently
//! of the worker loop implementation.

use crate::executor::ExecutionError;
use crate::storage::{ExecutionLog, ScheduledFlow, TaskStatus};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Completes a child flow and signals its parent (Level 3 API internal).
///
/// When `success = false`, the error is signaled to the parent. The parent
/// receives the child's error via ExecutionError and can handle it appropriately.
///
/// This is called by the worker when a child flow scheduled via invoke()
/// finishes execution. It handles:
/// - Reading the child's result from storage
/// - Wrapping it in a SuspensionPayload (success/error)
/// - Finding the parent's waiting step via signal token
/// - Storing signal parameters
/// - Resuming the parent flow
///
/// This is an internal method used by Level 3 API automatic parent-child
/// coordination. Users don't call this directly - they use invoke().result().
pub(super) async fn complete_child_flow<S: ExecutionLog>(
    storage: &Arc<S>,
    flow_id: Uuid,
    parent_metadata: Option<(Uuid, String)>,
    success: bool,
    error_msg: Option<&str>,
) {
    let Some((parent_id, signal_token)) = parent_metadata else {
        return;
    };

    // Prepare signal payload
    let payload = if success {
        // Read the flow's result from storage (invocation step 0)
        let invocation = match storage.get_invocation(flow_id, 0).await {
            Ok(Some(inv)) => inv,
            Ok(None) => {
                error!("No invocation found for successful flow {}", flow_id);
                return;
            }
            Err(e) => {
                error!("Failed to get invocation for flow {}: {}", flow_id, e);
                return;
            }
        };

        let Some(result_bytes) = invocation.return_value() else {
            error!("No result bytes for successful flow {}", flow_id);
            return;
        };

        // result_bytes contains a serialized Result<R, E>
        // Since we're in the success branch, we know it's Result::Ok(value)
        // With JSON serialization, we need to deserialize the Result, extract the value,
        // and re-serialize just the value (not the Result wrapper)
        let value_bytes = match serde_json::from_slice::<serde_json::Value>(result_bytes) {
            Ok(json_value) => {
                // Extract the Ok variant's value from the JSON Result
                // JSON Result format is: {"Ok": <value>} or {"Err": <error>}
                if let Some(ok_value) = json_value.get("Ok") {
                    // Re-serialize just the value
                    serde_json::to_vec(ok_value).unwrap_or_default()
                } else {
                    error!("Expected Ok variant in Result JSON for flow {}", flow_id);
                    vec![]
                }
            }
            Err(e) => {
                error!(
                    "Failed to deserialize Result JSON for flow {}: {}",
                    flow_id, e
                );
                vec![]
            }
        };

        crate::executor::child_flow::SuspensionPayload {
            success: true,
            data: value_bytes.to_vec(),
            is_retryable: None, // Not applicable for success
        }
    } else {
        // Error case - use the error_msg which now contains properly formatted error
        // After our worker.rs fix, error_msg will be like:
        // "child_flow_custom_errors::CreditCheckError: Credit score 580..."
        let error_msg = error_msg.unwrap_or("Unknown error");
        let error_bytes = crate::core::serialize_value(&error_msg).unwrap_or_default();

        // Read child's step 0 retryability flag
        let is_retryable = storage
            .get_invocation(flow_id, 0)
            .await
            .ok()
            .flatten()
            .and_then(|inv| inv.is_retryable());

        crate::executor::child_flow::SuspensionPayload {
            success: false,
            data: error_bytes,
            is_retryable,
        }
    };

    // Serialize and send signal
    let signal_bytes = match crate::core::serialize_value(&payload) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!(
                "Failed to serialize signal payload for parent {}: {}",
                parent_id, e
            );
            return;
        }
    };

    let invocations = match storage.get_invocations_for_flow(parent_id).await {
        Ok(invs) => invs,
        Err(e) => {
            error!(
                "Failed to get invocations for parent flow {}: {}",
                parent_id, e
            );
            return;
        }
    };

    let Some(waiting_step) = invocations.iter().find(|inv| {
        inv.status() == crate::core::InvocationStatus::WaitingForSignal
            && inv.timer_name() == Some(signal_token.as_str())
    }) else {
        debug!(
            "No waiting step found for parent flow {} with token {}",
            parent_id, signal_token
        );
        return;
    };

    // CRITICAL: Store signal params - must succeed for parent to resume
    if let Err(e) = storage
        .store_signal_params(
            parent_id,
            waiting_step.step(),
            waiting_step.timer_name().unwrap_or(""),
            &signal_bytes,
        )
        .await
    {
        error!(
            "CRITICAL: Failed to store signal params for parent flow {}: {}",
            parent_id, e
        );
        return;
    }

    match storage.resume_flow(parent_id).await {
        Ok(true) => {
            let status = if success { "success" } else { "error" };
            debug!(
                "Auto-signaled parent flow {} with token {} ({}{})",
                parent_id,
                signal_token,
                status,
                if let Some(msg) = error_msg {
                    format!(": {}", msg)
                } else {
                    String::new()
                }
            );
        }
        Ok(false) => {
            // Parent not in SUSPENDED state - expected during race conditions
            debug!(
                "Parent flow {} not in SUSPENDED state (will resume when it suspends)",
                parent_id
            );
        }
        Err(e) => {
            warn!("Failed to resume parent flow {}: {}", parent_id, e);
        }
    }
}

/// Checks if a flow should be retried based on its retry policy.
///
/// Returns:
/// - `Ok(Some(delay))` if the flow should be retried with the given delay
/// - `Ok(None)` if the flow should not be retried (non-retryable error or max attempts reached)
/// - `Err(msg)` if there was an error checking the retry policy
pub(super) async fn check_should_retry<S: ExecutionLog>(
    storage: &Arc<S>,
    flow: &ScheduledFlow,
) -> Result<Option<Duration>, String> {
    // First check if any step has a non-retryable error
    match storage.has_non_retryable_error(flow.flow_id).await {
        Ok(true) => {
            info!(
                "Flow {} has a non-retryable error - will not retry",
                flow.flow_id
            );
            return Ok(None);
        }
        Ok(false) => {
            debug!(
                "Flow {} has no non-retryable errors (or flag not found) - continuing to retry policy check",
                flow.flow_id
            );
            // Continue to check retry policy
        }
        Err(e) => {
            warn!(
                "Failed to check for non-retryable errors for flow {}: {}",
                flow.flow_id, e
            );
            // Continue anyway
        }
    }

    // Get the flow's invocation (step 0) to check retry policy
    match storage.get_invocation(flow.flow_id, 0).await {
        Ok(Some(invocation)) => {
            // Get retry policy (explicit or default)
            let policy = invocation.retry_policy().unwrap_or_else(|| {
                // No explicit policy, but error is retryable (we passed has_non_retryable_error check)
                // Use a default policy for retryable errors
                debug!(
                    "Flow {} has no explicit retry policy but error is retryable, using default: RetryPolicy::STANDARD (3 attempts)",
                    flow.flow_id
                );
                crate::core::RetryPolicy::STANDARD
            });

            let next_attempt = flow.retry_count + 1;

            if let Some(delay) = policy.delay_for_attempt(next_attempt) {
                debug!(
                    "Flow {} will retry (attempt {}/{}) after {:?}",
                    flow.flow_id, next_attempt, policy.max_attempts, delay
                );
                Ok(Some(delay))
            } else {
                debug!(
                    "Flow {} exceeded max retry attempts ({}/{})",
                    flow.flow_id, next_attempt, policy.max_attempts
                );
                Ok(None)
            }
        }
        Ok(None) => {
            warn!("No invocation found for flow {}", flow.flow_id);
            Ok(None)
        }
        Err(e) => Err(format!("Failed to get invocation: {}", e)),
    }
}

/// Handles a suspended flow by marking it as suspended and checking for pending signals.
pub(super) async fn handle_suspended_flow<S: ExecutionLog>(
    storage: &Arc<S>,
    worker_id: &str,
    flow_task_id: Uuid,
    flow_id: Uuid,
    _reason: crate::executor::SuspendReason,
) {
    info!(
        "Worker {} flow suspended: task_id={}, reason={:?}",
        worker_id, flow_task_id, _reason
    );

    // Mark flow as SUSPENDED so resume_flow() can re-enqueue it
    if let Err(e) = storage
        .complete_flow(flow_task_id, TaskStatus::Suspended)
        .await
    {
        error!("Worker {} failed to mark flow suspended: {}", worker_id, e);
    }

    // Check if signal or timer result arrived while we were still RUNNING
    // This handles a race condition with multiple workers:
    // - Worker A: Parent suspends (RUNNING)
    // - Worker B: Child completes/timer fires, calls resume_flow() → returns false (parent still RUNNING)
    // - Worker A: Marks parent SUSPENDED
    // - Need to check for pending signals/timers and resume immediately
    if let Ok(invocations) = storage.get_invocations_for_flow(flow_id).await {
        for inv in invocations.iter() {
            let should_resume = if inv.status() == crate::core::InvocationStatus::WaitingForSignal {
                // Check if signal arrived
                storage
                    .get_suspension_result(flow_id, inv.step(), inv.timer_name().unwrap_or(""))
                    .await
                    .ok()
                    .flatten()
                    .is_some()
            } else if inv.status() == crate::core::InvocationStatus::WaitingForTimer {
                // Check if timer fired and result is cached
                storage
                    .get_suspension_result(flow_id, inv.step(), inv.timer_name().unwrap_or(""))
                    .await
                    .ok()
                    .flatten()
                    .is_some()
            } else {
                false
            };

            if should_resume {
                debug!(
                    "Found pending suspension result for suspended flow {} step {}, resuming immediately",
                    flow_id,
                    inv.step()
                );
                match storage.resume_flow(flow_id).await {
                    Ok(true) => {
                        debug!("Resumed flow {} with pending suspension result", flow_id);
                    }
                    Ok(false) => {
                        debug!("Flow {} already resumed by another worker", flow_id)
                    }
                    Err(e) => {
                        warn!(
                            "Failed to resume flow {} with pending suspension result: {}",
                            flow_id, e
                        );
                    }
                }
                break;
            }
        }
    }

    // Note: Task remains in database with all its step history intact.
    // When timer fires or signal arrives, resume_flow() will re-enqueue it,
    // and the flow will resume from where it left off (using cached step results).
}

/// Handles a successfully completed flow.
pub(super) async fn handle_flow_completion<S: ExecutionLog>(
    storage: &Arc<S>,
    worker_id: &str,
    flow_task_id: Uuid,
    flow_id: Uuid,
    parent_metadata: Option<(Uuid, String)>,
) {
    info!(
        "Worker {} completed flow: task_id={}",
        worker_id, flow_task_id
    );

    // Complete child flow (Level 3 API) - signals parent after successful completion
    complete_child_flow(storage, flow_id, parent_metadata, true, None).await;

    if let Err(e) = storage
        .complete_flow(flow_task_id, TaskStatus::Complete)
        .await
    {
        error!("Worker {} failed to mark flow complete: {}", worker_id, e);
    }
}

/// Handles a failed flow, checking retry policy and signaling parent if needed.
pub(super) async fn handle_flow_error<S: ExecutionLog>(
    storage: &Arc<S>,
    worker_id: &str,
    flow: &ScheduledFlow,
    flow_task_id: Uuid,
    error: ExecutionError,
    parent_metadata: Option<(Uuid, String)>,
) {
    let error_msg = error.to_string();

    error!(
        "Worker {} flow failed: task_id={}, error={}",
        worker_id, flow_task_id, error_msg
    );

    // Mark non-retryable errors in storage using is_retryable() trait method
    use crate::executor::Retryable;
    if !error.is_retryable() {
        if let Err(e) = storage.update_is_retryable(flow.flow_id, 0, false).await {
            warn!(
                "Worker {} failed to mark error as non-retryable: {}",
                worker_id, e
            );
        }
    }

    // Check retry policy
    match check_should_retry(storage, flow).await {
        Ok(Some(delay)) => {
            // Should retry
            info!(
                "Worker {} retrying flow: task_id={}, attempt={}, delay={:?}",
                worker_id,
                flow_task_id,
                flow.retry_count + 1,
                delay
            );
            if let Err(e) = storage
                .retry_flow(flow_task_id, error_msg.clone(), delay)
                .await
            {
                error!("Worker {} failed to schedule retry: {}", worker_id, e);
            }
        }
        Ok(None) => {
            // Should not retry - signal parent and mark as failed
            debug!(
                "Worker {} not retrying flow: task_id={} (not retryable or max attempts reached)",
                worker_id, flow_task_id
            );

            // Complete child flow (Level 3 API) - signals parent after retries exhausted
            complete_child_flow(
                storage,
                flow.flow_id,
                parent_metadata,
                false,
                Some(&error_msg),
            )
            .await;

            if let Err(e) = storage
                .complete_flow(flow_task_id, TaskStatus::Failed)
                .await
            {
                error!("Worker {} failed to mark flow failed: {}", worker_id, e);
            }
        }
        Err(e) => {
            // Error checking retry policy - mark as failed
            warn!(
                "Worker {} failed to check retry policy: {}, marking as failed",
                worker_id, e
            );

            // Complete child flow (Level 3 API) - failed to check retry policy
            complete_child_flow(
                storage,
                flow.flow_id,
                parent_metadata,
                false,
                Some(&error_msg),
            )
            .await;

            if let Err(e) = storage
                .complete_flow(flow_task_id, TaskStatus::Failed)
                .await
            {
                error!("Worker {} failed to mark flow failed: {}", worker_id, e);
            }
        }
    }
}
