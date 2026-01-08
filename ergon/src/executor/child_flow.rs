//! Child flow invocation with token-based signaling.
//!
//! This module implements Level 3 of the parent-child flow API:
//! type-safe, token-based child invocation that eliminates:
//! - Stringly-typed method names
//! - Manual parent_flow_id fields
//! - External scheduling coordination
//! - Hope-based signaling
//!
//! ## Dave Cheney's Principle: Define Errors Out of Existence
//!
//! Instead of trying to infer method names (proc macros, global state, or strings),
//! we use **tokens** as the await point identifier. The token IS the identifier.
//!
//! ## Usage
//!
//! ```ignore
//! #[flow]
//! async fn process_order(self: Arc<Self>) -> Result<OrderResult, ExecutionError> {
//!     // Parent invokes child directly - no external scheduling!
//!     let inventory = self
//!         .invoke(CheckInventory { product_id: self.product_id.clone() })
//!         .result()
//!         .await?;
//!
//!     // Child has NO parent_flow_id field!
//!     // Child has NO parent_info() implementation!
//!     // Just returns Ok(result) - worker handles the rest
//! }
//! ```

use super::context::EXECUTION_CONTEXT;
use super::error::{ExecutionError, Result};
use crate::core::FlowType;
use crate::storage::{ScheduledFlow, TaskStatus};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use uuid::Uuid;

/// Payload structure for suspension mechanisms (signals, timers).
///
/// This is a unified structure used by:
/// - **Signals**: Carry the actual child flow result in `data`
/// - **Timers**: Mark that the delay has passed (empty `data`)
///
/// This flat structure avoids nested enum serialization issues and works across
/// the worker boundary where the worker doesn't know the result type R.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuspensionPayload {
    /// Whether the suspension resolved successfully (true) or failed (false)
    pub success: bool,
    /// The serialized data:
    /// - For signals: the child flow's result or error message
    /// - For timers: empty (timer just marks delay completion)
    pub data: Vec<u8>,
    /// Whether the error is retryable (only meaningful when success=false)
    /// None when success=true
    #[serde(default)]
    pub is_retryable: Option<bool>,
}

/// A pending child flow invocation.
///
/// This is a pure data holder - no side effects until `.result()` is called.
/// All logic (UUID generation, scheduling, signaling) happens in `result()`,
/// where step caching automatically handles idempotency.
pub struct PendingChild<R> {
    /// Serialized child flow data
    child_bytes: Vec<u8>,

    /// Child flow type name
    child_type: String,

    /// Phantom data for result type
    _result: PhantomData<R>,
}

impl<R> PendingChild<R>
where
    R: DeserializeOwned + Serialize,
{
    /// Wait for the child flow to complete and return its result.
    ///
    /// This method does all the work:
    /// 1. Generates deterministic child UUID (based on parent + step)
    /// 2. Schedules the child flow (only on first execution)
    /// 3. Suspends waiting for child completion
    /// 4. Returns the child's result
    ///
    /// Step caching automatically handles idempotency - on replay, the cached
    /// result is returned immediately without re-scheduling the child.
    ///
    /// # Type Safety
    ///
    /// The result type `R` is checked at compile time. If the child returns
    /// a different type than expected, it's a compile error.
    pub async fn result(self) -> Result<R> {
        use chrono::Utc;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Get current context and storage
        let ctx = EXECUTION_CONTEXT
            .try_with(|c| c.clone())
            .expect("result called outside execution context");
        let parent_id = ctx.id;
        let storage = Arc::clone(&ctx.storage);

        // Generate a stable step ID for this child invocation based on:
        // 1. Parent step ID (if within a step)
        // 2. Child type
        // 3. Child data hash
        // This ensures each unique child invocation gets a stable, unique step ID
        // Generate a stable step ID based on child invocation
        // Child invocations ONLY happen at flow level (not inside steps)
        // Hash child_type + child_data for deterministic, unique step IDs
        let step = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.child_type.hash(&mut hasher);
            self.child_bytes.hash(&mut hasher);
            (hasher.finish() & 0x7FFFFFFF) as i32
        };

        // Log invocation start - this creates the invocation in storage
        // Required before await_external_signal can call log_signal
        let _ = ctx
            .log_step_start(crate::executor::LogStepStartParams {
                step,
                class_name: "<child_flow>",
                method_name: &format!("invoke({})", self.child_type),
                status: crate::core::InvocationStatus::Pending,
                params: &(),
                retry_policy: None,
            })
            .await;

        // Generate deterministic child UUID based on parent + child_type + child_data_hash
        // This ensures uniqueness regardless of step counter state (important on replay
        // when await_external_signal returns cached results without allocating steps)
        let mut hasher = DefaultHasher::new();
        self.child_bytes.hash(&mut hasher);
        let child_hash = hasher.finish();

        let mut seed = Vec::new();
        seed.extend_from_slice(self.child_type.as_bytes());
        seed.extend_from_slice(&child_hash.to_le_bytes());
        let child_flow_id = Uuid::new_v5(&parent_id, &seed);
        let signal_name = child_flow_id.to_string();

        // Check if we already completed this (replay scenario)
        let existing_inv = storage.get_invocation(parent_id, step).await.ok().flatten();
        if let Some(inv) = existing_inv {
            if inv.status() == crate::core::InvocationStatus::Complete {
                // Return cached result
                if let Some(bytes) = inv.return_value() {
                    let payload: SuspensionPayload = crate::core::deserialize_value(bytes)?;
                    if payload.success {
                        return Ok(crate::core::deserialize_value(&payload.data)?);
                    } else {
                        // Deserialize error message string
                        // After worker.rs fix, this will be properly formatted like:
                        // "child_flow_custom_errors::CreditCheckError: Credit score 580..."
                        let error_msg: String = crate::core::deserialize_value(&payload.data)?;

                        // Parse type_name and message from formatted string
                        // Format is "type_name: message"
                        let (type_name, message) = if let Some(colon_pos) = error_msg.find(": ") {
                            let type_name = error_msg[..colon_pos].to_string();
                            let message = error_msg[colon_pos + 2..].to_string();
                            (type_name, message)
                        } else {
                            // Fallback if format doesn't match
                            ("unknown".to_string(), error_msg)
                        };

                        return Err(ExecutionError::User {
                            type_name,
                            message,
                            retryable: payload.is_retryable.unwrap_or(true),
                        });
                    }
                }
            }

            // Check if we're already waiting and signal has arrived
            if inv.status() == crate::core::InvocationStatus::WaitingForSignal {
                if let Some(params) = storage
                    .get_suspension_result(parent_id, step, &signal_name)
                    .await?
                {
                    let payload: SuspensionPayload = crate::core::deserialize_value(&params)?;
                    // Complete invocation and clean up
                    storage
                        .log_invocation_completion(parent_id, step, &params)
                        .await?;
                    storage
                        .remove_suspension_result(parent_id, step, &signal_name)
                        .await?;

                    if payload.success {
                        return Ok(crate::core::deserialize_value(&payload.data)?);
                    } else {
                        // Deserialize error message string
                        let error_msg: String = crate::core::deserialize_value(&payload.data)?;

                        // Parse type_name and message from formatted string
                        let (type_name, message) = if let Some(colon_pos) = error_msg.find(": ") {
                            (
                                error_msg[..colon_pos].to_string(),
                                error_msg[colon_pos + 2..].to_string(),
                            )
                        } else {
                            ("unknown".to_string(), error_msg)
                        };

                        return Err(ExecutionError::User {
                            type_name,
                            message,
                            retryable: payload.is_retryable.unwrap_or(true),
                        });
                    }
                }
                // Otherwise continue to suspend below
            }
        }

        // CRITICAL: Set WaitingForSignal status BEFORE scheduling child!
        // This prevents race condition where child completes before parent is waiting
        storage
            .log_signal(parent_id, step, &signal_name)
            .await
            .map_err(ExecutionError::from)?;

        // Check if this child was already scheduled (replay scenario)
        // We check the child flow's existence in storage rather than step completion
        let child_already_scheduled = storage
            .get_scheduled_flow(child_flow_id)
            .await
            .ok()
            .flatten()
            .is_some();

        // Only schedule child on first execution (not replay)
        if !child_already_scheduled {
            // Create ScheduledFlow with parent metadata
            let now = Utc::now();
            let scheduled = ScheduledFlow {
                task_id: child_flow_id,
                flow_id: child_flow_id,
                flow_type: self.child_type,
                flow_data: self.child_bytes,
                status: TaskStatus::Pending,
                locked_by: None,
                created_at: now,
                updated_at: now,
                retry_count: 0,
                error_message: None,
                scheduled_for: None, // Execute immediately
                // Level 3 metadata: parent and token
                parent_flow_id: Some(parent_id),
                signal_token: Some(child_flow_id.to_string()),
                version: None,
            };

            // Enqueue synchronously - must succeed before we wait
            // This ensures child is guaranteed to be enqueued before parent suspends
            storage.enqueue_flow(scheduled).await.map_err(|e| {
                ExecutionError::Failed(format!("Failed to enqueue child flow: {}", e))
            })?;
        }

        // Check if signal already arrived (child might have been VERY fast)
        if let Some(params) = storage
            .get_suspension_result(parent_id, step, &signal_name)
            .await?
        {
            let payload: SuspensionPayload = crate::core::deserialize_value(&params)?;
            // Complete invocation and clean up
            storage
                .log_invocation_completion(parent_id, step, &params)
                .await?;
            storage
                .remove_suspension_result(parent_id, step, &signal_name)
                .await?;

            if payload.success {
                return Ok(crate::core::deserialize_value(&payload.data)?);
            } else {
                // Deserialize error message string
                let error_msg: String = crate::core::deserialize_value(&payload.data)?;

                // Parse type_name and message from formatted string
                let (type_name, message) = if let Some(colon_pos) = error_msg.find(": ") {
                    (
                        error_msg[..colon_pos].to_string(),
                        error_msg[colon_pos + 2..].to_string(),
                    )
                } else {
                    ("unknown".to_string(), error_msg)
                };

                return Err(ExecutionError::User {
                    type_name,
                    message,
                    retryable: payload.is_retryable.unwrap_or(true),
                });
            }
        }

        // Suspend - signal hasn't arrived yet
        let reason = crate::executor::SuspendReason::Signal {
            flow_id: parent_id,
            step,
            signal_name: signal_name.clone(),
        };
        ctx.set_suspend_reason(reason);

        // Suspension is control flow, not an error. Return Poll::Pending.
        // Worker will poll this future, get Poll::Pending, and check ctx.take_suspend_reason()
        // to determine the suspension reason. This makes suspension invisible to user code.
        std::future::pending::<Result<R>>().await
    }
}

/// Extension trait for invoking child flows.
///
/// This is implemented for all `Arc<T>` where `T: FlowType`, allowing
/// any flow to invoke child flows with compile-time type inference.
pub trait InvokeChild {
    /// Invoke a child flow and get a handle to await its result.
    ///
    /// This method:
    /// 1. Generates a unique token
    /// 2. Schedules the child flow with the token
    /// 3. Returns a `PendingChild` handle
    /// 4. Parent can later await the result
    ///
    /// # Type Inference
    ///
    /// The result type is automatically inferred from the child's `Output` type:
    ///
    /// ```ignore
    /// // CheckInventory::Output = InventoryStatus
    /// let inventory = self
    ///     .invoke(CheckInventory { product_id: "PROD-123".to_string() })
    ///     .result()
    ///     .await?;
    /// // ^^^^^^^^^^^ Type inferred as InventoryStatus!
    /// ```
    ///
    /// # No Manual Fields Required
    ///
    /// Unlike Level 2, the child flow does NOT need:
    /// - `parent_flow_id` field
    /// - Any knowledge of its parent
    ///
    /// The invocation establishes the relationship automatically.
    fn invoke<C>(&self, child: C) -> PendingChild<C::Output>
    where
        C: crate::core::InvokableFlow + Serialize + Send + Sync + Clone + 'static;
}

impl<T> InvokeChild for Arc<T>
where
    T: FlowType,
{
    fn invoke<C>(&self, child: C) -> PendingChild<C::Output>
    where
        C: crate::core::InvokableFlow + Serialize + Send + Sync + Clone + 'static,
    {
        // Pure builder - just capture data, no side effects
        // All logic (UUID generation, scheduling) happens in .result()
        let child_bytes =
            crate::core::serialize_value(&child).expect("Failed to serialize child flow");

        PendingChild {
            child_bytes,
            child_type: C::type_id().to_string(),
            _result: PhantomData,
        }
    }
}
