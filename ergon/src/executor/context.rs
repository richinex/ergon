//! Execution context management module.
//!
//! This module provides execution state management for durable flows:
//! - ExecutionContext: Holds flow ID, storage, step counter, and dependency graph
//! - Task-local variables: EXECUTION_CONTEXT and CALL_TYPE for context propagation
//!
//! Following Parnas's information hiding principle, this module encapsulates
//! decisions about how execution context is managed and propagated.

use super::error::{format_params_preview, ExecutionError, Result, SuspendReason};
use crate::core::{
    deserialize_value, hash_params, serialize_value, CallType, Invocation, InvocationStatus,
    RetryPolicy,
};
use crate::graph::{Graph, GraphResult, StepId};
use crate::storage::{ExecutionLog, InvocationStartParams};
use serde::de::DeserializeOwned;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use uuid::Uuid;

/// Parameters for logging a step start.
///
/// This struct groups related parameters to avoid having too many function arguments.
pub struct LogStepStartParams<'a, P: serde::Serialize> {
    pub step: i32,
    pub class_name: &'a str,
    pub method_name: &'a str,
    pub delay: Option<Duration>,
    pub status: InvocationStatus,
    pub params: &'a P,
    pub retry_policy: Option<RetryPolicy>,
}

tokio::task_local! {
    pub static CALL_TYPE: CallType;
}

tokio::task_local! {
    pub static EXECUTION_CONTEXT: Arc<ExecutionContext>;
}

/// Execution context for a single flow instance.
///
/// This struct holds the state needed during flow execution, including
/// the flow ID, storage backend, and step counter.
///
/// Uses Arc<dyn ExecutionLog> for storage to enable task-local context
/// without type erasure overhead. Storage implementations still benefit
/// from monomorphization at their level.
pub struct ExecutionContext {
    /// The unique identifier for this flow execution.
    pub id: Uuid,
    /// The storage backend for persisting invocation logs.
    pub storage: Arc<dyn ExecutionLog>,
    /// Atomic step counter to prevent race conditions.
    /// Using AtomicI32 instead of RwLock<i32> for lock-free increment operations.
    pub(super) step_counter: AtomicI32,
    /// Tracks the enclosing step for #[step] + invoke() coordination.
    /// Set by #[step] macro to tell invoke().result() which parent step is executing.
    /// Uses -1 to indicate "no enclosing step" (e.g., top-level flow execution).
    /// This enables the framework fix: when a step contains invoke().result(),
    /// we can record the step-child mapping for proper replay handling.
    enclosing_step: AtomicI32,
    /// Dependency graph for steps (built at runtime from step registrations).
    /// Uses RwLock for interior mutability since steps register during execution.
    dependency_graph: RwLock<Graph>,
    /// Runtime suspension tracking (in-memory only, not persisted).
    /// Set by schedule_timer()/await_external_signal() before returning error.
    /// This survives error type conversion (e.g., ExecutionError -> String),
    /// allowing the worker to detect suspension regardless of error type.
    /// Uses Mutex for thread-safety (required for Send + Sync).
    suspend_reason: Mutex<Option<SuspendReason>>,
}

impl ExecutionContext {
    /// Creates a new execution context for a flow.
    pub fn new(id: Uuid, storage: Arc<dyn ExecutionLog>) -> Self {
        Self {
            id,
            storage,
            step_counter: AtomicI32::new(0),
            enclosing_step: AtomicI32::new(-1), // -1 means no enclosing step
            dependency_graph: RwLock::new(Graph::new()),
            suspend_reason: Mutex::new(None),
        }
    }

    /// Registers a step with its dependencies in the flow graph.
    ///
    /// This method is called by the macro-generated code to build the
    /// dependency graph at runtime. The graph is then used by the executor
    /// to determine valid execution order and enable parallel execution.
    ///
    /// # Arguments
    ///
    /// * `step_name` - The name of the step (method name)
    /// * `dependencies` - List of step names this step depends on
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if registration succeeds, or an error if:
    /// - A cycle would be created
    /// - A dependency doesn't exist
    pub fn register_step(&self, step_name: &str, dependencies: &[&str]) -> GraphResult<()> {
        let mut graph = self
            .dependency_graph
            .write()
            .expect("Dependency graph RwLock poisoned - unrecoverable state");

        let step_id = StepId::new(step_name);

        // Add step if not already present
        if !graph.contains_step(&step_id) {
            graph.add_step(step_id.clone())?;
        }

        // Add dependencies
        for dep_name in dependencies {
            let dep_id = StepId::new(*dep_name);

            // Ensure dependency step exists (add if not)
            if !graph.contains_step(&dep_id) {
                graph.add_step(dep_id.clone())?;
            }

            // Add the dependency edge
            graph.add_dependency(step_id.clone(), dep_id)?;
        }

        Ok(())
    }

    /// Returns a reference to the dependency graph.
    pub fn dependency_graph(&self) -> std::sync::RwLockReadGuard<'_, Graph> {
        self.dependency_graph
            .read()
            .expect("Dependency graph RwLock poisoned - unrecoverable state")
    }

    /// Returns the current step number without incrementing the counter.
    ///
    /// This method is thread-safe and uses `load` with `SeqCst` ordering.
    pub fn current_step(&self) -> i32 {
        self.step_counter.load(Ordering::SeqCst)
    }

    /// Returns the current step number and atomically increments the counter.
    ///
    /// This method is thread-safe and uses `fetch_add` with `SeqCst` ordering
    /// to ensure that concurrent calls will always receive unique step numbers.
    pub fn next_step(&self) -> i32 {
        self.step_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Returns the most recently allocated step number.
    ///
    /// This is the step number that was returned by the last call to `next_step()`.
    /// Since `next_step()` increments the counter, the current value is one ahead,
    /// so we subtract 1 to get the last allocated step.
    ///
    /// Returns `None` if no steps have been allocated yet (counter is still 0).
    ///
    /// Use this when you need to reference the step that was just registered.
    pub fn last_allocated_step(&self) -> Option<i32> {
        let current = self.current_step();
        if current > 0 {
            Some(current - 1)
        } else {
            None
        }
    }

    /// Log the start of a step invocation.
    ///
    /// The storage layer computes the params_hash internally from the serialized parameters.
    pub async fn log_step_start<P: serde::Serialize>(
        &self,
        params: LogStepStartParams<'_, P>,
    ) -> Result<()> {
        let params_bytes = serialize_value(params.params)?;
        self.storage
            .log_invocation_start(InvocationStartParams {
                id: self.id,
                step: params.step,
                class_name: params.class_name,
                method_name: params.method_name,
                delay: params.delay,
                status: params.status,
                parameters: &params_bytes,
                retry_policy: params.retry_policy,
            })
            .await
            .map_err(ExecutionError::from)?;
        Ok(())
    }

    /// Log the completion of a step invocation.
    pub async fn log_step_completion<R: serde::Serialize>(
        &self,
        step: i32,
        return_value: &R,
    ) -> Result<()> {
        let return_bytes = serialize_value(return_value)?;
        self.storage
            .log_invocation_completion(self.id, step, &return_bytes)
            .await
            .map_err(ExecutionError::from)?;
        Ok(())
    }

    /// Update the is_retryable flag for a step after caching an error.
    ///
    /// This method is called by the step macro after an error is cached to mark
    /// whether the error is retryable or permanent.
    ///
    /// # Arguments
    ///
    /// * `step` - The step number
    /// * `is_retryable` - Whether the cached error is retryable (true = retryable, false = permanent)
    pub async fn update_step_retryability(&self, step: i32, is_retryable: bool) -> Result<()> {
        self.storage
            .update_is_retryable(self.id, step, is_retryable)
            .await
            .map_err(ExecutionError::from)?;
        Ok(())
    }

    /// Validates that an invocation matches the expected class/method and parameters.
    ///
    /// This method checks for non-determinism by validating:
    /// 1. The class and method names match the stored invocation
    /// 2. The parameter values hash matches (detects Option<Some> vs Option<None>, etc.)
    ///
    /// # Errors
    /// Returns `ExecutionError::Incompatible` if:
    /// - Class/method name mismatch (control flow changed)
    /// - Parameter hash mismatch (same method called with different args)
    fn validate_invocation<P: serde::Serialize>(
        &self,
        inv: &Invocation,
        step: i32,
        class_name: &str,
        method_name: &str,
        current_params: &P,
    ) -> Result<()> {
        // Validate that the same method is being executed at this step
        // This detects non-determinism where control flow takes a different path on replay
        if inv.class_name() != class_name || inv.method_name() != method_name {
            return Err(ExecutionError::Incompatible(format!(
                "Non-determinism detected at step {}: expected {}.{}, but stored invocation is {}.{}. \
                 This typically happens when control flow depends on non-deterministic values \
                 (like current time, random numbers, or external state) that weren't captured as steps.",
                step,
                class_name,
                method_name,
                inv.class_name(),
                inv.method_name()
            )));
        }

        // Validate that the parameter values match
        // This detects non-determinism where the same method is called with different arguments
        // (e.g., Option<Some> vs Option<None> based on external state)
        let current_params_bytes = serialize_value(current_params)?;
        let current_params_hash = hash_params(&current_params_bytes);
        if inv.params_hash() != current_params_hash {
            // Format parameter bytes for debugging (show first 64 bytes as hex)
            let stored_params = inv.parameters();
            let stored_preview = format_params_preview(stored_params);
            let current_preview = format_params_preview(&current_params_bytes);

            return Err(ExecutionError::Incompatible(format!(
                "Non-determinism detected at step {}: {}.{} called with different parameter values.\n\
                 Stored params:  {} (hash: 0x{:016x})\n\
                 Current params: {} (hash: 0x{:016x})\n\
                 This typically happens when parameter values depend on non-deterministic state \
                 (like Option<Some> vs Option<None> based on external conditions).",
                step,
                class_name,
                method_name,
                stored_preview,
                inv.params_hash(),
                current_preview,
                current_params_hash
            )));
        }

        Ok(())
    }

    /// Get a cached result for a step if it exists and is complete.
    ///
    /// This method:
    /// 1. Fetches the invocation from storage
    /// 2. Validates it matches the current execution (via `validate_invocation`)
    /// 3. Returns the cached result if the step is complete
    ///
    /// # Errors
    /// Returns `ExecutionError::Incompatible` if the stored invocation has a different
    /// class/method name or parameter hash, indicating non-deterministic control flow.
    pub async fn get_cached_result<R: DeserializeOwned, P: serde::Serialize>(
        &self,
        step: i32,
        class_name: &str,
        method_name: &str,
        current_params: &P,
    ) -> Result<Option<R>> {
        let invocation = self
            .storage
            .get_invocation(self.id, step)
            .await
            .map_err(ExecutionError::from)?;

        if let Some(inv) = invocation {
            // Validate the invocation matches current execution
            self.validate_invocation(&inv, step, class_name, method_name, current_params)?;

            // Return cached result if step is complete
            if inv.status() == InvocationStatus::Complete {
                if let Some(return_bytes) = inv.return_value() {
                    let result: R = deserialize_value(return_bytes)?;
                    return Ok(Some(result));
                }
            }
        }

        Ok(None)
    }

    /// Returns the flow ID for this execution context.
    pub fn flow_id(&self) -> Uuid {
        self.id
    }

    /// Returns the current step number.
    pub fn step(&self) -> i32 {
        self.current_step()
    }

    /// Returns a reference to the storage backend.
    pub fn storage(&self) -> &Arc<dyn ExecutionLog> {
        &self.storage
    }

    /// Sets the suspension reason for this flow execution.
    ///
    /// This is called by `schedule_timer()` and `await_external_signal()` before
    /// returning an error. The suspension reason is stored in the context and
    /// survives error type conversion (e.g., ExecutionError -> String via map_err).
    ///
    /// The worker checks this suspension reason before handling the result,
    /// allowing suspension to work with any error type.
    ///
    /// If the mutex is poisoned (panic while holding lock), this logs an error
    /// but does not panic. The suspension will not be recorded in this case.
    pub fn set_suspend_reason(&self, reason: SuspendReason) {
        match self.suspend_reason.lock() {
            Ok(mut guard) => *guard = Some(reason),
            Err(e) => {
                // Mutex poisoned - log error but don't panic
                // This is a serious issue but we shouldn't cascade failures
                tracing::error!("Failed to set suspend reason: mutex poisoned - {:?}", e);
            }
        }
    }

    /// Sets the enclosing step for #[step] + invoke() coordination.
    ///
    /// Called by the #[step] macro to inform invoke().result() which parent step
    /// is currently executing. This enables proper step-child mapping.
    pub fn set_enclosing_step(&self, step: i32) {
        self.enclosing_step
            .store(step, std::sync::atomic::Ordering::SeqCst);
    }

    /// Gets the current enclosing step, if any.
    ///
    /// Returns Some(step) if a #[step] wrapper is active, or None if executing
    /// at the top level (no enclosing step).
    pub fn get_enclosing_step(&self) -> Option<i32> {
        let step = self
            .enclosing_step
            .load(std::sync::atomic::Ordering::SeqCst);
        if step >= 0 {
            Some(step)
        } else {
            None
        }
    }

    /// Checks if a suspension reason is pending without consuming it.
    ///
    /// This is used by the flow macro to avoid caching "completion" when the flow
    /// is actually suspending. Unlike `take_suspend_reason()`, this method does not
    /// consume the suspend reason.
    ///
    /// # Thread Safety
    ///
    /// Uses a mutex internally, so this is safe to call from any thread.
    /// If the mutex is poisoned (rare), returns `false`.
    pub fn has_suspend_reason(&self) -> bool {
        self.suspend_reason
            .lock()
            .ok()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
    }

    /// Takes and clears the suspension reason.
    ///
    /// This is called by the worker after flow execution to check if the flow
    /// was suspended. Returns Some(reason) if suspended, None otherwise.
    ///
    /// Taking clears the value, ensuring each suspension is handled only once.
    ///
    /// If the mutex is poisoned, returns None (treats as not suspended).
    pub fn take_suspend_reason(&self) -> Option<SuspendReason> {
        self.suspend_reason
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
    }
}

// Note: No Drop implementation needed for ExecutionContext with the new signal API.
// The database-backed signal system doesn't require in-memory cleanup.
