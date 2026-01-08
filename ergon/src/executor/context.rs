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
use uuid::Uuid;

/// Parameters for logging a step start.
///
/// This struct groups related parameters to avoid having too many function arguments.
pub struct LogStepStartParams<'a, P: serde::Serialize> {
    pub step: i32,
    pub class_name: &'a str,
    pub method_name: &'a str,
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
        tracing::debug!(
            "Checking cache for step {} ({}::{})",
            step,
            class_name,
            method_name
        );
        let invocation = self
            .storage
            .get_invocation(self.id, step)
            .await
            .map_err(ExecutionError::from)?;

        if let Some(inv) = invocation {
            tracing::debug!(
                "Found invocation for step {} with status {:?}",
                step,
                inv.status()
            );
            // Validate the invocation matches current execution
            self.validate_invocation(&inv, step, class_name, method_name, current_params)?;

            // Return cached result if step is complete
            if inv.status() == InvocationStatus::Complete {
                if let Some(return_bytes) = inv.return_value() {
                    // Defensive: Check if return_value is empty
                    // This can happen in edge cases with storage inconsistencies
                    if return_bytes.is_empty() {
                        tracing::debug!(
                            "Step {} has empty return_value, treating as cache miss",
                            step
                        );
                        return Ok(None);
                    }
                    // Try to deserialize, handling type mismatches gracefully
                    // If deserialization fails, treat as cache miss rather than propagating error
                    // This provides resilience against storage schema changes
                    match deserialize_value::<R>(return_bytes) {
                        Ok(result) => return Ok(Some(result)),
                        Err(_e) => {
                            tracing::debug!(
                                "Step {} ({}) deserialization failed, treating as cache miss",
                                step,
                                method_name
                            );
                            return Ok(None);
                        }
                    }
                }
            } else if inv.status() == InvocationStatus::WaitingForSignal {
                // Check if signal result is available (child flow completed)
                // This prevents re-executing step bodies when replaying flows with child invocations
                tracing::debug!(
                    "Step {} is WaitingForSignal, checking for suspension result",
                    step
                );
                // Use timer_name as the signal name (it stores the signal token for child flows)
                let signal_name = inv.timer_name().unwrap_or("");
                if let Ok(Some(suspension_result)) = self
                    .storage
                    .get_suspension_result(self.id, step, signal_name)
                    .await
                {
                    tracing::debug!("Found suspension result for step {}, deserializing", step);
                    // Deserialize the suspension payload to get the result
                    use crate::executor::child_flow::SuspensionPayload;
                    if let Ok(payload) = deserialize_value::<SuspensionPayload>(&suspension_result)
                    {
                        if payload.success {
                            // Return the successful result from the child
                            tracing::debug!(
                                "Returning cached result from signal for step {}",
                                step
                            );
                            let result: R = deserialize_value(&payload.data)?;
                            return Ok(Some(result));
                        }
                        // If child failed, don't return cached result - let it fail again
                        tracing::debug!("Signal indicates failure for step {}, not caching", step);
                    } else {
                        tracing::debug!("Failed to deserialize signal payload for step {}", step);
                    }
                } else {
                    tracing::debug!("No suspension result found for step {} yet", step);
                }
            } else if inv.status() == InvocationStatus::WaitingForTimer {
                // Timer suspension handling (similar to signals, but with empty data)
                // Check if timer has fired (SuspensionPayload with empty data)
                // Unlike signals (which carry the actual step result in payload.data),
                // timers only mark that the delay has passed (payload.data is empty)
                // The step needs to execute to produce its actual return value
                tracing::debug!("Step {} is WaitingForTimer, checking if timer fired", step);

                // Use timer_name as the key for the suspension result
                let timer_key = inv.timer_name().unwrap_or("");
                if let Ok(Some(timer_result)) = self
                    .storage
                    .get_suspension_result(self.id, step, timer_key)
                    .await
                {
                    // Timer has fired - verify it's a valid SuspensionPayload
                    use crate::executor::child_flow::SuspensionPayload;
                    if let Ok(payload) = deserialize_value::<SuspensionPayload>(&timer_result) {
                        if payload.success {
                            tracing::debug!(
                                "Timer has fired for step {}, will let step execute to produce result",
                                step
                            );
                            // Timer fired successfully, but don't return cached result
                            // Let the step execute to produce its actual return value
                            // The schedule_timer() call will handle cleanup
                        } else {
                            tracing::debug!("Timer marked as failed for step {}", step);
                        }
                    }
                } else {
                    tracing::debug!("Timer not fired yet for step {}", step);
                }

                // Don't return cached result - let step execute to produce actual value
                // The schedule_timer() call will see the timer has fired and return Ok(())
                // Then the step continues and produces its real return value
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

    /// Gets a copy of the suspension reason without clearing it.
    ///
    /// Returns Some(reason) if suspended, None otherwise.
    /// If the mutex is poisoned, returns None.
    pub fn get_suspend_reason(&self) -> Option<SuspendReason> {
        self.suspend_reason
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryExecutionLog;

    fn create_test_context() -> ExecutionContext {
        let storage = Arc::new(InMemoryExecutionLog::new());
        ExecutionContext::new(Uuid::new_v4(), storage)
    }

    // =========================================================================
    // Step Counter Tests
    // =========================================================================

    #[test]
    fn test_new_context_starts_at_zero() {
        let ctx = create_test_context();
        assert_eq!(ctx.current_step(), 0);
    }

    #[test]
    fn test_next_step_increments() {
        let ctx = create_test_context();
        assert_eq!(ctx.next_step(), 0);
        assert_eq!(ctx.next_step(), 1);
        assert_eq!(ctx.next_step(), 2);
        assert_eq!(ctx.current_step(), 3);
    }

    #[test]
    fn test_current_step_does_not_increment() {
        let ctx = create_test_context();
        ctx.next_step(); // step = 1
        assert_eq!(ctx.current_step(), 1);
        assert_eq!(ctx.current_step(), 1);
        assert_eq!(ctx.current_step(), 1);
    }

    #[test]
    fn test_last_allocated_step_none_initially() {
        let ctx = create_test_context();
        assert_eq!(ctx.last_allocated_step(), None);
    }

    #[test]
    fn test_last_allocated_step_after_allocation() {
        let ctx = create_test_context();
        ctx.next_step(); // Returns 0, counter becomes 1
        assert_eq!(ctx.last_allocated_step(), Some(0));
        ctx.next_step(); // Returns 1, counter becomes 2
        assert_eq!(ctx.last_allocated_step(), Some(1));
        ctx.next_step(); // Returns 2, counter becomes 3
        assert_eq!(ctx.last_allocated_step(), Some(2));
    }

    #[test]
    fn test_step_counter_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let ctx = Arc::new(create_test_context());
        let mut handles = vec![];

        // Spawn 10 threads that each call next_step() 100 times
        for _ in 0..10 {
            let ctx_clone = Arc::clone(&ctx);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    ctx_clone.next_step();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 1000 total steps allocated
        assert_eq!(ctx.current_step(), 1000);
    }

    // =========================================================================
    // Dependency Graph Tests
    // =========================================================================

    #[test]
    fn test_register_step_no_dependencies() {
        let ctx = create_test_context();
        let result = ctx.register_step("step1", &[]);
        assert!(result.is_ok());

        let graph = ctx.dependency_graph();
        assert!(graph.contains_step(&StepId::new("step1")));
    }

    #[test]
    fn test_register_step_with_dependencies() {
        let ctx = create_test_context();
        ctx.register_step("step1", &[]).unwrap();
        ctx.register_step("step2", &["step1"]).unwrap();

        let graph = ctx.dependency_graph();
        assert!(graph.contains_step(&StepId::new("step1")));
        assert!(graph.contains_step(&StepId::new("step2")));
    }

    #[test]
    fn test_register_step_creates_missing_dependencies() {
        let ctx = create_test_context();
        // Register step2 with dependency on step1 (which doesn't exist yet)
        // This should auto-create step1
        ctx.register_step("step2", &["step1"]).unwrap();

        let graph = ctx.dependency_graph();
        assert!(graph.contains_step(&StepId::new("step1")));
        assert!(graph.contains_step(&StepId::new("step2")));
    }

    #[test]
    fn test_register_step_cycle_detection() {
        let ctx = create_test_context();
        ctx.register_step("step1", &[]).unwrap();
        ctx.register_step("step2", &["step1"]).unwrap();

        // Try to create a cycle: step1 -> step2 -> step1
        let result = ctx.register_step("step1", &["step2"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_register_duplicate_step_idempotent() {
        let ctx = create_test_context();
        ctx.register_step("step1", &[]).unwrap();
        // Registering again should work (idempotent)
        let result = ctx.register_step("step1", &[]);
        assert!(result.is_ok());
    }

    // =========================================================================
    // Logging Tests
    // =========================================================================

    #[tokio::test]
    async fn test_log_step_start() {
        let ctx = create_test_context();
        let step = ctx.next_step();

        let params = LogStepStartParams {
            step,
            class_name: "TestFlow",
            method_name: "test_step",
            status: InvocationStatus::Complete,
            params: &42,
            retry_policy: None,
        };

        let result = ctx.log_step_start(params).await;
        assert!(result.is_ok());

        // Verify invocation was logged
        let inv = ctx
            .storage
            .get_invocation(ctx.id, step)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inv.class_name(), "TestFlow");
        assert_eq!(inv.method_name(), "test_step");
    }

    #[tokio::test]
    async fn test_log_step_completion() {
        let ctx = create_test_context();
        let step = ctx.next_step();

        // First log the start
        let start_params = LogStepStartParams {
            step,
            class_name: "TestFlow",
            method_name: "test_step",
            status: InvocationStatus::Complete,
            params: &(),
            retry_policy: None,
        };
        ctx.log_step_start(start_params).await.unwrap();

        // Now log completion
        let result = ctx.log_step_completion(step, &"success").await;
        assert!(result.is_ok());

        // Verify return value was stored
        let inv = ctx
            .storage
            .get_invocation(ctx.id, step)
            .await
            .unwrap()
            .unwrap();
        assert!(inv.return_value().is_some());
    }

    #[tokio::test]
    async fn test_update_step_retryability() {
        let ctx = create_test_context();
        let step = ctx.next_step();

        // First log the start
        let params = LogStepStartParams {
            step,
            class_name: "TestFlow",
            method_name: "test_step",
            status: InvocationStatus::Complete,
            params: &(),
            retry_policy: Some(RetryPolicy::STANDARD),
        };
        ctx.log_step_start(params).await.unwrap();

        // Update retryability
        ctx.update_step_retryability(step, true).await.unwrap();

        // Verify flag was updated
        let inv = ctx
            .storage
            .get_invocation(ctx.id, step)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inv.is_retryable(), Some(true));
    }

    // =========================================================================
    // Cache Retrieval Tests
    // =========================================================================

    #[tokio::test]
    async fn test_get_cached_result_miss() {
        let ctx = create_test_context();
        let step = 0;

        let result: Result<Option<i32>> = ctx
            .get_cached_result(step, "TestFlow", "test_step", &())
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_cached_result_hit() {
        let ctx = create_test_context();
        let step = ctx.next_step();

        // Log step start and completion
        let params = LogStepStartParams {
            step,
            class_name: "TestFlow",
            method_name: "test_step",
            status: InvocationStatus::Complete,
            params: &42,
            retry_policy: None,
        };
        ctx.log_step_start(params).await.unwrap();
        ctx.log_step_completion(step, &"success").await.unwrap();

        // Try to get cached result
        let result: Option<String> = ctx
            .get_cached_result(step, "TestFlow", "test_step", &42)
            .await
            .unwrap();

        assert!(result.is_some());
        assert_eq!(result.unwrap(), "success");
    }

    #[tokio::test]
    async fn test_get_cached_result_class_mismatch() {
        let ctx = create_test_context();
        let step = ctx.next_step();

        // Log step with one class name
        let params = LogStepStartParams {
            step,
            class_name: "FlowA",
            method_name: "test_step",
            status: InvocationStatus::Complete,
            params: &(),
            retry_policy: None,
        };
        ctx.log_step_start(params).await.unwrap();

        // Try to get cached result with different class name
        let result: Result<Option<i32>> =
            ctx.get_cached_result(step, "FlowB", "test_step", &()).await;

        assert!(result.is_err());
        match result {
            Err(ExecutionError::Incompatible(msg)) => {
                assert!(msg.contains("Non-determinism detected"));
                assert!(msg.contains("FlowA.test_step"));
                assert!(msg.contains("FlowB.test_step"));
            }
            _ => panic!("Expected Incompatible error"),
        }
    }

    #[tokio::test]
    async fn test_get_cached_result_method_mismatch() {
        let ctx = create_test_context();
        let step = ctx.next_step();

        // Log step with one method name
        let params = LogStepStartParams {
            step,
            class_name: "TestFlow",
            method_name: "method_a",
            status: InvocationStatus::Complete,
            params: &(),
            retry_policy: None,
        };
        ctx.log_step_start(params).await.unwrap();

        // Try to get cached result with different method name
        let result: Result<Option<i32>> = ctx
            .get_cached_result(step, "TestFlow", "method_b", &())
            .await;

        assert!(result.is_err());
        match result {
            Err(ExecutionError::Incompatible(msg)) => {
                assert!(msg.contains("Non-determinism detected"));
            }
            _ => panic!("Expected Incompatible error"),
        }
    }

    #[tokio::test]
    async fn test_get_cached_result_params_mismatch() {
        let ctx = create_test_context();
        let step = ctx.next_step();

        // Log step with one param value
        let params = LogStepStartParams {
            step,
            class_name: "TestFlow",
            method_name: "test_step",
            status: InvocationStatus::Complete,
            params: &42,
            retry_policy: None,
        };
        ctx.log_step_start(params).await.unwrap();

        // Try to get cached result with different param value
        let result: Result<Option<i32>> = ctx
            .get_cached_result(step, "TestFlow", "test_step", &99)
            .await;

        assert!(result.is_err());
        match result {
            Err(ExecutionError::Incompatible(msg)) => {
                assert!(msg.contains("different parameter values"));
            }
            _ => panic!("Expected Incompatible error"),
        }
    }

    #[tokio::test]
    async fn test_get_cached_result_incomplete_step() {
        let ctx = create_test_context();
        let step = ctx.next_step();

        // Log step start but don't complete it
        let params = LogStepStartParams {
            step,
            class_name: "TestFlow",
            method_name: "test_step",
            status: InvocationStatus::Pending,
            params: &(),
            retry_policy: None,
        };
        ctx.log_step_start(params).await.unwrap();

        // Try to get cached result - should return None (not complete)
        let result: Option<i32> = ctx
            .get_cached_result(step, "TestFlow", "test_step", &())
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_cached_result_empty_return_value() {
        let ctx = create_test_context();
        let step = ctx.next_step();

        // Log step and mark as complete but with empty return value
        let params = LogStepStartParams {
            step,
            class_name: "TestFlow",
            method_name: "test_step",
            status: InvocationStatus::Complete,
            params: &(),
            retry_policy: None,
        };
        ctx.log_step_start(params).await.unwrap();

        // Manually set empty return value using storage API
        ctx.storage
            .log_invocation_completion(ctx.id, step, &[])
            .await
            .unwrap();

        // Should treat empty return value as cache miss
        let result: Option<i32> = ctx
            .get_cached_result(step, "TestFlow", "test_step", &())
            .await
            .unwrap();

        assert!(result.is_none());
    }

    // =========================================================================
    // Suspension Tracking Tests
    // =========================================================================

    #[test]
    fn test_suspend_reason_initially_none() {
        let ctx = create_test_context();
        assert!(!ctx.has_suspend_reason());
        assert!(ctx.get_suspend_reason().is_none());
    }

    #[test]
    fn test_set_and_get_suspend_reason() {
        let ctx = create_test_context();
        let reason = SuspendReason::Timer {
            flow_id: ctx.flow_id(),
            step: 1,
        };

        ctx.set_suspend_reason(reason.clone());

        assert!(ctx.has_suspend_reason());
        let retrieved = ctx.get_suspend_reason();
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_take_suspend_reason_clears() {
        let ctx = create_test_context();
        let reason = SuspendReason::Timer {
            flow_id: ctx.flow_id(),
            step: 1,
        };

        ctx.set_suspend_reason(reason);
        assert!(ctx.has_suspend_reason());

        let taken = ctx.take_suspend_reason();
        assert!(taken.is_some());

        // After taking, should be None
        assert!(!ctx.has_suspend_reason());
        assert!(ctx.get_suspend_reason().is_none());
        assert!(ctx.take_suspend_reason().is_none());
    }

    #[test]
    fn test_get_suspend_reason_does_not_clear() {
        let ctx = create_test_context();
        let reason = SuspendReason::Signal {
            flow_id: ctx.flow_id(),
            step: 2,
            signal_name: "test_signal".to_string(),
        };

        ctx.set_suspend_reason(reason);

        // Get should not clear
        let _retrieved1 = ctx.get_suspend_reason();
        assert!(ctx.has_suspend_reason());
        let _retrieved2 = ctx.get_suspend_reason();
        assert!(ctx.has_suspend_reason());
    }

    // =========================================================================
    // Enclosing Step Tests
    // =========================================================================

    #[test]
    fn test_enclosing_step_initially_none() {
        let ctx = create_test_context();
        assert_eq!(ctx.get_enclosing_step(), None);
    }

    #[test]
    fn test_set_and_get_enclosing_step() {
        let ctx = create_test_context();
        ctx.set_enclosing_step(5);
        assert_eq!(ctx.get_enclosing_step(), Some(5));

        ctx.set_enclosing_step(10);
        assert_eq!(ctx.get_enclosing_step(), Some(10));
    }

    #[test]
    fn test_clear_enclosing_step() {
        let ctx = create_test_context();
        ctx.set_enclosing_step(5);
        assert_eq!(ctx.get_enclosing_step(), Some(5));

        // Set to -1 to clear
        ctx.set_enclosing_step(-1);
        assert_eq!(ctx.get_enclosing_step(), None);
    }

    // =========================================================================
    // Accessor Tests
    // =========================================================================

    #[test]
    fn test_flow_id_accessor() {
        let flow_id = Uuid::new_v4();
        let storage = Arc::new(InMemoryExecutionLog::new());
        let ctx = ExecutionContext::new(flow_id, storage);

        assert_eq!(ctx.flow_id(), flow_id);
    }

    #[test]
    fn test_step_accessor() {
        let ctx = create_test_context();
        ctx.next_step(); // step = 1
        assert_eq!(ctx.step(), 1);
        assert_eq!(ctx.step(), ctx.current_step());
    }

    #[test]
    fn test_storage_accessor() {
        let storage = Arc::new(InMemoryExecutionLog::new());
        let ctx = ExecutionContext::new(Uuid::new_v4(), storage);

        // Verify storage is accessible (we can call methods on it)
        let _id = ctx.flow_id();
        let _storage = ctx.storage();
        // If we got here without panic, storage is accessible
    }
}
