//! Execution context management module.
//!
//! This module hides the complexity of:
//! - ExecutionContext state management (step counters, graph, storage)
//! - FlowContext scoping (task-local context propagation)
//! - CallType scoping for execution modes (Run, Resume, Await)
//!
//! Following Parnas's information hiding principle, this module encapsulates
//! decisions about how execution context is managed and propagated.

use super::error::{format_params_preview, ExecutionError, Result};
use super::signal::{RESUME_PARAMS, WAIT_NOTIFIERS};
use super::timer::TIMER_NOTIFIERS;
use crate::core::{
    deserialize_value, hash_params, serialize_value, CallType, Invocation, InvocationStatus,
    RetryPolicy,
};
use crate::graph::{FlowGraph, GraphResult, StepId};
use crate::storage::{ExecutionLog, InvocationStartParams};
use std::future::Future;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, RwLock};
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
    /// Dependency graph for steps (built at runtime from step registrations).
    /// Uses RwLock for interior mutability since steps register during execution.
    dependency_graph: RwLock<FlowGraph>,
}

impl ExecutionContext {
    /// Creates a new execution context for a flow.
    pub fn new(id: Uuid, storage: Arc<dyn ExecutionLog>) -> Self {
        Self {
            id,
            storage,
            step_counter: AtomicI32::new(0),
            dependency_graph: RwLock::new(FlowGraph::new()),
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
    pub fn dependency_graph(&self) -> std::sync::RwLockReadGuard<'_, FlowGraph> {
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
            .map_err(ExecutionError::Storage)?;
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
            .map_err(ExecutionError::Storage)?;
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
            .map_err(ExecutionError::Storage)?;
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
    pub async fn get_cached_result<R: for<'de> serde::Deserialize<'de>, P: serde::Serialize>(
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
            .map_err(ExecutionError::Storage)?;

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
}

impl Drop for ExecutionContext {
    /// Clean up notifiers when the execution context is dropped.
    ///
    /// This prevents memory leaks when flows are cancelled or aborted
    /// before completion. Without this cleanup, notifiers would remain
    /// in the global maps indefinitely.
    fn drop(&mut self) {
        // Clean up signal-related notifiers
        WAIT_NOTIFIERS.remove(&self.id);
        RESUME_PARAMS.remove(&self.id);

        // Clean up all timer notifiers for this flow
        // TIMER_NOTIFIERS uses (flow_id, step) as key, so we need to remove all entries
        // where flow_id matches self.id
        TIMER_NOTIFIERS.retain(|(flow_id, _step), _notifier| *flow_id != self.id);
    }
}

/// Manages execution context setup and scoping for flow execution.
///
/// FlowContext encapsulates the task-local context propagation mechanism,
/// providing a clean interface for running code within an execution context.
/// This separates the "how" of context management from the "what" of flow execution.
pub struct FlowContext {
    /// The execution context for this flow.
    context: Arc<ExecutionContext>,
}

impl FlowContext {
    /// Creates a new FlowContext for the given flow ID and storage backend.
    pub fn new(id: Uuid, storage: Arc<dyn ExecutionLog>) -> Self {
        Self {
            context: Arc::new(ExecutionContext::new(id, storage)),
        }
    }

    /// Creates a FlowContext from an existing ExecutionContext.
    pub fn from_context(context: Arc<ExecutionContext>) -> Self {
        Self { context }
    }

    /// Returns a reference to the underlying ExecutionContext.
    pub fn execution_context(&self) -> &Arc<ExecutionContext> {
        &self.context
    }

    /// Executes an async closure within this flow context with Run call type.
    ///
    /// This method sets up the task-local EXECUTION_CONTEXT and CALL_TYPE,
    /// executes the provided future, and properly cleans up afterward.
    pub async fn run_scoped<F, Fut, R>(&self, f: F) -> R
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = R>,
    {
        EXECUTION_CONTEXT
            .scope(
                Arc::clone(&self.context),
                CALL_TYPE.scope(CallType::Run, async { f().await }),
            )
            .await
    }

    /// Executes an async closure within this flow context with Resume call type.
    ///
    /// Used when resuming a flow that was waiting for an external signal.
    pub async fn resume_scoped<F, Fut, R>(&self, f: F) -> R
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = R>,
    {
        EXECUTION_CONTEXT
            .scope(
                Arc::clone(&self.context),
                CALL_TYPE.scope(CallType::Resume, async { f().await }),
            )
            .await
    }

    /// Executes a synchronous closure within this flow context.
    ///
    /// This method uses block_on to bridge from sync to async context,
    /// setting up the required task-local variables.
    ///
    /// # Requirements
    /// Requires an active tokio runtime created with Runtime::new() + enter().
    ///
    /// # Panics
    /// Panics if no tokio runtime is available.
    pub fn run_sync_scoped<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let handle = tokio::runtime::Handle::try_current().expect(
            "Sync flows require an active tokio runtime created with Runtime::new() + enter(). \
             Cannot use #[tokio::main] due to nested block_on restriction.",
        );

        handle.block_on(async {
            EXECUTION_CONTEXT
                .scope(
                    Arc::clone(&self.context),
                    CALL_TYPE.scope(CallType::Run, async { f() }),
                )
                .await
        })
    }
}
