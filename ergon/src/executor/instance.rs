//! Flow executor module.
//!
//! This module provides the Executor type for running flow instances with various
//! execution strategies (async, sync, resume, signal).
//!
//! Following Dave Cheney's simplicity principle and the guideline "The name of an
//! identifier includes its package name," we use `Executor` instead of `FlowExecutor`
//! since the `ergon::` namespace already indicates this is flow-related.

use super::context::{ExecutionContext, CALL_TYPE, EXECUTION_CONTEXT};
use super::error::{ExecutionError, FlowOutcome, Result};
use crate::core::{serialize_value, CallType, InvocationStatus};
use crate::storage::ExecutionLog;
use std::future::Future;
use std::sync::Arc;
use uuid::Uuid;

/// Executes flow instances with various execution strategies.
///
/// Executor holds the complete state of a flow execution (ID, flow object, storage)
/// and provides methods for executing flows in different contexts:
///
/// - `execute`: For async flows (primary API)
/// - `execute_sync`: For sync flows with manual runtime management (rare)
/// - `execute_sync_blocking`: For sync flows from async contexts (uses spawn_blocking)
/// - `resume`: For resuming flows waiting for external signals
/// - `signal_resume`: For sending signals to waiting flows
///
/// # Design Rationale
///
/// This type replaces the previous split between `FlowInstance` (state holder) and
/// `FlowExecutor` (execution methods). Following Dave Cheney's simplicity principle,
/// we merged them into a single type since there was no benefit to the indirection.
///
/// The generic parameter `S` allows for monomorphization over concrete storage
/// types, enabling compiler optimizations while avoiding vtable dispatch.
///
/// # Example
///
/// ```ignore
/// use ergon::prelude::*;
///
/// let executor = Executor::new(flow_id, flow, Arc::new(storage));
/// let result = executor.execute(|f| Box::pin(f.run())).await?;
/// ```
pub struct Executor<T, S: ExecutionLog> {
    /// The unique identifier for this flow execution.
    pub id: Uuid,
    /// The flow object containing the business logic.
    pub flow: T,
    /// The storage backend for persisting execution state.
    pub storage: Arc<S>,
}

impl<T, S: ExecutionLog + 'static> Executor<T, S> {
    /// Creates a new Executor for executing a flow instance.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let executor = Executor::new(flow_id, my_flow, storage);
    /// ```
    pub fn new(id: Uuid, flow: T, storage: Arc<S>) -> Self {
        Self { id, flow, storage }
    }

    /// Executes an async flow function that returns a value.
    ///
    /// This is the primary method for executing flows. Most flows should be async
    /// and use this method.
    ///
    /// # Return Value
    ///
    /// Returns `FlowOutcome<R>` which makes suspension explicit:
    /// - `FlowOutcome::Completed(result)` - Flow ran to completion (success or failure)
    /// - `FlowOutcome::Suspended(reason)` - Flow suspended, waiting for timer/signal
    ///
    /// Following Dave Cheney's principle: "If your function can suspend, you must tell the caller."
    ///
    /// # Example
    ///
    /// ```ignore
    /// use ergon::FlowOutcome;
    ///
    /// match executor.execute(|f| Box::pin(f.process_order())).await {
    ///     FlowOutcome::Completed(Ok(result)) => println!("Done: {:?}", result),
    ///     FlowOutcome::Completed(Err(e)) => println!("Failed: {:?}", e),
    ///     FlowOutcome::Suspended(reason) => println!("Waiting for {:?}", reason),
    /// }
    /// ```
    pub async fn execute<F, R>(&self, f: F) -> FlowOutcome<R>
    where
        F: FnOnce(&T) -> std::pin::Pin<Box<dyn std::future::Future<Output = R> + Send + '_>>,
        R: Send + 'static,
    {
        use std::future::poll_fn;
        use std::pin::pin;
        use std::task::Poll;

        let context = Arc::new(ExecutionContext::new(self.id, self.storage.clone()));

        // Create the flow future with execution context
        let flow_future = EXECUTION_CONTEXT.scope(
            Arc::clone(&context),
            CALL_TYPE.scope(CallType::Run, async { f(&self.flow).await }),
        );

        // Pin the future for polling
        let mut fut = pin!(flow_future);

        // Manually poll the future to detect suspension instantly
        // No timeouts - works for any duration of legitimate work
        poll_fn(|cx| {
            match fut.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    // Flow completed - check if it set suspend_reason just before completing
                    // (shouldn't happen, but handle it defensively)
                    match context.take_suspend_reason() {
                        Some(reason) => {
                            Poll::Ready(FlowOutcome::Suspended(reason))
                        }
                        None => {
                            Poll::Ready(FlowOutcome::Completed(result))
                        }
                    }
                }
                Poll::Pending => {
                    // Flow yielded - check if it's suspension or legitimate async work
                    if let Some(reason) = context.take_suspend_reason() {
                        // Suspension detected instantly! (no timeout delay)
                        Poll::Ready(FlowOutcome::Suspended(reason))
                    } else {
                        // Legitimate async work (I/O, spawned tasks, etc.) - keep waiting
                        Poll::Pending
                    }
                }
            }
        })
        .await
    }

    /// Executes a synchronous flow function that returns a value.
    ///
    /// This is a rare case - prefer async flows with `execute()` when possible.
    ///
    /// # Requirements
    ///
    /// This method requires an active tokio runtime context established via `Runtime::enter()`.
    ///
    /// # Important
    ///
    /// **Cannot be used from `#[tokio::main]` or async contexts** due to nested block_on restrictions.
    /// Use `execute_sync_blocking()` instead for those cases.
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime context. If you need error handling instead,
    /// check `tokio::runtime::Handle::try_current()` before calling this method.
    ///
    /// # Example
    ///
    /// ```ignore
    /// fn main() {
    ///     let rt = tokio::runtime::Runtime::new().unwrap();
    ///     let _guard = rt.enter();
    ///     let result = executor.execute_sync(|f| f.calculate());
    /// }
    /// ```
    pub fn execute_sync<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
        R: Send + 'static,
    {
        let context = Arc::new(ExecutionContext::new(self.id, self.storage.clone()));
        let handle = tokio::runtime::Handle::try_current().expect(
            "Sync flows require an active tokio runtime created with Runtime::new() + enter(). \
             Cannot use #[tokio::main] due to nested block_on restriction.",
        );

        handle.block_on(async {
            EXECUTION_CONTEXT
                .scope(
                    Arc::clone(&context),
                    CALL_TYPE.scope(CallType::Run, async { f(&self.flow) }),
                )
                .await
        })
    }

    /// Resumes a flow that was waiting for an external signal.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No invocation is found for this flow
    /// - The latest step is not in WaitingForSignal status
    /// - The class/method names don't match (incompatible flow structure)
    ///
    /// # Example
    ///
    /// ```ignore
    /// executor.resume("MyFlow", "wait_for_approval", |f| Box::pin(f.continue_after_approval())).await?;
    /// ```
    pub async fn resume<F>(&self, class_name: &str, method_name: &str, f: F) -> Result<()>
    where
        F: FnOnce(&T) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>,
    {
        let latest = self
            .storage
            .get_latest_invocation(self.id)
            .await
            .map_err(ExecutionError::from)?;

        if let Some(inv) = latest {
            if inv.status() != InvocationStatus::WaitingForSignal {
                return Err(ExecutionError::Failed(
                    "No waiting step to resume".to_string(),
                ));
            }

            if inv.class_name() != class_name || inv.method_name() != method_name {
                return Err(ExecutionError::Incompatible(
                    "Incompatible change of flow structure".to_string(),
                ));
            }
        } else {
            return Err(ExecutionError::Failed("No invocation found".to_string()));
        }

        let context = Arc::new(ExecutionContext::new(self.id, self.storage.clone()));
        EXECUTION_CONTEXT
            .scope(
                Arc::clone(&context),
                CALL_TYPE.scope(CallType::Resume, async { f(&self.flow).await }),
            )
            .await;

        Ok(())
    }

    /// Sends a signal to resume a waiting flow with the given parameters.
    ///
    /// This method persists the resume parameters to storage and re-enqueues the flow.
    /// Parameters are stored in persistent storage for crash recovery.
    ///
    /// # Example
    ///
    /// ```ignore
    /// executor.signal_resume(&approval_data).await?;
    /// ```
    pub async fn signal_resume<P: serde::Serialize>(&self, params: &P) -> Result<()> {
        let params_bytes = serialize_value(params)?;

        // Get the step number of the waiting invocation
        let latest = self
            .storage
            .get_latest_invocation(self.id)
            .await
            .map_err(ExecutionError::from)?;

        let step = if let Some(inv) = latest {
            if inv.status() != InvocationStatus::WaitingForSignal {
                return Err(ExecutionError::Failed(
                    "No waiting step to resume".to_string(),
                ));
            }
            inv.step()
        } else {
            return Err(ExecutionError::Failed("No invocation found".to_string()));
        };

        // Persist to storage for crash recovery
        self.storage
            .store_signal_params(self.id, step, &params_bytes)
            .await
            .map_err(ExecutionError::from)?;

        // Re-enqueue the flow so a worker can pick it up
        // Returns false if flow isn't suspended yet (race condition) - that's fine
        let _ = self
            .storage
            .resume_flow(self.id)
            .await
            .map_err(ExecutionError::from)?;

        Ok(())
    }
}

impl<T, S> Executor<T, S>
where
    T: Clone + Send + Sync + 'static,
    S: ExecutionLog + 'static,
{
    /// Executes a synchronous flow function from an async context using spawn_blocking.
    ///
    /// This is the recommended method for calling sync flows from `#[tokio::main]` or
    /// other async contexts. It uses `tokio::task::spawn_blocking` internally to avoid
    /// nested block_on panics.
    ///
    /// # Example
    ///
    /// ```ignore
    /// #[tokio::main]
    /// async fn main() {
    ///     let result = executor
    ///         .execute_sync_blocking(|f| f.calculate_sum(&numbers))
    ///         .await?;
    /// }
    /// ```
    pub async fn execute_sync_blocking<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&T) -> R + Send + 'static,
        R: Send + 'static,
    {
        let flow_clone = self.flow.clone();
        let id = self.id;
        let storage = Arc::clone(&self.storage);

        tokio::task::spawn_blocking(move || {
            let executor = Executor::new(id, flow_clone, storage);
            executor.execute_sync(f)
        })
        .await
        .map_err(|e| ExecutionError::TaskPanic(e.to_string()))
    }
}
