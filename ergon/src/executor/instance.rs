//! Flow instance and executor module.
//!
//! This module provides:
//! - FlowInstance: Holds flow state (id, flow object, storage)
//! - FlowExecutor: Execution strategies (async, sync, resume, signal)
//!
//! Following Parnas's information hiding principle, this module encapsulates
//! decisions about how flow instances are created and executed.

use super::context::FlowContext;
use super::error::{ExecutionError, Result};
use super::signal::{RESUME_PARAMS, WAIT_NOTIFIERS};
use crate::core::{serialize_value, InvocationStatus};
use crate::storage::ExecutionLog;
use std::sync::Arc;
use tokio::sync::Notify;
use uuid::Uuid;

/// Flow instance holding only the state of a flow execution.
///
/// FlowInstance follows the separation of concerns principle:
/// - **State**: This struct holds only the flow state (id, flow object, storage)
/// - **Execution**: Use `FlowExecutor` for execution strategies
/// - **Context**: Use `FlowContext` for task-local context management
///
/// The generic parameter `S` allows for monomorphization over concrete storage
/// types, enabling compiler optimizations. This replaces the previous
/// `Arc<dyn ExecutionLog + Send + Sync>` which required virtual dispatch.
///
/// # Example
/// ```ignore
/// use ergon::prelude::*;
///
/// let storage = SqliteExecutionLog::new("my.db").unwrap();
/// let instance = FlowInstance::new(id, flow, Arc::new(storage));
/// let executor = instance.executor();
/// let result = executor.execute(|f| Box::pin(f.run())).await?;
/// ```
pub struct FlowInstance<T, S: ExecutionLog> {
    /// The unique identifier for this flow execution.
    pub id: Uuid,
    /// The flow object containing the business logic.
    pub flow: T,
    /// The storage backend for persisting execution state.
    pub storage: Arc<S>,
}

impl<T, S: ExecutionLog + 'static> FlowInstance<T, S> {
    /// Creates a new FlowInstance with the given state.
    pub fn new(id: Uuid, flow: T, storage: Arc<S>) -> Self {
        Self { id, flow, storage }
    }

    /// Returns a FlowExecutor for this instance.
    ///
    /// The executor provides all execution methods:
    /// - `execute`: For async flows (primary API)
    /// - `execute_sync`: For sync flows with manual runtime management
    /// - `execute_sync_blocking`: For sync flows from async contexts
    /// - `resume`: For resuming flows waiting for signals
    /// - `signal_resume`: For sending signals to waiting flows
    pub fn executor(&self) -> FlowExecutor<'_, T, S> {
        FlowExecutor::new(&self.flow, self.id, Arc::clone(&self.storage))
    }

    /// Execute a flow method with ergonomic syntax.
    ///
    /// This helper method eliminates the boilerplate of boxing and pinning
    /// futures when executing flow methods.
    ///
    /// The closure receives the flow object (cloned from the instance), which
    /// can be passed directly to methods that use `self: Arc<Self>` receiver.
    ///
    /// # Example
    ///
    /// Instead of:
    /// ```ignore
    /// let result = instance
    ///     .executor()
    ///     .execute(|f| Box::pin(Arc::clone(f).process()))
    ///     .await??;
    /// ```
    ///
    /// You can write:
    /// ```ignore
    /// let result = instance.execute(|f| f.process()).await??;
    /// ```
    ///
    /// # Type Parameters
    ///
    /// - `F`: The closure that calls the flow method
    /// - `Fut`: The future returned by the flow method
    /// - `R`: The return type of the flow method
    ///
    /// # Requirements
    ///
    /// - `T` must implement `Clone`
    /// - The future must be `Send + 'static` for async execution
    pub async fn execute<F, Fut, R>(&self, method: F) -> Result<R>
    where
        T: Clone,
        F: FnOnce(T) -> Fut,
        Fut: std::future::Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        self.executor()
            .execute(move |f| {
                // f is &T from executor, clone it and pass to user's method
                Box::pin(method(f.clone()))
            })
            .await
    }

    /// Execute a synchronous flow method without boilerplate
    ///
    /// This is a convenience wrapper around `executor().execute_sync_blocking()` that
    /// eliminates the need to call `executor()` explicitly for synchronous flows.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Instead of:
    /// let result = instance.executor().execute_sync_blocking(|f| f.calculate_sum(&numbers)).await;
    ///
    /// // You can write:
    /// let result = instance.execute_sync(|f| f.calculate_sum(&numbers)).await;
    /// ```
    ///
    /// # Requirements
    ///
    /// - `T` must implement `Clone + Send + Sync`
    /// - The method must be synchronous (not async)
    /// - Uses `spawn_blocking` internally to avoid blocking the async runtime
    pub async fn execute_sync<F, R>(&self, method: F) -> Result<R>
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce(T) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.executor()
            .execute_sync_blocking(move |f| method(f.clone()))
            .await
    }
}

/// Executes flows with various execution strategies.
///
/// FlowExecutor separates the "how" of flow execution from the "what" of flow state.
/// It provides execution methods with clear naming:
///
/// - `execute`: For async flows (primary API)
/// - `execute_sync`: For sync flows with manual runtime management (rare)
/// - `execute_sync_blocking`: For sync flows from async contexts (uses spawn_blocking)
/// - `resume`: For resuming flows waiting for external signals
/// - `signal_resume`: For sending signals to waiting flows
///
/// The generic parameter `S` allows for monomorphization over concrete storage
/// types, enabling compiler optimizations.
pub struct FlowExecutor<'a, T, S: ExecutionLog> {
    /// Reference to the flow state.
    flow: &'a T,
    /// The flow execution ID.
    id: Uuid,
    /// The storage backend.
    storage: Arc<S>,
}

impl<'a, T, S: ExecutionLog + 'static> FlowExecutor<'a, T, S> {
    /// Creates a new FlowExecutor for the given flow state.
    pub fn new(flow: &'a T, id: Uuid, storage: Arc<S>) -> Self {
        Self { flow, id, storage }
    }

    /// Executes an async flow function that returns a value.
    ///
    /// This is the primary method for executing flows. Most flows should be async
    /// and use this method.
    ///
    /// # Example
    /// ```ignore
    /// let result = instance.executor()
    ///     .execute(|f| Box::pin(f.process_order("Alice", 149.99)))
    ///     .await?;
    /// ```
    pub async fn execute<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&T) -> std::pin::Pin<Box<dyn std::future::Future<Output = R> + Send + '_>>,
        R: Send + 'static,
    {
        let ctx = FlowContext::new(self.id, Arc::clone(&self.storage));
        let result = ctx.run_scoped(|| async { f(self.flow).await }).await;
        Ok(result)
    }

    /// Executes a synchronous flow function that returns a value.
    ///
    /// This is a rare case - prefer async flows with `execute()` when possible.
    ///
    /// # Requirements
    /// This method requires an active tokio runtime context established via `Runtime::enter()`.
    ///
    /// # Important
    /// **Cannot be used from `#[tokio::main]` or async contexts** due to nested block_on restrictions.
    /// Use `execute_sync_blocking()` instead for those cases.
    ///
    /// # Example
    /// ```ignore
    /// fn main() {
    ///     let rt = tokio::runtime::Runtime::new().unwrap();
    ///     let _guard = rt.enter();
    ///     let result = instance.executor().execute_sync(|f| f.calculate())?;
    /// }
    /// ```
    pub fn execute_sync<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&T) -> R,
        R: Send + 'static,
    {
        let ctx = FlowContext::new(self.id, Arc::clone(&self.storage));
        let result = ctx.run_sync_scoped(|| f(self.flow));
        Ok(result)
    }

    /// Resumes a flow that was waiting for an external signal.
    ///
    /// # Errors
    /// Returns an error if:
    /// - No invocation is found for this flow
    /// - The latest step is not in WaitingForSignal status
    /// - The class/method names don't match (incompatible flow structure)
    pub async fn resume<F, P>(&self, class_name: &str, method_name: &str, f: F) -> Result<()>
    where
        F: FnOnce(&T) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>,
        P: serde::Serialize,
    {
        let latest = self
            .storage
            .get_latest_invocation(self.id)
            .await
            .map_err(ExecutionError::Storage)?;

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

        let ctx = FlowContext::new(self.id, Arc::clone(&self.storage));
        ctx.resume_scoped(|| async { f(self.flow).await }).await;

        Ok(())
    }

    /// Sends a signal to resume a waiting flow with the given parameters.
    ///
    /// This method stores the resume parameters and notifies any waiting tasks.
    pub fn signal_resume<P: serde::Serialize>(&self, params: &P) -> Result<()> {
        let params_bytes = serialize_value(params)?;

        {
            let mut resume_params = RESUME_PARAMS
                .lock()
                .expect("RESUME_PARAMS Mutex poisoned - unrecoverable state");
            resume_params.insert(self.id, params_bytes);
        }

        let notifier = {
            let mut notifiers = WAIT_NOTIFIERS
                .lock()
                .expect("WAIT_NOTIFIERS Mutex poisoned - unrecoverable state");
            notifiers
                .entry(self.id)
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone()
        };

        notifier.notify_one();

        Ok(())
    }
}

impl<'a, T, S> FlowExecutor<'a, T, S>
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
    /// ```ignore
    /// #[tokio::main]
    /// async fn main() {
    ///     let result = instance.executor()
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
            let executor = FlowExecutor::new(&flow_clone, id, storage);
            executor.execute_sync(f)
        })
        .await
        .map_err(|e| ExecutionError::TaskPanic(e.to_string()))?
    }
}
