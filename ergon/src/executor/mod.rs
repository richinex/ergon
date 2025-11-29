use crate::core::{deserialize_value, hash_params, serialize_value, CallType, InvocationStatus};
use crate::storage::{ExecutionLog, StorageError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Notify;
use tracing::info;
use uuid::Uuid;

#[cfg(test)]
use crate::storage::SqliteExecutionLog;

// Module organization following Parnas's information hiding principles:
// - flow_context: Hides context setup and task-local scoping decisions
// - flow_executor: Hides execution strategy (sync vs async, blocking vs non-blocking)
// - flow_instance: Exposes only state management
// - graph_executor: Hides DAG-based parallel execution via DeferredRegistry/StepHandle
mod flow_context;
mod flow_executor;
mod graph_executor;

pub use flow_context::FlowContext;
pub use flow_executor::FlowExecutor;
pub use graph_executor::{DeferredRegistry, StepHandle};

// Re-export graph types for dependency management
pub use crate::graph::{FlowGraph, GraphError, GraphResult, StepId};

/// Executes multiple independent steps in parallel.
///
/// This macro takes multiple step futures and executes them concurrently using
/// `tokio::join!`. The results are returned as a tuple in the same order as
/// the input futures.
///
/// # Usage
///
/// ```ignore
/// use rust_de::parallel;
///
/// // In a flow method:
/// let (payment_valid, inventory_ok) = parallel!(
///     self.validate_payment(&customer),
///     self.check_inventory()
/// );
/// ```
///
/// # Important: Replay Considerations
///
/// **Current Limitation**: Steps executed in parallel may complete in different
/// orders on different runs. Since the step counter is sequential, this can cause
/// replay to fail with "non-determinism detected" errors.
///
/// **Safe to use when**:
/// - Steps are idempotent (safe to re-execute if replay fails)
/// - The flow will not be recovered mid-parallel-section
/// - Steps have no side effects that depend on execution order
///
/// **Future Enhancement**: A method-name-based step identification system will
/// make parallel execution fully replay-safe.
///
/// # Requirements
///
/// - Only use for steps that are truly independent (no data dependencies)
/// - Steps should be declared with appropriate `depends_on` attributes
/// - Consider the replay implications for your use case
///
/// # Design Note
///
/// This macro is syntactic sugar over `tokio::join!`. It exists to:
/// 1. Make parallel execution explicit and intentional
/// 2. Document that these steps are independent
/// 3. Prepare for future method-name-based step identification
#[macro_export]
macro_rules! parallel {
    ($($future:expr),+ $(,)?) => {
        tokio::join!($($future),+)
    };
}

lazy_static::lazy_static! {
    static ref WAIT_NOTIFIERS: Mutex<HashMap<Uuid, Arc<Notify>>> = Mutex::new(HashMap::new());
    static ref RESUME_PARAMS: Mutex<HashMap<Uuid, Vec<u8>>> = Mutex::new(HashMap::new());
}

/// Execution layer error type for the rust_de durable execution engine.
///
/// This error type wraps storage and core errors while also providing
/// execution-specific error variants for flow management.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ExecutionError {
    /// A storage operation failed.
    #[error("storage error")]
    Storage(#[from] StorageError),

    /// A core serialization or deserialization error occurred.
    #[error("core error")]
    Core(#[from] crate::core::Error),

    /// A graph operation failed (cycle detection, missing dependency, etc.)
    #[error("graph error: {0}")]
    Graph(#[from] GraphError),

    /// Flow execution failed with the given reason.
    #[error("execution failed: {0}")]
    Failed(String),

    /// The flow structure is incompatible with the stored state.
    #[error("flow incompatible: {0}")]
    Incompatible(String),

    /// A background task panicked during execution.
    #[error("task panicked: {0}")]
    TaskPanic(String),
}

pub type Result<T> = std::result::Result<T, ExecutionError>;

/// Formats parameter bytes as a human-readable preview for error messages.
/// Shows both hex representation and attempts to show printable ASCII characters.
fn format_params_preview(bytes: &[u8]) -> String {
    const MAX_BYTES: usize = 48;
    let truncated = bytes.len() > MAX_BYTES;
    let preview_bytes = if truncated {
        &bytes[..MAX_BYTES]
    } else {
        bytes
    };

    // Try to extract any printable strings from the bytes for context
    let printable: String = preview_bytes
        .iter()
        .filter_map(|&b| {
            if b.is_ascii_alphanumeric() || b == b' ' || b == b'_' || b == b'-' {
                Some(b as char)
            } else {
                None
            }
        })
        .collect();

    // Build hex representation
    let hex: String = preview_bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ");

    let suffix = if truncated { "..." } else { "" };

    if printable.len() >= 3 {
        // If we found meaningful printable content, show it
        format!("[{}{}] (contains: \"{}\")", hex, suffix, printable)
    } else {
        format!("[{}{}]", hex, suffix)
    }
}

tokio::task_local! {
    pub static CALL_TYPE: CallType;
}

tokio::task_local! {
    pub static EXECUTION_CONTEXT: Arc<ExecutionContext<Box<dyn ExecutionLog>>>;
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
/// Panics if called outside of an execution context (i.e., not within a flow).
pub async fn await_external_signal<Fut, R>(step_future: StepFuture<Fut>) -> R
where
    Fut: std::future::Future<Output = Option<R>>,
    R: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let ctx = EXECUTION_CONTEXT
        .try_with(|c| c.clone())
        .expect("await_external_signal called outside execution context");

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
            let result = CALL_TYPE
                .scope(CallType::Resume, async { step_future.inner.await })
                .await;
            // In Resume mode, step should return Some(value)
            return result.expect("Step returned None in Resume mode");
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
                    ctx.await_signal().await.unwrap()
                }
                Some(value) => {
                    // Step completed immediately (shouldn't happen in Await mode)
                    value
                }
            }
        })
        .await
}

/// Execution context for a single flow instance.
///
/// This struct holds the state needed during flow execution, including
/// the flow ID, storage backend, and step counter.
///
/// The generic parameter `S` allows for monomorphization over concrete storage
/// types, enabling compiler optimizations like inlining and devirtualization
/// for hot paths.
pub struct ExecutionContext<S: ExecutionLog> {
    /// The unique identifier for this flow execution.
    pub id: Uuid,
    /// The storage backend for persisting invocation logs.
    pub storage: Arc<S>,
    /// Atomic step counter to prevent race conditions.
    /// Using AtomicI32 instead of RwLock<i32> for lock-free increment operations.
    step_counter: AtomicI32,
    /// Dependency graph for steps (built at runtime from step registrations).
    /// Uses RwLock for interior mutability since steps register during execution.
    dependency_graph: RwLock<FlowGraph>,
}

impl<S: ExecutionLog> ExecutionContext<S> {
    /// Creates a new execution context for a flow.
    pub fn new(id: Uuid, storage: Arc<S>) -> Self {
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

    /// Log the start of a step invocation.
    ///
    /// The storage layer computes the params_hash internally from the serialized parameters.
    pub async fn log_step_start<P: serde::Serialize>(
        &self,
        step: i32,
        class_name: &str,
        method_name: &str,
        delay: Option<Duration>,
        status: InvocationStatus,
        params: &P,
    ) -> Result<()> {
        let params_bytes = serialize_value(params)?;
        self.storage
            .log_invocation_start(
                self.id,
                step,
                class_name,
                method_name,
                delay,
                status,
                &params_bytes,
            )
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

    /// Get a cached result for a step if it exists and is complete.
    ///
    /// This method validates that:
    /// 1. The class and method names match the stored invocation
    /// 2. The parameter values hash matches (detects Option<Some> vs Option<None>, etc.)
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

            if inv.status() == InvocationStatus::Complete {
                if let Some(return_bytes) = inv.return_value() {
                    let result: R = deserialize_value(return_bytes)?;
                    return Ok(Some(result));
                }
            }
        }

        Ok(None)
    }

    pub async fn await_signal<R: for<'de> serde::Deserialize<'de>>(&self) -> Result<R> {
        let notifier = {
            let mut notifiers = WAIT_NOTIFIERS
                .lock()
                .expect("WAIT_NOTIFIERS Mutex poisoned - unrecoverable state");
            notifiers
                .entry(self.id)
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone()
        };

        notifier.notified().await;

        let params = {
            let mut resume_params = RESUME_PARAMS
                .lock()
                .expect("RESUME_PARAMS Mutex poisoned - unrecoverable state");
            resume_params
                .remove(&self.id)
                .ok_or_else(|| ExecutionError::Failed("No resume parameters found".to_string()))?
        };

        let result: R = deserialize_value(&params)?;
        Ok(result)
    }
}

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
/// use rust_de::prelude::*;
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

// =============================================================================
// ARC HELPER FOR ERGONOMIC STEP CALLING
// =============================================================================

/// Extension trait for Arc that provides ergonomic step calling within flows
pub trait ArcStepExt<T> {
    /// Call a step method without manually cloning the Arc
    ///
    /// # Example
    ///
    /// Instead of:
    /// ```ignore
    /// let result = Arc::clone(&self).my_step().await?;
    /// ```
    ///
    /// You can write:
    /// ```ignore
    /// let result = self.call(|s| s.my_step()).await?;
    /// ```
    fn call<F, Fut, R>(&self, method: F) -> impl std::future::Future<Output = R> + Send
    where
        F: FnOnce(Arc<T>) -> Fut + Send,
        Fut: std::future::Future<Output = R> + Send;
}

impl<T: Clone + Send + Sync> ArcStepExt<T> for Arc<T> {
    fn call<F, Fut, R>(&self, method: F) -> impl std::future::Future<Output = R> + Send
    where
        F: FnOnce(Arc<T>) -> Fut + Send,
        Fut: std::future::Future<Output = R> + Send,
    {
        async move { method(Arc::clone(self)).await }
    }
}

// =============================================================================
// IDEMPOTENCY KEY ACCESSOR FUNCTIONS
// =============================================================================
//
// DESIGN CHOICE: Why we provide accessor functions rather than user-passed keys
//
// We considered three approaches for idempotency keys:
//
// 1. User passes idempotency key as parameter:
//    #[step]
//    async fn charge_card(&self, amount: f64, idempotency_key: &str) -> PaymentResult
//
//    REJECTED: Dave Cheney's principle "Design APIs that are hard to misuse"
//    applies directly here. Users can easily pass the wrong key, a stale key,
//    or forget to pass it entirely. The parameters being the same type (strings)
//    means the compiler cannot catch these mistakes.
//
// 2. Magic variable injected by macro:
//    #[step]
//    async fn charge_card(&self, amount: f64) -> PaymentResult {
//        let key = __idempotency_key; // Injected by macro
//    }
//
//    REJECTED: Not discoverable. Users would need to know about magic variables
//    from documentation. Violates the principle of least surprise.
//
// 3. Accessor function (CHOSEN):
//    #[step]
//    async fn charge_card(&self, amount: f64) -> PaymentResult {
//        let key = rust_de::idempotency_key();
//        payment_provider.charge(amount, key).await
//    }
//
//    ADVANTAGES:
//    - Explicit: The user sees exactly where the key comes from
//    - Hard to misuse: The key is always correct for the current step
//    - Discoverable: Standard function call, IDE autocomplete works
//    - Optional: Only accessed when actually needed (not every step needs it)
//    - Composable: Can be combined with idempotency_key_parts() for custom formats
//
// The key format is "{flow_id}-{step}" which provides:
// - Uniqueness: Each step in each flow execution has a unique key
// - Determinism: Same flow + same step always produces same key on replay
// - Debuggability: The key can be parsed to identify which flow/step it came from
//
// Reference: Dave Cheney's "Practical Go" - "APIs should be easy to use
// correctly and hard to use incorrectly"
// =============================================================================

/// Returns a deterministic idempotency key for the current step.
///
/// The key format is `{flow_id}-{step}`, which uniquely identifies this
/// specific step execution within this specific flow instance.
///
/// This key is guaranteed to be:
/// - **Unique**: Each step in each flow execution has a distinct key
/// - **Deterministic**: Same flow + same step always produces the same key
/// - **Stable across replays**: The key remains constant when a step is replayed
///
/// # Usage
///
/// Use this key when calling external services that support idempotency
/// (payment processors, email services, etc.) to prevent duplicate operations
/// if a step is retried after a partial failure.
///
/// ```ignore
/// #[step]
/// async fn charge_credit_card(&self, amount: f64) -> PaymentResult {
///     let idempotency_key = rust_de::idempotency_key();
///
///     // Stripe/Braintree/etc will deduplicate based on this key
///     self.payment_provider
///         .charge(amount)
///         .idempotency_key(&idempotency_key)
///         .await
/// }
/// ```
///
/// # Panics
///
/// Panics if called outside of a flow execution context (i.e., not within
/// a `#[step]` or `#[flow]` function).
///
/// # Why This Design?
///
/// We chose an accessor function over user-passed keys because:
/// 1. It's impossible to pass the wrong key (the framework generates it)
/// 2. It's explicit (you see exactly where it comes from)
/// 3. It's optional (only call it when you need idempotency)
///
/// See Dave Cheney's principle: "APIs should be hard to misuse"
pub fn idempotency_key() -> String {
    let (flow_id, step) = idempotency_key_parts();
    format!("{}-{}", flow_id, step)
}

/// Returns the components of the idempotency key: (flow_id, step_number).
///
/// This function is useful when you need to construct a custom idempotency
/// key format, perhaps because an external service has specific requirements.
///
/// # Examples
///
/// ```ignore
/// #[step]
/// async fn send_webhook(&self, event: &Event) -> Result<(), WebhookError> {
///     let (flow_id, step) = rust_de::idempotency_key_parts();
///
///     // Some services want a specific format
///     let custom_key = format!("myapp_{}_{}", flow_id, step);
///
///     self.webhook_client
///         .send(event)
///         .header("Idempotency-Key", custom_key)
///         .await
/// }
/// ```
///
/// # Panics
///
/// Panics if called outside of a flow execution context.
pub fn idempotency_key_parts() -> (Uuid, i32) {
    EXECUTION_CONTEXT
        .try_with(|ctx| {
            // The step counter represents the NEXT step, so current step is counter - 1
            // However, during step execution, the counter has already been incremented,
            // so we need to get the current value minus 1
            let current_step = ctx.step_counter.load(Ordering::SeqCst).saturating_sub(1);
            (ctx.id, current_step)
        })
        .expect(
            "idempotency_key_parts() called outside of flow execution context. \
             This function must be called from within a #[step] or #[flow] function.",
        )
}

/// Main entry point for the Ergon durable execution engine
pub struct Ergon;

impl Ergon {
    /// Create a new flow instance with the given flow object, ID, and storage backend.
    ///
    /// This method is generic over the storage type `S`, allowing for
    /// monomorphization and better performance compared to dynamic dispatch.
    ///
    /// # Example
    /// ```ignore
    /// let storage = Arc::new(SqliteExecutionLog::new("my.db").unwrap());
    /// let instance = Ergon::new_flow(my_flow, uuid, storage);
    /// let result = instance.executor().execute(|f| Box::pin(f.run())).await?;
    /// ```
    pub fn new_flow<T, S: ExecutionLog + 'static>(
        flow: T,
        id: Uuid,
        storage: Arc<S>,
    ) -> FlowInstance<T, S> {
        FlowInstance::new(id, flow, storage)
    }

    /// Recover all incomplete flows from storage
    pub async fn recover_incomplete_flows<S: ExecutionLog>(storage: Arc<S>) -> Result<()> {
        let incomplete = storage
            .get_incomplete_flows()
            .await
            .map_err(ExecutionError::Storage)?;

        info!("Found {} incomplete flows for recovery", incomplete.len());

        for invocation in incomplete {
            info!(
                "Recovering flow {} - {}.{}",
                invocation.id(),
                invocation.class_name(),
                invocation.method_name()
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_flow_instance() {
        struct TestFlow {
            value: i32,
        }

        let storage = Arc::new(SqliteExecutionLog::in_memory().unwrap());
        let flow = TestFlow { value: 42 };
        let id = Uuid::new_v4();

        let instance = FlowInstance::new(id, flow, storage);
        assert_eq!(instance.id, id);
        assert_eq!(instance.flow.value, 42);
    }

    #[tokio::test]
    async fn test_execution_context() {
        let storage = Arc::new(SqliteExecutionLog::in_memory().unwrap());
        let id = Uuid::new_v4();
        let context = ExecutionContext::new(id, storage);

        let step0 = context.next_step();
        let step1 = context.next_step();
        let step2 = context.next_step();

        assert_eq!(step0, 0);
        assert_eq!(step1, 1);
        assert_eq!(step2, 2);
    }

    #[tokio::test]
    async fn test_log_and_retrieve() {
        let storage = Arc::new(SqliteExecutionLog::in_memory().unwrap());
        let id = Uuid::new_v4();
        let context = ExecutionContext::new(id, Arc::clone(&storage));

        let params = vec!["hello".to_string(), "world".to_string()];
        context
            .log_step_start(
                0,
                "TestClass",
                "testMethod",
                None,
                InvocationStatus::Pending,
                &params,
            )
            .await
            .unwrap();

        let return_value = 42i32;
        context.log_step_completion(0, &return_value).await.unwrap();

        // Pass the same params to get_cached_result for hash comparison
        let cached: Option<i32> = context
            .get_cached_result(0, "TestClass", "testMethod", &params)
            .await
            .unwrap();
        assert_eq!(cached, Some(42));
    }

    #[tokio::test]
    async fn test_non_determinism_detection() {
        let storage = Arc::new(SqliteExecutionLog::in_memory().unwrap());
        let id = Uuid::new_v4();
        let context = ExecutionContext::new(id, Arc::clone(&storage));

        // Log a step with one method name
        let params = vec!["test".to_string()];
        context
            .log_step_start(
                0,
                "TestClass",
                "step_a",
                None,
                InvocationStatus::Pending,
                &params,
            )
            .await
            .unwrap();
        context.log_step_completion(0, &"result_a").await.unwrap();

        // Try to retrieve with a different method name - should detect non-determinism
        let result: Result<Option<String>> = context
            .get_cached_result(0, "TestClass", "step_b", &params)
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ExecutionError::Incompatible(_)));
        let err_msg = err.to_string();
        assert!(err_msg.contains("Non-determinism detected"));
        assert!(err_msg.contains("step_a"));
        assert!(err_msg.contains("step_b"));
    }

    #[tokio::test]
    async fn test_params_hash_non_determinism_detection() {
        let storage = Arc::new(SqliteExecutionLog::in_memory().unwrap());
        let id = Uuid::new_v4();
        let context = ExecutionContext::new(id, Arc::clone(&storage));

        // Log a step with Some discount code
        let params_with_discount: (f64, Option<String>) = (100.0, Some("SAVE20".to_string()));
        context
            .log_step_start(
                0,
                "PaymentProcessor",
                "apply_payment",
                None,
                InvocationStatus::Pending,
                &params_with_discount,
            )
            .await
            .unwrap();
        context.log_step_completion(0, &80.0f64).await.unwrap();

        // Try to retrieve with None discount - should detect non-determinism
        let params_without_discount: (f64, Option<String>) = (100.0, None);
        let result: Result<Option<f64>> = context
            .get_cached_result(
                0,
                "PaymentProcessor",
                "apply_payment",
                &params_without_discount,
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ExecutionError::Incompatible(_)));
        let err_msg = err.to_string();
        assert!(err_msg.contains("Non-determinism detected"));
        assert!(err_msg.contains("different parameter values"));
        // Error now shows actual param bytes with hex dump and hash
        assert!(err_msg.contains("Stored params:"));
        assert!(err_msg.contains("Current params:"));
        assert!(err_msg.contains("hash:"));
    }
}
