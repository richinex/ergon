//! Execution layer for the ergon durable execution engine.
//!
//! This module provides the runtime execution infrastructure for flows,
//! including context management, flow instances, and execution strategies.
//!
//! # Module Organization (Following Parnas's Information Hiding)
//!
//! Each submodule hides a design decision:
//!
//! - [`error`]: Error types for execution failures
//! - [`context`]: Execution context management and propagation
//! - [`signal`]: External signal coordination (wait/resume)
//! - [`instance`]: Flow instance lifecycle and execution strategies
//! - [`dag`]: DAG-based parallel execution (independent subsystem)
//!
//! # Entry Points
//!
//! - [`Ergon`]: Main factory for creating flow instances
//! - [`FlowInstance`]: Holds flow state (id, flow object, storage)
//! - [`FlowExecutor`]: Execution strategies (async, sync, resume)
//!
//! # Example
//!
//! ```ignore
//! use ergon::prelude::*;
//!
//! let storage = Arc::new(SqliteExecutionLog::new("flows.db")?);
//! let instance = Ergon::new_flow(my_flow, flow_id, storage);
//! let result = instance.execute(|f| f.run()).await?;
//! ```

mod context;
pub mod dag;
mod error;
mod instance;
mod retry_helper;
mod scheduler;
mod signal;
mod timer;
mod worker;

// Re-export public types
pub use context::{
    ExecutionContext, FlowContext, LogStepStartParams, CALL_TYPE, EXECUTION_CONTEXT,
};
pub use dag::{DagSummary, DeferredRegistry, StepHandle};
pub use error::{ExecutionError, Result};
pub use instance::{FlowExecutor, FlowInstance};
pub use retry_helper::retry_with_policy;
pub use scheduler::FlowScheduler;
pub use signal::{await_external_signal, StepFuture};
pub use timer::{schedule_timer, schedule_timer_named};
pub use worker::{
    FlowRegistry, FlowWorker, WithStructuredTracing, WithTimers, WithoutStructuredTracing,
    WithoutTimers, WorkerHandle,
};

// Re-export from other modules for convenience
use crate::storage::ExecutionLog;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

/// Executes multiple independent steps in parallel.
///
/// This macro provides a clean syntax for parallel step execution
/// using tokio::join! under the hood.
///
/// # Example
///
/// ```ignore
/// let (user, orders, preferences) = parallel!(
///     self.fetch_user(user_id),
///     self.fetch_orders(user_id),
///     self.fetch_preferences(user_id),
/// );
/// ```
#[macro_export]
macro_rules! parallel {
    ($($future:expr),+ $(,)?) => {
        tokio::join!($($future),+)
    };
}

// =============================================================================
// ARC HELPER FOR ERGONOMIC STEP CALLING
// =============================================================================

/// Extension trait for Arc that provides ergonomic step calling within flows
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
pub trait ArcStepExt<T> {
    /// Call a step method without manually cloning the Arc
    fn call<F, Fut, R>(&self, method: F) -> impl std::future::Future<Output = R> + Send
    where
        F: FnOnce(Arc<T>) -> Fut + Send,
        Fut: std::future::Future<Output = R> + Send;
}

impl<T: Clone + Send + Sync> ArcStepExt<T> for Arc<T> {
    async fn call<F, Fut, R>(&self, method: F) -> R
    where
        F: FnOnce(Arc<T>) -> Fut + Send,
        Fut: std::future::Future<Output = R> + Send,
    {
        method(Arc::clone(self)).await
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
//        let key = ergon::idempotency_key();
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
///     let idempotency_key = ergon::idempotency_key();
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
///     let (flow_id, step) = ergon::idempotency_key_parts();
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

// =============================================================================
// ERGON FACTORY
// =============================================================================

/// Main entry point for the Ergon durable execution engine.
///
/// This factory struct provides methods for creating flow instances
/// and recovering incomplete flows from storage.
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
    /// let instance = Ergon::new_flow(my_flow, flow_id, storage);
    /// let result = instance.execute(|f| f.run()).await?;
    /// ```
    pub fn new_flow<T, S: ExecutionLog + 'static>(
        flow: T,
        id: Uuid,
        storage: Arc<S>,
    ) -> FlowInstance<T, S> {
        FlowInstance::new(id, flow, storage)
    }

    /// Recover all incomplete flows from storage.
    ///
    /// This method queries the storage backend for all flows that haven't
    /// completed and logs them for manual recovery or replay.
    ///
    /// # Example
    /// ```ignore
    /// let storage = Arc::new(SqliteExecutionLog::new("flows.db")?);
    /// Ergon::recover_incomplete_flows(storage).await?;
    /// ```
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
