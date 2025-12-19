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
//! - [`Executor`]: Runs flows directly with durable execution
//! - [`Scheduler`]: Enqueues flows for distributed processing
//! - [`Worker`]: Polls queue and executes flows
//!
//! # Example
//!
//! ```ignore
//! use ergon::prelude::*;
//!
//! let storage = Arc::new(SqliteExecutionLog::new("flows.db")?);
//! let executor = Executor::new(flow_id, my_flow, storage);
//! let result = executor.run(|f| f.process()).await?;
//! ```

mod child_flow;
mod context;
pub mod dag;
mod error;
mod execution;
mod instance;
mod scheduler;
mod signal;
mod timer;
mod worker;

// Re-export public types
pub use child_flow::{InvokeChild, PendingChild};
pub use context::{ExecutionContext, LogStepStartParams, CALL_TYPE, EXECUTION_CONTEXT};
pub use dag::{DagSummary, DeferredRegistry, StepHandle};
pub use error::{ExecutionError, FlowOutcome, Result, Retryable, SuspendReason};
pub use instance::Executor;
pub use scheduler::{Configured, Scheduler, Unconfigured};
pub use signal::{
    await_external_signal, signal_parent_flow, SignalProcessing, SignalSource, StepFuture,
    WithSignals, WithoutSignals,
};
pub use timer::{schedule_timer, schedule_timer_named, TimerProcessing, WithTimers, WithoutTimers};
pub use worker::{Registry, WithStructuredTracing, WithoutStructuredTracing, Worker, WorkerHandle};

// Re-export from other modules for convenience
use std::sync::atomic::Ordering;
use std::sync::Arc;
use uuid::Uuid;

// =============================================================================
// ARC HELPER FOR ERGONOMIC STEP CALLING
// =============================================================================

/// Extension trait for Arc that provides ergonomic step calling within flows.
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
    /// Call a step method without manually cloning the Arc.
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
/// # Design Rationale
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
/// # Example
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
            let current_step = ctx.step_counter.load(Ordering::SeqCst).saturating_sub(1);
            (ctx.id, current_step)
        })
        .expect(
            "idempotency_key_parts() called outside of flow execution context. \
             This function must be called from within a #[step] or #[flow] function.",
        )
}
