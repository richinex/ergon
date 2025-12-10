//! Distributed worker for flow execution.
//!
//! Following Dave Cheney's principle "The name of an identifier includes its package name,"
//! we use `Worker` and `Registry` instead of `FlowWorker` and `FlowRegistry` since the
//! `ergon::` namespace already indicates these are flow-related.
//!
//! This module provides workers that poll a storage queue for flows and execute
//! them in a distributed manner.

use crate::core::{deserialize_value, FlowType};
use crate::executor::ExecutionError;
use crate::storage::{ExecutionLog, TaskStatus};
use crate::Executor;
use chrono::Utc;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Instrument};
use uuid::Uuid;

// ============================================================================
// Type Aliases
// ============================================================================

/// A boxed error that can be sent across threads.
///
/// This is the standard error type used throughout async Rust ecosystems
/// (tokio, tower, axum, etc.). Any error implementing `std::error::Error`
/// can be automatically converted to this type.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ============================================================================
// Timer Typestates
// ============================================================================

/// Typestate: Worker without timer processing
pub struct WithoutTimers;

/// Typestate: Worker with timer processing enabled
pub struct WithTimers {
    pub timer_poll_interval: Duration,
}

// ============================================================================
// Signal Typestates
// ============================================================================

/// Typestate: Worker without signal processing
pub struct WithoutSignals;

/// Typestate: Worker with signal processing enabled
pub struct WithSignals<Src> {
    pub signal_source: Arc<Src>,
    pub signal_poll_interval: Duration,
}

// ============================================================================
// Tracing Typestates
// ============================================================================

/// Typestate: Worker without structured tracing (uses basic log-style tracing only).
///
/// This is the default state, providing zero-cost abstraction when detailed
/// observability is not needed. Basic `info!`, `debug!`, `warn!`, and `error!`
/// calls are still used, but no spans are created.
#[derive(Clone, Copy)]
pub struct WithoutStructuredTracing;

/// Typestate: Worker with structured tracing enabled.
///
/// Enables creation of detailed tracing spans with structured fields for
/// comprehensive observability. Spans track temporal context and provide
/// rich debugging information in production.
#[derive(Clone, Copy)]
pub struct WithStructuredTracing;

// ============================================================================
// Timer Processing Trait
// ============================================================================

/// Trait that defines timer processing behavior based on type state.
///
/// This trait uses the typestate pattern to provide different behaviors
/// for workers with and without timer processing enabled.
#[async_trait::async_trait]
pub trait TimerProcessing: Send + Sync {
    async fn process_timers<S: ExecutionLog>(&self, storage: &Arc<S>, worker_id: &str);
}

// ============================================================================
// Signal Processing Trait
// ============================================================================

/// Trait that defines signal processing behavior based on type state.
///
/// This trait uses the typestate pattern to provide different behaviors
/// for workers with and without signal processing enabled.
#[async_trait::async_trait]
pub trait SignalProcessing: Send + Sync {
    async fn process_signals<S: ExecutionLog>(&self, storage: &Arc<S>, worker_id: &str);
}

// ============================================================================
// Tracing Behavior Trait
// ============================================================================

/// Trait that defines tracing behavior based on type state.
///
/// This trait uses the typestate pattern to provide different tracing behaviors:
/// - `WithoutStructuredTracing`: No span creation (zero cost)
/// - `WithStructuredTracing`: Creates detailed spans with structured fields
pub trait TracingBehavior: Send + Sync {
    /// Creates a span for the worker loop iteration.
    ///
    /// Returns `None` for `WithoutStructuredTracing`, a span for `WithStructuredTracing`.
    fn worker_loop_span(&self, worker_id: &str) -> Option<tracing::Span>;

    /// Creates a span for flow execution.
    fn flow_execution_span(
        &self,
        worker_id: &str,
        flow_id: Uuid,
        flow_type: &str,
        task_id: Uuid,
    ) -> Option<tracing::Span>;

    /// Creates a span for timer processing.
    fn timer_processing_span(&self, worker_id: &str) -> Option<tracing::Span>;
}

#[async_trait::async_trait]
impl TimerProcessing for WithoutTimers {
    async fn process_timers<S: ExecutionLog>(&self, _storage: &Arc<S>, _worker_id: &str) {
        // No-op: timer processing disabled
    }
}

#[async_trait::async_trait]
impl TimerProcessing for WithTimers {
    async fn process_timers<S: ExecutionLog>(&self, storage: &Arc<S>, _worker_id: &str) {
        // Fetch expired timers and process them
        match storage.get_expired_timers(Utc::now()).await {
            Ok(timers) => {
                if !timers.is_empty() {
                    debug!("Processing {} expired timers", timers.len());
                }

                for timer in timers {
                    // Try to claim the timer (optimistic concurrency)
                    match storage.claim_timer(timer.flow_id, timer.step).await {
                        Ok(true) => {
                            // Successfully claimed - resume the flow
                            info!(
                                "Timer fired: flow={} step={} name={:?}",
                                timer.flow_id, timer.step, timer.timer_name
                            );

                            // Resume the flow by re-enqueuing it
                            match storage.resume_flow(timer.flow_id).await {
                                Ok(true) => debug!("Resumed flow after timer: {}", timer.flow_id),
                                Ok(false) => debug!(
                                    "Flow {} not in SUSPENDED state after timer (may have already resumed)",
                                    timer.flow_id
                                ),
                                Err(e) => warn!(
                                    "Failed to resume flow after timer: flow={} step={} error={}",
                                    timer.flow_id, timer.step, e
                                ),
                            }
                        }
                        Ok(false) => {
                            // Another worker already claimed it
                            debug!(
                                "Timer already fired by another worker: flow={} step={}",
                                timer.flow_id, timer.step
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to claim timer (will retry): flow={} step={} error={}",
                                timer.flow_id, timer.step, e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to fetch expired timers: {}", e);
            }
        }
    }
}

// ============================================================================
// Signal Processing Implementations
// ============================================================================

#[async_trait::async_trait]
impl SignalProcessing for WithoutSignals {
    async fn process_signals<S: ExecutionLog>(&self, _storage: &Arc<S>, _worker_id: &str) {
        // No-op: signal processing disabled
    }
}

#[async_trait::async_trait]
impl<Src> SignalProcessing for WithSignals<Src>
where
    Src: crate::executor::SignalSource + 'static,
{
    async fn process_signals<S: ExecutionLog>(&self, storage: &Arc<S>, _worker_id: &str) {
        // Get all flows waiting for signals
        let waiting_signals = match storage.get_waiting_signals().await {
            Ok(signals) => signals,
            Err(e) => {
                warn!("Failed to fetch waiting signals: {}", e);
                return;
            }
        };

        if !waiting_signals.is_empty() {
            debug!("Processing {} waiting signals", waiting_signals.len());
        }

        for signal_info in waiting_signals {
            let Some(signal_name) = &signal_info.signal_name else {
                continue;
            };

            // Check if signal source has the signal
            if let Some(signal_data) = self.signal_source.poll_for_signal(signal_name).await {
                // Store signal params
                match storage
                    .store_signal_params(signal_info.flow_id, signal_info.step, &signal_data)
                    .await
                {
                    Ok(_) => {
                        // Consume signal so we don't re-process
                        self.signal_source.consume_signal(signal_name).await;

                        // Try to resume flow
                        match storage.resume_flow(signal_info.flow_id).await {
                            Ok(true) => {
                                debug!(
                                    "Signal '{}' delivered to flow {}",
                                    signal_name, signal_info.flow_id
                                );
                            }
                            Ok(false) => {
                                debug!(
                                    "Signal '{}' stored for flow {} (will resume when suspended)",
                                    signal_name, signal_info.flow_id
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to resume flow {} after signal '{}': {}",
                                    signal_info.flow_id, signal_name, e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to store signal params for '{}': {}", signal_name, e);
                    }
                }
            }
        }
    }
}

// ============================================================================
// Tracing Behavior Implementations
// ============================================================================

impl TracingBehavior for WithoutStructuredTracing {
    fn worker_loop_span(&self, _worker_id: &str) -> Option<tracing::Span> {
        // No-op: structured tracing disabled, returns None for zero cost
        None
    }

    fn flow_execution_span(
        &self,
        _worker_id: &str,
        _flow_id: Uuid,
        _flow_type: &str,
        _task_id: Uuid,
    ) -> Option<tracing::Span> {
        // No-op: structured tracing disabled
        None
    }

    fn timer_processing_span(&self, _worker_id: &str) -> Option<tracing::Span> {
        // No-op: structured tracing disabled
        None
    }
}

impl TracingBehavior for WithStructuredTracing {
    fn worker_loop_span(&self, worker_id: &str) -> Option<tracing::Span> {
        // Create a span for each worker loop iteration with worker context
        Some(tracing::debug_span!(
            "worker_loop",
            worker.id = worker_id,
            iteration = tracing::field::Empty, // Can be recorded later
        ))
    }

    fn flow_execution_span(
        &self,
        worker_id: &str,
        flow_id: Uuid,
        flow_type: &str,
        task_id: Uuid,
    ) -> Option<tracing::Span> {
        // Create a detailed span for flow execution with all key identifiers
        Some(tracing::info_span!(
            "flow_execution",
            worker.id = worker_id,
            flow.id = %flow_id,
            flow.type = flow_type,
            task.id = %task_id,
            result = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
        ))
    }

    fn timer_processing_span(&self, worker_id: &str) -> Option<tracing::Span> {
        // Create a span for timer processing operations
        Some(tracing::debug_span!(
            "timer_processing",
            worker.id = worker_id,
            timers.found = tracing::field::Empty,
            timers.claimed = tracing::field::Empty,
        ))
    }
}

/// Type-erased flow executor.
///
/// This trait allows us to store executors for different flow types in a
/// single registry without knowing their concrete types at compile time.
///
/// Returns `FlowOutcome` which makes suspension explicit (per Dave Cheney's principle).
///
/// Wrapped in Arc to allow cloning without holding the registry lock during execution.
///
/// The executor now takes parent metadata (parent_flow_id, signal_token) to support
/// Level 3 child flow signaling.
type BoxedExecutor<S> = Arc<
    dyn Fn(
            Vec<u8>,
            Uuid,
            Arc<S>,
            Option<(Uuid, String)>, // (parent_flow_id, signal_token)
        ) -> Pin<
            Box<
                dyn Future<Output = crate::executor::FlowOutcome<Result<(), ExecutionError>>>
                    + Send,
            >,
        > + Send
        + Sync,
>;

/// Registry that maps flow type names to their executors.
///
/// The `Registry` stores executor functions that can deserialize and execute
/// flows based on their type name. This allows the worker to handle different
/// flow types dynamically.
///
/// # Example
///
/// ```no_run
/// use ergon::executor::Registry;
/// use ergon::storage::InMemoryExecutionLog;
/// use ergon_macros::FlowType;
/// use serde::{Serialize, Deserialize};
/// use std::sync::Arc;
///
/// #[derive(Serialize, Deserialize, Clone, FlowType)]
/// struct MyFlow {
///     data: String,
/// }
///
/// impl MyFlow {
///     async fn run(self: Arc<Self>) -> Result<String, String> {
///         Ok(self.data.clone())
///     }
/// }
///
/// let mut registry: Registry<InMemoryExecutionLog> = Registry::new();
/// registry.register(|flow: Arc<MyFlow>| flow.run());
/// ```
pub struct Registry<S: ExecutionLog> {
    executors: HashMap<String, BoxedExecutor<S>>,
}

impl<S: ExecutionLog + 'static> Registry<S> {
    /// Creates a new empty flow registry.
    pub fn new() -> Self {
        Self {
            executors: HashMap::new(),
        }
    }

    /// Registers a flow type with its executor function.
    ///
    /// The executor function receives an `Arc<T>` of the deserialized flow
    /// and should return a future that produces the flow's result.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The flow type (must be `Serialize + Deserialize + Clone`)
    /// * `F` - The executor function
    /// * `Fut` - The future returned by the executor
    /// * `R` - The result type returned by the flow
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ergon::executor::Registry;
    /// # use ergon::storage::InMemoryExecutionLog;
    /// # use ergon_macros::FlowType;
    /// # use serde::{Serialize, Deserialize};
    /// # use std::sync::Arc;
    /// #
    /// # #[derive(Serialize, Deserialize, Clone, FlowType)]
    /// # struct OrderProcessor { order_id: String }
    /// #
    /// # impl OrderProcessor {
    /// #     async fn process(self: Arc<Self>) -> Result<String, String> {
    /// #         Ok(self.order_id.clone())
    /// #     }
    /// # }
    /// let mut registry: Registry<InMemoryExecutionLog> = Registry::new();
    /// registry.register(|flow: Arc<OrderProcessor>| flow.process());
    /// ```
    pub fn register<T, F, Fut, R, E>(&mut self, executor: F)
    where
        T: DeserializeOwned + FlowType + Send + Sync + Clone + 'static,
        F: Fn(Arc<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        R: serde::Serialize + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        // Use stable type ID instead of std::any::type_name()
        // This ensures compatibility across different compiler versions
        let type_name = T::type_id().to_string();

        let boxed: BoxedExecutor<S> = Arc::new(
            move |data: Vec<u8>,
                  flow_id: Uuid,
                  storage: Arc<S>,
                  _parent_metadata: Option<(Uuid, String)>| {
                // Clone the executor for this invocation
                let executor = executor.clone();

                Box::pin(async move {
                    use crate::executor::FlowOutcome;

                    // Deserialize the flow
                    let flow: T = match deserialize_value(&data) {
                        Ok(f) => f,
                        Err(e) => {
                            return FlowOutcome::Completed(Err(ExecutionError::Failed(format!(
                                "failed to deserialize flow: {}",
                                e
                            ))))
                        }
                    };

                    // Create executor
                    let exec = Executor::new(flow_id, flow, storage);

                    // Execute the flow - returns FlowOutcome
                    // The executor expects Arc<T>, so we clone &T to create owned T, then wrap in Arc
                    let outcome = exec
                        .execute(move |f: &T| Box::pin(executor(Arc::new(f.clone()))))
                        .await;

                    // Map FlowOutcome<Result<R, E>> to FlowOutcome<Result<(), ExecutionError>>
                    match outcome {
                        FlowOutcome::Suspended(reason) => FlowOutcome::Suspended(reason),
                        FlowOutcome::Completed(result) => {
                            // DON'T signal parent here - signaling happens in work loop after retry decision
                            // This ensures parent only gets notified when child truly completes (not during retries)

                            // Convert user's error type E to ExecutionError
                            let converted_result = result.map(|_| ()).map_err(|e| {
                                let boxed: BoxError = e.into();
                                ExecutionError::Failed(boxed.to_string())
                            });
                            FlowOutcome::Completed(converted_result)
                        }
                    }
                })
            },
        );

        debug!("Registered flow type: {}", type_name);
        self.executors.insert(type_name, boxed);
    }

    /// Gets an executor for a flow type.
    ///
    /// Returns None if the flow type is not registered.
    /// The Arc clone is cheap and allows releasing the lock before execution.
    fn get_executor(&self, flow_type: &str) -> Option<BoxedExecutor<S>> {
        self.executors.get(flow_type).cloned()
    }

    /// Returns the number of registered flow types.
    pub fn len(&self) -> usize {
        self.executors.len()
    }

    /// Returns true if no flow types are registered.
    pub fn is_empty(&self) -> bool {
        self.executors.is_empty()
    }
}

impl<S: ExecutionLog + 'static> Default for Registry<S> {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker that executes flows from a distributed queue.
///
/// The `Worker` polls a storage backend's task queue for pending flows,
/// executes them using registered flow executors, and marks them as complete.
///
/// Uses the typestate pattern to optionally enable:
/// - Timer processing (`WithTimers`)
/// - Structured tracing with spans (`WithStructuredTracing`)
///
/// Both features default to disabled for zero-cost abstraction.
///
/// # Example
///
/// ```no_run
/// use ergon::executor::{Worker, Registry};
/// use ergon::storage::SqliteExecutionLog;
/// use ergon_macros::FlowType;
/// use serde::{Serialize, Deserialize};
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// #[derive(Serialize, Deserialize, Clone, FlowType)]
/// struct MyFlow {
///     data: String,
/// }
///
/// impl MyFlow {
///     async fn run(self: Arc<Self>) -> Result<String, String> {
///         Ok(self.data.clone())
///     }
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
///
/// // Basic worker (no timers, no structured tracing)
/// let worker = Worker::new(storage.clone(), "worker-1");
///
/// // Worker with timers and structured tracing
/// let worker_advanced = Worker::new(storage.clone(), "worker-2")
///     .with_timers()
///     .with_timer_interval(Duration::from_millis(100))
///     .with_structured_tracing();
///
/// worker.register(|flow: Arc<MyFlow>| flow.run()).await;
///
/// let handle = worker.start().await;
/// // Worker is now running in the background...
///
/// // Later, to stop:
/// handle.shutdown().await;
/// # Ok(())
/// # }
/// ```
pub struct Worker<
    S: ExecutionLog + 'static,
    T = WithoutTimers,
    Sig = WithoutSignals,
    Tr = WithoutStructuredTracing,
> {
    storage: Arc<S>,
    worker_id: String,
    registry: Arc<RwLock<Registry<S>>>,
    poll_interval: Duration,
    timer_state: T,
    signal_state: Sig,
    tracing_state: Tr,
    /// Optional semaphore for backpressure control (limits concurrent flow execution)
    max_concurrent_flows: Option<Arc<Semaphore>>,
    /// Notification handle for event-driven worker wakeup
    work_notify: Arc<tokio::sync::Notify>,
}

impl<S: ExecutionLog + 'static, Sig, Tr> Worker<S, WithoutTimers, Sig, Tr> {
    /// Enables timer processing for this worker.
    ///
    /// Returns a worker in the `WithTimers` state, which allows configuring
    /// timer-specific options like `with_timer_interval()`.
    ///
    /// When enabled, the worker will process both scheduled flows AND
    /// expired timers, providing distributed timer coordination.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let worker = Worker::new(storage, "worker-1")
    ///     .with_timers()
    ///     .with_timer_interval(Duration::from_millis(100));
    /// ```
    pub fn with_timers(self) -> Worker<S, WithTimers, Sig, Tr> {
        Worker {
            storage: self.storage,
            worker_id: self.worker_id,
            registry: self.registry,
            poll_interval: self.poll_interval,
            timer_state: WithTimers {
                timer_poll_interval: Duration::from_secs(1),
            },
            signal_state: self.signal_state,
            tracing_state: self.tracing_state,
            max_concurrent_flows: self.max_concurrent_flows,
            work_notify: self.work_notify,
        }
    }
}

impl<S: ExecutionLog + 'static> Worker<S, WithoutTimers, WithoutSignals, WithoutStructuredTracing> {
    /// Creates a new flow worker without timer processing, signal processing, or structured tracing.
    ///
    /// This is the default state providing zero-cost abstraction.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage backend that implements the queue operations
    /// * `worker_id` - Unique identifier for this worker
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ergon::executor::Worker;
    /// # use ergon::storage::SqliteExecutionLog;
    /// # use std::sync::Arc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
    /// let worker = Worker::new(storage, "worker-1");
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(storage: Arc<S>, worker_id: impl Into<String>) -> Self {
        // Get the work notify from storage, or create a new one as fallback
        let work_notify = storage
            .work_notify()
            .cloned()
            .unwrap_or_else(|| Arc::new(tokio::sync::Notify::new()));

        Self {
            storage,
            worker_id: worker_id.into(),
            registry: Arc::new(RwLock::new(Registry::new())),
            poll_interval: Duration::from_secs(1),
            timer_state: WithoutTimers,
            signal_state: WithoutSignals,
            tracing_state: WithoutStructuredTracing,
            max_concurrent_flows: None,
            work_notify,
        }
    }
}

impl<S: ExecutionLog + 'static, Sig, Tr> Worker<S, WithTimers, Sig, Tr> {
    /// Sets the interval for checking expired timers.
    ///
    /// Only available when timer processing is enabled.
    /// Default is 1 second.
    ///
    /// Lower intervals provide better timer precision but higher CPU usage.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let worker = Worker::new(storage, "worker-1")
    ///     .with_timers()
    ///     .with_timer_interval(Duration::from_millis(100));
    /// ```
    pub fn with_timer_interval(mut self, interval: Duration) -> Self {
        self.timer_state.timer_poll_interval = interval;
        self
    }
}

// Methods for enabling signals
impl<S: ExecutionLog + 'static, T, Tr> Worker<S, T, WithoutSignals, Tr> {
    /// Enables signal processing for this worker.
    ///
    /// Returns a worker in the `WithSignals<Src>` state, which will automatically
    /// process external signals from the provided source.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let signal_source = Arc::new(MySignalSource::new());
    /// let worker = Worker::new(storage, "worker-1")
    ///     .with_signals(signal_source);
    /// ```
    pub fn with_signals<Src>(self, source: Arc<Src>) -> Worker<S, T, WithSignals<Src>, Tr>
    where
        Src: crate::executor::SignalSource + 'static,
    {
        Worker {
            storage: self.storage,
            worker_id: self.worker_id,
            registry: self.registry,
            poll_interval: self.poll_interval,
            timer_state: self.timer_state,
            signal_state: WithSignals {
                signal_source: source,
                signal_poll_interval: Duration::from_millis(500),
            },
            tracing_state: self.tracing_state,
            max_concurrent_flows: self.max_concurrent_flows,
            work_notify: self.work_notify,
        }
    }
}

impl<S: ExecutionLog + 'static, T, Src, Tr> Worker<S, T, WithSignals<Src>, Tr>
where
    Src: crate::executor::SignalSource + 'static,
{
    /// Sets the interval for polling signals.
    ///
    /// Only available when signal processing is enabled.
    /// Default is 500ms.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let worker = Worker::new(storage, "worker-1")
    ///     .with_signals(signal_source)
    ///     .with_signal_interval(Duration::from_millis(100));
    /// ```
    pub fn with_signal_interval(mut self, interval: Duration) -> Self {
        self.signal_state.signal_poll_interval = interval;
        self
    }
}

// State transition methods for tracing
impl<S: ExecutionLog + 'static, T, Sig> Worker<S, T, Sig, WithoutStructuredTracing> {
    /// Enables structured tracing for this worker.
    ///
    /// Returns a worker in the `WithStructuredTracing` state, which creates
    /// detailed spans with structured fields for all operations.
    ///
    /// When enabled, the worker will emit:
    /// - `worker_loop` spans for each iteration
    /// - `flow_execution` spans with flow_id, flow_type, task_id
    /// - `timer_processing` spans when checking timers
    ///
    /// # Example
    ///
    /// ```ignore
    /// let worker = Worker::new(storage, "worker-1")
    ///     .with_structured_tracing()
    ///     .start()
    ///     .await;
    /// ```
    pub fn with_structured_tracing(self) -> Worker<S, T, Sig, WithStructuredTracing> {
        Worker {
            storage: self.storage,
            worker_id: self.worker_id,
            registry: self.registry,
            poll_interval: self.poll_interval,
            timer_state: self.timer_state,
            signal_state: self.signal_state,
            tracing_state: WithStructuredTracing,
            max_concurrent_flows: self.max_concurrent_flows,
            work_notify: self.work_notify,
        }
    }
}

// Methods available for all timer, signal, and tracing state combinations
impl<
        S: ExecutionLog + 'static,
        T: TimerProcessing + 'static,
        Sig: SignalProcessing + 'static,
        Tr: TracingBehavior + 'static,
    > Worker<S, T, Sig, Tr>
{
    /// Sets the poll interval for checking the queue.
    ///
    /// Default is 1 second.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Enables backpressure control by limiting maximum concurrent flow executions.
    ///
    /// This prevents unbounded task spawning and provides flow control for high-load
    /// scenarios. When the limit is reached, the worker will wait for a slot to
    /// become available before picking up new flows.
    ///
    /// # Arguments
    ///
    /// * `max` - Maximum number of flows that can execute concurrently
    ///
    /// # Example
    ///
    /// ```ignore
    /// let worker = Worker::new(storage, "worker-1")
    ///     .with_max_concurrent_flows(100)  // Limit to 100 concurrent flows
    ///     .start()
    ///     .await;
    /// ```
    ///
    /// # Performance Considerations
    ///
    /// - **No limit** (default): Natural rate limiting via poll interval
    /// - **With limit**: Explicit backpressure, prevents resource exhaustion
    /// - Recommended for production: 50-500 depending on flow complexity
    pub fn with_max_concurrent_flows(mut self, max: usize) -> Self {
        self.max_concurrent_flows = Some(Arc::new(Semaphore::new(max)));
        self
    }

    /// Registers a flow type with its executor function.
    ///
    /// See [`Registry::register`] for details.
    ///
    /// Note: Using `Flow` as type parameter name to avoid collision with struct's `T` parameter.
    pub async fn register<Flow, F, Fut, R, E>(&self, executor: F)
    where
        Flow: DeserializeOwned + FlowType + Send + Sync + Clone + 'static,
        F: Fn(Arc<Flow>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        R: serde::Serialize + Send + 'static,
        E: Into<BoxError> + Send + 'static,
    {
        let mut registry = self.registry.write().await;
        registry.register(executor);
    }

    // ========================================================================
    // Helper Functions for Flow Execution
    // ========================================================================

    /// Signals a parent flow with the result of a child flow.
    ///
    /// This consolidates the parent signaling logic that was previously
    /// duplicated in three places (success, retry exhausted, retry check failed).
    async fn signal_parent_flow(
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
            // Bincode encodes Result as: [variant_index: 1 byte] + [data]
            // So we skip the first byte to get just the success value bytes
            if result_bytes.len() <= 1 {
                error!("Invalid result bytes for flow {}", flow_id);
                return;
            }

            let value_bytes = &result_bytes[1..]; // Skip the Ok variant byte

            crate::executor::child_flow::SignalPayload {
                success: true,
                data: value_bytes.to_vec(),
            }
        } else {
            // Error case
            let error_msg = error_msg.unwrap_or("Unknown error");
            let error_bytes = crate::core::serialize_value(&error_msg).unwrap_or_default();

            crate::executor::child_flow::SignalPayload {
                success: false,
                data: error_bytes,
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
            .store_signal_params(parent_id, waiting_step.step(), &signal_bytes)
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
    async fn check_should_retry(
        storage: &Arc<S>,
        flow: &crate::storage::ScheduledFlow,
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
    async fn handle_suspended_flow(
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

        // FIX: Check if signal arrived while we were still RUNNING
        // Race condition with multiple workers:
        // - Worker A: Parent suspends (RUNNING)
        // - Worker B: Child completes, calls resume_flow() → returns false (parent still RUNNING)
        // - Worker A: Marks parent SUSPENDED
        // - Need to check for pending signals and resume immediately
        if let Ok(invocations) = storage.get_invocations_for_flow(flow_id).await {
            for inv in invocations.iter() {
                if inv.status() == crate::core::InvocationStatus::WaitingForSignal {
                    if let Ok(Some(_)) = storage.get_signal_params(flow_id, inv.step()).await {
                        debug!(
                            "Found pending signal for suspended flow {} step {}, resuming immediately",
                            flow_id, inv.step()
                        );
                        match storage.resume_flow(flow_id).await {
                            Ok(true) => debug!("Resumed flow {} with pending signal", flow_id),
                            Ok(false) => {
                                debug!("Flow {} already resumed by another worker", flow_id)
                            }
                            Err(e) => warn!(
                                "Failed to resume flow {} with pending signal: {}",
                                flow_id, e
                            ),
                        }
                        break;
                    }
                }
            }
        }

        // Note: Task remains in database with all its step history intact.
        // When timer fires or signal arrives, resume_flow() will re-enqueue it,
        // and the flow will resume from where it left off (using cached step results).
    }

    /// Handles a successfully completed flow.
    async fn handle_flow_completion(
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

        // Signal parent (Level 3 API) - after successful completion
        Self::signal_parent_flow(storage, flow_id, parent_metadata, true, None).await;

        if let Err(e) = storage
            .complete_flow(flow_task_id, TaskStatus::Complete)
            .await
        {
            error!("Worker {} failed to mark flow complete: {}", worker_id, e);
        }
    }

    /// Handles a failed flow, checking retry policy and signaling parent if needed.
    async fn handle_flow_error(
        storage: &Arc<S>,
        worker_id: &str,
        flow: &crate::storage::ScheduledFlow,
        flow_task_id: Uuid,
        error: ExecutionError,
        parent_metadata: Option<(Uuid, String)>,
    ) {
        let error_msg = error.to_string();

        error!(
            "Worker {} flow failed: task_id={}, error={}",
            worker_id, flow_task_id, error_msg
        );

        // Mark NonRetryable errors as non-retryable in storage
        if matches!(error, ExecutionError::NonRetryable(_)) {
            if let Err(e) = storage.update_is_retryable(flow.flow_id, 0, false).await {
                warn!(
                    "Worker {} failed to mark error as non-retryable: {}",
                    worker_id, e
                );
            }
        }

        // Check retry policy
        match Self::check_should_retry(storage, flow).await {
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

                // Signal parent (Level 3 API) - after retries exhausted
                Self::signal_parent_flow(
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

                // Signal parent (Level 3 API) - failed to check retry policy
                Self::signal_parent_flow(
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

    /// Starts the worker in the background.
    ///
    /// Returns a [`WorkerHandle`] that can be used to control the worker.
    pub async fn start(self) -> WorkerHandle
    where
        Tr: Clone,
    {
        let cancellation_token = CancellationToken::new();
        let worker_token = cancellation_token.clone();
        let worker_id_for_handle = self.worker_id.clone();

        info!("Starting worker: {}", self.worker_id);

        let handle = tokio::spawn(async move {
            // Track spawned flow tasks for graceful shutdown
            let mut active_flows: JoinSet<()> = JoinSet::new();

            // Maintenance intervals
            let mut delayed_task_interval = tokio::time::interval(Duration::from_secs(1));
            let mut stale_lock_interval = tokio::time::interval(Duration::from_secs(60));

            // Don't fire immediately on startup - wait for first tick
            delayed_task_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            stale_lock_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            // Create channel for dequeue results to prevent concurrent dequeue calls
            // This ensures exactly ONE dequeue is in-flight at any time
            let (dequeue_tx, mut dequeue_rx) = tokio::sync::mpsc::unbounded_channel();

            // Spawn dedicated dequeue task to prevent concurrent dequeues
            let dequeue_storage = self.storage.clone();
            let dequeue_worker_id = self.worker_id.clone();
            let dequeue_token = worker_token.child_token();
            let dequeue_notify = self.work_notify.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = dequeue_token.cancelled() => {
                            break;
                        }
                        result = dequeue_storage.dequeue_flow(&dequeue_worker_id) => {
                            // Check if we got a task
                            let got_task = matches!(&result, Ok(Some(_)));

                            // Send result to main loop (non-blocking)
                            if dequeue_tx.send(result).is_err() {
                                // Channel closed, exit loop
                                break;
                            }

                            // If no task available, wait for notification from storage
                            // This provides event-driven wakeup instead of polling
                            // Redis Streams: XREADGROUP already blocks for 1000ms, this adds minimal overhead
                            // SQLite: Provides instant wakeup when work becomes available
                            // We use a timeout as a safety measure to prevent infinite waits
                            if !got_task {
                                tokio::time::timeout(
                                    Duration::from_millis(1000),
                                    dequeue_notify.notified()
                                ).await.ok();
                            }
                        }
                    }
                }
            });

            loop {
                // Create worker loop span (no-op for WithoutStructuredTracing)
                let loop_span = self.tracing_state.worker_loop_span(&self.worker_id);
                let _loop_guard = loop_span.as_ref().map(|span| span.enter());

                // Use select! to handle multiple concurrent concerns
                // biased; ensures shutdown and task cleanup are prioritized over accepting new work
                tokio::select! {
                    biased;

                    // Shutdown signal (highest priority)
                    // CancellationToken is cancel-safe and supports hierarchical cancellation
                    _ = worker_token.cancelled() => {
                        info!("Worker {} received shutdown signal", self.worker_id);
                        break;
                    }

                    // Maintenance: Move ready delayed tasks (every 1s)
                    _ = delayed_task_interval.tick() => {
                        match self.storage.move_ready_delayed_tasks().await {
                            Ok(count) if count > 0 => {
                                debug!("Worker {} moved {} delayed tasks to pending", self.worker_id, count);
                            }
                            Err(e) => {
                                warn!("Worker {} failed to move delayed tasks: {}", self.worker_id, e);
                            }
                            _ => {}
                        }
                    }

                    // Maintenance: Recover stale locks (every 60s)
                    _ = stale_lock_interval.tick() => {
                        match self.storage.recover_stale_locks().await {
                            Ok(count) if count > 0 => {
                                info!("Worker {} recovered {} stale locks", self.worker_id, count);
                            }
                            Err(e) => {
                                warn!("Worker {} failed to recover stale locks: {}", self.worker_id, e);
                            }
                            _ => {}
                        }
                    }

                    // Main work: Receive dequeued flow from dedicated task
                    Some(result) = dequeue_rx.recv() => {
                        // Reap completed flow tasks (non-blocking)
                        while let Some(result) = active_flows.try_join_next() {
                            match result {
                                Ok(_) => {
                                    // Task completed successfully
                                }
                                Err(e) => {
                                    // Task panicked or was cancelled
                                    error!("Worker {} flow task failed: {}", self.worker_id, e);
                                    // Note: The task's error handling should have already marked the flow as failed
                                    // This is a safety net for unexpected panics
                                }
                            }
                        }

                        // Process expired timers with optional span
                        {
                            let timer_span = self.tracing_state.timer_processing_span(&self.worker_id);
                            let _timer_guard = timer_span.as_ref().map(|span| span.enter());

                            self.timer_state
                                .process_timers(&self.storage, &self.worker_id)
                                .await;
                        }

                        // Process pending signals (deliver to waiting flows)
                        {
                            self.signal_state
                                .process_signals(&self.storage, &self.worker_id)
                                .await;
                        }

                        match result {
                    Ok(Some(flow)) => {
                        // Spawn flow execution in background so worker can continue processing timers
                        let registry = self.registry.clone();
                        let storage = self.storage.clone();
                        let worker_id = self.worker_id.clone();
                        let flow_task_id = flow.task_id;
                        let flow_id = flow.flow_id;
                        let flow_type = flow.flow_type.clone();
                        let tracing_state = self.tracing_state.clone(); // Clone the zero-sized tracing state

                        // Create flow execution span (returns Option<Span>)
                        let flow_span = tracing_state.flow_execution_span(
                            &worker_id,
                            flow_id,
                            &flow_type,
                            flow_task_id,
                        );

                        // Acquire semaphore permit for backpressure control (if enabled)
                        // This limits concurrent flow execution and provides flow control
                        let permit = if let Some(ref semaphore) = self.max_concurrent_flows {
                            match semaphore.clone().acquire_owned().await {
                                Ok(permit) => Some(permit),
                                Err(_) => {
                                    // Semaphore closed - should not happen in normal operation
                                    error!("Worker {} semaphore closed unexpectedly", worker_id);
                                    continue;
                                }
                            }
                        } else {
                            None
                        };

                        // Instrument the spawned task with the span (proper async way)
                        let task = async move {
                            // Hold permit during entire flow execution (RAII - released on drop)
                            let _permit = permit;

                            // Get executor with brief lock, then release before execution
                            let executor = {
                                let registry = registry.read().await;
                                registry.get_executor(&flow.flow_type)
                            };
                            // RwLock released here!

                            // Extract parent metadata for Level 3 API signaling
                            let parent_metadata = flow.parent_flow_id
                                .and_then(|parent_id| flow.signal_token.clone().map(|token| (parent_id, token)));

                            let outcome = match executor {
                                Some(exec) => {
                                    exec(flow.flow_data.clone(), flow.flow_id, storage.clone(), parent_metadata.clone()).await
                                }
                                None => {
                                    use crate::executor::FlowOutcome;
                                    FlowOutcome::Completed(Err(ExecutionError::Failed(format!("no executor registered for type: {}", flow.flow_type))))
                                }
                            };

                            // Handle FlowOutcome - suspension is explicit per Dave Cheney's principle
                            match outcome {
                                crate::executor::FlowOutcome::Suspended(reason) => {
                                    Self::handle_suspended_flow(
                                        &storage,
                                        &worker_id,
                                        flow_task_id,
                                        flow.flow_id,
                                        reason,
                                    )
                                    .await;
                                }
                                crate::executor::FlowOutcome::Completed(result) => {
                                    // Handle completion or retry
                                    match result {
                                        Ok(_) => {
                                            Self::handle_flow_completion(
                                                &storage,
                                                &worker_id,
                                                flow_task_id,
                                                flow.flow_id,
                                                parent_metadata.clone(),
                                            )
                                            .await;
                                        }
                                        Err(error) => {
                                            Self::handle_flow_error(
                                                &storage,
                                                &worker_id,
                                                &flow,
                                                flow_task_id,
                                                error,
                                                parent_metadata,
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                        };

                        // Spawn with proper instrumentation (avoids Span::enter() across await points)
                        // Track in JoinSet for graceful shutdown
                        if let Some(span) = flow_span {
                            active_flows.spawn(task.instrument(span));
                        } else {
                            active_flows.spawn(task);
                        }
                    }
                            Ok(None) => {
                                // No work available - sleep to avoid busy-looping and reduce lock contention
                                // Add jitter (deterministic based on worker_id) to prevent thundering herd
                                // Pattern from Redis Chapter 6: sleep 1ms between lock attempts
                                let worker_hash = self.worker_id.as_bytes().iter().fold(0u64, |acc, &b| acc.wrapping_add(b as u64));
                                let jitter_ms = 1 + (worker_hash % 5); // 1-5ms jitter
                                let sleep_duration = self.poll_interval + Duration::from_millis(jitter_ms);
                                tokio::time::sleep(sleep_duration).await;
                            }
                            Err(e) => {
                                warn!("Worker {} failed to dequeue flow: {}", self.worker_id, e);
                                // Add jitter to error retry as well
                                let worker_hash = self.worker_id.as_bytes().iter().fold(0u64, |acc, &b| acc.wrapping_add(b as u64));
                                let jitter_ms = 1 + (worker_hash % 5); // 1-5ms jitter
                                let sleep_duration = self.poll_interval + Duration::from_millis(jitter_ms);
                                tokio::time::sleep(sleep_duration).await;
                            }
                        }
                    }
                } // End of tokio::select!
            }

            // Gracefully wait for all in-flight flows to complete
            let in_flight_count = active_flows.len();
            if in_flight_count > 0 {
                info!(
                    "Worker {} waiting for {} in-flight flows to complete",
                    self.worker_id, in_flight_count
                );
                while (active_flows.join_next().await).is_some() {
                    // Wait for each flow to complete
                }
                info!(
                    "Worker {} completed all {} in-flight flows",
                    self.worker_id, in_flight_count
                );
            }

            info!("Worker {} stopped", self.worker_id);
        });

        WorkerHandle {
            worker_id: worker_id_for_handle,
            handle,
            cancellation_token,
        }
    }
}

/// Handle for controlling a running worker.
///
/// The `WorkerHandle` provides methods to check the worker's status and
/// request a graceful shutdown. Uses `CancellationToken` for hierarchical
/// cancellation support.
pub struct WorkerHandle {
    worker_id: String,
    handle: JoinHandle<()>,
    cancellation_token: CancellationToken,
}

impl WorkerHandle {
    /// Returns the worker ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Returns a reference to the cancellation token.
    ///
    /// This allows creating child tokens for hierarchical cancellation:
    ///
    /// ```ignore
    /// let child_token = handle.cancellation_token().child_token();
    /// // Child token will be cancelled when parent is cancelled
    /// ```
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    /// Requests the worker to shut down gracefully.
    ///
    /// This signals the worker to stop polling for new flows. The worker
    /// will complete any currently executing flows before shutting down.
    ///
    /// Uses `CancellationToken` which is cancel-safe and supports hierarchical
    /// cancellation patterns.
    pub async fn shutdown(self) {
        self.cancellation_token.cancel();
        let _ = self.handle.await;
    }

    /// Returns true if the worker task is still running.
    pub fn is_running(&self) -> bool {
        !self.handle.is_finished()
    }

    /// Aborts the worker immediately without waiting for completion.
    ///
    /// Note: This bypasses graceful shutdown and may leave flows in an
    /// inconsistent state. Prefer `shutdown()` for normal termination.
    pub fn abort(&self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Scheduler;
    use ergon_macros::FlowType;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, FlowType)]
    struct TestFlow {
        value: i32,
    }

    impl TestFlow {
        async fn execute(self: Arc<Self>) -> Result<i32, String> {
            // Simulate some work
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(self.value * 2)
        }
    }

    #[tokio::test]
    async fn test_worker_registry() {
        let storage = Arc::new(crate::storage::InMemoryExecutionLog::new());
        let worker = Worker::new(storage.clone(), "test-worker");

        worker.register(|flow: Arc<TestFlow>| flow.execute()).await;

        let registry = worker.registry.read().await;
        assert_eq!(registry.len(), 1);
    }

    #[tokio::test]
    async fn test_scheduler_enqueue() {
        let storage = Arc::new(crate::storage::InMemoryExecutionLog::new());
        let scheduler = Scheduler::new(storage.clone());

        let flow = TestFlow { value: 21 };
        let flow_id = Uuid::new_v4();
        let task_id = scheduler.schedule(flow, flow_id).await.unwrap();

        // Verify the task was enqueued
        let scheduled = storage.get_scheduled_flow(task_id).await.unwrap();
        assert!(scheduled.is_some());

        let scheduled = scheduled.unwrap();
        assert_eq!(scheduled.flow_id, flow_id);
        assert_eq!(scheduled.status, TaskStatus::Pending);
    }
}
