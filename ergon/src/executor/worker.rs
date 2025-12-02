//! Distributed flow worker.
//!
//! This module provides workers that poll a storage queue for flows and execute
//! them in a distributed manner.

use crate::core::{deserialize_value, FlowType};
use crate::storage::{ExecutionLog, ScheduledFlow, TaskStatus};
use crate::Ergon;
use chrono::Utc;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn, Instrument};
use uuid::Uuid;

use super::timer::TIMER_NOTIFIERS;

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
                            // Successfully claimed - fire the timer
                            let key = (timer.flow_id, timer.step);
                            if let Some(notifier) =
                                TIMER_NOTIFIERS.get(&key).map(|entry| entry.value().clone())
                            {
                                notifier.notify_one();
                            }
                            info!(
                                "Timer fired: flow={} step={} name={:?}",
                                timer.flow_id, timer.step, timer.timer_name
                            );
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
type BoxedExecutor<S> = Box<
    dyn Fn(Vec<u8>, Uuid, Arc<S>) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

/// Registry that maps flow type names to their executors.
///
/// The `FlowRegistry` stores executor functions that can deserialize and execute
/// flows based on their type name. This allows the worker to handle different
/// flow types dynamically.
///
/// # Example
///
/// ```no_run
/// use ergon::executor::FlowRegistry;
/// use ergon::storage::InMemoryExecutionLog;
/// use serde::{Serialize, Deserialize};
/// use std::sync::Arc;
///
/// #[derive(Serialize, Deserialize, Clone)]
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
/// let mut registry: FlowRegistry<InMemoryExecutionLog> = FlowRegistry::new();
/// registry.register(|flow: Arc<MyFlow>| flow.run());
/// ```
pub struct FlowRegistry<S: ExecutionLog> {
    executors: HashMap<String, BoxedExecutor<S>>,
}

impl<S: ExecutionLog + 'static> FlowRegistry<S> {
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
    /// # use ergon::executor::FlowRegistry;
    /// # use ergon::storage::InMemoryExecutionLog;
    /// # use serde::{Serialize, Deserialize};
    /// # use std::sync::Arc;
    /// #
    /// # #[derive(Serialize, Deserialize, Clone)]
    /// # struct OrderProcessor { order_id: String }
    /// #
    /// # impl OrderProcessor {
    /// #     async fn process(self: Arc<Self>) -> Result<String, String> {
    /// #         Ok(self.order_id.clone())
    /// #     }
    /// # }
    /// let mut registry: FlowRegistry<InMemoryExecutionLog> = FlowRegistry::new();
    /// registry.register(|flow: Arc<OrderProcessor>| flow.process());
    /// ```
    pub fn register<T, F, Fut, R, E>(&mut self, executor: F)
    where
        T: DeserializeOwned + FlowType + Send + Sync + Clone + 'static,
        F: Fn(Arc<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        // Use stable type ID instead of std::any::type_name()
        // This ensures compatibility across different compiler versions
        let type_name = T::type_id().to_string();

        let boxed: BoxedExecutor<S> =
            Box::new(move |data: Vec<u8>, flow_id: Uuid, storage: Arc<S>| {
                // Clone the executor for this invocation
                let executor = executor.clone();

                Box::pin(async move {
                    // Deserialize the flow
                    let flow: T = deserialize_value(&data)
                        .map_err(|e| format!("failed to deserialize flow: {}", e))?;

                    // Create flow instance using Ergon factory
                    let instance = Ergon::new_flow(flow, flow_id, storage);

                    // Execute the flow using the registered executor
                    // The executor expects Arc<T>, so we clone &T to create owned T, then wrap in Arc
                    let result = instance
                        .executor()
                        .execute(move |f: &T| Box::pin(executor(Arc::new(f.clone()))))
                        .await
                        .map_err(|e| format!("flow execution failed: {}", e))?;

                    // Check if the flow itself returned an error
                    result.map(|_| ()).map_err(|e| e.to_string())
                })
            });

        debug!("Registered flow type: {}", type_name);
        self.executors.insert(type_name, boxed);
    }

    /// Executes a scheduled flow using the registered executor for its type.
    ///
    /// Returns an error if the flow type is not registered.
    async fn execute(&self, flow: &ScheduledFlow, storage: Arc<S>) -> Result<(), String> {
        let executor = self
            .executors
            .get(&flow.flow_type)
            .ok_or_else(|| format!("no executor registered for type: {}", flow.flow_type))?;

        executor(flow.flow_data.clone(), flow.flow_id, storage).await
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

impl<S: ExecutionLog + 'static> Default for FlowRegistry<S> {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker that executes flows from a distributed queue.
///
/// The `FlowWorker` polls a storage backend's task queue for pending flows,
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
/// use ergon::executor::{FlowWorker, FlowRegistry};
/// use ergon::storage::SqliteExecutionLog;
/// use serde::{Serialize, Deserialize};
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// #[derive(Serialize, Deserialize, Clone)]
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
/// let storage = Arc::new(SqliteExecutionLog::new("flows.db")?);
///
/// // Basic worker (no timers, no structured tracing)
/// let worker = FlowWorker::new(storage.clone(), "worker-1");
///
/// // Worker with timers and structured tracing
/// let worker_advanced = FlowWorker::new(storage.clone(), "worker-2")
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
pub struct FlowWorker<S: ExecutionLog + 'static, T = WithoutTimers, Tr = WithoutStructuredTracing> {
    storage: Arc<S>,
    worker_id: String,
    registry: Arc<RwLock<FlowRegistry<S>>>,
    poll_interval: Duration,
    timer_state: T,
    tracing_state: Tr,
}

impl<S: ExecutionLog + 'static, Tr> FlowWorker<S, WithoutTimers, Tr> {
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
    /// let worker = FlowWorker::new(storage, "worker-1")
    ///     .with_timers()
    ///     .with_timer_interval(Duration::from_millis(100));
    /// ```
    pub fn with_timers(self) -> FlowWorker<S, WithTimers, Tr> {
        FlowWorker {
            storage: self.storage,
            worker_id: self.worker_id,
            registry: self.registry,
            poll_interval: self.poll_interval,
            timer_state: WithTimers {
                timer_poll_interval: Duration::from_secs(1),
            },
            tracing_state: self.tracing_state,
        }
    }
}

impl<S: ExecutionLog + 'static> FlowWorker<S, WithoutTimers, WithoutStructuredTracing> {
    /// Creates a new flow worker without timer processing or structured tracing.
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
    /// # use ergon::executor::FlowWorker;
    /// # use ergon::storage::SqliteExecutionLog;
    /// # use std::sync::Arc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let storage = Arc::new(SqliteExecutionLog::new("flows.db")?);
    /// let worker = FlowWorker::new(storage, "worker-1");
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(storage: Arc<S>, worker_id: impl Into<String>) -> Self {
        Self {
            storage,
            worker_id: worker_id.into(),
            registry: Arc::new(RwLock::new(FlowRegistry::new())),
            poll_interval: Duration::from_secs(1),
            timer_state: WithoutTimers,
            tracing_state: WithoutStructuredTracing,
        }
    }
}

impl<S: ExecutionLog + 'static, Tr> FlowWorker<S, WithTimers, Tr> {
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
    /// let worker = FlowWorker::new(storage, "worker-1")
    ///     .with_timers()
    ///     .with_timer_interval(Duration::from_millis(100));
    /// ```
    pub fn with_timer_interval(mut self, interval: Duration) -> Self {
        self.timer_state.timer_poll_interval = interval;
        self
    }
}

// State transition methods for tracing
impl<S: ExecutionLog + 'static, T> FlowWorker<S, T, WithoutStructuredTracing> {
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
    /// let worker = FlowWorker::new(storage, "worker-1")
    ///     .with_structured_tracing()
    ///     .start()
    ///     .await;
    /// ```
    pub fn with_structured_tracing(self) -> FlowWorker<S, T, WithStructuredTracing> {
        FlowWorker {
            storage: self.storage,
            worker_id: self.worker_id,
            registry: self.registry,
            poll_interval: self.poll_interval,
            timer_state: self.timer_state,
            tracing_state: WithStructuredTracing,
        }
    }
}

// Methods available for all timer and tracing state combinations
impl<S: ExecutionLog + 'static, T: TimerProcessing + 'static, Tr: TracingBehavior + 'static>
    FlowWorker<S, T, Tr>
{
    /// Sets the poll interval for checking the queue.
    ///
    /// Default is 1 second.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Registers a flow type with its executor function.
    ///
    /// See [`FlowRegistry::register`] for details.
    ///
    /// Note: Using `Flow` as type parameter name to avoid collision with struct's `T` parameter.
    pub async fn register<Flow, F, Fut, R, E>(&self, executor: F)
    where
        Flow: DeserializeOwned + FlowType + Send + Sync + Clone + 'static,
        F: Fn(Arc<Flow>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        let mut registry = self.registry.write().await;
        registry.register(executor);
    }

    /// Starts the worker in the background.
    ///
    /// Returns a [`WorkerHandle`] that can be used to control the worker.
    pub async fn start(self) -> WorkerHandle
    where
        Tr: Clone,
    {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let worker_id_for_handle = self.worker_id.clone();

        info!("Starting worker: {}", self.worker_id);

        let handle = tokio::spawn(async move {
            loop {
                // Create worker loop span (no-op for WithoutStructuredTracing)
                let loop_span = self.tracing_state.worker_loop_span(&self.worker_id);
                let _loop_guard = loop_span.as_ref().map(|span| span.enter());

                // Check for shutdown signal
                if shutdown_rx.try_recv().is_ok() {
                    info!("Worker {} received shutdown signal", self.worker_id);
                    break;
                }

                // Process expired timers with optional span
                {
                    let timer_span = self.tracing_state.timer_processing_span(&self.worker_id);
                    let _timer_guard = timer_span.as_ref().map(|span| span.enter());

                    self.timer_state
                        .process_timers(&self.storage, &self.worker_id)
                        .await;
                }

                // Poll for a flow
                match self.storage.dequeue_flow(&self.worker_id).await {
                    Ok(Some(flow)) => {
                        debug!(
                            "Worker {} picked up flow: task_id={}, flow_type={}",
                            self.worker_id, flow.task_id, flow.flow_type
                        );

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

                        // Instrument the spawned task with the span (proper async way)
                        let task = async move {
                            info!(
                                "Worker {} executing flow: task_id={}",
                                worker_id, flow_task_id
                            );

                            // Execute the flow
                            let registry = registry.read().await;
                            let result = registry.execute(&flow, storage.clone()).await;
                            drop(registry);

                            // Handle completion or retry
                            match result {
                                Ok(_) => {
                                    info!(
                                        "Worker {} completed flow: task_id={}",
                                        worker_id, flow_task_id
                                    );
                                    if let Err(e) = storage
                                        .complete_flow(flow_task_id, TaskStatus::Complete)
                                        .await
                                    {
                                        error!(
                                            "Worker {} failed to mark flow complete: {}",
                                            worker_id, e
                                        );
                                    }
                                }
                                Err(error_msg) => {
                                    // Flow failed - check if we should retry
                                    error!(
                                        "Worker {} flow failed: task_id={}, error={}",
                                        worker_id, flow_task_id, error_msg
                                    );

                                    // Check retry policy
                                    let should_retry = async {
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
                                                if let Some(policy) = invocation.retry_policy() {
                                                    let next_attempt = flow.retry_count + 1;

                                                    if let Some(delay) =
                                                        policy.delay_for_attempt(next_attempt)
                                                    {
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
                                                } else {
                                                    debug!(
                                                        "Flow {} has no retry policy",
                                                        flow.flow_id
                                                    );
                                                    Ok(None)
                                                }
                                            }
                                            Ok(None) => {
                                                warn!(
                                                    "No invocation found for flow {}",
                                                    flow.flow_id
                                                );
                                                Ok(None)
                                            }
                                            Err(e) => {
                                                Err(format!("Failed to get invocation: {}", e))
                                            }
                                        }
                                    };

                                    match should_retry.await {
                                        Ok(Some(delay)) => {
                                            info!(
                                                "Worker {} retrying flow: task_id={}, attempt={}, delay={:?}",
                                                worker_id, flow_task_id, flow.retry_count + 1, delay
                                            );
                                            if let Err(e) = storage
                                                .retry_flow(flow_task_id, error_msg.clone(), delay)
                                                .await
                                            {
                                                error!(
                                                    "Worker {} failed to schedule retry: {}",
                                                    worker_id, e
                                                );
                                            }
                                        }
                                        Ok(None) => {
                                            debug!(
                                                "Worker {} not retrying flow: task_id={} (not retryable or max attempts reached)",
                                                worker_id, flow_task_id
                                            );
                                            if let Err(e) = storage
                                                .complete_flow(flow_task_id, TaskStatus::Failed)
                                                .await
                                            {
                                                error!(
                                                    "Worker {} failed to mark flow failed: {}",
                                                    worker_id, e
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Worker {} failed to check retry policy: {}, marking as failed",
                                                worker_id, e
                                            );
                                            if let Err(e) = storage
                                                .complete_flow(flow_task_id, TaskStatus::Failed)
                                                .await
                                            {
                                                error!(
                                                    "Worker {} failed to mark flow failed: {}",
                                                    worker_id, e
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        };

                        // Spawn with proper instrumentation (avoids Span::enter() across await points)
                        if let Some(span) = flow_span {
                            tokio::spawn(task.instrument(span));
                        } else {
                            tokio::spawn(task);
                        }
                    }
                    Ok(None) => {
                        // No work available - sleep before next poll
                        tokio::time::sleep(self.poll_interval).await;
                    }
                    Err(e) => {
                        warn!("Worker {} failed to dequeue flow: {}", self.worker_id, e);
                        tokio::time::sleep(self.poll_interval).await;
                    }
                }
            }

            info!("Worker {} stopped", self.worker_id);
        });

        WorkerHandle {
            worker_id: worker_id_for_handle,
            handle,
            shutdown: Some(shutdown_tx),
        }
    }
}

/// Handle for controlling a running worker.
///
/// The `WorkerHandle` provides methods to check the worker's status and
/// request a graceful shutdown.
pub struct WorkerHandle {
    worker_id: String,
    handle: JoinHandle<()>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl WorkerHandle {
    /// Returns the worker ID.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Requests the worker to shut down gracefully.
    ///
    /// This signals the worker to stop polling for new flows. The worker
    /// will complete any currently executing flow before shutting down.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
            let _ = self.handle.await;
        }
    }

    /// Returns true if the worker task is still running.
    pub fn is_running(&self) -> bool {
        !self.handle.is_finished()
    }

    /// Aborts the worker immediately without waiting for completion.
    pub fn abort(&self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::FlowScheduler;
    use crate::storage::SqliteExecutionLog;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct TestFlow {
        value: i32,
    }

    // Manual FlowType implementation (will be generated by derive macro once implemented)
    impl FlowType for TestFlow {
        fn type_id() -> &'static str {
            "TestFlow"
        }
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
        let storage = Arc::new(SqliteExecutionLog::in_memory().unwrap());
        let worker = FlowWorker::new(storage.clone(), "test-worker");

        worker.register(|flow: Arc<TestFlow>| flow.execute()).await;

        let registry = worker.registry.read().await;
        assert_eq!(registry.len(), 1);
    }

    #[tokio::test]
    async fn test_scheduler_enqueue() {
        let storage = Arc::new(SqliteExecutionLog::in_memory().unwrap());
        let scheduler = FlowScheduler::new(storage.clone());

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
