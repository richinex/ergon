//! Distributed flow worker.
//!
//! This module provides workers that poll a storage queue for flows and execute
//! them in a distributed manner.

use crate::core::deserialize_value;
use crate::storage::{ExecutionLog, ScheduledFlow, TaskStatus};
use crate::Ergon;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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
/// let mut registry = FlowRegistry::new();
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
    /// let mut registry = FlowRegistry::new();
    /// registry.register(|flow: Arc<OrderProcessor>| flow.process());
    /// ```
    pub fn register<T, F, Fut, R, E>(&mut self, executor: F)
    where
        T: DeserializeOwned + Send + Sync + Clone + 'static,
        F: Fn(Arc<T>) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        E: std::fmt::Display + Send + 'static,
    {
        let type_name = std::any::type_name::<T>().to_string();

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
                    // The executor expects Arc<T>, but instance.execute expects FnOnce(T)
                    // So we wrap the executor call to convert T -> Arc<T>
                    let result = instance
                        .execute(move |f: T| executor(Arc::new(f)))
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
/// # Example
///
/// ```no_run
/// use ergon::executor::{FlowWorker, FlowRegistry};
/// use ergon::storage::SqliteExecutionLog;
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
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = Arc::new(SqliteExecutionLog::new("flows.db")?);
/// let mut worker = FlowWorker::new(storage, "worker-1");
///
/// worker.register(|flow: Arc<MyFlow>| flow.run());
///
/// let handle = worker.start().await;
/// // Worker is now running in the background...
///
/// // Later, to stop:
/// handle.shutdown().await;
/// # Ok(())
/// # }
/// ```
pub struct FlowWorker<S: ExecutionLog + 'static> {
    storage: Arc<S>,
    worker_id: String,
    registry: Arc<RwLock<FlowRegistry<S>>>,
    poll_interval: Duration,
}

impl<S: ExecutionLog + 'static> FlowWorker<S> {
    /// Creates a new flow worker.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage backend that implements the queue operations
    /// * `worker_id` - Unique identifier for this worker
    pub fn new(storage: Arc<S>, worker_id: impl Into<String>) -> Self {
        Self {
            storage,
            worker_id: worker_id.into(),
            registry: Arc::new(RwLock::new(FlowRegistry::new())),
            poll_interval: Duration::from_secs(1),
        }
    }

    /// Check if a failed flow should be retried and return the delay.
    ///
    /// This method checks the retry policy stored with the flow (step 0 invocation)
    /// and determines if the flow should be retried based on:
    /// - Retry count vs max_attempts
    /// - Whether the error is retryable (if RetryableError trait is implemented)
    ///
    /// Returns:
    /// - Ok(Some(delay)) if the flow should be retried with the given delay
    /// - Ok(None) if the flow should not be retried
    /// - Err(e) if there was an error checking the policy
    async fn should_retry_flow(
        &self,
        flow: &ScheduledFlow,
        _error_msg: &str,
    ) -> Result<Option<Duration>, String> {
        // First check if any step has a non-retryable error
        match self.storage.has_non_retryable_error(flow.flow_id).await {
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
                // Continue anyway - don't fail the retry check due to storage error
            }
        }

        // Get the flow's invocation (step 0) to check retry policy
        match self.storage.get_invocation(flow.flow_id, 0).await {
            Ok(Some(invocation)) => {
                if let Some(policy) = invocation.retry_policy() {
                    // Calculate delay for next attempt (retry_count is 0-indexed, attempt is 1-indexed)
                    let next_attempt = flow.retry_count + 1;

                    // delay_for_attempt returns None if we've exceeded max attempts
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
                } else {
                    // No retry policy - don't retry
                    debug!("Flow {} has no retry policy", flow.flow_id);
                    Ok(None)
                }
            }
            Ok(None) => {
                // No invocation found - shouldn't happen, but don't retry
                warn!("No invocation found for flow {}", flow.flow_id);
                Ok(None)
            }
            Err(e) => {
                // Error getting invocation
                Err(format!(
                    "Failed to get invocation for flow {}: {}",
                    flow.flow_id, e
                ))
            }
        }
    }

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
    pub async fn register<T, F, Fut, R, E>(&self, executor: F)
    where
        T: DeserializeOwned + Send + Sync + Clone + 'static,
        F: Fn(Arc<T>) -> Fut + Send + Sync + Clone + 'static,
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
    pub async fn start(self) -> WorkerHandle {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let worker_id_for_handle = self.worker_id.clone();

        info!("Starting worker: {}", self.worker_id);

        let handle = tokio::spawn(async move {
            loop {
                // Check for shutdown signal
                if shutdown_rx.try_recv().is_ok() {
                    info!("Worker {} received shutdown signal", self.worker_id);
                    break;
                }

                // Poll for a flow
                match self.storage.dequeue_flow(&self.worker_id).await {
                    Ok(Some(flow)) => {
                        debug!(
                            "Worker {} picked up flow: task_id={}, flow_type={}",
                            self.worker_id, flow.task_id, flow.flow_type
                        );

                        // Execute the flow
                        let registry = self.registry.read().await;
                        let result = registry.execute(&flow, self.storage.clone()).await;
                        drop(registry);

                        // Handle completion or retry
                        match result {
                            Ok(_) => {
                                info!(
                                    "Worker {} completed flow: task_id={}",
                                    self.worker_id, flow.task_id
                                );
                                if let Err(e) = self
                                    .storage
                                    .complete_flow(flow.task_id, TaskStatus::Complete)
                                    .await
                                {
                                    error!(
                                        "Worker {} failed to mark flow complete: {}",
                                        self.worker_id, e
                                    );
                                }
                            }
                            Err(error_msg) => {
                                // Flow failed - check if we should retry
                                error!(
                                    "Worker {} flow failed: task_id={}, error={}",
                                    self.worker_id, flow.task_id, error_msg
                                );

                                // Check if this flow should be retried
                                match self.should_retry_flow(&flow, &error_msg).await {
                                    Ok(Some(delay)) => {
                                        info!(
                                            "Worker {} retrying flow: task_id={}, attempt={}, delay={:?}",
                                            self.worker_id, flow.task_id, flow.retry_count + 1, delay
                                        );
                                        if let Err(e) = self
                                            .storage
                                            .retry_flow(flow.task_id, error_msg.clone(), delay)
                                            .await
                                        {
                                            error!(
                                                "Worker {} failed to schedule retry: {}",
                                                self.worker_id, e
                                            );
                                        }
                                    }
                                    Ok(None) => {
                                        // Don't retry - mark as failed permanently
                                        debug!(
                                            "Worker {} not retrying flow: task_id={} (not retryable or max attempts reached)",
                                            self.worker_id, flow.task_id
                                        );
                                        if let Err(e) = self
                                            .storage
                                            .complete_flow(flow.task_id, TaskStatus::Failed)
                                            .await
                                        {
                                            error!(
                                                "Worker {} failed to mark flow failed: {}",
                                                self.worker_id, e
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Worker {} failed to check retry policy: {}, marking as failed",
                                            self.worker_id, e
                                        );
                                        if let Err(e) = self
                                            .storage
                                            .complete_flow(flow.task_id, TaskStatus::Failed)
                                            .await
                                        {
                                            error!(
                                                "Worker {} failed to mark flow failed: {}",
                                                self.worker_id, e
                                            );
                                        }
                                    }
                                }
                            }
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
