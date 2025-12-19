//! Scheduler for distributed flow execution.
//!
//! Following Dave Cheney's principle "The name of an identifier includes its package name,"
//! we use `Scheduler` instead of `FlowScheduler` since the `ergon::` namespace already
//! indicates this is flow-related.
//!
//! # Typestate Pattern
//!
//! This scheduler uses the typestate pattern to enforce configuration at compile time.
//! A scheduler must be configured (versioned or explicitly unversioned) before use:
//!
//! ```no_run
//! use ergon::executor::Scheduler;
//! use ergon::storage::SqliteExecutionLog;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
//!
//! // Must configure before use
//! let scheduler = Scheduler::new(storage)
//!     .with_version("v1.0");  // or .unversioned() or .from_env()
//!
//! // Now you can schedule
//! # use serde::{Serialize, Deserialize};
//! # use ergon_macros::FlowType;
//! # #[derive(Serialize, Deserialize, FlowType)]
//! # struct Order { id: String }
//! # let order = Order { id: "123".into() };
//! scheduler.schedule(order).await?;
//! # Ok(())
//! # }
//! ```

use crate::core::{serialize_value, FlowType};
use crate::storage::{ExecutionLog, ScheduledFlow, StorageError};
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;
use uuid::Uuid;

/// Marker type for unconfigured scheduler state.
pub struct Unconfigured;

/// Marker type for configured scheduler state.
pub struct Configured;

/// Schedules flows for distributed execution.
///
/// Uses the typestate pattern to enforce configuration at compile time.
/// A scheduler must be configured (via `with_version`, `unversioned`, or `from_env`)
/// before flows can be scheduled.
///
/// # Design Philosophy
///
/// Following Dave Cheney's principle "design APIs for their default use case":
///
/// - **Deployment-level version**: Version is a deployment concern, not per-call.
///   Set once at startup, applied to all flows automatically.
/// - **No Option parameters**: Eliminates nil/None parameters in public API.
/// - **Compile-time safety**: Can't forget to configure - won't compile.
/// - **Simple default**: `schedule(flow)` auto-generates UUID.
/// - **Explicit control**: `schedule_with(flow, uuid)` for idempotency/retries.
///
/// # Examples
///
/// ## Basic Usage (Most Common)
///
/// ```no_run
/// use ergon::executor::Scheduler;
/// use ergon::storage::SqliteExecutionLog;
/// use ergon_macros::FlowType;
/// use serde::{Serialize, Deserialize};
/// use std::sync::Arc;
///
/// #[derive(Serialize, Deserialize, FlowType)]
/// struct OrderProcessor {
///     order_id: String,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
///
/// // Configure at deployment startup
/// let scheduler = Scheduler::new(storage)
///     .with_version(env!("CARGO_PKG_VERSION"));
///
/// // Simple scheduling - auto-generated UUID, deployment version applied
/// let order = OrderProcessor { order_id: "12345".to_string() };
/// let task_id = scheduler.schedule(order).await?;
///
/// println!("Scheduled flow with task_id: {}", task_id);
/// # Ok(())
/// # }
/// ```
///
/// ## From Environment Variable
///
/// ```no_run
/// # use ergon::executor::Scheduler;
/// # use ergon::storage::SqliteExecutionLog;
/// # use std::sync::Arc;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
/// // Set DEPLOY_VERSION=v1.2.3 in your environment
/// let scheduler = Scheduler::new(storage).from_env();
/// # Ok(())
/// # }
/// ```
///
/// ## Idempotent Scheduling (Retries)
///
/// ```no_run
/// # use ergon::executor::Scheduler;
/// # use ergon::storage::SqliteExecutionLog;
/// # use ergon_macros::FlowType;
/// # use serde::{Serialize, Deserialize};
/// # use std::sync::Arc;
/// # use uuid::Uuid;
/// #
/// # #[derive(Clone, Serialize, Deserialize, FlowType)]
/// # struct Payment { amount: f64 }
/// #
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
/// # let scheduler = Scheduler::new(storage).with_version("v1.0");
/// // Use specific UUID for retry safety
/// let idempotency_key = Uuid::new_v4();
/// let payment = Payment { amount: 99.99 };
///
/// // Safe to retry - same UUID won't create duplicate
/// scheduler.schedule_with(payment.clone(), idempotency_key).await?;
/// scheduler.schedule_with(payment, idempotency_key).await?;  // Deduped
/// # Ok(())
/// # }
/// ```
///
/// ## Explicitly Unversioned
///
/// ```no_run
/// # use ergon::executor::Scheduler;
/// # use ergon::storage::SqliteExecutionLog;
/// # use std::sync::Arc;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
/// // Explicitly opt out of versioning
/// let scheduler = Scheduler::new(storage).unversioned();
/// # Ok(())
/// # }
/// ```
pub struct Scheduler<S: ExecutionLog, State = Unconfigured> {
    storage: Arc<S>,
    version: Option<String>,
    _state: PhantomData<State>,
}

impl<S: ExecutionLog> Scheduler<S, Unconfigured> {
    /// Creates a new unconfigured scheduler.
    ///
    /// You must call `with_version()`, `unversioned()`, or `from_env()`
    /// before scheduling flows.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ergon::executor::Scheduler;
    /// # use ergon::storage::SqliteExecutionLog;
    /// # use std::sync::Arc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
    /// let scheduler = Scheduler::new(storage)
    ///     .with_version("v1.0");  // Must configure!
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            version: None,
            _state: PhantomData,
        }
    }

    /// Sets the deployment version for all scheduled flows.
    ///
    /// This is the most common configuration. The version is applied automatically
    /// to all flows scheduled by this scheduler, representing the code version
    /// of your deployment.
    ///
    /// # Arguments
    ///
    /// * `version` - Semantic version string (e.g., "1.0.0", "v2.0", "2024-01-15")
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ergon::executor::Scheduler;
    /// # use ergon::storage::SqliteExecutionLog;
    /// # use std::sync::Arc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
    /// // Use package version from Cargo.toml
    /// let scheduler = Scheduler::new(storage.clone())
    ///     .with_version(env!("CARGO_PKG_VERSION"));
    ///
    /// // Or a custom deployment tag
    /// let scheduler = Scheduler::new(storage.clone())
    ///     .with_version("production-2024-01");
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_version(self, version: impl Into<String>) -> Scheduler<S, Configured> {
        Scheduler {
            storage: self.storage,
            version: Some(version.into()),
            _state: PhantomData,
        }
    }

    /// Explicitly opts out of versioning.
    ///
    /// Use this when you don't need version tracking. All flows scheduled
    /// by this scheduler will have `version = None`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ergon::executor::Scheduler;
    /// # use ergon::storage::SqliteExecutionLog;
    /// # use std::sync::Arc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
    /// let scheduler = Scheduler::new(storage).unversioned();
    /// # Ok(())
    /// # }
    /// ```
    pub fn unversioned(self) -> Scheduler<S, Configured> {
        Scheduler {
            storage: self.storage,
            version: None,
            _state: PhantomData,
        }
    }

    /// Configures version from `DEPLOY_VERSION` environment variable.
    ///
    /// If the environment variable is set, uses that value as the version.
    /// If not set, creates an unversioned scheduler.
    ///
    /// This is useful for deployments where the version is injected at runtime.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ergon::executor::Scheduler;
    /// # use ergon::storage::SqliteExecutionLog;
    /// # use std::sync::Arc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
    /// // Reads from DEPLOY_VERSION env var
    /// // $ DEPLOY_VERSION=v1.2.3 cargo run
    /// let scheduler = Scheduler::new(storage).from_env();
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_env(self) -> Scheduler<S, Configured> {
        match std::env::var("DEPLOY_VERSION").ok() {
            Some(version) => self.with_version(version),
            None => self.unversioned(),
        }
    }
}

impl<S: ExecutionLog> Scheduler<S, Configured> {
    /// Schedules a flow with auto-generated UUID.
    ///
    /// This is the default scheduling method. A UUID is automatically generated
    /// for the flow execution, and the configured deployment version is applied.
    ///
    /// # Arguments
    ///
    /// * `flow` - The flow instance to schedule
    ///
    /// # Returns
    ///
    /// Returns the UUID assigned to this scheduled flow (the task ID).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The flow cannot be serialized
    /// - The storage backend doesn't support distributed execution
    /// - The enqueue operation fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ergon::executor::Scheduler;
    /// # use ergon::storage::SqliteExecutionLog;
    /// # use ergon_macros::FlowType;
    /// # use serde::{Serialize, Deserialize};
    /// # use std::sync::Arc;
    /// #
    /// # #[derive(Serialize, Deserialize, FlowType)]
    /// # struct Order { id: String }
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
    /// let scheduler = Scheduler::new(storage).with_version("v1.0");
    ///
    /// let order = Order { id: "12345".into() };
    /// let task_id = scheduler.schedule(order).await?;
    ///
    /// println!("Scheduled with ID: {}", task_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn schedule<T>(&self, flow: T) -> Result<Uuid, StorageError>
    where
        T: Serialize + FlowType + 'static,
    {
        let flow_id = Uuid::new_v4();
        self.schedule_impl(flow, flow_id).await
    }

    /// Schedules a flow with a specific UUID.
    ///
    /// Use this when you need to control the UUID, typically for:
    /// - **Idempotent retries**: Using the same UUID ensures duplicate schedules
    ///   are detected and deduplicated.
    /// - **Parent-child relationships**: When a child flow needs to reference
    ///   the parent's UUID.
    /// - **External correlation**: When integrating with external systems that
    ///   provide their own identifiers.
    ///
    /// # Arguments
    ///
    /// * `flow` - The flow instance to schedule
    /// * `flow_id` - The UUID to use for this flow execution
    ///
    /// # Returns
    ///
    /// Returns the UUID you provided (the task ID).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ergon::executor::Scheduler;
    /// # use ergon::storage::SqliteExecutionLog;
    /// # use ergon_macros::FlowType;
    /// # use serde::{Serialize, Deserialize};
    /// # use std::sync::Arc;
    /// # use uuid::Uuid;
    /// #
    /// # #[derive(Clone, Serialize, Deserialize, FlowType)]
    /// # struct Payment { amount: f64 }
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let storage = Arc::new(SqliteExecutionLog::new("flows.db").await?);
    /// let scheduler = Scheduler::new(storage).with_version("v1.0");
    ///
    /// // Idempotent retry - same UUID won't create duplicate
    /// let retry_key = Uuid::new_v4();
    /// let payment = Payment { amount: 99.99 };
    ///
    /// scheduler.schedule_with(payment.clone(), retry_key).await?;
    /// scheduler.schedule_with(payment, retry_key).await?;  // Safe retry
    /// # Ok(())
    /// # }
    /// ```
    pub async fn schedule_with<T>(&self, flow: T, flow_id: Uuid) -> Result<Uuid, StorageError>
    where
        T: Serialize + FlowType + 'static,
    {
        self.schedule_impl(flow, flow_id).await
    }

    /// Internal implementation for scheduling.
    ///
    /// Both `schedule` and `schedule_with` delegate to this method.
    async fn schedule_impl<T>(&self, flow: T, flow_id: Uuid) -> Result<Uuid, StorageError>
    where
        T: Serialize + FlowType + 'static,
    {
        // Get the stable flow type ID for worker routing
        let flow_type = T::type_id().to_string();

        // Serialize the flow instance
        let flow_data = serialize_value(&flow)?;

        // Create scheduled flow with configured version
        let scheduled =
            ScheduledFlow::new_with_version(flow_id, flow_type, flow_data, self.version.clone());

        // Enqueue it
        self.storage.enqueue_flow(scheduled).await
    }

    /// Returns a reference to the underlying storage.
    ///
    /// Useful for advanced use cases where you need direct storage access.
    pub fn storage(&self) -> &Arc<S> {
        &self.storage
    }

    /// Returns the configured deployment version, if any.
    ///
    /// Returns `None` if the scheduler was created with `unversioned()`.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ergon_macros::FlowType;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, FlowType)]
    struct TestFlow {
        data: String,
    }

    #[tokio::test]
    async fn test_schedule_with_version() {
        let storage = Arc::new(crate::storage::InMemoryExecutionLog::new());
        let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

        let flow = TestFlow {
            data: "test".to_string(),
        };

        let task_id = scheduler.schedule(flow).await.unwrap();

        // Verify the flow was enqueued with version
        let scheduled = storage.get_scheduled_flow(task_id).await.unwrap();
        assert!(scheduled.is_some());

        let scheduled = scheduled.unwrap();
        assert_eq!(scheduled.version.as_deref(), Some("v1.0"));
    }

    #[tokio::test]
    async fn test_schedule_unversioned() {
        let storage = Arc::new(crate::storage::InMemoryExecutionLog::new());
        let scheduler = Scheduler::new(storage.clone()).unversioned();

        let flow = TestFlow {
            data: "test".to_string(),
        };

        let task_id = scheduler.schedule(flow).await.unwrap();

        // Verify the flow was enqueued without version
        let scheduled = storage.get_scheduled_flow(task_id).await.unwrap();
        assert!(scheduled.is_some());

        let scheduled = scheduled.unwrap();
        assert_eq!(scheduled.version, None);
    }

    #[tokio::test]
    async fn test_schedule_with_specific_uuid() {
        let storage = Arc::new(crate::storage::InMemoryExecutionLog::new());
        let scheduler = Scheduler::new(storage.clone()).with_version("v1.0");

        let flow = TestFlow {
            data: "test".to_string(),
        };
        let expected_id = Uuid::new_v4();

        let task_id = scheduler.schedule_with(flow, expected_id).await.unwrap();

        assert_eq!(task_id, expected_id);

        let scheduled = storage.get_scheduled_flow(task_id).await.unwrap();
        assert!(scheduled.is_some());

        let scheduled = scheduled.unwrap();
        assert_eq!(scheduled.flow_id, expected_id);
    }

    #[tokio::test]
    async fn test_from_env_with_version() {
        // Clean up at start in case previous test left it set
        std::env::remove_var("DEPLOY_VERSION");
        std::env::set_var("DEPLOY_VERSION", "v2.0");

        let storage = Arc::new(crate::storage::InMemoryExecutionLog::new());
        let scheduler = Scheduler::new(storage.clone()).from_env();

        let flow = TestFlow {
            data: "test".to_string(),
        };

        let task_id = scheduler.schedule(flow).await.unwrap();

        let scheduled = storage.get_scheduled_flow(task_id).await.unwrap();
        let version = scheduled.as_ref().unwrap().version.clone();

        std::env::remove_var("DEPLOY_VERSION");

        assert_eq!(version.as_deref(), Some("v2.0"));
    }

    #[tokio::test]
    async fn test_from_env_without_version() {
        // Clean up any env var from other tests
        std::env::remove_var("DEPLOY_VERSION");

        let storage = Arc::new(crate::storage::InMemoryExecutionLog::new());
        let scheduler = Scheduler::new(storage.clone()).from_env();

        let flow = TestFlow {
            data: "test".to_string(),
        };

        let task_id = scheduler.schedule(flow).await.unwrap();

        let scheduled = storage.get_scheduled_flow(task_id).await.unwrap();
        let version = scheduled.as_ref().unwrap().version.clone();

        // Make sure to clean up before assertion
        std::env::remove_var("DEPLOY_VERSION");

        assert_eq!(version, None);
    }
}
