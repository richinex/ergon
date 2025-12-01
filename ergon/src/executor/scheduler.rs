//! Flow scheduler for distributed execution.
//!
//! The scheduler provides a simple API for enqueuing flows for distributed
//! execution by workers.

use crate::core::serialize_value;
use crate::storage::{ExecutionLog, ScheduledFlow, StorageError};
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

/// Schedules flows for distributed execution.
///
/// The `FlowScheduler` provides a convenient API for enqueuing flows into
/// a storage backend's task queue. Workers can then pick up these flows
/// and execute them.
///
/// # Example
///
/// ```no_run
/// use ergon::executor::FlowScheduler;
/// use ergon::storage::SqliteExecutionLog;
/// use std::sync::Arc;
/// use uuid::Uuid;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct MyFlow {
///     data: String,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let storage = Arc::new(SqliteExecutionLog::new("flows.db")?);
/// let scheduler = FlowScheduler::new(storage);
///
/// let flow = MyFlow { data: "test".to_string() };
/// let flow_id = Uuid::new_v4();
/// let task_id = scheduler.schedule(flow, flow_id).await?;
///
/// println!("Scheduled flow with task_id: {}", task_id);
/// # Ok(())
/// # }
/// ```
pub struct FlowScheduler<S: ExecutionLog> {
    storage: Arc<S>,
}

impl<S: ExecutionLog> FlowScheduler<S> {
    /// Creates a new flow scheduler with the given storage backend.
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Schedules a flow for distributed execution.
    ///
    /// This method serializes the flow and enqueues it in the storage backend's
    /// task queue. A worker can then pick up the flow and execute it.
    ///
    /// # Arguments
    ///
    /// * `flow` - The flow instance to schedule
    /// * `flow_id` - Unique identifier for the flow execution
    ///
    /// # Returns
    ///
    /// Returns the task ID assigned to this scheduled flow.
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
    /// # use ergon::executor::FlowScheduler;
    /// # use ergon::storage::SqliteExecutionLog;
    /// # use std::sync::Arc;
    /// # use uuid::Uuid;
    /// # use serde::{Serialize, Deserialize};
    /// #
    /// # #[derive(Serialize, Deserialize)]
    /// # struct OrderProcessor { order_id: String }
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let storage = Arc::new(SqliteExecutionLog::new("flows.db")?);
    /// let scheduler = FlowScheduler::new(storage);
    ///
    /// let order = OrderProcessor { order_id: "12345".to_string() };
    /// let task_id = scheduler.schedule(order, Uuid::new_v4()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn schedule<T>(&self, flow: T, flow_id: Uuid) -> Result<Uuid, StorageError>
    where
        T: Serialize + 'static,
    {
        // Get the flow type name for worker routing
        let flow_type = std::any::type_name::<T>().to_string();

        // Serialize the flow instance
        let flow_data = serialize_value(&flow)?;

        // Create scheduled flow
        let scheduled = ScheduledFlow::new(flow_id, flow_type, flow_data);

        // Enqueue it
        self.storage.enqueue_flow(scheduled).await
    }

    /// Returns a reference to the underlying storage.
    pub fn storage(&self) -> &Arc<S> {
        &self.storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SqliteExecutionLog;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestFlow {
        data: String,
    }

    #[tokio::test]
    async fn test_schedule_flow() {
        let storage = Arc::new(SqliteExecutionLog::in_memory().unwrap());
        let scheduler = FlowScheduler::new(storage.clone());

        let flow = TestFlow {
            data: "test".to_string(),
        };
        let flow_id = Uuid::new_v4();

        let task_id = scheduler.schedule(flow, flow_id).await.unwrap();

        // Verify the flow was enqueued
        let scheduled = storage.get_scheduled_flow(task_id).await.unwrap();
        assert!(scheduled.is_some());

        let scheduled = scheduled.unwrap();
        assert_eq!(scheduled.flow_id, flow_id);
        assert!(scheduled.flow_type.contains("TestFlow"));
    }
}
