//! Flow context management module.
//!
//! This module hides the complexity of:
//! - Task-local context setup and teardown
//! - CallType scoping for execution modes (Run, Resume, Await)
//! - ExecutionContext lifecycle management
//!
//! Following Parnas's information hiding principle, this module encapsulates
//! decisions about how execution context is propagated through the task-local
//! storage mechanism.

use super::{ExecutionContext, CALL_TYPE, EXECUTION_CONTEXT};
use crate::core::CallType;
use crate::storage::ExecutionLog;
use std::future::Future;
use std::sync::Arc;
use uuid::Uuid;

/// Manages execution context setup and scoping for flow execution.
///
/// FlowContext encapsulates the task-local context propagation mechanism,
/// providing a clean interface for running code within an execution context.
/// This separates the "how" of context management from the "what" of flow execution.
///
/// The generic parameter `S` allows for monomorphization over concrete storage
/// types, enabling compiler optimizations.
pub struct FlowContext<S: ExecutionLog> {
    /// The execution context for this flow.
    context: Arc<ExecutionContext<S>>,
}

impl<S: ExecutionLog + 'static> FlowContext<S> {
    /// Creates a new FlowContext for the given flow ID and storage backend.
    pub fn new(id: Uuid, storage: Arc<S>) -> Self {
        Self {
            context: Arc::new(ExecutionContext::new(id, storage)),
        }
    }

    /// Creates a FlowContext from an existing ExecutionContext.
    pub fn from_context(context: Arc<ExecutionContext<S>>) -> Self {
        Self { context }
    }

    /// Returns a reference to the underlying ExecutionContext.
    pub fn execution_context(&self) -> &Arc<ExecutionContext<S>> {
        &self.context
    }

    /// Executes an async closure within this flow context with Run call type.
    ///
    /// This method sets up the task-local EXECUTION_CONTEXT and CALL_TYPE,
    /// executes the provided future, and properly cleans up afterward.
    ///
    /// Note: Due to task-local requirements, the context is type-erased to
    /// `Box<dyn ExecutionLog>` for storage in the task-local variable.
    pub async fn run_scoped<F, Fut, R>(&self, f: F) -> R
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = R>,
    {
        // Create a type-erased context for the task-local
        let erased_ctx = self.create_erased_context();

        EXECUTION_CONTEXT
            .scope(
                erased_ctx,
                CALL_TYPE.scope(CallType::Run, async { f().await }),
            )
            .await
    }

    /// Executes an async closure within this flow context with Resume call type.
    ///
    /// Used when resuming a flow that was waiting for an external signal.
    pub async fn resume_scoped<F, Fut, R>(&self, f: F) -> R
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = R>,
    {
        let erased_ctx = self.create_erased_context();

        EXECUTION_CONTEXT
            .scope(
                erased_ctx,
                CALL_TYPE.scope(CallType::Resume, async { f().await }),
            )
            .await
    }

    /// Executes a synchronous closure within this flow context.
    ///
    /// This method uses block_on to bridge from sync to async context,
    /// setting up the required task-local variables.
    ///
    /// # Requirements
    /// Requires an active tokio runtime created with Runtime::new() + enter().
    ///
    /// # Panics
    /// Panics if no tokio runtime is available.
    pub fn run_sync_scoped<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let handle = tokio::runtime::Handle::try_current().expect(
            "Sync flows require an active tokio runtime created with Runtime::new() + enter(). \
             Cannot use #[tokio::main] due to nested block_on restriction.",
        );

        let erased_ctx = self.create_erased_context();

        handle.block_on(async {
            EXECUTION_CONTEXT
                .scope(erased_ctx, CALL_TYPE.scope(CallType::Run, async { f() }))
                .await
        })
    }

    /// Creates a type-erased execution context for task-local storage.
    ///
    /// This is necessary because task-local variables cannot be generic,
    /// so we need to erase the concrete storage type to `Box<dyn ExecutionLog>`.
    fn create_erased_context(&self) -> Arc<ExecutionContext<Box<dyn ExecutionLog>>> {
        // We need to create a new context with the storage wrapped in a Box
        // This allows us to store it in the non-generic task-local
        let boxed_storage: Box<dyn ExecutionLog> =
            Box::new(StorageWrapper(Arc::clone(&self.context.storage)));
        Arc::new(ExecutionContext::new(
            self.context.id,
            Arc::new(boxed_storage),
        ))
    }
}

/// Wrapper to allow `Arc<S>` to be used as a `Box<dyn ExecutionLog>`.
///
/// This wrapper delegates all `ExecutionLog` methods to the inner storage.
struct StorageWrapper<S: ExecutionLog>(Arc<S>);

#[async_trait::async_trait]
impl<S: ExecutionLog> ExecutionLog for StorageWrapper<S> {
    async fn log_invocation_start(
        &self,
        params: crate::storage::InvocationStartParams<'_>,
    ) -> crate::storage::Result<()> {
        self.0.log_invocation_start(params).await
    }

    async fn log_invocation_completion(
        &self,
        id: uuid::Uuid,
        step: i32,
        return_value: &[u8],
    ) -> crate::storage::Result<crate::core::Invocation> {
        self.0
            .log_invocation_completion(id, step, return_value)
            .await
    }

    async fn get_invocation(
        &self,
        id: uuid::Uuid,
        step: i32,
    ) -> crate::storage::Result<Option<crate::core::Invocation>> {
        self.0.get_invocation(id, step).await
    }

    async fn get_latest_invocation(
        &self,
        id: uuid::Uuid,
    ) -> crate::storage::Result<Option<crate::core::Invocation>> {
        self.0.get_latest_invocation(id).await
    }

    async fn get_invocations_for_flow(
        &self,
        id: uuid::Uuid,
    ) -> crate::storage::Result<Vec<crate::core::Invocation>> {
        self.0.get_invocations_for_flow(id).await
    }

    async fn get_incomplete_flows(&self) -> crate::storage::Result<Vec<crate::core::Invocation>> {
        self.0.get_incomplete_flows().await
    }

    async fn reset(&self) -> crate::storage::Result<()> {
        self.0.reset().await
    }

    async fn close(&self) -> crate::storage::Result<()> {
        self.0.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SqliteExecutionLog;

    #[tokio::test]
    async fn test_flow_context_run_scoped() {
        let storage = Arc::new(SqliteExecutionLog::in_memory().unwrap());
        let id = Uuid::new_v4();
        let ctx = FlowContext::new(id, storage);

        let result = ctx
            .run_scoped(|| async {
                // Verify we can access the execution context
                EXECUTION_CONTEXT.with(|c| {
                    assert_eq!(c.id, id);
                });
                42
            })
            .await;

        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_flow_context_resume_scoped() {
        let storage = Arc::new(SqliteExecutionLog::in_memory().unwrap());
        let id = Uuid::new_v4();
        let ctx = FlowContext::new(id, storage);

        let result = ctx
            .resume_scoped(|| async {
                // Verify call type is Resume
                CALL_TYPE.with(|ct| {
                    assert!(matches!(*ct, CallType::Resume));
                });
                "resumed"
            })
            .await;

        assert_eq!(result, "resumed");
    }
}
