//! Storage layer for the ergon durable execution engine.
//!
//! This module provides a trait-based interface for persisting execution state
//! with multiple backend implementations:
//!
//! - [`SqliteExecutionLog`]: Persistent SQLite-based storage with connection pooling
//! - [`InMemoryExecutionLog`]: Fast in-memory storage for testing and development
//!
//! # Example
//!
//! ```no_run
//! use ergon::storage::{ExecutionLog, InMemoryExecutionLog};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let log = InMemoryExecutionLog::new();
//! // Use the log for storing execution state
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use uuid::Uuid;

mod error;
mod params;

#[cfg(feature = "sqlite")]
pub mod sqlite;

pub mod memory;

// Re-export public types
pub use error::{Result, StorageError};
pub use memory::InMemoryExecutionLog;
pub use params::InvocationStartParams;

#[cfg(feature = "sqlite")]
pub use sqlite::{PoolConfig, SqliteExecutionLog};

use crate::core::Invocation;

/// Trait for execution log storage backends.
///
/// This trait defines the async interface for persisting and retrieving
/// flow execution state. Implementations must be thread-safe.
///
/// Using `async_trait` allows truly async storage backends (e.g., async
/// database drivers) without forcing blocking calls in async contexts.
#[async_trait]
pub trait ExecutionLog: Send + Sync {
    /// Log the start of a step invocation.
    /// The params_hash is computed internally from the parameters bytes.
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()>;

    /// Log the completion of a step invocation.
    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation>;

    /// Get a specific invocation by flow ID and step number.
    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>>;

    /// Get the latest invocation for a flow.
    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>>;

    /// Get all invocations for a flow.
    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>>;

    /// Get all incomplete flows (flows that haven't completed).
    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>>;

    /// Reset the execution log (delete all entries).
    async fn reset(&self) -> Result<()>;

    /// Close the execution log.
    async fn close(&self) -> Result<()>;
}

// Implement ExecutionLog for Box<dyn ExecutionLog> to allow type-erased storage
#[async_trait]
impl ExecutionLog for Box<dyn ExecutionLog> {
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()> {
        (**self).log_invocation_start(params).await
    }

    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation> {
        (**self)
            .log_invocation_completion(id, step, return_value)
            .await
    }

    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>> {
        (**self).get_invocation(id, step).await
    }

    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>> {
        (**self).get_latest_invocation(id).await
    }

    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>> {
        (**self).get_invocations_for_flow(id).await
    }

    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>> {
        (**self).get_incomplete_flows().await
    }

    async fn reset(&self) -> Result<()> {
        (**self).reset().await
    }

    async fn close(&self) -> Result<()> {
        (**self).close().await
    }
}
