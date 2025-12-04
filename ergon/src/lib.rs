//! Ergon: Durable Execution Framework for Rust
//!
//! `ergon` (ἔργον, Greek for "work" or "deed") is a durable execution framework inspired by
//! Temporal, providing fault-tolerant workflow execution with automatic retry, caching,
//! and DAG-based parallelization.
//!
//! # Features
//!
//! - **Durable execution**: Automatically persists execution state and resumes from failures
//! - **Step caching**: Memoizes step results for deterministic replay
//! - **DAG-based parallelization**: Automatically parallelizes independent steps
//! - **Retry logic**: Configurable retry with exponential backoff
//! - **External signals**: Wait for and respond to external events
//! - **Type-safe**: Full type safety with Rust's type system
//!
//! # Quick Start
//!
//! ```ignore
//! use ergon::prelude::*;
//!
//! #[derive(Clone)]
//! struct MyFlow {
//!     name: String,
//! }
//!
//! #[flow]
//! impl MyFlow {
//!     #[flow]
//!     async fn run(&self) -> Result<String, String> {
//!         let greeting = self.greet().await?;
//!         let result = self.process(&greeting).await?;
//!         Ok(result)
//!     }
//!
//!     #[step]
//!     async fn greet(&self) -> Result<String, String> {
//!         Ok(format!("Hello, {}!", self.name))
//!     }
//!
//!     #[step]
//!     async fn process(&self, msg: &str) -> Result<String, String> {
//!         Ok(msg.to_uppercase())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let storage = Arc::new(SqliteExecutionLog::new("my.db")?);
//!     let flow = MyFlow { name: "World".to_string() };
//!     let id = Uuid::new_v4();
//!
//!     let instance = FlowInstance::new(id, flow, storage);
//!     let result = instance.execute(|f| f.run()).await?;
//!
//!     println!("Result: {}", result);
//!     Ok(())
//! }
//! ```
//!
//! # Module Organization
//!
//! Following Parnas's information hiding principles, each module hides specific design
//! decisions that are likely to change:
//!
//! - [`core`]: Foundation types and traits (hides serialization format)
//! - [`storage`]: Persistence layer (hides database choice and schema)
//! - [`graph`]: DAG structures (hides graph implementation)
//! - [`executor`]: Execution engine (hides execution strategy)
//!
//! # Design Principles
//!
//! This library follows Dave Cheney's practical programming wisdom:
//! - **Simplicity**: Simple, focused APIs that do one thing well
//! - **Clarity**: Explicit over implicit, readable over clever
//! - **Safety**: Hard to misuse, defaults prevent common mistakes

// Allow the derive macro to reference ::ergon from within this crate
extern crate self as ergon;

pub mod core;
pub mod executor;
pub mod graph;
pub mod storage;

// Re-export commonly used types for convenience
pub use core::{
    deserialize_value, hash_params, serialize_value, CallType, Error as CoreError, Invocation,
    InvocationStatus, Result as CoreResult, RetryableError,
};

// Re-export the kind module for macro use (autoref specialization)
pub use core::kind;

pub use executor::{
    await_external_signal, idempotency_key, idempotency_key_parts, retry_with_policy, ArcStepExt,
    DeferredRegistry, ExecutionContext, ExecutionError, Executor, Registry,
    Result as ExecutionResult, Scheduler, StepFuture, StepHandle, Worker, WorkerHandle, CALL_TYPE,
    EXECUTION_CONTEXT,
};

pub use graph::{Graph, GraphError, GraphResult, StepId, StepNode};

pub use storage::{
    ExecutionLog, InMemoryExecutionLog, PoolConfig, Result as StorageResult, ScheduledFlow,
    StorageError, TaskStatus,
};

#[cfg(feature = "sqlite")]
pub use storage::SqliteExecutionLog;

#[cfg(feature = "redis")]
pub use storage::RedisExecutionLog;

// Re-export proc-macros
pub use ergon_macros::{dag, flow, step};

// Re-export dependencies used in public API
// This ensures users don't have version mismatch errors (Effective Rust Item 24)
pub use serde; // Users implement Serialize/Deserialize on their types
pub use tokio;
pub use uuid; // Users create Uuid::new_v4() for flow IDs // Users need tokio runtime and #[tokio::main]

/// Prelude module for convenient glob imports
///
/// # Example
///
/// ```ignore
/// use ergon::prelude::*;
/// ```
pub mod prelude {
    pub use crate::core::{
        CallType, Error as CoreError, Invocation, InvocationStatus, Result as CoreResult,
        RetryableError,
    };

    pub use crate::executor::{
        await_external_signal, idempotency_key, idempotency_key_parts, ArcStepExt,
        DeferredRegistry, ExecutionContext, ExecutionError, Executor, Registry,
        Result as ExecutionResult, Scheduler, StepFuture, StepHandle, Worker, WorkerHandle,
    };

    pub use crate::graph::{Graph, GraphError, GraphResult, StepId};

    pub use crate::storage::{
        ExecutionLog, InMemoryExecutionLog, PoolConfig, Result as StorageResult, ScheduledFlow,
        StorageError, TaskStatus,
    };

    #[cfg(feature = "sqlite")]
    pub use crate::storage::SqliteExecutionLog;

    #[cfg(feature = "redis")]
    pub use crate::storage::RedisExecutionLog;

    pub use ergon_macros::{dag, flow, step, FlowType};

    // Re-export commonly used external types
    pub use serde::{Deserialize, Serialize};
    pub use std::sync::Arc;
    pub use uuid::Uuid;
}
