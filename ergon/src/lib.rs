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
//!     let executor = Executor::new(id, flow, storage);
//!     match executor.execute(|f| Box::pin(f.run())).await {
//!         FlowOutcome::Completed(Ok(result)) => println!("Result: {}", result),
//!         FlowOutcome::Completed(Err(e)) => eprintln!("Flow failed: {}", e),
//!         FlowOutcome::Suspended(reason) => println!("Flow suspended: {:?}", reason),
//!     }
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
    deserialize_value, hash_params, retry_with_policy, serialize_value, CallType, CoreError,
    Invocation, InvocationStatus, Result as CoreResult,
};

// Re-export the kind module for macro use (autoref specialization)
pub use core::kind;

pub use executor::{
    await_external_signal, idempotency_key, idempotency_key_parts, ArcStepExt, DeferredRegistry,
    ExecutionContext, ExecutionError, Executor, Registry, Result as ExecutionResult, Retryable,
    Scheduler, StepHandle, Worker, WorkerHandle, CALL_TYPE, EXECUTION_CONTEXT,
};

pub use graph::{Graph, GraphError, GraphResult, StepId, StepNode};

pub use storage::{
    ExecutionLog, InMemoryExecutionLog, PoolConfig, Result as StorageResult, ScheduledFlow,
    StorageError, TaskStatus,
};

#[cfg(feature = "sqlite")]
pub use storage::SqliteExecutionLog;

#[cfg(feature = "postgres")]
pub use storage::PostgresExecutionLog;

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
///
/// # What's included:
/// - **Macros**: `#[flow]`, `#[step]`, `#[dag]`, `FlowType`
/// - **Core API**: `Scheduler`, `Worker`, `ExecutionContext`, `await_external_signal`
/// - **Traits**: `ArcStepExt` (extension methods for step futures)
/// - **Storage**: `ExecutionLog` and concrete implementations
/// - **External types**: `Arc`, `Uuid`, `Serialize`, `Deserialize`
///
/// # What's NOT included (import explicitly when needed):
/// - **Result types** - Use `ergon::executor::Result`, `ergon::storage::Result`, etc.
/// - **Error types** - Use `ergon::executor::ExecutionError`, `ergon::storage::StorageError`, etc.
/// - **Advanced types** - Use `ergon::Invocation`, `ergon::PoolConfig`, etc.
pub mod prelude {
    // ========================================================================
    // Macros - Essential entry point to the framework
    // ========================================================================
    pub use ergon_macros::{dag, flow, step, FlowType};

    // ========================================================================
    // Core API - Used in every workflow application
    // ========================================================================
    pub use crate::executor::{
        await_external_signal, // Signal/suspend API
        idempotency_key,       // Key generation utilities
        idempotency_key_parts,
        ExecutionContext, // Flow context (passed to every flow)
        Scheduler,        // Schedule flows
        Worker,           // Process flows
        WorkerHandle,     // Worker lifecycle management
    };

    // ========================================================================
    // Traits - Provide common extension methods
    // ========================================================================
    pub use crate::executor::ArcStepExt;

    // ========================================================================
    // Storage - Main abstraction and implementations
    // ========================================================================
    pub use crate::storage::ExecutionLog; // Core storage trait
    pub use crate::storage::InMemoryExecutionLog; // In-memory implementation

    #[cfg(feature = "sqlite")]
    pub use crate::storage::SqliteExecutionLog; // SQLite implementation

    #[cfg(feature = "postgres")]
    pub use crate::storage::PostgresExecutionLog; // Postgres implementation

    #[cfg(feature = "redis")]
    pub use crate::storage::RedisExecutionLog; // Redis implementation

    // ========================================================================
    // External types - Used throughout workflow code
    // ========================================================================
    pub use serde::{Deserialize, Serialize};
    pub use std::sync::Arc;
    pub use uuid::Uuid;
}
