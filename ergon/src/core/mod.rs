//! Core types and utilities for the ergon durable execution engine.
//!
//! This module provides the fundamental building blocks for flow execution:
//!
//! # Domain Model
//! - [`Invocation`]: Represents a single step execution with parameters, status, and result
//! - [`InvocationStatus`]: The execution state (Pending, WaitingForSignal, Complete)
//! - [`CallType`]: The type of flow invocation (Run, Await, Resume)
//!
//! # Serialization
//! - [`serialize_value`]: Convert Rust types to bytes for storage
//! - [`deserialize_value`]: Convert bytes back to Rust types
//! - [`hash_params`]: Create fingerprints of parameters for change detection
//!
//! # Error Handling
//! - [`CoreError`]: Core error type with proper error chains
//! - [`Result<T>`]: Type alias for Results using CoreError
//!
//! # Retry Behavior
//! - [`RetryPolicy`]: Configuration for retry attempts and backoff strategy
//! - [`crate::executor::Retryable`]: Trait for fine-grained control over which errors trigger retries
//! - Automatic detection via autoref specialization (no manual configuration needed)
//! - See [`retry`] module for detailed documentation on the retry system
//!
//! # Example
//!
//! ```
//! use ergon::core::{Invocation, InvocationStatus, serialize_value, hash_params};
//! use uuid::Uuid;
//! use chrono::Utc;
//!
//! // Serialize parameters
//! let params = vec!["user123".to_string()];
//! let params_bytes = serialize_value(&params).unwrap();
//! let params_hash = hash_params(&params_bytes);
//!
//! // Create an invocation
//! let invocation = Invocation::new(
//!     Uuid::new_v4(),
//!     0,
//!     Utc::now(),
//!     "UserFlow".to_string(),
//!     "run".to_string(),
//!     InvocationStatus::Pending,
//!     1,
//!     params_bytes,
//!     params_hash,
//!     None,
//!     None,
//!     None,
//!     None,
//! );
//! ```

mod error;
mod flow_type;
mod invocation;
pub mod retry;
mod serialization;

// Re-export public types from submodules
pub use error::{CoreError, Result};
pub use flow_type::{FlowType, InvokableFlow};
pub use invocation::{CallType, Invocation, InvocationStatus};
pub use retry::{retry_with_policy, DefaultKind, RetryPolicy, RetryableKind};
pub use serialization::{deserialize_value, hash_params, serialize_value};

// Re-export kind module for macro use
pub mod kind {
    pub use super::retry::kind::*;
}

// Re-export deprecated names for backwards compatibility
#[doc(hidden)]
pub use retry::{DefaultResultKind, RetryableResultKind};
