use crate::graph::GraphError;
use crate::storage::StorageError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Execution layer error type for the ergon durable execution engine.
///
/// This error type wraps storage and core errors while also providing
/// execution-specific error variants for flow management.
#[derive(Debug, Error, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ExecutionError {
    /// A storage operation failed.
    #[error("storage error: {0}")]
    Storage(String),

    /// A core serialization or deserialization error occurred.
    #[error("core error: {0}")]
    Core(String),

    /// A graph operation failed (cycle detection, missing dependency, etc.)
    #[error("graph error: {0}")]
    Graph(String),

    /// Flow execution failed with the given reason (retryable based on policy).
    #[error("execution failed: {0}")]
    Failed(String),

    /// The flow structure is incompatible with the stored state.
    #[error("flow incompatible: {0}")]
    Incompatible(String),

    /// A background task panicked during execution.
    #[error("task panicked: {0}")]
    TaskPanic(String),

    /// An external signal timed out while waiting.
    #[error("signal timeout: {message}")]
    SignalTimeout { message: String },

    /// A user error with preserved retryability metadata.
    #[error("{type_name}: {message}")]
    User {
        type_name: String,
        message: String,
        retryable: bool,
    },
}


// Manual From implementations to convert nested errors to strings
impl From<StorageError> for ExecutionError {
    fn from(e: StorageError) -> Self {
        ExecutionError::Storage(e.to_string())
    }
}

impl From<crate::core::Error> for ExecutionError {
    fn from(e: crate::core::Error) -> Self {
        ExecutionError::Core(e.to_string())
    }
}

impl From<GraphError> for ExecutionError {
    fn from(e: GraphError) -> Self {
        ExecutionError::Graph(e.to_string())
    }
}

/// Reason why a flow suspended execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SuspendReason {
    /// Waiting for a timer to fire.
    Timer { flow_id: uuid::Uuid, step: i32 },
    /// Waiting for an external signal.
    Signal {
        flow_id: uuid::Uuid,
        step: i32,
        signal_name: String,
    },
}

/// A flow can complete or suspend. Be honest about it.
///
/// This enum makes suspension explicit, following Dave Cheney's principle:
/// "If your function can suspend, you must tell the caller."
#[derive(Debug, Clone)]
pub enum FlowOutcome<R> {
    /// Flow ran to completion (success or failure in R).
    Completed(R),
    /// Flow suspended, waiting for external event (timer or signal).
    Suspended(SuspendReason),
}

impl From<String> for ExecutionError {
    fn from(s: String) -> Self {
        ExecutionError::Failed(s)
    }
}

// =============================================================================
// RETRYABLE ERROR TRAIT
// =============================================================================

/// Trait for error types to specify whether they should trigger a retry.
///
/// Implement this trait on your error types to get fine-grained control over
/// which errors are retried vs. cached as permanent failures.
///
/// # Default Behavior (without this trait)
///
/// If your error type does NOT implement `Retryable`:
/// - ALL errors trigger retry (not cached)
/// - Use `#[step(cache_errors)]` to cache all errors
///
/// # With Retryable
///
/// If your error type implements `Retryable`:
/// - `is_retryable() == true`: Error is NOT cached, step will retry
/// - `is_retryable() == false`: Error IS cached, step won't retry
///
/// # Example
///
/// ```rust
/// use ergon::executor::Retryable;
///
/// #[derive(Debug)]
/// enum PaymentError {
///     // Transient errors - should retry
///     NetworkTimeout,
///     ServiceUnavailable,
///     RateLimited,
///
///     // Permanent errors - should NOT retry
///     InsufficientFunds,
///     InvalidCard,
///     FraudDetected,
/// }
///
/// impl Retryable for PaymentError {
///     fn is_retryable(&self) -> bool {
///         matches!(self,
///             PaymentError::NetworkTimeout |
///             PaymentError::ServiceUnavailable |
///             PaymentError::RateLimited
///         )
///     }
/// }
/// ```
///
/// # Design Rationale
///
/// This approach follows Dave Cheney's principle "APIs should be hard to misuse":
/// - The safe default (retry all errors) requires no extra code
/// - Fine-grained control requires explicit implementation
/// - The logic lives with the error type, not scattered in step attributes
pub trait Retryable {
    /// Returns true if this error is transient and the operation should be retried.
    ///
    /// - `true`: Error is transient (network timeout, service unavailable).
    ///   The step will NOT be cached, allowing retry on next execution.
    /// - `false`: Error is permanent (invalid input, not found, business rule violation).
    ///   The step WILL be cached, preventing retry.
    fn is_retryable(&self) -> bool;
}

// Implement Retryable for common error types

impl Retryable for std::io::Error {
    fn is_retryable(&self) -> bool {
        use std::io::ErrorKind;
        matches!(
            self.kind(),
            ErrorKind::ConnectionRefused
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::NotConnected
                | ErrorKind::TimedOut
                | ErrorKind::Interrupted
                | ErrorKind::WouldBlock
        )
    }
}

impl Retryable for String {
    /// Strings are retryable by default - allowing for transient error messages.
    ///
    /// Since String is a generic error type, we cannot distinguish between
    /// transient errors ("connection timeout") and permanent errors ("invalid input").
    /// Following the "safe default" principle, we treat all String errors as retryable.
    ///
    /// Use `#[step(cache_errors)]` or a custom error type for permanent failures.
    fn is_retryable(&self) -> bool {
        true
    }
}

impl<T: Retryable> Retryable for Box<T> {
    fn is_retryable(&self) -> bool {
        (**self).is_retryable()
    }
}

impl<T: Retryable> Retryable for std::sync::Arc<T> {
    fn is_retryable(&self) -> bool {
        (**self).is_retryable()
    }
}

/// ExecutionError implements Retryable to distinguish between
/// transient infrastructure errors (retryable) and permanent framework errors (non-retryable).
///
/// After removing suspension-as-error, ExecutionError only contains:
/// - Infrastructure errors (Storage, Core, Failed, SignalTimeout): transient, should retry
/// - Framework permanent errors (Incompatible, TaskPanic, Graph): permanent, should NOT retry
/// - User errors: delegates to the retryable field set by the original error's is_retryable()
impl Retryable for ExecutionError {
    fn is_retryable(&self) -> bool {
        match self {
            // Infrastructure errors - transient, should retry
            ExecutionError::Storage(_) => true,
            ExecutionError::Core(_) => true,
            ExecutionError::Failed(_) => true,
            ExecutionError::SignalTimeout { .. } => true,

            // Framework permanent errors - should NOT retry
            ExecutionError::Incompatible(_) => false,
            ExecutionError::TaskPanic(_) => false,
            ExecutionError::Graph(_) => false,

            // User errors - use the retryable field from original error
            ExecutionError::User { retryable, .. } => *retryable,
        }
    }
}

pub type Result<T> = std::result::Result<T, ExecutionError>;

/// Formats parameter bytes as a human-readable preview for error messages.
/// Shows both hex representation and attempts to show printable ASCII characters.
pub(super) fn format_params_preview(bytes: &[u8]) -> String {
    const MAX_BYTES: usize = 48;
    let truncated = bytes.len() > MAX_BYTES;
    let preview_bytes = if truncated {
        &bytes[..MAX_BYTES]
    } else {
        bytes
    };

    // Try to extract any printable strings from the bytes for context
    let printable: String = preview_bytes
        .iter()
        .filter_map(|&b| {
            if b.is_ascii_alphanumeric() || b == b' ' || b == b'_' || b == b'-' {
                Some(b as char)
            } else {
                None
            }
        })
        .collect();

    // Build hex representation
    let hex: String = preview_bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ");

    let suffix = if truncated { "..." } else { "" };

    if printable.len() >= 3 {
        // If we found meaningful printable content, show it
        format!("[{}{}] (contains: \"{}\")", hex, suffix, printable)
    } else {
        format!("[{}{}]", hex, suffix)
    }
}
