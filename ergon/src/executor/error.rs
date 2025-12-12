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

    /// A flow error with preserved retryability metadata.
    #[error("{0}")]
    Flow(FlowError),
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

/// Unified error type for flow execution with type preservation.
///
/// This type wraps any serializable error while preserving type information
/// for potential downcasting. Inspired by Axum's IntoResponse pattern.
///
/// Key features:
/// - Preserves error type name and message
/// - Stores serialized error data for downcasting
/// - Tracks retry policy
/// - Works with any error type that implements Error + Serialize
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowError {
    /// Type name of the original error (e.g., "OrderError")
    pub type_name: String,
    /// Human-readable error message
    pub message: String,
    /// Serialized error data for downcasting (JSON bytes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<u8>>,
    /// Whether this error should trigger a retry
    pub retryable: bool,
}

impl FlowError {
    /// Create a retryable flow error from any serializable error.
    pub fn retryable<E>(err: E) -> Self
    where
        E: std::error::Error + Serialize,
    {
        Self {
            type_name: std::any::type_name::<E>().to_string(),
            message: err.to_string(),
            data: serde_json::to_vec(&err).ok(),
            retryable: true,
        }
    }

    /// Create a terminal (non-retryable) flow error from any serializable error.
    pub fn terminal<E>(err: E) -> Self
    where
        E: std::error::Error + Serialize,
    {
        Self {
            type_name: std::any::type_name::<E>().to_string(),
            message: err.to_string(),
            data: serde_json::to_vec(&err).ok(),
            retryable: false,
        }
    }

    /// Attempt to downcast this FlowError back to its original type.
    ///
    /// Returns Some(E) if:
    /// - The error data was serialized
    /// - Deserialization succeeds
    /// - Type matches
    ///
    /// Returns None otherwise.
    pub fn downcast<E>(&self) -> Option<E>
    where
        E: serde::de::DeserializeOwned,
    {
        self.data
            .as_ref()
            .and_then(|d| serde_json::from_slice(d).ok())
    }
}

impl std::fmt::Display for FlowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.type_name, self.message)
    }
}

impl std::error::Error for FlowError {}

impl crate::core::RetryableError for FlowError {
    fn is_retryable(&self) -> bool {
        self.retryable
    }
}

/// Trait for converting errors into FlowError.
///
/// This trait is automatically implemented for all types that implement
/// Error + Serialize, allowing natural use of the `?` operator with any
/// error type in flow methods.
///
/// # Default Behavior
///
/// If your error type does NOT implement `RetryableError`:
/// - All errors are treated as retryable (safe default)
///
/// If your error type implements `RetryableError`:
/// - Uses `is_retryable()` for fine-grained control
///
/// This uses autoref specialization (same pattern as RetryableError) to
/// automatically detect whether the error type implements RetryableError.
///
/// Inspired by Axum's IntoResponse pattern.
pub trait IntoFlowError {
    fn into_flow_error(self) -> FlowError;
}

/// Blanket implementation for all serializable errors.
///
/// Requires errors to implement RetryableError for fine-grained control.
/// This is intentionally restrictive - all flow errors should implement RetryableError.
impl<E> IntoFlowError for E
where
    E: std::error::Error + Serialize + crate::core::RetryableError,
{
    fn into_flow_error(self) -> FlowError {
        let is_retryable = self.is_retryable();
        if is_retryable {
            FlowError::retryable(self)
        } else {
            FlowError::terminal(self)
        }
    }
}

impl From<ExecutionError> for FlowError {
    fn from(err: ExecutionError) -> Self {
        err.into_flow_error()
    }
}

impl From<FlowError> for String {
    fn from(err: FlowError) -> Self {
        err.to_string()
    }
}

impl From<String> for FlowError {
    fn from(s: String) -> Self {
        FlowError {
            type_name: "String".to_string(),
            message: s.clone(),
            data: Some(serde_json::to_vec(&s).unwrap_or_default()),
            retryable: true,
        }
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

/// ExecutionError implements RetryableError to distinguish between
/// transient infrastructure errors (retryable) and permanent framework errors (non-retryable).
///
/// After removing suspension-as-error, ExecutionError only contains:
/// - Infrastructure errors (Storage, Core, Failed, SignalTimeout): transient, should retry
/// - Framework permanent errors (Incompatible, TaskPanic, Graph): permanent, should NOT retry
/// - Flow errors: delegates to FlowError's retryable field
impl crate::core::RetryableError for ExecutionError {
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

            // Flow errors - delegate to FlowError's retryable field
            ExecutionError::Flow(flow_err) => flow_err.retryable,
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
