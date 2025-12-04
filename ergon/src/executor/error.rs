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

    /// Flow execution failed with the given reason.
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

    /// Flow suspended, waiting for timer or signal.
    ///
    /// This is not an error - it indicates the flow should exit and be
    /// re-enqueued when the condition is met (timer fires, signal received).
    #[error("flow suspended")]
    Suspend(SuspendReason),
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
    Signal { flow_id: uuid::Uuid, step: i32, signal_name: String },
}

impl From<String> for ExecutionError {
    fn from(s: String) -> Self {
        ExecutionError::Failed(s)
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
