use thiserror::Error;
use uuid::Uuid;

/// Core error type for the rust_de durable execution engine.
///
/// This error type uses `thiserror` with proper `#[source]` annotations
/// to preserve error chains for debugging and error handling.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Serialization failed when encoding a value to bytes.
    #[error("serialization failed")]
    Serialization(#[source] bincode::error::EncodeError),

    /// Deserialization failed when decoding bytes to a value.
    #[error("deserialization failed")]
    Deserialization(#[source] bincode::error::DecodeError),

    /// The flow structure has changed incompatibly between executions.
    #[error("incompatible flow structure: expected {expected_class}.{expected_method}, got {actual_class}.{actual_method}")]
    IncompatibleFlowStructure {
        expected_class: String,
        expected_method: String,
        actual_class: String,
        actual_method: String,
    },

    /// The requested invocation was not found in storage.
    #[error("invocation not found: id={id}, step={step}")]
    InvocationNotFound { id: Uuid, step: i32 },

    /// The requested flow was not found in storage.
    #[error("flow not found: id={0}")]
    FlowNotFound(Uuid),

    /// An invalid status string was encountered during parsing.
    #[error("invalid invocation status: {0}")]
    InvalidStatus(String),

    /// General storage error.
    #[error("storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, Error>;
