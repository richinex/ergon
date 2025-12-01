use crate::core::Error as CoreError;
use thiserror::Error;
use uuid::Uuid;

/// Storage layer error type for the rust_de durable execution engine.
///
/// This error type wraps underlying storage and serialization errors
/// while preserving the full error chain for debugging.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StorageError {
    /// A database operation failed.
    #[error("database operation failed")]
    Database(#[from] rusqlite::Error),

    /// A core serialization or deserialization error occurred.
    #[error("core error: {0}")]
    Core(#[from] CoreError),

    /// An I/O operation failed.
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    /// Failed to get a connection from the pool.
    #[error("connection pool error: {0}")]
    Pool(#[from] r2d2::Error),

    /// The requested invocation was not found in storage.
    #[error("invocation not found: id={id}, step={step}")]
    InvocationNotFound { id: Uuid, step: i32 },

    /// The operation is not supported by this storage backend.
    #[error("operation not supported: {0}")]
    Unsupported(String),

    /// The scheduled flow was not found in storage.
    #[error("scheduled flow not found: id={0}")]
    ScheduledFlowNotFound(Uuid),

    /// A connection error occurred (e.g., Redis connection failure).
    #[error("connection error: {0}")]
    Connection(String),

    /// A serialization/deserialization error occurred.
    #[error("serialization error")]
    Serialization,
}

pub type Result<T> = std::result::Result<T, StorageError>;
