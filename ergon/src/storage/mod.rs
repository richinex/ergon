use crate::core::{hash_params, Error as CoreError, Invocation, InvocationStatus};
use async_trait::async_trait;
use chrono::Utc;
use std::str::FromStr;
use thiserror::Error;
use tracing::{debug, info};
use uuid::Uuid;

#[cfg(feature = "sqlite")]
use r2d2::{Pool, PooledConnection};
#[cfg(feature = "sqlite")]
use r2d2_sqlite::SqliteConnectionManager;
#[cfg(feature = "sqlite")]
use rusqlite::{params, OptionalExtension};
#[cfg(feature = "sqlite")]
use std::path::Path;
#[cfg(feature = "sqlite")]
use std::time::Duration;

/// Default pool size for SQLite connection pool.
#[cfg(feature = "sqlite")]
const DEFAULT_POOL_SIZE: u32 = 10;

/// Default connection timeout in seconds.
#[cfg(feature = "sqlite")]
const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 30;

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
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Parameters for logging the start of a step invocation.
///
/// This struct groups all the parameters needed to log an invocation start,
/// making the API cleaner and more maintainable.
pub struct InvocationStartParams<'a> {
    /// Flow execution ID
    pub id: Uuid,
    /// Step number in the flow
    pub step: i32,
    /// Class name (for compatibility tracking)
    pub class_name: &'a str,
    /// Method name (for compatibility tracking)
    pub method_name: &'a str,
    /// Optional delay before execution
    pub delay: Option<Duration>,
    /// Current invocation status
    pub status: InvocationStatus,
    /// Serialized parameters
    pub parameters: &'a [u8],
}

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

// SQLite-specific implementation (optional feature)
/// Configuration for the SQLite connection pool.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool.
    pub max_size: u32,
    /// Minimum number of idle connections to maintain.
    pub min_idle: Option<u32>,
    /// Maximum time to wait for a connection from the pool.
    pub connection_timeout: Duration,
    /// Maximum lifetime of a connection.
    pub max_lifetime: Option<Duration>,
    /// Idle timeout for connections.
    pub idle_timeout: Option<Duration>,
}

#[cfg(feature = "sqlite")]
impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_POOL_SIZE,
            min_idle: Some(2),
            connection_timeout: Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT_SECS),
            max_lifetime: None,
            idle_timeout: Some(Duration::from_secs(600)), // 10 minutes
        }
    }
}

/// SQLite-based execution log with connection pooling.
///
/// This implementation uses r2d2 connection pooling to efficiently
/// manage multiple concurrent database connections. The async methods
/// use `spawn_blocking` internally to avoid blocking the async runtime.
#[cfg(feature = "sqlite")]
pub struct SqliteExecutionLog {
    pool: Pool<SqliteConnectionManager>,
    db_path: String,
}

impl SqliteExecutionLog {
    /// Creates a new SQLite execution log with the specified database path.
    ///
    /// Uses default pool configuration.
    pub fn new(db_path: impl AsRef<Path>) -> Result<Self> {
        Self::with_config(db_path, PoolConfig::default())
    }

    /// Creates a new SQLite execution log with custom pool configuration.
    pub fn with_config(db_path: impl AsRef<Path>, config: PoolConfig) -> Result<Self> {
        let db_path_str = db_path.as_ref().to_string_lossy().to_string();
        let manager = SqliteConnectionManager::file(&db_path_str);

        let pool = Self::build_pool(manager, &config)?;

        let log = Self {
            pool,
            db_path: db_path_str,
        };

        log.initialize()?;

        Ok(log)
    }

    /// Creates an in-memory SQLite execution log.
    ///
    /// Note: In-memory databases with connection pooling share the same
    /// database across all connections using a special URI.
    pub fn in_memory() -> Result<Self> {
        Self::in_memory_with_config(PoolConfig::default())
    }

    /// Creates an in-memory SQLite execution log with custom pool configuration.
    ///
    /// For in-memory databases, we use a single connection to ensure data consistency
    /// across the application. This is suitable for sequential flows but NOT for DAG
    /// flows which require concurrent write access. For DAG flows, use file-based storage.
    pub fn in_memory_with_config(config: PoolConfig) -> Result<Self> {
        // For in-memory, we use a single connection to ensure data consistency
        let mut in_memory_config = config;
        in_memory_config.max_size = 1;
        // Fix: Ensure min_idle doesn't exceed max_size
        in_memory_config.min_idle = Some(0);

        let manager = SqliteConnectionManager::memory();
        let pool = Self::build_pool(manager, &in_memory_config)?;

        let log = Self {
            pool,
            db_path: ":memory:".to_string(),
        };

        log.initialize()?;

        Ok(log)
    }

    /// Builds the connection pool with the given configuration.
    fn build_pool(
        manager: SqliteConnectionManager,
        config: &PoolConfig,
    ) -> Result<Pool<SqliteConnectionManager>> {
        let mut builder = Pool::builder()
            .max_size(config.max_size)
            .connection_timeout(config.connection_timeout);

        if let Some(min_idle) = config.min_idle {
            builder = builder.min_idle(Some(min_idle));
        }

        if let Some(max_lifetime) = config.max_lifetime {
            builder = builder.max_lifetime(Some(max_lifetime));
        }

        if let Some(idle_timeout) = config.idle_timeout {
            builder = builder.idle_timeout(Some(idle_timeout));
        }

        let pool = builder.build(manager)?;
        Ok(pool)
    }

    /// Initialize the database schema and settings.
    fn initialize(&self) -> Result<()> {
        let conn = self.pool.get()?;

        // Configure SQLite for optimal concurrent access
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;

        // Create table with indexes for efficient queries
        conn.execute(
            "CREATE TABLE IF NOT EXISTS execution_log (
                id TEXT NOT NULL,
                step INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                class_name TEXT NOT NULL,
                method_name TEXT NOT NULL,
                delay INTEGER,
                status TEXT CHECK( status IN ('PENDING','WAITING_FOR_SIGNAL','COMPLETE') ) NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 1,
                parameters BLOB,
                params_hash INTEGER NOT NULL DEFAULT 0,
                return_value BLOB,
                PRIMARY KEY (id, step)
            )",
            [],
        )?;

        // Add params_hash column if it doesn't exist (migration for existing databases)
        let _ = conn.execute(
            "ALTER TABLE execution_log ADD COLUMN params_hash INTEGER NOT NULL DEFAULT 0",
            [],
        );

        // Create index for efficient flow lookups
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_execution_log_id ON execution_log(id)",
            [],
        )?;

        // Create index for incomplete flow queries
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_execution_log_status ON execution_log(step, status)",
            [],
        )?;

        Ok(())
    }

    /// Get a connection from the pool.
    fn get_connection(&self) -> Result<PooledConnection<SqliteConnectionManager>> {
        self.pool.get().map_err(StorageError::Pool)
    }

    /// Returns the current pool state for monitoring.
    pub fn pool_state(&self) -> r2d2::State {
        self.pool.state()
    }

    /// Returns the database path.
    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    fn row_to_invocation(row: &rusqlite::Row) -> rusqlite::Result<Invocation> {
        let id_str: String = row.get(0)?;
        let id = Uuid::parse_str(&id_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let step: i32 = row.get(1)?;
        let timestamp_millis: i64 = row.get(2)?;
        let timestamp =
            chrono::DateTime::from_timestamp_millis(timestamp_millis).unwrap_or_else(Utc::now);
        let class_name: String = row.get(3)?;
        let method_name: String = row.get(4)?;
        let status_str: String = row.get(5)?;
        let status = InvocationStatus::from_str(&status_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            )
        })?;
        let attempts: i32 = row.get(6)?;
        let parameters: Vec<u8> = row.get(7)?;
        let params_hash: i64 = row.get(8)?;
        let return_value: Option<Vec<u8>> = row.get(9)?;
        let delay: Option<i64> = row.get(10)?;

        Ok(Invocation::new(
            id,
            step,
            timestamp,
            class_name,
            method_name,
            status,
            attempts,
            parameters,
            params_hash as u64,
            return_value,
            delay,
        ))
    }
}

#[async_trait]
impl ExecutionLog for SqliteExecutionLog {
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()> {
        // Destructure params
        let InvocationStartParams {
            id,
            step,
            class_name,
            method_name,
            delay,
            status,
            parameters,
        } = params;

        // Clone data for move into spawn_blocking
        let class_name = class_name.to_string();
        let method_name = method_name.to_string();
        let parameters = parameters.to_vec();
        // Compute params_hash internally from the parameters bytes
        let params_hash = hash_params(&parameters);

        // Get a connection from the pool (this is fast)
        let conn = self.get_connection()?;

        // Use spawn_blocking to avoid blocking the async runtime
        let result = tokio::task::spawn_blocking(move || {
            let delay_millis = delay.map(|d| d.as_millis() as i64);

            conn.execute(
                "INSERT INTO execution_log (id, step, timestamp, class_name, method_name, delay, status, attempts, parameters, params_hash)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(id, step)
                 DO UPDATE SET attempts = attempts + 1",
                params![
                    id.to_string(),
                    step,
                    Utc::now().timestamp_millis(),
                    class_name,
                    method_name,
                    delay_millis,
                    status.as_str(),
                    1,
                    parameters,
                    params_hash as i64,
                ],
            )?;

            debug!(
                "Logged invocation start: id={}, step={}, class={}, method={}, params_hash={}",
                id, step, class_name, method_name, params_hash
            );

            Ok::<(), StorageError>(())
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))??;

        Ok(result)
    }

    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation> {
        let return_value = return_value.to_vec();
        let conn = self.get_connection()?;

        tokio::task::spawn_blocking(move || {
            conn.execute(
                "UPDATE execution_log
                 SET status = 'COMPLETE', return_value = ?
                 WHERE id = ? AND step = ?",
                params![return_value, id.to_string(), step],
            )?;

            let invocation = conn
                .query_row(
                    "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay
                     FROM execution_log
                     WHERE id = ? AND step = ?",
                    params![id.to_string(), step],
                    SqliteExecutionLog::row_to_invocation,
                )
                .optional()?
                .ok_or_else(|| StorageError::InvocationNotFound { id, step })?;

            debug!(
                "Logged invocation completion: id={}, step={}, status={:?}",
                id,
                step,
                invocation.status()
            );

            Ok(invocation)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>> {
        let conn = self.get_connection()?;

        tokio::task::spawn_blocking(move || {
            let invocation = conn
                .query_row(
                    "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay
                     FROM execution_log
                     WHERE id = ? AND step = ?",
                    params![id.to_string(), step],
                    SqliteExecutionLog::row_to_invocation,
                )
                .optional()?;

            Ok(invocation)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>> {
        let conn = self.get_connection()?;

        tokio::task::spawn_blocking(move || {
            let invocation = conn
                .query_row(
                    "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay
                     FROM execution_log
                     WHERE id = ?
                     ORDER BY step DESC
                     LIMIT 1",
                    params![id.to_string()],
                    SqliteExecutionLog::row_to_invocation,
                )
                .optional()?;

            Ok(invocation)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>> {
        let conn = self.get_connection()?;

        tokio::task::spawn_blocking(move || {
            let mut stmt = conn.prepare(
                "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay
                 FROM execution_log
                 WHERE id = ?
                 ORDER BY step ASC",
            )?;

            let invocations = stmt
                .query_map(params![id.to_string()], SqliteExecutionLog::row_to_invocation)?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            Ok(invocations)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>> {
        let conn = self.get_connection()?;

        tokio::task::spawn_blocking(move || {
            let mut stmt = conn.prepare(
                "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay
                 FROM execution_log
                 WHERE step = 0
                   AND status <> 'COMPLETE'
                 ORDER BY timestamp ASC",
            )?;

            let invocations = stmt
                .query_map([], SqliteExecutionLog::row_to_invocation)?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            info!("Found {} incomplete flows", invocations.len());

            Ok(invocations)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn reset(&self) -> Result<()> {
        let conn = self.get_connection()?;

        tokio::task::spawn_blocking(move || {
            conn.execute("DELETE FROM execution_log", [])?;
            info!("Reset execution log database");
            Ok(())
        })
        .await
        .map_err(|e| {
            StorageError::Io(std::io::Error::other(
                e.to_string(),
            ))
        })?
    }

    async fn close(&self) -> Result<()> {
        info!("Closing execution log database");
        Ok(())
    }
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

/// In-memory execution log using DashMap for concurrent access.
///
/// This implementation uses DashMap, a lock-free concurrent HashMap,
/// to provide thread-safe in-memory storage without the single-write
/// limitation of SQLite's shared cache mode. This is suitable for
/// both sequential and parallel DAG execution.
///
/// Reference: https://github.com/xacrimon/dashmap
/// DashMap provides concurrent access through sharding, eliminating
/// the bottleneck of a single mutex.
pub struct InMemoryExecutionLog {
    /// Concurrent storage for invocations keyed by (flow_id, step)
    invocations: dashmap::DashMap<(Uuid, i32), Invocation>,
}

impl InMemoryExecutionLog {
    /// Creates a new in-memory execution log.
    pub fn new() -> Self {
        Self {
            invocations: dashmap::DashMap::new(),
        }
    }
}

impl Default for InMemoryExecutionLog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExecutionLog for InMemoryExecutionLog {
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()> {
        // Destructure params
        let InvocationStartParams {
            id,
            step,
            class_name,
            method_name,
            delay,
            status,
            parameters,
        } = params;

        let params_hash = hash_params(parameters);
        let delay_ms = delay.map(|d| d.as_millis() as i64);

        let invocation = Invocation::new(
            id,
            step,
            Utc::now(),
            class_name.to_string(),
            method_name.to_string(),
            status,
            1,
            parameters.to_vec(),
            params_hash,
            None,
            delay_ms,
        );

        let key = (id, step);
        self.invocations.insert(key, invocation);
        Ok(())
    }

    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation> {
        let key = (id, step);
        if let Some(mut entry) = self.invocations.get_mut(&key) {
            let delay_ms = entry.delay().map(|d| d.as_millis() as i64);

            let invocation = Invocation::new(
                entry.id(),
                entry.step(),
                entry.timestamp(),
                entry.class_name().to_string(),
                entry.method_name().to_string(),
                InvocationStatus::Complete,
                entry.attempts(),
                entry.parameters().to_vec(),
                entry.params_hash(),
                Some(return_value.to_vec()),
                delay_ms,
            );
            *entry = invocation.clone();
            Ok(invocation)
        } else {
            Err(StorageError::InvocationNotFound { id, step })
        }
    }

    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>> {
        let key = (id, step);
        Ok(self
            .invocations
            .get(&key)
            .map(|entry| entry.value().clone()))
    }

    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>> {
        let invocations = self.get_invocations_for_flow(id).await?;
        Ok(invocations.into_iter().max_by_key(|inv| inv.step()))
    }

    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>> {
        let mut invocations: Vec<Invocation> = self
            .invocations
            .iter()
            .filter(|entry| entry.key().0 == id)
            .map(|entry| entry.value().clone())
            .collect();

        invocations.sort_by_key(|inv| inv.step());
        Ok(invocations)
    }

    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>> {
        let invocations: Vec<Invocation> = self
            .invocations
            .iter()
            .filter(|entry| entry.value().status() != InvocationStatus::Complete)
            .map(|entry| entry.value().clone())
            .collect();

        Ok(invocations)
    }

    async fn reset(&self) -> Result<()> {
        self.invocations.clear();
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{hash_params, serialize_value};

    #[tokio::test]
    async fn test_create_and_log_invocation() {
        let log = SqliteExecutionLog::in_memory().unwrap();
        let id = Uuid::new_v4();

        let params = serialize_value(&vec!["test".to_string()]).unwrap();
        let expected_hash = hash_params(&params);

        log.log_invocation_start(InvocationStartParams {
            id,
            step: 0,
            class_name: "TestClass",
            method_name: "testMethod",
            delay: None,
            status: InvocationStatus::Pending,
            parameters: &params,
        })
        .await
        .unwrap();

        let invocation = log.get_invocation(id, 0).await.unwrap().unwrap();
        assert_eq!(invocation.id(), id);
        assert_eq!(invocation.step(), 0);
        assert_eq!(invocation.class_name(), "TestClass");
        assert_eq!(invocation.method_name(), "testMethod");
        assert_eq!(invocation.status(), InvocationStatus::Pending);
        assert_eq!(invocation.attempts(), 1);
        // Verify the hash was computed internally by storage
        assert_eq!(invocation.params_hash(), expected_hash);
    }

    #[tokio::test]
    async fn test_log_completion() {
        let log = SqliteExecutionLog::in_memory().unwrap();
        let id = Uuid::new_v4();

        let params = serialize_value(&vec!["test".to_string()]).unwrap();
        log.log_invocation_start(InvocationStartParams {
            id,
            step: 0,
            class_name: "TestClass",
            method_name: "testMethod",
            delay: None,
            status: InvocationStatus::Pending,
            parameters: &params,
        })
        .await
        .unwrap();

        let return_val = serialize_value(&42i32).unwrap();
        let invocation = log
            .log_invocation_completion(id, 0, &return_val)
            .await
            .unwrap();

        assert_eq!(invocation.status(), InvocationStatus::Complete);
        assert!(invocation.return_value().is_some());
    }

    #[tokio::test]
    async fn test_get_incomplete_flows() {
        let log = SqliteExecutionLog::in_memory().unwrap();

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        let params = serialize_value(&vec!["test".to_string()]).unwrap();

        log.log_invocation_start(InvocationStartParams {
            id: id1,
            step: 0,
            class_name: "Flow1",
            method_name: "run",
            delay: None,
            status: InvocationStatus::Pending,
            parameters: &params,
        })
        .await
        .unwrap();

        log.log_invocation_start(InvocationStartParams {
            id: id2,
            step: 0,
            class_name: "Flow2",
            method_name: "run",
            delay: None,
            status: InvocationStatus::WaitingForSignal,
            parameters: &params,
        })
        .await
        .unwrap();

        log.log_invocation_start(InvocationStartParams {
            id: id3,
            step: 0,
            class_name: "Flow3",
            method_name: "run",
            delay: None,
            status: InvocationStatus::Complete,
            parameters: &params,
        })
        .await
        .unwrap();

        let incomplete = log.get_incomplete_flows().await.unwrap();
        assert_eq!(incomplete.len(), 2);
        assert!(incomplete.iter().any(|i| i.id() == id1));
        assert!(incomplete.iter().any(|i| i.id() == id2));
        assert!(!incomplete.iter().any(|i| i.id() == id3));
    }

    #[tokio::test]
    async fn test_retry_increments_attempts() {
        let log = SqliteExecutionLog::in_memory().unwrap();
        let id = Uuid::new_v4();

        let params = serialize_value(&vec!["test".to_string()]).unwrap();

        log.log_invocation_start(InvocationStartParams {
            id,
            step: 0,
            class_name: "TestClass",
            method_name: "testMethod",
            delay: None,
            status: InvocationStatus::Pending,
            parameters: &params,
        })
        .await
        .unwrap();

        let inv1 = log.get_invocation(id, 0).await.unwrap().unwrap();
        assert_eq!(inv1.attempts(), 1);

        log.log_invocation_start(InvocationStartParams {
            id,
            step: 0,
            class_name: "TestClass",
            method_name: "testMethod",
            delay: None,
            status: InvocationStatus::Pending,
            parameters: &params,
        })
        .await
        .unwrap();

        let inv2 = log.get_invocation(id, 0).await.unwrap().unwrap();
        assert_eq!(inv2.attempts(), 2);
    }

    #[tokio::test]
    async fn test_get_latest_invocation() {
        let log = SqliteExecutionLog::in_memory().unwrap();
        let id = Uuid::new_v4();

        let params = serialize_value(&vec!["test".to_string()]).unwrap();

        for step in 0..5 {
            log.log_invocation_start(InvocationStartParams {
                id,
                step,
                class_name: "TestClass",
                method_name: "testMethod",
                delay: None,
                status: InvocationStatus::Complete,
                parameters: &params,
            })
            .await
            .unwrap();
        }

        let latest = log.get_latest_invocation(id).await.unwrap().unwrap();
        assert_eq!(latest.step(), 4);
    }

    #[tokio::test]
    async fn test_pool_config() {
        let config = PoolConfig {
            max_size: 5,
            min_idle: Some(1),
            connection_timeout: Duration::from_secs(10),
            max_lifetime: None,
            idle_timeout: Some(Duration::from_secs(300)),
        };

        // For in-memory, pool size is forced to 1, but we can still create with config
        let log = SqliteExecutionLog::in_memory_with_config(config).unwrap();
        let state = log.pool_state();
        assert_eq!(state.connections, 1); // In-memory uses single connection
    }
}
