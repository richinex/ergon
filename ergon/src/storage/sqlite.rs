use super::{error::Result, error::StorageError, params::InvocationStartParams, ExecutionLog};
use crate::core::{hash_params, Invocation, InvocationStatus};
use async_trait::async_trait;
use chrono::Utc;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OptionalExtension};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info};
use uuid::Uuid;

/// Default pool size for SQLite connection pool.
const DEFAULT_POOL_SIZE: u32 = 10;

/// Default connection timeout in seconds.
const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 30;

/// Configuration for the SQLite connection pool.
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

        // Create flow queue table for distributed execution
        conn.execute(
            "CREATE TABLE IF NOT EXISTS flow_queue (
                task_id TEXT PRIMARY KEY,
                flow_id TEXT NOT NULL,
                flow_type TEXT NOT NULL,
                flow_data BLOB NOT NULL,
                status TEXT CHECK( status IN ('PENDING','RUNNING','COMPLETE','FAILED') ) NOT NULL,
                locked_by TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Create index for efficient pending flow lookups
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_flow_queue_status ON flow_queue(status, created_at)",
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

    fn row_to_scheduled_flow(row: &rusqlite::Row) -> rusqlite::Result<super::ScheduledFlow> {
        let task_id_str: String = row.get(0)?;
        let task_id = Uuid::parse_str(&task_id_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let flow_id_str: String = row.get(1)?;
        let flow_id = Uuid::parse_str(&flow_id_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
        })?;

        let flow_type: String = row.get(2)?;
        let flow_data: Vec<u8> = row.get(3)?;
        let status_str: String = row.get(4)?;
        let status = status_str.parse().map_err(|e: String| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Text,
                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            )
        })?;
        let locked_by: Option<String> = row.get(5)?;
        let created_at_millis: i64 = row.get(6)?;
        let created_at =
            chrono::DateTime::from_timestamp_millis(created_at_millis).unwrap_or_else(Utc::now);
        let updated_at_millis: i64 = row.get(7)?;
        let updated_at =
            chrono::DateTime::from_timestamp_millis(updated_at_millis).unwrap_or_else(Utc::now);

        Ok(super::ScheduledFlow {
            task_id,
            flow_id,
            flow_type,
            flow_data,
            status,
            locked_by,
            created_at,
            updated_at,
        })
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
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn close(&self) -> Result<()> {
        info!("Closing execution log database");
        Ok(())
    }

    async fn enqueue_flow(&self, flow: super::ScheduledFlow) -> Result<Uuid> {
        let conn = self.get_connection()?;
        let task_id = flow.task_id;

        tokio::task::spawn_blocking(move || {
            conn.execute(
                "INSERT INTO flow_queue (task_id, flow_id, flow_type, flow_data, status, locked_by, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    task_id.to_string(),
                    flow.flow_id.to_string(),
                    flow.flow_type,
                    flow.flow_data,
                    flow.status.as_str(),
                    flow.locked_by,
                    flow.created_at.timestamp_millis(),
                    flow.updated_at.timestamp_millis(),
                ],
            )?;

            debug!("Enqueued flow: task_id={}, flow_type={}", task_id, flow.flow_type);
            Ok(task_id)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<super::ScheduledFlow>> {
        let conn = self.get_connection()?;
        let worker_id = worker_id.to_string();

        tokio::task::spawn_blocking(move || {
            // Use a transaction to atomically find and lock a pending flow
            let tx = conn.unchecked_transaction()?;

            // Find the oldest pending flow
            let flow_opt = tx
                .query_row(
                    "SELECT task_id, flow_id, flow_type, flow_data, status, locked_by, created_at, updated_at
                     FROM flow_queue
                     WHERE status = 'PENDING'
                     ORDER BY created_at ASC
                     LIMIT 1",
                    [],
                    SqliteExecutionLog::row_to_scheduled_flow,
                )
                .optional()?;

            if let Some(flow) = flow_opt {
                // Lock it by updating status and locked_by
                tx.execute(
                    "UPDATE flow_queue
                     SET status = 'RUNNING', locked_by = ?, updated_at = ?
                     WHERE task_id = ?",
                    params![
                        &worker_id,
                        Utc::now().timestamp_millis(),
                        flow.task_id.to_string(),
                    ],
                )?;

                tx.commit()?;

                debug!(
                    "Dequeued flow: task_id={}, flow_type={}, worker={}",
                    flow.task_id, flow.flow_type, worker_id
                );

                // Return the flow with updated status
                Ok(Some(super::ScheduledFlow {
                    status: super::TaskStatus::Running,
                    locked_by: Some(worker_id),
                    updated_at: Utc::now(),
                    ..flow
                }))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn complete_flow(&self, task_id: Uuid, status: super::TaskStatus) -> Result<()> {
        let conn = self.get_connection()?;

        tokio::task::spawn_blocking(move || {
            let updated = conn.execute(
                "UPDATE flow_queue
                 SET status = ?, updated_at = ?
                 WHERE task_id = ?",
                params![
                    status.as_str(),
                    Utc::now().timestamp_millis(),
                    task_id.to_string(),
                ],
            )?;

            if updated == 0 {
                return Err(StorageError::ScheduledFlowNotFound(task_id));
            }

            debug!("Completed flow: task_id={}, status={}", task_id, status);
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn get_scheduled_flow(&self, task_id: Uuid) -> Result<Option<super::ScheduledFlow>> {
        let conn = self.get_connection()?;

        tokio::task::spawn_blocking(move || {
            let flow = conn
                .query_row(
                    "SELECT task_id, flow_id, flow_type, flow_data, status, locked_by, created_at, updated_at
                     FROM flow_queue
                     WHERE task_id = ?",
                    params![task_id.to_string()],
                    SqliteExecutionLog::row_to_scheduled_flow,
                )
                .optional()?;

            Ok(flow)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
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
