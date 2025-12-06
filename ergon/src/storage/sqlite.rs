use super::{error::Result, error::StorageError, params::InvocationStartParams, ExecutionLog};
use crate::core::{hash_params, Invocation, InvocationStatus};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Pool, Row, Sqlite, SqlitePool};
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
/// This implementation uses sqlx connection pooling to efficiently
/// manage multiple concurrent database connections. All methods are
/// natively async without `spawn_blocking` overhead.
pub struct SqliteExecutionLog {
    pool: SqlitePool,
    db_path: String,
}

impl SqliteExecutionLog {
    /// Creates a new SQLite execution log with the specified database path.
    ///
    /// Uses default pool configuration.
    pub async fn new(db_path: impl AsRef<Path>) -> Result<Self> {
        Self::with_config(db_path, PoolConfig::default()).await
    }

    /// Creates a new SQLite execution log with custom pool configuration.
    pub async fn with_config(db_path: impl AsRef<Path>, config: PoolConfig) -> Result<Self> {
        let db_path_str = db_path.as_ref().to_string_lossy().to_string();

        // Configure SQLite connection options for optimal concurrent access
        let connect_options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path_str))
            .map_err(|e| StorageError::Connection(e.to_string()))?
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5))
            .create_if_missing(true);

        let pool = Self::build_pool(connect_options, &config).await?;

        let log = Self {
            pool,
            db_path: db_path_str,
        };

        log.initialize().await?;

        Ok(log)
    }

    /// Creates an in-memory SQLite execution log.
    ///
    /// Note: In-memory databases with connection pooling share the same
    /// database across all connections using a special URI.
    pub async fn in_memory() -> Result<Self> {
        Self::in_memory_with_config(PoolConfig::default()).await
    }

    /// Creates an in-memory SQLite execution log with custom pool configuration.
    ///
    /// For in-memory databases, we use a single connection to ensure data consistency
    /// across the application. This is suitable for sequential flows but NOT for DAG
    /// flows which require concurrent write access. For DAG flows, use file-based storage.
    pub async fn in_memory_with_config(config: PoolConfig) -> Result<Self> {
        // For in-memory, we use a single connection to ensure data consistency
        let mut in_memory_config = config;
        in_memory_config.max_size = 1;
        in_memory_config.min_idle = Some(0);

        let connect_options = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|e| StorageError::Connection(e.to_string()))?
            .journal_mode(SqliteJournalMode::Memory)
            .synchronous(SqliteSynchronous::Normal);

        let pool = Self::build_pool(connect_options, &in_memory_config).await?;

        let log = Self {
            pool,
            db_path: ":memory:".to_string(),
        };

        log.initialize().await?;

        Ok(log)
    }

    /// Builds the connection pool with the given configuration.
    async fn build_pool(
        connect_options: SqliteConnectOptions,
        config: &PoolConfig,
    ) -> Result<Pool<Sqlite>> {
        let mut builder = SqlitePoolOptions::new()
            .max_connections(config.max_size)
            .acquire_timeout(config.connection_timeout);

        if let Some(min_idle) = config.min_idle {
            builder = builder.min_connections(min_idle);
        }

        if let Some(max_lifetime) = config.max_lifetime {
            builder = builder.max_lifetime(max_lifetime);
        }

        if let Some(idle_timeout) = config.idle_timeout {
            builder = builder.idle_timeout(idle_timeout);
        }

        let pool = builder
            .connect_with(connect_options)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(pool)
    }

    /// Initialize the database schema and settings.
    async fn initialize(&self) -> Result<()> {
        // Create table with complete schema (no migrations needed for new project)
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS execution_log (
                id TEXT NOT NULL,
                step INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                class_name TEXT NOT NULL,
                method_name TEXT NOT NULL,
                delay INTEGER,
                status TEXT CHECK( status IN ('PENDING','WAITING_FOR_SIGNAL','WAITING_FOR_TIMER','COMPLETE') ) NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 1,
                parameters BLOB,
                params_hash INTEGER NOT NULL DEFAULT 0,
                return_value BLOB,
                retry_policy TEXT,
                is_retryable INTEGER,
                timer_fire_at INTEGER,
                timer_name TEXT,
                PRIMARY KEY (id, step)
            )",
        )
        .execute(&self.pool)
        .await?;

        // Create index for efficient flow lookups
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_execution_log_id ON execution_log(id)")
            .execute(&self.pool)
            .await?;

        // Create index for incomplete flow queries
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_execution_log_status ON execution_log(step, status)",
        )
        .execute(&self.pool)
        .await?;

        // Create index for timer queries (find expired timers)
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_execution_log_timers ON execution_log(status, timer_fire_at)",
        )
        .execute(&self.pool)
        .await?;

        // Create flow queue table for distributed execution
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS flow_queue (
                task_id TEXT PRIMARY KEY,
                flow_id TEXT NOT NULL,
                flow_type TEXT NOT NULL,
                flow_data BLOB NOT NULL,
                status TEXT CHECK( status IN ('PENDING','RUNNING','SUSPENDED','COMPLETE','FAILED') ) NOT NULL,
                parent_flow_id TEXT,
                signal_token TEXT,
                locked_by TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                scheduled_for INTEGER
            )",
        )
        .execute(&self.pool)
        .await?;

        // Create index for efficient pending flow lookups
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_flow_queue_status ON flow_queue(status, created_at)",
        )
        .execute(&self.pool)
        .await?;

        // Create signal parameters table for durable signals
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS signal_params (
                flow_id TEXT NOT NULL,
                step INTEGER NOT NULL,
                params BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (flow_id, step)
            )",
        )
        .execute(&self.pool)
        .await?;

        // Create index for signal parameter cleanup
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_signal_params_created ON signal_params(created_at)",
        )
        .execute(&self.pool)
        .await?;

        // Create step-child mapping table for framework fix
        // Tracks which child invocation step belongs to which parent step
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS step_child_mappings (
                flow_id TEXT NOT NULL,
                parent_step INTEGER NOT NULL,
                child_step INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (flow_id, parent_step)
            )",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Returns the database path.
    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    /// Returns the current pool state for monitoring.
    pub fn pool_size(&self) -> u32 {
        self.pool.size()
    }

    /// Returns the number of idle connections.
    pub fn idle_connections(&self) -> usize {
        self.pool.num_idle()
    }

    fn row_to_scheduled_flow(row: &sqlx::sqlite::SqliteRow) -> Result<super::ScheduledFlow> {
        let task_id_str: String = row.try_get("task_id")?;
        let task_id =
            Uuid::parse_str(&task_id_str).map_err(|e| StorageError::Connection(e.to_string()))?;

        let flow_id_str: String = row.try_get("flow_id")?;
        let flow_id =
            Uuid::parse_str(&flow_id_str).map_err(|e| StorageError::Connection(e.to_string()))?;

        let flow_type: String = row.try_get("flow_type")?;
        let flow_data: Vec<u8> = row.try_get("flow_data")?;
        let status_str: String = row.try_get("status")?;
        let status = status_str
            .parse()
            .map_err(|e: String| StorageError::Connection(e))?;
        let locked_by: Option<String> = row.try_get("locked_by")?;
        let created_at_millis: i64 = row.try_get("created_at")?;
        let created_at =
            chrono::DateTime::from_timestamp_millis(created_at_millis).unwrap_or_else(Utc::now);
        let updated_at_millis: i64 = row.try_get("updated_at")?;
        let updated_at =
            chrono::DateTime::from_timestamp_millis(updated_at_millis).unwrap_or_else(Utc::now);

        let retry_count: u32 = row.try_get("retry_count").unwrap_or(0);
        let error_message: Option<String> = row.try_get("error_message").ok();
        let scheduled_for_millis: Option<i64> = row.try_get("scheduled_for").ok();
        let scheduled_for = scheduled_for_millis.and_then(chrono::DateTime::from_timestamp_millis);

        // Extract Level 3 parent metadata if present
        let parent_flow_id: Option<String> = row.try_get("parent_flow_id").ok();
        let parent_flow_id = parent_flow_id.and_then(|s| uuid::Uuid::parse_str(&s).ok());
        let signal_token: Option<String> = row.try_get("signal_token").ok();

        Ok(super::ScheduledFlow {
            task_id,
            flow_id,
            flow_type,
            flow_data,
            status,
            locked_by,
            created_at,
            updated_at,
            retry_count,
            error_message,
            scheduled_for,
            parent_flow_id,
            signal_token,
        })
    }

    fn row_to_invocation(row: &sqlx::sqlite::SqliteRow) -> Result<Invocation> {
        let id_str: String = row.try_get("id")?;
        let id = Uuid::parse_str(&id_str).map_err(|e| StorageError::Connection(e.to_string()))?;

        let step: i32 = row.try_get("step")?;
        let timestamp_millis: i64 = row.try_get("timestamp")?;
        let timestamp =
            chrono::DateTime::from_timestamp_millis(timestamp_millis).unwrap_or_else(Utc::now);
        let class_name: String = row.try_get("class_name")?;
        let method_name: String = row.try_get("method_name")?;
        let status_str: String = row.try_get("status")?;
        let status = InvocationStatus::from_str(&status_str)
            .map_err(|e| StorageError::Connection(e.to_string()))?;
        let attempts: i32 = row.try_get("attempts")?;
        let parameters: Vec<u8> = row.try_get("parameters")?;
        let params_hash: i64 = row.try_get("params_hash")?;
        let return_value: Option<Vec<u8>> = row.try_get("return_value")?;
        let delay: Option<i64> = row.try_get("delay")?;

        // Parse retry_policy from JSON if present
        let retry_policy_json: Option<String> = row.try_get("retry_policy")?;
        let retry_policy = retry_policy_json.and_then(|json| serde_json::from_str(&json).ok());

        // Parse is_retryable (NULL, 0, or 1)
        let is_retryable_int: Option<i32> = row.try_get("is_retryable").ok();
        let is_retryable = is_retryable_int.map(|v| v != 0);

        // Parse timer_fire_at from milliseconds since epoch
        let timer_fire_at_millis: Option<i64> = row.try_get("timer_fire_at").ok();
        let timer_fire_at = timer_fire_at_millis.and_then(chrono::DateTime::from_timestamp_millis);

        // Parse timer_name
        let timer_name: Option<String> = row.try_get("timer_name").ok();

        let mut invocation = Invocation::new(
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
            retry_policy,
            is_retryable,
        );

        // Set timer fields if present
        if let Some(fire_at) = timer_fire_at {
            invocation.set_timer_fire_at(Some(fire_at));
        }
        if let Some(name) = timer_name {
            invocation.set_timer_name(Some(name));
        }

        Ok(invocation)
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
            retry_policy,
        } = params;

        // Compute params_hash internally from the parameters bytes
        let params_hash = hash_params(parameters);

        // Serialize retry_policy to JSON if present
        let retry_policy_json = retry_policy.and_then(|p| serde_json::to_string(&p).ok());

        let delay_millis = delay.map(|d| d.as_millis() as i64);

        sqlx::query(
            "INSERT INTO execution_log (id, step, timestamp, class_name, method_name, delay, status, attempts, parameters, params_hash, retry_policy)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(id, step)
             DO UPDATE SET attempts = attempts + 1",
        )
        .bind(id.to_string())
        .bind(step)
        .bind(Utc::now().timestamp_millis())
        .bind(class_name)
        .bind(method_name)
        .bind(delay_millis)
        .bind(status.as_str())
        .bind(1)
        .bind(parameters)
        .bind(params_hash as i64)
        .bind(retry_policy_json)
        .execute(&self.pool)
        .await?;

        debug!(
            "Logged invocation start: id={}, step={}, class={}, method={}, params_hash={}",
            id, step, class_name, method_name, params_hash
        );

        Ok(())
    }

    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation> {
        sqlx::query(
            "UPDATE execution_log
             SET status = 'COMPLETE', return_value = ?
             WHERE id = ? AND step = ?",
        )
        .bind(return_value)
        .bind(id.to_string())
        .bind(step)
        .execute(&self.pool)
        .await?;

        let invocation = sqlx::query(
            "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay, retry_policy, is_retryable, timer_fire_at, timer_name
             FROM execution_log
             WHERE id = ? AND step = ?",
        )
        .bind(id.to_string())
        .bind(step)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| Self::row_to_invocation(&row))
        .transpose()?
        .ok_or_else(|| StorageError::InvocationNotFound { id, step })?;

        debug!(
            "Logged invocation completion: id={}, step={}, status={:?}",
            id,
            step,
            invocation.status()
        );

        Ok(invocation)
    }

    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>> {
        let invocation = sqlx::query(
            "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay, retry_policy, is_retryable, timer_fire_at, timer_name
             FROM execution_log
             WHERE id = ? AND step = ?",
        )
        .bind(id.to_string())
        .bind(step)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| Self::row_to_invocation(&row))
        .transpose()?;

        Ok(invocation)
    }

    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>> {
        let invocation = sqlx::query(
            "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay, retry_policy, is_retryable, timer_fire_at, timer_name
             FROM execution_log
             WHERE id = ?
             ORDER BY step DESC
             LIMIT 1",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .map(|row| Self::row_to_invocation(&row))
        .transpose()?;

        Ok(invocation)
    }

    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>> {
        let rows = sqlx::query(
            "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay, retry_policy, is_retryable, timer_fire_at, timer_name
             FROM execution_log
             WHERE id = ?
             ORDER BY step ASC",
        )
        .bind(id.to_string())
        .fetch_all(&self.pool)
        .await?;

        let invocations = rows
            .iter()
            .map(Self::row_to_invocation)
            .collect::<Result<Vec<_>>>()?;

        Ok(invocations)
    }

    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>> {
        let rows = sqlx::query(
            "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay, retry_policy, is_retryable, timer_fire_at, timer_name
             FROM execution_log
             WHERE step = 0
               AND status <> 'COMPLETE'
             ORDER BY timestamp ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        let invocations = rows
            .iter()
            .map(Self::row_to_invocation)
            .collect::<Result<Vec<_>>>()?;

        info!("Found {} incomplete flows", invocations.len());

        Ok(invocations)
    }

    async fn has_non_retryable_error(&self, flow_id: Uuid) -> Result<bool> {
        // Check if any step in the flow has is_retryable = 0 (non-retryable error)
        let has_non_retryable: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                     SELECT 1 FROM execution_log
                     WHERE id = ? AND is_retryable = 0
                 )",
        )
        .bind(flow_id.to_string())
        .fetch_one(&self.pool)
        .await?;

        debug!(
            "Flow {} has non-retryable error: {}",
            flow_id, has_non_retryable
        );

        Ok(has_non_retryable)
    }

    async fn update_is_retryable(&self, id: Uuid, step: i32, is_retryable: bool) -> Result<()> {
        sqlx::query(
            "UPDATE execution_log
             SET is_retryable = ?
             WHERE id = ? AND step = ?",
        )
        .bind(if is_retryable { 1 } else { 0 })
        .bind(id.to_string())
        .bind(step)
        .execute(&self.pool)
        .await?;

        debug!(
            "Updated is_retryable for flow {} step {}: {}",
            id, step, is_retryable
        );

        Ok(())
    }

    async fn reset(&self) -> Result<()> {
        sqlx::query("DELETE FROM execution_log")
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM flow_queue")
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM signal_params")
            .execute(&self.pool)
            .await?;
        sqlx::query("DELETE FROM step_child_mappings")
            .execute(&self.pool)
            .await?;
        info!("Reset execution log database (cleared execution_log, flow_queue, signal_params, step_child_mappings)");
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.pool.close().await;
        info!("Closing execution log database");
        Ok(())
    }

    async fn enqueue_flow(&self, flow: super::ScheduledFlow) -> Result<Uuid> {
        let task_id = flow.task_id;
        let flow_type = flow.flow_type.clone();

        sqlx::query(
            "INSERT INTO flow_queue (task_id, flow_id, flow_type, flow_data, status, locked_by, created_at, updated_at, retry_count, error_message, scheduled_for, parent_flow_id, signal_token)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(task_id.to_string())
        .bind(flow.flow_id.to_string())
        .bind(flow.flow_type)
        .bind(flow.flow_data)
        .bind(flow.status.as_str())
        .bind(flow.locked_by)
        .bind(flow.created_at.timestamp_millis())
        .bind(flow.updated_at.timestamp_millis())
        .bind(flow.retry_count as i64)
        .bind(flow.error_message)
        .bind(flow.scheduled_for.map(|dt| dt.timestamp_millis()))
        .bind(flow.parent_flow_id.map(|id| id.to_string()))
        .bind(flow.signal_token)
        .execute(&self.pool)
        .await?;

        debug!(
            "Enqueued flow: task_id={}, flow_type={}",
            task_id, flow_type
        );
        Ok(task_id)
    }

    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<super::ScheduledFlow>> {
        // Single atomic operation using UPDATE...RETURNING (SQLite 3.35+)
        // This eliminates the race condition where multiple workers SELECT the same row
        let now = Utc::now().timestamp_millis();

        let flow_opt = sqlx::query(
            "UPDATE flow_queue
             SET status = 'RUNNING', locked_by = ?1, updated_at = ?2
             WHERE task_id = (
                 SELECT task_id FROM flow_queue
                 WHERE status = 'PENDING'
                   AND (scheduled_for IS NULL OR scheduled_for <= ?3)
                 ORDER BY created_at ASC
                 LIMIT 1
             )
             RETURNING task_id, flow_id, flow_type, flow_data, status, locked_by,
                       created_at, updated_at, retry_count, error_message,
                       scheduled_for, parent_flow_id, signal_token",
        )
        .bind(worker_id)
        .bind(now)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| Self::row_to_scheduled_flow(&row))
        .transpose()?;

        if let Some(flow) = &flow_opt {
            debug!(
                "Dequeued flow: task_id={}, flow_type={}, worker={}",
                flow.task_id, flow.flow_type, worker_id
            );
        }

        Ok(flow_opt)
    }

    async fn complete_flow(&self, task_id: Uuid, status: super::TaskStatus) -> Result<()> {
        let result = sqlx::query(
            "UPDATE flow_queue
             SET status = ?, updated_at = ?
             WHERE task_id = ?",
        )
        .bind(status.as_str())
        .bind(Utc::now().timestamp_millis())
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(StorageError::ScheduledFlowNotFound(task_id));
        }

        debug!("Completed flow: task_id={}, status={}", task_id, status);
        Ok(())
    }

    async fn retry_flow(
        &self,
        task_id: Uuid,
        error_message: String,
        delay: std::time::Duration,
    ) -> Result<()> {
        // Calculate scheduled_for timestamp (current time + delay)
        let scheduled_for = Utc::now() + chrono::Duration::from_std(delay).unwrap();

        let result = sqlx::query(
            "UPDATE flow_queue
             SET retry_count = retry_count + 1,
                 error_message = ?,
                 status = ?,
                 locked_by = NULL,
                 scheduled_for = ?,
                 updated_at = ?
             WHERE task_id = ?",
        )
        .bind(error_message)
        .bind(super::TaskStatus::Pending.as_str())
        .bind(scheduled_for.timestamp_millis())
        .bind(Utc::now().timestamp_millis())
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(StorageError::ScheduledFlowNotFound(task_id));
        }

        debug!("Retried flow: task_id={}, retry_count incremented", task_id);
        Ok(())
    }

    async fn get_scheduled_flow(&self, task_id: Uuid) -> Result<Option<super::ScheduledFlow>> {
        let flow = sqlx::query(
            "SELECT task_id, flow_id, flow_type, flow_data, status, locked_by, created_at, updated_at,
                    retry_count, error_message, scheduled_for, parent_flow_id, signal_token
             FROM flow_queue
             WHERE task_id = ?",
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .map(|row| Self::row_to_scheduled_flow(&row))
        .transpose()?;

        Ok(flow)
    }

    async fn get_expired_timers(
        &self,
        now: chrono::DateTime<Utc>,
    ) -> Result<Vec<super::TimerInfo>> {
        let now_millis = now.timestamp_millis();

        let rows = sqlx::query(
            "SELECT id, step, timer_fire_at, timer_name
             FROM execution_log
             WHERE status = 'WAITING_FOR_TIMER'
               AND timer_fire_at IS NOT NULL
               AND timer_fire_at <= ?
             ORDER BY timer_fire_at ASC
             LIMIT 100",
        )
        .bind(now_millis)
        .fetch_all(&self.pool)
        .await?;

        let timers = rows
            .iter()
            .map(|row| {
                let flow_id_str: String = row.try_get("id")?;
                let flow_id = Uuid::parse_str(&flow_id_str)
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                let step: i32 = row.try_get("step")?;

                let fire_at_millis: i64 = row.try_get("timer_fire_at")?;
                let fire_at = chrono::DateTime::from_timestamp_millis(fire_at_millis)
                    .ok_or_else(|| StorageError::Connection("Invalid timestamp".to_string()))?;

                let timer_name: Option<String> = row.try_get("timer_name")?;

                Ok(super::TimerInfo {
                    flow_id,
                    step,
                    fire_at,
                    timer_name,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(timers)
    }

    async fn claim_timer(&self, flow_id: Uuid, step: i32) -> Result<bool> {
        // Start a transaction for atomic claim operation
        let mut tx = self.pool.begin().await?;

        // Optimistic concurrency: UPDATE with status check
        // Timer steps return Result<(), ExecutionError>, so we need to serialize Ok(()) properly
        use crate::executor::ExecutionError as ExecError;
        let result_ok: std::result::Result<(), ExecError> = Ok(());
        let unit_value = crate::core::serialize_value(&result_ok)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let result = sqlx::query(
            "UPDATE execution_log
             SET status = 'COMPLETE', return_value = ?
             WHERE id = ? AND step = ? AND status = 'WAITING_FOR_TIMER'",
        )
        .bind(&unit_value)
        .bind(flow_id.to_string())
        .bind(step)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        // If rows_affected == 0, another worker already claimed this timer
        Ok(result.rows_affected() > 0)
    }

    async fn log_timer(
        &self,
        flow_id: Uuid,
        step: i32,
        fire_at: chrono::DateTime<Utc>,
        timer_name: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE execution_log
             SET status = 'WAITING_FOR_TIMER',
                 timer_fire_at = ?,
                 timer_name = ?
             WHERE id = ? AND step = ?",
        )
        .bind(fire_at.timestamp_millis())
        .bind(timer_name)
        .bind(flow_id.to_string())
        .bind(step)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn log_signal(&self, flow_id: Uuid, step: i32, signal_name: &str) -> Result<()> {
        sqlx::query(
            "UPDATE execution_log
             SET status = 'WAITING_FOR_SIGNAL',
                 timer_name = ?
             WHERE id = ? AND step = ?",
        )
        .bind(signal_name)
        .bind(flow_id.to_string())
        .bind(step)
        .execute(&self.pool)
        .await?;

        debug!(
            "Logged signal wait: flow_id={}, step={}, signal_name={}",
            flow_id, step, signal_name
        );

        Ok(())
    }

    async fn store_signal_params(&self, flow_id: Uuid, step: i32, params: &[u8]) -> Result<()> {
        sqlx::query(
            "INSERT INTO signal_params (flow_id, step, params, created_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(flow_id, step)
             DO UPDATE SET params = excluded.params, created_at = excluded.created_at",
        )
        .bind(flow_id.to_string())
        .bind(step)
        .bind(params)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;

        debug!("Stored signal params: flow_id={}, step={}", flow_id, step);

        Ok(())
    }

    async fn get_signal_params(&self, flow_id: Uuid, step: i32) -> Result<Option<Vec<u8>>> {
        let params: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT params FROM signal_params WHERE flow_id = ? AND step = ?")
                .bind(flow_id.to_string())
                .bind(step)
                .fetch_optional(&self.pool)
                .await?;

        Ok(params)
    }

    async fn remove_signal_params(&self, flow_id: Uuid, step: i32) -> Result<()> {
        sqlx::query("DELETE FROM signal_params WHERE flow_id = ? AND step = ?")
            .bind(flow_id.to_string())
            .bind(step)
            .execute(&self.pool)
            .await?;

        debug!("Removed signal params: flow_id={}, step={}", flow_id, step);

        Ok(())
    }

    async fn get_waiting_signals(&self) -> Result<Vec<super::SignalInfo>> {
        let rows = sqlx::query(
            "SELECT id, step, timer_name
             FROM execution_log
             WHERE status = 'WAITING_FOR_SIGNAL'
             ORDER BY timestamp ASC
             LIMIT 100",
        )
        .fetch_all(&self.pool)
        .await?;

        let signals = rows
            .iter()
            .map(|row| {
                let flow_id_str: String = row.try_get("id")?;
                let flow_id = Uuid::parse_str(&flow_id_str)
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                let step: i32 = row.try_get("step")?;

                let signal_name: Option<String> = row.try_get("timer_name")?;

                Ok(super::SignalInfo {
                    flow_id,
                    step,
                    signal_name,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(signals)
    }

    async fn cleanup_completed(&self, older_than: std::time::Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::from_std(older_than).unwrap();
        let cutoff_millis = cutoff.timestamp_millis();

        // Start a transaction to delete from multiple tables atomically
        let mut tx = self.pool.begin().await?;

        // Delete completed invocations older than cutoff
        let result = sqlx::query(
            "DELETE FROM execution_log
             WHERE status = 'COMPLETE'
               AND timestamp < ?",
        )
        .bind(cutoff_millis)
        .execute(&mut *tx)
        .await?;

        let deleted_invocations = result.rows_affected();

        // Also cleanup old signal parameters (orphaned or very old)
        sqlx::query(
            "DELETE FROM signal_params
             WHERE created_at < ?",
        )
        .bind(cutoff_millis)
        .execute(&mut *tx)
        .await?;

        // Cleanup old completed flows from queue
        sqlx::query(
            "DELETE FROM flow_queue
             WHERE status IN ('COMPLETE', 'FAILED')
               AND updated_at < ?",
        )
        .bind(cutoff_millis)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        info!(
            "Cleaned up {} completed invocations older than {:?}",
            deleted_invocations, older_than
        );

        Ok(deleted_invocations)
    }

    async fn resume_flow(&self, flow_id: Uuid) -> Result<()> {
        // Find the task_id for this flow_id and re-enqueue it
        // ONLY resume flows that are SUSPENDED (waiting for signals/child flows)
        // DO NOT resume RUNNING flows - this causes race conditions with multiple workers
        let result = sqlx::query(
            "UPDATE flow_queue
             SET status = 'PENDING',
                 locked_by = NULL,
                 updated_at = ?
             WHERE flow_id = ?
               AND status = 'SUSPENDED'",
        )
        .bind(Utc::now().timestamp_millis())
        .bind(flow_id.to_string())
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(StorageError::Connection(format!(
                "Task not found or not in SUSPENDED state for flow_id: {}",
                flow_id
            )));
        }

        debug!("Resumed flow: flow_id={}", flow_id);
        Ok(())
    }

    async fn store_step_child_mapping(
        &self,
        flow_id: Uuid,
        parent_step: i32,
        child_step: i32,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO step_child_mappings (flow_id, parent_step, child_step, created_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(flow_id, parent_step)
             DO UPDATE SET child_step = excluded.child_step, created_at = excluded.created_at",
        )
        .bind(flow_id.to_string())
        .bind(parent_step)
        .bind(child_step)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;

        debug!(
            "Stored step-child mapping: flow={}, parent_step={}, child_step={}",
            flow_id, parent_step, child_step
        );
        Ok(())
    }

    async fn get_child_step_for_parent(
        &self,
        flow_id: Uuid,
        parent_step: i32,
    ) -> Result<Option<i32>> {
        let child_step: Option<i32> = sqlx::query_scalar(
            "SELECT child_step FROM step_child_mappings
             WHERE flow_id = ? AND parent_step = ?",
        )
        .bind(flow_id.to_string())
        .bind(parent_step)
        .fetch_optional(&self.pool)
        .await?;

        Ok(child_step)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{hash_params, serialize_value};

    #[tokio::test]
    async fn test_create_and_log_invocation() {
        let log = SqliteExecutionLog::in_memory().await.unwrap();
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
            retry_policy: None,
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
        let log = SqliteExecutionLog::in_memory().await.unwrap();
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
            retry_policy: None,
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
        let log = SqliteExecutionLog::in_memory().await.unwrap();

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
            retry_policy: None,
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
            retry_policy: None,
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
            retry_policy: None,
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
        let log = SqliteExecutionLog::in_memory().await.unwrap();
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
            retry_policy: None,
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
            retry_policy: None,
        })
        .await
        .unwrap();

        let inv2 = log.get_invocation(id, 0).await.unwrap().unwrap();
        assert_eq!(inv2.attempts(), 2);
    }

    #[tokio::test]
    async fn test_get_latest_invocation() {
        let log = SqliteExecutionLog::in_memory().await.unwrap();
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
                retry_policy: None,
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

        // For in-memory, pool size is forced to 1
        let log = SqliteExecutionLog::in_memory_with_config(config)
            .await
            .unwrap();
        assert_eq!(log.pool_size(), 1); // In-memory uses single connection
    }
}
