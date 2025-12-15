use super::{error::Result, error::StorageError, params::InvocationStartParams, ExecutionLog};
use crate::core::{hash_params, Invocation, InvocationStatus};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Pool, Postgres, Row};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, info};
use uuid::Uuid;

/// Default pool size for Postgres connection pool.
const DEFAULT_POOL_SIZE: u32 = 20;

/// Default connection timeout in seconds.
const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 30;

/// Configuration for the Postgres connection pool.
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
            min_idle: Some(5),
            connection_timeout: Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT_SECS),
            max_lifetime: None,
            idle_timeout: Some(Duration::from_secs(600)), // 10 minutes
        }
    }
}

/// Postgres-based execution log with connection pooling.
///
/// This implementation uses sqlx connection pooling to efficiently
/// manage multiple concurrent database connections. All methods are
/// natively async without `spawn_blocking` overhead.
///
/// # Database URL Format
///
/// ```text
/// postgres://username:password@hostname:port/database
/// ```
///
/// # Example
///
/// ```ignore
/// use ergon::storage::PostgresExecutionLog;
///
/// let log = PostgresExecutionLog::new("postgres://localhost/ergon").await?;
/// ```
pub struct PostgresExecutionLog {
    pool: PgPool,
    /// Optional notify handle for waking workers when work becomes available
    work_notify: Option<Arc<Notify>>,
}

impl PostgresExecutionLog {
    /// Creates a new Postgres execution log with the specified database URL.
    ///
    /// Uses default pool configuration.
    pub async fn new(database_url: impl AsRef<str>) -> Result<Self> {
        Self::with_config(database_url, PoolConfig::default()).await
    }

    /// Creates a new Postgres execution log with custom pool configuration.
    pub async fn with_config(database_url: impl AsRef<str>, config: PoolConfig) -> Result<Self> {
        let connect_options = PgConnectOptions::from_str(database_url.as_ref())
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let pool = Self::build_pool(connect_options, &config).await?;

        let log = Self {
            pool,
            work_notify: Some(Arc::new(Notify::new())),
        };

        log.initialize().await?;

        Ok(log)
    }

    /// Builds the connection pool with the given configuration.
    async fn build_pool(
        connect_options: PgConnectOptions,
        config: &PoolConfig,
    ) -> Result<Pool<Postgres>> {
        let mut builder = PgPoolOptions::new()
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

    /// Initialize the database schema.
    async fn initialize(&self) -> Result<()> {
        // Create execution_log table with Postgres-specific types
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS execution_log (
                id UUID NOT NULL,
                step INTEGER NOT NULL,
                timestamp BIGINT NOT NULL,
                class_name TEXT NOT NULL,
                method_name TEXT NOT NULL,
                delay BIGINT,
                status TEXT CHECK( status IN ('PENDING','WAITING_FOR_SIGNAL','WAITING_FOR_TIMER','COMPLETE') ) NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 1,
                parameters BYTEA,
                params_hash BIGINT NOT NULL DEFAULT 0,
                return_value BYTEA,
                retry_policy TEXT,
                is_retryable BOOLEAN,
                timer_fire_at BIGINT,
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
                task_id UUID PRIMARY KEY,
                flow_id UUID NOT NULL,
                flow_type TEXT NOT NULL,
                flow_data BYTEA NOT NULL,
                status TEXT CHECK( status IN ('PENDING','RUNNING','SUSPENDED','COMPLETE','FAILED') ) NOT NULL,
                parent_flow_id UUID,
                signal_token TEXT,
                locked_by TEXT,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                scheduled_for BIGINT
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
                flow_id UUID NOT NULL,
                step INTEGER NOT NULL,
                signal_name TEXT NOT NULL,
                params BYTEA NOT NULL,
                created_at BIGINT NOT NULL,
                PRIMARY KEY (flow_id, step, signal_name)
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

        // Create step-child mapping table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS step_child_mappings (
                flow_id UUID NOT NULL,
                parent_step INTEGER NOT NULL,
                child_step INTEGER NOT NULL,
                created_at BIGINT NOT NULL,
                PRIMARY KEY (flow_id, parent_step)
            )",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    fn row_to_scheduled_flow(row: &sqlx::postgres::PgRow) -> Result<super::ScheduledFlow> {
        let task_id: Uuid = row.try_get("task_id")?;
        let flow_id: Uuid = row.try_get("flow_id")?;
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

        let retry_count: i32 = row.try_get("retry_count").unwrap_or(0);
        let error_message: Option<String> = row.try_get("error_message").ok();
        let scheduled_for_millis: Option<i64> = row.try_get("scheduled_for").ok();
        let scheduled_for = scheduled_for_millis.and_then(chrono::DateTime::from_timestamp_millis);

        let parent_flow_id: Option<Uuid> = row.try_get("parent_flow_id").ok();
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
            retry_count: retry_count as u32,
            error_message,
            scheduled_for,
            parent_flow_id,
            signal_token,
        })
    }

    fn row_to_invocation(row: &sqlx::postgres::PgRow) -> Result<Invocation> {
        let id: Uuid = row.try_get("id")?;
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

        // Parse is_retryable
        let is_retryable: Option<bool> = row.try_get("is_retryable").ok();

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
impl ExecutionLog for PostgresExecutionLog {
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()> {
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

        let params_hash = hash_params(parameters);
        let retry_policy_json = retry_policy.and_then(|p| serde_json::to_string(&p).ok());
        let delay_millis = delay.map(|d| d.as_millis() as i64);

        sqlx::query(
            "INSERT INTO execution_log (id, step, timestamp, class_name, method_name, delay, status, attempts, parameters, params_hash, retry_policy)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT(id, step)
             DO UPDATE SET attempts = execution_log.attempts + 1",
        )
        .bind(id)
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
             SET status = 'COMPLETE', return_value = $1
             WHERE id = $2 AND step = $3",
        )
        .bind(return_value)
        .bind(id)
        .bind(step)
        .execute(&self.pool)
        .await?;

        let invocation = sqlx::query(
            "SELECT id, step, timestamp, class_name, method_name, status, attempts, parameters, params_hash, return_value, delay, retry_policy, is_retryable, timer_fire_at, timer_name
             FROM execution_log
             WHERE id = $1 AND step = $2",
        )
        .bind(id)
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
             WHERE id = $1 AND step = $2",
        )
        .bind(id)
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
             WHERE id = $1
             ORDER BY step DESC
             LIMIT 1",
        )
        .bind(id)
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
             WHERE id = $1
             ORDER BY step ASC",
        )
        .bind(id)
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
            "SELECT
                q.flow_id,
                q.flow_type,
                q.created_at,
                COALESCE(e.id, q.flow_id) as id,
                COALESCE(e.step, 0) as step,
                COALESCE(e.timestamp, q.created_at) as timestamp,
                COALESCE(e.class_name, q.flow_type) as class_name,
                COALESCE(e.method_name, 'flow') as method_name,
                COALESCE(e.status, 'PENDING') as status,
                COALESCE(e.attempts, 0) as attempts,
                COALESCE(e.parameters, ''::bytea) as parameters,
                COALESCE(e.params_hash, 0) as params_hash,
                e.return_value,
                e.delay,
                e.retry_policy,
                e.is_retryable,
                e.timer_fire_at,
                e.timer_name
             FROM flow_queue q
             LEFT JOIN execution_log e ON q.flow_id = e.id AND e.step = 0
             WHERE q.status NOT IN ('COMPLETE', 'FAILED')
             ORDER BY q.created_at ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        let invocations = rows
            .iter()
            .map(Self::row_to_invocation)
            .collect::<Result<Vec<_>>>()?;

        info!(
            "Found {} incomplete flows (includes scheduled-but-not-started)",
            invocations.len()
        );

        Ok(invocations)
    }

    async fn has_non_retryable_error(&self, flow_id: Uuid) -> Result<bool> {
        let has_non_retryable: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                     SELECT 1 FROM execution_log
                     WHERE id = $1 AND is_retryable = false
                 )",
        )
        .bind(flow_id)
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
             SET is_retryable = $1
             WHERE id = $2 AND step = $3",
        )
        .bind(is_retryable)
        .bind(id)
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
        info!("Reset execution log database (cleared all tables)");
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
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(task_id)
        .bind(flow.flow_id)
        .bind(flow.flow_type)
        .bind(flow.flow_data)
        .bind(flow.status.as_str())
        .bind(flow.locked_by)
        .bind(flow.created_at.timestamp_millis())
        .bind(flow.updated_at.timestamp_millis())
        .bind(flow.retry_count as i32)
        .bind(flow.error_message)
        .bind(flow.scheduled_for.map(|dt| dt.timestamp_millis()))
        .bind(flow.parent_flow_id)
        .bind(flow.signal_token)
        .execute(&self.pool)
        .await?;

        debug!(
            "Enqueued flow: task_id={}, flow_type={}",
            task_id, flow_type
        );

        // Wake up one waiting worker if this is an immediate (non-delayed) flow
        if flow.scheduled_for.is_none() {
            if let Some(ref notify) = self.work_notify {
                notify.notify_one();
            }
        }

        Ok(task_id)
    }

    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<super::ScheduledFlow>> {
        let now = Utc::now().timestamp_millis();

        // Postgres supports UPDATE...RETURNING with subquery for atomic dequeue
        let flow_opt = sqlx::query(
            "UPDATE flow_queue
             SET status = 'RUNNING', locked_by = $1, updated_at = $2
             WHERE task_id = (
                 SELECT task_id FROM flow_queue
                 WHERE status = 'PENDING'
                   AND (scheduled_for IS NULL OR scheduled_for <= $3)
                 ORDER BY created_at ASC
                 FOR UPDATE SKIP LOCKED
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
        .and_then(|row| Self::row_to_scheduled_flow(&row).ok());

        if let Some(flow) = &flow_opt {
            debug!(
                "Dequeued flow: task_id={}, flow_type={}, worker={}",
                flow.task_id, flow.flow_type, worker_id
            );
        }

        Ok(flow_opt)
    }

    async fn complete_flow(&self, task_id: Uuid, status: super::TaskStatus) -> Result<()> {
        let result = if status == super::TaskStatus::Suspended {
            sqlx::query(
                "UPDATE flow_queue
                 SET status = $1, locked_by = NULL, updated_at = $2
                 WHERE task_id = $3",
            )
            .bind(status.as_str())
            .bind(Utc::now().timestamp_millis())
            .bind(task_id)
            .execute(&self.pool)
            .await?
        } else {
            sqlx::query(
                "UPDATE flow_queue
                 SET status = $1, updated_at = $2
                 WHERE task_id = $3",
            )
            .bind(status.as_str())
            .bind(Utc::now().timestamp_millis())
            .bind(task_id)
            .execute(&self.pool)
            .await?
        };

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
        let scheduled_for = Utc::now()
            + chrono::Duration::from_std(delay)
                .map_err(|e| StorageError::Connection(format!("Invalid delay duration: {}", e)))?;

        let result = sqlx::query(
            "UPDATE flow_queue
             SET retry_count = retry_count + 1,
                 error_message = $1,
                 status = $2,
                 locked_by = NULL,
                 scheduled_for = $3,
                 updated_at = $4
             WHERE task_id = $5",
        )
        .bind(error_message)
        .bind(super::TaskStatus::Pending.as_str())
        .bind(scheduled_for.timestamp_millis())
        .bind(Utc::now().timestamp_millis())
        .bind(task_id)
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
             WHERE task_id = $1",
        )
        .bind(task_id)
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
               AND timer_fire_at <= $1
             ORDER BY timer_fire_at ASC
             LIMIT 100",
        )
        .bind(now_millis)
        .fetch_all(&self.pool)
        .await?;

        let timers = rows
            .iter()
            .map(|row| {
                let flow_id: Uuid = row.try_get("id")?;
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
        let mut tx = self.pool.begin().await?;

        use crate::executor::ExecutionError as ExecError;
        let result_ok: std::result::Result<(), ExecError> = Ok(());
        let unit_value = crate::core::serialize_value(&result_ok)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let result = sqlx::query(
            "UPDATE execution_log
             SET status = 'COMPLETE', return_value = $1
             WHERE id = $2 AND step = $3 AND status = 'WAITING_FOR_TIMER'",
        )
        .bind(&unit_value)
        .bind(flow_id)
        .bind(step)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

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
                 timer_fire_at = $1,
                 timer_name = $2
             WHERE id = $3 AND step = $4",
        )
        .bind(fire_at.timestamp_millis())
        .bind(timer_name)
        .bind(flow_id)
        .bind(step)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn log_signal(&self, flow_id: Uuid, step: i32, signal_name: &str) -> Result<()> {
        sqlx::query(
            "UPDATE execution_log
             SET status = 'WAITING_FOR_SIGNAL',
                 timer_name = $1
             WHERE id = $2 AND step = $3",
        )
        .bind(signal_name)
        .bind(flow_id)
        .bind(step)
        .execute(&self.pool)
        .await?;

        debug!(
            "Logged signal wait: flow_id={}, step={}, signal_name={}",
            flow_id, step, signal_name
        );

        Ok(())
    }

    async fn store_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
        params: &[u8],
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO signal_params (flow_id, step, signal_name, params, created_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT(flow_id, step, signal_name)
             DO UPDATE SET params = EXCLUDED.params, created_at = EXCLUDED.created_at",
        )
        .bind(flow_id)
        .bind(step)
        .bind(signal_name)
        .bind(params)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;

        debug!(
            "Stored signal params: flow_id={}, step={}, signal_name={}",
            flow_id, step, signal_name
        );

        Ok(())
    }

    async fn get_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
    ) -> Result<Option<Vec<u8>>> {
        let params: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT params FROM signal_params WHERE flow_id = $1 AND step = $2 AND signal_name = $3",
        )
        .bind(flow_id)
        .bind(step)
        .bind(signal_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(params)
    }

    async fn remove_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
    ) -> Result<()> {
        sqlx::query(
            "DELETE FROM signal_params WHERE flow_id = $1 AND step = $2 AND signal_name = $3",
        )
        .bind(flow_id)
        .bind(step)
        .bind(signal_name)
        .execute(&self.pool)
        .await?;

        debug!(
            "Removed signal params: flow_id={}, step={}, signal_name={}",
            flow_id, step, signal_name
        );

        Ok(())
    }

    async fn get_waiting_signals(&self) -> Result<Vec<super::SignalInfo>> {
        let rows = sqlx::query(
            "SELECT el.id, el.step, el.timer_name
             FROM execution_log el
             JOIN flow_queue fq ON el.id = fq.flow_id
             WHERE el.status = 'WAITING_FOR_SIGNAL'
               AND fq.status = 'SUSPENDED'
             ORDER BY el.timestamp ASC
             LIMIT 100",
        )
        .fetch_all(&self.pool)
        .await?;

        let signals = rows
            .iter()
            .map(|row| {
                let flow_id: Uuid = row.try_get("id")?;
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
        let cutoff = Utc::now()
            - chrono::Duration::from_std(older_than).map_err(|e| {
                StorageError::Connection(format!("Invalid duration for cleanup: {}", e))
            })?;
        let cutoff_millis = cutoff.timestamp_millis();

        let mut tx = self.pool.begin().await?;

        let result = sqlx::query(
            "DELETE FROM execution_log
             WHERE status = 'COMPLETE'
               AND timestamp < $1",
        )
        .bind(cutoff_millis)
        .execute(&mut *tx)
        .await?;

        let deleted_invocations = result.rows_affected();

        sqlx::query(
            "DELETE FROM signal_params
             WHERE created_at < $1",
        )
        .bind(cutoff_millis)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "DELETE FROM flow_queue
             WHERE status IN ('COMPLETE', 'FAILED')
               AND updated_at < $1",
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

    async fn resume_flow(&self, flow_id: Uuid) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE flow_queue
             SET status = 'PENDING',
                 locked_by = NULL,
                 updated_at = $1
             WHERE flow_id = $2
               AND status = 'SUSPENDED'",
        )
        .bind(Utc::now().timestamp_millis())
        .bind(flow_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            debug!(
                "Flow not resumed (not in SUSPENDED state): flow_id={}",
                flow_id
            );
            return Ok(false);
        }

        debug!("Resumed flow: flow_id={}", flow_id);

        if let Some(ref notify) = self.work_notify {
            notify.notify_one();
        }

        Ok(true)
    }

    fn work_notify(&self) -> Option<&Arc<Notify>> {
        self.work_notify.as_ref()
    }

    async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(StorageError::Database)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{hash_params, serialize_value};

    // Note: These tests require a running Postgres instance
    // Run with: cargo test --features postgres -- --ignored

    #[tokio::test]
    #[ignore]
    async fn test_create_and_log_invocation() {
        let log = PostgresExecutionLog::new("postgres://localhost/ergon_test")
            .await
            .unwrap();
        log.reset().await.unwrap(); // Clean slate

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
        assert_eq!(invocation.params_hash(), expected_hash);

        log.close().await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_log_completion() {
        let log = PostgresExecutionLog::new("postgres://localhost/ergon_test")
            .await
            .unwrap();
        log.reset().await.unwrap();

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

        log.close().await.unwrap();
    }
}
