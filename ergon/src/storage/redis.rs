//! Redis-based execution log implementation with async pool and best practices.
//!
//! This module provides a Redis backend for distributed execution with true
//! multi-machine support. Unlike SQLite which requires shared filesystem access,
//! Redis enables workers to run on completely separate machines.
//!
//! # Data Structures
//!
//! - `ergon:queue:pending` (LIST): FIFO queue of ready task IDs
//! - `ergon:queue:delayed` (ZSET): Delayed tasks (score = scheduled_timestamp)
//! - `ergon:flow:{task_id}` (HASH): Flow metadata and serialized data (with TTL)
//! - `ergon:invocations:{flow_id}` (LIST): Invocation history per flow
//! - `ergon:running` (ZSET): Running flows index (score = start time, for stale lock detection)
//! - `ergon:timers:pending` (ZSET): Pending timers (score = fire_at timestamp)
//! - `ergon:inv:{flow_id}:{step}` (HASH): Invocation data including timer info
//! - `ergon:signal:{flow_id}:{step}` (STRING): Signal parameters (with TTL)
//!
//! # Key Features
//!
//! - **Async connection pool**: Uses deadpool-redis for true async operations
//! - **Sorted sets for scheduling**: No busy-loop for delayed tasks
//! - **Atomic operations**: Uses pipelines and Lua scripts
//! - **TTL cleanup**: Automatic expiration of completed flows
//! - **Stale lock recovery**: Detects and recovers crashed workers
//!
//! # Automatic Maintenance
//!
//! When using the `Worker` type, Redis maintenance is **automatically handled**:
//! - **Every 1 second**: `move_ready_delayed_tasks()` checks for ready delayed tasks
//! - **Every 60 seconds**: `recover_stale_locks()` checks for stale locks
//!
//! No manual setup required! The Worker integrates these calls internally.
//!
//! # Performance Characteristics
//!
//! - Enqueue: O(log N) with ZADD to sorted set
//! - Dequeue: O(1) with BLPOP (blocks until available)
//! - Background mover: O(log N) per ready task
//! - Network overhead: ~0.1-0.5ms per operation (local network)

use super::{error::Result, error::StorageError, params::InvocationStartParams, ExecutionLog};
use crate::core::{hash_params, Invocation, InvocationStatus};
use async_trait::async_trait;
use chrono::Utc;
use deadpool_redis::{Config, Pool, Runtime};
use redis::AsyncCommands;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Default TTL for completed flows (7 days in seconds)
const DEFAULT_COMPLETED_TTL: i64 = 7 * 24 * 3600;

/// Default TTL for signal parameters (30 days in seconds)
const DEFAULT_SIGNAL_TTL: i64 = 30 * 24 * 3600;

/// Default stale lock timeout (5 minutes in milliseconds)
const DEFAULT_STALE_LOCK_TIMEOUT_MS: i64 = 5 * 60 * 1000;

/// Redis execution log using async connection pooling.
///
/// This implementation uses deadpool-redis for async operations and
/// follows Redis best practices for task scheduling, TTL, and lock management.
pub struct RedisExecutionLog {
    pool: Pool,
    completed_ttl: i64,
    signal_ttl: i64,
    stale_lock_timeout_ms: i64,
}

impl RedisExecutionLog {
    /// Creates a new Redis execution log with connection pooling.
    ///
    /// # Arguments
    ///
    /// * `redis_url` - Redis connection URL (e.g., "redis://127.0.0.1:6379")
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ergon::storage::RedisExecutionLog;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let storage = RedisExecutionLog::new("redis://localhost:6379").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(redis_url: &str) -> Result<Self> {
        let cfg = Config::from_url(redis_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self {
            pool,
            completed_ttl: DEFAULT_COMPLETED_TTL,
            signal_ttl: DEFAULT_SIGNAL_TTL,
            stale_lock_timeout_ms: DEFAULT_STALE_LOCK_TIMEOUT_MS,
        })
    }

    /// Creates a connection pool with custom TTL configuration.
    pub async fn with_ttl_config(
        redis_url: &str,
        completed_ttl_secs: i64,
        signal_ttl_secs: i64,
    ) -> Result<Self> {
        let cfg = Config::from_url(redis_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self {
            pool,
            completed_ttl: completed_ttl_secs,
            signal_ttl: signal_ttl_secs,
            stale_lock_timeout_ms: DEFAULT_STALE_LOCK_TIMEOUT_MS,
        })
    }

    /// Gets an async connection from the pool.
    async fn get_connection(
        &self,
    ) -> Result<deadpool_redis::Connection> {
        self.pool
            .get()
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))
    }

    /// Builds the Redis key for flow metadata.
    fn flow_key(task_id: Uuid) -> String {
        format!("ergon:flow:{}", task_id)
    }

    /// Builds the Redis key for invocations list.
    fn invocations_key(flow_id: Uuid) -> String {
        format!("ergon:invocations:{}", flow_id)
    }

    /// Builds the Redis key for a specific invocation.
    fn invocation_key(flow_id: Uuid, step: i32) -> String {
        format!("ergon:inv:{}:{}", flow_id, step)
    }

    /// Builds the Redis key for signal parameters.
    fn signal_key(flow_id: Uuid, step: i32) -> String {
        format!("ergon:signal:{}:{}", flow_id, step)
    }

    /// Helper to parse invocation from Redis hash data.
    fn parse_invocation(&self, data: HashMap<String, Vec<u8>>) -> Result<Invocation> {
        // Helper to get string field
        let get_str = |key: &str| -> Result<String> {
            data.get(key)
                .and_then(|v| String::from_utf8(v.clone()).ok())
                .ok_or_else(|| StorageError::Connection(format!("Missing field: {}", key)))
        };

        // Helper to get i32 field
        let get_i32 = |key: &str| -> Result<i32> {
            get_str(key)?
                .parse()
                .map_err(|_| StorageError::Connection(format!("Invalid i32 for {}", key)))
        };

        // Helper to get i64 field
        let get_i64 = |key: &str| -> Result<i64> {
            get_str(key)?
                .parse()
                .map_err(|_| StorageError::Connection(format!("Invalid i64 for {}", key)))
        };

        let id = Uuid::parse_str(&get_str("id")?)
            .map_err(|e| StorageError::Connection(e.to_string()))?;
        let step = get_i32("step")?;
        let timestamp_secs = get_i64("timestamp")?;
        let timestamp = chrono::DateTime::from_timestamp(timestamp_secs, 0)
            .unwrap_or_else(Utc::now);
        let class_name = get_str("class_name")?;
        let method_name = get_str("method_name")?;
        let status_str = get_str("status")?;
        let status = InvocationStatus::from_str(&status_str)
            .map_err(|e| StorageError::Connection(e.to_string()))?;
        let attempts = get_i32("attempts")?;
        let parameters = data.get("parameters").cloned().unwrap_or_default();
        let params_hash = get_str("params_hash")?
            .parse::<u64>()
            .unwrap_or(0);
        let return_value = data.get("return_value").cloned();
        let delay_ms = get_i64("delay_ms").ok();

        // Parse retry_policy
        let retry_policy_json = get_str("retry_policy").ok();
        let retry_policy = retry_policy_json.and_then(|json| {
            if json.is_empty() {
                None
            } else {
                serde_json::from_str(&json).ok()
            }
        });

        // Parse is_retryable
        let is_retryable_str = get_str("is_retryable").ok();
        let is_retryable = is_retryable_str.and_then(|s| {
            if s.is_empty() {
                None
            } else {
                s.parse::<i32>().ok().map(|v| v != 0)
            }
        });

        // Parse timer fields
        let timer_fire_at_millis = get_i64("timer_fire_at").ok();
        let timer_fire_at = timer_fire_at_millis.and_then(|ms| {
            chrono::DateTime::from_timestamp_millis(ms)
        });
        let timer_name = get_str("timer_name").ok();

        let mut invocation = Invocation::new(
            id,
            step,
            timestamp,
            class_name,
            method_name,
            status,
            attempts,
            parameters,
            params_hash,
            return_value,
            delay_ms,
            retry_policy,
            is_retryable,
        );

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
impl ExecutionLog for RedisExecutionLog {
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()> {
        let mut conn = self.get_connection().await?;

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
        let delay_ms = delay.map(|d| d.as_millis() as i64).unwrap_or(0);
        let timestamp = Utc::now().timestamp();

        // Serialize retry_policy to JSON if present
        let retry_policy_json = retry_policy
            .and_then(|p| serde_json::to_string(&p).ok())
            .unwrap_or_default();

        // Store invocation as HASH
        let inv_key = Self::invocation_key(id, step);
        let _: () = redis::pipe()
            .atomic()
            .hset(&inv_key, "id", id.to_string())
            .hset(&inv_key, "step", step)
            .hset(&inv_key, "timestamp", timestamp)
            .hset(&inv_key, "class_name", class_name)
            .hset(&inv_key, "method_name", method_name)
            .hset(&inv_key, "status", status.as_str())
            .hset(&inv_key, "attempts", 1)
            .hset(&inv_key, "parameters", parameters)
            .hset(&inv_key, "params_hash", params_hash.to_string())
            .hset(&inv_key, "delay_ms", delay_ms)
            .hset(&inv_key, "retry_policy", retry_policy_json)
            .hset(&inv_key, "is_retryable", "") // Empty = None
            .hset(&inv_key, "timer_fire_at", 0)
            .hset(&inv_key, "timer_name", "")
            .rpush(Self::invocations_key(id), &inv_key)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation> {
        let mut conn = self.get_connection().await?;
        let inv_key = Self::invocation_key(id, step);

        // Check if invocation exists
        let exists: bool = conn
            .exists(&inv_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Err(StorageError::InvocationNotFound { id, step });
        }

        // Update status and return value
        let _: () = redis::pipe()
            .atomic()
            .hset(&inv_key, "status", "Complete")
            .hset(&inv_key, "return_value", return_value)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // Fetch and reconstruct invocation
        let data: HashMap<String, Vec<u8>> = conn
            .hgetall(&inv_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        self.parse_invocation(data)
    }

    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>> {
        let mut conn = self.get_connection().await?;
        let inv_key = Self::invocation_key(id, step);

        let exists: bool = conn
            .exists(&inv_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Ok(None);
        }

        let data: HashMap<String, Vec<u8>> = conn
            .hgetall(&inv_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Some(self.parse_invocation(data)?))
    }

    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>> {
        let invocations = self.get_invocations_for_flow(id).await?;
        Ok(invocations.into_iter().max_by_key(|inv| inv.step()))
    }

    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>> {
        let mut conn = self.get_connection().await?;
        let inv_list_key = Self::invocations_key(id);

        // Get all invocation keys for this flow
        let inv_keys: Vec<String> = conn
            .lrange(&inv_list_key, 0, -1)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let mut invocations = Vec::new();
        for key in inv_keys {
            let data: HashMap<String, Vec<u8>> = conn
                .hgetall(&key)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;
            invocations.push(self.parse_invocation(data)?);
        }

        invocations.sort_by_key(|inv| inv.step());
        Ok(invocations)
    }

    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>> {
        let mut conn = self.get_connection().await?;

        // Scan for all flow invocation lists
        let mut cursor = 0u64;
        let mut all_flows = Vec::new();

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .cursor_arg(cursor)
                .arg("MATCH")
                .arg("ergon:invocations:*")
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            all_flows.extend(keys);
            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        // For each flow, check if incomplete
        let mut incomplete = Vec::new();
        for inv_list_key in all_flows {
            // Get first invocation (step 0) from the list
            let inv_keys: Vec<String> = conn
                .lrange(&inv_list_key, 0, 0)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            if let Some(first_key) = inv_keys.first() {
                let data: HashMap<String, Vec<u8>> = conn
                    .hgetall(first_key)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                let invocation = self.parse_invocation(data)?;
                if invocation.status() != InvocationStatus::Complete {
                    incomplete.push(invocation);
                }
            }
        }

        Ok(incomplete)
    }

    async fn has_non_retryable_error(&self, flow_id: Uuid) -> Result<bool> {
        let invocations = self.get_invocations_for_flow(flow_id).await?;
        Ok(invocations
            .iter()
            .any(|inv| inv.is_retryable() == Some(false)))
    }

    async fn update_is_retryable(&self, id: Uuid, step: i32, is_retryable: bool) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let inv_key = Self::invocation_key(id, step);

        let value = if is_retryable { "1" } else { "0" };
        let _: () = conn
            .hset(&inv_key, "is_retryable", value)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn reset(&self) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // Use SCAN to find all ergon keys, then delete in batches
        let mut cursor = 0u64;
        let mut all_keys = Vec::new();

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .cursor_arg(cursor)
                .arg("MATCH")
                .arg("ergon:*")
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            all_keys.extend(keys);
            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        // Delete all found keys in batches
        if !all_keys.is_empty() {
            for chunk in all_keys.chunks(100) {
                let _: () = conn
                    .del(chunk)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;
            }
        }

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        // Connection pool handles cleanup automatically
        Ok(())
    }

    // ===== Distributed Queue Operations =====

    async fn enqueue_flow(&self, flow: super::ScheduledFlow) -> Result<Uuid> {
        let mut conn = self.get_connection().await?;
        let task_id = flow.task_id;
        let flow_key = Self::flow_key(task_id);

        // Check if flow should be delayed
        if let Some(scheduled_for) = flow.scheduled_for {
            let scheduled_ts = scheduled_for.timestamp_millis();

            // Store flow metadata and add to delayed queue
            let _: () = redis::pipe()
                .atomic()
                .hset(&flow_key, "task_id", task_id.to_string())
                .hset(&flow_key, "flow_id", flow.flow_id.to_string())
                .hset(&flow_key, "flow_type", &flow.flow_type)
                .hset(&flow_key, "status", "Pending")
                .hset(&flow_key, "created_at", flow.created_at.timestamp())
                .hset(&flow_key, "updated_at", flow.updated_at.timestamp())
                .hset(&flow_key, "scheduled_for", scheduled_ts)
                .hset(&flow_key, "flow_data", flow.flow_data.as_slice())
                // Add to delayed queue (sorted by scheduled_for timestamp)
                .zadd("ergon:queue:delayed", task_id.to_string(), scheduled_ts)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;
        } else {
            // Immediate execution - add to pending queue
            let _: () = redis::pipe()
                .atomic()
                .hset(&flow_key, "task_id", task_id.to_string())
                .hset(&flow_key, "flow_id", flow.flow_id.to_string())
                .hset(&flow_key, "flow_type", &flow.flow_type)
                .hset(&flow_key, "status", "Pending")
                .hset(&flow_key, "created_at", flow.created_at.timestamp())
                .hset(&flow_key, "updated_at", flow.updated_at.timestamp())
                .hset(&flow_key, "flow_data", flow.flow_data.as_slice())
                // Add to pending queue (FIFO)
                .rpush("ergon:queue:pending", task_id.to_string())
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;
        }

        Ok(task_id)
    }

    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<super::ScheduledFlow>> {
        let mut conn = self.get_connection().await?;

        // Blocking pop with 1 second timeout (efficient!)
        let result: Option<Vec<String>> = conn
            .blpop("ergon:queue:pending", 1.0)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if let Some(values) = result {
            if values.len() < 2 {
                return Ok(None);
            }

            let task_id_str = &values[1];
            let task_id = Uuid::parse_str(task_id_str)
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            let flow_key = Self::flow_key(task_id);
            let now = Utc::now().timestamp();

            // Atomically update status and lock
            let _: () = redis::pipe()
                .atomic()
                .hset(&flow_key, "status", "Running")
                .hset(&flow_key, "locked_by", worker_id)
                .hset(&flow_key, "updated_at", now)
                // Add to running set with current timestamp for stale lock detection
                .zadd("ergon:running", task_id_str, Utc::now().timestamp_millis())
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            // Fetch flow metadata
            let data: HashMap<String, Vec<u8>> = conn
                .hgetall(&flow_key)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            // Parse flow
            let flow_id = Uuid::parse_str(
                &String::from_utf8_lossy(data.get("flow_id").ok_or_else(|| {
                    StorageError::Connection("Missing flow_id".to_string())
                })?),
            )
            .map_err(|e| StorageError::Connection(e.to_string()))?;

            let flow_type =
                String::from_utf8_lossy(data.get("flow_type").ok_or_else(|| {
                    StorageError::Connection("Missing flow_type".to_string())
                })?)
                .to_string();

            let flow_data = data
                .get("flow_data")
                .ok_or_else(|| StorageError::Connection("Missing flow_data".to_string()))?
                .clone();

            let created_at_ts: i64 = String::from_utf8_lossy(
                data.get("created_at").ok_or_else(|| {
                    StorageError::Connection("Missing created_at".to_string())
                })?,
            )
            .parse()
            .unwrap_or(0);
            let created_at =
                chrono::DateTime::from_timestamp(created_at_ts, 0).unwrap_or_else(Utc::now);

            let updated_at =
                chrono::DateTime::from_timestamp(now, 0).unwrap_or_else(Utc::now);

            Ok(Some(super::ScheduledFlow {
                task_id,
                flow_id,
                flow_type,
                flow_data,
                status: super::TaskStatus::Running,
                locked_by: Some(worker_id.to_string()),
                created_at,
                updated_at,
                retry_count: 0,
                error_message: None,
                scheduled_for: None,
            }))
        } else {
            Ok(None)
        }
    }

    async fn complete_flow(&self, task_id: Uuid, status: super::TaskStatus) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let flow_key = Self::flow_key(task_id);
        let now = Utc::now().timestamp();

        // Update flow status and remove from running set
        let _: () = redis::pipe()
            .atomic()
            .hset(&flow_key, "status", status.as_str())
            .hset(&flow_key, "updated_at", now)
            .zrem("ergon:running", task_id.to_string())
            // Set TTL on completed flow for automatic cleanup
            .expire(&flow_key, self.completed_ttl)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn retry_flow(
        &self,
        task_id: Uuid,
        error_message: String,
        delay: Duration,
    ) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let flow_key = Self::flow_key(task_id);
        let scheduled_for = Utc::now() + chrono::Duration::from_std(delay).unwrap();
        let scheduled_ts = scheduled_for.timestamp_millis();

        // Update flow and add to delayed queue
        let _: () = redis::pipe()
            .atomic()
            .hincr(&flow_key, "retry_count", 1)
            .hset(&flow_key, "error_message", error_message)
            .hset(&flow_key, "status", "Pending")
            .hset(&flow_key, "scheduled_for", scheduled_ts)
            .hdel(&flow_key, "locked_by")
            .hset(&flow_key, "updated_at", Utc::now().timestamp())
            .zrem("ergon:running", task_id.to_string())
            // Add to delayed queue
            .zadd("ergon:queue:delayed", task_id.to_string(), scheduled_ts)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn get_scheduled_flow(&self, task_id: Uuid) -> Result<Option<super::ScheduledFlow>> {
        let mut conn = self.get_connection().await?;
        let flow_key = Self::flow_key(task_id);

        let exists: bool = conn
            .exists(&flow_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Ok(None);
        }

        let data: HashMap<String, Vec<u8>> = conn
            .hgetall(&flow_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // Parse flow
        let flow_id = Uuid::parse_str(
            &String::from_utf8_lossy(data.get("flow_id").ok_or_else(|| {
                StorageError::Connection("Missing flow_id".to_string())
            })?),
        )
        .map_err(|e| StorageError::Connection(e.to_string()))?;

        let flow_type =
            String::from_utf8_lossy(data.get("flow_type").ok_or_else(|| {
                StorageError::Connection("Missing flow_type".to_string())
            })?)
            .to_string();

        let flow_data = data
            .get("flow_data")
            .ok_or_else(|| StorageError::Connection("Missing flow_data".to_string()))?
            .clone();

        let status_str =
            String::from_utf8_lossy(data.get("status").ok_or_else(|| {
                StorageError::Connection("Missing status".to_string())
            })?);
        let status: super::TaskStatus = status_str
            .parse()
            .map_err(|e: String| StorageError::Connection(e))?;

        let locked_by = data
            .get("locked_by")
            .map(|v| String::from_utf8_lossy(v).to_string());

        let created_at_ts: i64 = String::from_utf8_lossy(
            data.get("created_at").ok_or_else(|| {
                StorageError::Connection("Missing created_at".to_string())
            })?,
        )
        .parse()
        .unwrap_or(0);
        let created_at =
            chrono::DateTime::from_timestamp(created_at_ts, 0).unwrap_or_else(Utc::now);

        let updated_at_ts: i64 = String::from_utf8_lossy(
            data.get("updated_at").ok_or_else(|| {
                StorageError::Connection("Missing updated_at".to_string())
            })?,
        )
        .parse()
        .unwrap_or(0);
        let updated_at =
            chrono::DateTime::from_timestamp(updated_at_ts, 0).unwrap_or_else(Utc::now);

        let retry_count: u32 =
            String::from_utf8_lossy(data.get("retry_count").unwrap_or(&vec![]))
                .parse()
                .unwrap_or(0);

        let error_message = data
            .get("error_message")
            .map(|v| String::from_utf8_lossy(v).to_string());

        let scheduled_for_ms: Option<i64> =
            String::from_utf8_lossy(data.get("scheduled_for").unwrap_or(&vec![]))
                .parse()
                .ok();
        let scheduled_for = scheduled_for_ms.and_then(chrono::DateTime::from_timestamp_millis);

        Ok(Some(super::ScheduledFlow {
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
        }))
    }

    // ===== Timer Operations =====

    async fn get_expired_timers(
        &self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<super::TimerInfo>> {
        let mut conn = self.get_connection().await?;
        let now_millis = now.timestamp_millis();

        // Get expired timers from sorted set
        let timer_keys: Vec<String> = conn
            .zrangebyscore("ergon:timers:pending", 0, now_millis)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let mut timers = Vec::new();
        for key in timer_keys {
            // Parse key format: "flow_id:step"
            let parts: Vec<&str> = key.split(':').collect();
            if parts.len() != 2 {
                continue;
            }

            let flow_id = match Uuid::parse_str(parts[0]) {
                Ok(id) => id,
                Err(_) => continue,
            };

            let step = match parts[1].parse::<i32>() {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Get timer details from invocation
            let inv_key = Self::invocation_key(flow_id, step);
            let data: HashMap<String, Vec<u8>> = conn
                .hgetall(&inv_key)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            let default_vec = vec![b'0'];
            let fire_at_bytes = data.get("timer_fire_at").unwrap_or(&default_vec);
            let fire_at_str = String::from_utf8_lossy(fire_at_bytes);
            let fire_at_ms: i64 = fire_at_str.parse().unwrap_or(0);
            let fire_at = chrono::DateTime::from_timestamp_millis(fire_at_ms)
                .unwrap_or_else(Utc::now);

            let timer_name = data
                .get("timer_name")
                .map(|v| String::from_utf8_lossy(v).to_string())
                .filter(|s| !s.is_empty());

            timers.push(super::TimerInfo {
                flow_id,
                step,
                fire_at,
                timer_name,
            });
        }

        Ok(timers)
    }

    async fn claim_timer(&self, flow_id: Uuid, step: i32) -> Result<bool> {
        let mut conn = self.get_connection().await?;
        let inv_key = Self::invocation_key(flow_id, step);
        let timer_key = format!("{}:{}", flow_id, step);

        // Get current status
        let status: Option<String> = conn
            .hget(&inv_key, "status")
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if status.as_deref() != Some("WaitingForTimer") {
            return Ok(false); // Already claimed or not waiting
        }

        // Atomically update status and remove from timers set
        let unit_value = bincode::serde::encode_to_vec((), bincode::config::standard())
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let _: () = redis::pipe()
            .atomic()
            .hset(&inv_key, "status", "Complete")
            .hset(&inv_key, "return_value", unit_value)
            .zrem("ergon:timers:pending", &timer_key)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(true)
    }

    async fn log_timer(
        &self,
        flow_id: Uuid,
        step: i32,
        fire_at: chrono::DateTime<chrono::Utc>,
        timer_name: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let inv_key = Self::invocation_key(flow_id, step);
        let timer_key = format!("{}:{}", flow_id, step);
        let fire_at_ms = fire_at.timestamp_millis();

        // Update invocation and add to timers sorted set
        let _: () = redis::pipe()
            .atomic()
            .hset(&inv_key, "status", "WaitingForTimer")
            .hset(&inv_key, "timer_fire_at", fire_at_ms)
            .hset(&inv_key, "timer_name", timer_name.unwrap_or(""))
            // Add to timers sorted set (score = fire_at timestamp)
            .zadd("ergon:timers:pending", &timer_key, fire_at_ms)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    // ===== Signal Operations =====

    async fn store_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        params: &[u8],
    ) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let signal_key = Self::signal_key(flow_id, step);

        // Store signal params with TTL
        let _: () = redis::pipe()
            .atomic()
            .set(&signal_key, params)
            .expire(&signal_key, self.signal_ttl)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        debug!("Stored signal params: flow_id={}, step={}", flow_id, step);
        Ok(())
    }

    async fn get_signal_params(&self, flow_id: Uuid, step: i32) -> Result<Option<Vec<u8>>> {
        let mut conn = self.get_connection().await?;
        let signal_key = Self::signal_key(flow_id, step);

        let params: Option<Vec<u8>> = conn
            .get(&signal_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(params)
    }

    async fn remove_signal_params(&self, flow_id: Uuid, step: i32) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let signal_key = Self::signal_key(flow_id, step);

        let _: () = conn
            .del(&signal_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        debug!(
            "Removed signal params: flow_id={}, step={}",
            flow_id, step
        );
        Ok(())
    }

    // ===== Cleanup Operations =====

    async fn cleanup_completed(&self, older_than: Duration) -> Result<u64> {
        let mut conn = self.get_connection().await?;
        let cutoff = Utc::now() - chrono::Duration::from_std(older_than).unwrap();
        let cutoff_ts = cutoff.timestamp();

        // Scan for all flow keys
        let mut cursor = 0u64;
        let mut deleted = 0u64;

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .cursor_arg(cursor)
                .arg("MATCH")
                .arg("ergon:flow:*")
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            for flow_key in keys {
                // Check if flow is completed and old
                let status: Option<String> = conn
                    .hget(&flow_key, "status")
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                let updated_at: Option<i64> = conn
                    .hget(&flow_key, "updated_at")
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                if let (Some(s), Some(ts)) = (status, updated_at) {
                    if (s == "Complete" || s == "Failed") && ts < cutoff_ts {
                        // Delete the flow key
                        let _: () = conn
                            .del(&flow_key)
                            .await
                            .map_err(|e| StorageError::Connection(e.to_string()))?;
                        deleted += 1;
                    }
                }
            }

            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        info!(
            "Cleaned up {} completed flows older than {:?}",
            deleted, older_than
        );
        Ok(deleted)
    }

    async fn move_ready_delayed_tasks(&self) -> Result<u64> {
        let mut conn = self.get_connection().await?;
        let now = Utc::now().timestamp_millis();

        // Get all tasks ready to execute (score <= now)
        let ready_tasks: Vec<String> = conn
            .zrangebyscore("ergon:queue:delayed", 0, now)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if ready_tasks.is_empty() {
            return Ok(0);
        }

        // Atomically move tasks from delayed to pending
        let count = ready_tasks.len() as u64;
        for task_id in &ready_tasks {
            let _: () = redis::pipe()
                .atomic()
                .zrem("ergon:queue:delayed", task_id)
                .rpush("ergon:queue:pending", task_id)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;
        }

        debug!("Moved {} delayed tasks to pending queue", count);
        Ok(count)
    }

    async fn recover_stale_locks(&self) -> Result<u64> {
        let mut conn = self.get_connection().await?;
        let now = Utc::now().timestamp_millis();
        let stale_cutoff = now - self.stale_lock_timeout_ms;

        // Find flows that started before the cutoff
        let stale_tasks: Vec<String> = conn
            .zrangebyscore("ergon:running", 0, stale_cutoff)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if stale_tasks.is_empty() {
            return Ok(0);
        }

        let count = stale_tasks.len() as u64;
        for task_id_str in &stale_tasks {
            let task_id = match Uuid::parse_str(task_id_str) {
                Ok(id) => id,
                Err(_) => continue,
            };

            let flow_key = Self::flow_key(task_id);

            // Reset flow to pending
            let _: () = redis::pipe()
                .atomic()
                .hset(&flow_key, "status", "Pending")
                .hdel(&flow_key, "locked_by")
                .zrem("ergon:running", task_id_str)
                .rpush("ergon:queue:pending", task_id_str)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            warn!("Recovered stale lock for flow: {}", task_id);
        }

        info!("Recovered {} stale locks", count);
        Ok(count)
    }
}
