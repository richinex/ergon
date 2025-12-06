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

    /// Creates a connection pool with full configuration including stale lock timeout.
    /// Useful for testing with shorter timeouts.
    pub async fn with_full_config(
        redis_url: &str,
        completed_ttl_secs: i64,
        signal_ttl_secs: i64,
        stale_lock_timeout_ms: i64,
    ) -> Result<Self> {
        let cfg = Config::from_url(redis_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self {
            pool,
            completed_ttl: completed_ttl_secs,
            signal_ttl: signal_ttl_secs,
            stale_lock_timeout_ms,
        })
    }

    /// Gets an async connection from the pool.
    async fn get_connection(&self) -> Result<deadpool_redis::Connection> {
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

    /// Builds the Redis key for step-child mapping.
    fn step_child_mapping_key(flow_id: Uuid, parent_step: i32) -> String {
        format!("ergon:step_child:{}:{}", flow_id, parent_step)
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
        let timestamp =
            chrono::DateTime::from_timestamp(timestamp_secs, 0).unwrap_or_else(Utc::now);
        let class_name = get_str("class_name")?;
        let method_name = get_str("method_name")?;
        let status_str = get_str("status")?;
        let status = InvocationStatus::from_str(&status_str)
            .map_err(|e| StorageError::Connection(e.to_string()))?;
        let attempts = get_i32("attempts")?;
        let parameters = data.get("parameters").cloned().unwrap_or_default();
        let params_hash = get_str("params_hash")?.parse::<u64>().unwrap_or(0);
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
        let timer_fire_at = timer_fire_at_millis.and_then(chrono::DateTime::from_timestamp_millis);
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

    /// DIAGNOSTIC: Count the number of pending flows in the queue
    pub async fn count_pending(&self) -> usize {
        let mut conn = match self.get_connection().await {
            Ok(c) => c,
            Err(_) => return 0,
        };

        let count: usize = conn.llen("ergon:queue:pending").await.unwrap_or(0);

        count
    }

    /// DIAGNOSTIC: List all pending task IDs with their flow IDs
    pub async fn list_pending(&self) -> Vec<(String, String)> {
        let mut conn = match self.get_connection().await {
            Ok(c) => c,
            Err(_) => return vec![],
        };

        // Get all task IDs from the pending queue
        let task_ids: Vec<String> = conn
            .lrange("ergon:queue:pending", 0, -1)
            .await
            .unwrap_or_default();

        let mut results = Vec::new();
        for task_id in task_ids {
            let flow_key =
                Self::flow_key(Uuid::parse_str(&task_id).unwrap_or_else(|_| Uuid::nil()));

            // Get flow_id from the flow metadata
            let flow_id: Option<String> = conn.hget(&flow_key, "flow_id").await.unwrap_or(None);

            if let Some(flow_id) = flow_id {
                results.push((task_id, flow_id));
            }
        }

        results
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

        let inv_key = Self::invocation_key(id, step);
        let invocations_list = Self::invocations_key(id);

        // Atomic operation using Lua script to match SQLite's "ON CONFLICT DO UPDATE SET attempts = attempts + 1"
        // If the invocation exists (has 'id' field), only increment attempts; otherwise create it
        let script = r#"
            if redis.call('HEXISTS', KEYS[1], 'id') == 1 then
                return redis.call('HINCRBY', KEYS[1], 'attempts', 1)
            else
                redis.call('HSET', KEYS[1],
                    'id', ARGV[1],
                    'step', ARGV[2],
                    'timestamp', ARGV[3],
                    'class_name', ARGV[4],
                    'method_name', ARGV[5],
                    'status', ARGV[6],
                    'attempts', 1,
                    'parameters', ARGV[7],
                    'params_hash', ARGV[8],
                    'delay_ms', ARGV[9],
                    'retry_policy', ARGV[10],
                    'is_retryable', '',
                    'timer_fire_at', 0,
                    'timer_name', ''
                )
                redis.call('RPUSH', KEYS[2], KEYS[1])
                return 1
            end
        "#;

        let _: i32 = redis::Script::new(script)
            .key(&inv_key)
            .key(&invocations_list)
            .arg(id.to_string())
            .arg(step)
            .arg(timestamp)
            .arg(class_name)
            .arg(method_name)
            .arg(status.as_str())
            .arg(parameters)
            .arg(params_hash.to_string())
            .arg(delay_ms)
            .arg(retry_policy_json)
            .invoke_async(&mut *conn)
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
            .hset(&inv_key, "status", InvocationStatus::Complete.as_str())
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
        let mut conn = self.get_connection().await?;
        let invocations_key = format!("ergon:invocations:{}", flow_id);

        // Use Lua script to check all invocations without fetching full data
        // This avoids N+1 queries and reduces network overhead
        let script = redis::Script::new(
            r#"
            local keys = redis.call('LRANGE', KEYS[1], 0, -1)
            for _, key in ipairs(keys) do
                local is_retryable = redis.call('HGET', key, 'is_retryable')
                if is_retryable == '0' then
                    return 1
                end
            end
            return 0
            "#,
        );

        let has_non_retryable: i32 = script
            .key(&invocations_key)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(has_non_retryable == 1)
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

        debug!("Found {} keys to delete", all_keys.len());

        // Delete all found keys in batches
        if !all_keys.is_empty() {
            for chunk in all_keys.chunks(100) {
                // Use redis::cmd with explicit DEL command
                let mut cmd = redis::cmd("DEL");
                for key in chunk {
                    cmd.arg(key);
                }
                let deleted: usize = cmd
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;
                debug!("Deleted {} keys from batch", deleted);
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
            let mut pipe = redis::pipe();
            pipe.atomic()
                .hset(&flow_key, "task_id", task_id.to_string())
                .hset(&flow_key, "flow_id", flow.flow_id.to_string())
                .hset(&flow_key, "flow_type", &flow.flow_type)
                .hset(&flow_key, "status", "PENDING")
                .hset(&flow_key, "created_at", flow.created_at.timestamp())
                .hset(&flow_key, "updated_at", flow.updated_at.timestamp())
                .hset(&flow_key, "scheduled_for", scheduled_ts)
                .hset(&flow_key, "flow_data", flow.flow_data.as_slice());

            // Level 3: Add parent metadata if present
            if let Some(parent_id) = flow.parent_flow_id {
                pipe.hset(&flow_key, "parent_flow_id", parent_id.to_string());
            }
            if let Some(ref signal_token) = flow.signal_token {
                pipe.hset(&flow_key, "signal_token", signal_token);
            }

            // Add to delayed queue (sorted by scheduled_for timestamp)
            // Also create flow_id -> task_id index for O(1) resume_flow lookup
            let _: () = pipe
                .set(
                    format!("ergon:flow_task_map:{}", flow.flow_id),
                    task_id.to_string(),
                )
                .zadd("ergon:queue:delayed", task_id.to_string(), scheduled_ts)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;
        } else {
            // Immediate execution - add to pending queue
            // Use Lua script for guaranteed atomicity: data THEN queue
            let script = redis::Script::new(
                r#"
                local flow_key = KEYS[1]
                local task_id = ARGV[1]
                local flow_id = ARGV[2]
                local flow_type = ARGV[3]
                local created_at = ARGV[4]
                local updated_at = ARGV[5]
                local flow_data = ARGV[6]
                local parent_flow_id = ARGV[7]
                local signal_token = ARGV[8]

                -- Set all flow metadata FIRST
                redis.call('HSET', flow_key,
                    'task_id', task_id,
                    'flow_id', flow_id,
                    'flow_type', flow_type,
                    'status', 'PENDING',
                    'created_at', created_at,
                    'updated_at', updated_at,
                    'flow_data', flow_data)

                -- Add optional parent metadata
                if parent_flow_id ~= '' then
                    redis.call('HSET', flow_key, 'parent_flow_id', parent_flow_id)
                end
                if signal_token ~= '' then
                    redis.call('HSET', flow_key, 'signal_token', signal_token)
                end

                -- Create flow_id -> task_id index for O(1) resume_flow lookup
                redis.call('SET', 'ergon:flow_task_map:' .. flow_id, task_id)

                -- ONLY AFTER data is written, add to queue
                redis.call('RPUSH', 'ergon:queue:pending', task_id)

                return redis.status_reply('OK')
            "#,
            );

            let parent_id_str = flow
                .parent_flow_id
                .map(|id| id.to_string())
                .unwrap_or_default();
            let signal_token_str = flow.signal_token.unwrap_or_default();

            let _: String = script
                .key(&flow_key)
                .arg(task_id.to_string())
                .arg(flow.flow_id.to_string())
                .arg(&flow.flow_type)
                .arg(flow.created_at.timestamp())
                .arg(flow.updated_at.timestamp())
                .arg(flow.flow_data.as_slice())
                .arg(parent_id_str)
                .arg(signal_token_str)
                .invoke_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;
        }

        Ok(task_id)
    }

    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<super::ScheduledFlow>> {
        let mut conn = self.get_connection().await?;
        let processing_key = format!("ergon:processing:{}", worker_id);
        let now = Utc::now().timestamp();
        let now_millis = Utc::now().timestamp_millis();

        // Single atomic LPOP with rollback on validation failure
        // If Rust loses the response, task is marked RUNNING and recover_stale_locks will handle it
        // This provides at-least-once delivery (task may run twice after recovery, but never lost)
        let script = redis::Script::new(
            r#"
            -- Atomically dequeue with validation and rollback capability
            local task_id = redis.call('LPOP', 'ergon:queue:pending')
            if not task_id then
                return nil
            end

            local flow_key = 'ergon:flow:' .. task_id

            -- Fetch ALL required fields
            local flow_id = redis.call('HGET', flow_key, 'flow_id')
            local flow_type = redis.call('HGET', flow_key, 'flow_type')
            local flow_data = redis.call('HGET', flow_key, 'flow_data')
            local created_at = redis.call('HGET', flow_key, 'created_at')

            -- Validate ALL required fields individually
            if not flow_id or flow_id == '' then
                redis.call('RPUSH', 'ergon:debug:dequeue', 'REJECT: missing flow_id for ' .. task_id)
                redis.call('LPUSH', 'ergon:queue:pending', task_id)
                return nil
            end
            if not flow_type or flow_type == '' then
                redis.call('RPUSH', 'ergon:debug:dequeue', 'REJECT: missing flow_type for ' .. task_id)
                redis.call('LPUSH', 'ergon:queue:pending', task_id)
                return nil
            end
            if not flow_data then
                redis.call('RPUSH', 'ergon:debug:dequeue', 'REJECT: missing flow_data for ' .. task_id)
                redis.call('LPUSH', 'ergon:queue:pending', task_id)
                return nil
            end
            if not created_at then
                redis.call('RPUSH', 'ergon:debug:dequeue', 'REJECT: missing created_at for ' .. task_id)
                redis.call('LPUSH', 'ergon:queue:pending', task_id)
                return nil
            end

            -- Validation passed - mark as RUNNING and add to processing queue
            redis.call('RPUSH', KEYS[1], task_id)
            redis.call('HSET', flow_key, 'status', 'RUNNING', 'locked_by', ARGV[1], 'updated_at', ARGV[2])
            redis.call('ZADD', 'ergon:running', ARGV[3], task_id)

            -- Fetch optional fields
            local retry_count = redis.call('HGET', flow_key, 'retry_count') or '0'
            local parent_flow_id = redis.call('HGET', flow_key, 'parent_flow_id') or ''
            local signal_token = redis.call('HGET', flow_key, 'signal_token') or ''

            -- Return task data
            -- NOTE: If this response is lost, task is RUNNING and will be recovered by stale lock detection
            return {task_id, flow_id, flow_type, flow_data, created_at, retry_count, parent_flow_id, signal_token}
        "#,
        );

        // Wrap in timeout to prevent indefinite hangs (10 second timeout)
        let timeout_duration = tokio::time::Duration::from_secs(10);
        let result: Option<Vec<redis::Value>> = match tokio::time::timeout(
            timeout_duration,
            script
                .key(&processing_key)
                .arg(worker_id)
                .arg(now)
                .arg(now_millis)
                .invoke_async(&mut *conn)
        ).await {
            Ok(Ok(val)) => val,
            Ok(Err(e)) => {
                return Err(StorageError::Connection(e.to_string()));
            }
            Err(_) => {
                // Timeout means invoke_async hung - drop connection and return None
                // The task is already marked RUNNING in Redis and will be recovered by stale lock detection
                warn!("Lua dequeue script timeout after {:?} - task will be recovered by stale lock detection", timeout_duration);
                return Ok(None);
            }
        };

        let Some(values) = result else {
            return Ok(None);
        };

        // Parse the returned values
        // Helper to extract string from redis::Value
        let get_string = |v: &redis::Value| -> Option<String> {
            match v {
                redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
                redis::Value::SimpleString(s) => Some(s.clone()),
                _ => None,
            }
        };

        let get_bytes = |v: &redis::Value| -> Option<Vec<u8>> {
            match v {
                redis::Value::BulkString(bytes) => Some(bytes.clone()),
                _ => None,
            }
        };

        if values.len() < 8 {
            warn!("Incomplete data from Lua dequeue (expected 8 values, got {})", values.len());
            return Ok(None);
        }

        // Parse task_id first (needed for error recovery)
        let task_id_str = match get_string(&values[0]) {
            Some(s) => s,
            None => {
                warn!("Missing task_id in Lua dequeue response");
                return Ok(None);
            }
        };
        let task_id = match Uuid::parse_str(&task_id_str) {
            Ok(id) => id,
            Err(e) => {
                warn!("Invalid task_id '{}': {}", task_id_str, e);
                return Ok(None);
            }
        };

        // Parse remaining required fields with error recovery
        let flow_id_str = match get_string(&values[1]) {
            Some(s) if !s.is_empty() => s,
            _ => {
                // Mark task as failed to prevent orphaning
                warn!("Missing flow_id for task {}, marking as FAILED", task_id);
                let mut conn = self.get_connection().await?;
                let flow_key = format!("ergon:flow:{}", task_id);
                let _: () = redis::pipe()
                    .atomic()
                    .hset(&flow_key, "status", "FAILED")
                    .hset(&flow_key, "error_message", "Missing flow_id in dequeue data")
                    .zrem("ergon:running", task_id.to_string())
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;
                return Ok(None);
            }
        };
        let flow_id = match Uuid::parse_str(&flow_id_str) {
            Ok(id) => id,
            Err(e) => {
                warn!("Invalid flow_id for task {}: {}, marking as FAILED", task_id, e);
                let mut conn = self.get_connection().await?;
                let flow_key = format!("ergon:flow:{}", task_id);
                let _: () = redis::pipe()
                    .atomic()
                    .hset(&flow_key, "status", "FAILED")
                    .hset(&flow_key, "error_message", format!("Invalid flow_id: {}", e))
                    .zrem("ergon:running", task_id.to_string())
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;
                return Ok(None);
            }
        };

        let flow_type = match get_string(&values[2]) {
            Some(s) if !s.is_empty() => s,
            _ => {
                warn!("Missing flow_type for task {}, marking as FAILED", task_id);
                let mut conn = self.get_connection().await?;
                let flow_key = format!("ergon:flow:{}", task_id);
                let _: () = redis::pipe()
                    .atomic()
                    .hset(&flow_key, "status", "FAILED")
                    .hset(&flow_key, "error_message", "Missing flow_type in dequeue data")
                    .zrem("ergon:running", task_id.to_string())
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;
                return Ok(None);
            }
        };

        let flow_data = match get_bytes(&values[3]) {
            Some(d) if !d.is_empty() => d,
            _ => {
                warn!("Missing flow_data for task {}, marking as FAILED", task_id);
                let mut conn = self.get_connection().await?;
                let flow_key = format!("ergon:flow:{}", task_id);
                let _: () = redis::pipe()
                    .atomic()
                    .hset(&flow_key, "status", "FAILED")
                    .hset(&flow_key, "error_message", "Missing flow_data in dequeue data")
                    .zrem("ergon:running", task_id.to_string())
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;
                return Ok(None);
            }
        };

        let created_at_ts: i64 = get_string(&values[4])
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let created_at =
            chrono::DateTime::from_timestamp(created_at_ts, 0).unwrap_or_else(Utc::now);

        let retry_count: u32 = get_string(&values[5])
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let parent_flow_id: Option<Uuid> = get_string(&values[6])
            .filter(|s| !s.is_empty())
            .and_then(|s| Uuid::parse_str(&s).ok());

        let signal_token: Option<String> = get_string(&values[7]).filter(|s| !s.is_empty());

        let updated_at = chrono::DateTime::from_timestamp(now, 0).unwrap_or_else(Utc::now);

        debug!(
            "Worker {} dequeued flow: task_id={}, flow_id={}, flow_type={}",
            worker_id,
            &task_id.to_string()[..8],
            &flow_id.to_string()[..8],
            &flow_type
        );

        Ok(Some(super::ScheduledFlow {
            task_id,
            flow_id,
            flow_type,
            flow_data,
            status: super::TaskStatus::Running,
            locked_by: Some(worker_id.to_string()),
            created_at,
            updated_at,
            retry_count,
            error_message: None,
            scheduled_for: None,
            parent_flow_id,
            signal_token,
        }))
    }

    async fn complete_flow(&self, task_id: Uuid, status: super::TaskStatus) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let flow_key = Self::flow_key(task_id);
        let now = Utc::now().timestamp();

        // Get worker_id to remove from correct processing list
        let locked_by: Option<String> = conn
            .hget(&flow_key, "locked_by")
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let mut pipe = redis::pipe();
        pipe.atomic()
            .hset(&flow_key, "status", status.as_str())
            .hset(&flow_key, "updated_at", now)
            .zrem("ergon:running", task_id.to_string())
            // Set TTL on completed flow for automatic cleanup
            .expire(&flow_key, self.completed_ttl);

        // Remove from worker's processing list (reliable queue pattern)
        if let Some(worker_id) = locked_by {
            let processing_key = format!("ergon:processing:{}", worker_id);
            pipe.lrem(&processing_key, 1, task_id.to_string());
        }

        let _: () = pipe
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
        let flow_id = Uuid::parse_str(&String::from_utf8_lossy(
            data.get("flow_id")
                .ok_or_else(|| StorageError::Connection("Missing flow_id".to_string()))?,
        ))
        .map_err(|e| StorageError::Connection(e.to_string()))?;

        let flow_type = String::from_utf8_lossy(
            data.get("flow_type")
                .ok_or_else(|| StorageError::Connection("Missing flow_type".to_string()))?,
        )
        .to_string();

        let flow_data = data
            .get("flow_data")
            .ok_or_else(|| StorageError::Connection("Missing flow_data".to_string()))?
            .clone();

        let status_str = String::from_utf8_lossy(
            data.get("status")
                .ok_or_else(|| StorageError::Connection("Missing status".to_string()))?,
        );
        let status: super::TaskStatus = status_str
            .parse()
            .map_err(|e: String| StorageError::Connection(e))?;

        let locked_by = data
            .get("locked_by")
            .map(|v| String::from_utf8_lossy(v).to_string());

        let created_at_ts: i64 = String::from_utf8_lossy(
            data.get("created_at")
                .ok_or_else(|| StorageError::Connection("Missing created_at".to_string()))?,
        )
        .parse()
        .unwrap_or(0);
        let created_at =
            chrono::DateTime::from_timestamp(created_at_ts, 0).unwrap_or_else(Utc::now);

        let updated_at_ts: i64 = String::from_utf8_lossy(
            data.get("updated_at")
                .ok_or_else(|| StorageError::Connection("Missing updated_at".to_string()))?,
        )
        .parse()
        .unwrap_or(0);
        let updated_at =
            chrono::DateTime::from_timestamp(updated_at_ts, 0).unwrap_or_else(Utc::now);

        let retry_count: u32 = String::from_utf8_lossy(data.get("retry_count").unwrap_or(&vec![]))
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

        // Extract Level 3 parent metadata if present
        let parent_flow_id: Option<String> = data
            .get("parent_flow_id")
            .map(|v| String::from_utf8_lossy(v).to_string());
        let parent_flow_id = parent_flow_id.and_then(|s| uuid::Uuid::parse_str(&s).ok());
        let signal_token: Option<String> = data
            .get("signal_token")
            .map(|v| String::from_utf8_lossy(v).to_string());

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
            parent_flow_id,
            signal_token,
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
            let fire_at =
                chrono::DateTime::from_timestamp_millis(fire_at_ms).unwrap_or_else(Utc::now);

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

        // Timer steps return Result<(), ExecutionError>, so we need to serialize Ok(()) properly
        use crate::executor::ExecutionError as ExecError;
        let result_ok: std::result::Result<(), ExecError> = Ok(());
        let unit_value = crate::core::serialize_value(&result_ok)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // Use Lua script for atomic check-and-set to prevent race conditions
        // between status check and update
        let script = redis::Script::new(
            r#"
            local status = redis.call('HGET', KEYS[1], 'status')
            if status ~= 'WAITING_FOR_TIMER' then
                return 0
            end
            redis.call('HSET', KEYS[1], 'status', 'COMPLETE')
            redis.call('HSET', KEYS[1], 'return_value', ARGV[1])
            redis.call('ZREM', KEYS[2], ARGV[2])
            return 1
            "#,
        );

        let claimed: i32 = script
            .key(&inv_key)
            .key("ergon:timers:pending")
            .arg(unit_value)
            .arg(&timer_key)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(claimed == 1)
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
            .hset(
                &inv_key,
                "status",
                InvocationStatus::WaitingForTimer.as_str(),
            )
            .hset(&inv_key, "timer_fire_at", fire_at_ms)
            .hset(&inv_key, "timer_name", timer_name.unwrap_or(""))
            // Add to timers sorted set (score = fire_at timestamp)
            .zadd("ergon:timers:pending", &timer_key, fire_at_ms)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn log_signal(&self, flow_id: Uuid, step: i32, signal_name: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let inv_key = Self::invocation_key(flow_id, step);

        // Update invocation status to WAITING_FOR_SIGNAL
        let _: () = redis::pipe()
            .atomic()
            .hset(
                &inv_key,
                "status",
                InvocationStatus::WaitingForSignal.as_str(),
            )
            .hset(&inv_key, "timer_name", signal_name)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        debug!(
            "Logged signal wait: flow_id={}, step={}, signal_name={}",
            flow_id, step, signal_name
        );

        Ok(())
    }

    async fn resume_flow(&self, flow_id: Uuid) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // O(1) lookup using flow_id -> task_id index (no more slow SCAN!)
        let task_id_str: Option<String> = conn
            .get(format!("ergon:flow_task_map:{}", flow_id))
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let task_id = task_id_str
            .and_then(|s| Uuid::parse_str(&s).ok())
            .ok_or_else(|| {
                StorageError::Connection(format!("Task not found for flow_id: {}", flow_id))
            })?;

        let flow_key = Self::flow_key(task_id);
        let now = Utc::now().timestamp();

        // Check flow status
        let status: Option<String> = conn
            .hget(&flow_key, "status")
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // Resume if SUSPENDED, or if RUNNING (race condition: child completed while parent being processed)
        // The dequeue Lua script will handle checking if task is already in queue
        match status.as_deref() {
            Some("SUSPENDED") | Some("RUNNING") => {
                // Resume the flow by moving to PENDING and adding to queue
                let _: () = redis::pipe()
                    .atomic()
                    .hset(&flow_key, "status", "PENDING")
                    .hdel(&flow_key, "locked_by")
                    .hset(&flow_key, "updated_at", now)
                    .zrem("ergon:running", task_id.to_string())
                    // Use LPUSH to add to front for priority (child just completed)
                    .lpush("ergon:queue:pending", task_id.to_string())
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                debug!(
                    "Resumed flow: flow_id={}, task_id={}, was_status={:?}",
                    flow_id, task_id, status
                );
            }
            Some("COMPLETE") | Some("FAILED") => {
                // Already completed - this is fine (duplicate signal/child completion)
                debug!(
                    "Flow already completed: flow_id={}, task_id={}",
                    flow_id, task_id
                );
                return Ok(());
            }
            Some("PENDING") => {
                // Already in queue - this is fine
                debug!(
                    "Flow already pending: flow_id={}, task_id={}",
                    flow_id, task_id
                );
                return Ok(());
            }
            _ => {
                return Err(StorageError::Connection(format!(
                    "Task in unexpected state {:?} for flow_id: {}",
                    status, flow_id
                )));
            }
        }

        Ok(())
    }

    // ===== Signal Operations =====

    async fn store_signal_params(&self, flow_id: Uuid, step: i32, params: &[u8]) -> Result<()> {
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

        debug!("Removed signal params: flow_id={}, step={}", flow_id, step);
        Ok(())
    }

    async fn get_waiting_signals(&self) -> Result<Vec<super::SignalInfo>> {
        let mut conn = self.get_connection().await?;

        // Scan for all invocation keys and check status
        let mut cursor = 0u64;
        let mut signals = Vec::new();

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .cursor_arg(cursor)
                .arg("MATCH")
                .arg("ergon:inv:*")
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            for inv_key in keys {
                // Get status from invocation
                let status: Option<String> = conn
                    .hget(&inv_key, "status")
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                if let Some(s) = status {
                    if s == "WAITING_FOR_SIGNAL" {
                        // Parse key format: "ergon:inv:flow_id:step"
                        let parts: Vec<&str> = inv_key.split(':').collect();
                        if parts.len() == 4 {
                            if let (Ok(flow_id), Ok(step)) =
                                (Uuid::parse_str(parts[2]), parts[3].parse::<i32>())
                            {
                                let signal_name: Option<String> = conn
                                    .hget(&inv_key, "timer_name")
                                    .await
                                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                                let signal_name = signal_name.filter(|s| !s.is_empty());

                                signals.push(super::SignalInfo {
                                    flow_id,
                                    step,
                                    signal_name,
                                });
                            }
                        }
                    }
                }
            }

            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        Ok(signals)
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
                    if (s == "COMPLETE" || s == "FAILED") && ts < cutoff_ts {
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

        // Use Lua script to atomically find and move ready tasks
        // This prevents race conditions where multiple workers try to move the same tasks
        let script = redis::Script::new(
            r#"
            local ready = redis.call('ZRANGEBYSCORE', 'ergon:queue:delayed', 0, ARGV[1], 'LIMIT', 0, 100)
            local count = 0
            for _, task_id in ipairs(ready) do
                redis.call('ZREM', 'ergon:queue:delayed', task_id)
                redis.call('RPUSH', 'ergon:queue:pending', task_id)
                count = count + 1
            end
            return count
            "#,
        );

        let count: u64 = script
            .arg(now)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if count > 0 {
            debug!("Moved {} delayed tasks to pending queue", count);
        }
        Ok(count)
    }

    async fn recover_stale_locks(&self) -> Result<u64> {
        let mut conn = self.get_connection().await?;
        let now = Utc::now().timestamp_millis();
        let stale_cutoff = now - self.stale_lock_timeout_ms;

        // Find flows that started before the cutoff (in ergon:running)
        let stale_tasks: Vec<String> = conn
            .zrangebyscore("ergon:running", 0, stale_cutoff)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let mut recovered_count = stale_tasks.len() as u64;

        for task_id_str in &stale_tasks {
            let task_id = match Uuid::parse_str(task_id_str) {
                Ok(id) => id,
                Err(_) => continue,
            };

            let flow_key = Self::flow_key(task_id);

            // Get worker_id to remove from processing list
            let locked_by: Option<String> = conn
                .hget(&flow_key, "locked_by")
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            let mut pipe = redis::pipe();
            pipe.atomic()
                .hset(&flow_key, "status", "PENDING")
                .hdel(&flow_key, "locked_by")
                .zrem("ergon:running", task_id_str)
                .rpush("ergon:queue:pending", task_id_str);

            // Remove from worker's processing list (reliable queue pattern)
            if let Some(worker_id) = locked_by {
                let processing_key = format!("ergon:processing:{}", worker_id);
                pipe.lrem(&processing_key, 1, task_id_str);
            }

            let _: () = pipe
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            warn!("Recovered stale lock for flow: {}", task_id);
        }

        // Also scan all processing lists for orphaned tasks (tasks in processing but not in running)
        // This handles the LPOP loss scenario where task was dequeued but never marked RUNNING
        let mut cursor = 0u64;
        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .cursor_arg(cursor)
                .arg("MATCH")
                .arg("ergon:processing:*")
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            cursor = new_cursor;

            for processing_key in keys {
                // Get all tasks in this processing list
                let tasks: Vec<String> = conn
                    .lrange(&processing_key, 0, -1)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                for task_id_str in tasks {
                    // Check if task is in running set
                    let score: Option<i64> = conn
                        .zscore("ergon:running", &task_id_str)
                        .await
                        .map_err(|e| StorageError::Connection(e.to_string()))?;

                    // If not in running set, it's orphaned - move back to pending
                    if score.is_none() {
                        let task_id = match Uuid::parse_str(&task_id_str) {
                            Ok(id) => id,
                            Err(_) => continue,
                        };

                        let flow_key = Self::flow_key(task_id);

                        let _: () = redis::pipe()
                            .atomic()
                            .hset(&flow_key, "status", "PENDING")
                            .lrem(&processing_key, 1, &task_id_str)
                            .rpush("ergon:queue:pending", &task_id_str)
                            .query_async(&mut *conn)
                            .await
                            .map_err(|e| StorageError::Connection(e.to_string()))?;

                        warn!("Recovered orphaned task from processing list: {}", task_id);
                        recovered_count += 1;
                    }
                }
            }

            if cursor == 0 {
                break;
            }
        }

        if recovered_count > 0 {
            info!("Recovered {} stale/orphaned flows", recovered_count);
        }
        Ok(recovered_count)
    }

    async fn store_step_child_mapping(
        &self,
        flow_id: Uuid,
        parent_step: i32,
        child_step: i32,
    ) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let mapping_key = Self::step_child_mapping_key(flow_id, parent_step);

        // Store the child step number as a simple string value
        let _: () = conn
            .set(&mapping_key, child_step)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn get_child_step_for_parent(
        &self,
        flow_id: Uuid,
        parent_step: i32,
    ) -> Result<Option<i32>> {
        let mut conn = self.get_connection().await?;
        let mapping_key = Self::step_child_mapping_key(flow_id, parent_step);

        // Check if key exists first
        let exists: bool = conn
            .exists(&mapping_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Ok(None);
        }

        // Get the child step number
        let child_step: i32 = conn
            .get(&mapping_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Some(child_step))
    }
}
