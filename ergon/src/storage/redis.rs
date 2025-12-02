//! Redis-based execution log implementation.
//!
//! This module provides a Redis backend for distributed execution with true
//! multi-machine support. Unlike SQLite which requires shared filesystem access,
//! Redis enables workers to run on completely separate machines.
//!
//! # Data Structures
//!
//! - `ergon:queue:pending` (LIST): FIFO queue of pending task IDs
//! - `ergon:flow:{task_id}` (HASH): Flow metadata and serialized data
//! - `ergon:invocations:{flow_id}` (LIST): Invocation history per flow
//! - `ergon:running` (ZSET): Running flows index (score = start time)
//! - `ergon:timers:pending` (ZSET): Pending timers (score = fire_at timestamp)
//! - `ergon:inv:{flow_id}:{step}` (HASH): Invocation data including timer info
//!
//! # Key Features
//!
//! - **Blocking dequeue**: Uses BLPOP for efficient worker polling
//! - **Atomic operations**: Uses MULTI/EXEC for consistency
//! - **Network-accessible**: True distributed execution across machines
//! - **Connection pooling**: r2d2 pool for concurrent access
//!
//! # Performance Characteristics
//!
//! - Enqueue: O(1) with RPUSH
//! - Dequeue: O(1) with BLPOP (blocks until available)
//! - Status update: O(1) with HSET
//! - Network overhead: ~0.1-0.5ms per operation (local network)

use super::{error::Result, error::StorageError, params::InvocationStartParams, ExecutionLog};
use crate::core::{hash_params, Invocation, InvocationStatus};
use async_trait::async_trait;
use chrono::Utc;
use redis::Commands;
use std::collections::HashMap;
use uuid::Uuid;

/// Redis execution log using connection pooling.
///
/// This implementation uses Redis data structures optimized for distributed
/// task queue processing with support for multiple workers across different
/// machines.
///
/// # Example
///
/// ```no_run
/// use ergon::storage::RedisExecutionLog;
/// use std::sync::Arc;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let redis_url = "redis://127.0.0.1:6379";
/// let storage = Arc::new(RedisExecutionLog::new(redis_url)?);
/// # Ok(())
/// # }
/// ```
pub struct RedisExecutionLog {
    pool: r2d2::Pool<redis::Client>,
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
    /// let storage = RedisExecutionLog::new("redis://localhost:6379")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(redis_url: &str) -> Result<Self> {
        let client =
            redis::Client::open(redis_url).map_err(|e| StorageError::Connection(e.to_string()))?;

        let pool = r2d2::Pool::builder()
            .max_size(16)
            .build(client)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self { pool })
    }

    /// Creates a connection pool with custom configuration.
    pub fn with_pool_config(redis_url: &str, max_connections: u32) -> Result<Self> {
        let client =
            redis::Client::open(redis_url).map_err(|e| StorageError::Connection(e.to_string()))?;

        let pool = r2d2::Pool::builder()
            .max_size(max_connections)
            .build(client)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self { pool })
    }

    /// Gets a connection from the pool.
    fn get_connection(&self) -> Result<r2d2::PooledConnection<redis::Client>> {
        self.pool
            .get()
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
}

#[async_trait]
impl ExecutionLog for RedisExecutionLog {
    async fn log_invocation_start(&self, params: InvocationStartParams<'_>) -> Result<()> {
        let mut conn = self.get_connection()?;

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
        let delay_ms = delay.map(|d| d.as_millis() as i64);
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
            .hset(&inv_key, "status", format!("{:?}", status))
            .hset(&inv_key, "attempts", 1)
            .hset(&inv_key, "parameters", parameters)
            .hset(&inv_key, "params_hash", params_hash)
            .hset(&inv_key, "delay_ms", delay_ms.unwrap_or(0))
            .hset(&inv_key, "retry_policy", retry_policy_json)
            .hset(&inv_key, "is_retryable", "") // Empty string = None (not an error yet)
            .lpush(Self::invocations_key(id), &inv_key)
            .query(&mut *conn)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn log_invocation_completion(
        &self,
        id: Uuid,
        step: i32,
        return_value: &[u8],
    ) -> Result<Invocation> {
        let mut conn = self.get_connection()?;
        let inv_key = Self::invocation_key(id, step);

        // Check if invocation exists
        let exists: bool = conn
            .exists(&inv_key)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Err(StorageError::InvocationNotFound { id, step });
        }

        // Update status and return value
        let _: () = redis::pipe()
            .atomic()
            .hset(&inv_key, "status", "Complete")
            .hset(&inv_key, "return_value", return_value)
            .query(&mut *conn)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // Fetch and reconstruct invocation
        let data: HashMap<String, Vec<u8>> = conn
            .hgetall(&inv_key)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        self.parse_invocation(data)
    }

    async fn get_invocation(&self, id: Uuid, step: i32) -> Result<Option<Invocation>> {
        let mut conn = self.get_connection()?;
        let inv_key = Self::invocation_key(id, step);

        let exists: bool = conn
            .exists(&inv_key)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Ok(None);
        }

        let data: HashMap<String, Vec<u8>> = conn
            .hgetall(&inv_key)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Some(self.parse_invocation(data)?))
    }

    async fn get_latest_invocation(&self, id: Uuid) -> Result<Option<Invocation>> {
        let invocations = self.get_invocations_for_flow(id).await?;
        Ok(invocations.into_iter().max_by_key(|inv| inv.step()))
    }

    async fn get_invocations_for_flow(&self, id: Uuid) -> Result<Vec<Invocation>> {
        let mut conn = self.get_connection()?;
        let inv_list_key = Self::invocations_key(id);

        // Get all invocation keys for this flow
        let inv_keys: Vec<String> = conn
            .lrange(&inv_list_key, 0, -1)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let mut invocations = Vec::new();
        for key in inv_keys {
            let data: HashMap<String, Vec<u8>> = conn
                .hgetall(&key)
                .map_err(|e| StorageError::Connection(e.to_string()))?;
            invocations.push(self.parse_invocation(data)?);
        }

        invocations.sort_by_key(|inv| inv.step());
        Ok(invocations)
    }

    async fn get_incomplete_flows(&self) -> Result<Vec<Invocation>> {
        let mut conn = self.get_connection()?;

        // Scan for all invocation keys and filter incomplete
        let keys: Vec<String> = conn
            .keys("ergon:inv:*")
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let mut incomplete = Vec::new();
        for key in keys {
            let data: HashMap<String, Vec<u8>> = conn
                .hgetall(&key)
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            if let Ok(inv) = self.parse_invocation(data) {
                if inv.status() != InvocationStatus::Complete {
                    incomplete.push(inv);
                }
            }
        }

        Ok(incomplete)
    }

    async fn has_non_retryable_error(&self, flow_id: Uuid) -> Result<bool> {
        let mut conn = self.get_connection()?;
        let inv_list_key = Self::invocations_key(flow_id);

        // Get all invocation keys for this flow
        let inv_keys: Vec<String> = conn
            .lrange(&inv_list_key, 0, -1)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // Check if any invocation has is_retryable = "0"
        for key in inv_keys {
            let is_retryable: String = conn
                .hget(&key, "is_retryable")
                .unwrap_or_else(|_| String::new());

            if is_retryable == "0" {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn update_is_retryable(&self, id: Uuid, step: i32, is_retryable: bool) -> Result<()> {
        let mut conn = self.get_connection()?;
        let inv_key = Self::invocation_key(id, step);

        // Check if invocation exists
        let exists: bool = conn
            .exists(&inv_key)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Err(StorageError::InvocationNotFound { id, step });
        }

        // Update is_retryable field
        let value = if is_retryable { "1" } else { "0" };
        let _: () = conn
            .hset(&inv_key, "is_retryable", value)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn reset(&self) -> Result<()> {
        let mut conn = self.get_connection()?;

        // Delete all ergon keys
        let keys: Vec<String> = conn
            .keys("ergon:*")
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !keys.is_empty() {
            let _: () = conn
                .del(keys)
                .map_err(|e| StorageError::Connection(e.to_string()))?;
        }

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        // Connection pool handles cleanup automatically
        Ok(())
    }

    // ===== Distributed Queue Operations =====

    async fn enqueue_flow(&self, flow: super::ScheduledFlow) -> Result<Uuid> {
        let mut conn = self.get_connection()?;
        let task_id = flow.task_id;
        let flow_key = Self::flow_key(task_id);

        // Atomically store flow metadata and enqueue
        let _: () = redis::pipe()
            .atomic()
            // Store flow metadata as HASH
            .hset(&flow_key, "task_id", task_id.to_string())
            .hset(&flow_key, "flow_id", flow.flow_id.to_string())
            .hset(&flow_key, "flow_type", &flow.flow_type)
            .hset(&flow_key, "status", "Pending")
            .hset(&flow_key, "created_at", flow.created_at.timestamp())
            .hset(&flow_key, "updated_at", flow.updated_at.timestamp())
            .hset(&flow_key, "flow_data", flow.flow_data.as_slice())
            // Add to pending queue (FIFO)
            .rpush("ergon:queue:pending", task_id.to_string())
            .query(&mut *conn)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(task_id)
    }

    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<super::ScheduledFlow>> {
        let mut conn = self.get_connection()?;

        // Blocking pop with 1 second timeout (efficient!)
        let result: Option<(String, String)> = conn
            .blpop("ergon:queue:pending", 1.0)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if let Some((_queue_name, task_id_str)) = result {
            let task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            let flow_key = Self::flow_key(task_id);
            let now = Utc::now().timestamp();

            // Check if flow is ready to execute (scheduled_for has passed)
            let scheduled_for: Option<i64> = conn.hget(&flow_key, "scheduled_for").ok();

            if let Some(scheduled_ts) = scheduled_for {
                if scheduled_ts > now {
                    // Flow not ready yet, push back to queue
                    let _: () = conn
                        .rpush("ergon:queue:pending", task_id.to_string())
                        .map_err(|e| StorageError::Connection(e.to_string()))?;
                    return Ok(None);
                }
            }

            // Atomically update status and lock
            let _: () = redis::pipe()
                .atomic()
                .hset(&flow_key, "status", "Running")
                .hset(&flow_key, "locked_by", worker_id)
                .hset(&flow_key, "updated_at", now)
                .zadd("ergon:running", &task_id_str, now)
                .query(&mut *conn)
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            // Fetch flow metadata
            let data: HashMap<String, Vec<u8>> = conn
                .hgetall(&flow_key)
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            let flow = self.parse_scheduled_flow(data)?;
            return Ok(Some(flow));
        }

        Ok(None)
    }

    async fn complete_flow(&self, task_id: Uuid, status: super::TaskStatus) -> Result<()> {
        let mut conn = self.get_connection()?;
        let flow_key = Self::flow_key(task_id);

        // Check if flow exists
        let exists: bool = conn
            .exists(&flow_key)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Err(StorageError::ScheduledFlowNotFound(task_id));
        }

        let status_str = match status {
            super::TaskStatus::Pending => "Pending",
            super::TaskStatus::Running => "Running",
            super::TaskStatus::Complete => "Complete",
            super::TaskStatus::Failed => "Failed",
        };

        // Atomically update status and remove from running index
        let _: () = redis::pipe()
            .atomic()
            .hset(&flow_key, "status", status_str)
            .hset(&flow_key, "updated_at", Utc::now().timestamp())
            .zrem("ergon:running", task_id.to_string())
            .query(&mut *conn)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn retry_flow(
        &self,
        task_id: Uuid,
        error_message: String,
        delay: std::time::Duration,
    ) -> Result<()> {
        let mut conn = self.get_connection()?;
        let flow_key = Self::flow_key(task_id);

        // Check if flow exists
        let exists: bool = conn
            .exists(&flow_key)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Err(StorageError::ScheduledFlowNotFound(task_id));
        }

        // Get current retry_count
        let retry_count: u32 = conn.hget(&flow_key, "retry_count").unwrap_or(0);

        // Calculate scheduled_for timestamp (current time + delay)
        let scheduled_for = Utc::now() + chrono::Duration::from_std(delay).unwrap();

        // Atomically update flow for retry
        let _: () = redis::pipe()
            .atomic()
            .hset(&flow_key, "retry_count", retry_count + 1)
            .hset(&flow_key, "error_message", error_message)
            .hset(&flow_key, "status", "Pending")
            .hdel(&flow_key, "locked_by")
            .hset(&flow_key, "scheduled_for", scheduled_for.timestamp())
            .hset(&flow_key, "updated_at", Utc::now().timestamp())
            .zrem("ergon:running", task_id.to_string())
            .rpush("ergon:queue:pending", task_id.to_string())
            .query(&mut *conn)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn get_scheduled_flow(&self, task_id: Uuid) -> Result<Option<super::ScheduledFlow>> {
        let mut conn = self.get_connection()?;
        let flow_key = Self::flow_key(task_id);

        let exists: bool = conn
            .exists(&flow_key)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Ok(None);
        }

        let data: HashMap<String, Vec<u8>> = conn
            .hgetall(&flow_key)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Some(self.parse_scheduled_flow(data)?))
    }

    async fn log_timer(
        &self,
        flow_id: Uuid,
        step: i32,
        fire_at: chrono::DateTime<Utc>,
        timer_name: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.get_connection()?;
        let inv_key = Self::invocation_key(flow_id, step);
        let fire_at_millis = fire_at.timestamp_millis();

        // Update invocation to WAITING_FOR_TIMER status and add to sorted set
        let _: () = redis::pipe()
            .atomic()
            .hset(&inv_key, "status", "WaitingForTimer")
            .hset(&inv_key, "timer_fire_at", fire_at_millis)
            .hset(&inv_key, "timer_name", timer_name.unwrap_or(""))
            // Add to sorted set for efficient expiry queries (score = fire_at_millis)
            .zadd(
                "ergon:timers:pending",
                format!("{}:{}", flow_id, step),
                fire_at_millis,
            )
            .query(&mut *conn)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(())
    }

    async fn get_expired_timers(
        &self,
        now: chrono::DateTime<Utc>,
    ) -> Result<Vec<super::TimerInfo>> {
        let mut conn = self.get_connection()?;
        let now_millis = now.timestamp_millis();

        // Query sorted set for timers with score <= now_millis
        let expired: Vec<String> = conn
            .zrangebyscore_limit("ergon:timers:pending", 0, now_millis, 0, 100)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let mut timers = Vec::new();

        for key in expired {
            // key format: "flow_id:step"
            let parts: Vec<&str> = key.split(':').collect();
            if parts.len() != 2 {
                continue;
            }

            let flow_id = match Uuid::parse_str(parts[0]) {
                Ok(id) => id,
                Err(_) => continue,
            };

            let step: i32 = match parts[1].parse() {
                Ok(s) => s,
                Err(_) => continue,
            };

            let inv_key = Self::invocation_key(flow_id, step);

            // Get timer details from invocation hash
            let fire_at_millis: Option<i64> = conn
                .hget(&inv_key, "timer_fire_at")
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            let timer_name: Option<String> = conn
                .hget(&inv_key, "timer_name")
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            if let Some(millis) = fire_at_millis {
                if let Some(fire_at) = chrono::DateTime::from_timestamp_millis(millis) {
                    timers.push(super::TimerInfo {
                        flow_id,
                        step,
                        fire_at,
                        timer_name: timer_name.filter(|s| !s.is_empty()),
                    });
                }
            }
        }

        Ok(timers)
    }

    async fn claim_timer(&self, flow_id: Uuid, step: i32) -> Result<bool> {
        let mut conn = self.get_connection()?;
        let inv_key = Self::invocation_key(flow_id, step);

        // Lua script for atomic claim: check status and update
        let script = r#"
            local inv_key = KEYS[1]
            local timer_key = KEYS[2]
            local member = ARGV[1]

            local status = redis.call('HGET', inv_key, 'status')

            if status == 'WaitingForTimer' then
                redis.call('HSET', inv_key, 'status', 'Complete')
                redis.call('HSET', inv_key, 'return_value', ARGV[2])
                redis.call('ZREM', timer_key, member)
                return 1
            else
                return 0
            end
        "#;

        // Unit value for timer completion
        let unit_value = bincode::serde::encode_to_vec((), bincode::config::standard())
            .map_err(|_| StorageError::Serialization)?;

        let claimed: i32 = redis::Script::new(script)
            .key(&inv_key)
            .key("ergon:timers:pending")
            .arg(format!("{}:{}", flow_id, step))
            .arg(unit_value)
            .invoke(&mut *conn)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(claimed == 1)
    }
}

// Helper methods for parsing
impl RedisExecutionLog {
    /// Helper function to get a UTF-8 string from binary data
    fn get_string(data: &HashMap<String, Vec<u8>>, key: &str) -> Result<String> {
        let bytes = data.get(key).ok_or(StorageError::Serialization)?;
        String::from_utf8(bytes.clone()).map_err(|_| StorageError::Serialization)
    }

    /// Helper function to get binary data
    fn get_bytes(data: &HashMap<String, Vec<u8>>, key: &str) -> Result<Vec<u8>> {
        data.get(key).cloned().ok_or(StorageError::Serialization)
    }

    fn parse_invocation(&self, data: HashMap<String, Vec<u8>>) -> Result<Invocation> {
        let id = Uuid::parse_str(&Self::get_string(&data, "id")?)
            .map_err(|_| StorageError::Serialization)?;

        let step: i32 = Self::get_string(&data, "step")?
            .parse()
            .map_err(|_| StorageError::Serialization)?;

        let timestamp_secs: i64 = Self::get_string(&data, "timestamp")?
            .parse()
            .map_err(|_| StorageError::Serialization)?;

        let timestamp = chrono::DateTime::from_timestamp(timestamp_secs, 0)
            .ok_or(StorageError::Serialization)?;

        let class_name = Self::get_string(&data, "class_name")?;
        let method_name = Self::get_string(&data, "method_name")?;

        let status_str = Self::get_string(&data, "status")?;
        let status = match status_str.as_str() {
            "Pending" => InvocationStatus::Pending,
            "WaitingForSignal" => InvocationStatus::WaitingForSignal,
            "Complete" => InvocationStatus::Complete,
            _ => return Err(StorageError::Serialization),
        };

        let attempts: i32 = Self::get_string(&data, "attempts")?
            .parse()
            .map_err(|_| StorageError::Serialization)?;

        // Parameters are stored as binary
        let parameters = Self::get_bytes(&data, "parameters")?;

        let params_hash: u64 = Self::get_string(&data, "params_hash")?
            .parse()
            .map_err(|_| StorageError::Serialization)?;

        // Return value is optional and binary
        let return_value = data.get("return_value").cloned();

        let delay_ms: i64 = Self::get_string(&data, "delay_ms")?
            .parse()
            .map_err(|_| StorageError::Serialization)?;

        let delay_ms = if delay_ms > 0 { Some(delay_ms) } else { None };

        // Parse retry_policy from JSON if present
        let retry_policy = Self::get_string(&data, "retry_policy")
            .ok()
            .filter(|s| !s.is_empty())
            .and_then(|json| serde_json::from_str(&json).ok());

        // Parse is_retryable (empty string = None, "0" = false, "1" = true)
        let is_retryable = Self::get_string(&data, "is_retryable")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s == "1");

        Ok(Invocation::new(
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
        ))
    }

    fn parse_scheduled_flow(&self, data: HashMap<String, Vec<u8>>) -> Result<super::ScheduledFlow> {
        let task_id = Uuid::parse_str(&Self::get_string(&data, "task_id")?)
            .map_err(|_| StorageError::Serialization)?;

        let flow_id = Uuid::parse_str(&Self::get_string(&data, "flow_id")?)
            .map_err(|_| StorageError::Serialization)?;

        let flow_type = Self::get_string(&data, "flow_type")?;

        let status_str = Self::get_string(&data, "status")?;
        let status = match status_str.as_str() {
            "Pending" => super::TaskStatus::Pending,
            "Running" => super::TaskStatus::Running,
            "Complete" => super::TaskStatus::Complete,
            "Failed" => super::TaskStatus::Failed,
            _ => return Err(StorageError::Serialization),
        };

        let created_at_secs: i64 = Self::get_string(&data, "created_at")?
            .parse()
            .map_err(|_| StorageError::Serialization)?;

        let created_at = chrono::DateTime::from_timestamp(created_at_secs, 0)
            .ok_or(StorageError::Serialization)?;

        let updated_at_secs: i64 = Self::get_string(&data, "updated_at")?
            .parse()
            .map_err(|_| StorageError::Serialization)?;

        let updated_at = chrono::DateTime::from_timestamp(updated_at_secs, 0)
            .ok_or(StorageError::Serialization)?;

        let locked_by = data
            .get("locked_by")
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok());

        // flow_data is stored as binary
        let flow_data = Self::get_bytes(&data, "flow_data")?;

        // Parse retry fields (optional, default to 0/None if not present)
        let retry_count: u32 = data
            .get("retry_count")
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let error_message = data
            .get("error_message")
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok());

        let scheduled_for = data
            .get("scheduled_for")
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0));

        Ok(super::ScheduledFlow {
            task_id,
            flow_id,
            flow_type,
            status,
            created_at,
            updated_at,
            locked_by,
            flow_data,
            retry_count,
            error_message,
            scheduled_for,
        })
    }
}
