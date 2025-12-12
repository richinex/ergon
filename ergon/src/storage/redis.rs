//! Redis Streams-based execution log for distributed queue operations.
//!
//! This module provides a Redis backend for distributed execution with true
//! multi-machine support. Unlike SQLite which requires shared filesystem access,
//! Redis enables workers to run on completely separate machines.
//!
//! # Data Structures
//!
//! ## Queue (Streams-based)
//! - `ergon:flows` (STREAM): Main flow queue with consumer groups
//!   - Consumer group: `workers`
//!   - Each entry contains: task_id, flow_id, flow_type, flow_data, created_at, etc.
//!   - Uses Pending Entries List (PEL) for tracking and recovery
//! - `ergon:queue:delayed` (ZSET): Delayed tasks (score = scheduled_timestamp)
//!
//! ## Flow Metadata (Hash-based, unchanged)
//! - `ergon:flow:{task_id}` (HASH): Flow metadata (status, timestamps, etc.)
//! - `ergon:invocations:{flow_id}` (LIST): Invocation history per flow
//! - `ergon:inv:{flow_id}:{step}` (HASH): Invocation data including timer info
//! - `ergon:signal:{flow_id}:{step}` (STRING): Signal parameters (with TTL)
//! - `ergon:timers:pending` (ZSET): Pending timers (score = fire_at timestamp)
//!
//! # Key Features
//!
//! - **Redis Streams**: Atomic XADD for enqueue, XREADGROUP for dequeue
//! - **Consumer Groups**: Built-in distributed processing with exactly-once delivery
//! - **Automatic Recovery**: XAUTOCLAIM for stale message recovery (no Lua scripts)
//! - **Pending Entries List**: Tracks in-flight messages for fault tolerance
//! - **Async connection pool**: Uses deadpool-redis for true async operations
//! - **TTL cleanup**: Automatic expiration of completed flows
//!
//! # Automatic Maintenance
//!
//! When using the `Worker` type, Redis maintenance is **automatically handled**:
//! - **Every 1 second**: `move_ready_delayed_tasks()` checks for ready delayed tasks
//! - **Every 60 seconds**: `recover_stale_locks()` uses XAUTOCLAIM for stale messages
//!
//! No manual setup required! The Worker integrates these calls internally.
//!
//! # Performance Characteristics
//!
//! - Enqueue: O(1) with XADD (atomic write of all fields)
//! - Dequeue: O(1) with XREADGROUP (atomic read + claim)
//! - Recovery: O(N) with XAUTOCLAIM (N = pending messages)
//! - Network overhead: ~0.1-0.5ms per operation (local network)

use super::{error::Result, error::StorageError, params::InvocationStartParams, ExecutionLog};
use crate::core::{hash_params, Invocation, InvocationStatus};
use async_trait::async_trait;
use chrono::Utc;
use deadpool_redis::{Config, Pool, Runtime};
use redis::{
    streams::{StreamId, StreamReadOptions, StreamReadReply},
    AsyncCommands,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Default TTL for completed flows (7 days in seconds)
const DEFAULT_COMPLETED_TTL: i64 = 7 * 24 * 3600;

/// Default TTL for signal parameters (30 days in seconds)
const DEFAULT_SIGNAL_TTL: i64 = 30 * 24 * 3600;

/// Default stale message timeout for XAUTOCLAIM (60 seconds in milliseconds)
const DEFAULT_STALE_MESSAGE_TIMEOUT_MS: i64 = 60 * 1000;

/// Stream key for flow queue
const STREAM_KEY: &str = "ergon:flows";

/// Consumer group name for worker pool
const CONSUMER_GROUP: &str = "workers";

/// Max stream length (approximate trimming for performance)
const MAX_STREAM_LEN: isize = 100_000;

/// Redis execution log using async connection pooling with Redis Streams.
///
/// This implementation uses deadpool-redis for async operations and
/// Redis Streams for distributed queue with consumer groups.
pub struct RedisExecutionLog {
    pool: Pool,
    completed_ttl: i64,
    signal_ttl: i64,
    stale_message_timeout_ms: i64,
    /// Optional notify handle for waking workers when work becomes available
    work_notify: Option<Arc<Notify>>,
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

        let log = Self {
            pool,
            completed_ttl: DEFAULT_COMPLETED_TTL,
            signal_ttl: DEFAULT_SIGNAL_TTL,
            stale_message_timeout_ms: DEFAULT_STALE_MESSAGE_TIMEOUT_MS,
            work_notify: Some(Arc::new(Notify::new())),
        };

        // Ensure consumer group exists
        log.ensure_consumer_group().await?;

        Ok(log)
    }

    /// Creates a connection pool with full configuration including stale message timeout.
    /// Useful for testing with shorter timeouts.
    pub async fn with_full_config(
        redis_url: &str,
        completed_ttl_secs: i64,
        signal_ttl_secs: i64,
        stale_message_timeout_ms: i64,
    ) -> Result<Self> {
        let cfg = Config::from_url(redis_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let log = Self {
            pool,
            completed_ttl: completed_ttl_secs,
            signal_ttl: signal_ttl_secs,
            stale_message_timeout_ms,
            work_notify: Some(Arc::new(Notify::new())),
        };

        // Ensure consumer group exists
        log.ensure_consumer_group().await?;

        Ok(log)
    }

    /// Gets an async connection from the pool.
    async fn get_connection(&self) -> Result<deadpool_redis::Connection> {
        self.pool
            .get()
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))
    }

    /// Ensures the consumer group exists, creating it if necessary.
    async fn ensure_consumer_group(&self) -> Result<()> {
        let mut conn = self.get_connection().await?;

        // Try to create the group. If it already exists, that's fine.
        let result: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(STREAM_KEY)
            .arg(CONSUMER_GROUP)
            .arg("0") // Start from beginning
            .arg("MKSTREAM") // Create stream if it doesn't exist
            .query_async(&mut *conn)
            .await;

        match result {
            Ok(()) => {
                debug!(
                    "Created consumer group '{}' for stream '{}'",
                    CONSUMER_GROUP, STREAM_KEY
                );
                Ok(())
            }
            Err(e) if e.to_string().contains("BUSYGROUP") => {
                // Group already exists, that's fine
                debug!("Consumer group '{}' already exists", CONSUMER_GROUP);
                Ok(())
            }
            Err(e) => Err(StorageError::Connection(format!(
                "Failed to create consumer group: {}",
                e
            ))),
        }
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

    /// Helper to re-enqueue a task to the stream from its flow metadata.
    async fn reenqueue_to_stream(
        &self,
        conn: &mut deadpool_redis::Connection,
        task_id: Uuid,
    ) -> Result<()> {
        use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

        let flow_key = Self::flow_key(task_id);

        // Get flow data from hash
        let data: HashMap<String, Vec<u8>> = conn
            .hgetall(&flow_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let get_string = |key: &str| -> Result<String> {
            data.get(key)
                .and_then(|v| String::from_utf8(v.clone()).ok())
                .ok_or_else(|| StorageError::Connection(format!("Missing {} in flow data", key)))
        };

        let flow_id = get_string("flow_id")?;
        let flow_type = get_string("flow_type")?;
        let flow_data = data
            .get("flow_data")
            .ok_or_else(|| StorageError::Connection("Missing flow_data".to_string()))?;
        let flow_data_b64 = BASE64.encode(flow_data);
        let created_at =
            get_string("created_at").unwrap_or_else(|_| Utc::now().timestamp().to_string());
        let retry_count = get_string("retry_count").unwrap_or_else(|_| "0".to_string());

        let mut fields: Vec<(&str, String)> = vec![
            ("task_id", task_id.to_string()),
            ("flow_id", flow_id),
            ("flow_type", flow_type),
            ("flow_data", flow_data_b64),
            ("created_at", created_at),
            ("retry_count", retry_count),
        ];

        // Add optional fields
        if let Ok(parent_id) = get_string("parent_flow_id") {
            if !parent_id.is_empty() {
                fields.push(("parent_flow_id", parent_id));
            }
        }
        if let Ok(signal_token) = get_string("signal_token") {
            if !signal_token.is_empty() {
                fields.push(("signal_token", signal_token));
            }
        }

        // Add to stream
        let _entry_id: String = redis::cmd("XADD")
            .arg(STREAM_KEY)
            .arg("MAXLEN")
            .arg("~")
            .arg(MAX_STREAM_LEN)
            .arg("*")
            .arg(&fields)
            .query_async(&mut **conn)
            .await
            .map_err(|e| {
                StorageError::Connection(format!("XADD failed during re-enqueue: {}", e))
            })?;

        Ok(())
    }

    /// Helper to parse a stream entry into a ScheduledFlow.
    fn parse_stream_entry(
        &self,
        entry: &StreamId,
        worker_id: &str,
    ) -> Result<super::ScheduledFlow> {
        use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

        // Helper to extract string from redis::Value
        let get_string = |v: &redis::Value| -> Option<String> {
            match v {
                redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
                redis::Value::SimpleString(s) => Some(s.clone()),
                redis::Value::Okay => Some("OK".to_string()),
                redis::Value::Int(i) => Some(i.to_string()),
                _ => None,
            }
        };

        let map = &entry.map;

        // Parse required fields
        let task_id = map
            .get("task_id")
            .and_then(get_string)
            .and_then(|s| Uuid::parse_str(&s).ok())
            .ok_or_else(|| {
                StorageError::Connection("Missing or invalid task_id in stream entry".to_string())
            })?;

        let flow_id = map
            .get("flow_id")
            .and_then(get_string)
            .and_then(|s| Uuid::parse_str(&s).ok())
            .ok_or_else(|| {
                StorageError::Connection("Missing or invalid flow_id in stream entry".to_string())
            })?;

        let flow_type = map.get("flow_type").and_then(get_string).ok_or_else(|| {
            StorageError::Connection("Missing flow_type in stream entry".to_string())
        })?;

        let flow_data_b64 = map.get("flow_data").and_then(get_string).ok_or_else(|| {
            StorageError::Connection("Missing flow_data in stream entry".to_string())
        })?;

        let flow_data = BASE64
            .decode(&flow_data_b64)
            .map_err(|e| StorageError::Connection(format!("Failed to decode flow_data: {}", e)))?;

        let created_at_ts: i64 = map
            .get("created_at")
            .and_then(get_string)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let created_at =
            chrono::DateTime::from_timestamp(created_at_ts, 0).unwrap_or_else(Utc::now);

        let retry_count: u32 = map
            .get("retry_count")
            .and_then(get_string)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Parse optional fields
        let parent_flow_id: Option<Uuid> = map
            .get("parent_flow_id")
            .and_then(get_string)
            .filter(|s| !s.is_empty())
            .and_then(|s| Uuid::parse_str(&s).ok());

        let signal_token: Option<String> = map
            .get("signal_token")
            .and_then(get_string)
            .filter(|s| !s.is_empty());

        Ok(super::ScheduledFlow {
            task_id,
            flow_id,
            flow_type,
            flow_data,
            status: super::TaskStatus::Running,
            locked_by: Some(worker_id.to_string()),
            created_at,
            updated_at: Utc::now(),
            retry_count,
            error_message: None,
            scheduled_for: None,
            parent_flow_id,
            signal_token,
        })
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

        // Scan for all flow metadata keys (not invocations!)
        // This is the equivalent of querying flow_queue in SQLite
        let mut cursor = 0u64;
        let mut flow_keys = Vec::new();

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .cursor_arg(cursor)
                .arg("MATCH")
                .arg("ergon:flow:*") // ← Changed from ergon:invocations:*
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            flow_keys.extend(keys);
            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        // For each flow metadata, check if incomplete
        let mut incomplete_flows = Vec::new();
        for flow_key in flow_keys {
            // Get the flow status from metadata
            let status: Option<String> = conn
                .hget(&flow_key, "status")
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            // Skip completed and failed flows
            if let Some(status_str) = status {
                if status_str == "COMPLETE" || status_str == "FAILED" {
                    continue;
                }

                // Get flow_id from metadata
                let flow_id_str: String = conn
                    .hget(&flow_key, "flow_id")
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                let flow_id = Uuid::parse_str(&flow_id_str)
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                // Try to get actual invocation if it exists
                let inv_list_key = format!("ergon:invocations:{}", flow_id);
                let inv_keys: Vec<String> = conn
                    .lrange(&inv_list_key, 0, 0)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                if let Some(first_key) = inv_keys.first() {
                    // Flow has started - use actual invocation data
                    let data: HashMap<String, Vec<u8>> = conn
                        .hgetall(first_key)
                        .await
                        .map_err(|e| StorageError::Connection(e.to_string()))?;

                    let invocation = self.parse_invocation(data)?;
                    incomplete_flows.push(invocation);
                } else {
                    // Flow scheduled but not yet executed - build invocation from flow metadata
                    // This is equivalent to SQLite's LEFT JOIN with COALESCE for missing execution_log entries
                    let flow_type: String = conn
                        .hget(&flow_key, "flow_type")
                        .await
                        .map_err(|e| StorageError::Connection(e.to_string()))?;

                    let created_at: i64 = conn
                        .hget(&flow_key, "created_at")
                        .await
                        .map_err(|e| StorageError::Connection(e.to_string()))?;

                    // Build invocation data from flow metadata (equivalent to SQLite's COALESCE)
                    let mut invocation_data = HashMap::new();
                    invocation_data.insert("id".to_string(), flow_id.to_string().into_bytes());
                    invocation_data.insert("step".to_string(), "0".to_string().into_bytes());
                    invocation_data
                        .insert("timestamp".to_string(), created_at.to_string().into_bytes());
                    invocation_data.insert("class_name".to_string(), flow_type.into_bytes());
                    invocation_data
                        .insert("method_name".to_string(), "flow".to_string().into_bytes());
                    invocation_data.insert("status".to_string(), status_str.into_bytes());
                    invocation_data.insert("attempts".to_string(), "0".to_string().into_bytes());
                    invocation_data.insert("parameters".to_string(), Vec::new());
                    invocation_data.insert("params_hash".to_string(), "0".to_string().into_bytes());
                    invocation_data.insert("delay_ms".to_string(), "0".to_string().into_bytes());
                    invocation_data.insert("retry_policy".to_string(), "".to_string().into_bytes());
                    invocation_data
                        .insert("is_retryable".to_string(), "true".to_string().into_bytes());

                    let invocation = self.parse_invocation(invocation_data)?;
                    incomplete_flows.push(invocation);
                }
            }
        }

        Ok(incomplete_flows)
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

        // Check if flow should be delayed
        if let Some(scheduled_for) = flow.scheduled_for {
            let scheduled_ts = scheduled_for.timestamp_millis();
            let flow_key = Self::flow_key(task_id);

            // Store flow metadata and add to delayed queue (unchanged for delayed flows)
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

            if let Some(parent_id) = flow.parent_flow_id {
                pipe.hset(&flow_key, "parent_flow_id", parent_id.to_string());
            }
            if let Some(ref signal_token) = flow.signal_token {
                pipe.hset(&flow_key, "signal_token", signal_token);
            }

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
            // Immediate execution - use Redis Streams for atomic enqueue
            // XADD writes all fields atomically in a single operation
            use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
            let flow_data_b64 = BASE64.encode(&flow.flow_data);
            let flow_key = Self::flow_key(task_id);

            // First, create the flow hash with all metadata (needed for resume_flow)
            let mut pipe = redis::pipe();
            pipe.atomic()
                .hset(&flow_key, "task_id", task_id.to_string())
                .hset(&flow_key, "flow_id", flow.flow_id.to_string())
                .hset(&flow_key, "flow_type", &flow.flow_type)
                .hset(&flow_key, "flow_data", flow.flow_data.as_slice())
                .hset(&flow_key, "status", "PENDING")
                .hset(&flow_key, "created_at", flow.created_at.timestamp())
                .hset(&flow_key, "updated_at", flow.updated_at.timestamp())
                .hset(&flow_key, "retry_count", 0);

            if let Some(parent_id) = flow.parent_flow_id {
                pipe.hset(&flow_key, "parent_flow_id", parent_id.to_string());
            }
            if let Some(ref signal_token) = flow.signal_token {
                pipe.hset(&flow_key, "signal_token", signal_token);
            }

            // Create flow_id -> task_id index for resume_flow
            pipe.set(
                format!("ergon:flow_task_map:{}", flow.flow_id),
                task_id.to_string(),
            );

            let _: () = pipe
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            // Now write to stream with MAXLEN trimming
            let mut fields: Vec<(&str, String)> = vec![
                ("task_id", task_id.to_string()),
                ("flow_id", flow.flow_id.to_string()),
                ("flow_type", flow.flow_type.clone()),
                ("flow_data", flow_data_b64),
                ("created_at", flow.created_at.timestamp().to_string()),
                ("retry_count", "0".to_string()),
            ];

            // Add optional fields to stream
            if let Some(parent_id) = flow.parent_flow_id {
                fields.push(("parent_flow_id", parent_id.to_string()));
            }
            if let Some(ref signal_token) = flow.signal_token {
                fields.push(("signal_token", signal_token.clone()));
            }

            let entry_id: String = redis::cmd("XADD")
                .arg(STREAM_KEY)
                .arg("MAXLEN")
                .arg("~")
                .arg(MAX_STREAM_LEN)
                .arg("*")
                .arg(&fields)
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(format!("XADD failed: {}", e)))?;

            debug!(
                "Enqueued flow {} to stream with entry ID {}",
                task_id, entry_id
            );

            // Wake up one waiting worker (if any)
            if let Some(ref notify) = self.work_notify {
                notify.notify_one();
            }
        }

        Ok(task_id)
    }

    async fn dequeue_flow(&self, worker_id: &str) -> Result<Option<super::ScheduledFlow>> {
        let mut conn = self.get_connection().await?;

        // Use XREADGROUP to atomically read and claim a message from the stream
        // This automatically adds the message to the PEL (Pending Entries List)
        let opts = StreamReadOptions::default()
            .group(CONSUMER_GROUP, worker_id)
            .count(1)
            .block(1000); // Block for 1 second if no messages

        let reply: StreamReadReply = match conn.xread_options(&[STREAM_KEY], &[">"], &opts).await {
            Ok(r) => r,
            Err(e) => {
                // If error is about missing group, ensure it exists and retry
                if e.to_string().contains("NOGROUP") {
                    warn!("Consumer group missing, recreating...");
                    self.ensure_consumer_group().await?;
                    // Retry once
                    conn.xread_options(&[STREAM_KEY], &[">"], &opts)
                        .await
                        .map_err(|e| {
                            StorageError::Connection(format!("XREADGROUP failed: {}", e))
                        })?
                } else {
                    return Err(StorageError::Connection(format!(
                        "XREADGROUP failed: {}",
                        e
                    )));
                }
            }
        };

        // Check if we got any messages
        if reply.keys.is_empty() || reply.keys[0].ids.is_empty() {
            return Ok(None); // No messages available
        }

        // Parse the first (and only) entry
        let entry = &reply.keys[0].ids[0];
        let entry_id = entry.id.clone();

        match self.parse_stream_entry(entry, worker_id) {
            Ok(flow) => {
                // Store entry_id in flow metadata for XACK on completion
                let flow_key = Self::flow_key(flow.task_id);
                let _: () = conn
                    .hset(&flow_key, "stream_entry_id", &entry_id)
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                debug!(
                    "Worker {} dequeued flow from stream: task_id={}, entry_id={}",
                    worker_id,
                    &flow.task_id.to_string()[..8],
                    entry_id
                );
                Ok(Some(flow))
            }
            Err(e) => {
                // Failed to parse - acknowledge to remove from PEL
                warn!(
                    "Failed to parse stream entry {}: {}, acknowledging to skip",
                    entry_id, e
                );
                let _: u64 = conn
                    .xack(STREAM_KEY, CONSUMER_GROUP, &[&entry_id])
                    .await
                    .map_err(|e| StorageError::Connection(e.to_string()))?;
                Ok(None)
            }
        }
    }
    async fn complete_flow(&self, task_id: Uuid, status: super::TaskStatus) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let flow_key = Self::flow_key(task_id);

        // Get the stream entry_id for XACK
        let entry_id: Option<String> = conn
            .hget(&flow_key, "stream_entry_id")
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // Acknowledge the stream message to remove from PEL
        if let Some(entry_id) = entry_id {
            let acked: u64 = conn
                .xack(STREAM_KEY, CONSUMER_GROUP, &[&entry_id])
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            if acked > 0 {
                debug!(
                    "Acknowledged stream entry {} for task {}",
                    entry_id,
                    &task_id.to_string()[..8]
                );
            } else {
                warn!(
                    "Failed to acknowledge stream entry {} (already acked?)",
                    entry_id
                );
            }
        } else {
            // No entry_id means this was a delayed task that hasn't been dequeued from stream yet
            debug!(
                "No stream_entry_id for task {}, likely a delayed/retried flow",
                task_id
            );
        }

        // Update flow status and set TTL
        let now = Utc::now().timestamp();
        let _: () = redis::pipe()
            .atomic()
            .hset(&flow_key, "status", status.as_str())
            .hset(&flow_key, "updated_at", now)
            .hdel(&flow_key, "stream_entry_id") // Clean up
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
        let scheduled_for = Utc::now()
            + chrono::Duration::from_std(delay)
                .map_err(|e| StorageError::Connection(format!("Invalid delay duration: {}", e)))?;
        let scheduled_ts = scheduled_for.timestamp_millis();

        // Acknowledge the failed stream attempt (if it exists)
        let entry_id: Option<String> = conn
            .hget(&flow_key, "stream_entry_id")
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if let Some(entry_id) = entry_id {
            // XACK to remove from PEL - we're handling the retry manually via delayed queue
            let _: u64 = conn
                .xack(STREAM_KEY, CONSUMER_GROUP, &[&entry_id])
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;
            debug!("Acknowledged failed stream entry {} for retry", entry_id);
        }

        // Update flow and add to delayed queue for retry
        let _: () = redis::pipe()
            .atomic()
            .hincr(&flow_key, "retry_count", 1)
            .hset(&flow_key, "error_message", error_message)
            .hset(&flow_key, "status", "Pending")
            .hset(&flow_key, "scheduled_for", scheduled_ts)
            .hdel(&flow_key, "locked_by")
            .hdel(&flow_key, "stream_entry_id") // Clean up
            .hset(&flow_key, "updated_at", Utc::now().timestamp())
            // Add to delayed queue for retry after delay
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

    async fn resume_flow(&self, flow_id: Uuid) -> Result<bool> {
        let mut conn = self.get_connection().await?;

        // O(1) lookup using flow_id -> task_id index (no more slow SCAN!)
        let task_id_str: Option<String> = conn
            .get(format!("ergon:flow_task_map:{}", flow_id))
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let Some(task_id) = task_id_str.and_then(|s| Uuid::parse_str(&s).ok()) else {
            debug!("Task not found for flow_id: {}", flow_id);
            return Ok(false);
        };

        let flow_key = Self::flow_key(task_id);
        let now = Utc::now().timestamp();

        // Atomic check-and-update using Lua script to prevent race conditions
        // Multiple workers may call resume_flow() simultaneously when a signal arrives
        // This ensures only ONE worker actually resumes and re-enqueues the flow
        let script = redis::Script::new(
            r#"
            local status = redis.call('HGET', KEYS[1], 'status')
            if status == 'SUSPENDED' then
                redis.call('HSET', KEYS[1], 'status', 'PENDING')
                redis.call('HDEL', KEYS[1], 'locked_by')
                redis.call('HDEL', KEYS[1], 'stream_entry_id')
                redis.call('HSET', KEYS[1], 'updated_at', ARGV[1])
                return 1
            else
                return 0
            end
            "#,
        );

        let resumed: i32 = script
            .key(&flow_key)
            .arg(now)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        if resumed == 1 {
            // We won the race - re-enqueue to stream
            self.reenqueue_to_stream(&mut conn, task_id).await?;

            info!(
                "Resumed flow: flow_id={}, task_id={}",
                &flow_id.to_string()[..8],
                &task_id.to_string()[..8]
            );

            // Wake up one waiting worker (if any)
            if let Some(ref notify) = self.work_notify {
                notify.notify_one();
            }

            Ok(true)
        } else {
            // Another worker already resumed, or flow not in SUSPENDED state
            debug!(
                "Flow not resumed (already resumed or not suspended): flow_id={}, task_id={}",
                flow_id, task_id
            );
            Ok(false)
        }
    }

    // ===== Signal Operations =====

    async fn store_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
        params: &[u8],
    ) -> Result<()> {
        let mut conn = self.get_connection().await?;
        // Include signal_name in key to support multiple signals at same step
        let signal_key = format!("{}:{}", Self::signal_key(flow_id, step), signal_name);

        // Store signal params with TTL
        let _: () = redis::pipe()
            .atomic()
            .set(&signal_key, params)
            .expire(&signal_key, self.signal_ttl)
            .query_async(&mut *conn)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

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
        let mut conn = self.get_connection().await?;
        let signal_key = format!("{}:{}", Self::signal_key(flow_id, step), signal_name);

        let params: Option<Vec<u8>> = conn
            .get(&signal_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(params)
    }

    async fn remove_signal_params(
        &self,
        flow_id: Uuid,
        step: i32,
        signal_name: &str,
    ) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let signal_key = format!("{}:{}", Self::signal_key(flow_id, step), signal_name);

        let _: () = conn
            .del(&signal_key)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        debug!(
            "Removed signal params: flow_id={}, step={}, signal_name={}",
            flow_id, step, signal_name
        );
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
        let chrono_duration = chrono::Duration::from_std(older_than)
            .map_err(|e| StorageError::InvalidParameter(format!("Invalid duration: {}", e)))?;
        let cutoff = Utc::now() - chrono_duration;
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

        // Get ready tasks from delayed queue
        let ready_tasks: Vec<String> = conn
            .zrangebyscore_limit("ergon:queue:delayed", 0, now, 0, 100)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let mut moved_count = 0u64;

        for task_id_str in ready_tasks {
            let task_id = match Uuid::parse_str(&task_id_str) {
                Ok(id) => id,
                Err(_) => {
                    warn!("Invalid task_id in delayed queue: {}", task_id_str);
                    continue;
                }
            };

            // Remove from delayed queue first (atomic with ZREM)
            let removed: i64 = conn
                .zrem("ergon:queue:delayed", &task_id_str)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

            if removed > 0 {
                // Re-enqueue to stream
                match self.reenqueue_to_stream(&mut conn, task_id).await {
                    Ok(()) => {
                        moved_count += 1;
                        debug!("Moved delayed task {} to stream", &task_id.to_string()[..8]);
                    }
                    Err(e) => {
                        warn!("Failed to re-enqueue task {}: {}", task_id, e);
                        // Put it back in delayed queue to retry later
                        let _: () = conn
                            .zadd("ergon:queue:delayed", &task_id_str, now + 1000)
                            .await
                            .map_err(|e| StorageError::Connection(e.to_string()))?;
                    }
                }
            }
        }

        if moved_count > 0 {
            debug!("Moved {} delayed tasks to stream", moved_count);

            // Wake up workers for the moved tasks
            if let Some(ref notify) = self.work_notify {
                for _ in 0..moved_count {
                    notify.notify_one();
                }
            }
        }
        Ok(moved_count)
    }

    async fn recover_stale_locks(&self) -> Result<u64> {
        let mut conn = self.get_connection().await?;

        // Use XAUTOCLAIM to automatically claim stale messages from the PEL
        // This is built into Redis Streams and handles all the complexity
        let mut total_claimed = 0u64;
        let mut cursor = "0-0".to_string();

        // We use a dedicated "recovery" consumer name to claim messages
        // This prevents interference with actual workers
        let recovery_consumer = "recovery-agent";

        loop {
            // XAUTOCLAIM: claim messages idle for more than stale_message_timeout_ms
            let result: redis::Value = redis::cmd("XAUTOCLAIM")
                .arg(STREAM_KEY)
                .arg(CONSUMER_GROUP)
                .arg(recovery_consumer)
                .arg(self.stale_message_timeout_ms)
                .arg(&cursor)
                .arg("COUNT")
                .arg(10) // Process in batches
                .query_async(&mut *conn)
                .await
                .map_err(|e| StorageError::Connection(format!("XAUTOCLAIM failed: {}", e)))?;

            // Parse the result: [next_cursor, [claimed_messages], [deleted_ids]]
            let (next_cursor, claimed_entries) = match result {
                redis::Value::Array(arr) if arr.len() >= 2 => {
                    let next_cursor = match &arr[0] {
                        redis::Value::BulkString(bytes) => {
                            String::from_utf8(bytes.clone()).unwrap_or_else(|_| "0-0".to_string())
                        }
                        _ => "0-0".to_string(),
                    };

                    let entries = match &arr[1] {
                        redis::Value::Array(entries) => entries.clone(),
                        _ => vec![],
                    };

                    (next_cursor, entries)
                }
                _ => {
                    warn!("Unexpected XAUTOCLAIM response format");
                    break;
                }
            };

            // Process claimed messages
            for entry_value in &claimed_entries {
                if let redis::Value::Array(entry_arr) = entry_value {
                    if entry_arr.len() >= 2 {
                        // entry_arr[0] is the entry ID
                        // entry_arr[1] is the field-value pairs
                        total_claimed += 1;

                        // Immediately acknowledge these - the recovery agent doesn't process,
                        // it just re-enqueues back to the stream for a real worker to pick up
                        if let redis::Value::BulkString(entry_id_bytes) = &entry_arr[0] {
                            if let Ok(entry_id) = String::from_utf8(entry_id_bytes.clone()) {
                                // Acknowledge to remove from PEL
                                let _: u64 = conn
                                    .xack(STREAM_KEY, CONSUMER_GROUP, &[&entry_id])
                                    .await
                                    .map_err(|e| StorageError::Connection(e.to_string()))?;

                                debug!(
                                    "Auto-claimed and re-acknowledged stale message {}",
                                    entry_id
                                );

                                // The message is still in the stream, so it will be picked up
                                // by XREADGROUP with ID "0" (pending messages)
                            }
                        }
                    }
                }
            }

            cursor = next_cursor;

            // If cursor is back to "0-0", we've scanned everything
            if cursor == "0-0" {
                break;
            }
        }

        if total_claimed > 0 {
            info!("Recovered {} stale messages via XAUTOCLAIM", total_claimed);
        }

        Ok(total_claimed)
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

    fn work_notify(&self) -> Option<&Arc<Notify>> {
        self.work_notify.as_ref()
    }
}
