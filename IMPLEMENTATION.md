# Durable Timers Implementation Plan

**Status:** Ready for Implementation
**Date:** 2025-12-02
**Branch:** `durable-timers`
**Target Version:** 0.2.0

---

## Executive Summary

This document provides a detailed implementation plan for adding durable timers to Ergon. After thorough analysis of the codebase and research into distributed timer patterns (Temporal, Apalis, go-workflows), we've determined that **we already have the core solution**: the execution log + optimistic concurrency pattern.

### Key Insight

> The execution log already solves distribution. We don't need complex coordination protocols—the execution log IS the coordination layer.

This implementation reuses the proven signal pattern (`WAITING_FOR_SIGNAL`) and the optimistic concurrency approach from `dequeue_flow`. The schema already has timer support (`timer_fire_at` column exists!), making this a straightforward addition.

---

## Design Principles

Following Dave Cheney's "Practical Go" principles (adapted for Rust):

1. **Simplicity is Prerequisite for Reliability** - Single-purpose API, reuses existing patterns
2. **APIs Should Be Easy to Use and Hard to Misuse** - Type-safe `Duration`, no ambiguous parameters
3. **Eliminate Errors by Design** - Timers can't fail; late firing is acceptable
4. **Clear Lifecycle Management** - Explicit start/stop for timer processor
5. **Caller Controls Concurrency** - No hidden task spawning

---

## Architecture Overview

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Flow Execution                           │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  schedule_timer(duration)                                   │ │
│  │      ↓                                                      │ │
│  │  Check if resuming (WAITING_FOR_TIMER in execution_log)   │ │
│  │      ↓                                                      │ │
│  │  If not resuming: Log timer with fire_time = now + dur    │ │
│  │      ↓                                                      │ │
│  │  await_timer() - blocks execution (like signal pattern)   │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    Persisted to execution_log
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              Timer Processor (Background Task)                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Loop every poll_interval (default: 1 second):            │ │
│  │    1. Query: SELECT WHERE status='WAITING_FOR_TIMER'      │ │
│  │              AND timer_fire_at <= now()                    │ │
│  │    2. For each expired timer:                             │ │
│  │       - Try to claim (optimistic concurrency!)            │ │
│  │       - UPDATE status='COMPLETE' WHERE status='WAITING'   │ │
│  │       - Only ONE worker succeeds (like dequeue_flow)      │ │
│  │       - Enqueue flow for resumption                       │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    Flow Resumes Automatically
```

### Module Structure

```
ergon/src/
├── executor/
│   ├── signal.rs           (existing - reference pattern)
│   ├── timer.rs            (new - timer API & implementation)
│   ├── timer_processor.rs  (new - background timer firing)
│   └── context.rs          (modify - add timer awaiting)
├── core/
│   └── invocation.rs       (modify - add WaitingForTimer status)
└── storage/
    ├── mod.rs              (modify - add timer query methods)
    ├── sqlite.rs           (modify - timer queries & indexes)
    ├── redis.rs            (modify - Redis ZSET for timers)
    └── memory.rs           (modify - in-memory timer tracking)
```

---

## Implementation Phases

### Phase 1: Core Timer Infrastructure (Week 1)

**Goal:** Basic timer functionality with SQLite, single worker

#### 1.1 Extend `InvocationStatus` Enum

**File:** `ergon/src/core/invocation.rs:10-15`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvocationStatus {
    Pending,
    WaitingForSignal,
    WaitingForTimer,  // NEW: Waiting for timer to fire
    Complete,
}
```

Update `as_str()` and `from_str()` methods:

```rust
impl InvocationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            InvocationStatus::Pending => "PENDING",
            InvocationStatus::WaitingForSignal => "WAITING_FOR_SIGNAL",
            InvocationStatus::WaitingForTimer => "WAITING_FOR_TIMER",  // NEW
            InvocationStatus::Complete => "COMPLETE",
        }
    }
}

impl FromStr for InvocationStatus {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "PENDING" => Ok(InvocationStatus::Pending),
            "WAITING_FOR_SIGNAL" => Ok(InvocationStatus::WaitingForSignal),
            "WAITING_FOR_TIMER" => Ok(InvocationStatus::WaitingForTimer),  // NEW
            "COMPLETE" => Ok(InvocationStatus::Complete),
            _ => Err(Error::InvalidStatus(s.to_string())),
        }
    }
}
```

#### 1.2 Add Timer Fields to `Invocation`

**File:** `ergon/src/core/invocation.rs:47-66`

Add fields to the `Invocation` struct:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invocation {
    id: Uuid,
    step: i32,
    timestamp: DateTime<Utc>,
    class_name: String,
    method_name: String,
    status: InvocationStatus,
    attempts: i32,
    parameters: Vec<u8>,
    params_hash: u64,
    return_value: Option<Vec<u8>>,
    delay: Option<i64>,
    retry_policy: Option<RetryPolicy>,
    is_retryable: Option<bool>,

    // NEW: Timer fields
    #[serde(default)]
    timer_fire_at: Option<DateTime<Utc>>,

    #[serde(default)]
    timer_name: Option<String>,
}
```

Add accessor methods:

```rust
impl Invocation {
    // ... existing methods ...

    pub fn timer_fire_at(&self) -> Option<DateTime<Utc>> {
        self.timer_fire_at
    }

    pub fn timer_name(&self) -> Option<&str> {
        self.timer_name.as_deref()
    }

    pub fn is_timer_expired(&self) -> bool {
        match self.timer_fire_at {
            Some(ft) => Utc::now() >= ft,
            None => false,
        }
    }
}
```

#### 1.3 Storage Trait Extensions

**File:** `ergon/src/storage/mod.rs`

Add timer-related methods to the `ExecutionLog` trait:

```rust
#[async_trait]
pub trait ExecutionLog: Send + Sync {
    // ... existing methods ...

    /// Get all timers that should have fired by `now`.
    ///
    /// Returns timers with status=WAITING_FOR_TIMER and timer_fire_at <= now.
    /// Used by TimerProcessor to find expired timers.
    async fn get_expired_timers(&self, now: DateTime<Utc>) -> Result<Vec<TimerInfo>>;

    /// Atomically claim an expired timer using optimistic concurrency.
    ///
    /// Updates status from WAITING_FOR_TIMER to COMPLETE only if the
    /// status is still WAITING_FOR_TIMER (prevents duplicate firing).
    ///
    /// Returns true if we successfully claimed the timer, false if
    /// another worker already claimed it.
    async fn claim_timer(
        &self,
        flow_id: Uuid,
        step: i32,
    ) -> Result<bool>;

    /// Log a timer that should fire at the specified time.
    ///
    /// Called by schedule_timer() to persist the timer to storage.
    async fn log_timer(
        &self,
        flow_id: Uuid,
        step: i32,
        fire_at: DateTime<Utc>,
        timer_name: Option<&str>,
    ) -> Result<()>;
}

/// Information about a timer that needs to fire.
#[derive(Debug, Clone)]
pub struct TimerInfo {
    pub flow_id: Uuid,
    pub step: i32,
    pub fire_at: DateTime<Utc>,
    pub timer_name: Option<String>,
}
```

#### 1.4 SQLite Implementation

**File:** `ergon/src/storage/sqlite.rs`

##### Schema Migration

The schema already has `timer_fire_at` column! We just need to add the index:

```rust
impl SqliteExecutionLog {
    fn create_schema(&self) -> Result<()> {
        // ... existing schema creation ...

        // Add index for efficient timer queries
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_timer_ready
             ON execution_log(status, timer_fire_at)
             WHERE status = 'WAITING_FOR_TIMER' AND timer_fire_at IS NOT NULL",
            [],
        )?;

        Ok(())
    }
}
```

##### Timer Query Methods

```rust
impl ExecutionLog for SqliteExecutionLog {
    async fn get_expired_timers(&self, now: DateTime<Utc>) -> Result<Vec<TimerInfo>> {
        let conn = self.get_connection()?;
        let now_millis = now.timestamp_millis();

        tokio::task::spawn_blocking(move || {
            let mut stmt = conn.prepare(
                "SELECT id, step, timer_fire_at, timer_name
                 FROM execution_log
                 WHERE status = 'WAITING_FOR_TIMER'
                   AND timer_fire_at IS NOT NULL
                   AND timer_fire_at <= ?
                 ORDER BY timer_fire_at ASC
                 LIMIT 100"  // Batch size to prevent overwhelming single worker
            )?;

            let timers = stmt
                .query_map(params![now_millis], |row| {
                    let flow_id_str: String = row.get(0)?;
                    let flow_id = Uuid::parse_str(&flow_id_str)
                        .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;

                    let step: i32 = row.get(1)?;

                    let fire_at_millis: i64 = row.get(2)?;
                    let fire_at = DateTime::from_timestamp_millis(fire_at_millis)
                        .ok_or_else(|| rusqlite::Error::InvalidParameterName("Invalid timestamp".to_string()))?;

                    let timer_name: Option<String> = row.get(3)?;

                    Ok(TimerInfo {
                        flow_id,
                        step,
                        fire_at,
                        timer_name,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            Ok(timers)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn claim_timer(&self, flow_id: Uuid, step: i32) -> Result<bool> {
        let mut conn = self.get_connection()?;

        tokio::task::spawn_blocking(move || {
            // Use IMMEDIATE transaction (like dequeue_flow!)
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

            // Optimistic concurrency: UPDATE with status check
            let rows_updated = tx.execute(
                "UPDATE execution_log
                 SET status = 'COMPLETE', return_value = ?
                 WHERE id = ? AND step = ? AND status = 'WAITING_FOR_TIMER'",
                params![
                    // Return unit value () for timer completion
                    &bincode::serialize(&()).unwrap(),
                    flow_id.to_string(),
                    step,
                ],
            )?;

            tx.commit()?;

            // If rows_updated == 0, another worker already claimed this timer
            Ok(rows_updated > 0)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn log_timer(
        &self,
        flow_id: Uuid,
        step: i32,
        fire_at: DateTime<Utc>,
        timer_name: Option<&str>,
    ) -> Result<()> {
        let conn = self.get_connection()?;
        let timer_name = timer_name.map(|s| s.to_string());

        tokio::task::spawn_blocking(move || {
            conn.execute(
                "UPDATE execution_log
                 SET status = 'WAITING_FOR_TIMER',
                     timer_fire_at = ?,
                     timer_name = ?
                 WHERE id = ? AND step = ?",
                params![
                    fire_at.timestamp_millis(),
                    timer_name,
                    flow_id.to_string(),
                    step,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }
}
```

#### 1.5 Timer API Implementation

**File:** `ergon/src/executor/timer.rs` (NEW)

```rust
//! Durable timer implementation.
//!
//! This module hides the complexity of:
//! - Timer persistence and recovery
//! - Timer processor coordination
//! - Fire time calculation and storage
//!
//! Following the same pattern as signal.rs, this module provides
//! a simple API for scheduling durable timers that survive crashes.

use super::context::{ExecutionContext, CALL_TYPE, EXECUTION_CONTEXT};
use super::error::{ExecutionError, Result};
use crate::core::{CallType, InvocationStatus};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use uuid::Uuid;

lazy_static::lazy_static! {
    /// Notifiers for timers that have fired.
    /// Maps (flow_id, step) -> Notify for waking waiting flows.
    pub(super) static ref TIMER_NOTIFIERS: Mutex<HashMap<(Uuid, i32), Arc<Notify>>> =
        Mutex::new(HashMap::new());
}

/// Schedules a durable timer that pauses workflow execution.
///
/// The timer is persisted to storage and will fire even if the worker
/// crashes. When the timer fires, execution resumes from this point.
///
/// # Guarantees
/// - **Durable**: Timer survives worker crashes
/// - **Eventually Fires**: If timer missed while worker was down, fires immediately on recovery
/// - **Idempotent**: If timer fires multiple times, only first firing proceeds (step caching)
///
/// # Example
/// ```ignore
/// #[step]
/// async fn wait_five_minutes(&self) {
///     schedule_timer(Duration::from_secs(300)).await;
/// }
/// ```
pub async fn schedule_timer(duration: Duration) {
    schedule_timer_impl(duration, None).await
}

/// Schedules a named timer for debugging and observability.
///
/// Named timers appear in logs and can be queried for monitoring.
///
/// # Example
/// ```ignore
/// #[step]
/// async fn wait_for_trial_expiry(&self) {
///     schedule_timer_named(Duration::from_days(14), "trial-expiry").await;
/// }
/// ```
pub async fn schedule_timer_named(duration: Duration, name: &str) {
    schedule_timer_impl(duration, Some(name)).await
}

/// Internal implementation that handles timer scheduling.
async fn schedule_timer_impl(duration: Duration, name: Option<&str>) {
    let ctx = EXECUTION_CONTEXT
        .try_with(|c| c.clone())
        .expect("schedule_timer called outside execution context");

    // Get current step (don't increment yet - that happens in step macro)
    let current_step = ctx.step_counter.load(Ordering::SeqCst);

    // Check if we're resuming from a timer
    let existing_inv = ctx
        .storage
        .get_invocation(ctx.id, current_step)
        .await
        .ok()
        .flatten();

    if let Some(inv) = existing_inv {
        if inv.status() == InvocationStatus::Complete {
            // Timer already fired and step completed - we're resuming
            return;
        }

        if inv.status() == InvocationStatus::WaitingForTimer {
            // We're waiting for this timer - check if it's fired
            if inv.is_timer_expired() {
                // Timer fired, but we haven't processed it yet
                // This can happen during replay - just continue
                return;
            }

            // Timer not fired yet - wait for notification
            ctx.await_timer().await.unwrap();
            return;
        }
    }

    // First time - calculate fire time and log timer
    let fire_at = Utc::now() + ChronoDuration::from_std(duration).unwrap();

    ctx.storage
        .log_timer(ctx.id, current_step, fire_at, name)
        .await
        .expect("Failed to log timer");

    // Wait for timer to fire
    ctx.await_timer().await.unwrap();
}

impl<S: crate::storage::ExecutionLog> ExecutionContext<S> {
    /// Wait for a timer to fire.
    ///
    /// This method blocks until the timer processor fires the timer
    /// and notifies us via the TIMER_NOTIFIERS map.
    pub(super) async fn await_timer(&self) -> Result<()> {
        let key = (self.id, self.step_counter.load(Ordering::SeqCst));

        let notifier = {
            let mut notifiers = TIMER_NOTIFIERS
                .lock()
                .expect("TIMER_NOTIFIERS Mutex poisoned");
            notifiers
                .entry(key)
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone()
        };

        // Wait for timer to fire
        notifier.notified().await;

        // Clean up notifier
        {
            let mut notifiers = TIMER_NOTIFIERS
                .lock()
                .expect("TIMER_NOTIFIERS Mutex poisoned");
            notifiers.remove(&key);
        }

        Ok(())
    }
}
```

#### 1.6 Timer Processor Implementation

**File:** `ergon/src/executor/timer_processor.rs` (NEW)

```rust
//! Background timer processor that fires expired timers.
//!
//! This component runs in a separate task and periodically checks
//! storage for timers that should fire, then notifies waiting
//! workflows to resume.

use crate::storage::{ExecutionLog, TimerInfo};
use chrono::Utc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::timer::TIMER_NOTIFIERS;
use super::scheduler::FlowScheduler;

/// Background timer processor that fires expired timers.
///
/// # Lifecycle
/// 1. Create: `TimerProcessor::new(storage, scheduler)`
/// 2. Configure: `.with_poll_interval(duration)`
/// 3. Start: `.start()` returns handle
/// 4. Shutdown: `handle.shutdown().await`
///
/// # Example
/// ```ignore
/// let processor = TimerProcessor::new(storage, scheduler)
///     .with_poll_interval(Duration::from_secs(1))
///     .start();
///
/// // ... application runs ...
///
/// processor.shutdown().await;
/// ```
pub struct TimerProcessor<S: ExecutionLog> {
    storage: Arc<S>,
    scheduler: Arc<FlowScheduler<S>>,
    poll_interval: Duration,
    shutdown: Arc<AtomicBool>,
}

impl<S: ExecutionLog + 'static> TimerProcessor<S> {
    /// Creates a new timer processor with default configuration.
    pub fn new(storage: Arc<S>, scheduler: Arc<FlowScheduler<S>>) -> Self {
        Self {
            storage,
            scheduler,
            poll_interval: Duration::from_secs(1),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Sets the poll interval for checking expired timers.
    ///
    /// Lower intervals = better precision, higher CPU usage.
    /// Higher intervals = lower precision, reduced CPU usage.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Starts the timer processor in a background task.
    ///
    /// Returns a handle that MUST be used to stop the processor.
    pub fn start(self) -> TimerProcessorHandle {
        let shutdown = Arc::clone(&self.shutdown);

        let handle = tokio::spawn(async move {
            self.run().await;
        });

        TimerProcessorHandle { handle, shutdown }
    }

    /// Main processing loop.
    async fn run(self) {
        info!(
            "Timer processor started (poll_interval={:?})",
            self.poll_interval
        );

        while !self.shutdown.load(Ordering::SeqCst) {
            if let Err(e) = self.process_expired_timers().await {
                error!("Error processing timers: {}", e);
            }

            tokio::time::sleep(self.poll_interval).await;
        }

        info!("Timer processor stopped cleanly");
    }

    /// Process all expired timers.
    async fn process_expired_timers(&self) -> crate::core::Result<()> {
        let expired = self.storage.get_expired_timers(Utc::now()).await?;

        if !expired.is_empty() {
            debug!("Processing {} expired timers", expired.len());
        }

        for timer in expired {
            match self.fire_timer(timer).await {
                Ok(true) => {
                    // Successfully fired
                }
                Ok(false) => {
                    // Another worker fired it
                    debug!(
                        "Timer already fired by another worker: flow={} step={}",
                        timer.flow_id, timer.step
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to fire timer (will retry): flow={} step={} error={}",
                        timer.flow_id, timer.step, e
                    );
                }
            }
        }

        Ok(())
    }

    /// Fire a single timer.
    ///
    /// Returns true if we successfully fired the timer,
    /// false if another worker already fired it.
    async fn fire_timer(&self, timer: TimerInfo) -> crate::core::Result<bool> {
        // Try to claim the timer (optimistic concurrency!)
        let claimed = self
            .storage
            .claim_timer(timer.flow_id, timer.step)
            .await?;

        if !claimed {
            return Ok(false);
        }

        // We claimed it! Notify waiting flow (if it's in memory)
        let key = (timer.flow_id, timer.step);
        if let Some(notifier) = TIMER_NOTIFIERS.lock().unwrap().get(&key).cloned() {
            notifier.notify_one();
        }

        // Enqueue flow for resumption (in case it's not in memory)
        // The scheduler will resume the flow from this step
        // TODO: Add flow resumption to scheduler

        info!(
            "Timer fired: flow={} step={} name={:?} latency={:?}",
            timer.flow_id,
            timer.step,
            timer.timer_name,
            Utc::now().signed_duration_since(timer.fire_at)
        );

        Ok(true)
    }
}

/// Handle for stopping the timer processor.
pub struct TimerProcessorHandle {
    handle: JoinHandle<()>,
    shutdown: Arc<AtomicBool>,
}

impl TimerProcessorHandle {
    /// Gracefully shutdown the timer processor.
    ///
    /// Signals the processor to stop and waits for it to complete.
    pub async fn shutdown(self) {
        info!("Shutting down timer processor...");
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.handle.await;
        info!("Timer processor shutdown complete");
    }
}
```

---

### Phase 2: Multi-Backend Support (Week 2)

**Goal:** Redis and in-memory timer implementations

#### 2.1 Redis Implementation

**File:** `ergon/src/storage/redis.rs`

Redis is ideal for timers using sorted sets (ZSET):

```rust
impl ExecutionLog for RedisExecutionLog {
    async fn get_expired_timers(&self, now: DateTime<Utc>) -> Result<Vec<TimerInfo>> {
        let mut conn = self.pool.get()?;
        let now_millis = now.timestamp_millis();

        tokio::task::spawn_blocking(move || {
            // ZRANGEBYSCORE: Get timers sorted by fire_time
            let keys: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg("ergon:timers:pending")
                .arg("-inf")
                .arg(now_millis)
                .arg("LIMIT")
                .arg(0)
                .arg(100)  // Batch size
                .query(&mut *conn)?;

            let mut timers = Vec::new();
            for key in keys {
                // key format: "flow_id:step"
                let parts: Vec<&str> = key.split(':').collect();
                if parts.len() != 2 {
                    continue;
                }

                let flow_id = Uuid::parse_str(parts[0])?;
                let step: i32 = parts[1].parse()?;

                // Get timer metadata from invocation
                let inv_key = format!("ergon:inv:{}:{}", flow_id, step);
                let timer_data: HashMap<String, String> = conn.hgetall(&inv_key)?;

                if let Some(fire_at_str) = timer_data.get("timer_fire_at") {
                    let fire_at_millis: i64 = fire_at_str.parse()?;
                    let fire_at = DateTime::from_timestamp_millis(fire_at_millis)
                        .ok_or_else(|| StorageError::InvalidData("Invalid timestamp".to_string()))?;

                    let timer_name = timer_data.get("timer_name").cloned();

                    timers.push(TimerInfo {
                        flow_id,
                        step,
                        fire_at,
                        timer_name,
                    });
                }
            }

            Ok(timers)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn claim_timer(&self, flow_id: Uuid, step: i32) -> Result<bool> {
        let mut conn = self.pool.get()?;

        tokio::task::spawn_blocking(move || {
            // Lua script for atomic check-and-set
            let script = redis::Script::new(r#"
                local inv_key = KEYS[1]
                local timer_key = KEYS[2]
                local current_status = redis.call('HGET', inv_key, 'status')

                if current_status == 'WAITING_FOR_TIMER' then
                    redis.call('HSET', inv_key, 'status', 'COMPLETE')
                    redis.call('HSET', inv_key, 'return_value', ARGV[1])
                    redis.call('ZREM', timer_key, ARGV[2])
                    return 1
                else
                    return 0
                end
            "#);

            let inv_key = format!("ergon:inv:{}:{}", flow_id, step);
            let timer_key = "ergon:timers:pending";
            let timer_id = format!("{}:{}", flow_id, step);
            let unit_value = bincode::serialize(&()).unwrap();

            let claimed: i64 = script
                .key(&inv_key)
                .key(timer_key)
                .arg(&unit_value)
                .arg(&timer_id)
                .invoke(&mut *conn)?;

            Ok(claimed == 1)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }

    async fn log_timer(
        &self,
        flow_id: Uuid,
        step: i32,
        fire_at: DateTime<Utc>,
        timer_name: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.pool.get()?;
        let timer_name = timer_name.map(|s| s.to_string());

        tokio::task::spawn_blocking(move || {
            let inv_key = format!("ergon:inv:{}:{}", flow_id, step);
            let timer_id = format!("{}:{}", flow_id, step);

            redis::pipe()
                .atomic()
                // Update invocation status
                .hset(&inv_key, "status", "WAITING_FOR_TIMER")
                .hset(&inv_key, "timer_fire_at", fire_at.timestamp_millis())
                .ignore()
                // Add to timer index (ZSET with fire_time as score)
                .zadd("ergon:timers:pending", &timer_id, fire_at.timestamp_millis())
                .ignore()
                .query(&mut *conn)?;

            if let Some(name) = timer_name {
                conn.hset(&inv_key, "timer_name", name)?;
            }

            Ok(())
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?
    }
}
```

#### 2.2 In-Memory Implementation

**File:** `ergon/src/storage/memory.rs`

```rust
use std::collections::BTreeMap;

pub struct InMemoryExecutionLog {
    invocations: Arc<RwLock<HashMap<(Uuid, i32), Invocation>>>,

    // NEW: Timer index for efficient querying
    // BTreeMap allows range queries by fire_time
    timer_index: Arc<RwLock<BTreeMap<DateTime<Utc>, Vec<(Uuid, i32)>>>>,
}

impl ExecutionLog for InMemoryExecutionLog {
    async fn get_expired_timers(&self, now: DateTime<Utc>) -> Result<Vec<TimerInfo>> {
        let timer_index = self.timer_index.read().unwrap();
        let invocations = self.invocations.read().unwrap();

        let mut timers = Vec::new();

        // Get all fire times up to now
        for (fire_at, keys) in timer_index.range(..=now) {
            for (flow_id, step) in keys {
                if let Some(inv) = invocations.get(&(*flow_id, *step)) {
                    if inv.status() == InvocationStatus::WaitingForTimer {
                        timers.push(TimerInfo {
                            flow_id: *flow_id,
                            step: *step,
                            fire_at: *fire_at,
                            timer_name: inv.timer_name().map(|s| s.to_string()),
                        });
                    }
                }
            }
        }

        Ok(timers)
    }

    async fn claim_timer(&self, flow_id: Uuid, step: i32) -> Result<bool> {
        let mut invocations = self.invocations.write().unwrap();

        if let Some(inv) = invocations.get_mut(&(flow_id, step)) {
            if inv.status() == InvocationStatus::WaitingForTimer {
                // Claim it!
                inv.set_status(InvocationStatus::Complete);
                inv.set_return_value(Some(bincode::serialize(&()).unwrap()));
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn log_timer(
        &self,
        flow_id: Uuid,
        step: i32,
        fire_at: DateTime<Utc>,
        timer_name: Option<&str>,
    ) -> Result<()> {
        let mut invocations = self.invocations.write().unwrap();
        let mut timer_index = self.timer_index.write().unwrap();

        if let Some(inv) = invocations.get_mut(&(flow_id, step)) {
            inv.set_status(InvocationStatus::WaitingForTimer);
            inv.set_timer_fire_at(Some(fire_at));
            inv.set_timer_name(timer_name.map(|s| s.to_string()));
        }

        // Add to timer index
        timer_index
            .entry(fire_at)
            .or_insert_with(Vec::new)
            .push((flow_id, step));

        Ok(())
    }
}
```

---

### Phase 3: Integration & Testing (Week 3)

**Goal:** Integrate with FlowWorker, add tests and examples

#### 3.1 FlowWorker Integration

**File:** `ergon/src/executor/worker.rs`

```rust
pub struct FlowWorker<S: ExecutionLog> {
    storage: Arc<S>,
    scheduler: Arc<FlowScheduler<S>>,
    worker_id: String,
    timer_processor: Option<Arc<TimerProcessor<S>>>,  // NEW
}

impl<S: ExecutionLog + 'static> FlowWorker<S> {
    /// Enable timer processing for this worker.
    ///
    /// Call this to enable automatic timer firing. The worker will
    /// start a background task that polls for expired timers.
    pub fn with_timer_processor(mut self) -> Self {
        let processor = Arc::new(
            TimerProcessor::new(
                self.storage.clone(),
                self.scheduler.clone(),
            )
        );
        self.timer_processor = Some(processor);
        self
    }

    /// Start the worker with optional timer processor.
    pub async fn start(self: Arc<Self>) -> WorkerHandle {
        // Start timer processor if enabled
        let timer_handle = if let Some(processor) = &self.timer_processor {
            Some(processor.clone().start())
        } else {
            None
        };

        // Start worker loop
        let worker_handle = tokio::spawn({
            let worker = self.clone();
            async move {
                worker.run().await;
            }
        });

        WorkerHandle {
            worker: worker_handle,
            timer: timer_handle,
        }
    }
}

pub struct WorkerHandle {
    worker: JoinHandle<()>,
    timer: Option<TimerProcessorHandle>,
}

impl WorkerHandle {
    pub async fn shutdown(self) {
        // Shutdown timer processor first
        if let Some(timer) = self.timer {
            timer.shutdown().await;
        }

        // Then shutdown worker
        let _ = self.worker.await;
    }
}
```

#### 3.2 Example: Timer Demo

**File:** `ergon/examples/timer_demo.rs` (NEW)

```rust
//! Durable Timer Demo
//!
//! Demonstrates:
//! - Basic timer usage
//! - Named timers for debugging
//! - Timer survival across worker crashes
//! - Multiple concurrent timers
//!
//! Run: cargo run --example timer_demo

use ergon::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize)]
struct OrderWorkflow {
    order_id: String,
}

impl OrderWorkflow {
    #[flow]
    async fn process_order(self: Arc<Self>) -> Result<String, String> {
        println!("Order {} created at {:?}", self.order_id, Utc::now());

        // Wait 5 seconds before processing
        self.wait_processing_delay().await;

        println!("Processing order {} at {:?}", self.order_id, Utc::now());
        self.charge_payment().await?;

        // Wait 10 seconds before shipping
        self.wait_shipping_delay().await;

        println!("Shipping order {} at {:?}", self.order_id, Utc::now());
        self.ship_order().await?;

        Ok(format!("Order {} completed", self.order_id))
    }

    #[step]
    async fn wait_processing_delay(&self) {
        schedule_timer(Duration::from_secs(5)).await;
    }

    #[step]
    async fn charge_payment(&self) -> Result<(), String> {
        println!("  💳 Charging payment for {}", self.order_id);
        Ok(())
    }

    #[step]
    async fn wait_shipping_delay(&self) {
        schedule_timer_named(Duration::from_secs(10), "shipping-delay").await;
    }

    #[step]
    async fn ship_order(&self) -> Result<(), String> {
        println!("  📦 Shipping order {}", self.order_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let storage = Arc::new(InMemoryExecutionLog::new());
    let scheduler = Arc::new(FlowScheduler::new(storage.clone()));

    // Start worker with timer processor
    let worker = Arc::new(
        FlowWorker::new(storage.clone(), scheduler.clone(), "worker-1")
            .with_timer_processor()  // Enable timers!
    );

    worker.register_flow(|flow: Arc<OrderWorkflow>| flow.process_order());
    let handle = worker.start().await;

    // Schedule an order
    let order = OrderWorkflow {
        order_id: "ORDER-001".to_string(),
    };

    let flow_id = Uuid::new_v4();
    scheduler.schedule(order, flow_id).await?;

    println!("Order scheduled, will complete in ~15 seconds");
    println!("Timers: 5s (processing) + 10s (shipping)\n");

    // Wait for completion
    tokio::time::sleep(Duration::from_secs(20)).await;

    handle.shutdown().await;
    Ok(())
}
```

#### 3.3 Testing Strategy

**File:** `ergon/tests/timer_tests.rs` (NEW)

```rust
#[tokio::test]
async fn test_timer_fires_after_duration() {
    let storage = Arc::new(InMemoryExecutionLog::new());
    let flow_id = Uuid::new_v4();
    let step = 1;

    // Schedule timer for 100ms in future
    let fire_at = Utc::now() + chrono::Duration::milliseconds(100);
    storage.log_timer(flow_id, step, fire_at, None).await.unwrap();

    // Timer should not be expired yet
    let expired = storage.get_expired_timers(Utc::now()).await.unwrap();
    assert_eq!(expired.len(), 0);

    // Wait for timer to expire
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Timer should be expired now
    let expired = storage.get_expired_timers(Utc::now()).await.unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].flow_id, flow_id);
    assert_eq!(expired[0].step, step);
}

#[tokio::test]
async fn test_optimistic_concurrency_prevents_duplicate_firing() {
    let storage = Arc::new(InMemoryExecutionLog::new());
    let flow_id = Uuid::new_v4();
    let step = 1;

    // Schedule a timer that's already expired
    let fire_at = Utc::now() - chrono::Duration::seconds(1);
    storage.log_timer(flow_id, step, fire_at, None).await.unwrap();

    // Two workers try to claim it
    let claimed1 = storage.claim_timer(flow_id, step).await.unwrap();
    let claimed2 = storage.claim_timer(flow_id, step).await.unwrap();

    // Only ONE should succeed
    assert_eq!(claimed1, true);
    assert_eq!(claimed2, false);
}

#[tokio::test]
async fn test_timer_survives_worker_restart() {
    // This test demonstrates crash recovery
    let storage = Arc::new(SqliteExecutionLog::new(":memory:").unwrap());

    // Worker 1: Schedule timer and crash
    {
        let flow = TestFlow { id: "test".to_string() };
        let instance = FlowInstance::new(Uuid::new_v4(), flow, storage.clone());

        // Start flow, schedule timer, then "crash" (drop instance)
        tokio::spawn(async move {
            instance.execute(|f| f.with_timer()).await.ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;
        // Worker crashed! Instance dropped.
    }

    // Worker 2: Resume and complete
    let scheduler = Arc::new(FlowScheduler::new(storage.clone()));
    let processor = TimerProcessor::new(storage, scheduler)
        .with_poll_interval(Duration::from_millis(10))
        .start();

    // Wait for timer to fire
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Timer should have fired and flow resumed
    // (Check execution_log for completion)

    processor.shutdown().await;
}
```

---

### Phase 4: Observability & Production Readiness (Week 4)

**Goal:** Add metrics, logging, documentation

#### 4.1 Metrics

Add to `ergon/src/executor/timer_processor.rs`:

```rust
use prometheus::{Counter, Histogram, Registry};

lazy_static::lazy_static! {
    static ref TIMER_SCHEDULED_TOTAL: Counter = Counter::new(
        "ergon_timer_scheduled_total",
        "Total number of timers scheduled"
    ).unwrap();

    static ref TIMER_FIRED_TOTAL: Counter = Counter::new(
        "ergon_timer_fired_total",
        "Total number of timers fired"
    ).unwrap();

    static ref TIMER_CLAIM_FAILURES_TOTAL: Counter = Counter::new(
        "ergon_timer_claim_failures_total",
        "Total number of failed timer claims (another worker won)"
    ).unwrap();

    static ref TIMER_FIRE_LATENCY: Histogram = Histogram::new(
        "ergon_timer_fire_latency_seconds",
        "Latency between scheduled fire time and actual fire time"
    ).unwrap();
}
```

#### 4.2 Documentation

Create `docs/timers.md`:

```markdown
# Durable Timers Guide

## Overview

Ergon's durable timers allow workflows to pause execution for arbitrary
durations while maintaining crash resilience and exactly-once semantics.

## Basic Usage

```rust
#[step]
async fn wait_one_day(&self) {
    schedule_timer(Duration::from_secs(86400)).await;
}
```

## Advanced Patterns

### Named Timers
### Cancellation
### Timeout Patterns

## Architecture

### How Timers Work
### Distributed Timer Coordination
### Crash Recovery

## Best Practices

1. Use appropriate durations (timers have ±1s precision)
2. Make timer handlers idempotent
3. Use named timers for long-running workflows
4. Don't use timers for sub-second precision

## Troubleshooting
```

---

## Testing Strategy

### Unit Tests
- ✅ Timer fires after duration
- ✅ Optimistic concurrency prevents duplicate firing
- ✅ Timer survives worker crash
- ✅ Late firing (timer missed, fires immediately on recovery)
- ✅ Named timers are tracked correctly
- ✅ Multiple timers fire independently

### Integration Tests
- ✅ Flow with multiple timers
- ✅ Timer + signal combination
- ✅ Timer in parallel DAG execution
- ✅ Multi-worker timer coordination
- ✅ Redis backend timer coordination

### Performance Tests
- ✅ 1000 timers/second throughput
- ✅ 1 million pending timers (memory usage)
- ✅ Timer fire latency < 2 seconds (p99)

---

## Success Criteria

✅ Timers survive worker crashes
✅ Multiple workers don't fire same timer twice
✅ Clock skew < 5 seconds between workers is handled
✅ Performance: 1000+ timers/second on single worker
✅ All tests pass (unit + integration)
✅ Example code works end-to-end
✅ Documentation complete

---

## What We're NOT Doing (v1)

To keep this simple and ship quickly:

❌ **Sub-second precision** - ±1 second granularity is fine
❌ **Custom clocks** - Use system time (Utc::now())
❌ **Logical clocks** - Clock skew is acceptable
❌ **Timer cancellation API** - Add later if needed
❌ **Timer introspection API** - Add later for debugging

We can add these later if users demand them.

---

## Migration Path

### Upgrading Existing Installations

The `timer_fire_at` column already exists in the execution_log table!
Only need to add the index:

```sql
CREATE INDEX IF NOT EXISTS idx_timer_ready
ON execution_log(status, timer_fire_at)
WHERE status = 'WAITING_FOR_TIMER' AND timer_fire_at IS NOT NULL;
```

For Redis, no migration needed (timers are in memory).

---

## Timeline

- **Week 1:** Core infrastructure (status enum, storage methods, basic API)
- **Week 2:** Multi-backend support (Redis, in-memory)
- **Week 3:** Integration & testing
- **Week 4:** Polish & documentation

**Total:** 4 weeks to production-ready durable timers

---

## Key Files to Create/Modify

### New Files
- `ergon/src/executor/timer.rs` - Timer API
- `ergon/src/executor/timer_processor.rs` - Background processor
- `ergon/examples/timer_demo.rs` - Example usage
- `ergon/tests/timer_tests.rs` - Tests
- `docs/timers.md` - Documentation

### Modified Files
- `ergon/src/core/invocation.rs` - Add `WaitingForTimer` status, timer fields
- `ergon/src/storage/mod.rs` - Add timer trait methods
- `ergon/src/storage/sqlite.rs` - Implement timer methods, add index
- `ergon/src/storage/redis.rs` - Implement timer methods with ZSET
- `ergon/src/storage/memory.rs` - Implement in-memory timer tracking
- `ergon/src/executor/worker.rs` - Integrate timer processor
- `ergon/src/executor/mod.rs` - Export timer types

---

## Implementation Checklist

### Phase 1: Core Infrastructure
- [ ] Add `WaitingForTimer` to `InvocationStatus` enum
- [ ] Add timer fields to `Invocation` struct
- [ ] Add timer methods to `ExecutionLog` trait
- [ ] Implement SQLite timer methods
- [ ] Create `timer.rs` with `schedule_timer` API
- [ ] Create `timer_processor.rs` with background processor
- [ ] Add SQLite index for timer queries

### Phase 2: Multi-Backend
- [ ] Implement Redis timer methods with ZSET
- [ ] Implement in-memory timer tracking with BTreeMap
- [ ] Test timer coordination across backends

### Phase 3: Integration & Testing
- [ ] Integrate `TimerProcessor` with `FlowWorker`
- [ ] Create `timer_demo.rs` example
- [ ] Write unit tests (10+ tests)
- [ ] Write integration tests (5+ tests)
- [ ] Test crash recovery scenarios

### Phase 4: Production Readiness
- [ ] Add Prometheus metrics
- [ ] Add structured logging
- [ ] Write timer documentation
- [ ] Update README with timer examples
- [ ] Performance testing (1000 timers/sec)

---

## Conclusion

Durable timers are **simpler than initially thought** because:

1. ✅ We already have the coordination mechanism (execution log)
2. ✅ We already have the concurrency pattern (optimistic updates in dequeue_flow)
3. ✅ Schema already has timer support (timer_fire_at column exists!)
4. ✅ Apalis/Temporal prove this approach works at scale

This is a **4-week project** to add a killer feature that brings Ergon to feature-parity with Temporal's timer capabilities.

**Let's build it!** 🚀
