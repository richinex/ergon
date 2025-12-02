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

use super::timer::TIMER_NOTIFIERS;

/// Background timer processor that fires expired timers.
///
/// # Lifecycle
/// 1. Create: `TimerProcessor::new(storage)`
/// 2. Configure: `.with_poll_interval(duration)`
/// 3. Start: `.start()` returns handle
/// 4. Shutdown: `handle.shutdown().await`
///
/// # Example
/// ```ignore
/// let processor = TimerProcessor::new(storage)
///     .with_poll_interval(Duration::from_secs(1))
///     .start();
///
/// // ... application runs ...
///
/// processor.shutdown().await;
/// ```
pub struct TimerProcessor<S: ExecutionLog> {
    storage: Arc<S>,
    poll_interval: Duration,
    shutdown: Arc<AtomicBool>,
}

impl<S: ExecutionLog + 'static> TimerProcessor<S> {
    /// Creates a new timer processor with default configuration.
    ///
    /// Default poll interval is 1 second, providing ±1 second precision.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            poll_interval: Duration::from_secs(1),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Sets the poll interval for checking expired timers.
    ///
    /// Lower intervals = better precision, higher CPU usage.
    /// Higher intervals = lower precision, reduced CPU usage.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // High precision (100ms) - for time-critical timers
    /// let processor = TimerProcessor::new(storage)
    ///     .with_poll_interval(Duration::from_millis(100));
    ///
    /// // Low frequency (10s) - for long-duration timers
    /// let processor = TimerProcessor::new(storage)
    ///     .with_poll_interval(Duration::from_secs(10));
    /// ```
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Starts the timer processor in a background task.
    ///
    /// Returns a `TimerProcessorHandle` that must be used to stop the processor.
    /// Dropping the handle without calling shutdown will leak the background task.
    ///
    /// # Important
    ///
    /// You MUST call `handle.shutdown().await` to cleanly stop the processor.
    /// This ensures all in-flight timer operations complete before exit.
    pub fn start(self) -> TimerProcessorHandle {
        let shutdown = Arc::clone(&self.shutdown);

        let handle = tokio::spawn(async move {
            self.run().await;
        });

        TimerProcessorHandle { handle, shutdown }
    }

    /// Main processing loop - runs until shutdown signal received.
    ///
    /// This is the core of the processor lifecycle:
    /// 1. Check shutdown flag
    /// 2. Process expired timers
    /// 3. Sleep for poll interval
    /// 4. Repeat
    async fn run(self) {
        info!(
            "Timer processor started (poll_interval={:?})",
            self.poll_interval
        );

        while !self.shutdown.load(Ordering::SeqCst) {
            if let Err(e) = self.process_expired_timers().await {
                error!("Error processing timers: {}", e);
                // Continue processing despite errors - timers will retry on next poll
            }

            tokio::time::sleep(self.poll_interval).await;
        }

        info!("Timer processor stopped cleanly");
    }

    /// Process all expired timers.
    ///
    /// This method is idempotent - it can be called multiple times safely.
    /// If a timer was already fired, it won't be fired again.
    async fn process_expired_timers(&self) -> crate::storage::Result<()> {
        let expired = self.storage.get_expired_timers(Utc::now()).await?;

        if !expired.is_empty() {
            debug!("Processing {} expired timers", expired.len());
        }

        for timer in expired {
            match self.fire_timer(&timer).await {
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
    async fn fire_timer(&self, timer: &TimerInfo) -> crate::storage::Result<bool> {
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

        // TODO: Enqueue flow for resumption (in case it's not in memory)
        // This would use the scheduler to resume the flow from this step
        // For now, timers only work for in-memory flows

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
///
/// This handle must be kept alive for the duration of the processor's lifetime.
/// Dropping it without calling `shutdown()` will leak the background task.
pub struct TimerProcessorHandle {
    handle: JoinHandle<()>,
    shutdown: Arc<AtomicBool>,
}

impl TimerProcessorHandle {
    /// Gracefully shutdown the timer processor.
    ///
    /// Signals the processor to stop and waits for it to complete.
    /// This ensures all in-flight timer operations finish before returning.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let processor = TimerProcessor::new(storage).start();
    ///
    /// // ... run application ...
    ///
    /// // Clean shutdown
    /// processor.shutdown().await;
    /// ```
    pub async fn shutdown(self) {
        info!("Shutting down timer processor...");
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.handle.await;
        info!("Timer processor shutdown complete");
    }
}
