//! Retry helper for step execution.
//!
//! This module provides a generic retry mechanism that:
//! - Respects RetryPolicy (max attempts, backoff delays)
//! - Uses RetryableError trait to determine which errors to retry
//! - Handles exponential backoff with delays

use crate::core::{RetryPolicy, RetryableError};
use std::future::Future;

/// Executes a fallible operation with retry logic.
///
/// This function:
/// 1. Attempts to execute the operation
/// 2. On success, returns the result
/// 3. On failure:
///    - Checks if the error is retryable (using RetryableError trait)
///    - Checks if we have attempts remaining (using RetryPolicy)
///    - If both conditions are met, delays and retries
///    - Otherwise, returns the error
///
/// # Type Parameters
///
/// * `F` - The async operation to execute
/// * `T` - The success type
/// * `E` - The error type (must implement RetryableError)
///
/// # Arguments
///
/// * `retry_policy` - Optional retry policy (None means no retries)
/// * `operation` - The async operation to execute (receives current attempt number)
///
/// # Example
///
/// ```ignore
/// use ergon::RetryPolicy;
///
/// let result = retry_with_policy(
///     Some(RetryPolicy::STANDARD),
///     || async { risky_operation().await }
/// ).await;
/// ```
pub async fn retry_with_policy<F, Fut, T, E>(
    retry_policy: Option<RetryPolicy>,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: RetryableError + std::fmt::Debug,
{
    // If no retry policy, execute once
    let Some(policy) = retry_policy else {
        return operation(1).await;
    };

    let mut attempt = 1;

    loop {
        match operation(attempt).await {
            Ok(value) => return Ok(value),
            Err(error) => {
                // Check if we should retry this error
                let should_retry = error.is_retryable();

                // Check if we have attempts remaining
                let has_attempts_remaining = attempt < policy.max_attempts;

                if should_retry && has_attempts_remaining {
                    // Calculate delay for next attempt
                    if let Some(delay) = policy.delay_for_attempt(attempt) {
                        tracing::debug!(
                            "Step failed with retryable error (attempt {}/{}): {:?}. Retrying after {:?}",
                            attempt,
                            policy.max_attempts,
                            error,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    } else {
                        // No delay means we've exhausted retries
                        tracing::warn!(
                            "Step failed and exhausted retry attempts ({}/{}): {:?}",
                            attempt,
                            policy.max_attempts,
                            error
                        );
                        return Err(error);
                    }
                } else {
                    if !should_retry {
                        tracing::debug!("Step failed with non-retryable error: {:?}", error);
                    } else {
                        tracing::warn!(
                            "Step failed and exhausted retry attempts ({}/{}): {:?}",
                            attempt,
                            policy.max_attempts,
                            error
                        );
                    }
                    return Err(error);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug)]
    struct TestError {
        retryable: bool,
    }

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestError(retryable={})", self.retryable)
        }
    }

    impl std::error::Error for TestError {}

    impl RetryableError for TestError {
        fn is_retryable(&self) -> bool {
            self.retryable
        }
    }

    #[tokio::test]
    async fn test_retry_success_first_attempt() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_policy(Some(RetryPolicy::STANDARD), |_attempt| {
            let counter = counter_clone.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok::<_, TestError>(42)
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let policy = RetryPolicy {
            max_attempts: 3,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            backoff_multiplier: 2.0,
        };

        let result = retry_with_policy(Some(policy), |_attempt| {
            let counter = counter_clone.clone();
            async move {
                let count = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if count < 3 {
                    Err(TestError { retryable: true })
                } else {
                    Ok::<_, TestError>(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_non_retryable_error() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_policy(Some(RetryPolicy::STANDARD), |_attempt| {
            let counter = counter_clone.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<i32, _>(TestError { retryable: false })
            }
        })
        .await;

        assert!(result.is_err());
        assert!(!result.unwrap_err().retryable);
        assert_eq!(counter.load(Ordering::SeqCst), 1); // Only tried once
    }

    #[tokio::test]
    async fn test_retry_exhausted_attempts() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let policy = RetryPolicy {
            max_attempts: 3,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            backoff_multiplier: 2.0,
        };

        let result = retry_with_policy(Some(policy), |_attempt| {
            let counter = counter_clone.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<i32, _>(TestError { retryable: true })
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 3); // Tried max_attempts times
    }

    #[tokio::test]
    async fn test_no_retry_policy() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_policy(None, |_attempt| {
            let counter = counter_clone.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<i32, _>(TestError { retryable: true })
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1); // No retries without policy
    }
}
