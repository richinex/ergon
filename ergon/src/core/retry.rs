// =============================================================================
// STEP ERROR RETRY BEHAVIOR
// =============================================================================
//
// DESIGN RATIONALE (Option D + A from brainstorm):
//
// By default, `Result::Err` values are NOT cached because most errors are
// transient (network timeouts, service unavailable, etc.) and should be retried.
//
// Two mechanisms control retry behavior:
//
// 1. RETRY POLICY: `#[step(retry = 3)]` or `#[step(retry = RetryPolicy::STANDARD)]`
//    - Controls HOW MANY times and WHEN to retry (backoff strategy)
//    - Simple: `retry = 3` for up to 3 attempts with standard backoff
//    - Advanced: Custom `RetryPolicy` for full control
//
// 2. RETRYABLE ERROR TRAIT: `Retryable` trait
//    - Controls WHICH errors should be retried
//    - Implement on your error type for fine-grained control
//    - Each error variant can specify if it's retryable
//
// 3. SIMPLE OVERRIDE: `#[step(cache_errors)]`
//    - Caches ALL errors immediately (treats them as permanent)
//    - Overrides both RetryPolicy and Retryable
//
// This follows Dave Cheney's principle "APIs should be hard to misuse":
// - Safe default: no automatic retries (backward compatible)
// - Simple retry: `retry = 3` (up to 3 attempts with standard backoff)
// - Advanced control: `Retryable` trait + custom `RetryPolicy`
//
// IMPLEMENTATION:
//
// The macro generates code that:
// 1. If `cache_errors` is set: always cache the result, no retries
// 2. If result is Ok: always cache
// 3. If result is Err and retry_policy is set:
//    - Check if error.is_retryable() (if Retryable implemented)
//    - If is_retryable() == false: cache error, stop retrying
//    - If is_retryable() == true AND attempts < max_attempts: retry with backoff
//    - If is_retryable() == true AND attempts >= max_attempts: return error
// 4. If result is Err and no retry_policy: return error (no retry)
// =============================================================================

use std::time::Duration;

// =============================================================================
// RETRY POLICY
// =============================================================================

/// Configuration for step retry behavior.
///
/// Controls how many times a step should retry on transient errors and
/// the backoff strategy between attempts.
///
/// # Examples
///
/// ```rust,ignore
/// use ergon::RetryPolicy;
/// use std::time::Duration;
///
/// // Simple: just specify max attempts (uses standard delays)
/// #[step(retry = 3)]
/// async fn fetch_data() -> Result<Data, Error> { ... }
///
/// // Named policy: predefined sensible defaults
/// #[step(retry = RetryPolicy::STANDARD)]
/// async fn fetch_data() -> Result<Data, Error> { ... }
///
/// // Custom policy: full control, reusable
/// const API_RETRY: RetryPolicy = RetryPolicy {
///     max_attempts: 5,
///     initial_delay: Duration::from_secs(1),
///     max_delay: Duration::from_secs(30),
///     backoff_multiplier: 2.0,
/// };
///
/// #[step(retry = API_RETRY)]
/// async fn call_api() -> Result<Response, Error> { ... }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first try).
    ///
    /// For example, `max_attempts = 3` means:
    /// - Attempt 1: immediate (first try)
    /// - Attempt 2: after initial_delay
    /// - Attempt 3: after initial_delay * backoff_multiplier
    ///
    /// Default: 1 (no retries, backward compatible)
    pub max_attempts: u32,

    /// Initial delay before the first retry.
    ///
    /// Default: 1 second
    pub initial_delay: Duration,

    /// Maximum delay between retries (caps exponential backoff).
    ///
    /// Default: 60 seconds
    pub max_delay: Duration,

    /// Multiplier for exponential backoff.
    ///
    /// Each retry delay is calculated as:
    /// `min(initial_delay * backoff_multiplier^(attempt-1), max_delay)`
    ///
    /// Default: 2.0 (doubles each time)
    pub backoff_multiplier: f64,
}

impl RetryPolicy {
    /// No retries - fail immediately on first error.
    ///
    /// Use this for validation errors or other permanent failures.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// #[step(retry = RetryPolicy::NONE)]
    /// async fn validate_input(input: String) -> Result<(), Error> {
    ///     if input.is_empty() {
    ///         return Err("Input cannot be empty".into());
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub const NONE: Self = Self {
        max_attempts: 1,
        initial_delay: Duration::from_secs(0),
        max_delay: Duration::from_secs(0),
        backoff_multiplier: 1.0,
    };

    /// Standard retry policy - sensible defaults for most use cases.
    ///
    /// - Max attempts: 3 (initial try + 2 retries)
    /// - Initial delay: 1 second
    /// - Max delay: 30 seconds
    /// - Backoff: exponential (2x each time)
    ///
    /// Retry schedule: immediate → 1s → 2s
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// #[step(retry = RetryPolicy::STANDARD)]
    /// async fn fetch_data() -> Result<Data, Error> {
    ///     http::get("https://api.example.com/data").await
    /// }
    /// ```
    pub const STANDARD: Self = Self {
        max_attempts: 3,
        initial_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        backoff_multiplier: 2.0,
    };

    /// Aggressive retry policy for critical operations.
    ///
    /// - Max attempts: 10
    /// - Initial delay: 100ms
    /// - Max delay: 10 seconds
    /// - Backoff: exponential (1.5x each time)
    ///
    /// Retry schedule: immediate → 100ms → 150ms → 225ms → ... → max 10s
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// #[step(retry = RetryPolicy::AGGRESSIVE)]
    /// async fn save_to_db() -> Result<(), DbError> {
    ///     database.save(data).await
    /// }
    /// ```
    pub const AGGRESSIVE: Self = Self {
        max_attempts: 10,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(10),
        backoff_multiplier: 1.5,
    };

    /// Create a policy with custom max_attempts (uses standard delays).
    ///
    /// This is used for the shorthand `retry = 3` syntax.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // These are equivalent:
    /// #[step(retry = 3)]
    /// #[step(retry = RetryPolicy::with_max_attempts(3))]
    /// async fn my_step() -> Result<T, E> { ... }
    /// ```
    pub const fn with_max_attempts(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        }
    }

    /// Calculate the delay before the next retry attempt.
    ///
    /// Uses exponential backoff: `initial_delay * backoff_multiplier^(attempt-1)`
    /// capped at `max_delay`.
    ///
    /// # Arguments
    ///
    /// * `attempt` - The current attempt number (1-indexed)
    ///
    /// # Returns
    ///
    /// Duration to wait before the next retry, or None if no more retries.
    pub fn delay_for_attempt(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_attempts {
            return None; // No more retries
        }

        // Calculate: initial_delay * multiplier^(attempt-1)
        // attempt=1 (first retry): multiplier^0 = 1 → initial_delay
        // attempt=2 (second retry): multiplier^1 → initial_delay * multiplier
        // attempt=3 (third retry): multiplier^2 → initial_delay * multiplier^2
        let exponent = (attempt - 1) as f64;
        let multiplier = self.backoff_multiplier.powf(exponent);
        let delay_secs = self.initial_delay.as_secs_f64() * multiplier;

        // Cap at max_delay
        let delay = Duration::from_secs_f64(delay_secs.min(self.max_delay.as_secs_f64()));

        Some(delay)
    }
}

impl Default for RetryPolicy {
    /// Default is NONE (no retries) for backward compatibility.
    ///
    /// Existing code without `retry` attribute will continue to work
    /// with the same behavior (no automatic retries).
    fn default() -> Self {
        Self::NONE
    }
}

impl From<u32> for RetryPolicy {
    /// Convert a number to a RetryPolicy with that many max_attempts.
    ///
    /// Enables the shorthand syntax: `retry = 3`
    ///
    /// Uses standard delays: 1s initial, 30s max, 2x multiplier
    fn from(max_attempts: u32) -> Self {
        Self::with_max_attempts(max_attempts)
    }
}

// =============================================================================
// Autoref-based Specialization for Cache Behavior
// =============================================================================
//
// This technique allows automatic detection of whether an error type implements
// `Retryable`, without requiring extra attributes on the step.
//
// Based on dtolnay's autoref specialization used in anyhow:
// https://github.com/dtolnay/anyhow/blob/master/src/kind.rs
//
// HOW IT WORKS:
// We use two traits with the same method name but different Self types:
// - RetryableKind: implemented for E where E: Retryable (higher priority)
// - DefaultKind: implemented for &E for all E (lower priority, needs autoref)
//
// When the macro calls `(&error).error_kind().should_cache(&error)`:
// - If E: Retryable -> RetryableKind method takes &E, matches directly
// - If E: !Retryable -> DefaultKind method takes &&E, needs autoref
//
// CRITICAL: The macro must call error_kind() on the ERROR TYPE, not on Result.
// The macro generates:
//   match &__result {
//       Ok(_) => true,  // Always cache Ok results
//       Err(__e) => {
//           // __e is &E here due to matching on &__result
//           // We need to deref to get E, then take &E for the call
//           // Using (*__e) directly and calling (&*__e).error_kind()
//           (&*__e).error_kind().should_cache(&*__e)
//       }
//   }
//
// When __e: &E, (*__e): E, (&*__e): &E
// For &E calling error_kind():
// - RetryableKind: Self=E, method takes &self = &E, matches &E directly
// - DefaultKind: Self=&E, method takes &self = &&E, needs autoref from &E
// RetryableKind wins if E: Retryable!

/// Tag for retryable error handling (uses is_retryable() for fine-grained control).
pub struct RetryableTag;

/// Tag for default error handling (cache Ok, retry all Err).
pub struct DefaultTag;

/// Trait for error types that implement Retryable (higher priority).
///
/// When calling `(&error).error_kind()` where error is owned:
/// - This trait is implemented for E where E: Retryable
/// - The method takes &self = &E
/// - &E matches the call (&error) directly
///
/// Based on dtolnay's autoref specialization pattern from anyhow.
pub trait RetryableKind: Sized {
    #[inline]
    fn error_kind(&self) -> RetryableTag {
        RetryableTag
    }
}

/// Trait for all error types (lower priority - requires autoref).
///
/// When calling `(&error).error_kind()` where error is owned:
/// - This trait is implemented for &E (note the &)
/// - The method takes &self = &&E
/// - &E calling this method needs autoref to &&E
///
/// Based on dtolnay's autoref specialization pattern from anyhow.
pub trait DefaultKind: Sized {
    #[inline]
    fn error_kind(&self) -> DefaultTag {
        DefaultTag
    }
}

// Higher priority: implemented for E where E: Retryable
// When calling (error).error_kind() where error: E and E: Retryable:
// - error has type E
// - E implements RetryableKind directly
// - Returns RetryableTag which delegates to user's Retryable impl
impl<E: crate::executor::Retryable> RetryableKind for E {}

// Lower priority: implemented for &E for any E (via autoref)
// When calling (error).error_kind() where error: E and E does NOT implement Retryable:
// - error has type E
// - E doesn't implement RetryableKind (no Retryable bound)
// - Rust autorefs to &E to match this impl
// - Returns DefaultTag (all errors retryable by default)
impl<E> DefaultKind for &E {}

impl RetryableTag {
    /// Returns true if the error should be cached (permanent error).
    /// Uses Retryable::is_retryable() for fine-grained control.
    #[inline]
    pub fn should_cache<E: crate::executor::Retryable>(self, error: &E) -> bool {
        !error.is_retryable() // Permanent errors cached, transient errors retry
    }

    /// Returns true if the error is retryable (transient error).
    /// Uses Retryable::is_retryable() for fine-grained control.
    #[inline]
    pub fn is_retryable<E: crate::executor::Retryable>(self, error: &E) -> bool {
        error.is_retryable() // Delegate to the trait implementation
    }
}

impl DefaultTag {
    /// Returns true if the error should be cached.
    /// Default behavior: don't cache any errors (allow retry for all).
    #[inline]
    pub fn should_cache<E>(self, _error: &E) -> bool {
        false // Default: don't cache errors, allow retry
    }

    /// Returns true if the error is retryable.
    /// Default behavior: all errors are retryable (transient).
    #[inline]
    pub fn is_retryable<E>(self, _error: &E) -> bool {
        true // Default: all errors are retryable
    }
}

// Re-export the old names for backwards compatibility (deprecated)
#[doc(hidden)]
pub use DefaultKind as DefaultResultKind;
#[doc(hidden)]
pub use RetryableKind as RetryableResultKind;

// Re-export kind module for macro use
pub mod kind {
    pub use super::{DefaultKind, RetryableKind};
}

// =============================================================================
// RETRY EXECUTION (moved from executor/retry_helper.rs)
// =============================================================================

use std::future::Future;

/// Executes a fallible operation with retry logic.
///
/// This function executes the policy definitions above. It:
/// 1. Attempts to execute the operation
/// 2. On success, returns the result
/// 3. On failure:
///    - Checks if the error is retryable (using Retryable trait)
///    - Checks if we have attempts remaining (using RetryPolicy)
///    - If both conditions are met, delays and retries
///    - Otherwise, returns the error
///
/// # Type Parameters
///
/// * `F` - The async operation to execute
/// * `T` - The success type
/// * `E` - The error type (must implement Retryable)
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
///     |_attempt| async { risky_operation().await }
/// ).await;
/// ```
pub async fn retry_with_policy<F, Fut, T, E>(
    retry_policy: Option<RetryPolicy>,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: crate::executor::Retryable + std::fmt::Debug,
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
                            "Operation failed with retryable error (attempt {}/{}): {:?}. Retrying after {:?}",
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
                            "Operation failed and exhausted retry attempts ({}/{}): {:?}",
                            attempt,
                            policy.max_attempts,
                            error
                        );
                        return Err(error);
                    }
                } else {
                    if !should_retry {
                        tracing::debug!("Operation failed with non-retryable error: {:?}", error);
                    } else {
                        tracing::warn!(
                            "Operation failed and exhausted retry attempts ({}/{}): {:?}",
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
    use crate::executor::Retryable;

    // =============================================================================
    // RETRY POLICY TESTS
    // =============================================================================

    #[test]
    fn test_retry_policy_none() {
        let policy = RetryPolicy::NONE;
        assert_eq!(policy.max_attempts, 1);
        assert_eq!(policy.initial_delay, Duration::from_secs(0));
        assert_eq!(policy.max_delay, Duration::from_secs(0));
        assert_eq!(policy.backoff_multiplier, 1.0);

        // No retries - should return None for any attempt
        assert_eq!(policy.delay_for_attempt(1), None);
        assert_eq!(policy.delay_for_attempt(2), None);
    }

    #[test]
    fn test_retry_policy_standard() {
        let policy = RetryPolicy::STANDARD;
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_delay, Duration::from_secs(1));
        assert_eq!(policy.max_delay, Duration::from_secs(30));
        assert_eq!(policy.backoff_multiplier, 2.0);

        // Test backoff schedule: 1s, 2s, then None
        assert_eq!(policy.delay_for_attempt(1), Some(Duration::from_secs(1))); // First retry
        assert_eq!(policy.delay_for_attempt(2), Some(Duration::from_secs(2))); // Second retry
        assert_eq!(policy.delay_for_attempt(3), None); // No more retries
        assert_eq!(policy.delay_for_attempt(4), None); // Still no retries
    }

    #[test]
    fn test_retry_policy_aggressive() {
        let policy = RetryPolicy::AGGRESSIVE;
        assert_eq!(policy.max_attempts, 10);
        assert_eq!(policy.initial_delay, Duration::from_millis(100));
        assert_eq!(policy.max_delay, Duration::from_secs(10));
        assert_eq!(policy.backoff_multiplier, 1.5);

        // Test first few backoff delays: 100ms, 150ms, 225ms, ...
        assert_eq!(
            policy.delay_for_attempt(1),
            Some(Duration::from_millis(100))
        ); // 100 * 1.5^0
        assert_eq!(
            policy.delay_for_attempt(2),
            Some(Duration::from_millis(150))
        ); // 100 * 1.5^1
        assert_eq!(
            policy.delay_for_attempt(3),
            Some(Duration::from_millis(225))
        ); // 100 * 1.5^2
    }

    #[test]
    fn test_retry_policy_with_max_attempts() {
        let policy = RetryPolicy::with_max_attempts(5);
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.initial_delay, Duration::from_secs(1));
        assert_eq!(policy.max_delay, Duration::from_secs(30));
        assert_eq!(policy.backoff_multiplier, 2.0);

        // Should allow 5 attempts total (attempt 1-4 return delays, attempt 5 returns None)
        assert!(policy.delay_for_attempt(1).is_some());
        assert!(policy.delay_for_attempt(2).is_some());
        assert!(policy.delay_for_attempt(3).is_some());
        assert!(policy.delay_for_attempt(4).is_some());
        assert_eq!(policy.delay_for_attempt(5), None); // max_attempts reached
    }

    #[test]
    fn test_retry_policy_exponential_backoff() {
        let policy = RetryPolicy {
            max_attempts: 10,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
        };

        // Test exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, 60s (capped)
        assert_eq!(policy.delay_for_attempt(1), Some(Duration::from_secs(1))); // 1 * 2^0
        assert_eq!(policy.delay_for_attempt(2), Some(Duration::from_secs(2))); // 1 * 2^1
        assert_eq!(policy.delay_for_attempt(3), Some(Duration::from_secs(4))); // 1 * 2^2
        assert_eq!(policy.delay_for_attempt(4), Some(Duration::from_secs(8))); // 1 * 2^3
        assert_eq!(policy.delay_for_attempt(5), Some(Duration::from_secs(16))); // 1 * 2^4
        assert_eq!(policy.delay_for_attempt(6), Some(Duration::from_secs(32))); // 1 * 2^5
        assert_eq!(policy.delay_for_attempt(7), Some(Duration::from_secs(60))); // 1 * 2^6 = 64, capped at 60
        assert_eq!(policy.delay_for_attempt(8), Some(Duration::from_secs(60))); // Still capped
    }

    #[test]
    fn test_retry_policy_max_delay_capping() {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(15), // Cap at 15 seconds
            backoff_multiplier: 2.0,
        };

        // Test that delays are capped at max_delay
        assert_eq!(policy.delay_for_attempt(1), Some(Duration::from_secs(10))); // 10 * 2^0 = 10
        assert_eq!(policy.delay_for_attempt(2), Some(Duration::from_secs(15))); // 10 * 2^1 = 20, capped at 15
        assert_eq!(policy.delay_for_attempt(3), Some(Duration::from_secs(15))); // 10 * 2^2 = 40, capped at 15
        assert_eq!(policy.delay_for_attempt(4), Some(Duration::from_secs(15))); // Still capped
    }

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy, RetryPolicy::NONE);

        // Default should be NONE (no retries) for backward compatibility
        assert_eq!(policy.max_attempts, 1);
    }

    #[test]
    fn test_retry_policy_from_u32() {
        let policy: RetryPolicy = 5.into();
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.initial_delay, Duration::from_secs(1));
        assert_eq!(policy.max_delay, Duration::from_secs(30));
        assert_eq!(policy.backoff_multiplier, 2.0);

        // Should be equivalent to with_max_attempts
        assert_eq!(policy, RetryPolicy::with_max_attempts(5));
    }

    #[test]
    fn test_retry_policy_edge_cases() {
        // Test with 0 max_attempts (shouldn't happen, but let's be defensive)
        let policy_zero = RetryPolicy::with_max_attempts(0);
        assert_eq!(policy_zero.delay_for_attempt(0), None);
        assert_eq!(policy_zero.delay_for_attempt(1), None);

        // Test with 1 max_attempt (equivalent to NONE)
        let policy_one = RetryPolicy::with_max_attempts(1);
        assert_eq!(policy_one.delay_for_attempt(1), None); // Already at max
        assert_eq!(policy_one.delay_for_attempt(2), None);

        // Test with very large multiplier
        let policy_large = RetryPolicy {
            max_attempts: 5,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(100),
            backoff_multiplier: 10.0,
        };
        assert_eq!(
            policy_large.delay_for_attempt(1),
            Some(Duration::from_millis(10))
        ); // 10 * 10^0
        assert_eq!(
            policy_large.delay_for_attempt(2),
            Some(Duration::from_millis(100))
        ); // 10 * 10^1
        assert_eq!(
            policy_large.delay_for_attempt(3),
            Some(Duration::from_millis(1000))
        ); // 10 * 10^2
        assert_eq!(
            policy_large.delay_for_attempt(4),
            Some(Duration::from_secs(10))
        ); // 10 * 10^3 = 10000ms = 10s
    }

    #[test]
    fn test_retry_policy_realistic_scenario() {
        // Test a realistic API retry policy:
        // - 3 attempts
        // - Start with 500ms delay
        // - Cap at 5 seconds
        // - 2x multiplier
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 2.0,
        };

        // Attempt 1 (first retry): 500ms
        assert_eq!(
            policy.delay_for_attempt(1),
            Some(Duration::from_millis(500))
        );

        // Attempt 2 (second retry): 1000ms
        assert_eq!(
            policy.delay_for_attempt(2),
            Some(Duration::from_millis(1000))
        );

        // Attempt 3: no more retries
        assert_eq!(policy.delay_for_attempt(3), None);
    }

    // =============================================================================
    // RETRYABLE ERROR TESTS
    // =============================================================================

    #[test]
    fn test_retryable_error_trait() {
        // Test std::io::Error implementation
        let timeout_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
        assert!(timeout_err.is_retryable());

        let not_found_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        assert!(!not_found_err.is_retryable());

        // Test String implementation (retryable by default - safe default)
        let string_err = "some error".to_string();
        assert!(string_err.is_retryable());
    }

    #[test]
    fn test_autoref_specialization_with_retryable_error() {
        // Test autoref specialization: when E: Retryable, uses is_retryable()
        // The macro matches on Result and calls (&*error).error_kind().should_cache(&*error)

        // io::Error implements Retryable
        // TimedOut is retryable -> should NOT cache (allow retry)
        let timeout_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
        assert!(!timeout_err.error_kind().should_cache(&timeout_err));

        // NotFound is NOT retryable -> should cache (permanent error)
        let not_found_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        assert!(not_found_err.error_kind().should_cache(&not_found_err));

        // String implements Retryable (always returns true = retryable)
        // String.is_retryable() returns true -> should NOT cache (transient error)
        let string_err = "error".to_string();
        assert!(!string_err.error_kind().should_cache(&string_err));
    }

    #[test]
    fn test_autoref_specialization_without_retryable_error() {
        // Test autoref specialization: when E: !Retryable, uses default behavior
        // This tests with an error type that does NOT implement Retryable

        #[derive(Debug)]
        struct CustomError;

        // CustomError doesn't implement Retryable -> default behavior
        // Default: Err is NOT cached (allows retry for all errors)
        let custom_err = CustomError;
        assert!(!(&custom_err).error_kind().should_cache(&custom_err));
    }

    #[test]
    fn test_autoref_specialization_macro_pattern() {
        // Test the exact pattern the macro generates
        // This simulates what happens inside the macro-generated code

        // Case 1: Error implements Retryable, is_retryable = true (transient)
        let result1: std::result::Result<i32, std::io::Error> =
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
        let should_cache1 = match &result1 {
            Ok(_) => true,
            Err(__e) => (*__e).error_kind().should_cache(__e),
        };
        assert!(!should_cache1); // Transient error should NOT be cached

        // Case 2: Error implements Retryable, is_retryable = false (permanent)
        let result2: std::result::Result<i32, std::io::Error> = Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not found",
        ));
        let should_cache2 = match &result2 {
            Ok(_) => true,
            Err(__e) => (*__e).error_kind().should_cache(__e),
        };
        assert!(should_cache2); // Permanent error should be cached

        // Case 3: Error does NOT implement Retryable
        #[derive(Debug)]
        struct CustomError;
        let result3: std::result::Result<i32, CustomError> = Err(CustomError);
        let should_cache3 = match &result3 {
            Ok(_) => true,
            Err(__e) => __e.error_kind().should_cache(__e),
        };
        assert!(!should_cache3); // Default: don't cache errors

        // Case 4: Ok result (always cached)
        let result4: std::result::Result<i32, CustomError> = Ok(42);
        let should_cache4 = match &result4 {
            Ok(_) => true,
            Err(__e) => __e.error_kind().should_cache(__e),
        };
        assert!(should_cache4); // Ok always cached
    }
}
