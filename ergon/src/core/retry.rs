// =============================================================================
// STEP ERROR RETRY BEHAVIOR
// =============================================================================
//
// DESIGN RATIONALE (Option D + A from brainstorm):
//
// By default, `Result::Err` values are NOT cached because most errors are
// transient (network timeouts, service unavailable, etc.) and should be retried.
//
// Two mechanisms control this behavior:
//
// 1. SIMPLE OVERRIDE: `#[step(cache_errors)]`
//    - Caches ALL errors (treats them as permanent)
//    - Use when you know all errors from this step are permanent
//
// 2. TRAIT-BASED: `RetryableError` trait
//    - Implement on your error type for fine-grained control
//    - Each error variant can specify if it's retryable
//    - The macro automatically uses this if the error type implements it
//
// This follows Dave Cheney's principle "APIs should be hard to misuse":
// - Safe default: all errors trigger retry (prevents silent failures)
// - Simple override: `cache_errors` for permanent failures
// - Advanced control: `RetryableError` trait for mixed error types
//
// IMPLEMENTATION:
//
// The macro generates code that:
// 1. If `cache_errors` is set: always cache the result
// 2. If result is Ok: always cache
// 3. If result is Err and error implements RetryableError:
//    - Cache if !error.is_retryable()
//    - Don't cache if error.is_retryable()
// 4. If result is Err and error doesn't implement RetryableError:
//    - Don't cache (default: retry all errors)
// =============================================================================

/// Trait for error types to specify whether they should trigger a retry.
///
/// Implement this trait on your error types to get fine-grained control over
/// which errors are retried vs. cached as permanent failures.
///
/// # Default Behavior (without this trait)
///
/// If your error type does NOT implement `RetryableError`:
/// - ALL errors trigger retry (not cached)
/// - Use `#[step(cache_errors)]` to cache all errors
///
/// # With RetryableError
///
/// If your error type implements `RetryableError`:
/// - `is_retryable() == true`: Error is NOT cached, step will retry
/// - `is_retryable() == false`: Error IS cached, step won't retry
///
/// # Example
///
/// ```rust
/// use ergon::RetryableError;
///
/// #[derive(Debug)]
/// enum PaymentError {
///     // Transient errors - should retry
///     NetworkTimeout,
///     ServiceUnavailable,
///     RateLimited,
///
///     // Permanent errors - should NOT retry
///     InsufficientFunds,
///     InvalidCard,
///     FraudDetected,
/// }
///
/// impl RetryableError for PaymentError {
///     fn is_retryable(&self) -> bool {
///         matches!(self,
///             PaymentError::NetworkTimeout |
///             PaymentError::ServiceUnavailable |
///             PaymentError::RateLimited
///         )
///     }
/// }
/// ```
///
/// # Design Rationale
///
/// This approach follows Dave Cheney's principle "APIs should be hard to misuse":
/// - The safe default (retry all errors) requires no extra code
/// - Fine-grained control requires explicit implementation
/// - The logic lives with the error type, not scattered in step attributes
pub trait RetryableError {
    /// Returns true if this error is transient and the operation should be retried.
    ///
    /// - `true`: Error is transient (network timeout, service unavailable).
    ///   The step will NOT be cached, allowing retry on next execution.
    /// - `false`: Error is permanent (invalid input, not found, business rule violation).
    ///   The step WILL be cached, preventing retry.
    fn is_retryable(&self) -> bool;
}

// Implement RetryableError for common error types

impl RetryableError for std::io::Error {
    fn is_retryable(&self) -> bool {
        use std::io::ErrorKind;
        matches!(
            self.kind(),
            ErrorKind::ConnectionRefused
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::NotConnected
                | ErrorKind::TimedOut
                | ErrorKind::Interrupted
                | ErrorKind::WouldBlock
        )
    }
}

impl RetryableError for String {
    /// Strings are retryable by default - allowing for transient error messages.
    ///
    /// Since String is a generic error type, we cannot distinguish between
    /// transient errors ("connection timeout") and permanent errors ("invalid input").
    /// Following the "safe default" principle, we treat all String errors as retryable.
    ///
    /// Use `#[step(cache_errors)]` or a custom error type for permanent failures.
    fn is_retryable(&self) -> bool {
        true
    }
}

impl<T: RetryableError> RetryableError for Box<T> {
    fn is_retryable(&self) -> bool {
        (**self).is_retryable()
    }
}

impl<T: RetryableError> RetryableError for std::sync::Arc<T> {
    fn is_retryable(&self) -> bool {
        (**self).is_retryable()
    }
}

// =============================================================================
// Autoref-based Specialization for Cache Behavior
// =============================================================================
//
// This technique allows automatic detection of whether an error type implements
// `RetryableError`, without requiring extra attributes on the step.
//
// Based on dtolnay's autoref specialization used in anyhow:
// https://github.com/dtolnay/anyhow/blob/master/src/kind.rs
//
// HOW IT WORKS:
// We use two traits with the same method name but different Self types:
// - RetryableKind: implemented for E where E: RetryableError (higher priority)
// - DefaultKind: implemented for &E for all E (lower priority, needs autoref)
//
// When the macro calls `(&error).error_kind().should_cache(&error)`:
// - If E: RetryableError -> RetryableKind method takes &E, matches directly
// - If E: !RetryableError -> DefaultKind method takes &&E, needs autoref
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
// RetryableKind wins if E: RetryableError!

/// Tag for retryable error handling (uses is_retryable() for fine-grained control).
pub struct RetryableTag;

/// Tag for default error handling (cache Ok, retry all Err).
pub struct DefaultTag;

/// Trait for error types that implement RetryableError (higher priority).
///
/// When calling `(&error).error_kind()` where error is owned:
/// - This trait is implemented for E where E: RetryableError
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

// Higher priority: implemented for E where E: RetryableError
// Self = E (the error type directly)
// Method takes &self = &E
// When calling (&error).error_kind(), &E matches &self directly
impl<E: RetryableError> RetryableKind for E {}

// Lower priority: implemented for &E for any E
// Self = &E (reference to error type)
// Method takes &self = &&E
// When calling (&error).error_kind(), &E needs autoref to get &&E
impl<E> DefaultKind for &E {}

impl RetryableTag {
    /// Returns true if the error should be cached (permanent error).
    /// Uses RetryableError::is_retryable() for fine-grained control.
    #[inline]
    pub fn should_cache<E: RetryableError>(self, error: &E) -> bool {
        !error.is_retryable() // Permanent errors cached, transient errors retry
    }
}

impl DefaultTag {
    /// Returns true if the error should be cached.
    /// Default behavior: don't cache any errors (allow retry for all).
    #[inline]
    pub fn should_cache<E>(self, _error: &E) -> bool {
        false // Default: don't cache errors, allow retry
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

#[cfg(test)]
mod tests {
    use super::*;

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
        // Test autoref specialization: when E: RetryableError, uses is_retryable()
        // The macro matches on Result and calls (&*error).error_kind().should_cache(&*error)

        // io::Error implements RetryableError
        // TimedOut is retryable -> should NOT cache (allow retry)
        let timeout_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
        assert!(!timeout_err.error_kind().should_cache(&timeout_err));

        // NotFound is NOT retryable -> should cache (permanent error)
        let not_found_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        assert!(not_found_err.error_kind().should_cache(&not_found_err));

        // String implements RetryableError (always returns true = retryable)
        // String.is_retryable() returns true -> should NOT cache (transient error)
        let string_err = "error".to_string();
        assert!(!string_err.error_kind().should_cache(&string_err));
    }

    #[test]
    fn test_autoref_specialization_without_retryable_error() {
        // Test autoref specialization: when E: !RetryableError, uses default behavior
        // This tests with an error type that does NOT implement RetryableError

        #[derive(Debug)]
        struct CustomError;

        // CustomError doesn't implement RetryableError -> default behavior
        // Default: Err is NOT cached (allows retry for all errors)
        let custom_err = CustomError;
        assert!(!(&custom_err).error_kind().should_cache(&custom_err));
    }

    #[test]
    fn test_autoref_specialization_macro_pattern() {
        // Test the exact pattern the macro generates
        // This simulates what happens inside the macro-generated code

        // Case 1: Error implements RetryableError, is_retryable = true (transient)
        let result1: std::result::Result<i32, std::io::Error> =
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
        let should_cache1 = match &result1 {
            Ok(_) => true,
            Err(__e) => (*__e).error_kind().should_cache(__e),
        };
        assert!(!should_cache1); // Transient error should NOT be cached

        // Case 2: Error implements RetryableError, is_retryable = false (permanent)
        let result2: std::result::Result<i32, std::io::Error> = Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "not found",
        ));
        let should_cache2 = match &result2 {
            Ok(_) => true,
            Err(__e) => (*__e).error_kind().should_cache(__e),
        };
        assert!(should_cache2); // Permanent error should be cached

        // Case 3: Error does NOT implement RetryableError
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
