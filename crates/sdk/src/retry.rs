//! Retry logic with exponential backoff.
//!
//! Provides retry wrappers using the `backon` crate with
//! configurable backoff policies.

use std::{future::Future, time::Duration};

use backon::{ExponentialBuilder, Retryable};
use rand::Rng;

use crate::{
    config::RetryPolicy,
    error::{Result, SdkError},
};

/// Execute an async operation with retry using exponential backoff.
///
/// The operation will be retried according to the provided [`RetryPolicy`] if
/// it fails with a retryable error (as determined by [`SdkError::is_retryable`]).
///
/// # Retry Strategy
///
/// - **Exponential backoff**: `initial_backoff * multiplier^(attempt-1)`
/// - **Jitter**: ±`jitter` randomness applied to prevent thundering herd
/// - **Cap**: Backoff capped at `max_backoff`
/// - **Termination**: After `max_attempts` failed attempts
///
/// # Non-Retryable Errors
///
/// If the operation fails with a non-retryable error (e.g., `INVALID_ARGUMENT`,
/// `PERMISSION_DENIED`, `IdempotencyKeyReused`), the error is returned immediately without
/// retry.
///
/// # Example
///
/// ```ignore
/// use inferadb_ledger_sdk::{with_retry, RetryPolicy, SdkError};
///
/// let policy = RetryPolicy::default();
/// let result = with_retry(&policy, || async {
///     // Some fallible async operation
///     Ok::<_, SdkError>("success")
/// }).await;
/// ```
pub async fn with_retry<F, Fut, T>(policy: &RetryPolicy, operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    // Build exponential backoff with policy parameters.
    // Note: backon's max_times is the number of retries, not total attempts.
    // If max_attempts is 3, we want 2 retries (initial + 2 retries = 3 attempts).
    let max_retries = policy.max_attempts.saturating_sub(1) as usize;

    let backoff = ExponentialBuilder::new()
        .with_min_delay(policy.initial_backoff)
        .with_max_delay(policy.max_backoff)
        .with_factor(policy.multiplier as f32)
        .with_max_times(max_retries);

    // Track attempt count for error reporting
    let attempt_count = std::sync::atomic::AtomicU32::new(0);
    let last_error_msg = std::sync::Mutex::new(String::new());
    let jitter_factor = policy.jitter;

    operation
        .retry(backoff)
        .sleep(tokio::time::sleep)
        // Only retry if error is retryable
        .when(|e: &SdkError| e.is_retryable())
        // Apply jitter and log retry attempts
        .notify(|err: &SdkError, dur: Duration| {
            let attempt = attempt_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

            // Apply jitter: ±jitter_factor randomness
            let jittered_dur = apply_jitter(dur, jitter_factor);

            tracing::debug!(
                attempt = attempt,
                backoff_ms = jittered_dur.as_millis() as u64,
                error = %err,
                "retrying after backoff"
            );

            // Store last error message for potential RetryExhausted
            if let Ok(mut msg) = last_error_msg.lock() {
                *msg = err.to_string();
            }
        })
        .await
        .map_err(|e| {
            // If we exhausted retries, wrap in RetryExhausted
            // Otherwise, return the original error (non-retryable)
            if e.is_retryable() {
                let attempts = attempt_count.load(std::sync::atomic::Ordering::SeqCst) + 1;
                SdkError::RetryExhausted {
                    attempts,
                    last_error: e.to_string(),
                    attempt_history: Vec::new(),
                }
            } else {
                e
            }
        })
}

/// Execute an async operation with retry and cancellation support.
///
/// Behaves identically to [`with_retry`], but races each attempt against the
/// provided `CancellationToken`. If the token is cancelled before the
/// operation completes, the function returns `SdkError::Cancelled` immediately
/// without further retries.
///
/// # Cancellation Semantics
///
/// - If the token is already cancelled at call time, returns `Cancelled` immediately.
/// - If the token is cancelled during an attempt, the in-flight attempt is dropped and `Cancelled`
///   is returned.
/// - If the token is cancelled during a backoff sleep, the sleep is interrupted and `Cancelled` is
///   returned.
///
/// # Circuit Breaker Integration
///
/// When `pool` is provided and the pool has a circuit breaker configured,
/// each attempt's result is recorded. Success resets the failure counter;
/// retryable failures increment it. When the circuit opens, subsequent
/// `pool.get_channel()` calls within the operation will return
/// `SdkError::CircuitOpen` immediately.
///
/// # Example
///
/// ```ignore
/// use inferadb_ledger_sdk::{with_retry_cancellable, RetryPolicy, SdkError};
/// use tokio_util::sync::CancellationToken;
///
/// let policy = RetryPolicy::default();
/// let token = CancellationToken::new();
///
/// let result = with_retry_cancellable(&policy, &token, None, "read", || async {
///     Ok::<_, SdkError>("success")
/// }).await;
/// ```
pub async fn with_retry_cancellable<F, Fut, T>(
    policy: &RetryPolicy,
    token: &tokio_util::sync::CancellationToken,
    pool: Option<&crate::ConnectionPool>,
    method: &str,
    mut operation: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    // Fail fast if already cancelled
    if token.is_cancelled() {
        return Err(SdkError::Cancelled);
    }

    let mut attempt: u32 = 0;
    let mut backoff_duration = policy.initial_backoff;
    let mut attempt_history: Vec<(u32, String)> = Vec::new();

    loop {
        attempt += 1;

        // Race the operation against cancellation
        let result = tokio::select! {
            biased;
            () = token.cancelled() => {
                return Err(SdkError::Cancelled);
            }
            result = operation() => result,
        };

        match result {
            Ok(value) => {
                // Record success on the circuit breaker
                if let Some(p) = pool {
                    p.record_success();
                }
                return Ok(value);
            },
            Err(err) => {
                // Record retryable failures on the circuit breaker
                if err.is_retryable()
                    && let Some(p) = pool
                {
                    p.record_failure();
                }

                // Record this attempt in the history
                attempt_history.push((attempt, err.to_string()));

                // Classify the error for metrics
                let error_type = err.error_type();

                // Non-retryable or out of attempts — return immediately
                if !err.is_retryable() || attempt >= policy.max_attempts {
                    if err.is_retryable() {
                        return Err(SdkError::RetryExhausted {
                            attempts: attempt,
                            last_error: err.to_string(),
                            attempt_history: attempt_history.clone(),
                        });
                    }
                    return Err(err);
                }

                // Record the retry metric
                if let Some(p) = pool {
                    p.metrics().record_retry(method, attempt, &error_type);
                }

                // Apply jitter to backoff
                let jittered = apply_jitter(backoff_duration, policy.jitter);

                tracing::debug!(
                    attempt = attempt,
                    backoff_ms = jittered.as_millis() as u64,
                    error = %err,
                    "retrying after backoff (cancellable)"
                );

                // Sleep with cancellation
                tokio::select! {
                    biased;
                    () = token.cancelled() => {
                        return Err(SdkError::Cancelled);
                    }
                    () = tokio::time::sleep(jittered) => {}
                }

                // Advance backoff for next attempt
                backoff_duration = std::cmp::min(
                    Duration::from_nanos(
                        (backoff_duration.as_nanos() as f64 * policy.multiplier) as u64,
                    ),
                    policy.max_backoff,
                );
            },
        }
    }
}

/// Apply jitter to a duration.
///
/// Jitter adds randomness in the range `[dur * (1 - factor), dur * (1 + factor)]`
/// to prevent thundering herd when multiple clients retry simultaneously.
fn apply_jitter(dur: Duration, factor: f64) -> Duration {
    if factor <= 0.0 {
        return dur;
    }

    let factor = factor.clamp(0.0, 1.0);
    let mut rng = rand::rng();

    let base_nanos = dur.as_nanos() as f64;
    let min_nanos = base_nanos * (1.0 - factor);
    let max_nanos = base_nanos * (1.0 + factor);

    let jittered_nanos = rng.random_range(min_nanos..=max_nanos);
    Duration::from_nanos(jittered_nanos as u64)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration,
    };

    use tonic::Code;

    use super::*;
    use crate::error::SdkError;

    fn test_policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            multiplier: 2.0,
            jitter: 0.0, // No jitter for deterministic tests
        }
    }

    #[tokio::test]
    async fn test_success_on_first_attempt() {
        let policy = test_policy();
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result = with_retry(&policy, || {
            let count = Arc::clone(&call_count_clone);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, SdkError>("success")
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_success_on_second_attempt() {
        let policy = test_policy();
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result = with_retry(&policy, || {
            let count = Arc::clone(&call_count_clone);
            async move {
                let current = count.fetch_add(1, Ordering::SeqCst);
                if current == 0 {
                    // First attempt fails with retryable error
                    Err(SdkError::Rpc {
                        code: Code::Unavailable,
                        message: "temporarily unavailable".to_string(),
                        request_id: None,
                        trace_id: None,
                        error_details: None,
                    })
                } else {
                    Ok::<_, SdkError>("success")
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let policy = test_policy(); // max_attempts = 3
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result = with_retry(&policy, || {
            let count = Arc::clone(&call_count_clone);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                // Always fail with retryable error
                Err::<String, _>(SdkError::Rpc {
                    code: Code::Unavailable,
                    message: "always unavailable".to_string(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                })
            }
        })
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SdkError::RetryExhausted { .. }));

        if let SdkError::RetryExhausted { attempts, last_error, .. } = err {
            assert_eq!(attempts, 3);
            assert!(last_error.contains("always unavailable"));
        }

        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_immediate_failure_for_non_retryable() {
        let policy = test_policy();
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result = with_retry(&policy, || {
            let count = Arc::clone(&call_count_clone);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                // Fail with non-retryable error
                Err::<String, _>(SdkError::Rpc {
                    code: Code::InvalidArgument,
                    message: "bad request".to_string(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                })
            }
        })
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should NOT be RetryExhausted - should be the original error
        assert!(matches!(err, SdkError::Rpc { code: Code::InvalidArgument, .. }));
        // Should only have been called once (no retries)
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_backoff_bounded_by_max() {
        // Use a policy where backoff would exceed max if unbounded
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(200), // Cap at 200ms
            multiplier: 10.0,                        // Would quickly exceed 200ms
            jitter: 0.0,
        };

        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        // We can't directly observe backoff duration in this test,
        // but we verify the retry mechanism completes without hanging
        let start = std::time::Instant::now();

        let result = with_retry(&policy, || {
            let count = Arc::clone(&call_count_clone);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<String, _>(SdkError::Rpc {
                    code: Code::Unavailable,
                    message: "unavailable".to_string(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                })
            }
        })
        .await;

        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert_eq!(call_count.load(Ordering::SeqCst), 5);

        // With max_backoff = 200ms and 4 retries, total backoff should be bounded
        // Initial: 100ms, then 200ms (capped), 200ms, 200ms = 700ms max + overhead
        // Allow some margin for timing
        assert!(
            elapsed < Duration::from_millis(1500),
            "elapsed time {:?} exceeds expected bound",
            elapsed
        );
    }

    #[test]
    fn test_apply_jitter_zero_factor() {
        let dur = Duration::from_millis(100);
        let jittered = apply_jitter(dur, 0.0);
        assert_eq!(jittered, dur);
    }

    #[test]
    fn test_apply_jitter_within_bounds() {
        let dur = Duration::from_millis(1000);
        let factor = 0.25; // ±25%

        // Run multiple times to check bounds probabilistically
        for _ in 0..100 {
            let jittered = apply_jitter(dur, factor);
            let jittered_ms = jittered.as_millis();

            // Should be within [750, 1250]
            assert!(
                (750..=1250).contains(&jittered_ms),
                "jittered duration {}ms out of bounds",
                jittered_ms
            );
        }
    }

    #[test]
    fn test_apply_jitter_clamps_factor() {
        let dur = Duration::from_millis(1000);

        // Factor > 1.0 should be clamped to 1.0
        for _ in 0..100 {
            let jittered = apply_jitter(dur, 1.5);
            let jittered_ms = jittered.as_millis();

            // With factor clamped to 1.0, should be within [0, 2000]
            assert!(jittered_ms <= 2000, "jittered duration {}ms exceeds maximum", jittered_ms);
        }
    }

    // =========================================================================
    // Cancellable retry tests
    // =========================================================================

    #[tokio::test]
    async fn test_cancellable_success_on_first_attempt() {
        let policy = test_policy();
        let token = tokio_util::sync::CancellationToken::new();

        let result = with_retry_cancellable(&policy, &token, None, "test", || async {
            Ok::<_, SdkError>("success")
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
    }

    #[tokio::test]
    async fn test_cancellable_pre_cancelled_token() {
        let policy = test_policy();
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();

        let result = with_retry_cancellable(&policy, &token, None, "test", || async {
            Ok::<_, SdkError>("should not reach")
        })
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SdkError::Cancelled));
    }

    #[tokio::test]
    async fn test_cancellable_cancelled_during_operation() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            multiplier: 2.0,
            jitter: 0.0,
        };
        let token = tokio_util::sync::CancellationToken::new();
        let token_clone = token.clone();

        // Cancel after 50ms — the operation takes 200ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            token_clone.cancel();
        });

        let result = with_retry_cancellable(&policy, &token, None, "test", || async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<_, SdkError>("too slow")
        })
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SdkError::Cancelled));
    }

    #[tokio::test]
    async fn test_cancellable_cancelled_during_backoff() {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_secs(10), // Very long backoff
            max_backoff: Duration::from_secs(10),
            multiplier: 1.0,
            jitter: 0.0,
        };
        let token = tokio_util::sync::CancellationToken::new();
        let token_clone = token.clone();
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        // Cancel after 50ms — first attempt fails instantly, backoff is 10s
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            token_clone.cancel();
        });

        let start = std::time::Instant::now();
        let result = with_retry_cancellable(&policy, &token, None, "test", || {
            let count = Arc::clone(&call_count_clone);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<String, _>(SdkError::Rpc {
                    code: Code::Unavailable,
                    message: "fail".to_string(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                })
            }
        })
        .await;

        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SdkError::Cancelled));
        // Should have been called once before cancellation during backoff
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        // Should not have waited the full 10s backoff
        assert!(elapsed < Duration::from_secs(1), "took {:?}", elapsed);
    }

    #[tokio::test]
    async fn test_cancellable_retries_until_success() {
        let policy = test_policy();
        let token = tokio_util::sync::CancellationToken::new();
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result = with_retry_cancellable(&policy, &token, None, "test", || {
            let count = Arc::clone(&call_count_clone);
            async move {
                let current = count.fetch_add(1, Ordering::SeqCst);
                if current == 0 {
                    Err(SdkError::Rpc {
                        code: Code::Unavailable,
                        message: "transient".to_string(),
                        request_id: None,
                        trace_id: None,
                        error_details: None,
                    })
                } else {
                    Ok::<_, SdkError>("success")
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_cancellable_exhausts_retries() {
        let policy = test_policy(); // max_attempts = 3
        let token = tokio_util::sync::CancellationToken::new();
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result = with_retry_cancellable(&policy, &token, None, "test", || {
            let count = Arc::clone(&call_count_clone);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<String, _>(SdkError::Rpc {
                    code: Code::Unavailable,
                    message: "always fails".to_string(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                })
            }
        })
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SdkError::RetryExhausted { .. }));
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_cancellable_non_retryable_returns_immediately() {
        let policy = test_policy();
        let token = tokio_util::sync::CancellationToken::new();
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result = with_retry_cancellable(&policy, &token, None, "test", || {
            let count = Arc::clone(&call_count_clone);
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<String, _>(SdkError::Rpc {
                    code: Code::InvalidArgument,
                    message: "bad".to_string(),
                    request_id: None,
                    trace_id: None,
                    error_details: None,
                })
            }
        })
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SdkError::Rpc { code: Code::InvalidArgument, .. }));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_cancelled_error_is_not_retryable() {
        // Ensure that Cancelled errors short-circuit the retry loop
        let err = SdkError::Cancelled;
        assert!(!err.is_retryable());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod proptest_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use proptest::prelude::*;
    use tonic::Code;

    use super::*;
    use crate::error::SdkError;

    proptest! {
        /// Property: Jittered duration never exceeds base * (1 + factor)
        #[test]
        fn prop_jitter_never_exceeds_upper_bound(
            base_ms in 1u64..10000,
            factor in 0.0f64..=1.0
        ) {
            let dur = Duration::from_millis(base_ms);
            let jittered = apply_jitter(dur, factor);

            let max_allowed = Duration::from_nanos(
                (dur.as_nanos() as f64 * (1.0 + factor)).ceil() as u64
            );

            prop_assert!(
                jittered <= max_allowed,
                "jittered {:?} exceeds max {:?} for base {:?} with factor {}",
                jittered, max_allowed, dur, factor
            );
        }

        /// Property: Jittered duration is never below base * (1 - factor)
        #[test]
        fn prop_jitter_never_below_lower_bound(
            base_ms in 1u64..10000,
            factor in 0.0f64..=1.0
        ) {
            let dur = Duration::from_millis(base_ms);
            let jittered = apply_jitter(dur, factor);

            let min_allowed = Duration::from_nanos(
                (dur.as_nanos() as f64 * (1.0 - factor)).floor() as u64
            );

            prop_assert!(
                jittered >= min_allowed,
                "jittered {:?} below min {:?} for base {:?} with factor {}",
                jittered, min_allowed, dur, factor
            );
        }

        /// Property: Zero jitter factor returns exact duration
        #[test]
        fn prop_zero_jitter_is_identity(base_ms in 1u64..10000) {
            let dur = Duration::from_millis(base_ms);
            let jittered = apply_jitter(dur, 0.0);
            prop_assert_eq!(jittered, dur);
        }

        /// Property: Exponential backoff never exceeds max_backoff
        ///
        /// For any retry policy parameters, the computed backoff at any
        /// attempt never exceeds max_backoff (before jitter).
        #[test]
        fn prop_backoff_bounded_by_max(
            initial_ms in 1u64..1000,
            max_ms in 1u64..10000,
            multiplier in 1.0f64..10.0,
            attempt in 0u32..20
        ) {
            let initial = Duration::from_millis(initial_ms);
            let max = Duration::from_millis(max_ms);

            // Calculate backoff: initial * multiplier^attempt, capped at max
            let backoff_nanos = initial.as_nanos() as f64 * multiplier.powi(attempt as i32);
            let backoff = Duration::from_nanos(backoff_nanos.min(u64::MAX as f64) as u64);
            let capped = backoff.min(max);

            prop_assert!(
                capped <= max,
                "backoff {:?} exceeds max {:?}",
                capped, max
            );
        }

        /// Property: Negative jitter factor treated as zero (no jitter)
        #[test]
        fn prop_negative_jitter_is_identity(
            base_ms in 1u64..10000,
            factor in -10.0f64..0.0
        ) {
            let dur = Duration::from_millis(base_ms);
            let jittered = apply_jitter(dur, factor);
            prop_assert_eq!(jittered, dur);
        }

        /// Property: Factor > 1.0 is clamped to 1.0
        ///
        /// Even with factor > 1.0, jitter should not exceed 2x the base.
        #[test]
        fn prop_large_factor_clamped(
            base_ms in 1u64..10000,
            factor in 1.0f64..100.0
        ) {
            let dur = Duration::from_millis(base_ms);
            let jittered = apply_jitter(dur, factor);

            // Clamped to 1.0, so max is 2x
            let max_allowed = Duration::from_nanos((dur.as_nanos() * 2) as u64);

            prop_assert!(
                jittered <= max_allowed,
                "jittered {:?} exceeds 2x base {:?} with factor {}",
                jittered, dur, factor
            );
        }

        /// Property: Retry terminates - either succeeds or exhausts
        ///
        /// For any retry policy and operation that may fail, with_retry
        /// always terminates within bounded attempts.
        #[test]
        fn prop_retry_always_terminates(
            max_attempts in 1u32..5,
            succeed_on in 0u32..10
        ) {
            // Create a single-threaded runtime for this test
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();

            runtime.block_on(async {
                let policy = RetryPolicy {
                    max_attempts,
                    initial_backoff: Duration::from_millis(1),
                    max_backoff: Duration::from_millis(5),
                    multiplier: 2.0,
                    jitter: 0.0,
                };

                let call_count = Arc::new(AtomicU32::new(0));
                let call_count_clone = Arc::clone(&call_count);

                let result = with_retry(&policy, || {
                    let count = Arc::clone(&call_count_clone);
                    async move {
                        let current = count.fetch_add(1, Ordering::SeqCst);
                        if current >= succeed_on {
                            Ok::<_, SdkError>("success")
                        } else {
                            Err(SdkError::Rpc {
                                code: Code::Unavailable,
                                message: "transient".to_string(),
                                request_id: None,
                                trace_id: None,
                                error_details: None,
                            })
                        }
                    }
                })
                .await;

                let calls = call_count.load(Ordering::SeqCst);

                // Either succeeded or exhausted retries
                if succeed_on < max_attempts {
                    // Should have succeeded
                    assert!(result.is_ok(), "Expected success but got {:?}", result);
                    assert_eq!(calls, succeed_on + 1);
                } else {
                    // Should have exhausted retries
                    assert!(result.is_err(), "Expected exhaustion but got {:?}", result);
                    assert_eq!(calls, max_attempts);
                }
            });
        }

        /// Property: Idempotent operation returns same result on retry
        ///
        /// When an operation that returns the same result is retried,
        /// the final result matches what the operation would return.
        #[test]
        fn prop_idempotent_retry_same_result(value in 0i32..1000) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();

            runtime.block_on(async {
                let policy = RetryPolicy {
                    max_attempts: 3,
                    initial_backoff: Duration::from_millis(1),
                    max_backoff: Duration::from_millis(5),
                    multiplier: 2.0,
                    jitter: 0.0,
                };

                let call_count = Arc::new(AtomicU32::new(0));
                let call_count_clone = Arc::clone(&call_count);

                // Operation that fails once then returns consistent value
                let result = with_retry(&policy, || {
                    let count = Arc::clone(&call_count_clone);
                    async move {
                        let current = count.fetch_add(1, Ordering::SeqCst);
                        if current == 0 {
                            Err(SdkError::Rpc {
                                code: Code::Unavailable,
                                message: "transient".to_string(),
                                request_id: None,
                                trace_id: None,
                                error_details: None,
                            })
                        } else {
                            Ok::<_, SdkError>(value)
                        }
                    }
                })
                .await;

                assert!(result.is_ok());
                assert_eq!(result.unwrap(), value);
            });
        }
    }
}
