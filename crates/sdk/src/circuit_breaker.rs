//! Per-endpoint circuit breaker for fast-failing requests to unhealthy servers.
//!
//! Implements the three-state circuit breaker pattern:
//!
//! ```text
//! ┌────────┐  N failures   ┌──────┐  reset_timeout  ┌───────────┐
//! │ Closed ├───────────────►│ Open ├────────────────►│ Half-Open │
//! └────┬───┘               └──────┘                 └─────┬─────┘
//!      │                       ▲                          │
//!      │                       │  failure                 │ success
//!      │                       └──────────────────────────┤
//!      │                                                  │
//!      │◄─────────────────────────────────────────────────┘
//!      │        M successes in half-open
//! ```
//!
//! Each endpoint has independent circuit state. When a circuit opens, requests
//! to that endpoint immediately fail with [`SdkError::CircuitOpen`] without
//! network I/O, preventing cascading failures.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::RwLock;

use crate::error::SdkError;

/// Configuration for the per-endpoint circuit breaker.
///
/// # Defaults
///
/// - `failure_threshold`: 5 consecutive failures to open the circuit
/// - `success_threshold`: 2 consecutive successes in half-open to close
/// - `reset_timeout`: 30 seconds before transitioning from open to half-open
///
/// # Example
///
/// ```no_run
/// # use inferadb_ledger_sdk::CircuitBreakerConfig;
/// use std::time::Duration;
///
/// let config = CircuitBreakerConfig::builder()
///     .failure_threshold(10)
///     .reset_timeout(Duration::from_secs(60))
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before the circuit opens.
    failure_threshold: u32,

    /// Number of consecutive successes in half-open state before the circuit closes.
    success_threshold: u32,

    /// Duration after which an open circuit transitions to half-open.
    reset_timeout: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self { failure_threshold: 5, success_threshold: 2, reset_timeout: Duration::from_secs(30) }
    }
}

#[bon::bon]
impl CircuitBreakerConfig {
    /// Creates a new circuit breaker configuration.
    #[builder]
    #[must_use]
    pub fn new(
        #[builder(default = 5)] failure_threshold: u32,
        #[builder(default = 2)] success_threshold: u32,
        #[builder(default = Duration::from_secs(30))] reset_timeout: Duration,
    ) -> Self {
        Self { failure_threshold, success_threshold, reset_timeout }
    }

    /// Returns the number of consecutive failures to open the circuit.
    #[must_use]
    pub fn failure_threshold(&self) -> u32 {
        self.failure_threshold
    }

    /// Returns the number of consecutive successes to close a half-open circuit.
    #[must_use]
    pub fn success_threshold(&self) -> u32 {
        self.success_threshold
    }

    /// Returns the duration before an open circuit transitions to half-open.
    #[must_use]
    pub fn reset_timeout(&self) -> Duration {
        self.reset_timeout
    }
}

/// Circuit breaker states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation — requests flow through.
    Closed,

    /// Circuit tripped — requests are immediately rejected.
    Open,

    /// Probing — a limited number of requests are allowed through to test recovery.
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "closed"),
            Self::Open => write!(f, "open"),
            Self::HalfOpen => write!(f, "half-open"),
        }
    }
}

/// Internal state for a single endpoint's circuit breaker.
#[derive(Debug)]
struct EndpointCircuit {
    /// Current circuit state.
    state: CircuitState,

    /// Consecutive failure count (reset on success).
    consecutive_failures: u32,

    /// Consecutive success count in half-open state (reset on failure or state change).
    consecutive_successes: u32,

    /// When the circuit was last opened (used for reset timeout).
    last_opened: Option<Instant>,
}

impl EndpointCircuit {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            consecutive_successes: 0,
            last_opened: None,
        }
    }
}

/// Per-endpoint circuit breaker.
///
/// Tracks failure/success counts per endpoint and transitions between
/// Closed, Open, and Half-Open states. Thread-safe for concurrent access
/// from multiple tasks.
///
/// # Integration
///
/// The circuit breaker integrates with [`ServerSelector`](crate::ServerSelector)
/// by marking endpoints unhealthy when the circuit opens and healthy when
/// the circuit closes.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    /// Per-endpoint circuit state.
    circuits: Arc<RwLock<HashMap<String, EndpointCircuit>>>,

    /// Configuration.
    config: CircuitBreakerConfig,
}

impl CircuitBreaker {
    /// Creates a new circuit breaker with the given configuration.
    #[must_use]
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self { circuits: Arc::new(RwLock::new(HashMap::new())), config }
    }

    /// Checks whether a request to the given endpoint should be allowed.
    ///
    /// Returns `Ok(())` if the request can proceed, or
    /// `Err(SdkError::CircuitOpen)` if the circuit is open and the reset
    /// timeout has not elapsed.
    ///
    /// If the circuit is open and the reset timeout has elapsed, the circuit
    /// transitions to half-open and the request is allowed through as a probe.
    pub fn check(&self, endpoint: &str) -> Result<(), SdkError> {
        let mut circuits = self.circuits.write();
        let circuit = circuits.entry(endpoint.to_string()).or_insert_with(EndpointCircuit::new);

        match circuit.state {
            CircuitState::Closed | CircuitState::HalfOpen => Ok(()),
            CircuitState::Open => {
                // Check if reset timeout has elapsed
                if let Some(last_opened) = circuit.last_opened
                    && last_opened.elapsed() >= self.config.reset_timeout
                {
                    // Transition to half-open
                    let previous = circuit.state;
                    circuit.state = CircuitState::HalfOpen;
                    circuit.consecutive_successes = 0;

                    tracing::warn!(
                        endpoint = endpoint,
                        from = %previous,
                        to = %circuit.state,
                        reset_timeout_ms = self.config.reset_timeout.as_millis() as u64,
                        "circuit breaker state transition: allowing probe request"
                    );

                    return Ok(());
                }

                // Circuit is open and reset timeout has not elapsed
                let retry_after = circuit
                    .last_opened
                    .map(|t| self.config.reset_timeout.saturating_sub(t.elapsed()))
                    .unwrap_or(self.config.reset_timeout);

                Err(SdkError::CircuitOpen { endpoint: endpoint.to_string(), retry_after })
            },
        }
    }

    /// Records a successful request to the given endpoint.
    ///
    /// In closed state, resets the failure counter.
    /// In half-open state, increments the success counter and closes the
    /// circuit once the success threshold is reached.
    pub fn record_success(&self, endpoint: &str) {
        let mut circuits = self.circuits.write();
        let circuit = circuits.entry(endpoint.to_string()).or_insert_with(EndpointCircuit::new);

        match circuit.state {
            CircuitState::Closed => {
                circuit.consecutive_failures = 0;
            },
            CircuitState::HalfOpen => {
                circuit.consecutive_successes += 1;

                if circuit.consecutive_successes >= self.config.success_threshold {
                    let previous = circuit.state;
                    circuit.state = CircuitState::Closed;
                    circuit.consecutive_failures = 0;
                    circuit.consecutive_successes = 0;
                    circuit.last_opened = None;

                    tracing::warn!(
                        endpoint = endpoint,
                        from = %previous,
                        to = %circuit.state,
                        success_threshold = self.config.success_threshold,
                        "circuit breaker state transition: recovery confirmed"
                    );
                }
            },
            CircuitState::Open => {
                // Shouldn't happen (requests blocked in open state), but reset just in case
                circuit.consecutive_failures = 0;
            },
        }
    }

    /// Records a failed request to the given endpoint.
    ///
    /// In closed state, increments the failure counter and opens the circuit
    /// once the failure threshold is reached.
    /// In half-open state, immediately reopens the circuit.
    pub fn record_failure(&self, endpoint: &str) {
        let mut circuits = self.circuits.write();
        let circuit = circuits.entry(endpoint.to_string()).or_insert_with(EndpointCircuit::new);

        match circuit.state {
            CircuitState::Closed => {
                circuit.consecutive_failures += 1;

                if circuit.consecutive_failures >= self.config.failure_threshold {
                    let previous = circuit.state;
                    circuit.state = CircuitState::Open;
                    circuit.last_opened = Some(Instant::now());
                    circuit.consecutive_successes = 0;

                    tracing::warn!(
                        endpoint = endpoint,
                        from = %previous,
                        to = %circuit.state,
                        failure_threshold = self.config.failure_threshold,
                        consecutive_failures = circuit.consecutive_failures,
                        "circuit breaker state transition: endpoint marked unavailable"
                    );
                }
            },
            CircuitState::HalfOpen => {
                // Probe failed — reopen the circuit
                let previous = circuit.state;
                circuit.state = CircuitState::Open;
                circuit.last_opened = Some(Instant::now());
                circuit.consecutive_successes = 0;

                tracing::warn!(
                    endpoint = endpoint,
                    from = %previous,
                    to = %circuit.state,
                    "circuit breaker state transition: probe failed, reopening"
                );
            },
            CircuitState::Open => {
                // Already open, refresh the open timestamp
                circuit.last_opened = Some(Instant::now());
            },
        }
    }

    /// Returns the current state of the circuit for the given endpoint.
    ///
    /// Returns `Closed` for endpoints without any tracked state.
    #[must_use]
    pub fn state(&self, endpoint: &str) -> CircuitState {
        let mut circuits = self.circuits.write();
        let circuit = circuits.entry(endpoint.to_string()).or_insert_with(EndpointCircuit::new);

        // Check for implicit transition from Open to HalfOpen
        if circuit.state == CircuitState::Open
            && let Some(last_opened) = circuit.last_opened
            && last_opened.elapsed() >= self.config.reset_timeout
        {
            circuit.state = CircuitState::HalfOpen;
            circuit.consecutive_successes = 0;
        }

        circuit.state
    }

    /// Returns the circuit breaker configuration.
    #[must_use]
    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }

    /// Removes all tracked circuit state.
    pub fn clear(&self) {
        self.circuits.write().clear();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use super::*;

    fn test_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig::builder()
            .failure_threshold(3)
            .success_threshold(2)
            .reset_timeout(Duration::from_millis(100))
            .build()
    }

    #[test]
    fn test_default_config() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold(), 5);
        assert_eq!(config.success_threshold(), 2);
        assert_eq!(config.reset_timeout(), Duration::from_secs(30));
    }

    #[test]
    fn test_custom_config() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(10)
            .success_threshold(3)
            .reset_timeout(Duration::from_secs(60))
            .build();
        assert_eq!(config.failure_threshold(), 10);
        assert_eq!(config.success_threshold(), 3);
        assert_eq!(config.reset_timeout(), Duration::from_secs(60));
    }

    #[test]
    fn test_circuit_starts_closed() {
        let cb = CircuitBreaker::new(test_config());
        assert_eq!(cb.state("endpoint-a"), CircuitState::Closed);
    }

    #[test]
    fn test_check_allows_closed_circuit() {
        let cb = CircuitBreaker::new(test_config());
        assert!(cb.check("endpoint-a").is_ok());
    }

    #[test]
    fn test_opens_after_failure_threshold() {
        let cb = CircuitBreaker::new(test_config());
        let ep = "endpoint-a";

        // Failures below threshold keep circuit closed
        cb.record_failure(ep);
        cb.record_failure(ep);
        assert_eq!(cb.state(ep), CircuitState::Closed);
        assert!(cb.check(ep).is_ok());

        // Third failure trips the circuit
        cb.record_failure(ep);
        assert_eq!(cb.state(ep), CircuitState::Open);
    }

    #[test]
    fn test_open_circuit_rejects_requests() {
        let cb = CircuitBreaker::new(test_config());
        let ep = "endpoint-a";

        // Trip the circuit
        for _ in 0..3 {
            cb.record_failure(ep);
        }

        let result = cb.check(ep);
        assert!(result.is_err());

        if let Err(SdkError::CircuitOpen { endpoint, retry_after }) = result {
            assert_eq!(endpoint, ep);
            assert!(retry_after.as_millis() > 0);
        } else {
            panic!("expected CircuitOpen error");
        }
    }

    #[test]
    fn test_transitions_to_half_open_after_timeout() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(2)
            .success_threshold(1)
            .reset_timeout(Duration::from_millis(10))
            .build();
        let cb = CircuitBreaker::new(config);
        let ep = "endpoint-a";

        // Trip the circuit
        cb.record_failure(ep);
        cb.record_failure(ep);
        assert_eq!(cb.state(ep), CircuitState::Open);

        // Wait for reset timeout
        std::thread::sleep(Duration::from_millis(15));

        // Should transition to half-open
        assert_eq!(cb.state(ep), CircuitState::HalfOpen);
    }

    #[test]
    fn test_half_open_allows_probe_request() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(2)
            .success_threshold(1)
            .reset_timeout(Duration::from_millis(10))
            .build();
        let cb = CircuitBreaker::new(config);
        let ep = "endpoint-a";

        // Trip and wait for half-open
        cb.record_failure(ep);
        cb.record_failure(ep);
        std::thread::sleep(Duration::from_millis(15));

        // Probe should be allowed
        assert!(cb.check(ep).is_ok());
    }

    #[test]
    fn test_half_open_closes_after_success_threshold() {
        let cb = CircuitBreaker::new(test_config());
        let ep = "endpoint-a";

        // Trip the circuit
        for _ in 0..3 {
            cb.record_failure(ep);
        }

        // Wait for half-open
        std::thread::sleep(Duration::from_millis(110));
        assert_eq!(cb.state(ep), CircuitState::HalfOpen);

        // First success
        cb.record_success(ep);
        assert_eq!(cb.state(ep), CircuitState::HalfOpen);

        // Second success closes the circuit
        cb.record_success(ep);
        assert_eq!(cb.state(ep), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_failure_reopens_circuit() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(2)
            .success_threshold(2)
            .reset_timeout(Duration::from_millis(10))
            .build();
        let cb = CircuitBreaker::new(config);
        let ep = "endpoint-a";

        // Trip and wait for half-open
        cb.record_failure(ep);
        cb.record_failure(ep);
        std::thread::sleep(Duration::from_millis(15));
        assert_eq!(cb.state(ep), CircuitState::HalfOpen);

        // Probe failure reopens
        cb.record_failure(ep);
        assert_eq!(cb.state(ep), CircuitState::Open);
    }

    #[test]
    fn test_success_resets_failure_counter() {
        let cb = CircuitBreaker::new(test_config());
        let ep = "endpoint-a";

        // Two failures, then a success
        cb.record_failure(ep);
        cb.record_failure(ep);
        cb.record_success(ep);

        // Another two failures should not open (counter was reset)
        cb.record_failure(ep);
        cb.record_failure(ep);
        assert_eq!(cb.state(ep), CircuitState::Closed);

        // Third failure (from zero) opens it
        cb.record_failure(ep);
        assert_eq!(cb.state(ep), CircuitState::Open);
    }

    #[test]
    fn test_independent_per_endpoint_state() {
        let cb = CircuitBreaker::new(test_config());

        // Trip endpoint-a
        for _ in 0..3 {
            cb.record_failure("endpoint-a");
        }

        // endpoint-a is open, endpoint-b is still closed
        assert_eq!(cb.state("endpoint-a"), CircuitState::Open);
        assert_eq!(cb.state("endpoint-b"), CircuitState::Closed);
        assert!(cb.check("endpoint-b").is_ok());
    }

    #[test]
    fn test_clear_resets_all_state() {
        let cb = CircuitBreaker::new(test_config());

        for _ in 0..3 {
            cb.record_failure("endpoint-a");
        }
        assert_eq!(cb.state("endpoint-a"), CircuitState::Open);

        cb.clear();
        assert_eq!(cb.state("endpoint-a"), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_open_error_is_not_retryable() {
        let err = SdkError::CircuitOpen {
            endpoint: "test".to_string(),
            retry_after: Duration::from_secs(30),
        };
        // CircuitOpen should not be retryable — the retry loop should
        // not hammer an endpoint with an open circuit.
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_circuit_state_display() {
        assert_eq!(format!("{}", CircuitState::Closed), "closed");
        assert_eq!(format!("{}", CircuitState::Open), "open");
        assert_eq!(format!("{}", CircuitState::HalfOpen), "half-open");
    }

    #[test]
    fn test_circuit_breaker_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CircuitBreaker>();
    }

    #[test]
    fn test_check_transitions_open_to_half_open() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(2)
            .success_threshold(1)
            .reset_timeout(Duration::from_millis(10))
            .build();
        let cb = CircuitBreaker::new(config);
        let ep = "endpoint-a";

        // Trip the circuit
        cb.record_failure(ep);
        cb.record_failure(ep);
        assert!(cb.check(ep).is_err());

        // Wait for reset timeout
        std::thread::sleep(Duration::from_millis(15));

        // check() should transition to half-open and allow the request
        assert!(cb.check(ep).is_ok());
        assert_eq!(cb.state(ep), CircuitState::HalfOpen);
    }

    #[test]
    fn test_retry_after_decreases_over_time() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(2)
            .success_threshold(1)
            .reset_timeout(Duration::from_millis(200))
            .build();
        let cb = CircuitBreaker::new(config);
        let ep = "endpoint-a";

        // Trip the circuit
        cb.record_failure(ep);
        cb.record_failure(ep);

        // Get initial retry_after
        let err1 = cb.check(ep).unwrap_err();
        let retry_after_1 = match err1 {
            SdkError::CircuitOpen { retry_after, .. } => retry_after,
            _ => panic!("expected CircuitOpen"),
        };

        // Wait a bit
        std::thread::sleep(Duration::from_millis(50));

        // retry_after should be smaller
        let err2 = cb.check(ep).unwrap_err();
        let retry_after_2 = match err2 {
            SdkError::CircuitOpen { retry_after, .. } => retry_after,
            _ => panic!("expected CircuitOpen"),
        };

        assert!(retry_after_2 < retry_after_1);
    }

    #[test]
    fn test_open_state_failure_refreshes_timestamp() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(2)
            .success_threshold(1)
            .reset_timeout(Duration::from_millis(50))
            .build();
        let cb = CircuitBreaker::new(config);
        let ep = "endpoint-a";

        // Trip the circuit
        cb.record_failure(ep);
        cb.record_failure(ep);

        // Wait partway through the reset timeout
        std::thread::sleep(Duration::from_millis(30));

        // Record another failure while open — refreshes the timestamp
        cb.record_failure(ep);

        // Wait another 30ms (would have been enough without refresh)
        std::thread::sleep(Duration::from_millis(30));

        // Still open because the timestamp was refreshed
        assert_eq!(cb.state(ep), CircuitState::Open);
    }
}
