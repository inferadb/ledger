//! Per-shard circuit breaker for commit liveness detection.
//!
//! Tracks whether a shard is making forward progress by monitoring
//! commit timestamps. If no commit arrives within the configured
//! timeout, the circuit opens to enable fast-fail behavior upstream.

use std::time::{Duration, Instant};

/// Circuit state for a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation — commits are arriving on time.
    Closed,
    /// No commit within the timeout — shard appears stalled.
    Open,
}

/// Monitors commit liveness for a single shard.
///
/// Starts in [`CircuitState::Closed`] with a grace period (no commits
/// recorded yet counts as healthy). Once a commit is recorded, the
/// circuit opens if no subsequent commit arrives within `timeout`.
#[derive(Debug)]
pub struct ShardCircuitBreaker {
    state: CircuitState,
    last_commit: Option<Instant>,
    timeout: Duration,
}

impl ShardCircuitBreaker {
    /// Creates a new circuit breaker starting in the closed state.
    pub fn new(timeout: Duration) -> Self {
        Self { state: CircuitState::Closed, last_commit: None, timeout }
    }

    /// Records a successful commit, closing the circuit if it was open.
    pub fn record_commit(&mut self, now: Instant) {
        self.last_commit = Some(now);
        self.state = CircuitState::Closed;
    }

    /// Checks the circuit state based on the current time.
    ///
    /// Returns [`CircuitState::Open`] if a commit was previously recorded
    /// but the elapsed time since then exceeds the timeout. Returns
    /// [`CircuitState::Closed`] during the initial grace period (before
    /// any commit) or when commits are recent enough.
    pub fn check(&mut self, now: Instant) -> CircuitState {
        match self.last_commit {
            None => {
                // Grace period: no commits yet, stay closed.
                CircuitState::Closed
            },
            Some(last) => {
                if now.duration_since(last) > self.timeout {
                    self.state = CircuitState::Open;
                } else {
                    self.state = CircuitState::Closed;
                }
                self.state
            },
        }
    }

    /// Returns the current circuit state without re-evaluating the timeout.
    pub fn state(&self) -> CircuitState {
        self.state
    }

    /// Manually resets the circuit to closed.
    pub fn reset(&mut self) {
        self.state = CircuitState::Closed;
        self.last_commit = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All valid state transitions for the circuit breaker.
    ///
    /// | Initial | Action            | Expected |
    /// |---------|-------------------|----------|
    /// | Closed  | check before timeout | Closed |
    /// | Closed  | check after timeout  | Open   |
    /// | Open    | record_commit        | Closed |
    /// | Open    | reset                | Closed |
    /// | (none)  | grace period check   | Closed |
    #[test]
    fn state_transitions() {
        struct Case {
            name: &'static str,
            timeout: Duration,
            steps: Vec<Step>,
            expected: CircuitState,
        }

        enum Step {
            RecordCommit(Duration),
            Check(Duration),
            Reset,
        }

        let cases = vec![
            Case {
                name: "grace_period_stays_closed",
                timeout: Duration::from_secs(5),
                steps: vec![Step::Check(Duration::from_secs(10))],
                expected: CircuitState::Closed,
            },
            Case {
                name: "commit_then_check_within_timeout_stays_closed",
                timeout: Duration::from_secs(5),
                steps: vec![
                    Step::RecordCommit(Duration::ZERO),
                    Step::Check(Duration::from_secs(3)),
                ],
                expected: CircuitState::Closed,
            },
            Case {
                name: "commit_then_check_after_timeout_opens",
                timeout: Duration::from_secs(5),
                steps: vec![
                    Step::RecordCommit(Duration::ZERO),
                    Step::Check(Duration::from_secs(6)),
                ],
                expected: CircuitState::Open,
            },
            Case {
                name: "commit_at_exact_boundary_stays_closed",
                timeout: Duration::from_secs(5),
                steps: vec![
                    Step::RecordCommit(Duration::ZERO),
                    Step::Check(Duration::from_secs(5)),
                ],
                expected: CircuitState::Closed,
            },
            Case {
                name: "open_then_commit_recloses",
                timeout: Duration::from_secs(5),
                steps: vec![
                    Step::RecordCommit(Duration::ZERO),
                    Step::Check(Duration::from_secs(6)),
                    Step::RecordCommit(Duration::from_secs(7)),
                    Step::Check(Duration::from_secs(8)),
                ],
                expected: CircuitState::Closed,
            },
            Case {
                name: "open_then_reset_recloses",
                timeout: Duration::from_secs(5),
                steps: vec![
                    Step::RecordCommit(Duration::ZERO),
                    Step::Check(Duration::from_secs(6)),
                    Step::Reset,
                ],
                expected: CircuitState::Closed,
            },
            Case {
                name: "repeated_commits_keep_circuit_closed",
                timeout: Duration::from_secs(5),
                steps: vec![
                    Step::RecordCommit(Duration::ZERO),
                    Step::Check(Duration::from_secs(3)),
                    Step::RecordCommit(Duration::from_secs(3)),
                    Step::Check(Duration::from_secs(6)),
                    Step::RecordCommit(Duration::from_secs(6)),
                    Step::Check(Duration::from_secs(9)),
                ],
                expected: CircuitState::Closed,
            },
            Case {
                name: "reset_clears_last_commit_restoring_grace_period",
                timeout: Duration::from_secs(5),
                steps: vec![
                    Step::RecordCommit(Duration::ZERO),
                    Step::Reset,
                    Step::Check(Duration::from_secs(100)),
                ],
                expected: CircuitState::Closed,
            },
        ];

        let base = Instant::now();
        for case in cases {
            let mut cb = ShardCircuitBreaker::new(case.timeout);
            let mut last_state = cb.state();
            for step in &case.steps {
                match step {
                    Step::RecordCommit(offset) => cb.record_commit(base + *offset),
                    Step::Check(offset) => {
                        last_state = cb.check(base + *offset);
                    },
                    Step::Reset => cb.reset(),
                }
            }
            // For steps ending with Reset or RecordCommit, read state directly.
            let final_state = match case.steps.last() {
                Some(Step::Check(_)) => last_state,
                _ => cb.state(),
            };
            assert_eq!(final_state, case.expected, "case: {}", case.name);
        }
    }
}
