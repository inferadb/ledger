//! Health snapshots for `WireClient`.
//!
//! [`ClientStatus`] is a Phase 14 hook — operators query it for diagnostics,
//! the SDK can plumb individual fields into Prometheus metrics in a later
//! phase. The wire-transport crate itself emits no metrics; this module is
//! pure observability.
//!
//! Internally, `ClientHealth` tracks the mutable counters and timestamps
//! behind a mix of [`parking_lot::Mutex`] (for `Option<Instant>` fields —
//! held for nanoseconds, no contention with status readers) and
//! [`AtomicU64`] (for monotonic counters). The state is wrapped in `Arc` so
//! the connect path can update it without contending with `status` readers.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use parking_lot::Mutex;

/// Snapshot of a [`crate::client::WireClient`]'s current health.
///
/// Returned by [`crate::client::WireClient::status`] for diagnostics —
/// operators surface this through admin tooling, and the SDK can plumb
/// individual fields into Prometheus metrics in a later phase. The snapshot
/// is consistent at the instant of the call but may be stale by the time
/// the caller observes it (e.g., a reconnect can land between status return
/// and operator read); callers needing latest state should call `status`
/// again rather than caching.
#[derive(Debug, Clone)]
pub struct ClientStatus {
    /// Current connection state.
    pub state: ConnectionStateKind,
    /// When the [`crate::client::WireClient`] was constructed.
    pub created_at: Instant,
    /// When the most recent successful connection completed handshake.
    /// `None` if no connection has succeeded yet.
    pub last_connected_at: Option<Instant>,
    /// When the most recent connect attempt began (successful or not).
    /// `None` if no connect has been attempted.
    pub last_reconnect_at: Option<Instant>,
    /// Cumulative count of completed connect attempts (success or failure).
    /// Includes the initial connect.
    pub connect_attempts: u64,
    /// Count of consecutive connect failures since the last successful
    /// connect. Resets to zero on success.
    pub consecutive_failures: u64,
}

/// Lighter-weight projection of [`crate::client::ConnectionState`] suitable
/// for embedding in [`ClientStatus`].
///
/// Does not carry the `Arc<Connection>` payload so callers can compare
/// states cheaply (e.g., for change detection in observability pipelines).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionStateKind {
    /// No live connection; next `acquire` triggers a connect.
    Disconnected,
    /// One task is currently handshaking.
    Connecting,
    /// Connection is live.
    Connected,
    /// Permanent close from [`crate::client::WireClient::close`].
    Closed,
}

/// Mutable health counters shared between the connect path and `status`
/// readers.
///
/// Held as `Arc<ClientHealth>` on [`crate::client::WireClient`] so the
/// async-locked connect path can update it without contending with sync
/// status readers.
#[derive(Debug)]
pub(crate) struct ClientHealth {
    /// When [`crate::client::WireClient::new`] returned.
    pub created_at: Instant,
    /// When the most recent successful connection completed handshake.
    pub last_connected_at: Mutex<Option<Instant>>,
    /// When the most recent connect attempt began.
    pub last_reconnect_at: Mutex<Option<Instant>>,
    /// Cumulative count of connect attempts (success or failure).
    pub connect_attempts: AtomicU64,
    /// Consecutive failures since the last success.
    pub consecutive_failures: AtomicU64,
}

impl ClientHealth {
    /// Construct a fresh health record with `created_at` set to "now".
    pub(crate) fn new() -> Self {
        Self {
            created_at: Instant::now(),
            last_connected_at: Mutex::new(None),
            last_reconnect_at: Mutex::new(None),
            connect_attempts: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
        }
    }

    /// Record the start of a connect attempt — bumps the cumulative
    /// counter and stamps `last_reconnect_at`.
    ///
    /// Called at the entry to `do_connect` so every attempt (initial,
    /// retry, post-disconnect re-dial) is captured uniformly.
    pub(crate) fn record_attempt_start(&self) {
        self.connect_attempts.fetch_add(1, Ordering::Relaxed);
        *self.last_reconnect_at.lock() = Some(Instant::now());
    }

    /// Record a successful connect — stamps `last_connected_at` and
    /// resets the consecutive-failure counter.
    pub(crate) fn record_success(&self) {
        *self.last_connected_at.lock() = Some(Instant::now());
        self.consecutive_failures.store(0, Ordering::Relaxed);
    }

    /// Record a failed connect — bumps the consecutive-failure counter.
    pub(crate) fn record_failure(&self) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
    }
}
