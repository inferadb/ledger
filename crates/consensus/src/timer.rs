//! Timer wheel for managing per-shard election and heartbeat deadlines.
//!
//! Backed by a `BTreeMap` keyed by `(Instant, ShardId, TimerKind)` for
//! efficient expiration polling and next-deadline queries.

use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

use crate::types::{ShardId, TimerKind};

/// A timer wheel that tracks per-shard election and heartbeat deadlines.
///
/// Timers are stored in a `BTreeMap` ordered by deadline, enabling O(log n)
/// insertion, cancellation, and O(1) next-deadline queries. A reverse index
/// maps `(ShardId, TimerKind)` to the current deadline for O(log n)
/// replacement of existing timers.
#[derive(Debug)]
pub struct TimerWheel {
    /// Forward index: ordered by (deadline, shard, kind).
    timers: BTreeMap<(Instant, ShardId, TimerKind), ()>,
    /// Reverse index: (shard, kind) → current deadline for fast lookup on replace/cancel.
    index: HashMap<(ShardId, TimerKind), Instant>,
}

impl TimerWheel {
    /// Creates an empty timer wheel.
    pub fn new() -> Self {
        Self { timers: BTreeMap::new(), index: HashMap::new() }
    }

    /// Schedules a timer for the given shard and kind at the specified deadline.
    ///
    /// If a timer already exists for the same `(shard, kind)`, it is replaced
    /// with the new deadline.
    pub fn schedule(&mut self, shard: ShardId, kind: TimerKind, deadline: Instant) {
        // Remove any existing timer for this (shard, kind).
        if let Some(old_deadline) = self.index.remove(&(shard, kind)) {
            self.timers.remove(&(old_deadline, shard, kind));
        }

        self.timers.insert((deadline, shard, kind), ());
        self.index.insert((shard, kind), deadline);
    }

    /// Cancels a specific timer for the given shard and kind.
    ///
    /// Returns `true` if a timer was removed, `false` if none existed.
    pub fn cancel(&mut self, shard: ShardId, kind: TimerKind) -> bool {
        if let Some(deadline) = self.index.remove(&(shard, kind)) {
            self.timers.remove(&(deadline, shard, kind));
            true
        } else {
            false
        }
    }

    /// Cancels all timers for the given shard (both election and heartbeat).
    pub fn cancel_all(&mut self, shard: ShardId) {
        for kind in [TimerKind::Election, TimerKind::Heartbeat] {
            self.cancel(shard, kind);
        }
    }

    /// Returns the next expired timer whose deadline is at or before `now`.
    ///
    /// Removes the timer from the wheel before returning it. Call repeatedly
    /// until `None` to drain all expired timers.
    pub fn poll_expired(&mut self, now: Instant) -> Option<(ShardId, TimerKind, Instant)> {
        // Peek at the earliest entry.
        let (&(deadline, shard, kind), _) = self.timers.first_key_value()?;

        if deadline > now {
            return None;
        }

        self.timers.pop_first();
        self.index.remove(&(shard, kind));

        Some((shard, kind, deadline))
    }

    /// Returns the earliest deadline in the wheel, or `None` if empty.
    ///
    /// Useful for computing the next `tokio::time::sleep_until` target.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.timers.first_key_value().map(|(&(deadline, ..), _)| deadline)
    }

    /// Returns the number of active timers.
    pub fn len(&self) -> usize {
        self.timers.len()
    }

    /// Returns `true` if there are no active timers.
    pub fn is_empty(&self) -> bool {
        self.timers.is_empty()
    }
}

impl Default for TimerWheel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn schedule_and_poll() {
        let mut wheel = TimerWheel::new();
        let base = Instant::now();
        let early = base + Duration::from_millis(100);
        let late = base + Duration::from_millis(200);

        wheel.schedule(ShardId(1), TimerKind::Election, late);
        wheel.schedule(ShardId(2), TimerKind::Heartbeat, early);

        // Poll at a time after both deadlines.
        let now = base + Duration::from_millis(300);

        let first = wheel.poll_expired(now);
        assert_eq!(first, Some((ShardId(2), TimerKind::Heartbeat, early)));

        let second = wheel.poll_expired(now);
        assert_eq!(second, Some((ShardId(1), TimerKind::Election, late)));

        assert_eq!(wheel.poll_expired(now), None);
        assert!(wheel.is_empty());
    }

    #[test]
    fn cancel_removes_timer() {
        let mut wheel = TimerWheel::new();
        let deadline = Instant::now() + Duration::from_millis(100);

        wheel.schedule(ShardId(1), TimerKind::Election, deadline);
        assert_eq!(wheel.len(), 1);

        assert!(wheel.cancel(ShardId(1), TimerKind::Election));
        assert!(wheel.is_empty());

        // Cancel again returns false.
        assert!(!wheel.cancel(ShardId(1), TimerKind::Election));
    }

    #[test]
    fn schedule_replaces_existing() {
        let mut wheel = TimerWheel::new();
        let base = Instant::now();
        let first_deadline = base + Duration::from_millis(100);
        let second_deadline = base + Duration::from_millis(200);

        wheel.schedule(ShardId(1), TimerKind::Election, first_deadline);
        assert_eq!(wheel.len(), 1);

        // Replace with a later deadline.
        wheel.schedule(ShardId(1), TimerKind::Election, second_deadline);
        assert_eq!(wheel.len(), 1);

        // The old deadline should not fire.
        let mid = base + Duration::from_millis(150);
        assert_eq!(wheel.poll_expired(mid), None);

        // The new deadline should fire.
        let after = base + Duration::from_millis(250);
        let result = wheel.poll_expired(after);
        assert_eq!(result, Some((ShardId(1), TimerKind::Election, second_deadline)));
    }

    #[test]
    fn cancel_all_for_shard() {
        let mut wheel = TimerWheel::new();
        let base = Instant::now();

        // Shard 1 gets both timer kinds.
        wheel.schedule(ShardId(1), TimerKind::Election, base + Duration::from_millis(100));
        wheel.schedule(ShardId(1), TimerKind::Heartbeat, base + Duration::from_millis(150));

        // Shard 2 gets an election timer.
        wheel.schedule(ShardId(2), TimerKind::Election, base + Duration::from_millis(200));

        assert_eq!(wheel.len(), 3);

        wheel.cancel_all(ShardId(1));

        assert_eq!(wheel.len(), 1);

        // Only shard 2's timer remains.
        let now = base + Duration::from_millis(300);
        let result = wheel.poll_expired(now);
        assert_eq!(
            result,
            Some((ShardId(2), TimerKind::Election, base + Duration::from_millis(200)))
        );
        assert!(wheel.is_empty());
    }

    #[test]
    fn poll_returns_none_when_not_expired() {
        let mut wheel = TimerWheel::new();
        let base = Instant::now();
        let future = base + Duration::from_secs(60);

        wheel.schedule(ShardId(1), TimerKind::Election, future);

        // Poll at a time before the deadline.
        assert_eq!(wheel.poll_expired(base), None);
        assert_eq!(wheel.len(), 1);
    }

    #[test]
    fn next_deadline_returns_earliest() {
        let mut wheel = TimerWheel::new();
        assert_eq!(wheel.next_deadline(), None);

        let base = Instant::now();
        let early = base + Duration::from_millis(50);
        let mid = base + Duration::from_millis(100);
        let late = base + Duration::from_millis(200);

        wheel.schedule(ShardId(3), TimerKind::Heartbeat, late);
        wheel.schedule(ShardId(1), TimerKind::Election, mid);
        wheel.schedule(ShardId(2), TimerKind::Election, early);

        assert_eq!(wheel.next_deadline(), Some(early));

        // After cancelling the earliest, next_deadline advances.
        wheel.cancel(ShardId(2), TimerKind::Election);
        assert_eq!(wheel.next_deadline(), Some(mid));
    }

    #[test]
    fn poll_at_exact_deadline_fires() {
        let mut wheel = TimerWheel::new();
        let deadline = Instant::now() + Duration::from_millis(100);
        wheel.schedule(ShardId(1), TimerKind::Election, deadline);

        // Polling at exactly the deadline should fire (<=).
        let result = wheel.poll_expired(deadline);
        assert_eq!(result, Some((ShardId(1), TimerKind::Election, deadline)));
        assert!(wheel.is_empty());
    }

    #[test]
    fn same_shard_different_kinds_are_independent() {
        let mut wheel = TimerWheel::new();
        let base = Instant::now();

        wheel.schedule(ShardId(1), TimerKind::Election, base + Duration::from_millis(100));
        wheel.schedule(ShardId(1), TimerKind::Heartbeat, base + Duration::from_millis(200));
        assert_eq!(wheel.len(), 2);

        // Cancel only election; heartbeat remains.
        wheel.cancel(ShardId(1), TimerKind::Election);
        assert_eq!(wheel.len(), 1);
        assert_eq!(
            wheel.poll_expired(base + Duration::from_millis(300)),
            Some((ShardId(1), TimerKind::Heartbeat, base + Duration::from_millis(200)))
        );
    }
}
