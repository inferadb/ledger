//! Shard wake-up notification trait used by the reactor when a peer message
//! arrives for a paused shard.
//!
//! This is the consensus-crate side of the O6 hibernation API. The trait is
//! defined here (with a no-op default implementation) so the consensus crate
//! does not depend on the raft crate; the raft crate will install a real
//! implementation in a later pass that wakes the per-vault group, brings its
//! storage and apply pipeline back online, and resumes the shard.
//!
//! ## Drop-and-retry semantics
//!
//! When a [`PeerMessage`](crate::reactor::ReactorEvent::PeerMessage) arrives
//! for a shard that has been paused via
//! [`ConsensusEngine::pause_shard`](crate::ConsensusEngine::pause_shard),
//! the reactor invokes
//! [`ShardWakeNotifier::on_peer_message_for_paused_shard`] and then
//! **drops the originating message**. The reactor does not buffer the
//! message and does not redeliver it after the shard resumes.
//!
//! Raft tolerates this naturally: peers retry AppendEntries on every
//! heartbeat tick (~50ms by default), so the next heartbeat after the
//! shard resumes lands on the awoken state and progress continues. The
//! tradeoff is a small first-wake tail-latency for a vastly simpler
//! design — no buffer, no backpressure, no message-redelivery contract.
//!
//! ## Implementor contract
//!
//! [`ShardWakeNotifier::on_peer_message_for_paused_shard`] is called from the
//! reactor's event loop. The reactor's event loop is paused for the duration
//! of the call, so implementors **must** dispatch any wake-up work
//! asynchronously (e.g., spawn a tokio task) and return immediately.
//! Blocking inside this method stalls every other shard managed by the
//! reactor.

use crate::types::ConsensusStateId;

/// Notification trait invoked by the reactor when a peer message arrives for
/// a paused shard.
///
/// See module docs for the drop-and-retry semantics and the implementor
/// contract.
pub trait ShardWakeNotifier: Send + Sync + 'static {
    /// Called by the reactor when a `PeerMessage` arrives for a paused shard.
    ///
    /// The implementor MUST dispatch any wake-up work asynchronously and
    /// return immediately — the reactor's event loop is on hold until this
    /// returns. The reactor will drop the originating message; the peer's
    /// next retry (Raft heartbeat, ~50ms typical) will land on the awoken
    /// shard.
    fn on_peer_message_for_paused_shard(&self, shard_id: ConsensusStateId);
}

/// Default no-op implementation used when no wake notifier is supplied.
///
/// Used by [`ConsensusEngine::start`](crate::ConsensusEngine::start) so
/// existing call sites that do not yet wire up O6 hibernation continue to
/// compile without code changes.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopShardWakeNotifier;

impl ShardWakeNotifier for NoopShardWakeNotifier {
    fn on_peer_message_for_paused_shard(&self, _shard_id: ConsensusStateId) {
        // No-op. The reactor logs and increments a metric at the call site;
        // there is no further wake action to take when no notifier is wired.
    }
}
