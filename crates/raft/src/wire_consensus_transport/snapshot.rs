//! Leader-side snapshot streaming over the wire transport.
//!
//! Implements the consensus crate's
//! [`SnapshotSender`](inferadb_ledger_consensus::snapshot_sender::SnapshotSender)
//! trait on top of the wire transport's snapshot sub-protocol
//! ([`inferadb_ledger_wire_transport::snapshot_stream::SnapshotSender`]).
//! When the reactor processes
//! [`Action::SendSnapshot`](inferadb_ledger_consensus::action::Action::SendSnapshot),
//! the sender resolves the shard ID to its `(region, organization_id,
//! vault_id?)` scope, opens the at-rest encrypted snapshot file via
//! [`SnapshotPersister::open_encrypted`](crate::snapshot_persister::SnapshotPersister::open_encrypted),
//! and ships the encrypted envelope chunk-by-chunk to the follower over a
//! QUIC bidi stream.
//!
//! ## Wire format (verbatim at-rest envelope)
//!
//! The streamer ships the at-rest AES-256-GCM envelope produced by
//! [`SnapshotPersister`](crate::snapshot_persister::SnapshotPersister)
//! verbatim — the wire protocol carries the same bytes that live on disk.
//! On-the-wire encryption is provided by WireGuard (or QUIC's own TLS) at
//! the link layer, so the streaming protocol itself does not encrypt.
//! This avoids divergence between the disk and wire formats and skips a
//! decrypt/re-encrypt cycle on the leader.
//!
//! ## Drop-and-let-Raft-retry on failure
//!
//! Any failure (no scope mapping, follower address unknown, transport
//! error, short read, sub-protocol error) is logged at warn level and
//! recorded as a metric, then discarded. The leader's heartbeat
//! replicator re-emits `Action::SendSnapshot` on the next cycle, so
//! transient failures self-heal without explicit retry state in the
//! reactor.

use std::sync::{Arc, Weak};

use bytes::Bytes;
use inferadb_ledger_consensus::{
    snapshot_sender::SnapshotSender,
    types::{ConsensusStateId, NodeId},
};
use inferadb_ledger_types::Region;
use inferadb_ledger_wire::services::raft::{
    InstallSnapshotHeader, InstallSnapshotOrgScope, InstallSnapshotScope, InstallSnapshotVaultScope,
};
use inferadb_ledger_wire_transport::{WireClient, snapshot_stream::SnapshotStreamError};
use snafu::{Location, ResultExt, Snafu};
use tokio::io::AsyncReadExt;

use crate::{
    metrics, raft_manager::RaftManager, snapshot::SnapshotScope, snapshot_persister::PersistError,
};

/// Default streaming chunk size (256 KiB).
///
/// Sized to amortize QUIC framing without inflating tail latency on a
/// stalled follower. Smaller chunks waste framing overhead; larger chunks
/// hold flow-control credit longer and delay backpressure signals from
/// the receiver. Matches the legacy gRPC streamer's chunk size to keep
/// observability dashboards stable across the wire migration.
pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 256 * 1024;

/// Static LSNP version string included in every header. Mirrors the
/// version stamped into the on-disk snapshot file by `crates/raft/src/snapshot.rs`.
const LSNP_VERSION: &str = "LSNP-v2";

/// Errors produced by the wire-side leader streaming path.
#[derive(Debug, Snafu)]
pub enum WireSnapshotError {
    /// The shard ID did not resolve to a known scope on this node.
    #[snafu(display("no scope mapping for shard {shard_id:?}"))]
    NoScopeMapping {
        /// Shard whose `Action::SendSnapshot` we tried to dispatch.
        shard_id: ConsensusStateId,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Could not resolve the follower's network address.
    #[snafu(display(
        "follower address not found: node_id={follower_id} (peer-address map empty?)"
    ))]
    UnknownFollower {
        /// Follower node id.
        follower_id: u64,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Could not open the encrypted snapshot on disk.
    #[snafu(display("open encrypted snapshot: {source}"))]
    OpenEncrypted {
        /// Underlying persister error.
        source: PersistError,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Failed to acquire a `WireClient` for the follower from the registry.
    #[snafu(display("acquire wire client for {address}: {source}"))]
    AcquireClient {
        /// Address string from the peer-address map.
        address: String,
        /// Underlying registry error.
        source: crate::node_registry::RegistryError,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// I/O error while reading the encrypted snapshot file.
    #[snafu(display("read encrypted snapshot: {source}"))]
    Io {
        /// Underlying I/O error.
        source: std::io::Error,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// The wire snapshot sub-protocol failed (transport / encode / decode /
    /// CRC / byte-count / WireError-from-receiver).
    #[snafu(display("wire snapshot stream: {source}"))]
    Stream {
        /// Underlying sub-protocol error.
        source: SnapshotStreamError,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// `WireClient::snapshot_stream` returned a non-transport error before
    /// the sub-protocol started — typically a header encode failure or the
    /// receiver immediately returning an error frame.
    #[snafu(display("open wire snapshot stream: {reason}"))]
    OpenStream {
        /// Diagnostic about the failure.
        reason: String,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },
}

/// Leader-side snapshot sender bound to a [`RaftManager`], driven by the
/// wire transport's snapshot sub-protocol.
///
/// Sister of `RaftManagerWakeNotifier`, `RaftManagerSnapshotCoordinator`,
/// and `RaftManagerSnapshotInstaller` in `raft_manager.rs`: holds an
/// `Arc<Mutex<Weak<RaftManager>>>` shared with the manager, which
/// bootstrap fills via [`RaftManager::install_self_weak`] immediately
/// after wrapping the manager in `Arc`.
///
/// The reactor's snapshot-sender contract requires
/// [`SnapshotSender::send_snapshot`] to dispatch I/O asynchronously and
/// return immediately; this implementation `tokio::spawn`s the streaming
/// task and returns to the reactor after resolving the manager handle.
pub(crate) struct WireSnapshotSender {
    /// Shared weak handle filled by `RaftManager::install_self_weak`.
    pub(crate) manager: Arc<parking_lot::Mutex<Weak<RaftManager>>>,
    /// Per-chunk byte ceiling for the data-frame loop.
    pub(crate) chunk_size_bytes: usize,
}

impl SnapshotSender for WireSnapshotSender {
    fn send_snapshot(&self, shard_id: ConsensusStateId, follower_id: NodeId, snapshot_index: u64) {
        let manager = match self.manager.lock().upgrade() {
            Some(arc) => arc,
            None => {
                // Two cases (mirroring the wake notifier and snapshot coordinator):
                // 1. RaftManager is being dropped (process shutdown) — the send is obsolete.
                // 2. install_self_weak was never called (test fixture without snapshot streaming).
                tracing::debug!(
                    ?shard_id,
                    follower_id = follower_id.0,
                    snapshot_index,
                    "WireSnapshotSender: weak upgrade returned None; skipping send \
                     (manager dropped or self_weak not installed)",
                );
                return;
            },
        };

        let chunk_size = self.chunk_size_bytes;
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            match dispatch_send(&manager, shard_id, follower_id, snapshot_index, chunk_size).await {
                Ok(bytes) => {
                    metrics::record_snapshot_send(
                        kind_label(&manager, shard_id),
                        true,
                        started.elapsed().as_secs_f64(),
                        bytes,
                    );
                },
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        ?shard_id,
                        follower_id = follower_id.0,
                        snapshot_index,
                        "WireSnapshotSender: send_snapshot failed (will retry on next heartbeat)",
                    );
                    metrics::record_snapshot_send(
                        kind_label(&manager, shard_id),
                        false,
                        started.elapsed().as_secs_f64(),
                        0,
                    );
                },
            }
        });
    }
}

/// Resolves the shard's scope and returns the metrics label without
/// borrowing the manager beyond the call. Returns `"unknown"` if the
/// shard does not resolve — same fallthrough the dispatch path uses, so
/// failure metrics still land on a stable label.
fn kind_label(manager: &RaftManager, shard_id: ConsensusStateId) -> &'static str {
    match manager.resolve_shard_to_scope(shard_id) {
        Some((_, _, Some(_))) => "vault",
        Some((_, _, None)) => "org",
        None => "unknown",
    }
}

fn scope_kind(scope: SnapshotScope) -> &'static str {
    if scope.is_vault() { "vault" } else { "org" }
}

/// Resolves shard → scope → snapshot file → follower address, then runs
/// the wire snapshot sub-protocol. Returns total bytes streamed on success.
async fn dispatch_send(
    manager: &RaftManager,
    shard_id: ConsensusStateId,
    follower_id: NodeId,
    snapshot_index: u64,
    chunk_size_bytes: usize,
) -> Result<u64, WireSnapshotError> {
    let (region, organization_id, vault_id) = manager
        .resolve_shard_to_scope(shard_id)
        .ok_or(WireSnapshotError::NoScopeMapping { shard_id, location: snafu::location!() })?;

    let scope = match vault_id {
        Some(vault_id) => SnapshotScope::Vault { organization_id, vault_id },
        None => SnapshotScope::Org { organization_id },
    };

    let address =
        manager.peer_addresses().get(follower_id.0).ok_or(WireSnapshotError::UnknownFollower {
            follower_id: follower_id.0,
            location: snafu::location!(),
        })?;

    let (file, total_bytes) = manager
        .snapshot_persister()
        .open_encrypted(region, scope, snapshot_index)
        .await
        .context(OpenEncryptedSnafu)?;

    // The leader's current term + node id come from the shard's local
    // state. The reactor passed `(term, leader_id)` in the action, but we
    // re-read on the spawned task to avoid plumbing those fields through
    // the SnapshotSender trait — keeping the trait surface tight to
    // `(shard, follower, snapshot_index)`.
    // Failure case: still send; receiver will validate.
    let (leader_term, leader_id) =
        manager.leader_term_and_id_for_shard(shard_id).unwrap_or((0, follower_id.0));

    // Acquire (or build) the wire client for this follower from the
    // shared `NodeConnectionRegistry`. The registry caches one
    // `Arc<WireClient>` per peer; the same client is shared with the
    // consensus replicate-stream transport, so the snapshot RPC reuses
    // the same QUIC connection.
    let registry = manager.registry();
    let wire_client = registry
        .wire_client_for(follower_id.0, &address)
        .await
        .context(AcquireClientSnafu { address: address.clone() })?;

    let bytes_sent = stream_to_follower(
        wire_client.as_ref(),
        file,
        total_bytes,
        chunk_size_bytes,
        leader_term,
        leader_id,
        scope,
        region,
        snapshot_index,
    )
    .await?;

    Ok(bytes_sent)
}

/// Reads the encrypted snapshot file in `chunk_size_bytes` chunks and
/// streams each via `SnapshotSender::send_chunk`.
///
/// Sequence: header (carried in `open`) → 1..N data chunks → footer
/// (carried in `finish`). Returns the total byte count on success.
#[allow(clippy::too_many_arguments)]
async fn stream_to_follower(
    wire_client: &WireClient,
    mut file: tokio::fs::File,
    total_bytes: u64,
    chunk_size_bytes: usize,
    leader_term: u64,
    leader_id: u64,
    scope: SnapshotScope,
    region: Region,
    snapshot_index: u64,
) -> Result<u64, WireSnapshotError> {
    let header_scope = match scope {
        SnapshotScope::Org { organization_id } => {
            InstallSnapshotScope::Org(InstallSnapshotOrgScope {
                region: region.as_str().to_owned(),
                organization_id: organization_id.value(),
            })
        },
        SnapshotScope::Vault { organization_id, vault_id } => {
            InstallSnapshotScope::Vault(InstallSnapshotVaultScope {
                region: region.as_str().to_owned(),
                organization_id: organization_id.value(),
                vault_id: vault_id.value(),
            })
        },
    };

    let header = InstallSnapshotHeader {
        leader_term,
        leader_id,
        scope: Some(header_scope),
        snapshot_index,
        // Best-known approximation; receiver does not gate on it.
        snapshot_term: leader_term,
        total_bytes,
        chunk_size_bytes: chunk_size_bytes as u64,
        lsnp_version: LSNP_VERSION.to_owned(),
    };

    // Generate a fresh request-id for this stream. The receiver echoes it
    // on every ack; collision tolerance is acceptable here because the
    // sub-protocol never multiplexes two snapshot streams onto the same
    // bidi.
    let request_id = make_request_id(snapshot_index, leader_term, leader_id);

    let mut sender = wire_client.snapshot_stream(request_id, header).await.map_err(|err| {
        WireSnapshotError::OpenStream { reason: format!("{err}"), location: snafu::location!() }
    })?;

    let mut buf = vec![0u8; chunk_size_bytes];
    let mut bytes_sent: u64 = 0;
    loop {
        let n = file.read(&mut buf).await.context(IoSnafu)?;
        if n == 0 {
            break;
        }
        metrics::record_snapshot_send_chunk_size(n);
        bytes_sent = bytes_sent.saturating_add(n as u64);
        let chunk = Bytes::copy_from_slice(&buf[..n]);
        sender.send_chunk(chunk).await.context(StreamSnafu)?;
    }

    sender.finish().await.context(StreamSnafu)?;

    let _ = scope_kind(scope); // suppress unused warning when no metrics arms touch the helper
    Ok(bytes_sent)
}

/// Build a 128-bit request-id for the stream. The id has no functional
/// role beyond a diagnostic cookie the receiver echoes — the wire
/// sub-protocol's frame correlation runs entirely off the bidi stream's
/// send/recv pair, not the request id. Mixing in the snapshot index +
/// leader term + leader id keeps it stable per-stream and recognizable
/// in tracing without needing rng.
fn make_request_id(snapshot_index: u64, leader_term: u64, leader_id: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0..8].copy_from_slice(&snapshot_index.to_be_bytes());
    id[8..12].copy_from_slice(&(leader_term as u32).to_be_bytes());
    id[12..16].copy_from_slice(&(leader_id as u32).to_be_bytes());
    id
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::{Arc, Weak};

    use inferadb_ledger_consensus::{
        snapshot_sender::SnapshotSender,
        types::{ConsensusStateId, NodeId},
    };
    use inferadb_ledger_types::{OrganizationId, Region, VaultId};
    use tempfile::TempDir;

    use super::{WireSnapshotSender, scope_kind};
    use crate::{
        node_registry::NodeConnectionRegistry,
        raft_manager::{RaftManager, RaftManagerConfig},
        snapshot::SnapshotScope,
    };

    #[test]
    fn scope_kind_label_matches() {
        let org = SnapshotScope::Org { organization_id: OrganizationId::new(1) };
        let vault = SnapshotScope::Vault {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(2),
        };
        assert_eq!(scope_kind(org), "org");
        assert_eq!(scope_kind(vault), "vault");
    }

    #[test]
    fn make_request_id_is_deterministic() {
        let a = super::make_request_id(100, 5, 1);
        let b = super::make_request_id(100, 5, 1);
        assert_eq!(a, b, "same inputs must produce same request_id");

        let c = super::make_request_id(101, 5, 1);
        assert_ne!(a, c, "different snapshot_index must produce different request_id");
    }

    /// Sender with no resolvable scope mapping should drop silently — the
    /// reactor's contract is "fire-and-forget"; the leader's heartbeat
    /// retransmits on the next cycle.
    #[tokio::test]
    async fn send_snapshot_with_no_scope_mapping_drops_silently() {
        let temp = TempDir::new().expect("tempdir");
        let cfg = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let registry = Arc::new(NodeConnectionRegistry::new());
        let manager = Arc::new(RaftManager::new(cfg, registry));
        manager.install_self_weak();

        let sender = WireSnapshotSender {
            manager: Arc::new(parking_lot::Mutex::new(Arc::downgrade(&manager))),
            chunk_size_bytes: super::DEFAULT_CHUNK_SIZE_BYTES,
        };
        // Shard 999 was never registered — the dispatch task will log +
        // metric and exit. The contract is no panic.
        sender.send_snapshot(ConsensusStateId(999), NodeId(2), 100);
        // Give the spawned task a beat to land.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    /// Dropping the manager between weak-construction and dispatch must
    /// degrade to a debug log rather than a panic — same contract as the
    /// wake notifier and snapshot coordinator.
    #[tokio::test]
    async fn send_snapshot_with_dropped_manager_no_panic() {
        let weak: Weak<RaftManager> = Weak::new();
        let sender = WireSnapshotSender {
            manager: Arc::new(parking_lot::Mutex::new(weak)),
            chunk_size_bytes: super::DEFAULT_CHUNK_SIZE_BYTES,
        };
        sender.send_snapshot(ConsensusStateId(1), NodeId(1), 1);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
