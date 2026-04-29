//! Leader-side streaming of encrypted snapshot bytes to a follower over the
//! internal Raft transport's `InstallSnapshotStream` RPC.
//!
//! Stage 3 of the snapshot install path. The crate-private
//! `RaftManagerSnapshotSender` implements the consensus-crate
//! [`SnapshotSender`](inferadb_ledger_consensus::snapshot_sender::SnapshotSender)
//! trait: when the reactor processes
//! [`Action::SendSnapshot`](inferadb_ledger_consensus::action::Action::SendSnapshot),
//! the sender resolves the shard ID to its
//! `(region, organization_id, vault_id?)` scope, opens the at-rest
//! encrypted snapshot file via
//! [`SnapshotPersister::open_encrypted`](crate::snapshot_persister::SnapshotPersister::open_encrypted),
//! and ships chunks over a streaming gRPC client to the follower's
//! `RaftService`.
//!
//! ## Wire format (verbatim at-rest envelope)
//!
//! The streamer sends the at-rest AES-256-GCM envelope produced by
//! [`SnapshotPersister`](crate::snapshot_persister::SnapshotPersister)
//! verbatim — the wire protocol carries the same bytes that live on disk.
//! On-the-wire encryption is provided by WireGuard at the link layer (per
//! root CLAUDE.md), so the streaming protocol itself does not encrypt.
//! This avoids divergence between the disk and wire formats and skips a
//! decrypt/re-encrypt cycle on the leader.
//!
//! ## Drop-and-let-Raft-retry on failure
//!
//! Any failure (no scope mapping, follower address unknown, gRPC error,
//! short read) is logged at warn level and recorded as a metric, then
//! discarded. The leader's heartbeat replicator re-emits
//! `Action::SendSnapshot` on the next cycle, so transient failures
//! self-heal without explicit retry state in the reactor.

use std::sync::{Arc, Weak};

// Re-exported under a stable name so the receiver module can import the
// same hasher type through this module without a second crate dep entry.
pub(crate) use crc32fast::Hasher as Crc32Hasher;
use inferadb_ledger_consensus::{
    snapshot_sender::SnapshotSender,
    types::{ConsensusStateId, NodeId},
};
use inferadb_ledger_proto::proto::{
    InstallSnapshotChunk, InstallSnapshotFooter, InstallSnapshotHeader, InstallSnapshotOrgScope,
    InstallSnapshotVaultScope, install_snapshot_chunk::Payload as ChunkPayload,
    install_snapshot_header::Scope as HeaderScope, raft_service_client::RaftServiceClient,
};
use inferadb_ledger_types::Region;
#[cfg(test)]
use inferadb_ledger_types::VaultId;
use snafu::{Location, ResultExt, Snafu};
use tokio::io::AsyncReadExt;
use tonic::transport::Channel;

use crate::{
    metrics, raft_manager::RaftManager, snapshot::SnapshotScope, snapshot_persister::PersistError,
};

/// Default streaming chunk size (256 KiB).
///
/// Sized to amortize HTTP/2 framing without inflating tail latency on a
/// stalled follower. Smaller chunks waste framing overhead; larger chunks
/// hold flow-control credit longer and delay backpressure signals from
/// the receiver.
pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 256 * 1024;

/// Static LSNP version string included in every header. Mirrors the
/// version stamped into the on-disk snapshot file by `crates/raft/src/snapshot.rs`.
const LSNP_VERSION: &str = "LSNP-v2";

/// Errors produced by the leader-side streaming path.
#[derive(Debug, Snafu)]
pub enum StreamError {
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

    /// Could not parse the follower's address into a tonic endpoint.
    #[snafu(display("invalid follower address {address}: {source}"))]
    InvalidAddress {
        /// Address string from the peer-address map.
        address: String,
        /// Underlying tonic error.
        source: tonic::transport::Error,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Connecting to the follower failed.
    #[snafu(display("connect to follower at {address}: {source}"))]
    Connect {
        /// Address string from the peer-address map.
        address: String,
        /// Underlying tonic error.
        source: tonic::transport::Error,
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

    /// gRPC error while streaming to the follower.
    #[snafu(display("install_snapshot_stream rpc: {source}"))]
    Rpc {
        /// Underlying tonic status.
        source: tonic::Status,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// The follower rejected the stream (returned `success = false`).
    #[snafu(display(
        "follower rejected stream at scope={scope_kind}: term={follower_term} \
         staged_at_index={staged_at_index} message={error_message}"
    ))]
    FollowerRejected {
        /// `"org"` or `"vault"`.
        scope_kind: &'static str,
        /// Follower's reported current term.
        follower_term: u64,
        /// Echoed staged index (0 on rejection).
        staged_at_index: u64,
        /// Diagnostic message from the follower.
        error_message: String,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },
}

/// Leader-side snapshot sender bound to a [`RaftManager`].
///
/// Sister of `RaftManagerWakeNotifier` and `RaftManagerSnapshotCoordinator`
/// in `raft_manager.rs`: holds an `Arc<Mutex<Weak<RaftManager>>>` shared
/// with the manager, which bootstrap fills via
/// [`RaftManager::install_self_weak`] immediately after wrapping the
/// manager in `Arc`.
///
/// The reactor's snapshot-sender contract requires
/// [`SnapshotSender::send_snapshot`] to dispatch I/O asynchronously and
/// return immediately; this implementation `tokio::spawn`s the streaming
/// task and returns to the reactor after resolving the manager handle.
pub(crate) struct RaftManagerSnapshotSender {
    pub(crate) manager: Arc<parking_lot::Mutex<Weak<RaftManager>>>,
    pub(crate) chunk_size_bytes: usize,
}

impl SnapshotSender for RaftManagerSnapshotSender {
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
                    "RaftManagerSnapshotSender: weak upgrade returned None; skipping send \
                     (manager dropped or self_weak not installed)",
                );
                return;
            },
        };

        let chunk_size = self.chunk_size_bytes;
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            let scope_kind_for_metrics = "unknown";
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
                        "RaftManagerSnapshotSender: send_snapshot failed (will retry on next heartbeat)",
                    );
                    let _ = scope_kind_for_metrics;
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

/// Resolves shard → scope → snapshot file → follower address, then runs
/// the streaming RPC. Returns total bytes streamed on success.
async fn dispatch_send(
    manager: &RaftManager,
    shard_id: ConsensusStateId,
    follower_id: NodeId,
    snapshot_index: u64,
    chunk_size_bytes: usize,
) -> Result<u64, StreamError> {
    let (region, organization_id, vault_id) = manager
        .resolve_shard_to_scope(shard_id)
        .ok_or(StreamError::NoScopeMapping { shard_id, location: snafu::location!() })?;

    let scope = match vault_id {
        Some(vault_id) => SnapshotScope::Vault { organization_id, vault_id },
        None => SnapshotScope::Org { organization_id },
    };

    let address =
        manager.peer_addresses().get(follower_id.0).ok_or(StreamError::UnknownFollower {
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

    let channel = build_channel(&address).await?;
    let mut client = RaftServiceClient::new(channel);

    let response = stream_to_follower(
        &mut client,
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

    if !response.success {
        return Err(StreamError::FollowerRejected {
            scope_kind: scope_kind(scope),
            follower_term: response.follower_term,
            staged_at_index: response.staged_at_index,
            error_message: response.error_message,
            location: snafu::location!(),
        });
    }
    Ok(total_bytes)
}

/// Builds an HTTP/2 channel to the follower. Re-implements the
/// "endpoint string → tonic Channel" plumbing used elsewhere in this
/// crate; production-grade channel reuse via
/// [`NodeConnectionRegistry`](crate::node_registry::NodeConnectionRegistry)
/// is a future optimisation — the streaming RPC is rare enough that a
/// fresh connect-on-send is acceptable for Stage 3.
async fn build_channel(address: &str) -> Result<Channel, StreamError> {
    // Endpoint expects a URI with scheme; peer-address-map entries are bare
    // `host:port` for HTTP/2-without-TLS clusters (the production WireGuard
    // setup terminates TLS at the link layer). Prefix `http://` if missing
    // so `Endpoint::from_shared` parses cleanly.
    let uri = if address.starts_with("http://") || address.starts_with("https://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    };
    let endpoint =
        tonic::transport::Endpoint::from_shared(uri).map_err(|e| StreamError::InvalidAddress {
            address: address.to_owned(),
            source: e,
            location: snafu::location!(),
        })?;
    endpoint.connect().await.map_err(|e| StreamError::Connect {
        address: address.to_owned(),
        source: e,
        location: snafu::location!(),
    })
}

/// Reads the encrypted snapshot file in `chunk_size_bytes` chunks and
/// streams each as an `InstallSnapshotChunk`.
///
/// Sequence: header → 1..N data chunks → footer. Returns the gRPC
/// response so the caller can verify `success`.
#[allow(clippy::too_many_arguments)]
async fn stream_to_follower(
    client: &mut RaftServiceClient<Channel>,
    mut file: tokio::fs::File,
    total_bytes: u64,
    chunk_size_bytes: usize,
    leader_term: u64,
    leader_id: u64,
    scope: SnapshotScope,
    region: Region,
    snapshot_index: u64,
) -> Result<inferadb_ledger_proto::proto::InstallSnapshotStreamResponse, StreamError> {
    // Drain the file into memory through a bounded channel of chunks. We
    // build the channel large enough that the producer rarely blocks: a
    // 4-chunk buffer at 256KB is 1MB, which keeps RAM usage bounded
    // regardless of snapshot size.
    let (tx, rx) = tokio::sync::mpsc::channel::<InstallSnapshotChunk>(4);

    let chunk_size_u64 = chunk_size_bytes as u64;
    let header_scope = match scope {
        SnapshotScope::Org { organization_id } => HeaderScope::Org(InstallSnapshotOrgScope {
            region: region.as_str().to_owned(),
            organization_id: organization_id.value(),
        }),
        SnapshotScope::Vault { organization_id, vault_id } => {
            HeaderScope::Vault(InstallSnapshotVaultScope {
                region: region.as_str().to_owned(),
                organization_id: organization_id.value(),
                vault_id: vault_id.value(),
            })
        },
    };

    // Emit the header before spawning the data loop so the receiver
    // observes the scope and total_bytes before any data chunk.
    let header = InstallSnapshotChunk {
        payload: Some(ChunkPayload::Header(InstallSnapshotHeader {
            leader_term,
            leader_id,
            scope: Some(header_scope),
            snapshot_index,
            snapshot_term: leader_term, // best-known approximation; receiver does not gate on it
            total_bytes,
            chunk_size_bytes: chunk_size_u64,
            lsnp_version: LSNP_VERSION.to_owned(),
        })),
    };
    if tx.send(header).await.is_err() {
        // Receiver dropped before we even sent the header — happens when
        // the gRPC connect raced with a follower restart. Treat as RPC
        // failure; the caller will fail the send and Raft retries.
        return Err(StreamError::Rpc {
            source: tonic::Status::aborted(
                "install_snapshot_stream: receiver dropped before header",
            ),
            location: snafu::location!(),
        });
    }

    // Spawn the data-chunk producer. CRC32 (IEEE) is computed by the
    // producer and emitted in the footer so the receiver can validate
    // end-to-end. The wire field is `stream_crc32c` for naming
    // consistency with the proto, but the actual algorithm is CRC32-IEEE
    // (matching `crc32fast`, the workspace's standard CRC).
    let producer_handle = tokio::spawn(async move {
        let mut buf = vec![0u8; chunk_size_bytes];
        let mut crc = Crc32Hasher::new();
        let mut bytes_sent: u64 = 0;
        loop {
            let n = match file.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => return Err(StreamError::Io { source: e, location: snafu::location!() }),
            };
            // Capture chunk size for the histogram before we move the bytes.
            metrics::record_snapshot_send_chunk_size(n);
            crc.update(&buf[..n]);
            bytes_sent = bytes_sent.saturating_add(n as u64);
            let chunk =
                InstallSnapshotChunk { payload: Some(ChunkPayload::Data(buf[..n].to_vec())) };
            if tx.send(chunk).await.is_err() {
                return Err(StreamError::Rpc {
                    source: tonic::Status::aborted(
                        "install_snapshot_stream: receiver dropped mid-stream",
                    ),
                    location: snafu::location!(),
                });
            }
        }
        let footer = InstallSnapshotChunk {
            payload: Some(ChunkPayload::Footer(InstallSnapshotFooter {
                stream_crc32c: crc.finalize(),
                final_byte_count: bytes_sent,
            })),
        };
        if tx.send(footer).await.is_err() {
            return Err(StreamError::Rpc {
                source: tonic::Status::aborted(
                    "install_snapshot_stream: receiver dropped before footer",
                ),
                location: snafu::location!(),
            });
        }
        Ok::<_, StreamError>(bytes_sent)
    });

    let request = tokio_stream::wrappers::ReceiverStream::new(rx);
    let response = client
        .install_snapshot_stream(tonic::Request::new(request))
        .await
        .context(RpcSnafu)?
        .into_inner();

    // Surface producer-side errors (e.g., short-read on the snapshot file)
    // even when the server returned OK — the server may have OK'd a
    // partial stream that the producer aborted.
    match producer_handle.await {
        Ok(Ok(_)) => {},
        Ok(Err(e)) => return Err(e),
        Err(join_err) => {
            return Err(StreamError::Rpc {
                source: tonic::Status::internal(format!(
                    "install_snapshot_stream producer panicked: {join_err}"
                )),
                location: snafu::location!(),
            });
        },
    }

    Ok(response)
}

fn scope_kind(scope: SnapshotScope) -> &'static str {
    if scope.is_vault() { "vault" } else { "org" }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use std::sync::{Arc, Weak};

    use inferadb_ledger_consensus::{
        snapshot_sender::SnapshotSender,
        types::{ConsensusStateId, NodeId},
    };
    use inferadb_ledger_types::OrganizationId;
    use tempfile::TempDir;

    use super::{Crc32Hasher, RaftManagerSnapshotSender, Region, SnapshotScope, VaultId};
    use crate::raft_manager::{RaftManager, RaftManagerConfig};

    #[test]
    fn crc_chunked_matches_single() {
        let bytes: Vec<u8> = (0..1024u32).map(|i| (i & 0xff) as u8).collect();
        // Chunked
        let mut h = Crc32Hasher::new();
        h.update(&bytes[..400]);
        h.update(&bytes[400..]);
        let chunked = h.finalize();
        // Single
        let mut h_single = Crc32Hasher::new();
        h_single.update(&bytes);
        let single = h_single.finalize();
        assert_eq!(chunked, single);
    }

    #[test]
    fn scope_kind_label_matches_scope_kind() {
        let org = SnapshotScope::Org { organization_id: OrganizationId::new(1) };
        let vault = SnapshotScope::Vault {
            organization_id: OrganizationId::new(1),
            vault_id: VaultId::new(2),
        };
        assert_eq!(super::scope_kind(org), "org");
        assert_eq!(super::scope_kind(vault), "vault");
        // unused suppression
        let _ = Region::GLOBAL;
    }

    /// Sender with no resolvable scope mapping should drop silently — the
    /// reactor's contract is "fire-and-forget"; the leader's heartbeat
    /// retransmits on the next cycle.
    #[tokio::test]
    async fn send_snapshot_with_no_scope_mapping_drops_silently() {
        let temp = TempDir::new().expect("tempdir");
        let cfg = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let registry = Arc::new(crate::node_registry::NodeConnectionRegistry::new());
        let manager = Arc::new(RaftManager::new(cfg, registry));
        manager.install_self_weak();

        let sender = RaftManagerSnapshotSender {
            manager: Arc::new(parking_lot::Mutex::new(Arc::downgrade(&manager))),
            chunk_size_bytes: super::DEFAULT_CHUNK_SIZE_BYTES,
        };
        // Shard 999 was never registered — the dispatch task will log +
        // metric and exit. The contract is no panic.
        sender.send_snapshot(ConsensusStateId(999), NodeId(2), 100);
        // Give the spawned task a beat to land.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // No panic == pass.
    }

    /// Dropping the manager between weak-construction and dispatch must
    /// degrade to a debug log rather than a panic — same contract as the
    /// wake notifier and snapshot coordinator.
    #[tokio::test]
    async fn send_snapshot_with_dropped_manager_no_panic() {
        let weak: Weak<RaftManager> = Weak::new();
        let sender = RaftManagerSnapshotSender {
            manager: Arc::new(parking_lot::Mutex::new(weak)),
            chunk_size_bytes: super::DEFAULT_CHUNK_SIZE_BYTES,
        };
        sender.send_snapshot(ConsensusStateId(1), NodeId(1), 1);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
