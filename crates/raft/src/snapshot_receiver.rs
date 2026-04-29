//! Follower-side reception of streamed snapshots from a leader.
//!
//! Stage 3 of the snapshot install path. Mirror of
//! [`snapshot_streamer`](crate::snapshot_streamer):
//!
//! 1. The leader's `RaftManagerSnapshotSender` (in [`snapshot_streamer`](crate::snapshot_streamer))
//!    opens an `InstallSnapshotStream` RPC, ships a header chunk, then 1..N data chunks, then a
//!    footer.
//! 2. The follower's [`receive_install_snapshot_stream`] handler validates the header (term,
//!    scope), opens a staging temp file, appends every `data` chunk, and on `footer` validates the
//!    CRC + final byte count before atomically renaming the temp into a
//!    `staging-{index:020}.snap.staged` file under the same per-scope directory used by
//!    [`SnapshotPersister`](crate::snapshot_persister::SnapshotPersister).
//! 3. Stage 4's installer discovers staged files via
//!    [`SnapshotPersister::list_staged`](crate::snapshot_persister::SnapshotPersister::list_staged),
//!    decrypts via the [`SnapshotKeyProvider`](crate::snapshot_key_provider::SnapshotKeyProvider),
//!    and installs through
//!    [`RaftLogStore::install_snapshot`](crate::log_storage::RaftLogStore::install_snapshot).
//!
//! ## Validation order
//!
//! Header → data*+ → footer is the required sequence. The receiver rejects
//! and cleans up the staging temp file on:
//!
//! - A non-`Header` first chunk.
//! - A `Header` chunk after the first.
//! - A `Footer` chunk before any `Data` chunk.
//! - A `Data` chunk after a `Footer`.
//! - A stream that ends without a `Footer`.
//! - A `Footer` whose CRC does not match the running checksum.
//! - A `Footer` whose `final_byte_count` does not match the actual bytes received.
//!
//! ## Why no decrypt here
//!
//! The follower writes the at-rest envelope verbatim and lets Stage 4's
//! installer decrypt on consumption. This keeps Stage 3's responsibilities
//! narrow (CRC + scope match + atomic rename) and lets the install path
//! own the policy decision of how to surface a key-mismatch (operator
//! warning vs. abort).

use std::{str::FromStr, sync::Arc};

use inferadb_ledger_proto::proto::{
    InstallSnapshotChunk, InstallSnapshotHeader, InstallSnapshotStreamResponse,
    install_snapshot_chunk::Payload as ChunkPayload, install_snapshot_header::Scope as HeaderScope,
};
use inferadb_ledger_types::{OrganizationId, Region, VaultId};
use snafu::{Location, ResultExt, Snafu};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

use crate::{
    metrics, raft_manager::RaftManager, snapshot::SnapshotScope, snapshot_persister::PersistError,
    snapshot_streamer::Crc32Hasher,
};

/// Errors emitted by the follower-side stream handler. These are converted
/// to `tonic::Status` at the gRPC service boundary.
#[derive(Debug, Snafu)]
pub enum ReceiveError {
    /// First chunk on the stream was not a header.
    #[snafu(display("install_snapshot_stream: first chunk was not a header"))]
    MissingHeader {
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Stream carried a header twice or in the wrong order.
    #[snafu(display("install_snapshot_stream: out-of-order chunk: {detail}"))]
    OutOfOrder {
        /// Diagnostic about the violation.
        detail: &'static str,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Header carried no scope or an invalid region.
    #[snafu(display("install_snapshot_stream: invalid scope: {reason}"))]
    InvalidScope {
        /// Why the scope is invalid.
        reason: String,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Stream's CRC32 footer did not match the bytes the receiver got.
    #[snafu(display(
        "install_snapshot_stream: crc mismatch: expected={expected:#010x} actual={actual:#010x}"
    ))]
    CrcMismatch {
        /// CRC reported by the leader in the footer.
        expected: u32,
        /// CRC computed over received bytes.
        actual: u32,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Stream's footer byte-count did not match the bytes the receiver got.
    #[snafu(display(
        "install_snapshot_stream: byte-count mismatch: footer={footer} received={received}"
    ))]
    ByteCountMismatch {
        /// Bytes reported by the leader in the footer.
        footer: u64,
        /// Bytes the receiver actually wrote to the staging file.
        received: u64,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Stream ended without a footer.
    #[snafu(display("install_snapshot_stream: stream ended without footer"))]
    UnterminatedStream {
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Underlying gRPC stream error.
    #[snafu(display("install_snapshot_stream: gRPC stream error: {source}"))]
    Stream {
        /// Underlying tonic status.
        source: tonic::Status,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Filesystem error opening / writing / renaming the staging file.
    #[snafu(display("install_snapshot_stream: I/O error: {source}"))]
    Io {
        /// Underlying I/O error.
        source: std::io::Error,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },

    /// Persister-side error (creating the staging directory, removing a
    /// failed temp).
    #[snafu(display("install_snapshot_stream: persister error: {source}"))]
    Persister {
        /// Underlying persister error.
        source: PersistError,
        /// Implicit location.
        #[snafu(implicit)]
        location: Location,
    },
}

/// Outcome of a successful receive. The receiver-side handler returns this
/// to the gRPC service which packages it into the streaming RPC response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedSnapshot {
    /// Region the staged snapshot belongs to.
    pub region: Region,
    /// Scope the staged snapshot belongs to.
    pub scope: SnapshotScope,
    /// Last log index covered by the staged snapshot.
    pub snapshot_index: u64,
    /// Bytes written to the staging file (post-CRC validation).
    pub bytes_received: u64,
    /// Absolute path to the staged file. Stage 4 picks up via
    /// [`SnapshotPersister::list_staged`](crate::snapshot_persister::SnapshotPersister::list_staged).
    pub staged_path: std::path::PathBuf,
}

/// Consumes the inbound stream, validates ordering + CRC + scope, and
/// writes the encrypted bytes to a staging file under the per-scope
/// directory.
///
/// Caller (the gRPC `RaftService`) owns the manager handle and translates
/// the result to a [`InstallSnapshotStreamResponse`].
pub async fn receive_install_snapshot_stream(
    manager: &Arc<RaftManager>,
    inbound: tonic::Streaming<InstallSnapshotChunk>,
) -> Result<ReceivedSnapshot, ReceiveError> {
    receive_chunks(manager, inbound).await
}

/// Inner receive routine generic over the stream type. Lets tests drive
/// the receiver with an in-memory `Stream` without spinning up a full
/// gRPC server. Production callers go through
/// [`receive_install_snapshot_stream`] which fixes the inbound type to
/// `tonic::Streaming<InstallSnapshotChunk>`.
pub async fn receive_chunks<S>(
    manager: &Arc<RaftManager>,
    inbound: S,
) -> Result<ReceivedSnapshot, ReceiveError>
where
    S: tokio_stream::Stream<Item = Result<InstallSnapshotChunk, tonic::Status>> + Unpin,
{
    let mut inbound = inbound;
    // ── Header ──────────────────────────────────────────────────────
    let first =
        inbound.next().await.ok_or(ReceiveError::MissingHeader { location: snafu::location!() })?;
    let first = first.context(StreamSnafu)?;
    let header = match first.payload {
        Some(ChunkPayload::Header(h)) => h,
        Some(ChunkPayload::Data(_)) => {
            return Err(ReceiveError::OutOfOrder {
                detail: "first chunk was data; expected header",
                location: snafu::location!(),
            });
        },
        Some(ChunkPayload::Footer(_)) => {
            return Err(ReceiveError::OutOfOrder {
                detail: "first chunk was footer; expected header",
                location: snafu::location!(),
            });
        },
        None => return Err(ReceiveError::MissingHeader { location: snafu::location!() }),
    };

    let (region, scope) = parse_header_scope(&header)?;
    let scope_kind = if scope.is_vault() { "vault" } else { "org" };

    // Open the staging temp file under the per-scope directory.
    let persister = manager.snapshot_persister();
    persister.ensure_staging_dir(region, scope).await.context(PersisterSnafu)?;
    let temp_path =
        persister.staging_temp_path(region, scope, header.leader_term, header.snapshot_index);
    let staged_path = persister.staged_path(region, scope, header.snapshot_index);

    // Open the temp file with `create_new` so a stale orphan from a
    // prior crash forces a fresh attempt — the leader's retry will
    // observe the rename succeed once the staging directory is clean.
    let temp_file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)
        .await
        .context(IoSnafu);
    // If `create_new` fails with AlreadyExists, blow away the orphan and
    // retry once. Concurrent senders for the same `(leader_term, index)`
    // would otherwise both fail; we accept the second-wins race here, since
    // the staging file's contents are byte-for-byte identical for the same
    // snapshot.
    let mut temp_file = match temp_file {
        Ok(f) => f,
        Err(ReceiveError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            tokio::fs::remove_file(&temp_path).await.ok();
            tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)
                .await
                .context(IoSnafu)?
        },
        Err(other) => return Err(other),
    };

    // ── Data chunks ─────────────────────────────────────────────────
    let mut hasher = Crc32Hasher::new();
    let mut bytes_received: u64 = 0;
    let mut footer_observed: Option<inferadb_ledger_proto::proto::InstallSnapshotFooter> = None;

    while let Some(next) = inbound.next().await {
        // Wrap each chunk's poll error as a stream error; the cleanup
        // path runs unconditionally below.
        let chunk = match next {
            Ok(c) => c,
            Err(e) => {
                cleanup_temp(&temp_path).await;
                metrics::record_snapshot_recv(scope_kind, "interrupted", bytes_received);
                return Err(ReceiveError::Stream { source: e, location: snafu::location!() });
            },
        };
        match chunk.payload {
            Some(ChunkPayload::Header(_)) => {
                cleanup_temp(&temp_path).await;
                metrics::record_snapshot_recv(scope_kind, "failure", bytes_received);
                return Err(ReceiveError::OutOfOrder {
                    detail: "second header observed mid-stream",
                    location: snafu::location!(),
                });
            },
            Some(ChunkPayload::Data(bytes)) => {
                if footer_observed.is_some() {
                    cleanup_temp(&temp_path).await;
                    metrics::record_snapshot_recv(scope_kind, "failure", bytes_received);
                    return Err(ReceiveError::OutOfOrder {
                        detail: "data chunk after footer",
                        location: snafu::location!(),
                    });
                }
                if let Err(e) = temp_file.write_all(&bytes).await {
                    cleanup_temp(&temp_path).await;
                    metrics::record_snapshot_recv(scope_kind, "failure", bytes_received);
                    return Err(ReceiveError::Io { source: e, location: snafu::location!() });
                }
                hasher.update(&bytes);
                bytes_received = bytes_received.saturating_add(bytes.len() as u64);
            },
            Some(ChunkPayload::Footer(footer)) => {
                footer_observed = Some(footer);
                // Stop reading the stream — the leader sends nothing after
                // the footer. The outer loop's `next().await` would then
                // observe the half-close and exit.
                break;
            },
            None => {
                cleanup_temp(&temp_path).await;
                metrics::record_snapshot_recv(scope_kind, "failure", bytes_received);
                return Err(ReceiveError::OutOfOrder {
                    detail: "chunk with no payload",
                    location: snafu::location!(),
                });
            },
        }
    }

    let Some(footer) = footer_observed else {
        cleanup_temp(&temp_path).await;
        metrics::record_snapshot_recv(scope_kind, "interrupted", bytes_received);
        return Err(ReceiveError::UnterminatedStream { location: snafu::location!() });
    };

    // Validate footer.
    let actual_crc = hasher.finalize();
    if actual_crc != footer.stream_crc32c {
        cleanup_temp(&temp_path).await;
        metrics::record_snapshot_recv(scope_kind, "failure", bytes_received);
        return Err(ReceiveError::CrcMismatch {
            expected: footer.stream_crc32c,
            actual: actual_crc,
            location: snafu::location!(),
        });
    }
    if footer.final_byte_count != bytes_received {
        cleanup_temp(&temp_path).await;
        metrics::record_snapshot_recv(scope_kind, "failure", bytes_received);
        return Err(ReceiveError::ByteCountMismatch {
            footer: footer.final_byte_count,
            received: bytes_received,
            location: snafu::location!(),
        });
    }

    // Flush + sync the temp file before rename so the staged file's
    // contents are durable across a crash. Without `sync_data`, the rename
    // can land before the file body, leaving Stage 4 with a half-written
    // file that decrypt or LSNP-verify will reject.
    if let Err(e) = temp_file.flush().await {
        cleanup_temp(&temp_path).await;
        metrics::record_snapshot_recv(scope_kind, "failure", bytes_received);
        return Err(ReceiveError::Io { source: e, location: snafu::location!() });
    }
    if let Err(e) = temp_file.sync_data().await {
        cleanup_temp(&temp_path).await;
        metrics::record_snapshot_recv(scope_kind, "failure", bytes_received);
        return Err(ReceiveError::Io { source: e, location: snafu::location!() });
    }
    drop(temp_file);

    // Atomic rename to the `.snap.staged` final name.
    if let Err(e) = tokio::fs::rename(&temp_path, &staged_path).await {
        cleanup_temp(&temp_path).await;
        metrics::record_snapshot_recv(scope_kind, "failure", bytes_received);
        return Err(ReceiveError::Io { source: e, location: snafu::location!() });
    }

    metrics::record_snapshot_recv(scope_kind, "success", bytes_received);
    metrics::record_snapshot_recv_staged(scope_kind);

    Ok(ReceivedSnapshot {
        region,
        scope,
        snapshot_index: header.snapshot_index,
        bytes_received,
        staged_path,
    })
}

/// Best-effort delete of a staging temp file. Logged on failure but never
/// surfaced — the cleanup is opportunistic and a left-behind file is
/// safe (the next staging attempt's rename overwrites it via the new
/// `staging-` filename, and any orphaned temp is excluded from
/// `list_staged` because the suffix differs from `.snap.staged`).
async fn cleanup_temp(path: &std::path::Path) {
    if let Err(e) = tokio::fs::remove_file(path).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            error = %e,
            path = %path.display(),
            "snapshot_receiver: failed to clean up staging temp file",
        );
    }
}

/// Translates the proto header's scope oneof into a `(region, SnapshotScope)`
/// tuple. Returns `InvalidScope` for missing oneofs or unknown region slugs.
fn parse_header_scope(
    header: &InstallSnapshotHeader,
) -> Result<(Region, SnapshotScope), ReceiveError> {
    let scope = header.scope.as_ref().ok_or(ReceiveError::InvalidScope {
        reason: "header.scope is None".to_owned(),
        location: snafu::location!(),
    })?;
    match scope {
        HeaderScope::Org(org) => {
            let region = parse_region(&org.region)?;
            Ok((
                region,
                SnapshotScope::Org { organization_id: OrganizationId::new(org.organization_id) },
            ))
        },
        HeaderScope::Vault(v) => {
            let region = parse_region(&v.region)?;
            Ok((
                region,
                SnapshotScope::Vault {
                    organization_id: OrganizationId::new(v.organization_id),
                    vault_id: VaultId::new(v.vault_id),
                },
            ))
        },
    }
}

fn parse_region(raw: &str) -> Result<Region, ReceiveError> {
    if raw.is_empty() {
        return Ok(Region::GLOBAL);
    }
    Region::from_str(raw).map_err(|e| ReceiveError::InvalidScope {
        reason: format!("invalid region {raw:?}: {e}"),
        location: snafu::location!(),
    })
}

/// Builds a `InstallSnapshotStreamResponse` for a successful receive. The
/// follower's term is queried from the manager via
/// [`RaftManager::current_term_for_scope`]; on failure (shard
/// disappeared mid-stream) the term defaults to 0 and the response is
/// still `success = true` since the file has been staged.
pub fn success_response(
    manager: &RaftManager,
    received: &ReceivedSnapshot,
) -> InstallSnapshotStreamResponse {
    let follower_term = manager
        .current_term_for_scope(
            received.region,
            received.scope.organization_id(),
            received.scope.vault_id(),
        )
        .unwrap_or(0);
    InstallSnapshotStreamResponse {
        follower_term,
        success: true,
        error_message: String::new(),
        staged_at_index: received.snapshot_index,
    }
}

/// Builds a `InstallSnapshotStreamResponse` for a receive failure. The
/// region anchor (when known from the header) lets us answer with the
/// data-region group's current term so the leader can step down on a
/// stale-term mismatch. The error message surfaces the receive-side cause
/// without leaking internal types.
pub fn failure_response(
    manager: &RaftManager,
    region: Option<Region>,
    err: &ReceiveError,
) -> InstallSnapshotStreamResponse {
    let follower_term = region
        .and_then(|r| manager.current_term_for_scope(r, OrganizationId::new(0), None))
        .unwrap_or(0);
    InstallSnapshotStreamResponse {
        follower_term,
        success: false,
        error_message: err.to_string(),
        staged_at_index: 0,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::disallowed_methods, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use inferadb_ledger_proto::proto::{
        InstallSnapshotChunk, InstallSnapshotFooter, InstallSnapshotHeader,
        InstallSnapshotOrgScope, InstallSnapshotVaultScope, install_snapshot_chunk::Payload,
        install_snapshot_header::Scope,
    };
    use inferadb_ledger_types::{OrganizationId, Region, VaultId};
    use tempfile::TempDir;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;
    use crate::{
        raft_manager::{RaftManager, RaftManagerConfig},
        snapshot::SnapshotScope,
        snapshot_streamer::Crc32Hasher,
    };

    fn make_test_manager() -> (Arc<RaftManager>, TempDir) {
        let temp = tempfile::tempdir().expect("tempdir");
        let cfg = RaftManagerConfig::new(temp.path().to_path_buf(), 1, Region::GLOBAL);
        let registry = Arc::new(crate::node_registry::NodeConnectionRegistry::new());
        let manager = Arc::new(RaftManager::new(cfg, registry));
        (manager, temp)
    }

    /// Builds an in-memory chunk-stream + the expected CRC.
    fn build_chunks(
        scope: SnapshotScope,
        region: Region,
        snapshot_index: u64,
        leader_term: u64,
        body: &[u8],
        chunk_size: usize,
    ) -> (Vec<InstallSnapshotChunk>, u32) {
        let mut chunks = Vec::new();
        let scope_proto = match scope {
            SnapshotScope::Org { organization_id } => Scope::Org(InstallSnapshotOrgScope {
                region: region.as_str().to_owned(),
                organization_id: organization_id.value(),
            }),
            SnapshotScope::Vault { organization_id, vault_id } => {
                Scope::Vault(InstallSnapshotVaultScope {
                    region: region.as_str().to_owned(),
                    organization_id: organization_id.value(),
                    vault_id: vault_id.value(),
                })
            },
        };
        chunks.push(InstallSnapshotChunk {
            payload: Some(Payload::Header(InstallSnapshotHeader {
                leader_term,
                leader_id: 1,
                scope: Some(scope_proto),
                snapshot_index,
                snapshot_term: leader_term,
                total_bytes: body.len() as u64,
                chunk_size_bytes: chunk_size as u64,
                lsnp_version: "LSNP-v2".to_owned(),
            })),
        });
        let mut hasher = Crc32Hasher::new();
        for slice in body.chunks(chunk_size) {
            hasher.update(slice);
            chunks.push(InstallSnapshotChunk { payload: Some(Payload::Data(slice.to_vec())) });
        }
        let crc = hasher.finalize();
        chunks.push(InstallSnapshotChunk {
            payload: Some(Payload::Footer(InstallSnapshotFooter {
                stream_crc32c: crc,
                final_byte_count: body.len() as u64,
            })),
        });
        (chunks, crc)
    }

    /// Drives `receive_chunks` with a `mpsc`-backed `ReceiverStream`.
    async fn run_receive(
        manager: &Arc<RaftManager>,
        chunks: Vec<Result<InstallSnapshotChunk, tonic::Status>>,
    ) -> Result<ReceivedSnapshot, ReceiveError> {
        let (tx, rx) = tokio::sync::mpsc::channel(chunks.len().max(1));
        for c in chunks {
            tx.send(c).await.expect("send");
        }
        drop(tx);
        let stream = ReceiverStream::new(rx);
        receive_chunks(manager, stream).await
    }

    #[test]
    fn parse_header_scope_org() {
        let header = InstallSnapshotHeader {
            leader_term: 1,
            leader_id: 1,
            scope: Some(Scope::Org(InstallSnapshotOrgScope {
                region: String::new(),
                organization_id: 7,
            })),
            snapshot_index: 100,
            snapshot_term: 1,
            total_bytes: 0,
            chunk_size_bytes: 0,
            lsnp_version: "LSNP-v2".to_owned(),
        };
        let (region, scope) = parse_header_scope(&header).unwrap();
        assert_eq!(region, Region::GLOBAL);
        assert_eq!(scope, SnapshotScope::Org { organization_id: OrganizationId::new(7) });
    }

    #[test]
    fn parse_header_scope_vault() {
        let header = InstallSnapshotHeader {
            leader_term: 1,
            leader_id: 1,
            scope: Some(Scope::Vault(InstallSnapshotVaultScope {
                region: String::new(),
                organization_id: 7,
                vault_id: 3,
            })),
            snapshot_index: 100,
            snapshot_term: 1,
            total_bytes: 0,
            chunk_size_bytes: 0,
            lsnp_version: "LSNP-v2".to_owned(),
        };
        let (region, scope) = parse_header_scope(&header).unwrap();
        assert_eq!(region, Region::GLOBAL);
        assert_eq!(
            scope,
            SnapshotScope::Vault {
                organization_id: OrganizationId::new(7),
                vault_id: VaultId::new(3),
            }
        );
    }

    #[test]
    fn parse_header_scope_missing_returns_invalid() {
        let header = InstallSnapshotHeader {
            leader_term: 0,
            leader_id: 0,
            scope: None,
            snapshot_index: 0,
            snapshot_term: 0,
            total_bytes: 0,
            chunk_size_bytes: 0,
            lsnp_version: String::new(),
        };
        let err = parse_header_scope(&header).expect_err("should reject missing scope");
        assert!(matches!(err, ReceiveError::InvalidScope { .. }));
    }

    #[test]
    fn build_chunks_round_trips_crc() {
        let body = b"some encrypted snapshot bytes for the test";
        let (chunks, crc) = build_chunks(
            SnapshotScope::Org { organization_id: OrganizationId::new(1) },
            Region::GLOBAL,
            42,
            1,
            body,
            16,
        );
        // 1 header + ceil(len/16) data + 1 footer
        let expected_data = body.len().div_ceil(16);
        assert_eq!(chunks.len(), 1 + expected_data + 1);
        // Footer CRC matches the running hash.
        let mut h = Crc32Hasher::new();
        h.update(body);
        assert_eq!(crc, h.finalize());
    }

    #[tokio::test]
    async fn receive_round_trips_org_snapshot() {
        let (manager, _temp) = make_test_manager();
        let body: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
        let scope = SnapshotScope::Org { organization_id: OrganizationId::new(99) };
        let (chunks, _crc) = build_chunks(scope, Region::GLOBAL, 100, 1, &body, 256);
        let received =
            run_receive(&manager, chunks.into_iter().map(Ok).collect()).await.expect("receive");
        assert_eq!(received.snapshot_index, 100);
        assert_eq!(received.bytes_received, body.len() as u64);
        // Staging file exists and matches the body byte-for-byte.
        let on_disk = tokio::fs::read(&received.staged_path).await.expect("read staged");
        assert_eq!(on_disk, body);
        // Stage 4 will discover the staged file via list_staged.
        let staged =
            manager.snapshot_persister().list_staged(Region::GLOBAL, scope).await.expect("list");
        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].index, 100);
    }

    #[tokio::test]
    async fn receive_rejects_crc_mismatch_and_cleans_up() {
        let (manager, _temp) = make_test_manager();
        let body = b"corrupted body".to_vec();
        let scope = SnapshotScope::Vault {
            organization_id: OrganizationId::new(7),
            vault_id: VaultId::new(3),
        };
        let (mut chunks, _crc) = build_chunks(scope, Region::GLOBAL, 50, 1, &body, 8);
        // Replace the footer with a garbage CRC.
        let last = chunks.last_mut().expect("footer");
        last.payload = Some(Payload::Footer(InstallSnapshotFooter {
            stream_crc32c: 0xDEADBEEF,
            final_byte_count: body.len() as u64,
        }));
        let err = run_receive(&manager, chunks.into_iter().map(Ok).collect())
            .await
            .expect_err("crc mismatch must reject");
        assert!(matches!(err, ReceiveError::CrcMismatch { .. }));
        // No staged file should remain.
        let staged =
            manager.snapshot_persister().list_staged(Region::GLOBAL, scope).await.expect("list");
        assert!(staged.is_empty(), "staged file must be cleaned up on CRC mismatch");
    }

    #[tokio::test]
    async fn receive_rejects_byte_count_mismatch() {
        let (manager, _temp) = make_test_manager();
        let body = b"some bytes".to_vec();
        let scope = SnapshotScope::Org { organization_id: OrganizationId::new(2) };
        let (mut chunks, crc) = build_chunks(scope, Region::GLOBAL, 7, 1, &body, 4);
        // Replace the footer with a wrong byte count (CRC stays correct).
        let last = chunks.last_mut().expect("footer");
        last.payload = Some(Payload::Footer(InstallSnapshotFooter {
            stream_crc32c: crc,
            final_byte_count: (body.len() + 1) as u64,
        }));
        let err = run_receive(&manager, chunks.into_iter().map(Ok).collect())
            .await
            .expect_err("byte-count mismatch must reject");
        assert!(matches!(err, ReceiveError::ByteCountMismatch { .. }));
    }

    #[tokio::test]
    async fn receive_rejects_missing_header() {
        let (manager, _temp) = make_test_manager();
        // Send a data chunk with no preceding header.
        let chunks = vec![Ok(InstallSnapshotChunk { payload: Some(Payload::Data(vec![1, 2, 3])) })];
        let err = run_receive(&manager, chunks).await.expect_err("must reject");
        assert!(matches!(err, ReceiveError::OutOfOrder { .. }));
    }

    #[tokio::test]
    async fn receive_rejects_unterminated_stream() {
        let (manager, _temp) = make_test_manager();
        let body = b"partial".to_vec();
        let scope = SnapshotScope::Org { organization_id: OrganizationId::new(2) };
        // Drop the footer.
        let (mut chunks, _) = build_chunks(scope, Region::GLOBAL, 1, 1, &body, 4);
        chunks.pop();
        let err = run_receive(&manager, chunks.into_iter().map(Ok).collect())
            .await
            .expect_err("unterminated must reject");
        assert!(matches!(err, ReceiveError::UnterminatedStream { .. }));
    }

    #[tokio::test]
    async fn receive_ignores_chunks_after_footer() {
        // The receiver stops reading after the footer (which validates
        // CRC + byte count). Any chunks the leader sends after the
        // footer are dropped on the floor — the leader's contract is
        // header → data* → footer, and our wire framing does not
        // require the receiver to drain a closed stream beyond the
        // footer.
        let (manager, _temp) = make_test_manager();
        let body = b"hi".to_vec();
        let scope = SnapshotScope::Org { organization_id: OrganizationId::new(3) };
        let (mut chunks, _) = build_chunks(scope, Region::GLOBAL, 1, 1, &body, 1);
        chunks.push(InstallSnapshotChunk { payload: Some(Payload::Data(vec![0])) });
        let received = run_receive(&manager, chunks.into_iter().map(Ok).collect())
            .await
            .expect("footer-validated stream must succeed; trailing data is ignored");
        assert_eq!(received.bytes_received, body.len() as u64);
    }

    #[tokio::test]
    async fn receive_rejects_second_header() {
        let (manager, _temp) = make_test_manager();
        let scope = SnapshotScope::Org { organization_id: OrganizationId::new(3) };
        let header_chunk = InstallSnapshotChunk {
            payload: Some(Payload::Header(InstallSnapshotHeader {
                leader_term: 1,
                leader_id: 1,
                scope: Some(Scope::Org(InstallSnapshotOrgScope {
                    region: Region::GLOBAL.as_str().to_owned(),
                    organization_id: 3,
                })),
                snapshot_index: 1,
                snapshot_term: 1,
                total_bytes: 0,
                chunk_size_bytes: 0,
                lsnp_version: "LSNP-v2".to_owned(),
            })),
        };
        // Two headers in a row.
        let chunks = vec![Ok(header_chunk.clone()), Ok(header_chunk)];
        let err = run_receive(&manager, chunks).await.expect_err("must reject");
        assert!(matches!(err, ReceiveError::OutOfOrder { .. }));
        // No staged file should remain.
        let staged =
            manager.snapshot_persister().list_staged(Region::GLOBAL, scope).await.expect("list");
        assert!(staged.is_empty());
    }

    #[tokio::test]
    async fn receive_handles_zero_byte_body() {
        let (manager, _temp) = make_test_manager();
        let scope = SnapshotScope::Org { organization_id: OrganizationId::new(11) };
        let (chunks, _) = build_chunks(scope, Region::GLOBAL, 5, 1, b"", 16);
        // 1 header + 0 data + 1 footer
        assert_eq!(chunks.len(), 2);
        let received = run_receive(&manager, chunks.into_iter().map(Ok).collect())
            .await
            .expect("zero-byte must succeed");
        assert_eq!(received.bytes_received, 0);
        let on_disk = tokio::fs::read(&received.staged_path).await.expect("read");
        assert!(on_disk.is_empty());
    }
}
