//! Sub-protocol for streaming Raft snapshot installation.
//!
//! gRPC's client-streaming semantics enforced the legacy
//! `InstallSnapshotStream` RPC's chunk ordering implicitly. Over QUIC's
//! bidi stream, we re-implement that ordering in application code:
//!
//! ```text
//!   client → server: header
//!   client → server: chunk @ offset 0
//!   client → server: chunk @ offset N
//!   server → client: ack @ offset N    (backpressure)
//!   client → server: chunk @ offset N+M
//!   ...
//!   client → server: footer (CRC32, is_stream_end)
//! ```
//!
//! The wire-macro DSL doesn't model client-streaming, so the macro-generated
//! protocol surface declares `RaftService::install_snapshot_stream` as a
//! unary placeholder at opcode [`OPCODE_INSTALL_SNAPSHOT_STREAM`]. The
//! transport's stream dispatcher recognises that opcode and routes the bidi
//! stream into [`run_snapshot_receive_loop`] instead of going through the
//! standard unary/server-streaming path. The sender side opens an outbound
//! bidi via [`SnapshotSender::open`] and drives the chunked exchange
//! manually — it never goes through [`crate::client::WireClient::unary`] or
//! [`crate::client::WireClient::server_stream`].
//!
//! ## Frame shape on the wire
//!
//! All frames carry opcode [`OPCODE_INSTALL_SNAPSHOT_STREAM`] and the sender's
//! 128-bit `request_id` (echoed by the receiver on every ack). The
//! sub-protocol lives in two places:
//!
//! * **Client→server** frames carry a postcard-encoded [`InstallSnapshotChunk`] payload (header,
//!   then 1..N data, then footer). Header carries `flags = 0`; data chunks carry
//!   [`crate::frame_io::FLAG_IS_STREAM_CHUNK`]; the footer carries
//!   [`crate::frame_io::FLAG_IS_STREAM_END`].
//! * **Server→client** ack frames carry a postcard-encoded [`InstallSnapshotAck`] payload with
//!   [`crate::frame_io::FLAG_IS_RESPONSE`] set. The terminal ack additionally carries
//!   [`crate::frame_io::FLAG_IS_STREAM_END`] and is sent only after the staging file has been
//!   atomically renamed.
//!
//! ## Staging file naming + crash recovery
//!
//! Receivers write each in-progress snapshot to
//! `staging-{index:020}.snap.staged.partial` under the staging directory.
//! On successful footer + CRC validation the file is atomically renamed to
//! `staging-{index:020}.snap.staged`. On any error mid-stream the `.partial`
//! file is left in place; [`cleanup_partial_snapshots`] deletes any
//! `.partial` files at startup before any Raft engines come up.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use inferadb_ledger_wire::{
    CURRENT_PROTOCOL_VERSION, ErrorCode, Frame, FrameHeader, OPCODE_INSTALL_SNAPSHOT_STREAM,
    WireError,
    services::raft::{
        InstallSnapshotAck, InstallSnapshotChunk, InstallSnapshotFooter, InstallSnapshotHeader,
        InstallSnapshotPayload,
    },
};
use quinn::{Connection, RecvStream, SendStream};
use tokio::{fs, io::AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::{
    error::TransportError,
    frame_io::{
        FLAG_IS_RESPONSE, FLAG_IS_STREAM_CHUNK, FLAG_IS_STREAM_END, read_frame, send_error_frame,
        write_frame,
    },
};

/// In-flight byte ceiling enforced by the sender. When the most-recent ack
/// offset trails the most-recent sent offset by more than this many bytes,
/// `send_chunk` blocks awaiting the next ack frame from the receiver.
///
/// 4 MiB is large enough to absorb a few RTTs of pipelined chunks at typical
/// snapshot chunk sizes (256 KiB) without saturating QUIC's flow-control
/// window, but small enough that a stalled receiver pauses the sender within
/// a single CWND.
pub const MAX_INFLIGHT_BYTES: u64 = 4 * 1024 * 1024;

/// Filename prefix for staging files (`staging-{index:020}.snap.staged{,.partial}`).
pub const STAGING_FILENAME_PREFIX: &str = "staging-";

/// Filename suffix for in-progress staging files. Renamed to
/// [`STAGING_FILENAME_SUFFIX_FINAL`] on successful footer + CRC validation.
pub const STAGING_FILENAME_SUFFIX_PARTIAL: &str = ".snap.staged.partial";

/// Filename suffix for finalized staging files — the install path consumes
/// these via the receiver's
/// [`SnapshotPersister::list_staged`](https://docs.rs/inferadb-ledger-raft)
/// equivalent.
pub const STAGING_FILENAME_SUFFIX_FINAL: &str = ".snap.staged";

/// Errors surfaced by the snapshot sub-protocol.
///
/// Each arm carries enough context for the caller to decide whether to retry
/// the upload (transport hiccup) or escalate (CRC mismatch indicating a
/// corrupt sender). Recoverable transport errors are not auto-retried inside
/// the sub-protocol — Raft's heartbeat replicator will re-emit
/// `Action::SendSnapshot` on the next cycle.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SnapshotStreamError {
    /// QUIC-level transport failure: the bidi stream couldn't be opened, or
    /// a frame read/write failed.
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),

    /// Failed to encode an outbound frame's payload via postcard. Indicates
    /// an internally malformed wire type — never happens in practice.
    #[error("encode error: {0}")]
    Encode(String),

    /// Failed to decode an inbound payload via postcard. Indicates the peer
    /// sent malformed bytes.
    #[error("decode error: {0}")]
    Decode(String),

    /// The peer closed the bidi stream before the protocol reached a
    /// terminal state. For the sender this means no terminal ack arrived;
    /// for the receiver this means no footer arrived.
    #[error("peer closed stream early: {reason}")]
    UnexpectedClose {
        /// Diagnostic about what state the protocol was in when the close
        /// was observed.
        reason: &'static str,
    },

    /// A chunk's offset did not match the receiver's expected position.
    /// Either the sender re-ordered chunks (a bug) or the wire was corrupted
    /// in transit (QUIC should rule this out at the segment layer).
    #[error("offset mismatch: expected {expected}, got {actual}")]
    OffsetMismatch {
        /// Offset the receiver expected based on bytes accepted so far.
        expected: u64,
        /// Offset the inbound chunk claimed.
        actual: u64,
    },

    /// The footer's CRC did not match the running checksum the receiver
    /// computed. Either the sender's CRC is wrong, or some chunk was
    /// corrupted between the sender's CRC update and the wire (QUIC should
    /// also rule this out).
    #[error("crc mismatch: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch {
        /// CRC the sender claimed in the footer.
        expected: u32,
        /// CRC the receiver computed from the data chunks it accepted.
        actual: u32,
    },

    /// The footer's `final_byte_count` did not match the bytes the receiver
    /// accepted. Indicates a sender bookkeeping bug.
    #[error("byte count mismatch: expected {expected}, got {actual}")]
    ByteCountMismatch {
        /// Total bytes the sender claimed in the footer.
        expected: u64,
        /// Bytes the receiver actually wrote to the staging file.
        actual: u64,
    },

    /// The payload arrived in the wrong order: a non-header first chunk, a
    /// data chunk after a footer, etc.
    #[error("out-of-order chunk: {detail}")]
    OutOfOrder {
        /// Specific violation observed (hard-coded strings for greppability).
        detail: &'static str,
    },

    /// I/O error writing the staging file or renaming on success.
    #[error("staging io error: {0}")]
    Io(#[from] std::io::Error),

    /// The receiver was cancelled (server shutdown, peer reset).
    #[error("snapshot receive cancelled")]
    Cancelled,

    /// The sender received a terminal `WireError` from the peer instead of
    /// an ack — the receiver rejected the snapshot for a non-protocol
    /// reason (e.g., scope mismatch, term too low).
    #[error("receiver returned wire error: {0:?}")]
    WireError(WireError),
}

// ---------------------------------------------------------------------------
// Sender
// ---------------------------------------------------------------------------

/// Sender side of the snapshot streaming sub-protocol.
///
/// Construct via [`SnapshotSender::open`], then call
/// [`SnapshotSender::send_chunk`] for each data chunk and finish with
/// [`SnapshotSender::finish`]. The sender buffers nothing internally — each
/// `send_chunk` either writes to the wire immediately or blocks awaiting an
/// ack when the in-flight ceiling is reached.
pub struct SnapshotSender {
    send: SendStream,
    recv: RecvStream,
    request_id: [u8; 16],
    bytes_sent: u64,
    last_ack_offset: u64,
    crc: crc32fast::Hasher,
}

impl std::fmt::Debug for SnapshotSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotSender")
            .field("request_id", &self.request_id)
            .field("bytes_sent", &self.bytes_sent)
            .field("last_ack_offset", &self.last_ack_offset)
            .finish_non_exhaustive()
    }
}

impl SnapshotSender {
    /// Open a snapshot stream against `connection` and write the initial
    /// header frame.
    ///
    /// Returns a sender ready for [`SnapshotSender::send_chunk`] calls.
    /// The bidi stream's send and recv halves are kept by the sender for
    /// the lifetime of the upload — receive is used solely for backpressure
    /// acks.
    ///
    /// # Errors
    ///
    /// * [`SnapshotStreamError::Transport`] if the bidi stream cannot be opened.
    /// * [`SnapshotStreamError::Encode`] if the header cannot be postcard-encoded (would only
    ///   happen for a malformed `InstallSnapshotHeader`).
    /// * [`SnapshotStreamError::Transport`] if the header frame cannot be written.
    pub async fn open(
        connection: &Connection,
        request_id: [u8; 16],
        header: InstallSnapshotHeader,
    ) -> Result<Self, SnapshotStreamError> {
        let (mut send, recv) = connection.open_bi().await.map_err(|err| {
            SnapshotStreamError::Transport(TransportError::AuthHandshakeFailed {
                reason: format!("open_bi: {err}"),
            })
        })?;

        // Encode header as `InstallSnapshotChunk { Header(...) }`.
        let chunk = InstallSnapshotChunk { payload: Some(InstallSnapshotPayload::Header(header)) };
        let payload = encode_payload(&chunk)?;
        let frame = build_request_frame(request_id, payload, 0u8)?;
        write_frame(&mut send, &frame).await?;

        Ok(Self {
            send,
            recv,
            request_id,
            bytes_sent: 0,
            last_ack_offset: 0,
            crc: crc32fast::Hasher::new(),
        })
    }

    /// Send a single data chunk.
    ///
    /// Encodes the bytes as `InstallSnapshotChunk { Data(bytes) }`, writes
    /// it as a frame with the stream-chunk flag set, and updates the local
    /// CRC + bytes-sent counters. If the in-flight ceiling
    /// ([`MAX_INFLIGHT_BYTES`]) would be exceeded, blocks awaiting ack
    /// frames from the receiver until the ceiling is satisfied.
    ///
    /// # Errors
    ///
    /// * [`SnapshotStreamError::Encode`] if encoding the chunk fails.
    /// * [`SnapshotStreamError::Transport`] if writing the frame fails.
    /// * [`SnapshotStreamError::UnexpectedClose`] if the recv side is closed while waiting for an
    ///   ack.
    /// * [`SnapshotStreamError::Decode`] if the receiver sends a malformed ack payload.
    pub async fn send_chunk(&mut self, data: Bytes) -> Result<(), SnapshotStreamError> {
        // Backpressure: drain acks until we're under the in-flight ceiling.
        // Check BEFORE adding this chunk's size — the ceiling is total
        // unacked bytes at any point in time, and the next chunk pushes us
        // past it iff (bytes_sent_after - last_ack) > MAX_INFLIGHT_BYTES.
        let chunk_size = data.len() as u64;
        while self.bytes_sent.saturating_add(chunk_size).saturating_sub(self.last_ack_offset)
            > MAX_INFLIGHT_BYTES
        {
            self.await_ack().await?;
        }

        // Update CRC + bytes_sent BEFORE writing — keeps the local view
        // consistent with what the receiver will recompute.
        self.crc.update(&data);
        self.bytes_sent = self.bytes_sent.saturating_add(chunk_size);

        let chunk = InstallSnapshotChunk { payload: Some(InstallSnapshotPayload::Data(data)) };
        let payload = encode_payload(&chunk)?;
        let frame = build_request_frame(self.request_id, payload, FLAG_IS_STREAM_CHUNK)?;
        write_frame(&mut self.send, &frame).await?;
        Ok(())
    }

    /// Finalize the snapshot.
    ///
    /// Writes the footer frame with the running CRC and total byte count,
    /// closes the send side, then awaits the receiver's terminal ack frame
    /// (carrying the stream-end flag). On clean completion the staging file
    /// has been atomically renamed to `.staged` on the receiver side.
    ///
    /// # Errors
    ///
    /// * [`SnapshotStreamError::Encode`] / [`SnapshotStreamError::Transport`] if the footer frame
    ///   cannot be encoded or written.
    /// * [`SnapshotStreamError::UnexpectedClose`] if the recv side is closed before the terminal
    ///   ack arrives.
    /// * [`SnapshotStreamError::WireError`] if the receiver returned an error frame instead of an
    ///   ack.
    pub async fn finish(mut self) -> Result<(), SnapshotStreamError> {
        let crc = self.crc.clone().finalize();
        let footer =
            InstallSnapshotFooter { stream_crc32c: crc, final_byte_count: self.bytes_sent };
        let chunk = InstallSnapshotChunk { payload: Some(InstallSnapshotPayload::Footer(footer)) };
        let payload = encode_payload(&chunk)?;
        let frame = build_request_frame(self.request_id, payload, FLAG_IS_STREAM_END)?;
        write_frame(&mut self.send, &frame).await?;
        self.send.finish().map_err(|err| {
            SnapshotStreamError::Transport(TransportError::WriteFrame {
                reason: format!("finish send: {err}"),
            })
        })?;

        // Drain remaining acks until we see the terminal ack (or an error
        // frame). Acks lag behind sends; we may receive several
        // intermediate acks before the terminal one.
        loop {
            let ack_frame = read_frame(&mut self.recv).await?;
            if ack_frame.header.error_code != 0 {
                let err: WireError = postcard::from_bytes(&ack_frame.payload)
                    .map_err(|e| SnapshotStreamError::Decode(e.to_string()))?;
                return Err(SnapshotStreamError::WireError(err));
            }
            // Decode ack but otherwise ignore intermediate offsets — the
            // sender has already flushed everything it intended to send.
            let _ack: InstallSnapshotAck = postcard::from_bytes(&ack_frame.payload)
                .map_err(|e| SnapshotStreamError::Decode(e.to_string()))?;
            if ack_frame.header.flags & FLAG_IS_STREAM_END != 0 {
                return Ok(());
            }
        }
    }

    /// Read one ack frame from the recv side and update `last_ack_offset`.
    /// Used by `send_chunk`'s backpressure loop.
    async fn await_ack(&mut self) -> Result<(), SnapshotStreamError> {
        let frame = read_frame(&mut self.recv).await?;
        if frame.header.error_code != 0 {
            let err: WireError = postcard::from_bytes(&frame.payload)
                .map_err(|e| SnapshotStreamError::Decode(e.to_string()))?;
            return Err(SnapshotStreamError::WireError(err));
        }
        let ack: InstallSnapshotAck = postcard::from_bytes(&frame.payload)
            .map_err(|e| SnapshotStreamError::Decode(e.to_string()))?;
        // Ack offsets are monotonically non-decreasing; ignore stale acks
        // rather than treat them as protocol errors.
        if ack.offset_received > self.last_ack_offset {
            self.last_ack_offset = ack.offset_received;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Receiver
// ---------------------------------------------------------------------------

/// Receiver-side staging-file driver.
///
/// Constructed by the dispatch loop (see [`run_snapshot_receive_loop`]) once
/// the inbound header frame has been parsed. The receiver writes each data
/// chunk to a `.partial` staging file, sends ack frames back on the bidi
/// stream's send side, and atomically renames on footer + CRC validation.
pub struct SnapshotReceiver {
    file: fs::File,
    partial_path: PathBuf,
    final_path: PathBuf,
    bytes_received: u64,
    crc: crc32fast::Hasher,
}

impl std::fmt::Debug for SnapshotReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotReceiver")
            .field("partial_path", &self.partial_path)
            .field("final_path", &self.final_path)
            .field("bytes_received", &self.bytes_received)
            .finish_non_exhaustive()
    }
}

impl SnapshotReceiver {
    /// Open a fresh `.partial` staging file under `staging_dir` derived
    /// from `header.snapshot_index`.
    ///
    /// Returns a receiver ready for [`SnapshotReceiver::write_chunk`]
    /// calls. The staging directory is created if it does not exist (the
    /// install path can be invoked before any local snapshot has been
    /// written).
    ///
    /// # Errors
    ///
    /// [`SnapshotStreamError::Io`] if the directory cannot be created or
    /// the staging file cannot be opened for write.
    pub async fn open(
        staging_dir: PathBuf,
        header: &InstallSnapshotHeader,
    ) -> Result<Self, SnapshotStreamError> {
        fs::create_dir_all(&staging_dir).await?;

        let stem = format!("{STAGING_FILENAME_PREFIX}{:020}", header.snapshot_index);
        let partial_path = staging_dir.join(format!("{stem}{STAGING_FILENAME_SUFFIX_PARTIAL}"));
        let final_path = staging_dir.join(format!("{stem}{STAGING_FILENAME_SUFFIX_FINAL}"));

        // Truncate any prior `.partial` for this index — a previous attempt
        // crashed mid-stream. We trust `cleanup_partial_snapshots` ran at
        // startup, but be defensive against same-process retries.
        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&partial_path)
            .await?;

        Ok(Self {
            file,
            partial_path,
            final_path,
            bytes_received: 0,
            crc: crc32fast::Hasher::new(),
        })
    }

    /// Append a data chunk to the staging file.
    ///
    /// Verifies the chunk's offset matches the running `bytes_received`,
    /// writes the data, and updates the running CRC.
    ///
    /// # Errors
    ///
    /// * [`SnapshotStreamError::OffsetMismatch`] if the offset is wrong.
    /// * [`SnapshotStreamError::Io`] if the write fails.
    pub async fn write_chunk(
        &mut self,
        offset: u64,
        data: &[u8],
    ) -> Result<(), SnapshotStreamError> {
        if offset != self.bytes_received {
            return Err(SnapshotStreamError::OffsetMismatch {
                expected: self.bytes_received,
                actual: offset,
            });
        }
        self.file.write_all(data).await?;
        self.crc.update(data);
        self.bytes_received = self.bytes_received.saturating_add(data.len() as u64);
        Ok(())
    }

    /// Validate the footer + atomic-rename the staging file.
    ///
    /// 1. Asserts `footer.final_byte_count == bytes_received`.
    /// 2. Asserts `footer.stream_crc32c == crc.finalize()`.
    /// 3. Calls `sync_all` then drops the file handle.
    /// 4. Atomic rename `.partial` → `.staged`.
    ///
    /// On error the `.partial` file is left in place — the next start-up
    /// scan ([`cleanup_partial_snapshots`]) will delete it.
    ///
    /// Returns the final `.staged` path on success.
    ///
    /// # Errors
    ///
    /// * [`SnapshotStreamError::ByteCountMismatch`]
    /// * [`SnapshotStreamError::CrcMismatch`]
    /// * [`SnapshotStreamError::Io`]
    pub async fn finish(
        self,
        footer: InstallSnapshotFooter,
    ) -> Result<PathBuf, SnapshotStreamError> {
        if footer.final_byte_count != self.bytes_received {
            return Err(SnapshotStreamError::ByteCountMismatch {
                expected: footer.final_byte_count,
                actual: self.bytes_received,
            });
        }
        let computed_crc = self.crc.clone().finalize();
        if footer.stream_crc32c != computed_crc {
            return Err(SnapshotStreamError::CrcMismatch {
                expected: footer.stream_crc32c,
                actual: computed_crc,
            });
        }
        self.file.sync_all().await?;
        // Drop the file handle explicitly before renaming — Windows would
        // reject the rename otherwise; on Unix it's harmless.
        drop(self.file);
        fs::rename(&self.partial_path, &self.final_path).await?;
        Ok(self.final_path)
    }

    /// Bytes accepted on the staging file so far. Used by the dispatch
    /// loop to populate ack frames.
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received
    }
}

// ---------------------------------------------------------------------------
// Dispatcher integration
// ---------------------------------------------------------------------------

/// Server-side hook a dispatcher exposes to participate in the snapshot
/// sub-protocol.
///
/// The `Dispatcher` trait carries an optional [`SnapshotHandler`]
/// implementation; a dispatcher that returns `Some` from
/// [`crate::dispatcher::Dispatcher::snapshot_handler`] will receive snapshot
/// streams routed through [`run_snapshot_receive_loop`]. Implementations
/// own scope-to-directory mapping (per-org vs per-vault staging dirs) and
/// the post-rename hand-off into the apply pipeline.
///
/// A bare `Dispatcher` (no Raft transport) returns `None` from
/// `snapshot_handler` and the server replies `ErrorCode::Unimplemented` to
/// any inbound snapshot stream.
pub trait SnapshotHandler: Send + Sync {
    /// Resolve the staging directory for the snapshot scope encoded in
    /// `header`. The directory is created by [`SnapshotReceiver::open`] if
    /// it does not exist.
    fn staging_dir(&self, header: &InstallSnapshotHeader) -> PathBuf;

    /// Hook called once after the staging file is renamed to `.staged`.
    /// Implementations should kick off the actual Raft snapshot install
    /// (decrypt + verify + apply) from this path. The hook is fire-and-
    /// forget — the dispatch loop has already sent the terminal ack to
    /// the sender and closed the bidi stream.
    fn on_snapshot_complete(&self, header: &InstallSnapshotHeader, staged_path: &Path);
}

/// Drive a server-side snapshot receive loop on `(send, recv)`.
///
/// Called from `crate::server::dispatch::dispatch_stream` when an inbound
/// frame's opcode is [`OPCODE_INSTALL_SNAPSHOT_STREAM`]. The header has
/// already been parsed off `frame_payload` by the dispatcher; this function
/// owns the rest of the protocol.
///
/// 1. Open a [`SnapshotReceiver`] under the handler-supplied staging dir.
/// 2. Loop reading frames; each one decodes to an [`InstallSnapshotPayload::Data`] or
///    [`InstallSnapshotPayload::Footer`].
/// 3. After every committed data chunk, write an ack frame back.
/// 4. On footer + CRC validation, atomic rename, send terminal ack frame, close send side, invoke
///    `handler.on_snapshot_complete`.
/// 5. On any error mid-stream, send a [`WireError`] frame and leave the `.partial` file for crash
///    recovery to clean up.
///
/// `cancel` is the per-stream child cancellation token from the dispatch
/// loop. Cancellation drops the partial file in place — the same recovery
/// path handles it.
pub async fn run_snapshot_receive_loop<H: SnapshotHandler + ?Sized>(
    mut send: SendStream,
    mut recv: RecvStream,
    request_id: [u8; 16],
    header: InstallSnapshotHeader,
    handler: &H,
    cancel: CancellationToken,
) {
    let staging_dir = handler.staging_dir(&header);

    let mut receiver = match SnapshotReceiver::open(staging_dir, &header).await {
        Ok(r) => r,
        Err(err) => {
            tracing::warn!(error = %err, "snapshot_stream: open staging file failed");
            send_error_frame(
                &mut send,
                request_id,
                WireError::new(ErrorCode::Internal, format!("open staging: {err}")),
            )
            .await;
            return;
        },
    };

    let outcome: Result<InstallSnapshotFooter, SnapshotStreamError> = loop {
        let frame_res = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                tracing::debug!("snapshot_stream: receive cancelled");
                break Err(SnapshotStreamError::Cancelled);
            }
            res = read_frame(&mut recv) => res,
        };
        let frame = match frame_res {
            Ok(f) => f,
            Err(err) => {
                break Err(SnapshotStreamError::Transport(err));
            },
        };

        // Decode the chunk envelope.
        let chunk: InstallSnapshotChunk = match postcard::from_bytes(&frame.payload) {
            Ok(c) => c,
            Err(e) => {
                break Err(SnapshotStreamError::Decode(e.to_string()));
            },
        };
        let Some(payload) = chunk.payload else {
            break Err(SnapshotStreamError::OutOfOrder { detail: "chunk missing payload" });
        };

        match payload {
            InstallSnapshotPayload::Header(_) => {
                break Err(SnapshotStreamError::OutOfOrder { detail: "duplicate header" });
            },
            InstallSnapshotPayload::Data(data) => {
                let offset = receiver.bytes_received();
                if let Err(err) = receiver.write_chunk(offset, &data).await {
                    break Err(err);
                }
                // Emit an ack frame — the sender uses it for backpressure.
                if let Err(err) = write_ack_frame(
                    &mut send,
                    request_id,
                    receiver.bytes_received(),
                    // terminal
                    false,
                )
                .await
                {
                    break Err(SnapshotStreamError::Transport(err));
                }
            },
            InstallSnapshotPayload::Footer(footer) => {
                break Ok(footer);
            },
        }
    };

    match outcome {
        Ok(footer) => match receiver.finish(footer).await {
            Ok(staged_path) => {
                // Send terminal ack BEFORE invoking the handler so the
                // sender can release its bidi stream promptly. The handler
                // hook is fire-and-forget — its work proceeds on the apply
                // pipeline asynchronously.
                // Final ack: offset is irrelevant on the terminal frame
                // (the sender uses only the stream-end flag to detect
                // completion); pass 0 as a placeholder.
                if let Err(err) = write_ack_frame(&mut send, request_id, 0, true).await {
                    tracing::debug!(error = %err, "snapshot_stream: terminal ack write failed");
                }
                let _ = send.finish();
                handler.on_snapshot_complete(&header, &staged_path);
            },
            Err(err) => {
                tracing::warn!(error = %err, "snapshot_stream: footer validation failed");
                send_error_frame(&mut send, request_id, snapshot_err_to_wire(&err)).await;
            },
        },
        Err(err) => {
            tracing::warn!(error = %err, "snapshot_stream: receive failed");
            send_error_frame(&mut send, request_id, snapshot_err_to_wire(&err)).await;
        },
    }
}

// ---------------------------------------------------------------------------
// Crash recovery
// ---------------------------------------------------------------------------

/// Delete every `*.partial` file under `staging_dir`.
///
/// Called at startup before any Raft engines come up — `.partial` files
/// represent in-flight snapshots that crashed mid-stream and cannot be
/// resumed (the sender's CRC state is gone). The leader's heartbeat
/// replicator re-emits `Action::SendSnapshot` on the next cycle, so
/// dropping these files is the right move; resuming would require
/// replaying CRC state, which the sub-protocol does not persist.
///
/// Returns the count of files deleted. Missing directory is treated as
/// success (zero deletions); other I/O errors propagate.
///
/// # Errors
///
/// [`std::io::Error`] if reading the directory or deleting a specific file
/// fails (other than the directory being absent).
pub async fn cleanup_partial_snapshots(staging_dir: &Path) -> std::io::Result<usize> {
    let mut count = 0usize;
    let mut entries = match fs::read_dir(staging_dir).await {
        Ok(e) => e,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else { continue };
        if name.ends_with(STAGING_FILENAME_SUFFIX_PARTIAL) {
            match fs::remove_file(&path).await {
                Ok(()) => {
                    tracing::info!(path = %path.display(), "removed partial snapshot file");
                    count = count.saturating_add(1);
                },
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    // Race against another concurrent cleanup — fine.
                },
                Err(err) => return Err(err),
            }
        }
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// Frame helpers (private)
// ---------------------------------------------------------------------------

/// Encode an [`InstallSnapshotChunk`] into a `Bytes` payload. Wraps the
/// postcard encode error in a typed `SnapshotStreamError::Encode`.
fn encode_payload(chunk: &InstallSnapshotChunk) -> Result<Bytes, SnapshotStreamError> {
    let bytes =
        postcard::to_allocvec(chunk).map_err(|e| SnapshotStreamError::Encode(e.to_string()))?;
    Ok(Bytes::from(bytes))
}

/// Build a client→server (request) frame on the snapshot stream.
fn build_request_frame(
    request_id: [u8; 16],
    payload: Bytes,
    extra_flags: u8,
) -> Result<Frame, SnapshotStreamError> {
    let payload_length = u32::try_from(payload.len()).map_err(|_| {
        SnapshotStreamError::Encode("snapshot frame payload exceeds u32::MAX".into())
    })?;
    let mut header = FrameHeader::default();
    header.request_id = request_id;
    header.opcode = OPCODE_INSTALL_SNAPSHOT_STREAM;
    header.version = CURRENT_PROTOCOL_VERSION;
    header.flags = extra_flags;
    header.payload_length = payload_length;
    Ok(Frame { header, payload })
}

/// Write a server→client ack frame. Terminal acks carry the stream-end
/// flag in addition to the response flag.
async fn write_ack_frame(
    send: &mut SendStream,
    request_id: [u8; 16],
    offset_received: u64,
    terminal: bool,
) -> Result<(), TransportError> {
    let ack = InstallSnapshotAck { offset_received };
    let payload_vec = postcard::to_allocvec(&ack)
        .map_err(|e| TransportError::WriteFrame { reason: format!("encode ack: {e}") })?;
    let payload = Bytes::from(payload_vec);
    let payload_length = u32::try_from(payload.len()).map_err(|_| TransportError::WriteFrame {
        reason: "ack payload exceeds u32::MAX".into(),
    })?;

    let mut flags = FLAG_IS_RESPONSE;
    if terminal {
        flags |= FLAG_IS_STREAM_END;
    } else {
        flags |= FLAG_IS_STREAM_CHUNK;
    }

    let mut header = FrameHeader::default();
    header.request_id = request_id;
    header.opcode = OPCODE_INSTALL_SNAPSHOT_STREAM;
    header.version = CURRENT_PROTOCOL_VERSION;
    header.flags = flags;
    header.payload_length = payload_length;
    let frame = Frame { header, payload };
    write_frame(send, &frame).await
}

/// Map a [`SnapshotStreamError`] to a [`WireError`] for transmission to the
/// sender as a terminal error frame. Non-protocol errors map to `Internal`;
/// protocol violations map to `InvalidArgument`.
fn snapshot_err_to_wire(err: &SnapshotStreamError) -> WireError {
    match err {
        SnapshotStreamError::OffsetMismatch { .. }
        | SnapshotStreamError::CrcMismatch { .. }
        | SnapshotStreamError::ByteCountMismatch { .. }
        | SnapshotStreamError::OutOfOrder { .. }
        | SnapshotStreamError::Decode(_) => WireError {
            code: ErrorCode::InvalidArgument,
            message: format!("snapshot_stream: {err}"),
            retryable: false,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "leader will re-emit Action::SendSnapshot on next heartbeat".into(),
        },
        SnapshotStreamError::Cancelled => WireError {
            code: ErrorCode::Internal,
            message: "snapshot_stream: receive cancelled".into(),
            retryable: true,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "retry snapshot install".into(),
        },
        _ => WireError {
            code: ErrorCode::Internal,
            message: format!("snapshot_stream: {err}"),
            retryable: true,
            retry_after_ms: 0,
            context: Default::default(),
            suggested_action: "retry snapshot install".into(),
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use bytes::Bytes;
    use inferadb_ledger_wire::services::raft::{
        InstallSnapshotHeader, InstallSnapshotScope, InstallSnapshotVaultScope,
    };
    use tempfile::TempDir;
    use tokio::time::{Duration, timeout};

    use super::*;

    fn sample_header(snapshot_index: u64, total_bytes: u64) -> InstallSnapshotHeader {
        InstallSnapshotHeader {
            leader_term: 1,
            leader_id: 1,
            scope: Some(InstallSnapshotScope::Vault(InstallSnapshotVaultScope {
                region: "us-east-va".to_owned(),
                organization_id: 1,
                vault_id: 1,
            })),
            snapshot_index,
            snapshot_term: 1,
            total_bytes,
            chunk_size_bytes: 256 * 1024,
            lsnp_version: "LSNP-v2".to_owned(),
        }
    }

    /// Receiver: write-chunks-in-order then footer; assert the staged file
    /// exists with the expected bytes and the `.partial` file is gone.
    #[tokio::test]
    async fn receiver_atomic_rename_on_success() {
        let dir = TempDir::new().unwrap();
        let header = sample_header(1000, 4);
        let mut receiver = SnapshotReceiver::open(dir.path().to_path_buf(), &header).await.unwrap();
        receiver.write_chunk(0, b"ab").await.unwrap();
        receiver.write_chunk(2, b"cd").await.unwrap();

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(b"abcd");
        let crc = hasher.finalize();

        let footer = InstallSnapshotFooter { stream_crc32c: crc, final_byte_count: 4 };
        let staged = receiver.finish(footer).await.unwrap();

        assert!(staged.exists(), "staged path must exist");
        assert!(staged.to_string_lossy().ends_with(".snap.staged"));
        assert!(!staged.to_string_lossy().ends_with(".partial"));

        // The .partial file should be gone.
        let partial =
            staged.with_extension("staged.partial").file_name().map(|n| n.to_owned()).unwrap();
        let partial_path = dir.path().join(&partial);
        assert!(!partial_path.exists(), "partial file must not exist after rename");

        let bytes = tokio::fs::read(&staged).await.unwrap();
        assert_eq!(&bytes[..], b"abcd");
    }

    /// Receiver: a chunk with the wrong offset must fail.
    #[tokio::test]
    async fn receiver_validates_offset_ordering() {
        let dir = TempDir::new().unwrap();
        let header = sample_header(2000, 4);
        let mut receiver = SnapshotReceiver::open(dir.path().to_path_buf(), &header).await.unwrap();
        receiver.write_chunk(0, b"ab").await.unwrap();
        // Wrong offset (should be 2, not 99).
        let err = receiver.write_chunk(99, b"cd").await.unwrap_err();
        match err {
            SnapshotStreamError::OffsetMismatch { expected, actual } => {
                assert_eq!(expected, 2);
                assert_eq!(actual, 99);
            },
            other => panic!("expected OffsetMismatch, got {other:?}"),
        }
    }

    /// Receiver: footer with wrong CRC must fail and leave the .partial
    /// file in place for the cleanup-on-startup path to delete.
    #[tokio::test]
    async fn receiver_validates_crc() {
        let dir = TempDir::new().unwrap();
        let header = sample_header(3000, 2);
        let mut receiver = SnapshotReceiver::open(dir.path().to_path_buf(), &header).await.unwrap();
        receiver.write_chunk(0, b"xy").await.unwrap();

        // Construct a deliberately-wrong CRC.
        let footer = InstallSnapshotFooter { stream_crc32c: 0xDEAD_BEEF, final_byte_count: 2 };
        let err = receiver.finish(footer).await.unwrap_err();
        match err {
            SnapshotStreamError::CrcMismatch { expected, actual } => {
                assert_eq!(expected, 0xDEAD_BEEF);
                assert_ne!(actual, 0xDEAD_BEEF);
            },
            other => panic!("expected CrcMismatch, got {other:?}"),
        }

        // .partial file must still exist (leftover for crash-recovery).
        let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().flatten().collect();
        let partials: Vec<_> = entries
            .iter()
            .filter(|e| e.path().to_string_lossy().ends_with(STAGING_FILENAME_SUFFIX_PARTIAL))
            .collect();
        assert_eq!(partials.len(), 1, "exactly one .partial file must remain after CRC failure");
    }

    /// Receiver: byte-count mismatch in footer must fail and leave
    /// `.partial` in place.
    #[tokio::test]
    async fn receiver_validates_byte_count() {
        let dir = TempDir::new().unwrap();
        let header = sample_header(4000, 2);
        let mut receiver = SnapshotReceiver::open(dir.path().to_path_buf(), &header).await.unwrap();
        receiver.write_chunk(0, b"ab").await.unwrap();

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(b"ab");
        let crc = hasher.finalize();
        let footer = InstallSnapshotFooter { stream_crc32c: crc, final_byte_count: 99 };
        let err = receiver.finish(footer).await.unwrap_err();
        assert!(
            matches!(err, SnapshotStreamError::ByteCountMismatch { .. }),
            "expected ByteCountMismatch, got {err:?}",
        );
    }

    /// `cleanup_partial_snapshots`: deletes `.partial` files, leaves
    /// `.staged` and unrelated files alone.
    #[tokio::test]
    async fn cleanup_partial_snapshots_deletes_partial_files() {
        let dir = TempDir::new().unwrap();
        // Create a mix of files.
        tokio::fs::write(
            dir.path().join("staging-00000000000000000001.snap.staged.partial"),
            b"junk",
        )
        .await
        .unwrap();
        tokio::fs::write(
            dir.path().join("staging-00000000000000000002.snap.staged.partial"),
            b"junk",
        )
        .await
        .unwrap();
        tokio::fs::write(dir.path().join("staging-00000000000000000003.snap.staged"), b"good")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("README.txt"), b"unrelated").await.unwrap();

        let removed = cleanup_partial_snapshots(dir.path()).await.unwrap();
        assert_eq!(removed, 2, "expected exactly 2 partial files to be deleted");

        // Confirm: the .staged file and README.txt survive.
        assert!(dir.path().join("staging-00000000000000000003.snap.staged").exists());
        assert!(dir.path().join("README.txt").exists());
        assert!(
            !dir.path().join("staging-00000000000000000001.snap.staged.partial").exists(),
            ".partial #1 must be gone",
        );
        assert!(
            !dir.path().join("staging-00000000000000000002.snap.staged.partial").exists(),
            ".partial #2 must be gone",
        );
    }

    /// `cleanup_partial_snapshots` on a missing directory returns Ok(0).
    #[tokio::test]
    async fn cleanup_partial_snapshots_missing_dir_is_ok() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("does-not-exist");
        let removed = cleanup_partial_snapshots(&missing).await.unwrap();
        assert_eq!(removed, 0);
    }

    // -----------------------------------------------------------------------
    // End-to-end test: real quinn connection, sender + receiver round trip.
    // -----------------------------------------------------------------------

    use crate::tls;

    fn loopback_zero() -> SocketAddr {
        SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0)
    }

    fn build_test_endpoints() -> (quinn::Endpoint, quinn::Endpoint) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let chain = vec![cert_der];
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key_der);
        let server_crypto = tls::rustls_server_crypto(chain, key).expect("server crypto");
        let server_config = tls::server_config(server_crypto);
        let server = quinn::Endpoint::server(server_config, loopback_zero()).unwrap();

        let client_crypto = tls::rustls_client_crypto_skip_verify();
        let client_config = tls::client_config(client_crypto);
        let mut client = quinn::Endpoint::client(loopback_zero()).unwrap();
        client.set_default_client_config(client_config);

        (server, client)
    }

    /// End-to-end: open a real QUIC connection, run sender + receiver in
    /// parallel, ship 1 MiB of bytes, assert the staged file matches.
    #[tokio::test]
    async fn end_to_end_one_mib_round_trip() {
        let (server, client) = build_test_endpoints();
        let server_addr = server.local_addr().unwrap();

        // Generate 1 MiB of pseudo-random bytes.
        let payload: Vec<u8> = (0..1_048_576).map(|i| (i as u8).wrapping_mul(31)).collect();
        let payload_bytes = Bytes::from(payload.clone());
        let total_bytes = payload_bytes.len() as u64;

        // Spawn a tiny custom server that handles ONE bidi stream and
        // drives the receive loop directly. We don't go through
        // WireServer / Dispatcher because that machinery is exercised in
        // its own tests; here we want to focus on the snapshot
        // sub-protocol's end-to-end behavior.
        let staging_dir = TempDir::new().unwrap();
        let staging_path = staging_dir.path().to_path_buf();

        struct TestHandler {
            staging: PathBuf,
            done: tokio::sync::mpsc::UnboundedSender<PathBuf>,
        }
        impl SnapshotHandler for TestHandler {
            fn staging_dir(&self, _header: &InstallSnapshotHeader) -> PathBuf {
                self.staging.clone()
            }
            fn on_snapshot_complete(&self, _header: &InstallSnapshotHeader, staged_path: &Path) {
                let _ = self.done.send(staged_path.to_path_buf());
            }
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let handler = Arc::new(TestHandler { staging: staging_path.clone(), done: tx });

        let server_task = {
            let handler = handler.clone();
            tokio::spawn(async move {
                let incoming = server.accept().await.expect("incoming");
                let connecting = incoming.accept().expect("accept");
                let conn = connecting.await.expect("handshake");
                let (send, mut recv) = conn.accept_bi().await.expect("accept_bi");

                // Read the header frame ourselves (the dispatch loop would
                // do this; here we bypass it for the test).
                let frame = read_frame(&mut recv).await.expect("read header frame");
                assert_eq!(frame.header.opcode, OPCODE_INSTALL_SNAPSHOT_STREAM);
                let chunk: InstallSnapshotChunk =
                    postcard::from_bytes(&frame.payload).expect("decode header chunk");
                let header = match chunk.payload.expect("payload present") {
                    InstallSnapshotPayload::Header(h) => h,
                    other => panic!("expected header, got {other:?}"),
                };

                run_snapshot_receive_loop(
                    send,
                    recv,
                    frame.header.request_id,
                    header,
                    handler.as_ref(),
                    CancellationToken::new(),
                )
                .await;

                // Keep the connection alive until the peer closes — the
                // sender's terminal ack needs to be flushed off the wire
                // before the connection drops, and dropping `conn` here
                // would tear down the QUIC stream prematurely.
                let _ = conn.closed().await;
                drop(server);
            })
        };

        // Driver the sender from the test task.
        let connection =
            client.connect(server_addr, "localhost").expect("connect").await.expect("handshake");

        let request_id = [0xABu8; 16];
        let mut sender =
            SnapshotSender::open(&connection, request_id, sample_header(5000, total_bytes))
                .await
                .expect("open sender");

        // Send in 256 KiB chunks.
        const CHUNK: usize = 256 * 1024;
        let mut offset = 0usize;
        while offset < payload.len() {
            let end = (offset + CHUNK).min(payload.len());
            let slice = Bytes::copy_from_slice(&payload[offset..end]);
            sender.send_chunk(slice).await.expect("send_chunk");
            offset = end;
        }
        timeout(Duration::from_secs(10), sender.finish())
            .await
            .expect("not timeout")
            .expect("finish");

        // Wait for handler hook.
        let staged_path = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("hook fires within timeout")
            .expect("hook delivers path");

        let actual = tokio::fs::read(&staged_path).await.unwrap();
        assert_eq!(actual.len(), payload.len(), "staged file size matches input");
        assert_eq!(actual, payload, "staged file bytes match input");

        connection.close(0u32.into(), b"test done");
        client.wait_idle().await;
        let _ = server_task.await;
    }

    /// Sender: confirm backpressure stalls when ack offsets fall behind.
    ///
    /// We have the receiver actively drain incoming chunk frames (so QUIC
    /// flow-control doesn't kick in and confound the test) but never write
    /// ack frames — exercising the sender's app-level
    /// `MAX_INFLIGHT_BYTES` ceiling specifically.
    #[tokio::test]
    async fn sender_chunks_with_backpressure() {
        let (server, client) = build_test_endpoints();
        let server_addr = server.local_addr().unwrap();

        // Receiver: drain everything off `recv` to keep QUIC's flow-control
        // window open, but never write to `send` (no acks). The sender's
        // app-level backpressure must engage.
        let server_task = tokio::spawn(async move {
            let incoming = server.accept().await.unwrap();
            let connecting = incoming.accept().unwrap();
            let conn = connecting.await.unwrap();
            let (_send, mut recv) = conn.accept_bi().await.unwrap();
            loop {
                let mut buf = [0u8; 65_536];
                match recv.read(&mut buf).await {
                    Ok(Some(_n)) => {},
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        });

        let connection = client.connect(server_addr, "localhost").unwrap().await.unwrap();

        let request_id = [0xCDu8; 16];
        // Sample header with chunk_size that hints at large chunks.
        let header = sample_header(7000, MAX_INFLIGHT_BYTES * 4);
        let mut sender = SnapshotSender::open(&connection, request_id, header).await.unwrap();

        // Send chunks in 256 KiB increments. The first ~16 chunks (= 4 MiB
        // of cumulative bytes_sent) should fit under MAX_INFLIGHT_BYTES.
        // The 17th must stall waiting for an ack.
        const CHUNK_SIZE: usize = 256 * 1024;
        let chunk = Bytes::from(vec![0x42u8; CHUNK_SIZE]);

        let mut sent = 0u64;
        // Send up to the ceiling. We allow a small headroom since
        // postcard adds a tiny per-frame overhead, not counted against
        // MAX_INFLIGHT_BYTES (which counts payload bytes only).
        while sent + (CHUNK_SIZE as u64) <= MAX_INFLIGHT_BYTES {
            timeout(Duration::from_secs(2), sender.send_chunk(chunk.clone()))
                .await
                .expect("under-ceiling send completes")
                .expect("send result ok");
            sent += CHUNK_SIZE as u64;
        }

        // Next chunk pushes us over MAX_INFLIGHT_BYTES — must stall
        // because the receiver never sent an ack frame.
        let stalled = timeout(Duration::from_millis(400), sender.send_chunk(chunk.clone())).await;
        assert!(stalled.is_err(), "over-ceiling chunk must stall awaiting ack");

        // Tear down. Drop the sender (its bidi stream halves drop with
        // it), close the connection, abort the server task.
        drop(sender);
        connection.close(0u32.into(), b"test done");
        server_task.abort();
        client.wait_idle().await;
    }
}
