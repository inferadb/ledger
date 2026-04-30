//! Shared frame read/write helpers used by the per-connection task
//! ([`crate::connection`]) and the per-stream dispatch loop
//! ([`crate::server::dispatch`]).
//!
//! These helpers exist so the auth-handshake path and the per-stream RPC
//! path encode framing the same way — and so a future fix to the on-wire
//! framing (e.g., padding rules, length-prefix changes) lives in one place.

use bytes::{Bytes, BytesMut};
use inferadb_ledger_wire::{
    ErrorCode, Frame, FrameHeader, HEADER_SIZE, WireError, peek_payload_length,
};
use quinn::{RecvStream, SendStream};

use crate::error::TransportError;

/// Bit-0 of `FrameHeader::flags` — set on every server→client response frame.
pub const FLAG_IS_RESPONSE: u8 = 0b0000_0001;

/// Bit-1 of `FrameHeader::flags` — set on every server→client stream chunk.
pub const FLAG_IS_STREAM_CHUNK: u8 = 0b0000_0010;

/// Bit-2 of `FrameHeader::flags` — set on the terminal frame of a stream.
pub const FLAG_IS_STREAM_END: u8 = 0b0000_0100;

/// Read exactly one frame off `recv`.
///
/// 1. Read [`HEADER_SIZE`] bytes.
/// 2. Use [`peek_payload_length`] to learn the declared payload size (this also validates version
///    and the DoS guard).
/// 3. Read the payload bytes.
/// 4. Decode the assembled buffer via [`Frame::decode`] for the full validation pass.
pub async fn read_frame(recv: &mut RecvStream) -> Result<Frame, TransportError> {
    let mut header_buf = [0u8; HEADER_SIZE];
    recv.read_exact(&mut header_buf)
        .await
        .map_err(|err| TransportError::ReadFrame { reason: format!("read header: {err}") })?;

    let payload_length = peek_payload_length(&header_buf).map_err(TransportError::Codec)?;

    let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload_length as usize);
    buf.extend_from_slice(&header_buf);
    if payload_length > 0 {
        let prev_len = buf.len();
        buf.resize(prev_len + payload_length as usize, 0);
        recv.read_exact(&mut buf[prev_len..])
            .await
            .map_err(|err| TransportError::ReadFrame { reason: format!("read payload: {err}") })?;
    }

    let mut bytes = buf.freeze();
    Frame::decode(&mut bytes).map_err(TransportError::Codec)
}

/// Encode `frame` into a buffer and write it in full to `send`.
pub async fn write_frame(send: &mut SendStream, frame: &Frame) -> Result<(), TransportError> {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + frame.payload.len());
    frame.encode(&mut buf).map_err(TransportError::Codec)?;
    send.write_all(&buf)
        .await
        .map_err(|err| TransportError::WriteFrame { reason: format!("write frame: {err}") })?;
    Ok(())
}

/// Encode a [`WireError`] into a response frame and best-effort write it
/// to `send`, then close the send side. Used on every error exit path —
/// any failure to write or close is logged at DEBUG and swallowed (the
/// peer either already gave up or will see the QUIC stream reset).
///
/// The frame is built with `FLAG_IS_RESPONSE`, the supplied `request_id`,
/// `error_code` taken from the [`WireError::code`] field, and a
/// postcard-encoded [`WireError`] as the payload. If postcard fails (which
/// would only happen for an internally malformed `WireError`), a fallback
/// minimal frame is sent with [`ErrorCode::Internal`] and an empty
/// `WireError` body — the wire-protocol invariant that `error_code != 0`
/// frames carry a payload is preserved.
pub(crate) async fn send_error_frame(send: &mut SendStream, request_id: [u8; 16], err: WireError) {
    let payload_bytes = match postcard::to_allocvec(&err) {
        Ok(v) => Bytes::from(v),
        Err(encode_err) => {
            tracing::error!(error = %encode_err, "failed to encode WireError; sending fallback");
            // Fall back to a tiny canned error.
            let fallback = WireError::new(ErrorCode::Internal, "encode error");
            match postcard::to_allocvec(&fallback) {
                Ok(v) => Bytes::from(v),
                Err(_) => {
                    // If we can't even encode the canned error there's nothing
                    // useful to send; reset the stream and bail.
                    tracing::error!("failed to encode fallback WireError; dropping stream");
                    return;
                },
            }
        },
    };
    let payload_length = u32::try_from(payload_bytes.len()).unwrap_or(u32::MAX);

    let mut header = FrameHeader::default();
    header.request_id = request_id;
    header.version = inferadb_ledger_wire::CURRENT_PROTOCOL_VERSION;
    header.flags = FLAG_IS_RESPONSE;
    header.error_code = err.code.as_u32();
    header.payload_length = payload_length;
    let frame = Frame { header, payload: payload_bytes };

    if let Err(write_err) = write_frame(send, &frame).await {
        tracing::debug!(error = %write_err, "failed to write error frame");
        return;
    }
    if let Err(finish_err) = send.finish() {
        tracing::debug!(error = %finish_err, "failed to finish error frame stream");
    }
}
