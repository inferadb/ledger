//! Server-assigned sequence idempotency support.
//!
//! With server-assigned sequences, the client no longer tracks sequences.
//! The server assigns monotonically increasing sequence numbers at Raft
//! commit time using a UUID idempotency key for deduplication.
//!
//! # Idempotency Model
//!
//! The client sends a unique `idempotency_key` (UUID) with each write request.
//! The server:
//! - On first request: Commits the write, assigns a sequence, caches the result
//! - On retry with same key and payload: Returns cached result
//! - On retry with same key but different payload: Returns `IdempotencyKeyReused` error
//!
//! This module is intentionally minimal as all idempotency logic now lives
//! on the server side. The client simply generates UUIDs for each request.
