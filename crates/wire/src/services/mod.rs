//! Wire-protocol message types — the canonical source of truth for all RPC
//! request/response types. The single `define_protocol!` invocation in
//! `crates/wire-services/src/lib.rs` consumes these types and emits server
//! traits, client structs, dispatch fns, and the `OpCode` enum.
//!
//! Each submodule owns the types specific to one service. Cross-service
//! types live in [`shared`].
//!
//! Field-type conventions:
//!
//! 1. Byte payloads → [`bytes::Bytes`] (zero-copy at the codec boundary), not `Vec<u8>`.
//! 2. Maps → [`std::collections::BTreeMap`] (deterministic encoding required by postcard for
//!    content-addressable caching), not `HashMap`.
//! 3. Timestamps → `u64` UNIX nanoseconds, matching `FrameHeader::deadline_unix_nanos` in the wire
//!    transport.
//!
//! Optional and repeated fields follow standard Rust conventions: `Option<T>`
//! for nullable, `Vec<T>` for repeated.

#![allow(missing_docs)]

pub mod admin;
pub mod app;
pub mod discovery;
pub mod events;
pub mod health;
pub mod invitation;
pub mod organization;
pub mod raft;
pub mod read;
pub mod schema;
pub mod shared;
pub mod token;
pub mod user;
pub mod vault;
pub mod write;
