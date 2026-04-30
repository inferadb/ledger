//! QUIC server: accept loop, per-connection auth, dispatch.
//!
//! [`accept::WireServer`] is the public entry point — `bind` to a UDP
//! address, hand it `Arc<V>` and `Arc<D>` for the verifier and dispatcher,
//! and the accept loop runs in the background. B.4 added the bind path and
//! address-validation gate; B.5 added the per-connection auth handshake +
//! revocation watchdog; B.6 added stream-level dispatch; B.7 added the
//! public [`accept::WireServer::shutdown`] API for graceful drain.

pub mod accept;
pub mod auth_cache;
pub mod dispatch;

pub use accept::{ShutdownOutcome, WireServer};
