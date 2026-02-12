//! gRPC service implementations for InferaDB Ledger.
//!
//! All services run behind WireGuard VPN. Authentication and authorization
//! are handled by Engine/Control services upstream. Ledger trusts all requests.
//!
//! ## Multi-Shard Support
//!
//! For multi-shard deployments, use [`MultiShardReadService`] and
//! [`MultiShardWriteService`] with a [`ShardResolver`] to route requests
//! to the correct shard.
//!
//! ## Request Forwarding
//!
//! When a namespace is on a remote shard, the `ForwardClient` can be used to
//! proxy requests to the correct node via gRPC.

mod admin;
mod discovery;
pub(crate) mod error_details;
mod forward_client;
mod health;
pub(crate) mod helpers;
pub(crate) mod metadata;
mod multi_shard_read;
mod multi_shard_write;
mod raft;
mod read;
pub mod shard_resolver;
mod write;

pub use admin::AdminServiceImpl;
pub use discovery::DiscoveryServiceImpl;
pub use forward_client::ForwardClient;
pub use health::HealthServiceImpl;
pub use multi_shard_read::MultiShardReadService;
pub use multi_shard_write::MultiShardWriteService;
pub use raft::{MultiShardRaftService, RaftServiceImpl};
pub use read::ReadServiceImpl;
pub use shard_resolver::{MultiShardResolver, ShardResolver};
pub use write::WriteServiceImpl;
