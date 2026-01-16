//! gRPC service implementations for InferaDB Ledger.
//!
//! All services run behind WireGuard VPN. Authentication and authorization
//! are handled by Engine/Control services upstream. Ledger trusts all requests.
//!
//! ## Multi-Shard Support
//!
//! For multi-shard deployments, use the `MultiShard*` service variants with a
//! `ShardResolver` to route requests to the correct shard.
//!
//! ## Request Forwarding
//!
//! When a namespace is on a remote shard, the `ForwardClient` can be used to
//! proxy requests to the correct node via gRPC.

mod admin;
mod discovery;
mod forward_client;
mod health;
mod multi_shard_read;
mod multi_shard_write;
mod raft;
mod read;
mod shard_resolver;
mod write;

pub use admin::AdminServiceImpl;
pub use discovery::DiscoveryServiceImpl;
pub use forward_client::ForwardClient;
pub use health::HealthServiceImpl;
pub use multi_shard_read::MultiShardReadService;
pub use multi_shard_write::MultiShardWriteService;
pub use raft::{MultiShardRaftService, RaftServiceImpl};
pub use read::ReadServiceImpl;
pub use shard_resolver::{
    MultiShardResolver, RemoteShardInfo, ResolveResult, ShardContext, ShardResolver,
    SingleShardResolver,
};
pub use write::WriteServiceImpl;
