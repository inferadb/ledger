//! gRPC service implementations for InferaDB Ledger.
//!
//! All services run behind WireGuard VPN. Authentication and authorization
//! are handled by Engine/Control services upstream. Ledger trusts all requests.
//!
//! ## Multi-Region Support
//!
//! For multi-region deployments, use [`MultiRegionReadService`] and
//! [`MultiRegionWriteService`] with a [`RegionResolver`] to route requests
//! to the correct region.
//!
//! ## Request Forwarding
//!
//! When an organization is on a remote region, the `ForwardClient` can be used to
//! proxy requests to the correct node via gRPC.

mod admin;
mod discovery;
pub(crate) mod error_details;
mod events;
mod forward_client;
mod health;
pub(crate) mod helpers;
pub(crate) mod metadata;
mod multi_region_read;
mod multi_region_write;
mod raft;
mod read;
pub mod region_resolver;
pub mod slug_resolver;
mod write;

pub use admin::AdminServiceImpl;
pub use discovery::DiscoveryServiceImpl;
pub use events::EventsServiceImpl;
pub use forward_client::ForwardClient;
pub use health::HealthServiceImpl;
pub use multi_region_read::MultiRegionReadService;
pub use multi_region_write::MultiRegionWriteService;
pub use raft::{MultiRegionRaftService, RaftServiceImpl};
pub use read::ReadServiceImpl;
pub use region_resolver::{MultiRegionResolver, RegionResolver};
pub use slug_resolver::SlugResolver;
pub use write::WriteServiceImpl;
