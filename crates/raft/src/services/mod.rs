//! gRPC service implementations for InferaDB Ledger.
//!
//! All services run behind WireGuard VPN. Authentication and authorization
//! are handled by Engine/Control services upstream. Ledger trusts all requests.
//!
//! ## Multi-Region Support
//!
//! Every service is multi-region capable via [`RegionResolver`], which routes
//! requests to the correct region based on organization assignment. A
//! single-region deployment is simply a resolver with one region (GLOBAL).
//!
//! ## Request Forwarding
//!
//! When an organization is on a remote region, the [`ForwardClient`] proxies
//! requests to the correct node via gRPC.

mod admin;
mod discovery;
pub(crate) mod error_details;
mod events;
mod forward_client;
mod health;
pub(crate) mod helpers;
pub(crate) mod metadata;
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
pub use raft::RaftServiceImpl;
pub use read::ReadServiceImpl;
pub use region_resolver::{RegionResolver, RegionResolverImpl};
pub use slug_resolver::SlugResolver;
pub use write::WriteServiceImpl;
