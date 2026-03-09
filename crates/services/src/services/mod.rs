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
mod app;
mod discovery;
pub(crate) mod error_details;
mod events;
mod forward_client;
mod health;
pub(crate) mod helpers;
pub(crate) mod metadata;
mod organization;
mod raft;
mod read;
pub mod region_resolver;
pub(crate) mod service_infra;
pub mod slug_resolver;
mod token;
mod user;
mod vault;
mod write;

pub use admin::AdminService;
pub use app::AppService;
pub use discovery::DiscoveryService;
pub use events::EventsService;
pub use forward_client::ForwardClient;
pub use health::HealthService;
pub use organization::OrganizationService;
pub use raft::RaftService;
pub use read::ReadService;
pub use region_resolver::{RegionResolver, RegionResolverService};
pub use slug_resolver::SlugResolver;
pub use token::TokenServiceImpl;
pub use user::UserService;
pub use vault::VaultService;
pub use write::WriteService;
