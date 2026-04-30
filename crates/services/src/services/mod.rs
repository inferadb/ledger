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
//! Cross-region and cross-leader requests are handled via redirect-only routing:
//! services return `Status::unavailable` with addressing metadata so the SDK can
//! retry against the correct node. No server-side request proxying.

mod admin;
mod admin_wire;
mod app;
mod app_wire;
pub(crate) mod auth_errors;
mod discovery;
mod discovery_wire;
pub(crate) mod error_classify;
pub(crate) mod error_details;
mod events;
mod events_wire;
mod health;
mod health_wire;
pub(crate) mod helpers;
mod invitation;
mod invitation_wire;
pub mod jwt_auth_verifier;
pub(crate) mod metadata;
mod organization;
mod organization_wire;
pub(crate) mod pagination;
#[cfg(any(test, feature = "permissive-wire-auth"))]
pub mod permissive_verifier;
mod read;
mod read_wire;
pub mod region_resolver;
mod schema_wire;
pub(crate) mod service_infra;
pub mod slug_resolver;
mod token;
mod token_wire;
pub(crate) mod tonic_compat;
mod user;
mod user_wire;
mod vault;
mod vault_wire;
pub mod wire_helpers;
mod write;
mod write_wire;

pub use admin::AdminService;
pub use app::AppService;
pub use discovery::DiscoveryService;
pub use events::{EventsService, VaultEventSource, VaultEventSources};
pub use health::HealthService;
pub use invitation::InvitationService;
pub use jwt_auth_verifier::JwtAuthVerifier;
pub use organization::OrganizationService;
pub use read::ReadService;
pub use region_resolver::{RegionResolver, RegionResolverService};
pub use schema_wire::SchemaServiceStub;
pub use slug_resolver::SlugResolver;
pub use token::TokenServiceImpl;
pub use user::UserService;
pub use vault::VaultService;
pub use write::WriteService;
