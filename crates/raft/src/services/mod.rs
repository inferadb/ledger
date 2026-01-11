//! gRPC service implementations for InferaDB Ledger.
//!
//! All services run behind WireGuard VPN. Authentication and authorization
//! are handled by Engine/Control services upstream. Ledger trusts all requests.

mod admin;
mod discovery;
mod health;
mod read;
mod write;

pub use admin::AdminServiceImpl;
pub use discovery::DiscoveryServiceImpl;
pub use health::HealthServiceImpl;
pub use read::ReadServiceImpl;
pub use write::WriteServiceImpl;
