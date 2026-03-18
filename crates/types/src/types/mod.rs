//! Core type definitions for InferaDB Ledger.
//!
//! Defines identifier types (OrganizationId, OrganizationSlug, VaultId, VaultSlug, etc.),
//! block and transaction structures, operations, and conditions.

/// Block, transaction, entity, and relationship types.
mod block;
/// User credential types: passkeys, TOTP, recovery codes.
mod credentials;
/// Status and role enums for users, organizations, apps, and signing keys.
mod enums;
/// Identifier newtypes (internal IDs and external Snowflake slugs).
mod ids;
/// Geographic region type for data residency.
mod region;
/// Block retention policy types.
mod retention;
/// Per-organization resource accounting types.
mod usage;

pub use block::*;
pub use credentials::*;
pub use enums::*;
pub use ids::*;
pub use region::*;
pub use retention::*;
pub use usage::*;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)]
mod tests;
