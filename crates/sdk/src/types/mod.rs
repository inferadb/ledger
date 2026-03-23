//! Domain-specific types for the SDK public API.
//!
//! Each submodule groups related types by service area:
//! - [`admin`]: Organizations, vaults, users, teams, health checks
//! - [`app`]: Applications, credentials, client assertions
//! - [`credential`]: Passkeys, TOTP, recovery codes
//! - [`events`]: Audit event queries and ingestion
//! - [`invitation`]: Organization invitations
//! - [`query`]: Entities, relationships, operations, pagination
//! - [`read`]: Read consistency levels and write results
//! - [`schema`]: Schema versioning and deployment
//! - [`streaming`]: Block announcement streams
//! - [`verified_read`]: Merkle proofs, block headers, chain proofs

pub mod admin;
pub mod app;
pub mod credential;
pub mod events;
pub mod invitation;
pub mod query;
pub mod read;
pub mod schema;
pub mod streaming;
pub mod verified_read;
