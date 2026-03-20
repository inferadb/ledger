//! Operation implementations for [`LedgerClient`](crate::LedgerClient).
//!
//! Each submodule provides an `impl LedgerClient` block with domain-specific methods.

mod app;
mod credential;
mod data;
mod events;
mod health;
mod invitation;
mod list;
mod onboarding;
mod organization;
mod schema;
mod token;
mod user;
mod vault;
mod verified_read;
