//! Server discovery and selection.
//!
//! This module provides dynamic server discovery for the SDK, supporting:
//! - DNS domain resolution (for Kubernetes headless services)
//! - File-based server manifests (for static configurations)
//! - Latency-based server selection (prefer fastest servers)
//!
//! # Architecture
//!
//! ```text
//! ServerSource (Config)
//!       │
//!       ▼
//! ServerResolver (DNS/File resolution)
//!       │
//!       ▼
//! ServerSelector (Latency tracking + ordering)
//!       │
//!       ▼
//! ConnectionPool (Connection management)
//! ```

mod resolver;
mod selector;

pub use resolver::{DnsConfig, FileConfig, ResolvedServer, ServerResolver, ServerSource};
pub use selector::{SelectorStats, ServerSelector};
