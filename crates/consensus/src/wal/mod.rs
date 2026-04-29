//! WAL implementations.

pub mod encrypted;
mod memory;
pub mod segmented;

pub use encrypted::EncryptedWalBackend;
pub use memory::InMemoryWalBackend;
pub use segmented::SegmentedWalBackend;

pub use crate::wal_backend::{WalBackend, WalError, WalFrame};
