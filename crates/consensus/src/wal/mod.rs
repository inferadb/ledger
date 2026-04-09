//! WAL implementations.

pub mod encrypted;
pub mod io_uring_backend;
mod memory;
pub mod segmented;

pub use encrypted::EncryptedWalBackend;
pub use memory::InMemoryWalBackend;
pub use segmented::SegmentedWalBackend;

pub use crate::wal_backend::{WalBackend, WalError, WalFrame};
