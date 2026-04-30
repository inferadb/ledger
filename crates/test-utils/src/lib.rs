//! Shared test scaffolding for InferaDB Ledger crates.
//!
//! Add this crate as a `[dev-dependency]` to avoid duplicating test boilerplate.
//! It exposes four building blocks:
//!
//! - [`TestDir`] — temporary directory that auto-deletes on drop; use instead of hand-rolling
//!   `tempfile::TempDir` in every test.
//! - [`test_batch_config`] — a [`types::config::BatchConfig`] tuned for fast, deterministic test
//!   runs (small batches, 10ms timeout, serial flushing).
//! - [`CrashInjector`] / [`CrashPoint`] — deterministic crash simulation for the B+ tree dual-slot
//!   commit protocol. Wire the injector into a store backend's I/O hooks to verify recovery at each
//!   commit boundary.
//! - [`strategies`] — composable proptest strategies for every domain type (`Operation`,
//!   `Transaction`, `VaultBlock`, `EventEntry`, slugs, IDs, …). Run with `just test-proptest`;
//!   control iteration count via `PROPTEST_CASES`.
//!
//! [`types::config::BatchConfig`]: inferadb_ledger_types::config::BatchConfig

#![deny(unsafe_code)]
#![warn(missing_docs)]
// Test utilities are allowed to use unwrap for simplicity
#![cfg_attr(test, allow(clippy::disallowed_methods))]

mod test_dir;
pub use test_dir::TestDir;

mod config;
pub use config::test_batch_config;

mod crash_injector;
pub use crash_injector::{CrashInjector, CrashPoint};

/// Proptest strategies for generating domain types.
pub mod strategies;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_dir_creates_temp_directory() {
        let dir = TestDir::new();
        assert!(dir.path().exists(), "temp directory should exist");
        assert!(dir.path().is_dir(), "should be a directory");
    }

    #[test]
    fn test_dir_path_returns_valid_path() {
        let dir = TestDir::new();
        let path = dir.path();
        std::fs::write(path.join("test.txt"), "hello").expect("write file");
        assert!(path.join("test.txt").exists());
    }

    #[test]
    fn test_dir_join_creates_subdirectory_path() {
        let dir = TestDir::new();
        let subpath = dir.join("subdir/nested");
        assert!(subpath.starts_with(dir.path()));
        assert!(subpath.ends_with("subdir/nested"));
    }

    #[test]
    fn test_dir_cleanup_on_drop() {
        let path = {
            let dir = TestDir::new();
            let p = dir.path().to_path_buf();
            std::fs::write(p.join("file.txt"), "data").expect("write file");
            assert!(p.exists());
            p
        };
        assert!(!path.exists(), "temp directory should be cleaned up on drop");
    }

    #[test]
    fn test_batch_config_returns_valid_config() {
        let config = test_batch_config();
        assert!(config.max_batch_size > 0, "batch size should be positive");
        assert!(config.batch_timeout.as_millis() > 0, "timeout should be positive");
    }
}
