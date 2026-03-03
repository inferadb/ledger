//! Shared test utilities for InferaDB Ledger crates.
//!
//! This crate provides common test helpers to reduce boilerplate across test modules:
//!
//! - [`TestDir`] - Managed temporary directory with path helpers
//! - [`test_batch_config`] - Default batch configuration for tests
//! - [`CrashInjector`] - Crash injection for testing crash recovery
//! - [`CrashPoint`] - Enumeration of crash points in commit protocol
//! - [`strategies`] - Proptest strategies for domain types

#![deny(unsafe_code)]
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
