//! Async fsync via io_uring (Linux 5.6+, feature-gated).

/// Fsync lifecycle state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncState {
    /// No pending fsync operation.
    Idle,
    /// Data written to the WAL but not yet submitted for fsync.
    Written,
    /// Fsync submitted to the io_uring submission queue.
    Submitted,
    /// Fsync completion confirmed from the io_uring completion queue.
    Confirmed,
}

/// io_uring configuration.
#[derive(Debug, Clone)]
pub struct IoUringConfig {
    /// Whether io_uring-based async fsync is enabled.
    pub enabled: bool,
    /// Number of submission queue entries in the io_uring ring.
    pub ring_size: u32,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self { enabled: false, ring_size: 64 }
    }
}

impl IoUringConfig {
    /// Returns whether io_uring support is available on this platform.
    ///
    /// Requires Linux and the `io-uring` feature flag.
    pub fn is_available() -> bool {
        cfg!(all(target_os = "linux", feature = "io-uring"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fsync_state_all_variants_distinct() {
        let states =
            [FsyncState::Idle, FsyncState::Written, FsyncState::Submitted, FsyncState::Confirmed];

        for (i, a) in states.iter().enumerate() {
            for (j, b) in states.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn test_io_uring_config_default_is_disabled() {
        let config = IoUringConfig::default();

        assert!(!config.enabled);
        assert_eq!(config.ring_size, 64);
    }

    #[test]
    fn test_io_uring_is_available_matches_platform() {
        let available = IoUringConfig::is_available();

        if cfg!(not(all(target_os = "linux", feature = "io-uring"))) {
            assert!(!available);
        }
    }
}
