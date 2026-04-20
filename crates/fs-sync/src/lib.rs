//! File sync primitives with configurable durability modes.
//!
//! This crate is the **only** place in the InferaDB Ledger workspace that is
//! permitted to use `unsafe` code. It wraps `fcntl(F_BARRIERFSYNC)` on Apple
//! platforms, which no audited safe-syscall crate (`rustix`, `nix`) currently
//! exposes. All `unsafe` blocks are isolated here, documented with `SAFETY:`
//! comments, and subject to manual review by the `unsafe-panic-auditor`
//! agent (which allowlists this crate).
//!
//! See `crates/fs-sync/CLAUDE.md` for the escalation rationale and
//! `docs/operations/durability.md` for the operator-facing durability matrix.

use std::{fs::File, io};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// File sync mode selecting the tradeoff between durability and latency.
///
/// # Durability matrix
///
/// | Mode      | Process crash | Kernel panic | Power loss                    |
/// |-----------|---------------|--------------|-------------------------------|
/// | `Barrier` | Safe          | Safe         | May lose last ~seconds        |
/// | `Full`    | Safe          | Safe         | Safe (non-volatile media)     |
///
/// `Barrier` is the default because on Apple platforms it is 4-8× lower
/// latency than `Full` while still surviving every failure mode short of
/// sudden power loss. Linux deployments see identical behavior in both
/// modes — `fdatasync` already implements barrier semantics at the VFS
/// layer — so the default change is macOS-centric.
///
/// # Platform behavior
///
/// - **Apple** (`target_vendor = "apple"`):
///   - `Barrier` (default) → `fcntl(F_BARRIERFSYNC)` (this crate's `unsafe` block).
///   - `Full` → `fcntl(F_FULLFSYNC)` via [`File::sync_data`].
/// - **Linux / other Unix**:
///   - `Barrier` (default) → `fdatasync` via [`File::sync_data`].
///   - `Full` → `fdatasync` (same call; Linux's `fdatasync` already has the
///     semantics `F_BARRIERFSYNC` approximates on macOS).
/// - **Windows / other**:
///   - Both modes → [`File::sync_data`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[serde(rename_all = "snake_case")]
pub enum FileSyncMode {
    /// Barrier fsync: data is written in order and reaches the device write
    /// cache before returning, but is not forced to non-volatile storage.
    /// Survives process crash and kernel panic; may lose the last few
    /// seconds of writes under sudden power loss (hardware-dependent).
    /// **This is the default.**
    ///
    /// On non-Apple platforms this is equivalent to `Full` — `fdatasync`
    /// on Linux is already what `F_BARRIERFSYNC` approximates on macOS.
    #[default]
    Barrier,
    /// Full durability: data is flushed to non-volatile storage before the
    /// call returns. Survives process crash, kernel panic, **and** power
    /// loss. Opt-in; set when the deployment cannot tolerate the power-loss
    /// window `Barrier` permits (commodity hardware without power-loss
    /// protection, strict compliance requirements).
    ///
    /// On Apple: `fcntl(F_FULLFSYNC)` (~15-25ms on APFS SSDs).
    /// On Linux: `fdatasync` (same call as `Barrier` on Linux).
    Full,
}

/// Syncs `file` to persistent storage using the selected `mode`.
///
/// Returns `Ok(())` on success or the underlying [`io::Error`] on failure.
pub fn sync(file: &File, mode: FileSyncMode) -> io::Result<()> {
    match mode {
        FileSyncMode::Full => file.sync_data(),
        FileSyncMode::Barrier => sync_barrier(file),
    }
}

#[cfg(target_vendor = "apple")]
fn sync_barrier(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: `fd` is a valid open file descriptor borrowed from `file`,
    // whose lifetime outlives this call (`&File` is held). `libc::fcntl`
    // with `F_BARRIERFSYNC` is a pure syscall that reads no userspace memory
    // beyond the fd argument and returns an integer; there is no aliasing,
    // lifetime, or thread-safety concern. On error the syscall returns -1
    // with `errno` set, which we propagate via `io::Error::last_os_error()`.
    let rc = unsafe { libc::fcntl(fd, libc::F_BARRIERFSYNC) };
    if rc == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(target_vendor = "apple"))]
fn sync_barrier(file: &File) -> io::Result<()> {
    file.sync_data()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn full_sync_roundtrips() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello").unwrap();
        sync(f.as_file(), FileSyncMode::Full).unwrap();
    }

    #[test]
    fn barrier_sync_roundtrips() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello").unwrap();
        sync(f.as_file(), FileSyncMode::Barrier).unwrap();
    }

    #[test]
    fn default_is_barrier() {
        // Barrier is the default because on Apple it is 4-8× lower latency
        // than Full while surviving every failure mode short of sudden
        // power loss. Flipping this back to `Full` is a deliberate
        // durability-class change — fail the test so it is caught in
        // review, not on a live deployment.
        assert_eq!(FileSyncMode::default(), FileSyncMode::Barrier);
    }

    #[test]
    fn sync_mode_roundtrips_serde() {
        for mode in [FileSyncMode::Full, FileSyncMode::Barrier] {
            let s = serde_json::to_string(&mode).unwrap();
            let back: FileSyncMode = serde_json::from_str(&s).unwrap();
            assert_eq!(back, mode);
        }
    }

    #[test]
    fn sync_mode_deserializes_snake_case() {
        let full: FileSyncMode = serde_json::from_str("\"full\"").unwrap();
        assert_eq!(full, FileSyncMode::Full);
        let barrier: FileSyncMode = serde_json::from_str("\"barrier\"").unwrap();
        assert_eq!(barrier, FileSyncMode::Barrier);
    }
}
