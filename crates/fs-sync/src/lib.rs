//! File sync primitive for InferaDB Ledger.
//!
//! This crate is the **only** place in the InferaDB Ledger workspace that is
//! permitted to use `unsafe` code. It wraps `fcntl(F_BARRIERFSYNC)` on Apple
//! platforms, which no audited safe-syscall crate (`rustix`, `nix`) currently
//! exposes. All `unsafe` blocks are isolated here, documented with `SAFETY:`
//! comments, and subject to manual review by the `unsafe-panic-auditor`
//! agent (which allowlists this crate).
//!
//! # Durability semantics
//!
//! [`sync`] issues a *barrier fsync*: the data is written in order and
//! reaches the device write cache, but is not forced to non-volatile
//! storage. This survives process crash and kernel panic. It may lose the
//! last few seconds of writes under sudden power loss on hardware without
//! power-loss-protection capacitors.
//!
//! - **Apple** (`target_vendor = "apple"`): `fcntl(F_BARRIERFSYNC)` — the
//!   this crate's sole `unsafe` block.
//! - **Linux / other Unix**: `fdatasync` via [`File::sync_data`] (already
//!   has barrier semantics at the VFS layer on ext4/xfs/btrfs with default
//!   mount options).
//! - **Windows / other**: [`File::sync_data`].
//!
//! See `docs/architecture/durability.md` for the operator-facing durability
//! matrix, and `crates/fs-sync/CLAUDE.md` for the escalation rationale
//! behind this crate's `unsafe` exception to the workspace-wide ban.

use std::{fs::File, io};

/// Syncs `file` to persistent storage with barrier semantics.
///
/// Returns `Ok(())` on success or the underlying [`io::Error`] on failure.
///
/// See module docs for the exact semantics per platform.
pub fn sync(file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        sync_barrier_apple(file)
    }
    #[cfg(not(target_vendor = "apple"))]
    {
        file.sync_data()
    }
}

#[cfg(target_vendor = "apple")]
fn sync_barrier_apple(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: `fd` is a valid open file descriptor borrowed from `file`,
    // whose lifetime outlives this call (`&File` is held). `libc::fcntl`
    // with `F_BARRIERFSYNC` is a pure syscall that reads no userspace
    // memory beyond the fd argument and returns an integer; there is no
    // aliasing, lifetime, or thread-safety concern. On error the syscall
    // returns -1 with `errno` set, which we propagate via
    // `io::Error::last_os_error()`.
    let rc = unsafe { libc::fcntl(fd, libc::F_BARRIERFSYNC) };
    if rc == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn sync_roundtrips() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello").unwrap();
        sync(f.as_file()).unwrap();
    }
}
