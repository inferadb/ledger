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
//! - **Apple** (`target_vendor = "apple"`): `fcntl(F_BARRIERFSYNC)` — the this crate's sole
//!   `unsafe` block.
//! - **Linux / other Unix**: `fdatasync` via [`File::sync_data`] (already has barrier semantics at
//!   the VFS layer on ext4/xfs/btrfs with default mount options).
//! - **Windows / other**: [`File::sync_data`].
//!
//! See `docs/architecture/durability.md` for the operator-facing durability
//! matrix, and `crates/fs/CLAUDE.md` for the escalation rationale
//! behind this crate's `unsafe` exception to the workspace-wide ban.
//!
//! # Page-cache eviction
//!
//! [`evict_page_cache`] hints to the OS that the file's pages can be dropped
//! from the page cache. Used by the O6 vault hibernation path to release
//! per-vault DB memory pressure when a vault transitions to `Dormant`.
//! Cross-platform best-effort:
//!
//! - **Linux** (`target_os = "linux"`): `posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)` — drops the
//!   cached pages for the file.
//! - **Apple / Windows / other**: no-op success. macOS does not expose `posix_fadvise` and
//!   `fcntl(F_NOCACHE)` only affects future I/O; truly dropping cached pages on macOS requires
//!   `mmap` + `madvise`, which is out of scope for a single-syscall wrapper. The hibernation
//!   contract tolerates a no-op — the operator-visible win is a Linux-deployment memory-pressure
//!   win, not a hard correctness requirement.

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
    if rc == -1 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

/// Hints to the OS that `file`'s cached pages may be dropped from the page
/// cache.
///
/// Best-effort across platforms:
///
/// - **Linux**: `posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)` — drops the cached pages for this
///   file from the kernel's page cache. Subsequent reads re-populate from disk.
/// - **Apple / Windows / other**: success no-op. macOS does not expose `posix_fadvise`, and
///   `fcntl(F_NOCACHE)` only suppresses caching for future I/O — it does not evict already-cached
///   pages without an mmap roundtrip. This is documented and acceptable; see module docs.
///
/// Idempotent. Returns `Ok(())` even when the platform has no eviction
/// path; failures from the underlying syscall on supported platforms are
/// propagated as [`io::Error`].
pub fn evict_page_cache(file: &File) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        evict_page_cache_linux(file)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn evict_page_cache_linux(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: `fd` is a valid open file descriptor borrowed from `file`,
    // whose lifetime outlives this call (`&File` is held).
    // `libc::posix_fadvise` is a pure syscall that takes the fd, an offset,
    // a length, and an advice constant; passing `(0, 0, POSIX_FADV_DONTNEED)`
    // means "the entire file from offset 0". The kernel reads no userspace
    // memory beyond the fd argument. There is no aliasing, lifetime, or
    // thread-safety concern. On error `posix_fadvise` returns the errno
    // value directly (it does NOT set the global `errno` and does NOT
    // return -1), so we map a non-zero return into an `io::Error::from_raw_os_error`.
    let rc = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
    if rc == 0 { Ok(()) } else { Err(io::Error::from_raw_os_error(rc)) }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
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

    /// Round-trip: write → evict → read should still observe the data.
    /// Eviction is best-effort and idempotent; the test asserts both
    /// (Ok return + correctness preservation) regardless of platform.
    #[test]
    fn evict_page_cache_roundtrips_and_preserves_data() {
        use std::io::{Read, Seek, SeekFrom};

        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"page-cache-eviction-roundtrip-payload").unwrap();
        sync(f.as_file()).unwrap();

        // First eviction.
        evict_page_cache(f.as_file()).unwrap();
        // Second call: idempotent.
        evict_page_cache(f.as_file()).unwrap();

        // Data is still readable after eviction (the OS re-populates from disk).
        f.as_file().seek(SeekFrom::Start(0)).unwrap();
        let mut buf = String::new();
        f.as_file().read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "page-cache-eviction-roundtrip-payload");
    }
}
