//! Platform-specific file-system syscall wrappers for InferaDB Ledger.
//!
//! This crate is the **only** location in the InferaDB Ledger workspace
//! permitted to use `unsafe` code (workspace golden rule 8 exception).
//! All `unsafe` blocks are confined to this file, map to a single syscall
//! each, carry `SAFETY:` comments, and are subject to manual review via the
//! `unsafe-panic-auditor` agent allowlist.
//!
//! The crate exists because two syscalls needed by the storage layer lack
//! wrappers in any audited safe-syscall crate as of this writing:
//!
//! - `fcntl(F_BARRIERFSYNC)` on Apple platforms — Apple's ordered-write primitive, substantially
//!   faster than `F_FULLFSYNC` on APFS SSDs. Neither `rustix` nor `nix` expose a safe wrapper for
//!   this command.
//! - `posix_fadvise(POSIX_FADV_DONTNEED)` on Linux — page-cache eviction hint used by the vault
//!   hibernation path. `rustix` gates this behind unstable feature flags.
//!
//! Confining both blocks here prevents `unsafe` from spreading across the
//! workspace while keeping the storage layer's hot paths optimal.
//!
//! # Barrier-fsync semantics
//!
//! [`sync`] issues a *barrier fsync*: writes reach the device write cache in
//! program order, but are not forced to non-volatile storage. This guarantees
//! durability across process crash and kernel panic. Under sudden power loss
//! on hardware without power-loss-protection capacitors, the last few seconds
//! of writes may be lost.
//!
//! Platform dispatch:
//!
//! - **Apple** (`target_vendor = "apple"`): `fcntl(F_BARRIERFSYNC)` — this crate's first `unsafe`
//!   block.
//! - **Linux / other Unix**: [`File::sync_data`] (`fdatasync`), which already carries barrier
//!   semantics at the VFS layer on ext4, xfs, and btrfs with default mount options.
//! - **Windows / other**: [`File::sync_data`].
//!
//! See `docs/architecture/durability.md` for the operator-facing durability
//! matrix.
//!
//! # Page-cache eviction
//!
//! [`evict_page_cache`] hints to the OS that the file's pages may be dropped
//! from the page cache. The vault hibernation path calls this when a vault
//! transitions to `Dormant` to release per-vault DB memory pressure.
//!
//! Platform dispatch:
//!
//! - **Linux** (`target_os = "linux"`): `posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)` — this
//!   crate's second `unsafe` block. Drops all cached pages for the file; subsequent reads
//!   re-populate from disk.
//! - **Apple / Windows / other**: no-op success. macOS does not expose `posix_fadvise`, and
//!   `fcntl(F_NOCACHE)` only suppresses caching for future I/O without evicting already-resident
//!   pages. Truly dropping cached pages on macOS requires `mmap` + `madvise`, which is outside the
//!   scope of a single-syscall wrapper. The hibernation contract tolerates a no-op here; the
//!   memory-pressure benefit is a Linux-deployment win, not a hard correctness requirement.

use std::{fs::File, io};

/// Syncs `file` to the device write cache with barrier ordering guarantees.
///
/// Barrier semantics mean that writes issued before this call are ordered
/// ahead of writes issued after it and are visible to the device write cache,
/// but are not forced to non-volatile storage. This is sufficient to survive
/// process crash and kernel panic; it is not sufficient to survive sudden
/// power loss on hardware without power-loss-protection capacitors.
///
/// # Platform
///
/// - **Apple** (`target_vendor = "apple"`): delegates to `sync_barrier_apple`, which calls
///   `fcntl(F_BARRIERFSYNC)` via an `unsafe` block (see its `SAFETY:` comment).
/// - **All other targets**: calls [`File::sync_data`] (`fdatasync`).
///
/// # Errors
///
/// Returns the [`io::Error`] from the underlying syscall on failure.
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

/// Issues `fcntl(F_BARRIERFSYNC)` on `file` (Apple platforms only).
///
/// `F_BARRIERFSYNC` flushes dirty pages to the device write cache in program
/// order without waiting for non-volatile commit, making it substantially
/// faster than `F_FULLFSYNC` on APFS SSDs while still preventing write
/// reordering across the call.
///
/// # Safety
///
/// The `unsafe` block calls `libc::fcntl(fd, libc::F_BARRIERFSYNC)`.
///
/// Preconditions that make this sound:
///
/// 1. **Valid open fd**: `fd` is obtained from [`AsRawFd::as_raw_fd`] on a `&File` whose borrow
///    (`file`) is held for the duration of this call. The fd cannot be closed or reused while the
///    reference is live.
/// 2. **No aliasing**: `libc::fcntl` with `F_BARRIERFSYNC` reads no userspace memory beyond the
///    integer `fd` argument. There are no pointer arguments and no shared mutable state touched by
///    this call.
/// 3. **Thread safety**: `fcntl` with `F_BARRIERFSYNC` is documented by Apple as safe to call
///    concurrently on separate file descriptors. Concurrent calls on the *same* fd are also safe —
///    the syscall is idempotent with respect to ordering.
/// 4. **Error handling**: on failure the syscall returns `-1` and sets the thread-local `errno`. We
///    immediately call [`io::Error::last_os_error`] before any other syscall could overwrite
///    `errno`.
#[cfg(target_vendor = "apple")]
fn sync_barrier_apple(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: see function-level `# Safety` doc above.
    let rc = unsafe { libc::fcntl(fd, libc::F_BARRIERFSYNC) };
    if rc == -1 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

/// Hints to the OS that `file`'s pages may be dropped from the page cache.
///
/// This is a best-effort, idempotent operation. Callers must not rely on
/// eviction actually occurring; the OS may ignore the hint.
///
/// # Platform
///
/// - **Linux** (`target_os = "linux"`): delegates to `evict_page_cache_linux`, which calls
///   `posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)` via an `unsafe` block (see its `SAFETY:`
///   comment). Covers the entire file (offset `0`, length `0` means "to end of file" per POSIX).
///   Subsequent reads re-populate the page cache from disk.
/// - **Apple / Windows / other**: returns `Ok(())` immediately without performing any syscall. See
///   module docs for why a no-op is acceptable.
///
/// # Errors
///
/// On Linux, propagates the [`io::Error`] from `posix_fadvise` on failure.
/// On all other platforms, always returns `Ok(())`.
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

/// Issues `posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)` on `file` (Linux only).
///
/// Advises the kernel that the entire file's pages are no longer needed and
/// may be reclaimed from the page cache. The kernel is free to ignore the
/// hint; callers must tolerate a no-op.
///
/// # Safety
///
/// The `unsafe` block calls
/// `libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED)`.
///
/// Preconditions that make this sound:
///
/// 1. **Valid open fd**: `fd` is obtained from [`AsRawFd::as_raw_fd`] on a `&File` whose borrow
///    (`file`) is held for the duration of this call. The fd cannot be closed or reused while the
///    reference is live.
/// 2. **No aliasing**: `posix_fadvise` is a pure syscall that accepts an fd, two `off_t` range
///    arguments, and an `int` advice constant. It reads no userspace memory. There are no pointer
///    arguments.
/// 3. **Thread safety**: `posix_fadvise` is safe to call concurrently on separate or the same file
///    descriptor; the advice is advisory and does not mutate kernel structures in a way that
///    requires external synchronization from userspace.
/// 4. **Error handling**: unlike most POSIX syscalls, `posix_fadvise` returns the error number
///    directly as a positive integer on failure — it does *not* return `-1` and set `errno`. We map
///    a non-zero return directly to [`io::Error::from_raw_os_error`].
#[cfg(target_os = "linux")]
fn evict_page_cache_linux(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = file.as_raw_fd();
    // SAFETY: see function-level `# Safety` doc above.
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
