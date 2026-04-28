# CLAUDE.md — `inferadb-ledger-fs-sync`

## Purpose

File sync primitives with configurable durability modes. Wraps
`fcntl(F_BARRIERFSYNC)` on Apple platforms so the rest of the workspace can
select between "full" (non-volatile flush) and "barrier" (device write-cache
ordering only) sync semantics.

## Why this crate exists — the unsafe exception

The workspace `CLAUDE.md` golden rule #8 bans `unsafe` in production code.
**This crate is the single narrowly-scoped exception.**

Reason: `F_BARRIERFSYNC` (Apple's fast ordered-write primitive, ~2-5ms vs
`F_FULLFSYNC`'s ~15-25ms on APFS SSDs) is the largest single lever for
write throughput on macOS deployments. No audited safe-syscall crate —
neither `rustix` nor `nix` as of 2026-04 — exposes a wrapper for it. The
only path through the Rust ecosystem is `libc::fcntl` with an `unsafe`
block.

Rather than relax the workspace-wide ban, all unsafe code is confined here,
audited, and documented. Any new `unsafe` block in this crate must:

1. Be in `src/lib.rs` (no other files).
2. Carry a `SAFETY:` comment explaining fd lifetime, aliasing, and error
   handling invariants.
3. Map directly to a single syscall with no additional logic.
4. Be reviewed by a human before merge.

## Public surface

- `sync(file)` — barrier fsync: `fcntl(F_BARRIERFSYNC)` on Apple,
  `sync_data()` elsewhere. Used on every commit / WAL flush.
- `evict_page_cache(file)` — best-effort page-cache eviction, used by the
  O6 vault hibernation path to release Linux page-cache pressure on
  Dormant vaults. `posix_fadvise(POSIX_FADV_DONTNEED)` on Linux; no-op
  success on Apple / Windows / other (macOS does not expose
  `posix_fadvise`, and `fcntl(F_NOCACHE)` does not evict already-cached
  pages — see module docs).

## Rules

- **No `unsafe` outside `sync_barrier_apple` and `evict_page_cache_linux`.**
  Both blocks map to a single syscall and carry a `SAFETY:` comment. Any
  new unsafe block must be justified in a PR against this file.
- **No additional dependencies without review.** The dependency surface is
  `libc` + `serde` intentionally. Adding a third dep should trigger a
  conversation about whether the scope is creeping.
- **No I/O beyond a single `fcntl` / `sync_data` / `posix_fadvise` call
  per public function.** This crate is a syscall wrapper, not an
  orchestration layer.

## Auditor allowlist

The `unsafe-panic-auditor` agent (see `.claude/agents/unsafe-panic-auditor.md`)
allowlists this crate for `unsafe_code`. Any other crate tripping the
unsafe check is still a hard failure.

## Testing

- Unit tests cover both `Full` and `Barrier` round-trips plus serde round-
  tripping — see `src/lib.rs` test module.
- Property and crash-recovery tests for the consumers (SegmentedWalBackend,
  FileBackend) live in those crates; this crate has no fixture complexity
  of its own.
