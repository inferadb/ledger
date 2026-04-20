//! Shared, bounded rayon thread pool for apply-path parallelism.
//!
//! # Why a dedicated pool
//!
//! `compute_state_root` (the dominant apply-path span) fans per-dirty-bucket
//! hashing across rayon. The `raft` crate also fans `compute_state_root` +
//! `compute_vault_block_hash` across unique vaults post-apply-loop. Both
//! paths previously used rayon's default global pool, which sizes itself to
//! `num_cpus` threads — the same core count tokio's async runtime uses for
//! its worker threads, so firing rayon from inside a tokio task creates
//! ~2× CPU oversubscription. Other tokio work (network I/O, response
//! delivery, BatchWriter tick) gets preempted, inflating p99/p999 tail
//! latency by ~2× even when p50 improves.
//!
//! A bounded dedicated pool (`~num_cpus / 2`, clamped `[2, 8]`) caps the
//! rayon burst at roughly half the cores, leaving headroom for the tokio
//! runtime. In return you give up some of the theoretical parallel
//! speedup, but the real-world gain dominates because:
//!
//! - Dirty-bucket counts are typically 10-50 per batch; parallelism above
//!   ~8 workers rarely saturates the work queue.
//! - Bucket hashing is bursty (sub-millisecond per bucket); thread spin-up
//!   dominates beyond ~8 threads.
//! - The tail-regression cost is measurable; the speedup ceiling above 8
//!   threads is not.
//!
//! # Callers
//!
//! - `state::StateLayer::compute_state_root` — inner dirty-bucket scan.
//! - `raft::log_storage::raft_impl::apply_committed_entries` — post-loop
//!   per-vault `compute_state_root` + per-entry `compute_vault_block_hash`
//!   via the re-export below.

use std::sync::LazyLock;

/// Maximum threads the apply-path rayon pool will use, regardless of core
/// count. Beyond this, thread-scheduling overhead starts to dominate the
/// actual hashing work for typical batch sizes (10-50 dirty buckets).
const MAX_POOL_THREADS: usize = 8;

/// Minimum threads — below this, serial execution would be faster due to
/// rayon overhead.
const MIN_POOL_THREADS: usize = 2;

/// Fraction of available cores the pool is allowed to use. The rest are
/// reserved for tokio's runtime workers (network I/O, response delivery,
/// BatchWriter tick, other async work).
const POOL_CORE_DIVISOR: usize = 2;

/// Bounded rayon pool for apply-path parallelism.
///
/// Lazy-initialised on first use. Sized to leave CPU headroom for tokio so
/// rayon bursts don't starve network / response-delivery work and inflate
/// tail latency.
pub static APPLY_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    let cores = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
    let threads = (cores / POOL_CORE_DIVISOR).clamp(MIN_POOL_THREADS, MAX_POOL_THREADS);
    // SAFETY: `num_threads` is clamped to `[MIN_POOL_THREADS, MAX_POOL_THREADS]`
    // (both > 0), the only ThreadPoolBuilder::build precondition that returns
    // an Err is a zero thread count or platform-level thread-creation failure.
    // A zero count is impossible by construction; thread-creation failure
    // would mean the host is incapable of spawning threads at all, in which
    // case the process cannot function regardless. We treat the latter as
    // fatal at startup — no recovery is meaningful.
    #[allow(clippy::expect_used)]
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("apply-hash-{i}"))
        .build()
        .expect("failed to build apply rayon pool — platform unable to spawn threads")
});
