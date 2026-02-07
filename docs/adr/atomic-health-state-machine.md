# ADR: AtomicU8 State Machine for Health State

## Context

InferaDB Ledger exposes Kubernetes-compatible health probes (startup, liveness, readiness) that are polled frequently by orchestrators, load balancers, and internal watchdogs. The health state tracks a node's lifecycle phase: initialization, operational readiness, and shutdown.

### The Problem

Health probes are called on the hot path — every 5–10 seconds by Kubernetes, potentially more frequently by load balancers. The state must be:

1. **Lock-free on reads** — probes must never block, even during state transitions
2. **Thread-safe** — multiple probe handlers, the bootstrap sequence, and the shutdown coordinator all access the state concurrently
3. **One-way transitions** — a node that starts shutting down must never appear ready again (prevents routing traffic to a draining node)

### Options Evaluated

| Option                          | Read Cost                | Write Cost                       | Blocking Risk                               | Complexity |
| ------------------------------- | ------------------------ | -------------------------------- | ------------------------------------------- | ---------- |
| `AtomicU8` + `compare_exchange` | Single CPU instruction   | Single CAS instruction           | None                                        | Low        |
| `RwLock<NodePhase>`             | Lock acquisition (~50ns) | Lock acquisition (~50ns)         | Yes (writer starvation, priority inversion) | Low        |
| `Mutex<NodePhase>`              | Lock acquisition (~50ns) | Lock acquisition (~50ns)         | Yes (any probe blocks all others)           | Low        |
| `Arc<ArcSwap<NodePhase>>`       | Atomic pointer load      | Atomic pointer swap + allocation | None                                        | Medium     |

## Decision

**Use `AtomicU8` with `compare_exchange` for one-way state transitions** (`crates/raft/src/graceful_shutdown.rs`).

### State Machine

```
Starting (0) ──mark_ready()──▶ Ready (1) ──mark_shutting_down()──▶ ShuttingDown (2)
    │                                                                      ▲
    └──────────────────── mark_shutting_down() ────────────────────────────┘
```

Three states, three `#[repr(u8)]` values, two transitions. A node never goes backward.

### Implementation

```rust
#[repr(u8)]
pub enum NodePhase {
    Starting = 0,
    Ready = 1,
    ShuttingDown = 2,
}

pub struct HealthState {
    phase: Arc<AtomicU8>,
    // ... connection_tracker, watchdog
}
```

**Lock-free reads (every probe call):**

```rust
pub fn phase(&self) -> NodePhase {
    NodePhase::from_u8(self.phase.load(Ordering::Acquire))
}
```

**CAS-enforced transition (Starting → Ready):**

```rust
pub fn mark_ready(&self) -> bool {
    self.phase.compare_exchange(
        NodePhase::Starting as u8,
        NodePhase::Ready as u8,
        Ordering::AcqRel,
        Ordering::Acquire,
    ).is_ok()
}
```

**Idempotent terminal transition (→ ShuttingDown):**

```rust
pub fn mark_shutting_down(&self) {
    self.phase.store(NodePhase::ShuttingDown as u8, Ordering::Release);
}
```

### Why Not RwLock?

- **Blocking risk.** Kubernetes kills pods if liveness probes timeout. A stuck writer (crashed thread, priority inversion) would block all probe readers, triggering pod restart.
- **Unnecessary overhead.** State transitions happen 2–3 times in a node's lifetime (startup, ready, shutdown). Reads happen 10–20+ times per second. Optimizing reads at the cost of slightly more complex writes is the correct trade-off.
- **No complex state.** The entire state is a single byte. Locks are justified when protecting multi-field invariants — not for a single-value read.

### Why Not ArcSwap?

`ArcSwap` is used elsewhere in the codebase (`RuntimeConfigHandle`) for hot-reloadable configuration structs with multiple fields. For a single `u8`:

- ArcSwap adds pointer indirection (atomic pointer load → dereference → read value)
- AtomicU8 is a direct value load — strictly simpler and faster
- ArcSwap's allocation-free swap is valuable for large structs, not for `u8`

### Why u8 and Not u64?

Only 3 states exist. `u8` provides 256 possible values — more than sufficient. Using `u64` would waste 7 bytes per access and pollute cache lines unnecessarily. On x86_64, `AtomicU8` operations compile to the same `LOCK CMPXCHG` instruction family as `AtomicU64` — no performance difference, but smaller memory footprint.

### Why One-Way Transitions?

One-way transitions prevent a class of race conditions:

- **Shutdown→Ready race:** If a shutdown signal arrives while bootstrap is completing, a two-way state machine could oscillate: shutdown sets ShuttingDown, bootstrap sets Ready, shutdown reads Ready and doesn't drain. With one-way transitions, `mark_ready()` fails via CAS if shutdown already occurred.
- **Simplified reasoning.** Code that checks `phase() == Ready` knows the node will remain ready until shutdown begins — no need to handle transient states.
- **Kubernetes contract.** Once readiness fails, the pod should not re-enter the ready state. Load balancers assume a pod that leaves the ready set stays out.

## Consequences

### Positive

- **Zero contention on reads** — probe handlers never wait, even during transitions
- **Correctness by construction** — `compare_exchange` prevents invalid transitions at the CPU level
- **Minimal memory** — 1 byte + Arc overhead; fits in any cache line
- **Architecture-portable** — AtomicU8 compiles to native instructions on x86_64 (`LOCK CMPXCHG`), ARM64 (`LDAXR/STLXR`), and all other Rust targets

### Negative

- **Limited expressiveness** — cannot store additional context (e.g., "shutting down because of signal X") in the atomic value. Additional context is logged separately.
- **No notification mechanism** — callers must poll `phase()` rather than subscribing to transitions. In practice, this is fine because probe handlers are already polling.

### Neutral

- `Ordering::Acquire` on reads and `Ordering::Release` on writes ensures correct visibility across threads without full sequential consistency overhead.
- The watchdog check (`liveness_check()`) and connection tracker are separate fields on `HealthState` — only the phase itself uses atomics.

## References

- `crates/raft/src/graceful_shutdown.rs` — `HealthState`, `NodePhase`, transition methods, 32 tests
- `crates/raft/src/services/health.rs` — Three-probe pattern using `phase()` on every check
- `crates/raft/src/runtime_config.rs` — `ArcSwap` used for multi-field config (contrast: different use case)
