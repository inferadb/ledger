# Ledger Dependencies

Recommended libraries for core Ledger components.

## Consensus: Raft

### Recommendation: [Openraft](https://lib.rs/crates/openraft)

| Criteria               | raft-rs (TiKV)                  | Openraft                |
| ---------------------- | ------------------------------- | ----------------------- |
| **Architecture**       | Core module only                | Full async framework    |
| **Async model**        | Tick-based                      | Event-driven            |
| **Integration effort** | High (build transport, storage) | Lower (traits provided) |
| **Performance**        | Battle-tested                   | 70K-1M writes/sec       |
| **Production use**     | TiKV                            | Databend meta-service   |

**Why Openraft:**

- Event-driven matches our async architecture
- Built-in traits for storage (`RaftLogStorage`, `RaftStateMachine`) and networking (`RaftNetwork`)
- Transport-agnostic: `RaftNetwork` trait lets us use tonic/gRPC over TCP (see [databend-meta](https://github.com/databendlabs/databend/blob/main/src/meta/service/src/network.rs) for production example)
- Optimized message batching for high throughput
- Active development and good documentation

**raft-rs alternative:** [raft-rs](https://github.com/tikv/raft-rs) offers finer control at the cost of more integration work. Extensively validated in TiKV production.

## State Tree: Merkle Patricia Trie

### Recommendation: [paritytech/trie](https://github.com/paritytech/trie) (trie-db crate)

| Library                | Pros                                          | Cons                          |
| ---------------------- | --------------------------------------------- | ----------------------------- |
| **trie-db**            | Battle-tested (Substrate), flexible backend   | Substrate-oriented API        |
| **reth_trie**          | Parallel support (rayon), Ethereum-compatible | Ethereum-specific assumptions |
| **sparse-merkle-tree** | Simpler, no-std support                       | Less mature                   |

**Why trie-db:**

- Production-proven in Substrate/Polkadot
- Generic backend trait allows custom storage
- Apache 2.0 license
- Active maintenance

**Alternative:** Sparse Merkle Trees suit simpler key-value patterns. SMTs have fixed depth (256 levels for SHA-256 keys), simplifying proof generation.

## Networking: gRPC

| Component | Library                                    | Purpose               |
| --------- | ------------------------------------------ | --------------------- |
| gRPC      | [tonic](https://github.com/hyperium/tonic) | Client API + Raft RPC |
| Protobuf  | [prost](https://github.com/tokio-rs/prost) | Message serialization |

### Transport Architecture

| Traffic Type       | Transport           | Rationale                                                                                                                                                 |
| ------------------ | ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Raft consensus** | gRPC/HTTP2 over TCP | Long-lived connections, ordered delivery, latency-critical. TCP's kernel implementation and hardware offload outperform QUIC for single-stream workloads. |
| **Client API**     | gRPC/HTTP2 over TCP | Default. QUIC optional for mobile/edge clients needing 0-RTT reconnection.                                                                                |
| **Discovery**      | gRPC/HTTP2 over TCP | Same as client API; `_system` Raft group serves as registry.                                                                                              |

**Why TCP for Raft**: Production Raft systems (etcd, TiKV, CockroachDB) use TCP. Benchmarks show QUIC achieves ~50% of TCP throughput for server-to-server workloads due to user-space overhead. QUIC's multiplexing benefits don't apply to Raft's single ordered log stream.

## Discovery & Coordination

Uses the `_system` Raft group as a strongly-consistent registry. No additional P2P dependencies required.

| Aspect         | Value                   |
| -------------- | ----------------------- |
| Lookup latency | O(1) local cache        |
| Consistency    | Strong (Raft consensus) |
| Precedent      | TiKV, CockroachDB, etcd |

**Why Raft-native over libp2p DHT**: In a trusted WireGuard network, libp2p's security features are redundant. Raft provides strong consistency vs DHT's eventual consistency. O(1) lookups vs O(log n). No additional operational complexity.

## Storage

| Component   | Library                                                                             |
| ----------- | ----------------------------------------------------------------------------------- |
| Embedded KV | [redb](https://github.com/cberner/redb) or [sled](https://github.com/spacejam/sled) |
| WAL         | Custom or [fjall](https://github.com/fjall-rs/fjall)                                |

**redb:** Pure Rust, ACID, simple API. Recommended for block storage and state snapshots.

**sled:** Higher performance but slower development cadence. Best for hot paths requiring maximum throughput.

## Cryptography

| Component            | Library                                         |
| -------------------- | ----------------------------------------------- |
| SHA-256              | [sha2](https://github.com/RustCrypto/hashes)    |
| Blake3 (alternative) | [blake3](https://github.com/BLAKE3-team/BLAKE3) |

**Recommendation:** Use SHA-256 for broad compatibility. Blake3 offers 3-4x speed but limited tooling support.

## Serialization

| Component       | Library                                              |
| --------------- | ---------------------------------------------------- |
| Binary encoding | [bincode](https://github.com/bincode-org/bincode) v2 |
| JSON (debug)    | [serde_json](https://github.com/serde-rs/json)       |
| Protobuf        | [prost](https://github.com/tokio-rs/prost)           |

## Observability

| Component     | Library                                                               |
| ------------- | --------------------------------------------------------------------- |
| Metrics       | [metrics](https://github.com/metrics-rs/metrics)                      |
| Tracing       | [tracing](https://github.com/tokio-rs/tracing)                        |
| OpenTelemetry | [opentelemetry](https://github.com/open-telemetry/opentelemetry-rust) |

## Summary: Cargo.toml Dependencies

```toml
[dependencies]
# Consensus
openraft = "0.10"

# Networking (gRPC over TCP for Raft + Client API + Discovery)
tonic = "0.12"
prost = "0.13"

# Storage
redb = "2.2"

# Crypto
sha2 = "0.10"

# Serialization
bincode = "2.0"
serde = { version = "1.0", features = ["derive"] }

# Async runtime
tokio = { version = "1.0", features = ["full"] }

# Observability
tracing = "0.1"
metrics = "0.23"
```

## Sources

- [Openraft](https://lib.rs/crates/openraft)
- [raft-rs (TiKV)](https://github.com/tikv/raft-rs)
- [paritytech/trie](https://github.com/paritytech/trie)
- [reth_trie](https://reth.rs/docs/reth_trie/index.html)
- [Sparse Merkle Tree topic](https://github.com/topics/sparse-merkle-tree)
