# InferaDB Ledger: A Cryptographically Verifiable Authorization Database

**Last Updated**: January 2026

--

## Executive Summary

Authorization systems face a fundamental tension: they must be fast enough for real-time decisions yet provide audit trails that can withstand legal and security scrutiny. Traditional approaches force a choice between performance and verifiability.

InferaDB Ledger resolves this tension through a hybrid architecture that separates state commitment from state storage. Authorization data lives in high-performance indexes optimized for sub-millisecond reads. A parallel cryptographic layer computes state roots using bucket-based merkleization, enabling proof generation without impacting query latency.

The system targets sub-2ms read latency at the 99th percentile while maintaining cryptographic proofs for every state transition. Each authorization decision can be independently verified against a tamper-evident chain of blocks signed by the cluster.

This paper describes Ledger's architecture, explains the engineering trade-offs, and provides honest assessment of both capabilities and limitations.

## 1. Introduction

### The Authorization Audit Gap

Access control failures consistently rank among the most severe security vulnerabilities. The OWASP Top 10 lists Broken Access Control as the number one web application security risk, present in 94% of applications tested. The Verizon Data Breach Investigations Report finds that privilege misuse and credential abuse account for a substantial portion of confirmed breaches.

When breaches occur, organizations need to answer critical questions: Who had access to what? When did permissions change? Was this access legitimate at the time it was granted?

Traditional authorization systems struggle to provide definitive answers. Audit logs can be modified, timestamps can be forged, and reconstructing historical state requires trusting the system that may have been compromised.

### The Performance-Verifiability Trade-off

Cryptographic verification typically imposes performance costs. Merkle Patricia Tries (MPTs), used by Ethereum and many blockchain systems, require O(log n) operations per key access. Each state update triggers multiple tree rebalances and hash recomputations.

For authorization systems handling thousands of permission checks per second, this overhead is prohibitive. A single request to a microservices application might trigger dozens of authorization checks. Adding 10-50ms of latency per check would degrade user experience unacceptably.

### Ledger's Approach

InferaDB Ledger takes a different path. Rather than embedding cryptographic structures in the hot path, it separates concerns:

1. **State storage** uses conventional high-performance data structures optimized for read latency
2. **State commitment** runs in parallel, computing cryptographic proofs without blocking queries
3. **Verification** happens on-demand, typically during audits or incident response

This separation enables sub-millisecond authorization checks during normal operation while maintaining a complete cryptographic history for when it matters most.

## 2. Background

### Authorization Data Model

Ledger follows the relationship-based access control model pioneered by Google's Zanzibar system. Authorization state consists of tuples:

```
(resource, relation, subject)
```

For example: `(document:budget-2024, viewer, user:alice)` grants Alice viewer access to the budget document.

This model supports:

- **Direct relationships**: Explicit grants between resources and subjects
- **Computed relationships**: Permissions derived from other relationships (e.g., editors are also viewers)
- **Hierarchical resources**: Permissions that flow through containment (e.g., folder access implies document access)

### Consistency Requirements

Authorization decisions must be consistent across the cluster. A permission granted on one node must be visible to all nodes before any node can act on it. Stale reads could allow access that should have been revoked.

Ledger uses Raft consensus to ensure linearizable writes. All mutations flow through a single leader, are replicated to a majority of nodes, and only then become visible to readers. This provides strong consistency guarantees at the cost of write throughput.

### Prior Art

**Google Zanzibar** introduced relationship-based access control at scale, handling millions of authorization checks per second. However, Zanzibar optimizes for availability over verifiability—it does not provide cryptographic proofs of state.

**SpiceDB and OpenFGA** are open-source implementations of the Zanzibar model. They provide excellent authorization semantics but inherit the same limitation: audit logs without cryptographic guarantees.

**Blockchain databases** like BigchainDB and Hyperledger Fabric provide cryptographic verification but impose significant performance overhead. They are designed for scenarios where verification happens on every read, not just during audits.

## 3. Architecture

### Component Overview

Ledger consists of four primary layers:

```
┌─────────────────────────────────────────────────────────────┐
│                      gRPC Services                          │
│         Read | Write | Admin | Health | Discovery           │
├─────────────────────────────────────────────────────────────┤
│                    Consensus Layer                          │
│              Raft (OpenRaft) + Batching                     │
├─────────────────────────────────────────────────────────────┤
│                     State Layer                             │
│     Entity Store | Relationship Store | State Roots         │
├─────────────────────────────────────────────────────────────┤
│                    Storage Layer                            │
│           B+ Tree Engine | Page Management                  │
└─────────────────────────────────────────────────────────────┘
```

**gRPC Services** expose the public API. ReadService handles queries; WriteService processes mutations; AdminService manages namespaces and vaults; HealthService provides liveness and readiness checks.

**Consensus Layer** implements Raft using the OpenRaft library (v0.9+). All writes are proposed to the leader, replicated to followers, and committed only after majority acknowledgment. A batching layer aggregates multiple client requests into single Raft proposals, amortizing consensus overhead.

**State Layer** maintains the domain model. Entities store key-value data with versioning and TTL support. Relationships store authorization tuples. The StateLayer applies blocks and computes state roots.

**Storage Layer** provides durable persistence via a custom B+ tree implementation with page-level management. Backend-agnostic design supports both file-based production storage and in-memory testing.

### Isolation Model

Ledger isolates tenants through a three-level hierarchy:

- **Namespace**: Top-level isolation boundary. Each namespace has independent storage. Cross-namespace operations are prohibited.
- **Vault**: Container for related authorization data within a namespace. Each vault maintains its own blockchain with independent state roots.
- **Shard**: Operational grouping for consensus. A shard contains one or more namespaces and maps to a Raft group.

This isolation model enables:

1. **Fault containment**: A bug affecting one vault cannot corrupt another vault's state
2. **Independent verification**: Auditors can verify a single vault without accessing other data
3. **Flexible deployment**: Different vaults can have different replication factors based on criticality

### Write Path

A write request follows this path:

1. Client sends WriteRequest to any node via gRPC
2. Non-leader nodes forward to current leader
3. Leader validates request and assigns sequence number for idempotency
4. Batcher aggregates request with others (up to 100 transactions or 5ms timeout)
5. Batch becomes a Raft proposal
6. Leader replicates proposal to followers
7. Majority acknowledgment commits the proposal
8. State layer applies transactions, updating indexes and computing new state root
9. Response returns to client with new state root hash

Total write latency targets sub-50ms at the 99th percentile under normal load.

### Read Path

Read requests take a simpler path:

1. Client sends ReadRequest to any node
2. For linearizable reads: node confirms leadership or forwards to leader
3. For eventually consistent reads: node serves directly from local state
4. Storage layer retrieves data from B+ tree indexes
5. Response includes current state root for optional client-side verification

Read latency targets sub-2ms at the 99th percentile.

## 4. State Root Computation

### The Challenge

Traditional Merkle Patricia Tries recompute hashes from leaf to root on every update. For a tree with n keys, each update requires O(log n) hash computations. Worse, tree rebalancing can trigger cascading updates affecting many nodes.

At scale, this becomes prohibitive. A vault with millions of relationships would spend more time maintaining the Merkle tree than serving authorization requests.

### Bucket-Based Approach

Ledger uses a bucket-based merkleization scheme inspired by research on efficient state commitments (QMDB, SeiDB). Instead of a per-key tree structure, keys are distributed across 256 buckets based on the first byte of their hash.

Each bucket maintains:

- A sorted list of (key, value) pairs
- A running hash updated incrementally as pairs change
- Metadata for efficient proof generation

The state root is computed as:

```
state_root = SHA-256(bucket_hash[0] || bucket_hash[1] || ... || bucket_hash[255])
```

### Complexity Analysis

When a block contains k key updates:

1. Identify affected buckets: O(k)
2. Update bucket hashes: O(k) hash operations
3. Recompute state root: O(256) = O(1) hash operations

Total: **O(k)** where k is the number of modified keys, independent of total database size.

Compare to naive MPT: O(k × log n) where n is total keys.

For a vault with 10 million keys and a block updating 100 keys:

- Bucket approach: ~356 hash operations
- Naive MPT: ~100 × 23 = 2,300 hash operations

The bucket approach provides approximately 6x fewer hash operations in this scenario.

### Write Amplification

Storage writes tell a similar story. Each key update in Ledger requires:

1. Write to entity/relationship store
2. Update to bucket state
3. Write to block log

Total: **3 key-value writes per update**

A naive MPT implementation updating internal nodes would require approximately 15-45 writes per update depending on tree depth and rebalancing.

### Trade-offs

The bucket approach sacrifices proof size for computation efficiency. Proving a single key's inclusion requires:

1. The key's bucket contents (or a sub-proof)
2. All 255 other bucket hashes
3. The state root

For sparse proofs, this is larger than an MPT proof of O(log n) hashes. However, for audit scenarios that verify entire vault state rather than individual keys, the trade-off favors bucket-based approaches.

## 5. Fault Isolation and Recovery

### Per-Vault Chains

Each vault maintains an independent blockchain. A vault's chain contains:

- **Block headers**: Height, timestamp, previous hash, state root, transactions root
- **Transaction log**: All mutations applied to this vault
- **State snapshots**: Periodic checkpoints for efficient recovery

Independence means a corrupted vault does not affect siblings. If vault A experiences a Byzantine failure (hardware corruption, software bug), vault B continues operating normally.

### Automatic Recovery

When Ledger detects state divergence—for example, a follower computing a different state root than the leader—it initiates automatic recovery:

1. Mark affected vault as `DIVERGED` (blocks new writes)
2. Identify last known-good state root
3. Replay transactions from that point
4. If replay succeeds, mark vault as `HEALTHY`
5. If replay fails, escalate for manual intervention

The recovery process uses a circuit breaker pattern: 3 attempts with exponential backoff (1s, 2s, 4s) before escalating.

### Consistency Verification

Followers continuously verify state against the leader:

1. Apply block received from leader
2. Compute local state root
3. Compare against leader's state root in block header
4. If mismatch, trigger divergence detection

This catches non-determinism bugs (timestamp dependencies, floating-point, hash iteration order) before they propagate.

## 6. Performance Characteristics

### Targets

Ledger targets the following performance characteristics:

| Metric              | Target          | Condition             |
| ------------------- | --------------- | --------------------- |
| Read latency (p99)  | < 2 ms          | Single key lookup     |
| Write latency (p99) | < 50 ms         | Batch committed       |
| Write throughput    | 5,000 tx/sec    | 3-node cluster        |
| Read throughput     | 100,000 req/sec | Eventually consistent |

These are design targets validated through benchmarks in development. Production performance depends on hardware, network conditions, and workload characteristics.

### Batching Impact

Write batching significantly affects both latency and throughput:

- **Without batching**: Each write incurs full Raft round-trip (~10-20ms network + consensus)
- **With batching**: Multiple writes share consensus overhead

Default batch configuration:

- Maximum batch size: 100 transactions
- Maximum batch delay: 5 milliseconds

A batch of 100 transactions with 5ms delay achieves 20,000 tx/sec theoretical throughput. Actual throughput depends on transaction size, network latency, and disk I/O.

### Idempotency

Clients include a `client_id` and `sequence` number with each request. The leader maintains an in-memory map of recently processed sequences. Duplicate requests return the cached response without re-execution.

This enables safe client retries on timeout without risking duplicate mutations. The cache evicts entries after a configurable TTL (default: 60 seconds).

**Limitation**: The idempotency cache does not survive leader failover. Clients should use application-level idempotency keys for critical operations.

## 7. Limitations and Trade-offs

### No Byzantine Fault Tolerance

Ledger uses Raft, a crash fault-tolerant consensus protocol. It tolerates (n-1)/2 node failures in a cluster of n nodes. It does **not** tolerate Byzantine (malicious or arbitrary) failures.

If a node is compromised and sends incorrect data, other nodes may accept it. Ledger detects this through state root verification but cannot prevent a Byzantine leader from proposing invalid blocks.

For environments requiring Byzantine fault tolerance, consider Tendermint-based systems or PBFT variants. These provide stronger guarantees at significant performance cost.

### Single-Leader Write Bottleneck

All writes flow through the Raft leader. This provides strong consistency but limits write scalability. Horizontal scaling requires sharding at the namespace level.

Read scaling is more flexible: any node can serve eventually consistent reads, and linearizable reads only require a leadership check.

### No Cross-Vault Transactions

Transactions are scoped to a single vault. Operations spanning multiple vaults require application-level coordination (saga pattern, two-phase commit).

This limitation enables the per-vault isolation that makes independent verification and fault containment possible.

### No Range Proofs

The bucket-based state root design optimizes for point queries and full-vault verification. It does not efficiently support range proofs (e.g., prove all keys between A and B).

Applications requiring range proofs should consider augmenting Ledger with a separate range-proof structure or using traditional MPT-based systems.

### Eventual Consistency Window

Eventually consistent reads may return stale data during the Raft replication window (typically milliseconds). Applications requiring strict consistency must use linearizable reads, which add latency.

## 8. Comparison with Alternatives

| Capability              | Ledger         | SpiceDB        | Traditional Blockchain |
| ----------------------- | -------------- | -------------- | ---------------------- |
| Authorization model     | Zanzibar-style | Zanzibar-style | Generic                |
| Cryptographic proofs    | Yes            | No             | Yes                    |
| Read latency target     | < 2ms          | < 5ms          | 10-100ms               |
| Write throughput target | 5,000 tx/sec   | 10,000+ tx/sec | 100-1,000 tx/sec       |
| Fault tolerance         | Crash          | Crash          | Byzantine              |
| State verification      | Per-vault      | N/A            | Global                 |

**Choose Ledger when**: You need cryptographic verification of authorization state without blockchain-level latency, and crash fault tolerance is sufficient.

**Choose SpiceDB/OpenFGA when**: You need maximum write throughput and don't require cryptographic proofs. Audit logs suffice for compliance.

**Choose traditional blockchain when**: You need Byzantine fault tolerance and can accept higher latency. Multi-party trust scenarios where no single operator is trusted.

## 9. Conclusion

InferaDB Ledger addresses the gap between high-performance authorization and cryptographic verifiability. By separating state storage from state commitment, it achieves authorization latency competitive with non-verifiable systems while maintaining tamper-evident audit trails.

The bucket-based state root computation reduces hash operations from O(k × log n) to O(k), making cryptographic commitment practical at scale. Per-vault isolation enables independent verification and fault containment.

These benefits come with trade-offs: no Byzantine fault tolerance, single-leader write bottleneck, and no cross-vault transactions. Organizations should evaluate whether these limitations are acceptable for their threat model and operational requirements.

For authorization scenarios requiring both performance and verifiability—regulatory compliance, security-critical applications, or environments where audit integrity matters—Ledger provides a practical middle ground between traditional databases and full blockchain systems.

---

## References

1. Zanzibar: Google's Consistent, Global Authorization System. USENIX ATC 2019.
2. OWASP Top 10:2021. https://owasp.org/Top10/
3. Verizon Data Breach Investigations Report. https://www.verizon.com/business/resources/reports/dbir/
4. OpenRaft: Raft consensus library. https://github.com/datafuselabs/openraft
5. QMDB: Quick Merkle Database. https://arxiv.org/abs/2501.05262
6. Raft: In Search of an Understandable Consensus Algorithm. USENIX ATC 2014.

---

_This document describes InferaDB Ledger version 0.x (pre-1.0). Architecture and performance characteristics may change before stable release._
