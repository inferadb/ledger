# FAQ

Frequently asked questions about InferaDB Ledger.

## Architecture

### Why doesn't Ledger authenticate its own gRPC connections?

Ledger runs within a secure network perimeter (WireGuard, VPC) and trusts all incoming connections at the network layer. Only Engine and Control can reach Ledger's gRPC port.

However, Ledger **is** the JWT signing authority for the platform — its TokenService issues, validates, and revokes JWTs consumed by Engine and Control for end-user authentication. The distinction: Ledger doesn't authenticate _who is calling it_, but it _issues the tokens_ that authenticate end users elsewhere.

See [Security](operations/security.md) for the trust model.

### How does Ledger relate to Engine and Control?

- **Engine**: Evaluates authorization policies, performs graph traversal. Reads/writes relationships to Ledger.
- **Control**: Manages users and organizations. Delegates token issuance and validation to Ledger's TokenService.
- **Ledger**: Stores authorization data with cryptographic verification. Does not interpret the data semantically.

Engine and Control are the only intended clients of Ledger.

### Why use Raft instead of a Byzantine fault-tolerant consensus?

Ledger operates within a trusted network where all nodes are honest. Byzantine fault tolerance (BFT) adds significant complexity and overhead for a threat model that doesn't apply. Raft provides crash fault tolerance with simpler implementation and better performance.

### What is a vault?

A vault is an isolated authorization store within an organization. Each vault has its own blockchain (genesis block, transaction history, state root). Vaults enable:

- Tenant isolation within an organization
- Independent scaling per region
- Separate retention policies

### What is an organization?

An organization is an organizational boundary (typically one per customer/org). Organizations are assigned to a geographic region for data residency and scaling. Multiple vaults can exist within an organization.

## Operations

### How do I add a node to an existing cluster?

1. Start the new node with `--join`:

   ```bash
   inferadb-ledger --join --data /data --peers existing-cluster.internal
   ```

2. From an existing cluster member, add the node:
   ```bash
   grpcurl -plaintext existing-node:50051 \
     -d '{"node_id": {"id": "NEW_NODE_ID"}, "addr": "new-node:50051"}' \
     ledger.v1.AdminService/JoinCluster
   ```

### How do I remove a node from the cluster?

```bash
grpcurl -plaintext any-node:50051 \
  -d '{"node_id": {"id": "NODE_TO_REMOVE"}}' \
  ledger.v1.AdminService/LeaveCluster
```

The node gracefully transfers leadership (if leader) and removes itself from Raft membership.

### What happens if the leader fails?

Raft automatically elects a new leader within seconds. During election:

- Writes are temporarily unavailable (return `UNAVAILABLE`)
- Reads continue on followers

Clients should retry with exponential backoff.

### How do I backup Ledger?

Each vault maintains a complete transaction history (blockchain). Backups are automatic through:

1. Raft snapshots (automatic)
2. Block archive (if retention policy is `FULL`)

For manual backup, trigger a snapshot:

```bash
grpcurl -plaintext localhost:50051 \
  ledger.v1.AdminService/CreateSnapshot
```

### How do I restore from backup?

1. Stop the node
2. Copy snapshot files to `{data_dir}/raft/snapshots/`
3. Start the node
4. Node automatically restores from the latest snapshot

### What does `VAULT_UNAVAILABLE` mean?

The vault has detected a state divergence (determinism bug). This is a critical error indicating the vault's state root doesn't match what was expected after replaying transactions.

**Resolution:**

1. Check `ledger_determinism_bug_total` metric
2. Run `RecoverVault` to replay from block archive
3. If recovery fails, restore from backup on healthy nodes

### Why are writes slow?

Common causes:

| Symptom                       | Cause                           | Solution                                            |
| ----------------------------- | ------------------------------- | --------------------------------------------------- |
| High `proposals_pending`      | Network latency to followers    | Check network, consider geographically closer nodes |
| High `apply_latency`          | Disk I/O bottleneck             | Use faster storage (NVMe SSD)                       |
| Low `batch_coalesce_size` avg | Low write volume or concurrency | Normal; reduce `batch_timeout` for lower latency    |

### Can I run a single-node cluster?

Yes, for development and testing:

```bash
inferadb-ledger --single --data /tmp/ledger
```

Not recommended for production. Single-node provides no fault tolerance.

## Data Model

### What's the difference between entities and relationships?

- **Entity**: Key-value pair with optional TTL and versioning. Used for arbitrary data storage.
- **Relationship**: Authorization tuple (resource, relation, subject). Used for access control.
- **Credential**: User authentication factor (passkey, TOTP, recovery code). Stored in the system organization, encrypted via `EncryptedUserSystemRequest`.

Entities and relationships are stored in vaults and included in the blockchain. Credentials are stored in the system organization's REGIONAL state layer.

### Can I query historical state?

Yes. All read operations accept an optional `height` parameter:

```bash
grpcurl -plaintext \
  -d '{"organization": {"slug": 1234567890}, "key": "user:alice", "height": "100"}' \
  localhost:50051 ledger.v1.ReadService/Read
```

Historical data availability depends on the vault's retention policy.

### What is a state root?

The state root is a cryptographic hash (SHA-256) representing the entire vault state at a specific block height. It's the root of a Merkle tree over all entities and relationships.

State roots enable:

- Detecting divergence across replicas
- Generating and verifying Merkle proofs
- Cryptographic audit trails

## Performance

### What latency should I expect?

| Operation     | p50   | p99   |
| ------------- | ----- | ----- |
| Write         | <10ms | <50ms |
| Read          | <1ms  | <5ms  |
| Verified read | <2ms  | <10ms |

Actual latency depends on network, disk, and cluster configuration.

### How many writes per second can Ledger handle?

With default batching settings on a 3-node cluster:

- **Single vault**: 5,000-10,000 writes/second
- **Multiple vaults**: Scales linearly (each vault is independent)

Throughput can be increased by:

- Increasing `batch_size`
- Using multiple vaults for parallel workloads
- Distributing organizations across regions

### Does Ledger support horizontal scaling?

Yes, through geographic regions:

- Each region is an independent Raft group
- Organizations are assigned to regions based on data residency requirements
- New regions can be activated without downtime

Cross-region operations are not supported; design your organization placement accordingly.

## Debugging

### How do I enable debug logging?

```bash
RUST_LOG=debug inferadb-ledger --single

# Module-specific
RUST_LOG=inferadb_ledger_raft=debug,inferadb_ledger_state=info inferadb-ledger

# Raft consensus
RUST_LOG=inferadb_ledger_consensus=debug,inferadb_ledger_raft=debug inferadb-ledger
```

### Where are the logs?

Ledger logs to stderr. In container environments:

```bash
# Docker
docker logs ledger

# Kubernetes
kubectl logs ledger-0
```

For persistent logging, configure a log aggregator (Loki, CloudWatch, etc.).

### How do I check if a vault is healthy?

```bash
grpcurl -plaintext \
  -d '{"organization": {"slug": 1234567890}, "vault": {"slug": 7180591718400}}' \
  localhost:50051 ledger.v1.ReadService/GetTip
```

If successful, the vault is operational. If it returns `VAULT_UNAVAILABLE`, the vault has diverged and needs recovery.
