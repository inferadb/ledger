# Phase B.1 — Three-tier consensus (System / Region / Organization): Implementation Spec

**Status:** Draft, ready to plan.
**Supersedes:** `2026-04-20-phase-a-multi-shard-per-region.md` Steps 6–9 (the fixed-N-shard model). Phase A Steps 1–5 stay shipped — their per-`(region, X)` storage + consensus + apply primitives are exactly what B.1 builds on.
**Goal:** Replace fixed-`shards_per_region` with a three-tier consensus hierarchy. The cluster has one **system Raft group** (control plane), each data region has one **region Raft group** (regional control plane and unified leader for the region's organization Rafts), and each organization has its own **organization Raft group** (data plane). Tier responsibilities are disjoint and enforced by the type system.

---

## Goal

Per-organization data isolation is perfect: a hot organization cannot starve a quiet one. Every organization owns its Raft group, its WAL, its state/raft/blocks/events DBs, its apply worker, its batch writer, its checkpointer. The organization id _is_ the routing key — no hash function, no fixed shard count.

Operationally, the system stays simple even at hundreds of orgs per region because the **region Raft elects one leader for the entire region** and that leader is also the leader for every organization's Raft group in that region. There is no per-organization election, no per-organization leader transfer, no per-organization lease management.

The data plane and the control plane are strictly separated at the type level. Entity writes apply through `OrganizationGroup` only. Placement / hibernation / wake apply through `RegionGroup` only. Cluster membership applies through `SystemGroup` only. Crossing tier boundaries is a compile error.

**Concretely (Phase B.1, eager mode):**

- An organization's Raft group is created on `CreateOrganization` and runs for the organization's lifetime.
- No hibernation in B.1 — every created organization has live tasks.
- Suitable for tens-to-hundreds of organizations per region (target horizon: 12–36 months on big cloud nodes).
- Hibernation arrives in B.2; cluster-membership reconciliation in B.3.

**Non-goals (deferred to B.2 / B.3):**

- Hibernation / wake (idle organization resource reclaim) — B.2.
- <100ms cold-start optimization — B.2 (becomes trivial under unified leadership; design doc still warranted).
- Cluster-membership reconciliation across many organizations — B.3.
- Cross-organization atomic writes — out of scope; sagas handle eventual consistency.
- Per-organization self-election (load balancing, finer fault isolation) — runtime escape hatch reserved via `LeadershipMode` but not exercised in B.1.

---

## Locked design decisions

Confirmed through 2026-04-20 design conversation:

1. **Vocabulary cut.** "Shard" disappears from the codebase. Use `organization_id` (and the type `OrganizationId`) everywhere. The metric label `shard` becomes `organization_id`. Use the full word `Organization` in type names — `OrganizationRequest`, `OrganizationGroup`, `OrganizationApplyWorker` — never the abbreviation `Org`.

2. **Storage layout** mirrors the conceptual model — one directory per region, one subdirectory per organization within it, plus a `_meta/` subdirectory for the region Raft itself:

   ```
   {data_dir}/global/0/{state,raft,blocks,events}.db + wal/   # the system group
   {data_dir}/{region}/_meta/{state,raft,events}.db + wal/    # the region group
   {data_dir}/{region}/{organization_id}/{state,raft,blocks,events}.db + wal/
   ```

   The `_meta/` underscore prefix matches the existing storage-key naming convention (`_dir:`, `_meta:`, `_idx:`); the region Raft has no `blocks.db` because it does not maintain a Merkle chain.

3. **Three peer Raft tiers, no inheritance.** `SystemGroup`, `RegionGroup`, `OrganizationGroup` are distinct types with distinct request enums (`SystemRequest`, `RegionRequest`, `OrganizationRequest`) and distinct apply pipelines (`SystemApplyWorker`, `RegionApplyWorker`, `OrganizationApplyWorker`). The compiler enforces tier discipline.

4. **Uniform replication within a region.** Every node hosting any organization in region R is a voter for _every_ organization in R. The region's voter set IS each organization's voter set. This:
   - Eliminates voter selection algorithms — voters = all in-region nodes
   - Makes Raft Leader Completeness trivially hold under unified leadership
   - Trades modest extra storage on big cloud nodes for huge operational simplicity
   - Strengthens durability (more replicas) at marginal cost

5. **Unified leadership via lease delegation.** Each `OrganizationGroup` runs in `LeadershipMode::Delegated { source: RegionGroupRef }`. Election timeouts are disabled on organization Rafts; the elected leader of the region group is automatically the leader for every organization in that region for that term. Region leader changes propagate atomically to every organization Raft on every node via the region group's apply pipeline.

6. **`LeadershipMode::SelfElect` reserved as a runtime escape hatch.** A future per-org self-election mode is implementable as a one-field flip on `OrganizationGroup`; the data plane (replication, apply, state) is mode-agnostic. B.1 does not exercise this mode.

7. **System group lives at GLOBAL exclusively.** It is the only inhabitant of the GLOBAL region — the system group simultaneously plays the cluster control-plane role and the GLOBAL data-plane role. There is no separate region group for GLOBAL. The system group bootstraps at startup; every other group is created on demand.

8. **`CreateOrganization` apply is a multi-tier orchestration.** System group apply records the organization in the cluster directory and notifies the target region group; region group apply chooses placement (in B.1: trivially, all in-region nodes) and triggers per-node organization-group bootstrap. Each step is idempotent so retries after crashes converge.

9. **3 voters per region in protected regions** by default (matching today's per-region voter count). Smaller organizations do not get fewer voters — that would weaken the residency story. Under uniform replication, every organization in such a region inherits the 3-voter set.

10. **`TestCluster` drops eager region setup.** Tests call `cluster.create_data_region(R)` and `cluster.create_organization(R, name)` explicitly when needed. This eliminates the per-shard-membership-fanout class of test infra bugs from Phase A.

11. **Atomic rip-and-replace.** No production users yet → no migration burden. Phase A's fixed-shard machinery is deleted in the same commit series that introduces B.1. No "support both for a release" gradual cutover.

12. **Stop Phase A's pending integration test** (`tests/multi_shard.rs`). It validates a model we're dropping. B.1 ships its own integration test (`tests/three_tier_consensus.rs`).

---

## Concept: three tiers, disjoint responsibilities

```
Cluster
└── System group  (GLOBAL Raft, singleton)        ← cluster control plane
    │   - Cluster membership
    │   - Region directory
    │   - Organization directory
    │   - Cluster-wide signing keys, refresh tokens
    │   - Cross-region saga records
    │
    ├── Region group  (one per data region)        ← regional control plane + unified leader
    │   │   - Organization placement (status, voter set)
    │   │   - Hibernation status (B.2)
    │   │   - Last-known leader hint (B.2)
    │   │   - Region quotas, rate-limit state
    │   │   - Region audit log
    │   │   - Region-scoped saga PII (cross-org-but-single-region case)
    │   │
    │   └── Organization group  (one per organization in this region)  ← data plane
    │       - Entity writes (Write, BatchWrite, IngestExternalEvents)
    │       - Vault lifecycle (CreateVault, UpdateVault, DeleteVault)
    │       - App credentials (CreateApp, RotateAppClientSecret, …)
    │       - User/team memberships within this organization
    │       - Organization-scoped saga PII
    │       - Leader is delegated from the parent region group
```

The tier boundary is a hard invariant: **entity writes never touch the region or system Raft.** A `Write` request always proposes to the destination organization group's log directly. The region group is consulted only for "who is the current leader" — and that consultation is a local in-memory cache lookup, not a Raft proposal.

---

## Storage layout

### Old (Phase A)

```
{data_dir}/
  global/
    shard-0/{state,raft,blocks,events}.db + wal/
  regions/
    {region}/
      shard-0/{state,raft,blocks,events}.db + wal/
      shard-1/...
```

### New (Phase B.1)

```
{data_dir}/
  global/
    0/                                  # system group (= the GLOBAL Raft)
      state.db
      raft.db
      blocks.db
      events.db
      wal/

  us-east-va/
    _meta/                              # region group (regional control plane)
      state.db
      raft.db
      events.db                         # region audit / placement events
      wal/
      # No blocks.db — region group has no Merkle chain

    304789...12345/                     # organization group
      state.db
      raft.db
      blocks.db
      events.db
      wal/

    304789...67890/                     # another organization group
      state.db
      raft.db
      blocks.db
      events.db
      wal/

  ie-east-dub/
    _meta/...
    304789...11111/...
```

**Key path properties:**

- `{data_dir}/{region}/{organization_id}/...` for organization data (your simplification — no `regions/` parent, no `orgs/` subdirectory, no `shard-N/` subdirectory).
- `{data_dir}/{region}/_meta/...` for the region group itself (underscore prefix matches existing storage-key naming convention; lexically distinguishable from numeric `organization_id` directories).
- GLOBAL keeps the `global/` top-level directory (the system group is special — it has no parent region group above it).

---

## Type changes

### Three peer group types (no shared base)

```rust
// crates/raft/src/system_group.rs
pub struct SystemGroup {
    handle: Arc<ConsensusHandle>,                    // its consensus engine
    state: Arc<StateLayer<FileBackend>>,             // GLOBAL state.db
    raft_db: Arc<Database<FileBackend>>,
    blocks_db: Arc<Database<FileBackend>>,           // system has a chain (saga records, etc.)
    events_db: Option<Arc<Database<FileBackend>>>,
    block_archive: Arc<BlockArchive<FileBackend>>,
    applied_state: AppliedStateAccessor,
    block_announcements: broadcast::Sender<BlockAnnouncement>,
    background_jobs: parking_lot::Mutex<RegionBackgroundJobs>,
    // System group does NOT use a BatchWriter — admin-style traffic is
    // low-concurrency, latency-sensitive. Direct propose only.
    leader_lease: Arc<LeaderLease>,
    applied_index_rx: watch::Receiver<u64>,
    consensus_transport: Arc<ConsensusTransport>,
    // Notifies region groups when CreateDataRegion / CreateOrganization
    // applies — replaces today's region_creation_rx mechanism.
    coordination_event_tx: tokio::sync::mpsc::UnboundedSender<CoordinationEvent>,
    last_activity: Arc<parking_lot::Mutex<Instant>>,
    jobs_active: Arc<AtomicBool>,
}

// crates/raft/src/region_group.rs
pub struct RegionGroup {
    region: Region,
    handle: Arc<ConsensusHandle>,
    state: Arc<StateLayer<FileBackend>>,             // region state.db (placement, hibernation, etc.)
    raft_db: Arc<Database<FileBackend>>,
    events_db: Option<Arc<Database<FileBackend>>>,   // region audit events
    // No blocks.db — region group does not maintain a Merkle chain.
    applied_state: AppliedStateAccessor,
    background_jobs: parking_lot::Mutex<RegionBackgroundJobs>,
    leader_lease: Arc<LeaderLease>,                  // THE source of truth for leadership in this region
    applied_index_rx: watch::Receiver<u64>,
    consensus_transport: Arc<ConsensusTransport>,
    // Notifies organization groups on this node when the region leader
    // changes. Drives the LeadershipMode::Delegated propagation.
    leader_change_tx: tokio::sync::watch::Sender<RegionLeaderState>,
    // Notifies organization-group bootstrap on PlaceOrganization apply.
    organization_event_tx: tokio::sync::mpsc::UnboundedSender<OrganizationEvent>,
    last_activity: Arc<parking_lot::Mutex<Instant>>,
    jobs_active: Arc<AtomicBool>,
}

// crates/raft/src/organization_group.rs
pub struct OrganizationGroup {
    region: Region,
    organization_id: OrganizationId,
    handle: Arc<ConsensusHandle>,                    // delegated-leadership consensus engine
    state: Arc<StateLayer<FileBackend>>,             // organization state.db
    raft_db: Arc<Database<FileBackend>>,
    blocks_db: Arc<Database<FileBackend>>,
    block_archive: Arc<BlockArchive<FileBackend>>,
    events_db: Option<Arc<Database<FileBackend>>>,
    applied_state: AppliedStateAccessor,
    block_announcements: broadcast::Sender<BlockAnnouncement>,
    background_jobs: parking_lot::Mutex<RegionBackgroundJobs>,
    batch_handle: Option<BatchWriterHandle>,         // BatchWriter on the data plane
    commitment_buffer: Arc<Mutex<Vec<StateRootCommitment>>>,
    leadership_mode: LeadershipMode,                 // Delegated in B.1; SelfElect reserved
    applied_index_rx: watch::Receiver<u64>,
    last_activity: Arc<parking_lot::Mutex<Instant>>,
    jobs_active: Arc<AtomicBool>,
    // No consensus_transport field — organization groups share the
    // region group's transport (one TCP connection per peer per region,
    // multiplexed by ShardId).
}
```

### Three peer request enums

```rust
// crates/raft/src/types.rs

pub enum SystemRequest {
    CreateDataRegion { region: Region },
    DeleteDataRegion { region: Region },             // future; B.1 may stub
    CreateOrganization { slug: u64, region: Region, name: String },
    DeleteOrganization { id: OrganizationId },
    UpdateOrganizationStatus { id: OrganizationId, status: OrganizationStatus },
    CreateSigningKey { ... },
    RotateSigningKey { ... },
    CreateRefreshToken { ... },
    DeleteExpiredRefreshTokens,
    SaveSaga { saga_id: SagaId, payload: Vec<u8> },  // cross-region saga records
    DeleteSaga { saga_id: SagaId },
    AddClusterVoter { node_id: NodeId, address: String },
    RemoveClusterVoter { node_id: NodeId },
    // ... other cluster-wide variants
}

pub enum RegionRequest {
    PlaceOrganization { id: OrganizationId, voter_set: Vec<NodeId> },
    UnplaceOrganization { id: OrganizationId },
    UpdateOrganizationVoters { id: OrganizationId, voters: Vec<NodeId> },  // B.3
    HibernateOrganization { id: OrganizationId, last_known_leader: NodeId, lease_expiry: Timestamp },  // B.2
    WakeOrganization { id: OrganizationId },                                                            // B.2
    UpdateRegionQuota { id: OrganizationId, quota: Quota },
    SaveRegionalSagaPii { saga_id: SagaId, payload: Vec<u8> },  // cross-org-single-region case
    DeleteRegionalSagaPii { saga_id: SagaId },
    AddRegionVoter { node_id: NodeId },
    RemoveRegionVoter { node_id: NodeId },
}

pub enum OrganizationRequest {
    Write { vault: VaultId, transactions: Vec<Transaction>, idempotency_key: [u8; 16], request_hash: u64 },
    BatchWrite { requests: Vec<OrganizationRequest> },
    CreateVault { ... },
    UpdateVault { ... },
    DeleteVault { ... },
    UpdateVaultHealth { ... },
    CreateApp { ... },
    DeleteApp { ... },
    RotateAppClientSecret { ... },
    CreateAppClientAssertion { ... },
    DeleteAppClientAssertion { ... },
    AddAppVault { ... },
    UpdateAppVault { ... },
    AddOrganizationMember { ... },
    UpdateOrganizationMemberRole { ... },
    CreateOrganizationInvite { ... },
    CreateOrganizationTeam { ... },
    DeleteOrganizationTeam { ... },
    CreateUser { ... },
    UpdateUser { ... },
    DeleteUser { ... },
    IngestExternalEvents { ... },
    SaveOrganizationSagaPii { saga_id: SagaId, payload: Vec<u8> },  // single-org saga state
    DeleteOrganizationSagaPii { saga_id: SagaId },
    // Notably absent: `organization` field — implicit in the routing target.
}
```

`OrganizationRequest::Write` carries no `organization` field. The request is implicitly for the organization whose group accepted it; the `OrganizationId` is the routing key, not a payload field. This eliminates the "what if the request's organization_id doesn't match the routing target?" inconsistency class entirely.

### Three apply pipelines

```rust
pub struct SystemApplyWorker { /* applies SystemRequest only */ }
pub struct RegionApplyWorker { /* applies RegionRequest only */ }
pub struct OrganizationApplyWorker { /* applies OrganizationRequest only */ }
```

Each is dedicated to its tier's request type. `OrganizationApplyWorker` is the per-organization parallelism that delivers the throughput target (one apply pipeline per organization, all running in parallel on the region leader's node).

### `LeadershipMode` for organization groups

```rust
pub enum LeadershipMode {
    /// B.1 default: leader is whoever leads the parent region group.
    /// Election timeouts on this organization's `Shard` are disabled.
    /// The `Shard` receives `SetLeader { node, term }` events from the
    /// region group's apply path whenever the region leader changes.
    Delegated { source: Arc<RegionLeaderState> },

    /// Future B.x escape hatch: the organization runs its own elections.
    /// Election timeouts active; standard Raft. Not exercised in B.1.
    SelfElect,
}
```

The data plane (replication via AppendEntries, commit-index advancement, apply pipeline, state machine) is mode-agnostic. To flip an organization to self-election later: change one field, restart the `Shard`'s timer machinery. Apply pipeline, WAL, replication path, state — all untouched.

### Removed types and fields

| Old (Phase A)                                                         | Status         | Replaced by                                                                                               |
| --------------------------------------------------------------------- | -------------- | --------------------------------------------------------------------------------------------------------- |
| `state::shard_routing` module (entire)                                | **deleted**    | direct `(Region, OrganizationId)` lookup                                                                  |
| `state::shard_routing::ShardIdx`                                      | **deleted**    | `OrganizationId`                                                                                          |
| `state::shard_routing::ShardRouter`                                   | **deleted**    | no router needed                                                                                          |
| `raft::raft_manager::RegionGroup` (Phase A type)                      | **renamed**    | This name is now used for the region control-plane group. The data-plane analogue is `OrganizationGroup`. |
| `RaftManager::regions: HashMap<(Region, ShardIdx), Arc<RegionGroup>>` | **removed**    | Replaced by three maps below                                                                              |
| `RaftManager::start_region_shards(...)`                               | **deleted**    | `start_organization_group(region, organization_id)` triggered by region apply                             |
| `RaftManager::has_shard / get_shard_group / list_shards / shards_for` | **deleted**    | `has_organization / get_organization_group / list_organizations`                                          |
| `RaftManagerConfig::shards_per_region`                                | **deleted**    | Organization count is dynamic; no config knob                                                             |
| `Config::shards_per_region` (server CLI)                              | **deleted**    |                                                                                                           |
| `--shards-per-region` flag, `INFERADB__LEDGER__SHARDS_PER_REGION` env | **deleted**    |                                                                                                           |
| `LedgerRequest` (mega-enum)                                           | **split**      | `SystemRequest` + `RegionRequest` + `OrganizationRequest`                                                 |
| `LedgerRequest::AddRegionLearner`                                     | **superseded** | `RegionRequest::AddRegionVoter` (region group membership)                                                 |
| `ConsensusHandle::shard_idx` field + accessor                         | **renamed**    | drops to `organization_id` for org handles; region group handles have no organization_id                  |
| `LeaderHint.shard_idx` (SDK)                                          | **renamed**    | `LeaderHint.organization_id`                                                                              |
| `ErrorDetails.context["leader_shard"]`                                | **renamed**    | `["leader_organization"]`                                                                                 |
| `fields::SHARD = "shard"` (metrics)                                   | **renamed**    | `fields::ORGANIZATION_ID = "organization_id"`                                                             |

### `RaftManager` shape

```rust
pub struct RaftManager {
    // ... unchanged fields ...
    system_group: OnceCell<Arc<SystemGroup>>,                                 // singleton
    region_groups: RwLock<HashMap<Region, Arc<RegionGroup>>>,                 // one per data region
    organization_groups: RwLock<HashMap<(Region, OrganizationId), Arc<OrganizationGroup>>>,
}
```

Three maps, three accessors:

- `system_group() -> Arc<SystemGroup>`
- `region_group(region) -> Result<Arc<RegionGroup>>`
- `organization_group(region, organization_id) -> Result<Arc<OrganizationGroup>>`

Routing:

```rust
pub fn route_organization(&self, organization_id: OrganizationId) -> Option<Arc<OrganizationGroup>> {
    let region = self.get_organization_region(organization_id)?;
    self.organization_groups.read().get(&(region, organization_id)).cloned()
}
```

The `system_group` accessor is the only path to system data; it's not addressable via `route_organization`. The region group is similarly only addressable via `region_group(region)`.

### Consensus `ShardId` derivation

Three derivation patterns to keep groups distinct in the consensus engine's internal shard map:

```rust
let system_shard_id = ShardId(seahash::hash(b"_system"));

let region_shard_id = ShardId({
    let mut bytes = Vec::with_capacity(region.as_str().len() + 1);
    bytes.push(b'_');
    bytes.extend_from_slice(region.as_str().as_bytes());
    seahash::hash(&bytes)
});

let organization_shard_id = ShardId({
    let mut bytes = Vec::with_capacity(region.as_str().len() + 8);
    bytes.extend_from_slice(region.as_str().as_bytes());
    bytes.extend_from_slice(&organization_id.value().to_le_bytes());
    seahash::hash(&bytes)
});
```

Operators never see these; they're internal disambiguators within the consensus engine.

---

## Bootstrap sequence

### Single-node fresh start

1. Server process starts.
2. `RaftManager` initializes with empty `region_groups` and `organization_groups` maps.
3. `bootstrap_node` calls `start_system_group([self_node_id], bootstrap = true)`.
4. System group runs leader election — single node trivially wins.
5. Server becomes ready; no data regions or organizations exist yet.

No region groups, no organization groups. The system group is the only consensus group running.

### Cluster join

1. Joining node starts; same flow through step 2 of single-node bootstrap.
2. Joining node calls `JoinCluster` RPC against an existing node.
3. Existing node proposes `SystemRequest::AddClusterVoter(joining_node_id)` to the system group.
4. System group commits + applies → joining node is now a voter in the system group.
5. Joining node's system group catches up via AppendEntries.
6. Joining node sees `SystemRequest::CreateDataRegion` history → starts a region group for each data region it belongs to (in B.1 with uniform replication, every node is in every region).
7. Each region group catches up; on each `RegionRequest::PlaceOrganization` apply, the joining node bootstraps the corresponding `OrganizationGroup` locally (since uniform replication makes the joining node a voter for every organization in the region).

Joining is sequential (system catches up first, then regions, then organizations). For tens-to-hundreds of organizations this is acceptable; B.3 introduces concurrent + lazy reconciliation if scale demands.

### `CreateDataRegion`

`SystemRequest::CreateDataRegion(region)` proposed to the system group:

1. System apply records `_meta:data_region:{region}` (region exists)
2. System apply emits a `CoordinationEvent::DataRegionCreated(region)` to every node's local handler
3. Each node calls `start_region_group(region)`:
   - Creates `{data_dir}/{region}/_meta/`
   - Bootstraps the region group's consensus engine with the cluster's current node set as voters
   - First-voter (lexically-first node id) is `bootstrap=true`; others join
4. Region group runs leader election; one node wins
5. Region group is now the regional control plane and the unified leader source for any future organizations

### `CreateOrganization`

The canonical multi-tier orchestration. Demonstrates tier discipline:

```
Step 1 (System tier):
  Client → SystemService::CreateOrganization
  → System group commits SystemRequest::CreateOrganization { slug, region, name }
  → System apply:
      - Writes _directory:org:{id} in system state (with status: Provisioning)
      - Emits CoordinationEvent::OrganizationProvisioning{ id, region } to local handlers

Step 2 (Region tier):
  Local handler on the region's leader node observes CoordinationEvent
  → Region group leader proposes RegionRequest::PlaceOrganization { id, voter_set }
    (voter_set in B.1 = the region's current voter set, by uniform replication)
  → Region group commits + applies on every region voter
  → Region apply on each voter:
      - Writes _meta:org_placement:{id} in region state (with status: Bootstrapping)
      - Emits OrganizationEvent::Bootstrap { id, voters } to local handler

Step 3 (Organization tier):
  Local handler on each region voter observes OrganizationEvent
  → Calls start_organization_group(region, id, voters)
      - Creates {data_dir}/{region}/{id}/ directory
      - Bootstraps OrganizationGroup in LeadershipMode::Delegated
        (no election; subscribes to RegionLeaderState watch from parent region group)
      - Region group's current leader is automatically this org's leader

Step 4 (Region tier follow-up):
  Region group leader observes that all voters bootstrapped (via heartbeat / status)
  → Proposes RegionRequest::PlaceOrganization status update (Bootstrapping → Active)
  → Region apply: marks _meta:org_placement:{id} status: Active

Step 5 (System tier follow-up):
  Region group leader notifies system group via existing event channel
  → System group leader proposes SystemRequest::UpdateOrganizationStatus { id, Active }
  → System apply: marks _directory:org:{id} status: Active

Organization is ready. First OrganizationRequest::Write proposals can land.
```

Each step is idempotent. A crash at any step leaves the next attempt to converge: re-attempting `CreateOrganization` is a no-op if the directory entry exists; re-attempting `PlaceOrganization` is a no-op if placement exists; re-attempting `start_organization_group` is a no-op if the group is already running. The status-machine progression (`Provisioning` → `Bootstrapping` → `Active`) makes the in-flight state observable for debugging.

### `DeleteOrganization`

The mirror sequence:

```
Step 1: SystemRequest::DeleteOrganization → status: Erasing in directory
Step 2: System notifies region; region proposes RegionRequest::UnplaceOrganization
Step 3: Region apply triggers stop_organization_group on each voter →
        drains in-flight, flushes, closes DBs, deletes {data_dir}/{region}/{id}/ tree
        (after WAL barrier-fsync + crypto-shred of any per-org keys)
Step 4: Region apply marks placement Erased; notifies system
Step 5: System apply marks directory entry Erased (or removes it)
```

---

## Tier discipline: enforcement and invariants

### What the type system catches

- `OrganizationApplyWorker` has signature `apply(req: OrganizationRequest) -> OrganizationResponse`. You cannot pass a `SystemRequest` to it — type error.
- `RegionGroup` exposes `propose(req: RegionRequest)`. Proposing a `Write` here is a type error.
- `SystemGroup::propose(req: SystemRequest)` rejects `OrganizationRequest::Write` at compile time.

There is no `LedgerRequest` mega-enum to accidentally match the wrong variant against.

### Cross-tier communication

Communication across tiers happens through three explicit channels — never by direct mutation:

| Channel                                           | Direction                                                             | Purpose                                                                                                      |
| ------------------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `CoordinationEvent` (system → region)             | System apply → local handler → region group propose                   | Triggers region-tier orchestration on system-tier apply (e.g., DataRegionCreated, OrganizationProvisioning)  |
| `OrganizationEvent` (region → organization)       | Region apply → local handler → start/stop organization group          | Triggers per-organization bootstrap / teardown on region-tier apply                                          |
| `RegionLeaderState` watch (region → organization) | Region apply → broadcast watch → organization Shard `SetLeader` event | Propagates unified leadership: when region leader changes, every organization Shard updates its leader state |

Each channel is uni-directional and well-typed. Region apply cannot directly write to system state; organization apply cannot directly write to region state. Cross-tier writes must round-trip through a proposal at the destination tier.

### Read paths respect the separation

| Reading                                                        | From                                         |
| -------------------------------------------------------------- | -------------------------------------------- |
| Cluster directory (`does org X exist?`)                        | System group state (cached)                  |
| Organization placement (`which voters for org X in region R?`) | Region group state (cached on region voters) |
| Last-known leader (B.2 wake)                                   | Region group state                           |
| Entity data (`Read` RPC)                                       | Organization group state                     |
| Saga records (cross-region)                                    | System group state                           |
| Saga PII (single-org)                                          | Organization group state                     |
| Saga PII (cross-org-single-region, rare)                       | Region group state                           |

The data plane never reads from a control-plane group for entity content; control-plane groups never read from data-plane groups for placement.

### Documented invariants (additions to root `CLAUDE.md`)

To be added under "Golden Rules":

> **Tier discipline.** The three Raft tiers — system group (cluster control plane), region group (regional control plane and unified leader), organization group (data plane) — own disjoint operation sets. Entity writes apply through `OrganizationGroup` only via `OrganizationRequest`; placement / hibernation / wake / region-quota / region-audit apply through `RegionGroup` only via `RegionRequest`; cluster membership / region directory / organization directory / cluster signing keys apply through `SystemGroup` only via `SystemRequest`. The three request enums are disjoint; there is no shared base type. A Vec<u8> opaque payload that "decodes inside the wrong apply pipeline" is the failure mode this discipline prevents — use the typed request enums always. Audited by `consensus-reviewer`.

To be added under `crates/raft/CLAUDE.md` Local Golden Rules:

> **`OrganizationGroup` never proposes coordination state; `RegionGroup` never proposes data; `SystemGroup` never proposes regional or per-organization state.** The three group types have no shared types, no shared apply path, no shared persistence. They communicate exclusively through the typed `CoordinationEvent` / `OrganizationEvent` / `RegionLeaderState` channels. Bypassing these channels (e.g., having a region apply directly mutate organization Raft state) is a tier violation.

> **`OrganizationGroup` defaults to `LeadershipMode::Delegated`.** Election timeouts on its `Shard` are disabled; the leader is whoever leads the parent region group. Per-organization self-election is reserved as a future runtime escape hatch via `LeadershipMode::SelfElect`. The data plane (replication, apply, state) is mode-agnostic; flipping the mode does not require changing the apply path or the state machine.

---

## Saga state placement (clarified)

| Saga state                                                       | Lives in                                          |
| ---------------------------------------------------------------- | ------------------------------------------------- |
| Saga record (`_meta:saga:{id}`) — coordinates the saga lifecycle | System group (cross-region orchestration)         |
| Single-organization saga PII                                     | The user's organization group                     |
| Cross-organization, single-region saga PII (rare)                | Region group's `_meta:regional_saga_pii:{id}`     |
| Cross-region saga PII (very rare)                                | System group's `_meta:cross_region_saga_pii:{id}` |

Most saga PII goes to the user's organization group, which means saga PII lifecycle naturally tracks the organization's lifecycle — erasing the organization erases its saga PII. This is a stronger residency-compliance posture than today's "shard 0 special case".

---

## Voter set policy

In B.1, voter selection is trivially answered by uniform replication:

- **Region group voter set** = every node assigned to the region (which in B.1 equals the cluster's full node set, since regions are not yet operationally fenced by node policy)
- **Organization group voter set** = the same as the region group voter set (uniform replication)

Adding a node to the cluster:

1. `SystemRequest::AddClusterVoter` commits to system group
2. System apply emits a coordination event
3. Each region group leader proposes `RegionRequest::AddRegionVoter` to its own region group
4. Region apply on each voter: starts the new node as a voter for every organization group in the region

In B.1, this is a single coordinated burst; B.3 makes it lazy and incremental for very large organization counts. For tens-to-hundreds, the burst is fine.

Removing a node is the mirror sequence.

---

## Test infrastructure rebuild

`TestCluster::new(size)` bootstraps `size` nodes with the system group only. No region groups, no organization groups.

```rust
impl TestCluster {
    /// Proposes SystemRequest::CreateDataRegion to the system group and
    /// waits for every node's region group to elect a leader.
    pub async fn create_data_region(&self, region: Region) -> Result<()>;

    /// Proposes SystemRequest::CreateOrganization to the system group.
    /// Waits for the multi-tier orchestration (system → region → organization)
    /// to complete on every region voter. Returns the new OrganizationId.
    pub async fn create_organization(
        &self,
        region: Region,
        slug: &str,
    ) -> Result<OrganizationId>;

    /// Returns the OrganizationGroup on `node` for `(region, organization)`,
    /// panicking if the node is not a region voter or the group is not
    /// yet started.
    pub fn organization_group(
        &self,
        node_idx: usize,
        region: Region,
        organization_id: OrganizationId,
    ) -> Arc<OrganizationGroup>;

    /// Returns the RegionGroup on `node` for `region`.
    pub fn region_group(&self, node_idx: usize, region: Region) -> Arc<RegionGroup>;

    /// Returns true when every organization group in `region` has
    /// converged across all nodes (applied indices match, > 0).
    pub fn organizations_synced(&self, region: Region) -> bool;
}
```

Removed:

- `with_data_regions(size, num_data_regions)` — regions and organizations created explicitly per test
- The Phase A per-shard membership-fanout loop in `build` — gone with the model
- The `data_regions_synced` per-shard iteration — replaced by `organizations_synced(region)` per-organization iteration
- The `shards_per_region: 1` config pin — deleted with the field

---

## Phase A test debt to resolve in B.1

Phase A's multi-shard work left the test suite passing only because `TestCluster` was pinned to `shards_per_region = 1`. With production-realistic configuration, **13 tests fail** because Phase A's per-shard membership-fanout in `TestCluster::build` was incomplete. The B.1 model resolves this architecturally — no per-shard membership exists; each organization gets its complete voter set at creation time, propagated by the region group's apply pipeline.

### Tests known to fail under multi-`X` configurations

| Test                                                    | Module               | Phase A failure mode                                      |
| ------------------------------------------------------- | -------------------- | --------------------------------------------------------- |
| `test_key_overwrite_consistency`                        | `leader_failover`    | Read on follower returns `None` after write+wait_for_sync |
| `test_committed_write_survives_leader_crash`            | `leader_failover`    | Committed write missing on follower                       |
| `test_read_consistency_after_leader_change`             | `leader_failover`    | Follower-read inconsistency post-failover                 |
| `test_sequential_writes_readable_on_three_node_cluster` | `leader_failover`    | Some writes missing on followers                          |
| `test_term_agreement_maintained`                        | `leader_failover`    | Term divergence across nodes                              |
| `test_rapid_writes_no_data_loss`                        | `leader_failover`    | Rapid writes lost on followers                            |
| `test_deterministic_block_height_across_nodes`          | `leader_failover`    | Block height divergence                                   |
| `test_ordered_replication`                              | `replication`        | Replication ordering broken across non-shard-0 traffic    |
| `test_follower_state_consistency`                       | `replication`        | Follower state diverges                                   |
| `test_replication_with_idle_gap_between_writes`         | `replication`        | Resume-after-idle missing entries                         |
| `test_leader_failover_mid_batch_no_data_loss`           | `externalized_state` | Mid-batch failover loses entries                          |
| `test_idempotency_survives_leader_failover`             | `design_compliance`  | Idempotency cache divergence                              |
| `test_saga_orchestrator_leader_only`                    | `saga_orchestrator`  | Saga orchestrator state divergence                        |

### B.1 acceptance for these tests

Each test must pass under the B.1 model. The migration pattern is:

```rust
// Before (Phase A, implicit shard-0):
let cluster = TestCluster::new(3).await;
let leader_id = cluster.wait_for_leader().await;
client.write(write_req).await.expect("initial");
client.write(write_req_overwrite).await.expect("overwrite");
cluster.wait_for_sync(Duration::from_secs(5)).await;
for node in cluster.nodes() {
    let value = read_entity(&node.addr, organization, vault, "key").await;
    assert_eq!(value, Some(b"updated".to_vec()));
}

// After (B.1, explicit organization creation):
let cluster = TestCluster::new(3).await;
cluster.create_data_region(Region::US_EAST_VA).await.unwrap();
let org_alpha = cluster.create_organization(Region::US_EAST_VA, "alpha").await.unwrap();
let org_beta = cluster.create_organization(Region::US_EAST_VA, "beta").await.unwrap();
let leader_id = cluster.wait_for_region_leader(Region::US_EAST_VA).await;
client.write(write_req_alpha_initial).await.expect("initial");
client.write(write_req_alpha_overwrite).await.expect("overwrite");
cluster.wait_for_organizations_synced(Region::US_EAST_VA, Duration::from_secs(5)).await;
for node in cluster.nodes() {
    // org_alpha: read should see the latest value
    let value = read_entity(&node.addr, org_alpha, vault, "key").await;
    assert_eq!(value, Some(b"updated".to_vec()));
    // org_beta: read should see no entry (organization isolation)
    let value = read_entity(&node.addr, org_beta, vault, "key").await;
    assert_eq!(value, None);
}
```

Each migrated test now exercises both replication correctness AND organization isolation — strengthens what they validate.

### Definition of done for B.1

Phase B.1 is **not complete** until:

1. All 13 tests above pass under the B.1 model, with explicit multi-organization setup
2. No test-only config divergence from production (no `shards_per_region: 1` pin or equivalent — the field doesn't exist)
3. The new `tests/three_tier_consensus.rs` validates per-organization isolation in scenarios the old tests didn't cover (concurrent organization creation, organization deletion during traffic, restart with multiple organizations)
4. Full `just ci` green at every commit boundary in the cutover series

---

## Sub-task breakdown

| #   | Task                                                                                                                                                                                                                                     | Effort | Depends |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ------- |
| 1   | Storage layout: `{data_dir}/{region}/{organization_id}/` + `{region}/_meta/` for region group + `global/0/` for system group                                                                                                             | 1d     | —       |
| 2   | Type rename: delete `state::shard_routing`; introduce `OrganizationId` everywhere; rename `ShardIdx` references                                                                                                                          | 0.5d   | 1       |
| 3   | Three peer group types: `SystemGroup`, `RegionGroup`, `OrganizationGroup` (`raft::raft_manager::RegionGroup` deleted; replaced)                                                                                                          | 1d     | 2       |
| 4   | Three peer request enums: split `LedgerRequest` into `SystemRequest` / `RegionRequest` / `OrganizationRequest`; update services to construct the right type per call site                                                                | 1.5d   | 3       |
| 5   | Three peer apply pipelines: `SystemApplyWorker` / `RegionApplyWorker` / `OrganizationApplyWorker`                                                                                                                                        | 1d     | 4       |
| 6   | Bootstrap rewrite: system group only at startup; `CreateDataRegion` triggers region group bootstrap; `CreateOrganization` triggers multi-tier orchestration                                                                              | 1.5d   | 3, 5    |
| 7   | `LeadershipMode::Delegated` implementation: extend `Shard` with `SetLeader` event + disable election timeouts when delegated; wire `RegionLeaderState` watch from region apply to organization Shards                                    | 1.5d   | 3       |
| 8   | Routing: `route_organization` becomes one-line `(Region, OrganizationId)` lookup; `route_request` updated for the saga-forwarding case (now per-tier-typed)                                                                              | 0.5d   | 4       |
| 9   | Saga PII placement: route to user's organization group; cross-org-single-region case uses region group                                                                                                                                   | 0.5d   | 4, 6    |
| 10  | `LeaderHint` field rename `shard_idx` → `organization_id` across server + SDK + tests + tests asserting                                                                                                                                  | 0.5d   | 3       |
| 11  | Delete `LedgerRequest::AddRegionLearner` and other obsolete per-region-Raft surface; introduce `RegionRequest::AddRegionVoter` for region group membership                                                                               | 0.5d   | 4       |
| 12  | Delete `RaftManagerConfig::shards_per_region`, server CLI flag, env var, validation                                                                                                                                                      | 0.25d  | 6       |
| 13  | Test infrastructure: `TestCluster::create_data_region` + `create_organization` + `region_group` + `organization_group` + `organizations_synced`; remove eager region setup, `shards_per_region: 1` pin, per-shard membership-fanout loop | 1.5d   | 6       |
| 14  | Migrate 13 known-broken integration tests to explicit organization creation per the documented pattern                                                                                                                                   | 1.5d   | 13      |
| 15  | New `tests/three_tier_consensus.rs` validating create / write / read / delete / restart / concurrent-create flow across multiple organizations and regions                                                                               | 1d     | 14      |
| 16  | Metrics relabel: `shard` → `organization_id` across all helpers + tests                                                                                                                                                                  | 0.5d   | 10      |
| 17  | Documentation: update root `CLAUDE.md` (golden rules: tier discipline), `crates/raft/CLAUDE.md` (group separation invariants), `crates/state/CLAUDE.md` (storage layout)                                                                 | 1d     | 15      |

**Total:** ~14.25 days. Realistic range 10–16 days given debugging interleave and the consensus-engine `LeadershipMode` work in task 7.

---

## What's NOT in B.1

Explicitly out of scope; tracked for B.2 and B.3:

### B.2 (hibernation)

- `OrganizationHibernator` background task (per region group leader)
- Hibernate transition (close DBs, stop tasks, persist `last_known_leader` + `lease_expiry` in region state)
- Wake transition (open DBs, restart tasks; under unified leadership, no election needed — region leader IS the organization leader by construction)
- Hibernation policy (idle timeout, watermark for keep-warm count)
- Idle-organization metrics + dashboards

The wake protocol becomes trivial under unified leadership — the region leader is already the organization leader for every organization, so wake is purely "open this organization's local resources on every region voter." No election, no leadership negotiation. The <100ms cold-start target is achievable on the warm-OS-cache path without further consensus design.

### B.3 (cluster-membership reconciliation)

- Lazy + concurrent voter-set updates per organization group when nodes are added or removed
- Coalesced membership-change propagation across organizations
- `add_node` / `remove_node` operational scaling beyond the B.1 burst model

### Not planned for any B.x phase (would need separate decisions)

- Cross-organization atomic transactions (sagas remain the answer)
- Per-organization self-election via `LeadershipMode::SelfElect` (escape hatch reserved but not exercised)
- Range merging or coalesced heartbeats (only relevant at very large organization counts where heartbeat fan-out becomes a CPU bottleneck)

---

## Validation strategy

### Lib tests

Most lib tests are pure logic and pass unchanged. Tests for `ShardRouter` are deleted. New unit tests cover:

- `route_organization` returns the correct organization group (or `None` for unknown organizations)
- The three apply pipelines reject the wrong tier's request (compile error preferred; runtime guard in serialization-boundary code)
- `LeadershipMode::Delegated` propagates `SetLeader` events from `RegionLeaderState` to organization `Shard`
- Multi-tier `CreateOrganization` orchestration is idempotent: re-running each step from any in-flight status converges

### Integration tests

`tests/three_tier_consensus.rs` (new) validates:

- Create organization A in region R; write to A; read back from every node
- Create organization B in region R; verify writes to A and B land in distinct on-disk directories
- Restart cluster; verify both organizations come back up and previously-committed writes are readable
- Delete organization A; verify directory tree removed; verify B unaffected
- Concurrent-create from many threads → all organizations come up cleanly
- Region group leader transfer: writes for every organization in the region succeed against the new leader without per-organization election delay
- Cross-region: create organization in region R1 and region R2; verify regions' state is independent

13 migrated tests pass per the documented migration pattern.

### Operational validation

- Grafana dashboard with `region` + `organization_id` labels; verify per-organization write rate, apply latency, queue depth visible
- Region-level dashboard showing region group leader, applied index, organization count, organization status histogram
- `inferadb-ledger organizations list` CLI lists active organizations with last-write timestamp + region + status
- `inferadb-ledger regions list` CLI lists data regions with leader, voter count, organization count

---

## Cutover plan

10–13-commit PR series, each green at `just ci`:

1. **Storage layout** (`shard-{N}/` → `{region}/{organization_id}/` + `{region}/_meta/` + `global/0/`)
2. **Type rename** (`ShardIdx` → `OrganizationId`; `state::shard_routing` deleted)
3. **Three peer group types** (`SystemGroup`, `RegionGroup`, `OrganizationGroup`)
4. **Three peer request enums** (`SystemRequest` / `RegionRequest` / `OrganizationRequest`)
5. **Three peer apply pipelines**
6. **Bootstrap rewrite + multi-tier orchestration** (system → region → organization)
7. **`LeadershipMode::Delegated` in the consensus engine** (`Shard` `SetLeader` event + delegated-mode wiring)
8. **Routing simplification** (delete `ShardRouter`)
9. **Saga PII repositioning**
10. **Cleanup** (`AddRegionLearner` → `AddRegionVoter`; `shards_per_region` config + CLI gone)
11. **Test infrastructure** (`TestCluster::create_data_region` + `create_organization` helpers; integration tests migrated)
12. **New `tests/three_tier_consensus.rs`**
13. **Metric label rename + documentation** (`shard` → `organization_id`; CLAUDE.md updates)

`just ci` green at every commit boundary. No "fixes coming in next commit" allowed.

---

## Open questions for implementation time

These don't block planning but want resolution during the work:

1. **`CoordinationEvent` and `OrganizationEvent` channel implementation.** Today's `region_creation_rx` mechanism (per-channel, per-handler) is the model. Should we generalize it into a typed event bus, or stick with per-event-kind channels? Lean toward per-event-kind for simplicity and traceability.

2. **Region group's `events.db` retention.** The region group emits audit events for placement changes, hibernation transitions (B.2), etc. These are low-volume but should be retained for compliance windows. Apply the same `BlockRetentionPolicy` as organization events, or define a separate region-tier policy?

3. **What happens if a region group loses quorum during organization-tier traffic?** Organization Rafts can continue serving reads (data is local) but can't accept writes (no leader, since leadership is delegated and the region group can't make leadership decisions without quorum). Acceptable degradation. Document the failure mode for operators.

4. **Lease extension for unified leadership.** The region group's leader holds a lease. If the lease is short (e.g., 150ms) and the region leader is processing many organization writes, the leader must extend the lease frequently. Should we extend the lease atomically with each organization write batch, or via a separate background heartbeat? Lean toward background heartbeat for simplicity; revisit if throughput is impacted.

---

## Risk register

| Risk                                                                                                                | Likelihood | Mitigation                                                                                                                                                                                         |
| ------------------------------------------------------------------------------------------------------------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LeadershipMode::Delegated` consensus-engine changes have safety bugs                                               | Medium     | Extensive simulation tests in `crates/consensus/src/simulation/` covering term advancement, leader transfer, partition + heal scenarios; safety analysis documented in the implementation PR       |
| Multi-tier `CreateOrganization` orchestration leaves an organization in an inconsistent intermediate state on crash | Medium     | Each step is idempotent with status-machine progression; recovery scan at startup re-drives any organization not in `Active` or `Erased` status                                                    |
| Region group becomes a single point of failure for the region                                                       | Low        | Acceptable trade-off documented; region group has 3-voter quorum same as organization groups; no SPOF beyond what 3-voter Raft tolerates                                                           |
| Single-node write throughput ceiling per region (all writes through region leader)                                  | Medium     | At target scale (tens-to-hundreds of orgs per region), a single big-cloud node handles 50k-150k writes/s with parallel apply across organizations; revisit only if measured throughput falls short |
| Test-infrastructure migration churn (Phase A test debt + 13 known broken tests + new test patterns)                 | Medium     | Sub-task budget covers it explicitly; migration pattern documented above; PRs incremental                                                                                                          |
| Cross-tier event channels deadlock or drop events on overload                                                       | Low        | Channels are bounded with backpressure; lost events trigger retries via the status-machine recovery path                                                                                           |
| Voter-set placement (B.1 uses uniform replication) doesn't match operator expectations for regions with many nodes  | Low        | Documented as B.1 simplification; non-uniform placement deferred to a future phase if scale demands                                                                                                |

---

## What success looks like

After B.1 ships:

- `find {data_dir}` shows `{region}/{organization_id}/` directories — one per organization per region — plus `{region}/_meta/` for the region group and `global/0/` for the system group. Storage layout matches the mental model.
- `grep -r ShardRouter crates/` returns zero matches. The shard concept is gone.
- `grep -r LedgerRequest crates/` returns zero matches as a type. `SystemRequest`, `RegionRequest`, `OrganizationRequest` are the only request types.
- A 3-node cluster bootstrapped fresh has exactly one Raft group: the system group.
- `CreateDataRegion` creates a region group on every node; `CreateOrganization` triggers the multi-tier orchestration and creates an organization group on every region voter within ~1s.
- Writes to organizations A and B land in distinct WAL streams, distinct state.db files, distinct apply pipelines. No code path mixes their data.
- Region leader transfer atomically transfers leadership for every organization in the region, with no per-organization election delay.
- Prometheus dashboards show per-`organization_id` write rate, apply latency, queue depth — operators can pinpoint which organization is hot.
- `consensus-reviewer` audits any cross-tier code path (organization apply touching region state, etc.) and flags violations.
- The 13 Phase A test-debt tests pass under the B.1 model with explicit multi-organization setup; full `just ci` green.
