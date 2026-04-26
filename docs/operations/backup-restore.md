# Backup and Restore (Operations Guide)

## Overview

InferaDB Ledger uses a per-organization `tar` + `zstd` archive format for
backups. Each archive captures the full physical state of a single
organization across every on-disk database the org owns:

- The four per-org control-plane files: `_meta.db`, `raft.db`, `blocks.db`,
  `events.db` (if events are configured for the org).
- Per-vault databases under `state/vault-{id}/`: `state.db`, `raft.db`,
  `blocks.db`, `events.db` (if events are configured).

The archive is keyed to the region's Region Master Key (RMK). Every
manifest stamps an `rmk_fingerprint` that the restoring node verifies
against its own region's RMK before any envelope unwrap is attempted.

## Creating a backup

### On-demand via gRPC

The `Admin.CreateBackup` RPC produces one archive per call:

```text
inferadb-ledger admin create-backup \
  --addr <leader-addr> \
  --organization <org-slug> \
  [--tag <free-form-label>]
```

The response carries:

- `backup_id` — string identifier of the form `{org_id}-{timestamp_micros}`.
- `backup_path` — absolute path of the archive on the leader's local disk.
- `size_bytes` — archive size on disk.
- `manifest` — the parsed manifest (region, organization, RMK fingerprint, DB list).

The archive lives at `{backups_dir}/backup-{backup_id}.tar.zst` where
`backups_dir` is the value of the node's `--backup-destination` flag (or
the equivalent config field).

### Automated via background job

When the backup config sets `enabled = true`, the leader of each region
runs `BackupJob` on the configured interval and produces one archive per
running per-organization group per cycle. Failures on a single org are
logged and counted in `ledger_backup_failures_total`; the rest of the
cycle continues.

The job is leader-only; followers skip the cycle. The
`ledger_backups_created_total` counter and `ledger_backup_last_size_bytes`
gauge surface job health. Track both alongside
`ledger_backup_failures_total` for SLO alerting.

### Listing archives

`Admin.ListBackups` enumerates archives in the configured backups
directory, opens each manifest non-destructively, and returns one
`BackupInfo` per archive. Corrupt manifests are skipped with a `WARN`
log entry rather than failing the whole call.

```text
inferadb-ledger admin list-backups --addr <leader-addr> [--limit N]
```

## Restoring from backup

Restore is two-phase. The first phase runs online via gRPC and only
unpacks the archive into a staging directory; the second phase runs
offline via the CLI on a stopped node and atomically swaps the staged
tree into the live data directory.

### Phase 1 — Online stage

`Admin.RestoreBackup` validates the archive against the local region's
RMK fingerprint and unpacks it under
`{data_dir}/.restore-staging/{org_id}-{timestamp}/`. Every member is
streamed through a `blake3` hasher and checked against the manifest's
recorded checksum; a mismatch surfaces as a hard error before the file
lands on disk.

```text
inferadb-ledger admin restore-backup \
  --addr <leader-addr> \
  --backup-id <id-from-create-or-list>
```

The response carries the manifest and the absolute `staging_dir` path
the operator hands to phase 2. The live data tree is untouched at this
stage — the running node continues to serve traffic.

### Phase 2 — Offline apply

The node must be stopped before the swap. The `inferadb-ledger restore
apply` CLI takes the staging path and atomically renames it onto the
live `{data_dir}/{region}/{organization_id}/` tree:

```text
inferadb-ledger restore apply \
  --data-dir <data-dir> \
  --staging <staging-dir-from-phase-1> \
  --region <region> \
  --organization-id <org-id>
```

The CLI:

1. Acquires the data-directory lock to confirm no other process is running. A held lock returns
   `LiveDirInUse` and the CLI refuses to proceed.
2. Moves the existing live tree (if present) to
   `{data_dir}/.restore-trash/{timestamp_micros}/{org_id}/`.
3. `rename(2)`s the staging tree onto the live path. Cross-filesystem stages fall back to a
   recursive copy.
4. Writes `{data_dir}/.restore-marker` with the region, organization id, and apply timestamp so the
   next operator checks the swap landed before starting the node.
5. Returns the trash path and the count of swapped files.

After verification, restart the node. The next start re-opens the
swapped DB files; no further intervention is required.

## Hard cutover from logical snapshots

Backups taken with the pre-Phase 4.3 logical snapshot format (a single
`.backup` file plus a `.meta.json` sidecar) are no longer restorable.
The page-level helpers and the `Snapshot::write_to_file` /
`read_from_file` path were retired in Phase 4.3. Operators must take
fresh backups after upgrading; old backups are operator-retained for
historical reasons only.

## Restore-trash retention

`apply` displaces — never deletes — the existing live tree. Old data
sits at `{data_dir}/.restore-trash/{timestamp_micros}/` until either:

- An operator removes it manually, or
- The `RestoreTrashSweepJob` background task prunes it.

The sweeper ticks every hour and removes trash entries older than 24
hours. Both intervals are configured by the job's defaults; the
`ledger_restore_trash_swept_total` and
`ledger_restore_trash_sweep_failures_total` counters surface its health.

The 24-hour window is the recovery budget for a botched restore. An
operator who realises the swap was a mistake within 24 hours can restore
the displaced tree by inverting the rename:

```text
mv {data_dir}/{region}/{org_id}    {data_dir}/{region}/{org_id}.bad
mv {data_dir}/.restore-trash/{ts}/{org_id}    {data_dir}/{region}/{org_id}
```

After 24 hours the sweeper removes the trash entry and recovery is no
longer possible.

## Cross-region restore

Cross-region restore is not supported. Each region has its own RMK; an
archive's `rmk_fingerprint` must match the destination region's. A
mismatch returns `RmkFingerprintMismatch` from `RestoreBackup`. Use a
fresh backup taken in the destination region instead.

Cross-region migration of organization data is a separate concern and is
not handled by the backup/restore pipeline.

## Operator runbook

### Routine backup verification

1. Inspect `ledger_backups_created_total` and `ledger_backup_last_size_bytes` daily.
2. Quarterly, on a non-production cluster: stage a recent archive via `RestoreBackup`, run `restore
   apply` on a stopped node, restart, and confirm the org's data is queryable. Record the elapsed
   time.

### Recovering from accidental data loss

1. Identify the most recent archive: `inferadb-ledger admin list-backups --addr <leader>`.
2. Stage it: `inferadb-ledger admin restore-backup --backup-id <id>`. Confirm the manifest matches
   expectations.
3. Stop the affected node.
4. Apply: `inferadb-ledger restore apply --staging <staging-dir> --region <region>
   --organization-id <org-id>`.
5. Inspect `{data_dir}/.restore-marker` to confirm the swap landed.
6. Restart the node. Verify reads against the org's vaults return the expected entities.

### Recovering from a botched restore

1. Within 24 hours of `apply`: locate the trash directory at
   `{data_dir}/.restore-trash/{timestamp}/{org_id}/`.
2. Stop the node.
3. Move the bad tree aside: `mv {data_dir}/{region}/{org_id} {data_dir}/{region}/{org_id}.bad`.
4. Promote the trash tree: `mv {data_dir}/.restore-trash/{timestamp}/{org_id}
   {data_dir}/{region}/{org_id}`.
5. Restart the node.
6. Manually remove `.bad` and the now-empty trash entry once the recovered state is verified.

### Cross-region archive received

1. The receiving node will reject `RestoreBackup` with `RmkFingerprintMismatch`. Do not attempt to
   force the restore.
2. Take a fresh backup in the destination region instead. Cross-region migration uses a separate
   pipeline, not this one.
