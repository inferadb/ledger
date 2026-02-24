# Backup Verification Runbook

Procedure for verifying backup integrity and testing restore procedures.

## Overview

Ledger backups consist of:

1. **Raft snapshots**: zstd-compressed binary files containing `AppliedStateCore`, 9 externalized B+ tree tables, entity data, and event data — with a SHA-256 checksum footer over the compressed bytes
2. **Block archive**: Transaction history (if retention policy is `FULL`)
3. **Node ID file**: `{data_dir}/node_id`

Snapshot integrity is verified at restore time: the SHA-256 checksum is validated before any decompression, and state is written into a single `WriteTransaction` (either all tables install atomically or none are visible).

Verification ensures backups can be restored successfully.

## Verification Schedule

| Environment | Frequency       | Type              |
| ----------- | --------------- | ----------------- |
| Production  | Weekly          | Full restore test |
| Production  | Daily           | Integrity check   |
| Staging     | Before upgrades | Full restore test |

## Daily Integrity Check

### 1. Trigger Snapshot

```bash
for vault in $(grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults | jq -r '.vaults[].vault.slug'); do
  grpcurl -plaintext \
    -d "{\"organization_slug\": {\"id\": \"1\"}, \"vault\": {\"slug\": \"$vault\"}}" \
    localhost:50051 ledger.v1.AdminService/CreateSnapshot
done
```

### 2. Run Integrity Check

```bash
for vault in $(grpcurl -plaintext localhost:50051 ledger.v1.AdminService/ListVaults | jq -r '.vaults[].vault.slug'); do
  grpcurl -plaintext \
    -d "{\"organization_slug\": {\"id\": \"1\"}, \"vault\": {\"slug\": \"$vault\"}, \"full_check\": false}" \
    localhost:50051 ledger.v1.AdminService/CheckIntegrity
done
```

### 3. Verify Metrics

```bash
# Check for determinism bugs
curl -s localhost:9090/metrics | grep ledger_determinism_bug_total
# Expected: 0

# Check snapshot creation
curl -s localhost:9090/metrics | grep ledger_snapshots_created_total
```

### 4. Record Results

```bash
# Export verification results
cat > /var/log/ledger/backup-check-$(date +%Y%m%d).json << EOF
{
  "date": "$(date -Iseconds)",
  "snapshots_created": $(curl -s localhost:9090/metrics | grep ledger_snapshots_created_total | awk '{print $2}'),
  "determinism_bugs": $(curl -s localhost:9090/metrics | grep ledger_determinism_bug_total | awk '{print $2}'),
  "status": "healthy"
}
EOF
```

## Weekly Restore Test

### Prerequisites

- Isolated test environment (separate Kubernetes namespace or VMs)
- Latest backup files available
- Test data directory

### 1. Prepare Test Environment

```bash
# Create isolated directory
RESTORE_DIR=/tmp/ledger-restore-test-$(date +%Y%m%d)
mkdir -p $RESTORE_DIR

# Copy latest snapshot from backup storage
aws s3 cp s3://ledger-backups/latest/snapshot.tar.gz $RESTORE_DIR/
tar -xzf $RESTORE_DIR/snapshot.tar.gz -C $RESTORE_DIR
```

### 2. Start Ledger in Restore Mode

```bash
# Start with restored data
docker run -d --name ledger-restore-test \
  -v $RESTORE_DIR:/data \
  -p 50052:50051 \
  -e INFERADB__LEDGER__LISTEN=0.0.0.0:50051 \
  -e INFERADB__LEDGER__DATA=/data \
  -e INFERADB__LEDGER__CLUSTER=1 \
  inferadb/ledger:latest
```

### 3. Verify Restore

```bash
# Wait for startup
sleep 10

# Health check
grpcurl -plaintext localhost:50052 ledger.v1.HealthService/Check

# Verify cluster info
grpcurl -plaintext localhost:50052 ledger.v1.AdminService/GetClusterInfo

# List organizations
grpcurl -plaintext localhost:50052 ledger.v1.AdminService/ListOrganizations

# List vaults
grpcurl -plaintext localhost:50052 ledger.v1.AdminService/ListVaults
```

### 4. Verify Data Integrity

```bash
# For each vault, run full integrity check
for vault in $(grpcurl -plaintext localhost:50052 ledger.v1.AdminService/ListVaults | jq -r '.vaults[].vault.slug'); do
  echo "Checking vault $vault..."

  # Get expected state from production
  PROD_TIP=$(grpcurl -plaintext localhost:50051 \
    -d "{\"organization_slug\": {\"id\": \"1\"}, \"vault\": {\"slug\": \"$vault\"}}" \
    ledger.v1.ReadService/GetTip)

  # Get restored state
  RESTORE_TIP=$(grpcurl -plaintext localhost:50052 \
    -d "{\"organization_slug\": {\"id\": \"1\"}, \"vault\": {\"slug\": \"$vault\"}}" \
    ledger.v1.ReadService/GetTip)

  # Compare (height may differ if snapshot is older)
  echo "Production height: $(echo $PROD_TIP | jq .height)"
  echo "Restored height: $(echo $RESTORE_TIP | jq .height)"

  # Full integrity check
  grpcurl -plaintext \
    -d "{\"organization_slug\": {\"id\": \"1\"}, \"vault\": {\"slug\": \"$vault\"}, \"full_check\": true}" \
    localhost:50052 ledger.v1.AdminService/CheckIntegrity
done
```

### 5. Test Write Operations

```bash
# Verify writes work on restored instance
grpcurl -plaintext \
  -d '{"organization_slug": {"id": "1"}, "client_id": {"id": "restore-test"}, "sequence": "1", "operations": [{"set_entity": {"key": "test:restore", "value": "dGVzdA=="}}]}' \
  localhost:50052 ledger.v1.WriteService/Write
```

### 6. Cleanup

```bash
# Stop test instance
docker stop ledger-restore-test
docker rm ledger-restore-test

# Remove test data
rm -rf $RESTORE_DIR
```

### 7. Document Results

```yaml
# /var/log/ledger/restore-test-YYYYMMDD.yaml
date: 2026-01-28
backup_source: s3://ledger-backups/2026-01-27/
restore_successful: true
health_check: passed
organizations_restored: 5
vaults_restored: 12
integrity_checks: all_passed
write_test: passed
notes: "Restore completed in 45 seconds. All vaults healthy."
```

## Recovery Point Objective (RPO)

With continuous Raft replication:

- **Normal operation**: RPO = 0 (synchronous replication)
- **From snapshot**: RPO = time since last snapshot

Configure snapshot frequency based on acceptable data loss window.

## Recovery Time Objective (RTO)

| Scenario                            | RTO                     |
| ----------------------------------- | ----------------------- |
| Node failure (quorum maintained)    | < 5 seconds (automatic) |
| Quorum loss (restore from snapshot) | 5-15 minutes            |
| Full cluster restore                | 15-30 minutes           |

## Monitoring Backup Health

### Prometheus Alerts

```yaml
groups:
  - name: ledger-backup
    rules:
      - alert: LedgerNoRecentSnapshot
        expr: time() - ledger_snapshot_last_created_timestamp > 86400
        labels:
          severity: warning
        annotations:
          summary: "No Ledger snapshot in 24 hours"

      - alert: LedgerBackupVerificationFailed
        expr: ledger_backup_verification_status == 0
        labels:
          severity: critical
        annotations:
          summary: "Ledger backup verification failed"
```

### Grafana Dashboard

Track these metrics:

- `ledger_snapshots_created_total`: Cumulative snapshots
- `ledger_snapshot_size_bytes`: Snapshot sizes (growth trend)
- `ledger_snapshot_create_latency_seconds`: Snapshot creation time

## Automation

### Kubernetes CronJob for Verification

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ledger-backup-verify
spec:
  schedule: "0 3 * * *" # Daily at 3 AM
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: verify
              image: inferadb/ledger-ops:latest
              command:
                - /scripts/verify-backups.sh
              env:
                - name: LEDGER_ENDPOINT
                  value: "ledger:50051"
          restartPolicy: OnFailure
```

## Checklist

Daily:

- [ ] Snapshots created successfully
- [ ] Integrity checks passed
- [ ] No determinism bugs detected
- [ ] Results logged

Weekly:

- [ ] Full restore test completed
- [ ] Restored data verified
- [ ] Write operations tested
- [ ] RTO measured and documented
- [ ] Test environment cleaned up
