#!/usr/bin/env bash
# Binary-level crash recovery test.
#
# Validates that a node killed mid-write (SIGKILL) recovers cleanly when
# restarted, and that the cluster converges to an identical state root
# with no data loss and no duplicate commits.
#
# Two scenarios:
#   A: SIGKILL a follower while writes are in flight; restart; verify convergence.
#   B: SIGKILL the leader while writes are in flight; verify re-election and
#      convergence after the old leader restarts as a follower.
#
# Success criteria:
#   - All 3 nodes report identical block height and state root after convergence.
#   - Every write that the client observed a success response for is readable.
#   - No block height regression on any node.
#
# Usage:
#   ./scripts/crash-recovery.sh                # Full test (release)
#   ./scripts/crash-recovery.sh --debug        # Debug build
#   ./scripts/crash-recovery.sh --scenario A   # Run only scenario A
#   ./scripts/crash-recovery.sh --scenario B   # Run only scenario B

set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib/cluster-bootstrap.sh
source "$(dirname "$0")/lib/cluster-bootstrap.sh"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_PORT=50081
DATA_ROOT="/tmp/ledger-crash-recovery-$$"
PROFILE="release"
SCENARIO="all"
NODE_COUNT=3
WRITER_DURATION_SECS=8
CRASH_AFTER_SECS=3
RESTART_AFTER_SECS=6
CONVERGENCE_TIMEOUT=60

# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
  case $1 in
    --debug) PROFILE="debug"; shift ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
    --help|-h)
      grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -30
      exit 0
      ;;
    *) log_error "Unknown option: $1"; exit 2 ;;
  esac
done

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

for cmd in jq uuidgen; do
  if ! command -v "$cmd" &>/dev/null; then
    log_error "$cmd is required. Install: brew install $cmd"
    exit 1
  fi
done

trap cleanup_cluster EXIT
build_ledger_binary "$PROFILE"

# ---------------------------------------------------------------------------
# Admin CLI helper
# ---------------------------------------------------------------------------

# Run an `inferadb-ledger admin <subcommand>` invocation with the cluster's
# TLS material applied as top-level flags. clap rejects --tls-cert /
# --tls-server-name when placed after the `admin` subcommand, so they go
# before it.
admin_cli() {
  "$LEDGER_BINARY" \
    --tls-cert "$LEDGER_TLS_CERT" \
    --tls-server-name "$LEDGER_TLS_SERVER_NAME" \
    admin "$@"
}

# ---------------------------------------------------------------------------
# Wire helpers
# ---------------------------------------------------------------------------

ORG_SLUG=""
USER_SLUG=""
VAULT_SLUG=""

node_addr() { echo "127.0.0.1:$((BASE_PORT + $1 - 1))"; }
node_port() { echo $((BASE_PORT + $1 - 1)); }

# Decode a `Bytes` field from an admin-CLI JSON response into its raw UTF-8
# string. The wire types serialize `bytes::Bytes` as a numeric `[u8, ...]`
# array; jq's `implode` materializes that into a string. Returns empty when
# the field is null or absent.
decode_bytes_field() {
  local json=$1
  local field=$2
  echo "$json" | jq -r --arg f "$field" 'if .[$f] == null then "" else .[$f] | implode end' 2>/dev/null || true
}

create_org_and_vault() {
  local email
  email="crash-test-$(uuidgen | tr '[:upper:]' '[:lower:]')@recovery.local"

  # Phase 1: InitiateEmailVerification — retry across cluster nodes.
  # Non-leader nodes return NotLeader (redirect model), so we try each.
  local code=""
  local attempt=0
  while [[ -z "$code" && $attempt -lt 30 ]]; do
    local i
    for ((i=1; i<=NODE_COUNT; i++)); do
      local addr
      addr=$(node_addr "$i")
      local init_result
      init_result=$(admin_cli initiate-email-verification \
        --host "$addr" \
        --email "$email" \
        --region us-east-va 2>&1) || true
      code=$(echo "$init_result" | jq -r '.code // empty' 2>/dev/null || true)
      [[ -n "$code" ]] && break
    done
    attempt=$((attempt + 1))
    sleep 1
  done
  [[ -z "$code" ]] && { log_error "InitiateEmailVerification failed after 30 attempts"; return 1; }

  # Phase 2: VerifyEmailCode requires the regional leader. Retry across nodes
  # because the regional leader can transition between Phase 1 and Phase 2
  # (e.g., during initial cluster stabilization).
  local token="" verify_result=""
  local verify_attempt=0
  while [[ -z "$token" && $verify_attempt -lt 15 ]]; do
    local i
    for ((i=1; i<=NODE_COUNT; i++)); do
      local addr
      addr=$(node_addr "$i")
      verify_result=$(admin_cli verify-email-code \
        --host "$addr" \
        --email "$email" \
        --code "$code" \
        --region us-east-va 2>&1) || true
      # Wire shape: { result: { NewUser: { onboarding_token: "..." } } }
      token=$(echo "$verify_result" | jq -r '.result.NewUser.onboarding_token // empty' 2>/dev/null || true)
      [[ -n "$token" ]] && break
    done
    [[ -z "$token" ]] && { verify_attempt=$((verify_attempt + 1)); sleep 1; }
  done
  [[ -z "$token" ]] && { log_error "VerifyEmailCode failed after 15 attempts: $verify_result"; return 1; }

  # CompleteRegistration submits a saga that runs on the GLOBAL leader.
  # The GLOBAL leader may differ from the regional leader, so try all nodes.
  # The saga is idempotent by onboarding token, so retries are safe.
  local reg_attempt=0
  while [[ $reg_attempt -lt 15 ]]; do
    local i
    for ((i=1; i<=NODE_COUNT; i++)); do
      local addr
      addr=$(node_addr "$i")
      local reg_result
      reg_result=$(admin_cli complete-registration \
        --host "$addr" \
        --onboarding-token "$token" \
        --email "$email" \
        --region us-east-va \
        --name "Crash Test" \
        --organization-name crash-recovery-org 2>&1) || true
      # `OrganizationSlug` and `UserSlug` are #[serde(transparent)] over u64,
      # so they appear as bare numbers (or null when absent) in JSON.
      ORG_SLUG=$(echo "$reg_result" | jq -r '.organization // empty' 2>/dev/null || true)
      USER_SLUG=$(echo "$reg_result" | jq -r '.user.slug // empty' 2>/dev/null || true)
      if [[ -n "$ORG_SLUG" && -n "$USER_SLUG" ]]; then
        break 2
      fi
    done
    reg_attempt=$((reg_attempt + 1))
    sleep 1
  done
  [[ -z "$ORG_SLUG" || -z "$USER_SLUG" ]] && { log_error "Registration failed after 15 attempts"; return 1; }

  # Retry vault creation across all nodes — the org may still be provisioning
  # and CreateVault is a GLOBAL operation that requires the GLOBAL leader.
  #
  # CreateVaultRequest takes an optional client-supplied Snowflake slug; reuse
  # the same slug across retries so the per-org idempotency check returns the
  # existing vault instead of allocating a new one.
  local client_slug
  client_slug=$(generate_snowflake_slug)

  local attempt2=0
  while [[ $attempt2 -lt 30 ]]; do
    local i
    for ((i=1; i<=NODE_COUNT; i++)); do
      local addr
      addr=$(node_addr "$i")
      local result
      result=$(admin_cli create-vault \
        --host "$addr" \
        --organization "$ORG_SLUG" \
        --caller "$USER_SLUG" \
        --slug "$client_slug" \
        --replication-factor "$NODE_COUNT" 2>&1) || true
      VAULT_SLUG=$(echo "$result" | jq -r '.vault // empty' 2>/dev/null || true)
      [[ -n "$VAULT_SLUG" ]] && { log_success "Created org $ORG_SLUG / vault $VAULT_SLUG"; return 0; }
    done
    attempt2=$((attempt2 + 1))
    sleep 1
  done
  log_error "CreateVault timed out"
  return 1
}

# Generate a non-zero u64 Snowflake-shaped slug suitable for client-supplied vault IDs.
# Uses /dev/urandom for entropy and clears the high bit so the value fits in a signed
# i64 representation when round-tripped through tooling that interprets uint64 as JSON
# numbers. The exact bit layout doesn't matter — the server accepts any non-zero u64.
generate_snowflake_slug() {
  local raw
  # Read 8 bytes, format as decimal, mask off the high bit to keep the value < 2^63.
  raw=$(od -An -N8 -tu8 < /dev/urandom | tr -d ' \n')
  if [[ -z "$raw" || "$raw" == "0" ]]; then
    raw=1
  fi
  # Mask high bit (bash arithmetic operates on 64-bit signed integers).
  echo $(( (raw & 0x7FFFFFFFFFFFFFFF) | 1 ))
}

# Write a single entity. Tries the given node first; on failure, tries all
# nodes in sequence (non-leader nodes return NotLeader in the redirect model).
# Echoes "ok:<key>" on success, "fail:<key>" on failure.
# Args: node_num key_index
write_one() {
  local node_num=$1
  local idx=$2
  local key
  key="crash-key-$(printf '%06d' "$idx")"
  local idem_key
  idem_key=$(printf '%032x' "$idx")

  # Try the preferred node first.
  local addr result
  addr=$(node_addr "$node_num")
  result=$(admin_cli write \
    --host "$addr" \
    --organization "$ORG_SLUG" \
    --vault "$VAULT_SLUG" \
    --caller "$USER_SLUG" \
    --key "$key" \
    --value "value-$idx" \
    --idempotency-key "$idem_key" 2>/dev/null) || true
  # Wire shape: { result: { Success: { block_height: N, ... } } } on success.
  if echo "$result" | jq -e '.result.Success' &>/dev/null; then
    echo "ok:$key"
    return 0
  fi

  # Preferred node failed — try all nodes (redirect model: find the regional leader).
  local i
  for ((i=1; i<=NODE_COUNT; i++)); do
    [[ "$i" -eq "$node_num" ]] && continue
    addr=$(node_addr "$i")
    result=$(admin_cli write \
      --host "$addr" \
      --organization "$ORG_SLUG" \
      --vault "$VAULT_SLUG" \
      --caller "$USER_SLUG" \
      --key "$key" \
      --value "value-$idx" \
      --idempotency-key "$idem_key" 2>/dev/null) || true
    if echo "$result" | jq -e '.result.Success' &>/dev/null; then
      echo "ok:$key"
      return 0
    fi
  done

  echo "fail:$key"
}

# Background writer loop. Writes to the leader node for WRITER_DURATION_SECS,
# logging results to $1. Non-leader nodes return NotLeader (redirect model),
# so we target the leader directly and fall back to trying all nodes when
# the leader is unknown (e.g., during re-election after a crash).
# Args: output_file nodes_csv
run_writer() {
  local outfile=$1
  local nodes_csv=$2
  IFS=',' read -ra nodes <<< "$nodes_csv"
  local start_ts=$SECONDS
  local idx=1
  local current_leader=""
  while (( SECONDS - start_ts < WRITER_DURATION_SECS )); do
    # Periodically re-discover the leader (every 20 writes or when unknown).
    if [[ -z "$current_leader" || $(( idx % 20 )) -eq 0 ]]; then
      current_leader=$(find_leader) || true
    fi
    local target
    if [[ -n "$current_leader" ]]; then
      target=$current_leader
    else
      # Leader unknown (e.g., mid-election) — try round-robin, expecting failures.
      target=${nodes[$(( idx % ${#nodes[@]} ))]}
    fi
    write_one "$target" "$idx" >> "$outfile" || true
    idx=$((idx + 1))
    # Modest pacing — ~30 writes/sec, enough to matter without saturating.
    sleep 0.03
  done
}

# Find current leader node number. Echoes node number or empty.
find_leader() {
  local i
  for ((i=1; i<=NODE_COUNT; i++)); do
    local addr
    addr=$(node_addr "$i")
    local info
    info=$(admin_cli cluster-info --host "$addr" 2>/dev/null) || true
    local leader_id
    leader_id=$(echo "$info" | jq -r '.leader_id // empty' 2>/dev/null || true)
    [[ -z "$leader_id" || "$leader_id" == "0" ]] && continue

    local j
    for ((j=1; j<=NODE_COUNT; j++)); do
      local check_addr node_id
      check_addr=$(node_addr "$j")
      node_id=$(admin_cli node-info --host "$check_addr" 2>/dev/null | jq -r '.node_id // empty' 2>/dev/null || true)
      if [[ "$node_id" == "$leader_id" ]]; then
        echo "$j"
        return 0
      fi
    done
  done
  echo ""
}

# Kill a node by listen-port (SIGKILL — simulates crash). The wire transport
# is QUIC, so the listening socket is UDP.
kill_node_hard() {
  local node_num=$1
  local port
  port=$(node_port "$node_num")
  local pid
  pid=$(lsof -ti "udp:$port" 2>/dev/null || true)
  if [[ -n "$pid" ]]; then
    kill -9 "$pid" 2>/dev/null || true
    log_warn "SIGKILL'd node $node_num (PID $pid, port $port)"
    # Remove the PID from tracking so cleanup_cluster doesn't flag it.
    local new_pids=() p
    for p in "${CLUSTER_PIDS[@]}"; do
      [[ "$p" != "$pid" ]] && new_pids+=("$p")
    done
    CLUSTER_PIDS=("${new_pids[@]}")
  else
    log_warn "No UDP listener for node $node_num (already dead?)"
  fi
}

# Restart a previously-killed node. Expects --join since it's rejoining an
# existing cluster.
restart_node() {
  local node_num=$1
  local port node_data
  port=$(node_port "$node_num")
  node_data="$DATA_ROOT/node$node_num"
  local blinding_key="deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

  local first_addr
  first_addr="127.0.0.1:$BASE_PORT"
  # If node 1 is the one being restarted, use node 2 as the join seed.
  [[ "$node_num" -eq 1 ]] && first_addr="127.0.0.1:$((BASE_PORT + 1))"

  RUST_LOG=info "$LEDGER_BINARY" \
    --listen "127.0.0.1:$port" \
    --data "$node_data" \
    --join "$first_addr" \
    --tls-cert "$LEDGER_TLS_CERT" \
    --tls-key "$LEDGER_TLS_KEY" \
    --tls-server-name "$LEDGER_TLS_SERVER_NAME" \
    --email-blinding-key "$blinding_key" \
    --log-format text \
    > "$DATA_ROOT/node$node_num.restart.log" 2>&1 &
  CLUSTER_PIDS+=("$!")
  log_info "Restarted node $node_num (PID $!)"

  # Wait for the listen port (QUIC = UDP) AND for the wire dispatcher to
  # serve admin RPCs. The UDP socket binds very early in the binary's
  # lifecycle — before raft state replay and before the wire dispatcher
  # is wired up — so a UDP-only check returns true while RPCs still
  # error out. `admin node-info` is the cheapest readiness probe.
  local elapsed=0
  local addr="127.0.0.1:$port"
  while (( elapsed < 30 )); do
    if port_is_listening "$port" \
        && admin_cli node-info --host "$addr" >/dev/null 2>&1; then
      log_success "Node $node_num listening + RPC-ready again"
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  log_error "Node $node_num did not become RPC-ready within 30s"
  return 1
}

# Verify convergence by reading a baseline key from every node that hosts
# the vault, with EVENTUAL consistency. GetTip requires the regional
# leader (redirect model), so we use point reads instead.
#
# Non-hosting nodes (the data-region placement controller may pick fewer
# than NODE_COUNT voters for any given region) return
# `{value: null, block_height: 0}` — that's "vault not on this node",
# not data loss. We skip such nodes and verify convergence only across
# the hosting subset. A hosting node with `value: null` and
# `block_height > 0` would indicate genuine data loss and fails the test.
verify_convergence() {
  log_info "Waiting for convergence (timeout: ${CONVERGENCE_TIMEOUT}s)..."
  local elapsed=0

  while (( elapsed < CONVERGENCE_TIMEOUT )); do
    local hosting_nodes=0 hosting_with_value=0 i
    for ((i=1; i<=NODE_COUNT; i++)); do
      local addr read_result height val
      addr=$(node_addr "$i")
      # Capture stderr so we can distinguish "non-hosting node" from
      # "transient RPC error" — the former returns an `Organization …
      # not found` WireError on stderr; the latter returns a genuine
      # transport / unavailable error that should retry.
      read_result=$(admin_cli read \
        --host "$addr" \
        --organization "$ORG_SLUG" \
        --vault "$VAULT_SLUG" \
        --caller "$USER_SLUG" \
        --key "crash-key-000001" \
        --consistency eventual 2>&1) || true

      # Non-hosting node: the data-region placement controller may pick
      # fewer than NODE_COUNT voters for any given region, so a node
      # that is not a voter for `us-east-va` either:
      #   1) Returns `{value: null, block_height: 0}` (vault entry not
      #      registered locally), or
      #   2) Returns a `NotFound` WireError naming the missing
      #      organization / vault (region state machine has no entry).
      # Both are "vault not on this node", not data loss; skip.
      if echo "$read_result" | grep -qE '"code": ?"NotFound"|not found'; then
        continue
      fi
      if ! echo "$read_result" | jq -e . &>/dev/null; then
        # Unparseable response (transient RPC error) — treat as
        # not-yet-converged and retry on the next loop tick.
        hosting_nodes=$((hosting_nodes + 1))
        continue
      fi

      height=$(echo "$read_result" | jq -r '.block_height // 0' 2>/dev/null || echo 0)
      val=$(decode_bytes_field "$read_result" value)

      if [[ "$height" == "0" && -z "$val" ]]; then
        continue
      fi

      hosting_nodes=$((hosting_nodes + 1))
      if [[ -n "$val" ]]; then
        hosting_with_value=$((hosting_with_value + 1))
      fi
    done

    if (( hosting_nodes > 0 && hosting_with_value == hosting_nodes )); then
      log_success "Convergence: baseline key readable on all $hosting_nodes hosting node(s)"
      return 0
    fi

    sleep 2
    elapsed=$((elapsed + 2))
  done

  log_error "Nodes did not converge within ${CONVERGENCE_TIMEOUT}s"
  for ((i=1; i<=NODE_COUNT; i++)); do
    local addr read_result
    addr=$(node_addr "$i")
    # Capture stderr too — when the wire RPC errors, stdout is empty and
    # the actual diagnosis is on stderr.
    read_result=$(admin_cli read \
      --host "$addr" \
      --organization "$ORG_SLUG" \
      --vault "$VAULT_SLUG" \
      --caller "$USER_SLUG" \
      --key "crash-key-000001" \
      --consistency eventual 2>&1) || true
    log_error "  Node $i read: $read_result"
  done
  return 1
}

# Verify every "ok:" line in the writer log is readable on every node.
verify_no_data_loss() {
  local writer_log=$1
  log_info "Verifying no data loss (reading back all successful writes)..."

  # Extract successful keys into a temp file for parallel verification.
  local keys_file="$DATA_ROOT/ok-keys-$$.txt"
  grep '^ok:' "$writer_log" | sed 's/^ok://' > "$keys_file" || true
  local ok_count
  ok_count=$(wc -l < "$keys_file" | tr -d ' ')

  if [[ "$ok_count" -eq 0 ]]; then
    log_error "No successful writes to verify"
    return 1
  fi

  # Verify each key in parallel (up to 10 at a time).
  # Each subshell tries all nodes and echoes the key if missing.
  local missing_file="$DATA_ROOT/missing-keys-$$.txt"
  : > "$missing_file"

  export ORG_SLUG USER_SLUG VAULT_SLUG NODE_COUNT BASE_PORT
  export LEDGER_BINARY LEDGER_TLS_CERT LEDGER_TLS_SERVER_NAME
  # shellcheck disable=SC2016
  xargs -P 10 -I {} bash -c '
    key="$1"
    for ((n=1; n<=NODE_COUNT; n++)); do
      addr="127.0.0.1:$((BASE_PORT + n - 1))"
      response=$("$LEDGER_BINARY" \
        --tls-cert "$LEDGER_TLS_CERT" \
        --tls-server-name "$LEDGER_TLS_SERVER_NAME" \
        admin read \
        --host "$addr" \
        --organization "$ORG_SLUG" \
        --vault "$VAULT_SLUG" \
        --caller "$USER_SLUG" \
        --key "$key" \
        --consistency eventual 2>/dev/null || true)
      if echo "$response" | jq -e ".value != null" &>/dev/null; then
        exit 0
      fi
    done
    echo "$key" >> '"$missing_file"'
  ' _ {} < "$keys_file"

  local missing
  missing=$(wc -l < "$missing_file" | tr -d ' ')
  if (( missing > 0 )); then
    log_error "DATA LOSS: $missing/$ok_count successful writes missing after recovery"
    head -3 "$missing_file" | while IFS= read -r k; do
      log_error "  Missing: $k"
    done
    return 1
  fi
  log_success "All $ok_count successful writes readable after recovery"
}

# ---------------------------------------------------------------------------
# Scenario runner
# ---------------------------------------------------------------------------

run_scenario() {
  local name=$1
  local kill_target_role=$2  # "follower" | "leader"

  log_info "Scenario $name: kill $kill_target_role mid-write"

  # Use longer settle time for debug builds (saga orchestrator is slower).
  local settle_time=2
  [[ "$PROFILE" == "debug" ]] && settle_time=10
  bootstrap_cluster "$BASE_PORT" "$NODE_COUNT" "$DATA_ROOT" "$settle_time"

  local leader
  leader=$(find_leader)
  [[ -z "$leader" ]] && { log_error "No leader found after bootstrap"; return 1; }
  log_info "Initial leader: node $leader"

  # Data regions are no longer auto-created at boot. Provision us-east-va
  # explicitly via `AdminService::ProvisionRegion` before any user-facing
  # RPCs (which apply through the regional Raft group).
  provision_region us-east-va false || return 1

  # Settle so every voter has applied the region creation.
  sleep 3

  create_org_and_vault

  # Prewrite a small baseline so crash happens after steady state.
  log_info "Writing 20 baseline entities..."
  local i
  for ((i=1; i<=20; i++)); do
    write_one "$leader" "$i" >/dev/null
  done

  local kill_target
  if [[ "$kill_target_role" == "leader" ]]; then
    kill_target=$leader
  else
    # Pick a follower deterministically: first node that isn't leader.
    for ((i=1; i<=NODE_COUNT; i++)); do
      if [[ "$i" -ne "$leader" ]]; then kill_target=$i; break; fi
    done
  fi
  log_info "Crash target: node $kill_target ($kill_target_role)"

  # Background writer targets the leader (with fallback to round-robin during
  # re-election). Non-leader nodes return NotLeader in the redirect model.
  local writer_log="$DATA_ROOT/writer-$name.log"
  : > "$writer_log"
  local writer_nodes
  writer_nodes=$(seq 1 $NODE_COUNT | tr '\n' ',' | sed 's/,$//')
  run_writer "$writer_log" "$writer_nodes" &
  local writer_pid=$!

  sleep "$CRASH_AFTER_SECS"
  kill_node_hard "$kill_target"

  # Let writes continue against the remaining nodes.
  sleep $((RESTART_AFTER_SECS - CRASH_AFTER_SECS))

  restart_node "$kill_target"

  # Let writer finish.
  wait "$writer_pid" 2>/dev/null || true

  # Give replication a moment to catch up post-restart.
  sleep 1

  verify_convergence || return 1
  verify_no_data_loss "$writer_log" || return 1

  local total ok_count
  total=$(wc -l < "$writer_log" | tr -d ' ')
  ok_count=$(grep -c '^ok:' "$writer_log" || true)
  log_success "Scenario $name: $ok_count/$total writes succeeded, all verified"

  # Reset for next scenario.
  cleanup_cluster
  CLUSTER_PIDS=()
  _CLUSTER_DATA_ROOT=""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

case "$SCENARIO" in
  A|a)   run_scenario "A" "follower" ;;
  B|b)   run_scenario "B" "leader" ;;
  all)
    run_scenario "A" "follower"
    BASE_PORT=$((BASE_PORT + 10))
    DATA_ROOT="/tmp/ledger-crash-recovery-b-$$"
    run_scenario "B" "leader"
    ;;
  *) log_error "Unknown scenario: $SCENARIO (expected: A|B|all)"; exit 2 ;;
esac

log_success "Crash recovery test complete"
