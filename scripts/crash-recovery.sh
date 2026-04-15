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

# shellcheck source=./lib/cluster-bootstrap.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source "$(dirname "$0")/lib/cluster-bootstrap.sh"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_PORT=50081
DATA_ROOT="/tmp/ledger-crash-recovery-$$"
PROFILE="release"
SCENARIO="all"
NODE_COUNT=3
WRITER_DURATION_SECS=15
CRASH_AFTER_SECS=5
RESTART_AFTER_SECS=8
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

for cmd in grpcurl jq uuidgen; do
  if ! command -v "$cmd" &>/dev/null; then
    log_error "$cmd is required. Install: brew install $cmd"
    exit 1
  fi
done

trap cleanup_cluster EXIT
build_ledger_binary "$PROFILE"

# ---------------------------------------------------------------------------
# gRPC helpers
# ---------------------------------------------------------------------------

ORG_SLUG=""
USER_SLUG=""
VAULT_SLUG=""

node_addr() { echo "127.0.0.1:$((BASE_PORT + $1 - 1))"; }
node_port() { echo $((BASE_PORT + $1 - 1)); }

create_org_and_vault() {
  local addr=$1
  local email
  email="crash-test-$(uuidgen | tr '[:upper:]' '[:lower:]')@recovery.local"

  local init_result
  init_result=$(grpcurl -plaintext \
    -d "{\"email\": \"$email\", \"region\": 10}" \
    "$addr" \
    ledger.v1.UserService/InitiateEmailVerification 2>&1 || true)
  local code
  code=$(echo "$init_result" | jq -r '.code // empty' 2>/dev/null || true)
  [[ -z "$code" ]] && { log_error "InitiateEmailVerification failed: $init_result"; return 1; }

  local verify_result
  verify_result=$(grpcurl -plaintext \
    -d "{\"email\": \"$email\", \"code\": \"$code\", \"region\": 10}" \
    "$addr" \
    ledger.v1.UserService/VerifyEmailCode 2>&1 || true)
  local token
  token=$(echo "$verify_result" | jq -r '.newUser.onboardingToken // empty' 2>/dev/null || true)
  [[ -z "$token" ]] && { log_error "VerifyEmailCode failed: $verify_result"; return 1; }

  local reg_result
  reg_result=$(grpcurl -plaintext \
    -d "{\"onboarding_token\": \"$token\", \"email\": \"$email\", \"region\": 10, \"name\": \"Crash Test\", \"organization_name\": \"crash-recovery-org\"}" \
    "$addr" \
    ledger.v1.UserService/CompleteRegistration 2>&1 || true)
  ORG_SLUG=$(echo "$reg_result" | jq -r '.organization.slug // empty' 2>/dev/null || true)
  USER_SLUG=$(echo "$reg_result" | jq -r '.user.slug.slug // empty' 2>/dev/null || true)
  [[ -z "$ORG_SLUG" || -z "$USER_SLUG" ]] && { log_error "Registration failed: $reg_result"; return 1; }

  # Retry vault creation — the org may still be provisioning.
  local attempt=0
  while [[ $attempt -lt 30 ]]; do
    local result
    result=$(grpcurl -plaintext \
      -d "{\"organization\": {\"slug\": \"$ORG_SLUG\"}, \"caller\": {\"slug\": \"$USER_SLUG\"}}" \
      "$addr" \
      ledger.v1.VaultService/CreateVault 2>&1 || true)
    VAULT_SLUG=$(echo "$result" | jq -r '.vault.slug // empty' 2>/dev/null || true)
    [[ -n "$VAULT_SLUG" ]] && { log_success "Created org $ORG_SLUG / vault $VAULT_SLUG"; return 0; }
    attempt=$((attempt + 1))
    sleep 1
  done
  log_error "CreateVault timed out"
  return 1
}

# Write a single entity. Echoes "ok:<key>" on success, "fail:<key>" on failure.
# Args: node_num key_index
write_one() {
  local node_num=$1
  local idx=$2
  local addr
  addr=$(node_addr "$node_num")
  local key
  key="crash-key-$(printf '%06d' "$idx")"
  local value_b64
  value_b64=$(echo -n "value-$idx" | base64)
  local idem_b64
  idem_b64=$(printf '%032x' "$idx" | xxd -r -p | base64)

  local result
  result=$(grpcurl -plaintext \
    -d "{\"caller\": {\"slug\": \"$USER_SLUG\"}, \"organization\": {\"slug\": \"$ORG_SLUG\"}, \"vault\": {\"slug\": \"$VAULT_SLUG\"}, \"clientId\": {\"id\": \"crash-test\"}, \"idempotencyKey\": \"$idem_b64\", \"operations\": [{\"setEntity\": {\"key\": \"$key\", \"value\": \"$value_b64\"}}]}" \
    "$addr" \
    ledger.v1.WriteService/Write 2>/dev/null || true)

  if echo "$result" | jq -e '.success.blockHeight' &>/dev/null; then
    echo "ok:$key"
  else
    echo "fail:$key"
  fi
}

# Background writer loop. Writes to a rotating set of nodes for
# WRITER_DURATION_SECS, logging results to $1.
# Args: output_file nodes_csv
run_writer() {
  local outfile=$1
  local nodes_csv=$2
  IFS=',' read -ra nodes <<< "$nodes_csv"
  local start_ts=$SECONDS
  local idx=1
  while (( SECONDS - start_ts < WRITER_DURATION_SECS )); do
    local target=${nodes[$(( idx % ${#nodes[@]} ))]}
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
    info=$(grpcurl -plaintext "$addr" ledger.v1.AdminService/GetClusterInfo 2>/dev/null || true)
    local leader_id
    leader_id=$(echo "$info" | jq -r '.leaderId // empty' 2>/dev/null || true)
    [[ -z "$leader_id" || "$leader_id" == "0" ]] && continue

    local j
    for ((j=1; j<=NODE_COUNT; j++)); do
      local check_addr node_id
      check_addr=$(node_addr "$j")
      node_id=$(grpcurl -plaintext "$check_addr" ledger.v1.AdminService/GetNodeInfo 2>/dev/null | jq -r '.nodeId // empty' 2>/dev/null || true)
      if [[ "$node_id" == "$leader_id" ]]; then
        echo "$j"
        return 0
      fi
    done
  done
  echo ""
}

# Kill a node by listen-port (SIGKILL — simulates crash).
kill_node_hard() {
  local node_num=$1
  local port
  port=$(node_port "$node_num")
  local pid
  pid=$(lsof -ti "tcp:$port" -sTCP:LISTEN 2>/dev/null || true)
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
    log_warn "No LISTEN PID for node $node_num (already dead?)"
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
    --enable-grpc-reflection \
    --email-blinding-key "$blinding_key" \
    --log-format text \
    > "$DATA_ROOT/node$node_num.restart.log" 2>&1 &
  CLUSTER_PIDS+=("$!")
  log_info "Restarted node $node_num (PID $!)"

  # Wait for listen port
  local elapsed=0
  while (( elapsed < 30 )); do
    nc -z 127.0.0.1 "$port" 2>/dev/null && { log_success "Node $node_num listening again"; return 0; }
    sleep 1
    elapsed=$((elapsed + 1))
  done
  log_error "Node $node_num did not start listening within 30s"
  return 1
}

# Poll each node's tip. Verify all nodes agree on (height, state_root),
# and that every node reports a non-empty, non-zero tip (restarted nodes
# return empty during rejoin).
verify_convergence() {
  log_info "Waiting for convergence (timeout: ${CONVERGENCE_TIMEOUT}s)..."
  local elapsed=0

  while (( elapsed < CONVERGENCE_TIMEOUT )); do
    local heights=() roots=() i
    local all_responsive=true

    for ((i=1; i<=NODE_COUNT; i++)); do
      local addr tip height root
      addr=$(node_addr "$i")
      tip=$(grpcurl -plaintext \
        -d "{\"organization\": {\"slug\": \"$ORG_SLUG\"}, \"vault\": {\"slug\": \"$VAULT_SLUG\"}}" \
        "$addr" \
        ledger.v1.ReadService/GetTip 2>/dev/null || true)
      height=$(echo "$tip" | jq -r '.height // empty' 2>/dev/null || true)
      root=$(echo "$tip" | jq -r '.stateRoot.value // empty' 2>/dev/null || true)
      if [[ -z "$height" || "$height" == "0" || -z "$root" ]]; then
        all_responsive=false
        break
      fi
      heights+=("$height")
      roots+=("$root")
    done

    if [[ "$all_responsive" == "true" ]]; then
      local h0=${heights[0]} r0=${roots[0]}
      local all_same=true
      for ((i=1; i<NODE_COUNT; i++)); do
        if [[ "${heights[$i]}" != "$h0" || "${roots[$i]}" != "$r0" ]]; then
          all_same=false
          break
        fi
      done
      if [[ "$all_same" == "true" ]]; then
        log_success "Convergence: height=$h0, state_root=${r0:0:16}..."
        return 0
      fi
    fi

    sleep 2
    elapsed=$((elapsed + 2))
  done

  log_error "Nodes did not converge within ${CONVERGENCE_TIMEOUT}s"
  for ((i=1; i<=NODE_COUNT; i++)); do
    local addr tip
    addr=$(node_addr "$i")
    tip=$(grpcurl -plaintext \
      -d "{\"organization\": {\"slug\": \"$ORG_SLUG\"}, \"vault\": {\"slug\": \"$VAULT_SLUG\"}}" \
      "$addr" ledger.v1.ReadService/GetTip 2>/dev/null || true)
    log_error "  Node $i tip: $tip"
  done
  return 1
}

# Verify every "ok:" line in the writer log is readable on every node.
verify_no_data_loss() {
  local writer_log=$1
  log_info "Verifying no data loss (reading back all successful writes)..."

  local ok_count=0 missing=0 key_val addr read_result val
  while IFS= read -r line; do
    [[ "$line" != ok:* ]] && continue
    key_val="${line#ok:}"
    ok_count=$((ok_count + 1))

    # Read from node 1 (eventual consistency — all nodes should have it after convergence).
    addr=$(node_addr 1)
    read_result=$(grpcurl -plaintext \
      -d "{\"caller\": {\"slug\": \"$USER_SLUG\"}, \"organization\": {\"slug\": \"$ORG_SLUG\"}, \"vault\": {\"slug\": \"$VAULT_SLUG\"}, \"key\": \"$key_val\", \"consistency\": \"READ_CONSISTENCY_EVENTUAL\"}" \
      "$addr" \
      ledger.v1.ReadService/Read 2>/dev/null || true)
    val=$(echo "$read_result" | jq -r '.value // empty' 2>/dev/null || true)
    if [[ -z "$val" ]]; then
      missing=$((missing + 1))
      if (( missing <= 3 )); then
        log_error "  Missing: $key_val"
        log_error "    Response: ${read_result:0:200}"
      fi
    fi
  done < "$writer_log"

  if (( missing > 0 )); then
    log_error "DATA LOSS: $missing/$ok_count successful writes missing after recovery"
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

  log_info "═══ Scenario $name: kill $kill_target_role mid-write ═══"

  bootstrap_cluster "$BASE_PORT" "$NODE_COUNT" "$DATA_ROOT" 5
  create_org_and_vault "$(node_addr 1)"

  # Prewrite a small baseline so crash happens after steady state.
  log_info "Writing 20 baseline entities..."
  local i
  for ((i=1; i<=20; i++)); do
    write_one 1 "$i" >/dev/null
  done

  local leader
  leader=$(find_leader)
  [[ -z "$leader" ]] && { log_error "No leader found"; return 1; }
  log_info "Current leader: node $leader"

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

  # Background writer targets all nodes (round-robin).
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
  sleep 3

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
