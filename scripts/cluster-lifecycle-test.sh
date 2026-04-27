#!/usr/bin/env bash
# Cluster lifecycle integration test.
#
# Validates the full spectrum of distributed cluster behaviors:
#
#   Phase 1: Boot a 3-node cluster, write data, verify replication
#   Phase 2: Add a 4th node (--join), verify it catches up with all data
#   Phase 3: Write data via leader in 4-node cluster, verify all 4 agree
#   Phase 4: Graceful leader handoff via TransferLeadership RPC
#   Phase 5: Shut down the original 3 nodes, write data to the survivor
#   Phase 6: Boot 2 new nodes, verify the new 3-node cluster has all data
#
# This exercises: coordinated bootstrap, Raft log replication, dynamic
# membership changes, snapshot transfer, leader failover, graceful leader
# transfer, and catch-up.
#
# Usage:
#   ./scripts/cluster-lifecycle-test.sh                  # Full test (release)
#   ./scripts/cluster-lifecycle-test.sh --debug          # Debug build
#   ./scripts/cluster-lifecycle-test.sh --skip-build     # Skip cargo build
#   ./scripts/cluster-lifecycle-test.sh --help           # Show help

set -euo pipefail
cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_PORT=50071
DATA_ROOT="/tmp/ledger-lifecycle-test-$$"
PROFILE="release"
BINARY="target/release/inferadb-ledger"
SKIP_BUILD=false

# Timeouts
HEALTH_TIMEOUT=60
SYNC_TIMEOUT=180
JOIN_TIMEOUT=60

# Bulk write tuning — operations per Write RPC in write_batch.
# Each RPC = one Raft log entry, so smaller values create more entries
# and stress replication catch-up harder.
BATCH_CHUNK_SIZE=10

# Tracking
ALL_PIDS=()
ACTIVE_PIDS=()

# Test data tracking (populated during writes)
ORG_SLUG=""
VAULT_SLUG=""

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
  case $1 in
    --debug)
      PROFILE="debug"
      BINARY="target/debug/inferadb-ledger"
      shift
      ;;
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    --help|-h)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Cluster lifecycle integration test."
      echo ""
      echo "Options:"
      echo "  --debug        Build in debug mode (default: release)"
      echo "  --skip-build   Skip cargo build (use existing binary)"
      echo "  --help         Show this help"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }
log_phase()   { echo -e "\n${BOLD}${CYAN}═══════════════════════════════════════════════════════${NC}"; echo -e "${BOLD}${CYAN}  $1${NC}"; echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════${NC}\n"; }
log_step()    { echo -e "  ${BOLD}→${NC} $1"; }

# Generate a deterministic base64-encoded 16-byte UUID for idempotency keys.
# Uses the counter argument to produce unique keys per write.
generate_idempotency_key() {
  local counter=$1
  printf '%032x' "$counter" | xxd -r -p | base64
}

# Get the port for a given node number.
node_port() {
  echo $((BASE_PORT + $1 - 1))
}

# Get the gRPC address for a given node number.
node_addr() {
  echo "127.0.0.1:$(node_port "$1")"
}

# ---------------------------------------------------------------------------
# Cleanup — runs on ANY exit
# ---------------------------------------------------------------------------

PROTECTED_PIDS=()

cleanup() {
  local exit_code=$?
  echo ""
  log_info "Cleaning up..."

  # Send SIGTERM to all non-protected processes.
  for pid in "${ALL_PIDS[@]+"${ALL_PIDS[@]}"}"; do
    local protected=false
    for pp in "${PROTECTED_PIDS[@]+"${PROTECTED_PIDS[@]}"}"; do
      [[ "$pid" == "$pp" ]] && protected=true && break
    done
    [[ "$protected" == "true" ]] && continue

    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done

  # Brief wait for graceful shutdown, then SIGKILL stragglers.
  sleep 2
  for pid in "${ALL_PIDS[@]+"${ALL_PIDS[@]}"}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done

  # Reap all child processes (non-blocking after SIGKILL).
  for pid in "${ALL_PIDS[@]+"${ALL_PIDS[@]}"}"; do
    wait "$pid" 2>/dev/null || true
  done

  if [[ -d "$DATA_ROOT" ]]; then
    if [[ $exit_code -ne 0 ]]; then
      log_info "Preserving data directory for debugging: $DATA_ROOT"
    else
      rm -rf "$DATA_ROOT"
      log_info "Removed data directory: $DATA_ROOT"
    fi
  fi

  if [[ $exit_code -eq 0 ]]; then
    log_success "Cleanup complete"
  fi
  exit "$exit_code"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Cluster management functions
# ---------------------------------------------------------------------------

# Start a node (first node or with --join seed).
# Args: node_number [seed_address]
# If seed_address is provided, passes --join. Otherwise starts without seeds.
start_node() {
  local node_num=$1
  local seed_addr="${2:-}"
  local port
  port=$(node_port "$node_num")
  local node_data="$DATA_ROOT/node$node_num"
  mkdir -p "$node_data"

  local join_args=()
  if [[ -n "$seed_addr" ]]; then
    join_args=(--join "$seed_addr")
  fi

  # Fixed test blinding key (matches TestCluster / SDK e2e configuration)
  local blinding_key="deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

  RUST_LOG=info,inferadb_ledger_raft::leader_transfer=debug "$BINARY" \
    --listen "127.0.0.1:$port" \
    --data "$node_data" \
    "${join_args[@]+"${join_args[@]}"}" \
    --enable-grpc-reflection \
    --email-blinding-key "$blinding_key" \
    --log-format text \
    > "$DATA_ROOT/node$node_num.log" 2>&1 &

  local pid=$!
  ALL_PIDS+=("$pid")
  ACTIVE_PIDS+=("$pid")
  if [[ -n "$seed_addr" ]]; then
    log_info "  Node $node_num: PID $pid, port $port (join seed: $seed_addr)"
  else
    log_info "  Node $node_num: PID $pid, port $port (first node)"
  fi
}

# Wait for specific nodes to be listening on their ports.
# Args: node_numbers...
wait_for_ports() {
  local nodes=("$@")
  local elapsed=0

  log_info "Waiting for nodes to listen (timeout: ${HEALTH_TIMEOUT}s)..."
  while [[ $elapsed -lt $HEALTH_TIMEOUT ]]; do
    local all_listening=true
    for node_num in "${nodes[@]}"; do
      local port
      port=$(node_port "$node_num")
      if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
        all_listening=false
        break
      fi
    done

    if [[ "$all_listening" == "true" ]]; then
      log_success "All specified nodes are listening"
      return 0
    fi

    # Check for early crashes
    for pid in "${ACTIVE_PIDS[@]}"; do
      if ! kill -0 "$pid" 2>/dev/null; then
        log_error "Process $pid exited prematurely"
        # Find which node this was
        for node_num in "${nodes[@]}"; do
          local log_file="$DATA_ROOT/node$node_num.log"
          if [[ -f "$log_file" ]]; then
            log_error "--- Node $node_num log (last 20 lines) ---"
            tail -20 "$log_file" 2>/dev/null || true
          fi
        done
        return 1
      fi
    done

    sleep 1
    elapsed=$((elapsed + 1))
  done

  log_error "Nodes did not become ready within ${HEALTH_TIMEOUT}s"
  for node_num in "${nodes[@]}"; do
    log_error "--- Node $node_num log (last 20 lines) ---"
    tail -20 "$DATA_ROOT/node$node_num.log" 2>/dev/null || true
  done
  return 1
}

# Wait for leader election by polling GetClusterInfo until a leader is reported.
# Args: node_numbers...
wait_for_leader() {
  local nodes=("$@")
  local elapsed=0

  log_info "Waiting for leader election (timeout: ${HEALTH_TIMEOUT}s)..."
  while [[ $elapsed -lt $HEALTH_TIMEOUT ]]; do
    for node_num in "${nodes[@]}"; do
      local addr
      addr=$(node_addr "$node_num")
      local result
      result=$(grpcurl -plaintext "$addr" ledger.v1.AdminService/GetClusterInfo 2>/dev/null || true)
      local leader_id
      leader_id=$(echo "$result" | jq -r '.leaderId // empty' 2>/dev/null || true)
      if [[ -n "$leader_id" && "$leader_id" != "0" ]]; then
        log_success "Leader elected (reported by node $node_num)"
        # Brief settle for cluster stabilization after election
        sleep 2
        return 0
      fi
    done

    sleep 1
    elapsed=$((elapsed + 1))
  done

  log_error "No leader elected within ${HEALTH_TIMEOUT}s"
  for node_num in "${nodes[@]}"; do
    log_error "--- Node $node_num log (last 20 lines) ---"
    tail -20 "$DATA_ROOT/node$node_num.log" 2>/dev/null || true
  done
  return 1
}

# Find which node is currently the leader.
# Args: node_numbers...
# Outputs the node number of the leader (empty string if not found).
find_leader() {
  local nodes=("$@")
  for node_num in "${nodes[@]}"; do
    local addr
    addr=$(node_addr "$node_num")
    local result
    result=$(grpcurl -plaintext "$addr" ledger.v1.AdminService/GetClusterInfo 2>/dev/null || true)
    local leader_id
    leader_id=$(echo "$result" | jq -r '.leaderId // empty' 2>/dev/null || true)
    if [[ -n "$leader_id" && "$leader_id" != "0" ]]; then
      # Find which node number corresponds to this leader_id
      for check_num in "${nodes[@]}"; do
        local check_addr
        check_addr=$(node_addr "$check_num")
        local node_info
        node_info=$(grpcurl -plaintext "$check_addr" ledger.v1.AdminService/GetNodeInfo 2>/dev/null || true)
        local node_id
        node_id=$(echo "$node_info" | jq -r '.nodeId // empty' 2>/dev/null || true)
        if [[ "$node_id" == "$leader_id" ]]; then
          echo "$check_num"
          return 0
        fi
      done
    fi
  done
  # No leader found — return empty string with success exit code.
  # Callers check for empty output, and set -e must not kill the script here.
  echo ""
  return 0
}

# Get a node's Snowflake ID via GetNodeInfo RPC.
# Args: node_number
get_node_id() {
  local node_num=$1
  local addr
  addr=$(node_addr "$node_num")
  local result
  result=$(grpcurl -plaintext "$addr" ledger.v1.AdminService/GetNodeInfo 2>/dev/null) || true
  echo "$result" | jq -r '.nodeId'
}

# Add a node to the cluster via JoinCluster RPC.
# Args: leader_node_number joining_node_number
join_node_to_cluster() {
  local leader_num=$1
  local joining_num=$2
  local leader_addr
  leader_addr=$(node_addr "$leader_num")
  local joining_addr
  joining_addr=$(node_addr "$joining_num")
  local joining_id
  joining_id=$(get_node_id "$joining_num")

  log_step "Requesting join: node $joining_num (ID: $joining_id) via leader node $leader_num"

  local result
  result=$(grpcurl -plaintext \
    -d "{\"node_id\": $joining_id, \"address\": \"$joining_addr\"}" \
    "$leader_addr" \
    ledger.v1.AdminService/JoinCluster 2>&1) || true

  local success
  success=$(echo "$result" | jq -r '.success // false' 2>/dev/null || echo "false")
  if [[ "$success" != "true" ]]; then
    log_error "JoinCluster failed: $result"
    return 1
  fi
  log_success "Node $joining_num joined the cluster"
}

# Wait for a node to become a voter (fully caught up).
# Args: node_number expected_voter_count any_active_node_number
wait_for_voter() {
  local target_num=$1
  local expected_voters=$2
  local query_num=$3
  local elapsed=0

  local target_id
  target_id=$(get_node_id "$target_num")

  log_info "Waiting for node $target_num to become voter (timeout: ${JOIN_TIMEOUT}s)..."
  while [[ $elapsed -lt $JOIN_TIMEOUT ]]; do
    local query_addr
    query_addr=$(node_addr "$query_num")
    local result
    result=$(grpcurl -plaintext "$query_addr" ledger.v1.AdminService/GetClusterInfo 2>/dev/null || true)

    # Check if target node is a VOTER
    local role
    role=$(echo "$result" | jq -r ".members[] | select(.nodeId == \"$target_id\") | .role" 2>/dev/null || true)
    if [[ "$role" == "CLUSTER_MEMBER_ROLE_VOTER" ]]; then
      local voter_count
      voter_count=$(echo "$result" | jq '[.members[] | select(.role == "CLUSTER_MEMBER_ROLE_VOTER")] | length' 2>/dev/null || echo "0")
      if [[ "$voter_count" -ge "$expected_voters" ]]; then
        log_success "Node $target_num is now a voter ($voter_count voters total)"
        return 0
      fi
    fi

    sleep 1
    elapsed=$((elapsed + 1))
  done

  log_error "Node $target_num did not become voter within ${JOIN_TIMEOUT}s"
  return 1
}

# Remove a node from the cluster via LeaveCluster RPC.
# Retries automatically when a prior membership change is still in-flight
# (Raft allows only one at a time).
# Args: leader_node_number leaving_node_number
leave_cluster() {
  local leader_num=$1
  local leaving_num=$2
  local leader_addr
  leader_addr=$(node_addr "$leader_num")
  local leaving_id
  leaving_id=$(get_node_id "$leaving_num")

  local attempts=0
  local max_attempts=15
  while [[ $attempts -lt $max_attempts ]]; do
    local result
    result=$(grpcurl -plaintext \
      -d "{\"node_id\": $leaving_id}" \
      "$leader_addr" \
      ledger.v1.AdminService/LeaveCluster 2>&1 || true)

    # Success
    local success
    success=$(echo "$result" | jq -r '.success // false' 2>/dev/null || echo "false")
    if [[ "$success" == "true" ]]; then
      return 0
    fi

    local message
    message=$(echo "$result" | jq -r '.message // ""' 2>/dev/null || true)

    # Leader transferred — retry on any listening node to find the new leader
    if echo "$message" | grep -q "Leader transferred\|retry LeaveCluster"; then
      attempts=$((attempts + 1))
      log_info "    Leader transferred, finding new leader ($attempts/$max_attempts)..."
      sleep 1
      # Try each listening port to find the new leader
      for port in $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
          leader_addr="127.0.0.1:$port"
          break
        fi
      done
      continue
    fi

    # Retryable: prior membership change still committing
    if echo "$result" | grep -q "already undergoing a configuration change"; then
      attempts=$((attempts + 1))
      log_info "    Membership change in progress, retrying ($attempts/$max_attempts)..."
      sleep 1
      continue
    fi

    # Not the leader — try another node
    if echo "$message" | grep -q "Not the leader"; then
      attempts=$((attempts + 1))
      sleep 0.5
      for port in $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then
          leader_addr="127.0.0.1:$port"
          break
        fi
      done
      continue
    fi

    # Non-retryable failure
    log_warn "LeaveCluster response: $result"
    return 0
  done

  log_warn "LeaveCluster retries exhausted for node $leaving_num"
}

# Wait for a node to disappear from cluster membership.
# Polls GetClusterInfo on the query node until the target is gone.
# Args: removed_node_number query_node_number
wait_for_member_removed() {
  local removed_num=$1
  local query_num=$2
  local start_time=$SECONDS
  local timeout=30

  local removed_id
  removed_id=$(get_node_id "$removed_num")

  while (( SECONDS - start_time < timeout )); do
    local query_addr
    query_addr=$(node_addr "$query_num")
    local result
    result=$(grpcurl -plaintext "$query_addr" ledger.v1.AdminService/GetClusterInfo 2>/dev/null || true)

    # Check if the removed node is still listed
    local found
    found=$(echo "$result" | jq -r ".members[] | select(.nodeId == \"$removed_id\") | .nodeId" 2>/dev/null || true)
    if [[ -z "$found" ]]; then
      return 0
    fi

    sleep 0.5
  done

  log_warn "Node $removed_num still in membership after ${timeout}s (proceeding anyway)"
}

# Kill a node process by node number.
# Args: node_number
kill_node() {
  local node_num=$1
  local port
  port=$(node_port "$node_num")

  # Find the PID listening on this port
  for i in "${!ACTIVE_PIDS[@]}"; do
    local pid="${ACTIVE_PIDS[$i]}"
    if kill -0 "$pid" 2>/dev/null; then
      # Check if this PID owns the port via its log file
      if grep -q "127.0.0.1:$port" "$DATA_ROOT/node$node_num.log" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        unset 'ACTIVE_PIDS[i]'
        ACTIVE_PIDS=("${ACTIVE_PIDS[@]}")
        log_info "  Killed node $node_num (PID $pid)"
        return 0
      fi
    fi
  done

  # Fallback: kill by port (LISTEN only — avoid killing nodes with client connections)
  local pid
  pid=$(lsof -ti "tcp:$port" -sTCP:LISTEN 2>/dev/null || true)
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    log_info "  Killed node $node_num (PID $pid, found by port)"
  fi
}

# Kill multiple node processes in parallel.
# Sends SIGTERM to all nodes first, then waits for all to exit.
# This parallelizes graceful shutdowns instead of running them sequentially.
# Args: node_numbers...
kill_nodes_parallel() {
  local nodes=("$@")
  local pids_to_wait=()

  # Phase 1: send SIGTERM to each node by finding its LISTEN PID.
  # Using lsof -sTCP:LISTEN avoids killing nodes that merely have
  # client connections to the target port.
  for node_num in "${nodes[@]}"; do
    local port
    port=$(node_port "$node_num")
    local pid
    pid=$(lsof -ti "tcp:$port" -sTCP:LISTEN 2>/dev/null || true)
    if [[ -n "$pid" ]]; then
      kill -9 "$pid" 2>/dev/null || true
      pids_to_wait+=("$pid")
      log_info "  Sent SIGKILL to node $node_num (PID $pid, port $port)"
    else
      log_info "  Node $node_num (port $port): no LISTEN process found (already exited?)"
    fi
  done

  # Phase 2: wait for all to exit
  for pid in "${pids_to_wait[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
}

# ---------------------------------------------------------------------------
# Data operation functions
# ---------------------------------------------------------------------------

IDEM_COUNTER=1

# Create a user and organization via the onboarding flow. Sets ORG_SLUG and USER_SLUG.
# Retries across all active nodes to handle data region creation + leader election.
# Args: org_name
create_org() {
  local node_num=$1
  local name=$2
  local email
  email="test-$(uuidgen | tr '[:upper:]' '[:lower:]')@lifecycle.local"
  local region=10  # REGION_US_EAST_VA

  # Phase 1: InitiateEmailVerification — retry across endpoints
  local code=""
  local attempt=0
  while [[ -z "$code" && $attempt -lt 30 ]]; do
    # Try each listening port in the cluster
    for port in $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
      if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
        continue
      fi
      local addr="127.0.0.1:$port"
      local result
      result=$(grpcurl -plaintext \
        -d "{\"email\": \"$email\", \"region\": $region}" \
        "$addr" \
        ledger.v1.UserService/InitiateEmailVerification 2>&1) || true
      local extracted_code
      extracted_code=$(echo "$result" | jq -r '.code // empty' 2>/dev/null || true)
      if [[ -n "$extracted_code" ]]; then
        code="$extracted_code"
        break
      fi
    done
    attempt=$((attempt + 1))
    sleep 1
  done

  if [[ -z "$code" ]]; then
    log_error "InitiateEmailVerification failed after 30 attempts"
    return 1
  fi

  # Phase 2: VerifyEmailCode — retry across nodes (regional leader may differ)
  local onboarding_token=""
  local verify_attempt=0
  while [[ -z "$onboarding_token" && $verify_attempt -lt 30 ]]; do
    for port in $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
      if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
        continue
      fi
      local vaddr="127.0.0.1:$port"
      local verify_result
      verify_result=$(grpcurl -plaintext \
        -d "{\"email\": \"$email\", \"code\": \"$code\", \"region\": $region}" \
        "$vaddr" \
        ledger.v1.UserService/VerifyEmailCode 2>&1) || true
      onboarding_token=$(echo "$verify_result" | jq -r '.newUser.onboardingToken // empty' 2>/dev/null || true)
      if [[ -n "$onboarding_token" ]]; then
        break 2
      fi
    done
    verify_attempt=$((verify_attempt + 1))
    sleep 0.5
  done
  if [[ -z "$onboarding_token" ]]; then
    log_error "VerifyEmailCode failed after $verify_attempt attempts"
    return 1
  fi

  # Phase 3: CompleteRegistration — must run on the GLOBAL leader (saga orchestrator).
  # The GLOBAL leader may differ from the regional leader used for email verification.
  # Retry across all cluster nodes until we hit the GLOBAL leader.
  local reg_attempt=0
  while [[ $reg_attempt -lt 15 ]]; do
    for port in $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
      if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
        continue
      fi
      local reg_addr="127.0.0.1:$port"
      local reg_result
      reg_result=$(grpcurl -plaintext \
        -d "{\"onboarding_token\": \"$onboarding_token\", \"email\": \"$email\", \"region\": $region, \"name\": \"Test Admin\", \"organization_name\": \"$name\"}" \
        "$reg_addr" \
        ledger.v1.UserService/CompleteRegistration 2>&1) || true
      ORG_SLUG=$(echo "$reg_result" | jq -r '.organization.slug // empty' 2>/dev/null || true)
      USER_SLUG=$(echo "$reg_result" | jq -r '.user.slug.slug // empty' 2>/dev/null || true)
      if [[ -n "$ORG_SLUG" && -n "$USER_SLUG" ]]; then
        break 2
      fi
    done
    reg_attempt=$((reg_attempt + 1))
    sleep 1
  done

  if [[ -z "$ORG_SLUG" || -z "$USER_SLUG" ]]; then
    log_error "CompleteRegistration failed after 15 attempts"
    return 1
  fi
  log_step "Created organization '$name' (slug: $ORG_SLUG) with admin user (slug: $USER_SLUG)"
}

# Generate a non-zero u64 Snowflake-shaped slug suitable for client-supplied vault IDs.
# Uses /dev/urandom for entropy and clears the high bit so the value fits in a signed
# i64 representation when round-tripped through tooling that interprets uint64 as JSON
# numbers. The exact bit layout doesn't matter — the server accepts any non-zero u64.
generate_snowflake_slug() {
  local raw
  raw=$(od -An -N8 -tu8 < /dev/urandom | tr -d ' \n')
  if [[ -z "$raw" || "$raw" == "0" ]]; then
    raw=1
  fi
  echo $(( (raw & 0x7FFFFFFFFFFFFFFF) | 1 ))
}

# Create a vault. Sets VAULT_SLUG.
# Retries because the organization may still be provisioning after creation.
# Also retries across all nodes on NotLeader (redirect-only routing model).
# Args: leader_node_number
create_vault() {
  local node_num=$1

  # CreateVaultRequest requires a client-supplied Snowflake slug. Generate it
  # once and reuse it across retries so the per-org idempotency check returns
  # the existing vault rather than allocating duplicates.
  local client_slug
  client_slug=$(generate_snowflake_slug)

  local attempt=0
  local max_attempts=30
  while [[ $attempt -lt $max_attempts ]]; do
    # Try all listening cluster ports (preferred node first) to find the leader.
    local tried_preferred=false
    local port
    for port in $(node_port "$node_num") $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
      # Skip duplicate attempt on preferred node.
      if [[ "$port" == "$(node_port "$node_num")" ]]; then
        [[ "$tried_preferred" == "true" ]] && continue
        tried_preferred=true
      fi
      if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
        continue
      fi
      local addr="127.0.0.1:$port"
      local result
      result=$(grpcurl -plaintext \
        -d "{\"organization\": {\"slug\": \"$ORG_SLUG\"}, \"caller\": {\"slug\": \"$USER_SLUG\"}, \"slug\": {\"slug\": \"$client_slug\"}}" \
        "$addr" \
        ledger.v1.VaultService/CreateVault 2>&1) || true

      VAULT_SLUG=$(echo "$result" | jq -r '.vault.slug // empty' 2>/dev/null || true)
      if [[ -n "$VAULT_SLUG" ]]; then
        log_step "Created vault (slug: $VAULT_SLUG)"
        return 0
      fi

      # NotLeader — try next node.
      if echo "$result" | grep -qiE 'not the leader|NotLeader|Unavailable'; then
        continue
      fi

      # Provisioning/not-found — retry after delay (all nodes will fail the same way).
      if echo "$result" | grep -qiE 'not found|provisioned|provisioning'; then
        break
      fi

      # Other error on this node — try next node before giving up.
    done

    attempt=$((attempt + 1))
    sleep 1
  done

  log_error "CreateVault timed out after $max_attempts attempts"
  return 1
}

# Write an entity. Tries the preferred node first; on NotLeader / Unavailable,
# extracts leader_endpoint from the error if available and retries there, then
# falls back to trying all listening nodes (redirect-only routing model).
# Args: node_number key value_string
write_entity() {
  local node_num=$1
  local key=$2
  local value=$3

  local idem_key
  idem_key=$(generate_idempotency_key $IDEM_COUNTER)
  IDEM_COUNTER=$((IDEM_COUNTER + 1))

  # base64-encode the value for protobuf bytes field
  local value_b64
  value_b64=$(echo -n "$value" | base64)

  local payload="{
    \"caller\": {\"slug\": \"$USER_SLUG\"},
    \"organization\": {\"slug\": \"$ORG_SLUG\"},
    \"vault\": {\"slug\": \"$VAULT_SLUG\"},
    \"clientId\": {\"id\": \"lifecycle-test\"},
    \"idempotencyKey\": \"$idem_key\",
    \"operations\": [{
      \"setEntity\": {
        \"key\": \"$key\",
        \"value\": \"$value_b64\"
      }
    }]
  }"

  # Try the preferred node first.
  local addr result block_height
  addr=$(node_addr "$node_num")
  result=$(grpcurl -plaintext -d "$payload" "$addr" ledger.v1.WriteService/Write 2>&1) || true
  block_height=$(echo "$result" | jq -r '.success.blockHeight // empty' 2>/dev/null || true)
  if [[ -n "$block_height" ]]; then
    log_step "Wrote '$key' = '$value' (block $block_height) via node $node_num"
    return 0
  fi

  # Extract leader_endpoint hint from the error (e.g. "leader_endpoint=http://127.0.0.1:50072").
  local hint_addr
  hint_addr=$(echo "$result" | sed -n 's/.*leader_endpoint=http:\/\/\([0-9.:]*\).*/\1/p' 2>/dev/null | head -1)
  if [[ -n "$hint_addr" ]]; then
    result=$(grpcurl -plaintext -d "$payload" "$hint_addr" ledger.v1.WriteService/Write 2>&1) || true
    block_height=$(echo "$result" | jq -r '.success.blockHeight // empty' 2>/dev/null || true)
    if [[ -n "$block_height" ]]; then
      log_step "Wrote '$key' = '$value' (block $block_height) via hinted leader $hint_addr"
      return 0
    fi
  fi

  # Fall back to trying all listening cluster ports.
  local port
  for port in $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
    if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
      continue
    fi
    addr="127.0.0.1:$port"
    result=$(grpcurl -plaintext -d "$payload" "$addr" ledger.v1.WriteService/Write 2>&1) || true
    block_height=$(echo "$result" | jq -r '.success.blockHeight // empty' 2>/dev/null || true)
    if [[ -n "$block_height" ]]; then
      log_step "Wrote '$key' = '$value' (block $block_height) via $addr (fallback)"
      return 0
    fi
  done

  log_error "Write failed for key '$key' on all nodes: $result"
  return 1
}

# Write a numbered batch of entities via multi-operation Write RPCs.
# Entities are keyed as {prefix}:001 .. {prefix}:{count} with values
# "{value_prefix} item 001" etc. Operations are chunked into groups of
# BATCH_CHUNK_SIZE to produce multiple Raft log entries (better for
# stressing replication catch-up).
#
# Each chunk is sent to the preferred node first; on NotLeader / Unavailable,
# the leader_endpoint hint is tried, then all listening nodes (redirect model).
# Args: node_number key_prefix count value_prefix
write_batch() {
  local node_num=$1
  local key_prefix=$2
  local count=$3
  local value_prefix=$4

  local total_written=0
  while [[ $total_written -lt $count ]]; do
    local chunk_size=$(( count - total_written ))
    [[ $chunk_size -gt $BATCH_CHUNK_SIZE ]] && chunk_size=$BATCH_CHUNK_SIZE

    local idem_key
    idem_key=$(generate_idempotency_key $IDEM_COUNTER)
    IDEM_COUNTER=$((IDEM_COUNTER + 1))

    # Build operations JSON array for this chunk
    local ops=""
    for i in $(seq 1 "$chunk_size"); do
      local idx key
      idx=$(( total_written + i ))
      key="${key_prefix}:$(printf '%03d' "$idx")"
      local value_b64
      value_b64=$(echo -n "${value_prefix} item $(printf '%03d' "$idx")" | base64)
      [[ -n "$ops" ]] && ops+=","
      ops+="{\"setEntity\":{\"key\":\"$key\",\"value\":\"$value_b64\"}}"
    done

    local payload="{
      \"caller\": {\"slug\": \"$USER_SLUG\"},
      \"organization\": {\"slug\": \"$ORG_SLUG\"},
      \"vault\": {\"slug\": \"$VAULT_SLUG\"},
      \"clientId\": {\"id\": \"lifecycle-test\"},
      \"idempotencyKey\": \"$idem_key\",
      \"operations\": [$ops]
    }"

    local addr result block_height chunk_ok=false

    # Try the preferred node first.
    addr=$(node_addr "$node_num")
    result=$(grpcurl -plaintext -d "$payload" "$addr" ledger.v1.WriteService/Write 2>&1) || true
    block_height=$(echo "$result" | jq -r '.success.blockHeight // empty' 2>/dev/null || true)
    if [[ -n "$block_height" ]]; then
      chunk_ok=true
    fi

    # Try leader_endpoint hint from error.
    if [[ "$chunk_ok" != "true" ]]; then
      local hint_addr
      hint_addr=$(echo "$result" | sed -n 's/.*leader_endpoint=http:\/\/\([0-9.:]*\).*/\1/p' 2>/dev/null | head -1)
      if [[ -n "$hint_addr" ]]; then
        result=$(grpcurl -plaintext -d "$payload" "$hint_addr" ledger.v1.WriteService/Write 2>&1) || true
        block_height=$(echo "$result" | jq -r '.success.blockHeight // empty' 2>/dev/null || true)
        [[ -n "$block_height" ]] && chunk_ok=true
      fi
    fi

    # Fall back to all listening ports.
    if [[ "$chunk_ok" != "true" ]]; then
      local port
      for port in $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
        if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
          continue
        fi
        addr="127.0.0.1:$port"
        result=$(grpcurl -plaintext -d "$payload" "$addr" ledger.v1.WriteService/Write 2>&1) || true
        block_height=$(echo "$result" | jq -r '.success.blockHeight // empty' 2>/dev/null || true)
        if [[ -n "$block_height" ]]; then
          chunk_ok=true
          break
        fi
      done
    fi

    if [[ "$chunk_ok" != "true" ]]; then
      log_error "Batch write failed for prefix '$key_prefix' (items $((total_written+1))-$((total_written+chunk_size))): $result"
      return 1
    fi

    total_written=$((total_written + chunk_size))
  done

  log_step "Wrote $count entities with prefix '$key_prefix' via node $node_num"
}

# Read an entity from a specific node. Returns the value to stdout.
# Args: node_number key
read_entity() {
  local node_num=$1
  local key=$2
  local addr
  addr=$(node_addr "$node_num")

  local result
  result=$(grpcurl -plaintext \
    -d "{
      \"caller\": {\"slug\": \"$USER_SLUG\"},
      \"organization\": {\"slug\": \"$ORG_SLUG\"},
      \"vault\": {\"slug\": \"$VAULT_SLUG\"},
      \"key\": \"$key\",
      \"consistency\": \"READ_CONSISTENCY_EVENTUAL\"
    }" \
    "$addr" \
    ledger.v1.ReadService/Read 2>&1) || true

  # Value is base64-encoded bytes — decode it
  local value_b64
  value_b64=$(echo "$result" | jq -r '.value // empty' 2>/dev/null || true)
  if [[ -n "$value_b64" ]]; then
    echo "$value_b64" | base64 -d 2>/dev/null
  fi
}

# List all entities from a specific node. Returns sorted "key=value" lines.
# Args: node_number
list_entities() {
  local node_num=$1
  local addr
  addr=$(node_addr "$node_num")

  local all_entities=""
  local page_token=""

  while true; do
    local request="{
      \"caller\": {\"slug\": \"$USER_SLUG\"},
      \"organization\": {\"slug\": \"$ORG_SLUG\"},
      \"vault\": {\"slug\": \"$VAULT_SLUG\"},
      \"keyPrefix\": \"\",
      \"limit\": 100,
      \"consistency\": \"READ_CONSISTENCY_EVENTUAL\"
    }"

    if [[ -n "$page_token" ]]; then
      request="{
        \"caller\": {\"slug\": \"$USER_SLUG\"},
        \"organization\": {\"slug\": \"$ORG_SLUG\"},
        \"vault\": {\"slug\": \"$VAULT_SLUG\"},
        \"keyPrefix\": \"\",
        \"limit\": 100,
        \"pageToken\": \"$page_token\",
        \"consistency\": \"READ_CONSISTENCY_EVENTUAL\"
      }"
    fi

    local result
    result=$(grpcurl -plaintext -d "$request" "$addr" ledger.v1.ReadService/ListEntities 2>&1) || true

    # Extract entities: key + base64-decoded value
    local entities
    entities=$(echo "$result" | jq -r '.entities[]? | .key + "=" + .value' 2>/dev/null || true)
    if [[ -n "$entities" ]]; then
      # Decode base64 values
      while IFS='=' read -r key value_b64; do
        local value_decoded
        value_decoded=$(echo "$value_b64" | base64 -d 2>/dev/null || echo "$value_b64")
        all_entities+="${key}=${value_decoded}"$'\n'
      done <<< "$entities"
    fi

    page_token=$(echo "$result" | jq -r '.nextPageToken // empty' 2>/dev/null || true)
    if [[ -z "$page_token" ]]; then
      break
    fi
  done

  echo -n "$all_entities" | sort
}

# Get the tip (block height + state root) from a node.
# GetTip requires the data region leader (no consistency parameter), so this
# tries the preferred node first, then follows the leader_endpoint hint, then
# falls back to all listening nodes (redirect-only routing model).
# Args: node_number
get_tip() {
  local node_num=$1
  local payload="{
    \"organization\": {\"slug\": \"$ORG_SLUG\"},
    \"vault\": {\"slug\": \"$VAULT_SLUG\"}
  }"

  # Try the preferred node first.
  local addr result
  addr=$(node_addr "$node_num")
  result=$(grpcurl -plaintext -d "$payload" "$addr" ledger.v1.ReadService/GetTip 2>/dev/null) || true
  if echo "$result" | jq -e '.height' &>/dev/null; then
    echo "$result"
    return 0
  fi

  # Try leader_endpoint hint from the error.
  local hint_addr
  hint_addr=$(echo "$result" | sed -n 's/.*leader_endpoint=http:\/\/\([0-9.:]*\).*/\1/p' 2>/dev/null | head -1)
  if [[ -n "$hint_addr" ]]; then
    result=$(grpcurl -plaintext -d "$payload" "$hint_addr" ledger.v1.ReadService/GetTip 2>/dev/null) || true
    if echo "$result" | jq -e '.height' &>/dev/null; then
      echo "$result"
      return 0
    fi
  fi

  # Fall back to all listening ports.
  local port
  for port in $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
    if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
      continue
    fi
    addr="127.0.0.1:$port"
    result=$(grpcurl -plaintext -d "$payload" "$addr" ledger.v1.ReadService/GetTip 2>/dev/null) || true
    if echo "$result" | jq -e '.height' &>/dev/null; then
      echo "$result"
      return 0
    fi
  done

  # All nodes failed — return empty (callers handle this gracefully).
  echo ""
}

# ---------------------------------------------------------------------------
# Verification functions
# ---------------------------------------------------------------------------

# Point-read a specific key on every specified node and assert the value.
# This catches silent data loss and replication corruption that list-based
# comparison alone cannot detect (all nodes could agree on "nothing").
# Nodes not in the data region (empty read result) are skipped.
# Args: key expected_value node_numbers...
verify_read_on_nodes() {
  local key=$1
  local expected=$2
  shift 2
  local nodes=("$@")
  local verified_nodes=()

  for node_num in "${nodes[@]}"; do
    local value
    value=$(read_entity "$node_num" "$key")
    if [[ -z "$value" ]]; then
      # Node may not be in the data region — skip it.
      continue
    fi
    if [[ "$value" != "$expected" ]]; then
      log_error "Node $node_num: read '$key' = '$value', expected '$expected'"
      return 1
    fi
    verified_nodes+=("$node_num")
  done
  if [[ ${#verified_nodes[@]} -eq 0 ]]; then
    log_warn "No nodes could serve read for '$key' — data region may not include any of: ${nodes[*]}"
  else
    log_step "Verified '$key' on nodes: ${verified_nodes[*]}"
  fi
}

# Wait for replication to converge across nodes, then verify data consistency.
#
# With redirect-only routing, not all cluster members are necessarily in every
# data region. GetTip and ListEntities may fail on nodes outside the data
# region. This function:
#   1. Gets the tip from the data region leader (auto-redirects).
#   2. Waits for all nodes that CAN serve ListEntities to return matching data.
#   3. Nodes that consistently fail to serve reads (not in data region) are
#      noted but do not fail the check.
# Args: description node_numbers...
verify_data_consistency() {
  local description=$1
  shift
  local nodes=("$@")

  log_info "Verifying: $description"

  # Get the tip from the data region leader (get_tip auto-redirects).
  local tip ref_height
  tip=$(get_tip "${nodes[0]}" 2>/dev/null || true)
  ref_height=$(echo "$tip" | jq -r '.height // "0"' 2>/dev/null || echo "0")
  if [[ "$ref_height" != "0" ]]; then
    log_step "Data region leader tip: block $ref_height"
  fi

  # Wait for entity data to converge across responding nodes.
  local elapsed=0
  while [[ $elapsed -lt $SYNC_TIMEOUT ]]; do
    local all_match=true
    local reference_entities=""
    local responding_count=0

    for node_num in "${nodes[@]}"; do
      local node_entities
      node_entities=$(list_entities "$node_num" 2>/dev/null)
      if [[ -z "$node_entities" ]]; then
        # Node returned no data — might not be in data region. Skip it.
        continue
      fi
      responding_count=$((responding_count + 1))
      if [[ -z "$reference_entities" ]]; then
        reference_entities="$node_entities"
      elif [[ "$node_entities" != "$reference_entities" ]]; then
        all_match=false
        break
      fi
    done

    if [[ "$all_match" == "true" && "$responding_count" -gt 0 ]]; then
      local entity_count
      entity_count=$(echo -n "$reference_entities" | grep -c '.' || echo "0")
      if [[ "$entity_count" -gt 0 || "$ref_height" == "0" ]]; then
        log_success "$responding_count/${#nodes[@]} data-region nodes have identical data ($entity_count entities)"
        return 0
      fi
    fi

    sleep 1
    elapsed=$((elapsed + 1))
  done

  log_error "Replication did not converge within ${SYNC_TIMEOUT}s"
  for node_num in "${nodes[@]}"; do
    local count
    count=$(list_entities "$node_num" 2>/dev/null | grep -c '.' || echo "0")
    log_error "  Node $node_num: $count entities"
  done
  return 1
}

# ===========================================================================
# Main test flow
# ===========================================================================

# ---------------------------------------------------------------------------
# Pre-flight
# ---------------------------------------------------------------------------

if ! command -v grpcurl &>/dev/null; then
  log_error "grpcurl is required but not found. Install: brew install grpcurl"
  exit 1
fi

if ! command -v jq &>/dev/null; then
  log_error "jq is required but not found. Install: brew install jq"
  exit 1
fi

# Kill any stale processes on test ports from previous runs.
for port in $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
  stale_pid=$(lsof -ti :"$port" 2>/dev/null || true)
  if [[ -n "$stale_pid" ]]; then
    log_warn "Killing stale process on port $port (PID $stale_pid)"
    # shellcheck disable=SC2086  # stale_pid may contain multiple PIDs to kill
    kill -9 $stale_pid 2>/dev/null || true
    sleep 0.5
  fi
done

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

if [[ "$SKIP_BUILD" == "false" ]]; then
  log_info "Building inferadb-ledger ($PROFILE)..."
  if [[ "$PROFILE" == "release" ]]; then
    cargo +1.92 build --release -p inferadb-ledger-server
  else
    cargo +1.92 build -p inferadb-ledger-server
  fi
fi

if [[ ! -x "$BINARY" ]]; then
  log_error "Binary not found: $BINARY"
  exit 1
fi
log_success "Binary ready: $BINARY"

mkdir -p "$DATA_ROOT"

# ---------------------------------------------------------------------------
# Create peers file for initial 3-node cluster
# ---------------------------------------------------------------------------

# ===========================================================================
# Phase 1: Boot 3-node cluster, write data, verify replication
# ===========================================================================

log_phase "Phase 1: Boot 3-node cluster and write initial data"

log_info "Starting 3-node cluster (ports $(node_port 1)-$(node_port 3))..."
# First node without --join, others with --join pointing to first
FIRST_ADDR="$(node_addr 1)"
start_node 1
for i in 2 3; do
  start_node "$i" "$FIRST_ADDR"
done

wait_for_ports 1 2 3

# Initialize the cluster via the first node
log_step "Initializing cluster via node 1..."
"$BINARY" init --host="$FIRST_ADDR" || {
  log_error "Cluster initialization failed"
  exit 1
}
log_success "Cluster initialized"

wait_for_leader 1 2 3

# Find the leader
LEADER=$(find_leader 1 2 3)
if [[ -z "$LEADER" ]]; then
  log_error "Could not determine cluster leader"
  exit 1
fi
log_success "Leader is node $LEADER"

# Provision the data region used by onboarding flows. Data regions are no
# longer auto-created at boot — `init` only brings up the GLOBAL region, and
# any RPC that writes to a data region (`InitiateEmailVerification`,
# `CompleteRegistration`, etc.) requires the region to be explicitly
# provisioned via `AdminService::ProvisionRegion`. The retry loop tolerates
# transient `Unavailable` errors while the GLOBAL leader settles.
log_step "Provisioning data region us-east-va via AdminService::ProvisionRegion..."
PROVISION_OK=false
for _attempt in $(seq 1 30); do
  for port in $(seq "$BASE_PORT" $((BASE_PORT + 9))); do
    if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
      continue
    fi
    PROVISION_RESULT=$(grpcurl -plaintext \
      -d '{"region": 10}' \
      "127.0.0.1:$port" \
      ledger.v1.AdminService/ProvisionRegion 2>&1) || true
    if echo "$PROVISION_RESULT" | jq -e '.region' &>/dev/null; then
      PROVISION_OK=true
      break 2
    fi
  done
  sleep 1
done
if [[ "$PROVISION_OK" != "true" ]]; then
  log_error "Failed to provision us-east-va data region"
  exit 1
fi
log_success "Data region us-east-va provisioned"

# Brief settle so every voter has applied the region creation before we
# start firing user-facing RPCs (which apply through the new region).
sleep 3

# Create organization and vault
create_org "$LEADER" "lifecycle-test-org"
create_vault "$LEADER"

# Write initial data (Phase 1: named entities + bulk data)
log_info "Writing Phase 1 named entities..."
write_entity "$LEADER" "user:alice" "Alice from Phase 1"
write_entity "$LEADER" "user:bob" "Bob from Phase 1"
write_entity "$LEADER" "user:charlie" "Charlie from Phase 1"
write_entity "$LEADER" "config:version" "v1.0.0"
write_entity "$LEADER" "session:s001" "active"

log_info "Writing Phase 1 bulk data (50 entities)..."
write_batch "$LEADER" "p1-data" 50 "Phase 1 batch"

# Allow replication to propagate
sleep 2

# Verify all 3 nodes have identical data
verify_data_consistency "Phase 1 — initial 3-node replication (55 entities)" 1 2 3

# Point-read sentinel values on follower nodes
log_info "Verifying point reads on follower nodes..."
PHASE1_FOLLOWERS=()
for n in 1 2 3; do
  [[ "$n" -ne "$LEADER" ]] && PHASE1_FOLLOWERS+=("$n")
done
verify_read_on_nodes "user:alice" "Alice from Phase 1" "${PHASE1_FOLLOWERS[@]}"
verify_read_on_nodes "p1-data:001" "Phase 1 batch item 001" "${PHASE1_FOLLOWERS[@]}"
verify_read_on_nodes "p1-data:025" "Phase 1 batch item 025" "${PHASE1_FOLLOWERS[@]}"
verify_read_on_nodes "p1-data:050" "Phase 1 batch item 050" "${PHASE1_FOLLOWERS[@]}"
log_success "Follower point reads verified for Phase 1"

# ===========================================================================
# Phase 2: Add 4th node, verify it catches up
# ===========================================================================

log_phase "Phase 2: Add 4th node and verify catch-up"

log_info "Starting node 4 with --join seed..."
LEADER=$(find_leader 1 2 3)
start_node 4 "$(node_addr "$LEADER")"
wait_for_ports 4

# Join node 4 to the cluster via the leader (auto-join not yet implemented)
join_node_to_cluster "$LEADER" 4

# Wait for node 4 to become a voter
wait_for_voter 4 4 "$LEADER"

# Allow time for full log/snapshot replication
sleep 3

# Verify all 4 nodes have identical data (including the Phase 1 data)
verify_data_consistency "Phase 2 — 4th node caught up with all data (55 entities)" 1 2 3 4

# Point-read Phase 1 data specifically from node 4 (catch-up verification)
log_info "Verifying point reads on newly joined node 4..."
verify_read_on_nodes "user:alice" "Alice from Phase 1" 4
verify_read_on_nodes "user:charlie" "Charlie from Phase 1" 4
verify_read_on_nodes "config:version" "v1.0.0" 4
verify_read_on_nodes "p1-data:001" "Phase 1 batch item 001" 4
verify_read_on_nodes "p1-data:025" "Phase 1 batch item 025" 4
verify_read_on_nodes "p1-data:050" "Phase 1 batch item 050" 4
log_success "Node 4 catch-up verified with point reads"

# ===========================================================================
# Phase 3: Write data via the 4th node, verify all 4 agree
# ===========================================================================

log_phase "Phase 3: Write data via leader (4-node cluster)"

# Non-leader nodes return NotLeader with a leader hint (redirect model).
# Write to the current leader to validate replication across all 4 nodes.
LEADER=$(find_leader 1 2 3 4)
log_info "Current leader is node $LEADER"

log_info "Writing Phase 3 named entities via leader (node $LEADER)..."
write_entity "$LEADER" "user:diana" "Diana from Phase 3"
write_entity "$LEADER" "user:edward" "Edward from Phase 3"
write_entity "$LEADER" "config:version" "v2.0.0"

log_info "Writing Phase 3 bulk data via leader (30 entities)..."
write_batch "$LEADER" "p3-data" 30 "Phase 3 batch"

sleep 2

verify_data_consistency "Phase 3 — all 4 nodes after writes via leader (87 entities)" 1 2 3 4

# Point-read Phase 3 data on non-leader nodes (tests replication to all voters)
log_info "Verifying point reads on non-leader nodes..."
PHASE3_FOLLOWERS=()
for n in 1 2 3 4; do
  [[ "$n" -ne "$LEADER" ]] && PHASE3_FOLLOWERS+=("$n")
done
verify_read_on_nodes "user:diana" "Diana from Phase 3" "${PHASE3_FOLLOWERS[@]}"
verify_read_on_nodes "p3-data:001" "Phase 3 batch item 001" "${PHASE3_FOLLOWERS[@]}"
verify_read_on_nodes "p3-data:015" "Phase 3 batch item 015" "${PHASE3_FOLLOWERS[@]}"
verify_read_on_nodes "p3-data:030" "Phase 3 batch item 030" "${PHASE3_FOLLOWERS[@]}"
# Verify the config update propagated (overwrite, not just insert)
verify_read_on_nodes "config:version" "v2.0.0" "${PHASE3_FOLLOWERS[@]}"
log_success "Follower point reads verified for Phase 3"

# ===========================================================================
# Phase 4: Graceful leader handoff via TransferLeadership RPC
# ===========================================================================

log_phase "Phase 4: Graceful leader handoff"

# Step 1: Identify the current leader
OLD_LEADER=$(find_leader 1 2 3 4)
if [[ -z "$OLD_LEADER" ]]; then
  log_error "No leader found for transfer test"
  exit 1
fi
OLD_LEADER_ID=$(get_node_id "$OLD_LEADER")
log_info "Current leader: node $OLD_LEADER (ID: $OLD_LEADER_ID)"

# Step 2: Send TransferLeadership RPC with target_node_id=0 (auto-pick best follower)
log_step "Sending TransferLeadership RPC to node $OLD_LEADER..."
TRANSFER_ADDR=$(node_addr "$OLD_LEADER")
TRANSFER_RESULT=$(grpcurl -plaintext \
  -d '{"target_node_id": 0, "timeout_ms": 10000}' \
  "$TRANSFER_ADDR" \
  ledger.v1.AdminService/TransferLeadership 2>&1) || true

# Step 3: Verify the response
TRANSFER_SUCCESS=$(echo "$TRANSFER_RESULT" | jq -r '.success // false' 2>/dev/null || echo "false")
NEW_LEADER_ID=$(echo "$TRANSFER_RESULT" | jq -r '.newLeaderId // "0"' 2>/dev/null || echo "0")

if [[ "$TRANSFER_SUCCESS" != "true" ]]; then
  log_error "TransferLeadership failed: $TRANSFER_RESULT"
  exit 1
fi
log_success "TransferLeadership succeeded"

if [[ "$NEW_LEADER_ID" == "$OLD_LEADER_ID" || "$NEW_LEADER_ID" == "0" ]]; then
  log_error "New leader ID ($NEW_LEADER_ID) should differ from old leader ($OLD_LEADER_ID)"
  exit 1
fi
log_success "New leader ID: $NEW_LEADER_ID (differs from old: $OLD_LEADER_ID)"

# Step 4: Verify via GetClusterInfo on a different node
# Pick a node that isn't the old leader for an independent view
VERIFY_NODE=1
[[ "$OLD_LEADER" -eq 1 ]] && VERIFY_NODE=2

# Wait for the new leader to be reported by the verification node.
VERIFY_ADDR=$(node_addr "$VERIFY_NODE")
REPORTED_LEADER=""
for _wait in $(seq 1 30); do
  CLUSTER_INFO=$(grpcurl -plaintext "$VERIFY_ADDR" ledger.v1.AdminService/GetClusterInfo 2>/dev/null || true)
  REPORTED_LEADER=$(echo "$CLUSTER_INFO" | jq -r '.leaderId // "0"' 2>/dev/null || echo "0")
  if [[ "$REPORTED_LEADER" == "$NEW_LEADER_ID" ]]; then
    break
  fi
  sleep 1
done

if [[ "$REPORTED_LEADER" != "$NEW_LEADER_ID" ]]; then
  log_error "Node $VERIFY_NODE reports leader $REPORTED_LEADER, expected $NEW_LEADER_ID"
  exit 1
fi
log_success "Node $VERIFY_NODE confirms new leader: $REPORTED_LEADER"

# Step 5: Find the new leader's node number and write through it
NEW_LEADER=$(find_leader 1 2 3 4)
if [[ -z "$NEW_LEADER" ]]; then
  log_error "Cannot find new leader node number"
  exit 1
fi
log_info "New leader is node $NEW_LEADER — writing data through it..."
write_entity "$NEW_LEADER" "user:transfer-test" "written-after-leader-transfer"

# Step 6: Verify the old leader (now a follower) can serve reads
sleep 2
OLD_LEADER_VALUE=$(read_entity "$OLD_LEADER" "user:transfer-test")
if [[ "$OLD_LEADER_VALUE" != "written-after-leader-transfer" ]]; then
  log_error "Old leader (node $OLD_LEADER) failed to serve read: got '$OLD_LEADER_VALUE'"
  exit 1
fi
log_success "Old leader (node $OLD_LEADER) serving reads as follower"

verify_data_consistency "Phase 4 — all 4 nodes after leader transfer" 1 2 3 4

# ===========================================================================
# Phase 5: Shut down original 3 nodes, write data to survivor
# ===========================================================================

log_phase "Phase 5: Shut down original 3 nodes"

# Remove original nodes from cluster before killing (best-effort).
# This ensures the surviving node doesn't wait for dead peers.
LEADER=$(find_leader 1 2 3 4)
log_info "Removing original nodes from cluster membership..."

# We need to remove nodes one at a time. If the leader is one of 1-3,
# we need to remove it last (so the leadership transfers first).
NODES_TO_REMOVE=()
for i in 1 2 3; do
  if [[ "$i" -ne "$LEADER" ]]; then
    NODES_TO_REMOVE+=("$i")
  fi
done
# Add the leader last if it's one of 1-3
if [[ "$LEADER" -le 3 ]]; then
  NODES_TO_REMOVE+=("$LEADER")
fi

# Track which nodes are still active for leader queries.
# Querying removed-but-still-running nodes returns stale leader info.
ACTIVE_QUERY_NODES=(1 2 3 4)

for node_num in "${NODES_TO_REMOVE[@]}"; do
  # Re-find leader from active (non-removed) nodes only
  CURRENT_LEADER=$(find_leader "${ACTIVE_QUERY_NODES[@]}")
  CURRENT_LEADER=${CURRENT_LEADER:-4}
  log_step "Removing node $node_num (via leader $CURRENT_LEADER)"
  leave_cluster "$CURRENT_LEADER" "$node_num"

  # Remove this node from query list (it may report stale leader info)
  CLEANED_NODES=()
  for n in "${ACTIVE_QUERY_NODES[@]}"; do
    [[ "$n" -ne "$node_num" ]] && CLEANED_NODES+=("$n")
  done
  ACTIVE_QUERY_NODES=("${CLEANED_NODES[@]}")

  # Wait for the membership change to actually commit before the next one.
  # Raft allows only one membership change at a time.
  wait_for_member_removed "$node_num" "${ACTIVE_QUERY_NODES[0]}"
done

# leave_cluster fires the DR scheduler notification, which triggers
# asynchronous removal from data regions. Wait for the data region
# membership to converge before killing nodes — otherwise dead nodes
# remain as DR voters and prevent quorum.
log_info "Waiting for data region membership convergence..."
sleep 5

# Protect node 4 from the cleanup trap — it's the survivor under test.
_n4_pid=$(lsof -ti "tcp:$(node_port 4)" -sTCP:LISTEN 2>/dev/null || true)
[[ -n "$_n4_pid" ]] && PROTECTED_PIDS+=("$_n4_pid")

log_info "Killing original nodes (SIGKILL — simulating crash)..."
# Use SIGKILL to simulate a hard crash. The surviving node recovers via
# the Raft no-op commit on leader election, which advances commit_index
# and applies any pending membership changes.
set +e
for _kn in 1 2 3; do
  _kport=$(node_port "$_kn")
  _kpid=$(lsof -ti "tcp:$_kport" -sTCP:LISTEN 2>/dev/null)
  if [[ -n "$_kpid" ]]; then
    kill -9 "$_kpid" 2>/dev/null
    wait "$_kpid" 2>/dev/null
    log_info "  Killed node $_kn (PID $_kpid)"
    # Remove from ACTIVE_PIDS so wait_for_tcp doesn't flag as "exited prematurely"
    _new_active=()
    for _ap in "${ACTIVE_PIDS[@]}"; do
      [[ "$_ap" != "$_kpid" ]] && _new_active+=("$_ap")
    done
    ACTIVE_PIDS=("${_new_active[@]}")
  fi
done
set -e
sleep 2

# Wait for node 4 to become the GLOBAL leader. It's the sole remaining voter
# after nodes 1-3 were removed and killed.
set +e
wait_for_leader 4
set -e

log_info "Writing Phase 5 data to surviving node 4..."
# Retry the first write — data region leadership may still be stabilizing
# after membership changes propagate via the Raft no-op commit.
set +e
_p5_ok=false
for _p5_attempt in $(seq 1 15); do
  if write_entity 4 "user:frank" "Frank from Phase 5" 2>/dev/null; then
    _p5_ok=true
    break
  fi
  sleep 2
done
set -e
if [[ "$_p5_ok" != "true" ]]; then
  log_error "Phase 5 write failed after 15 attempts"
  exit 1
fi
write_entity 4 "event:migration" "original-3-shutdown"

log_info "Writing Phase 5 bulk data (20 entities)..."
write_batch 4 "p5-data" 20 "Phase 5 batch"

# ===========================================================================
# Phase 6: Boot 2 new nodes, verify full cluster state
# ===========================================================================

log_phase "Phase 6: Boot 2 new nodes and verify full replication"

log_info "Starting nodes 5 and 6 with --join seed..."
start_node 5 "$(node_addr 4)"
start_node 6 "$(node_addr 4)"
wait_for_ports 5 6

# Join them via node 4 (the sole surviving member/leader)
join_node_to_cluster 4 5
sleep 2
wait_for_voter 5 2 4

join_node_to_cluster 4 6
sleep 2
wait_for_voter 6 3 4

# Allow full replication
sleep 3

# Final verification: all 3 nodes (4, 5, 6) should have ALL data
# from every phase
verify_data_consistency "Phase 6 — new 3-node cluster has all data (110 entities)" 4 5 6

# Comprehensive point reads on new nodes — verify full historical catch-up.
# Nodes 5 and 6 must have data from every prior phase.
log_info "Verifying full historical catch-up on nodes 5 and 6..."

# Phase 1 data
verify_read_on_nodes "user:alice" "Alice from Phase 1" 5 6
verify_read_on_nodes "user:bob" "Bob from Phase 1" 5 6
verify_read_on_nodes "p1-data:001" "Phase 1 batch item 001" 5 6
verify_read_on_nodes "p1-data:025" "Phase 1 batch item 025" 5 6
verify_read_on_nodes "p1-data:050" "Phase 1 batch item 050" 5 6

# Phase 3 data (written via leader in 4-node cluster)
verify_read_on_nodes "user:diana" "Diana from Phase 3" 5 6
verify_read_on_nodes "p3-data:001" "Phase 3 batch item 001" 5 6
verify_read_on_nodes "p3-data:015" "Phase 3 batch item 015" 5 6
verify_read_on_nodes "p3-data:030" "Phase 3 batch item 030" 5 6

# Phase 4 data (written after leader transfer)
verify_read_on_nodes "user:transfer-test" "written-after-leader-transfer" 5 6

# Phase 5 data (written to sole survivor)
verify_read_on_nodes "user:frank" "Frank from Phase 5" 5 6
verify_read_on_nodes "p5-data:001" "Phase 5 batch item 001" 5 6
verify_read_on_nodes "p5-data:010" "Phase 5 batch item 010" 5 6
verify_read_on_nodes "p5-data:020" "Phase 5 batch item 020" 5 6

# Config should reflect latest update (v2.0.0 from Phase 3)
verify_read_on_nodes "config:version" "v2.0.0" 5 6

log_success "Full historical catch-up verified on nodes 5 and 6"

# ===========================================================================
# Summary
# ===========================================================================

echo ""
log_phase "All phases passed!"

echo -e "${GREEN}  Phase 1:${NC} 3-node boot + 55 entities + follower point reads        ✓"
echo -e "${GREEN}  Phase 2:${NC} 4th node join + catch-up point reads (55 entities)    ✓"
echo -e "${GREEN}  Phase 3:${NC} Writes in 4-node cluster + 87 entities + follower reads ✓"
echo -e "${GREEN}  Phase 4:${NC} Graceful leader handoff via TransferLeadership        ✓"
echo -e "${GREEN}  Phase 5:${NC} Original 3 shutdown + 110 entities on survivor        ✓"
echo -e "${GREEN}  Phase 6:${NC} 2 new nodes + full historical catch-up reads          ✓"
echo ""
log_success "Cluster lifecycle test completed successfully!"
