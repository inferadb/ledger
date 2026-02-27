#!/usr/bin/env bash
# Cluster lifecycle integration test.
#
# Validates the full spectrum of distributed cluster behaviors:
#
#   Phase 1: Boot a 3-node cluster, write data, verify replication
#   Phase 2: Add a 4th node (--join), verify it catches up with all data
#   Phase 3: Write data via the 4th node, verify all 4 nodes agree
#   Phase 4: Graceful leader handoff via TransferLeadership RPC
#   Phase 5: Shut down the original 3 nodes, write data to the survivor
#   Phase 6: Boot 2 new nodes, verify the new 3-node cluster has all data
#
# This exercises: coordinated bootstrap, Raft log replication, dynamic
# membership changes, snapshot transfer, leader failover, graceful leader
# transfer, and catch-up.
#
# Usage:
#   ./scripts/cluster-lifecycle-test.sh                  # Full test
#   ./scripts/cluster-lifecycle-test.sh --release        # Release build
#   ./scripts/cluster-lifecycle-test.sh --skip-build     # Skip cargo build
#   ./scripts/cluster-lifecycle-test.sh --help           # Show help

set -euo pipefail
cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_PORT=50071
DATA_ROOT="/tmp/ledger-lifecycle-test-$$"
PROFILE="debug"
BINARY="target/debug/inferadb-ledger"
SKIP_BUILD=false

# Timeouts
HEALTH_TIMEOUT=60
SETTLE_TIME=5
SYNC_TIMEOUT=30
JOIN_TIMEOUT=60

# Tracking
ALL_PIDS=()
ACTIVE_PIDS=()
NEXT_NODE_NUM=1

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
    --release)
      PROFILE="release"
      BINARY="target/release/inferadb-ledger"
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
      echo "  --release      Build in release mode"
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

cleanup() {
  local exit_code=$?
  echo ""
  log_info "Cleaning up..."

  for pid in "${ALL_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done

  for pid in "${ALL_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done

  if [[ -d "$DATA_ROOT" ]]; then
    rm -rf "$DATA_ROOT"
    log_info "Removed data directory: $DATA_ROOT"
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

# Start a node with coordinated bootstrap (--cluster N).
# Args: node_number cluster_size
start_node_cluster() {
  local node_num=$1
  local cluster_size=$2
  local port
  port=$(node_port "$node_num")
  local node_data="$DATA_ROOT/node$node_num"
  mkdir -p "$node_data"

  RUST_LOG=info "$BINARY" \
    --listen "127.0.0.1:$port" \
    --data "$node_data" \
    --cluster "$cluster_size" \
    --peers "$PEERS_FILE" \
    --peers-timeout 30 \
    --log-format text \
    > "$DATA_ROOT/node$node_num.log" 2>&1 &

  local pid=$!
  ALL_PIDS+=("$pid")
  ACTIVE_PIDS+=("$pid")
  log_info "  Node $node_num: PID $pid, port $port (cluster mode)"
}

# Start a node in join mode (--join).
# Args: node_number
start_node_join() {
  local node_num=$1
  local port
  port=$(node_port "$node_num")
  local node_data="$DATA_ROOT/node$node_num"
  mkdir -p "$node_data"

  RUST_LOG=info "$BINARY" \
    --listen "127.0.0.1:$port" \
    --data "$node_data" \
    --join \
    --log-format text \
    > "$DATA_ROOT/node$node_num.log" 2>&1 &

  local pid=$!
  ALL_PIDS+=("$pid")
  ACTIVE_PIDS+=("$pid")
  log_info "  Node $node_num: PID $pid, port $port (join mode)"
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
  local max_attempts=10
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

    # Retryable: prior membership change still committing
    if echo "$result" | grep -q "already undergoing a configuration change"; then
      attempts=$((attempts + 1))
      log_info "    Membership change in progress, retrying ($attempts/$max_attempts)..."
      sleep 2
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
  local elapsed=0
  local timeout=15

  local removed_id
  removed_id=$(get_node_id "$removed_num")

  while [[ $elapsed -lt $timeout ]]; do
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

    sleep 1
    elapsed=$((elapsed + 1))
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

  # Fallback: kill by port
  local pid
  pid=$(lsof -ti "tcp:$port" 2>/dev/null || true)
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    log_info "  Killed node $node_num (PID $pid, found by port)"
  fi
}

# ---------------------------------------------------------------------------
# Data operation functions
# ---------------------------------------------------------------------------

IDEM_COUNTER=1

# Create an organization. Sets ORG_SLUG.
# Args: leader_node_number org_name
create_org() {
  local node_num=$1
  local name=$2
  local addr
  addr=$(node_addr "$node_num")

  local result
  result=$(grpcurl -plaintext \
    -d "{\"name\": \"$name\"}" \
    "$addr" \
    ledger.v1.AdminService/CreateOrganization 2>&1) || true

  ORG_SLUG=$(echo "$result" | jq -r '.slug.slug // empty' 2>/dev/null)
  if [[ -z "$ORG_SLUG" ]]; then
    log_error "CreateOrganization failed: $result"
    return 1
  fi
  log_step "Created organization '$name' (slug: $ORG_SLUG)"
}

# Create a vault. Sets VAULT_SLUG.
# Args: leader_node_number
create_vault() {
  local node_num=$1
  local addr
  addr=$(node_addr "$node_num")

  local result
  result=$(grpcurl -plaintext \
    -d "{\"organization\": {\"slug\": \"$ORG_SLUG\"}}" \
    "$addr" \
    ledger.v1.AdminService/CreateVault 2>&1) || true

  VAULT_SLUG=$(echo "$result" | jq -r '.vault.slug // empty' 2>/dev/null)
  if [[ -z "$VAULT_SLUG" ]]; then
    log_error "CreateVault failed: $result"
    return 1
  fi
  log_step "Created vault (slug: $VAULT_SLUG)"
}

# Write an entity.
# Args: node_number key value_string
write_entity() {
  local node_num=$1
  local key=$2
  local value=$3
  local addr
  addr=$(node_addr "$node_num")

  local idem_key
  idem_key=$(generate_idempotency_key $IDEM_COUNTER)
  IDEM_COUNTER=$((IDEM_COUNTER + 1))

  # base64-encode the value for protobuf bytes field
  local value_b64
  value_b64=$(echo -n "$value" | base64)

  local result
  result=$(grpcurl -plaintext \
    -d "{
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
    }" \
    "$addr" \
    ledger.v1.WriteService/Write 2>&1) || true

  local block_height
  block_height=$(echo "$result" | jq -r '.success.blockHeight // empty' 2>/dev/null)
  if [[ -z "$block_height" ]]; then
    log_error "Write failed for key '$key': $result"
    return 1
  fi
  log_step "Wrote '$key' = '$value' (block $block_height) via node $node_num"
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
      \"organization\": {\"slug\": \"$ORG_SLUG\"},
      \"vault\": {\"slug\": \"$VAULT_SLUG\"},
      \"key\": \"$key\",
      \"consistency\": \"READ_CONSISTENCY_EVENTUAL\"
    }" \
    "$addr" \
    ledger.v1.ReadService/Read 2>&1) || true

  # Value is base64-encoded bytes — decode it
  local value_b64
  value_b64=$(echo "$result" | jq -r '.value // empty' 2>/dev/null)
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
      \"organization\": {\"slug\": \"$ORG_SLUG\"},
      \"vault\": {\"slug\": \"$VAULT_SLUG\"},
      \"keyPrefix\": \"\",
      \"limit\": 100,
      \"consistency\": \"READ_CONSISTENCY_EVENTUAL\"
    }"

    if [[ -n "$page_token" ]]; then
      request="{
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

    page_token=$(echo "$result" | jq -r '.nextPageToken // empty' 2>/dev/null)
    if [[ -z "$page_token" ]]; then
      break
    fi
  done

  echo -n "$all_entities" | sort
}

# Get the tip (block height + state root) from a node.
# Args: node_number
get_tip() {
  local node_num=$1
  local addr
  addr=$(node_addr "$node_num")

  local result
  result=$(grpcurl -plaintext \
    -d "{
      \"organization\": {\"slug\": \"$ORG_SLUG\"},
      \"vault\": {\"slug\": \"$VAULT_SLUG\"}
    }" \
    "$addr" \
    ledger.v1.ReadService/GetTip 2>/dev/null) || true
  echo "$result"
}

# ---------------------------------------------------------------------------
# Verification functions
# ---------------------------------------------------------------------------

# Wait for replication to converge across nodes, then verify data consistency.
# Args: description node_numbers...
verify_data_consistency() {
  local description=$1
  shift
  local nodes=("$@")

  log_info "Verifying: $description"

  # Wait for replication to converge (same block height on all nodes)
  local elapsed=0
  while [[ $elapsed -lt $SYNC_TIMEOUT ]]; do
    local heights=()
    local all_same=true
    local reference_height=""

    for node_num in "${nodes[@]}"; do
      local tip
      tip=$(get_tip "$node_num" 2>/dev/null || true)
      local height
      height=$(echo "$tip" | jq -r '.height // "0"' 2>/dev/null || echo "0")
      heights+=("$height")

      if [[ -z "$reference_height" ]]; then
        reference_height="$height"
      elif [[ "$height" != "$reference_height" ]]; then
        all_same=false
      fi
    done

    if [[ "$all_same" == "true" && "$reference_height" != "0" ]]; then
      log_step "Block heights converged: ${heights[*]}"
      break
    fi

    sleep 1
    elapsed=$((elapsed + 1))
  done

  if [[ $elapsed -ge $SYNC_TIMEOUT ]]; then
    log_error "Replication did not converge within ${SYNC_TIMEOUT}s"
    for node_num in "${nodes[@]}"; do
      local tip
      tip=$(get_tip "$node_num" 2>/dev/null || true)
      log_error "  Node $node_num tip: $tip"
    done
    return 1
  fi

  # Verify block heights match exactly
  local reference_tip
  reference_tip=$(get_tip "${nodes[0]}")
  local ref_height
  ref_height=$(echo "$reference_tip" | jq -r '.height')

  for node_num in "${nodes[@]:1}"; do
    local tip
    tip=$(get_tip "$node_num")
    local height
    height=$(echo "$tip" | jq -r '.height')

    if [[ "$height" != "$ref_height" ]]; then
      log_error "Block height mismatch: node ${nodes[0]}=$ref_height, node $node_num=$height"
      return 1
    fi
  done
  log_step "Block heights match: $ref_height"

  # Verify entity data matches across all nodes
  local reference_entities
  reference_entities=$(list_entities "${nodes[0]}")

  for node_num in "${nodes[@]:1}"; do
    local node_entities
    node_entities=$(list_entities "$node_num")

    if [[ "$node_entities" != "$reference_entities" ]]; then
      log_error "Entity data mismatch between node ${nodes[0]} and node $node_num!"
      log_error "--- Node ${nodes[0]} entities ---"
      echo "$reference_entities"
      log_error "--- Node $node_num entities ---"
      echo "$node_entities"
      return 1
    fi
  done

  local entity_count
  entity_count=$(echo -n "$reference_entities" | grep -c '.' || echo "0")
  log_success "All ${#nodes[@]} nodes have identical data ($entity_count entities)"
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

PEERS_FILE="$DATA_ROOT/peers.json"
CACHED_AT=$(date +%s)
PEERS_ARRAY=""
for i in 1 2 3; do
  PORT=$(node_port "$i")
  [[ $i -gt 1 ]] && PEERS_ARRAY+=","
  PEERS_ARRAY+="{\"addr\":\"127.0.0.1:$PORT\"}"
done
cat > "$PEERS_FILE" <<EOF
{"cached_at": $CACHED_AT, "peers": [$PEERS_ARRAY]}
EOF

# ===========================================================================
# Phase 1: Boot 3-node cluster, write data, verify replication
# ===========================================================================

log_phase "Phase 1: Boot 3-node cluster and write initial data"

log_info "Starting 3-node cluster (ports $(node_port 1)-$(node_port 3))..."
for i in 1 2 3; do
  start_node_cluster "$i" 3
done
NEXT_NODE_NUM=4

wait_for_ports 1 2 3
wait_for_leader 1 2 3

# Find the leader
LEADER=$(find_leader 1 2 3)
if [[ -z "$LEADER" ]]; then
  log_error "Could not determine cluster leader"
  exit 1
fi
log_success "Leader is node $LEADER"

# Create organization and vault
create_org "$LEADER" "lifecycle-test-org"
create_vault "$LEADER"

# Write initial batch of data (Phase 1 data)
log_info "Writing Phase 1 data..."
write_entity "$LEADER" "user:alice" "Alice from Phase 1"
write_entity "$LEADER" "user:bob" "Bob from Phase 1"
write_entity "$LEADER" "user:charlie" "Charlie from Phase 1"
write_entity "$LEADER" "config:version" "v1.0.0"
write_entity "$LEADER" "session:s001" "active"

# Allow replication to propagate
sleep 2

# Verify all 3 nodes have identical data
verify_data_consistency "Phase 1 — initial 3-node replication" 1 2 3

# ===========================================================================
# Phase 2: Add 4th node, verify it catches up
# ===========================================================================

log_phase "Phase 2: Add 4th node and verify catch-up"

log_info "Starting node 4 in join mode..."
start_node_join 4
wait_for_ports 4

# Join node 4 to the cluster via the leader
LEADER=$(find_leader 1 2 3)
join_node_to_cluster "$LEADER" 4

# Wait for node 4 to become a voter
wait_for_voter 4 4 "$LEADER"

# Allow time for full log/snapshot replication
sleep 3

# Verify all 4 nodes have identical data (including the Phase 1 data)
verify_data_consistency "Phase 2 — 4th node caught up with all data" 1 2 3 4

# ===========================================================================
# Phase 3: Write data via the 4th node, verify all 4 agree
# ===========================================================================

log_phase "Phase 3: Write data via 4th node"

# The 4th node might not be leader, but writes should be forwarded.
# To be safe, find the current leader (could have changed).
LEADER=$(find_leader 1 2 3 4)
log_info "Current leader is node $LEADER"

log_info "Writing Phase 3 data via node 4..."
write_entity 4 "user:diana" "Diana from Phase 3"
write_entity 4 "user:edward" "Edward from Phase 3"
write_entity 4 "config:version" "v2.0.0"

sleep 2

verify_data_consistency "Phase 3 — all 4 nodes after writes via node 4" 1 2 3 4

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

# Wait briefly for the new leader to be reported by all nodes
sleep 2

VERIFY_ADDR=$(node_addr "$VERIFY_NODE")
CLUSTER_INFO=$(grpcurl -plaintext "$VERIFY_ADDR" ledger.v1.AdminService/GetClusterInfo 2>/dev/null || true)
REPORTED_LEADER=$(echo "$CLUSTER_INFO" | jq -r '.leaderId // "0"' 2>/dev/null || echo "0")

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

log_info "Killing original nodes..."
for i in 1 2 3; do
  kill_node "$i"
done

# Wait for node 4 to elect itself leader (it's the sole remaining voter).
# This replaces a blind sleep — the election timeout is non-deterministic.
wait_for_leader 4

log_info "Writing Phase 5 data to surviving node 4..."
write_entity 4 "user:frank" "Frank from Phase 5"
write_entity 4 "event:migration" "original-3-shutdown"

# ===========================================================================
# Phase 6: Boot 2 new nodes, verify full cluster state
# ===========================================================================

log_phase "Phase 6: Boot 2 new nodes and verify full replication"

log_info "Starting nodes 5 and 6 in join mode..."
start_node_join 5
start_node_join 6
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
verify_data_consistency "Phase 6 — new 3-node cluster has all data" 4 5 6

# ===========================================================================
# Summary
# ===========================================================================

echo ""
log_phase "All phases passed!"

echo -e "${GREEN}  Phase 1:${NC} 3-node cluster boot + initial writes + replication   ✓"
echo -e "${GREEN}  Phase 2:${NC} 4th node join + full catch-up verification            ✓"
echo -e "${GREEN}  Phase 3:${NC} Write via 4th node + 4-node consistency               ✓"
echo -e "${GREEN}  Phase 4:${NC} Graceful leader handoff via TransferLeadership        ✓"
echo -e "${GREEN}  Phase 5:${NC} Original 3 nodes shutdown + survivor writes            ✓"
echo -e "${GREEN}  Phase 6:${NC} 2 new nodes join + full data replication               ✓"
echo ""
log_success "Cluster lifecycle test completed successfully!"
