#!/usr/bin/env bash
# Shared cluster bootstrap helpers.
#
# Source this file from scripts that need to spawn a local cluster, wait for
# readiness, then hand off to `cargo test` (or similar). The caller configures
# cluster parameters, sources this file, and calls `bootstrap_cluster`.
#
# Exports after bootstrap_cluster returns:
#   CLUSTER_PIDS          — array of node PIDs
#   CLUSTER_ENDPOINTS     — comma-separated "http://127.0.0.1:PORT,..." list
#   LEDGER_ENDPOINTS      — same (for test binaries that read this env var)
#   LEDGER_NODE1..N       — per-node endpoints
#
# Installs an EXIT trap that kills nodes and removes the data directory.

# shellcheck shell=bash

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

# Colors are suppressed when stdout isn't a TTY so CI logs stay clean.
if [[ -t 1 ]]; then
  _C_RED='\033[0;31m'
  _C_GREEN='\033[0;32m'
  _C_YELLOW='\033[1;33m'
  _C_BLUE='\033[0;34m'
  _C_NC='\033[0m'
else
  _C_RED=''; _C_GREEN=''; _C_YELLOW=''; _C_BLUE=''; _C_NC=''
fi

log_info()    { echo -e "${_C_BLUE}[INFO]${_C_NC}  $*"; }
log_success() { echo -e "${_C_GREEN}[OK]${_C_NC}    $*"; }
log_warn()    { echo -e "${_C_YELLOW}[WARN]${_C_NC}  $*"; }
log_error()   { echo -e "${_C_RED}[ERROR]${_C_NC} $*"; }

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

# Shared state used by bootstrap_cluster + cleanup_cluster.
CLUSTER_PIDS=()
_CLUSTER_DATA_ROOT=""

cleanup_cluster() {
  local exit_code=$?

  if [[ ${#CLUSTER_PIDS[@]} -gt 0 ]]; then
    for pid in "${CLUSTER_PIDS[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
      fi
    done
    for pid in "${CLUSTER_PIDS[@]}"; do
      wait "$pid" 2>/dev/null || true
    done
  fi

  if [[ -n "$_CLUSTER_DATA_ROOT" && -d "$_CLUSTER_DATA_ROOT" ]]; then
    if [[ -n "${KEEP_LOGS:-}" ]]; then
      log_warn "KEEP_LOGS set; preserving $_CLUSTER_DATA_ROOT"
    else
      rm -rf "$_CLUSTER_DATA_ROOT"
    fi
  fi

  return "$exit_code"
}

# ---------------------------------------------------------------------------
# Binary build
# ---------------------------------------------------------------------------

# Usage: build_ledger_binary <profile>
#   profile: "debug" or "release"
# Sets LEDGER_BINARY to the absolute path of the built binary.
build_ledger_binary() {
  local profile=$1
  case "$profile" in
    debug)
      log_info "Building inferadb-ledger (debug)..."
      cargo +1.92 build -p inferadb-ledger-server
      LEDGER_BINARY="$PWD/target/debug/inferadb-ledger"
      ;;
    release)
      log_info "Building inferadb-ledger (release)..."
      cargo +1.92 build --release -p inferadb-ledger-server
      LEDGER_BINARY="$PWD/target/release/inferadb-ledger"
      ;;
    *)
      log_error "Unknown profile: $profile (expected debug|release)"
      return 1
      ;;
  esac

  if [[ ! -x "$LEDGER_BINARY" ]]; then
    log_error "Binary not found: $LEDGER_BINARY"
    return 1
  fi
  log_success "Built: $LEDGER_BINARY"
}

# ---------------------------------------------------------------------------
# Cluster bootstrap
# ---------------------------------------------------------------------------

# Kill any leftover processes bound to the listen ports in our range.
# Args: base_port node_count
kill_stale_listeners() {
  local base_port=$1
  local node_count=$2
  local i port stale_pids
  for ((i=1; i<=node_count; i++)); do
    port=$((base_port + i - 1))
    stale_pids=$(lsof -ti "tcp:$port" -sTCP:LISTEN 2>/dev/null || true)
    if [[ -n "$stale_pids" ]]; then
      log_warn "Killing stale listener on port $port (PIDs: $stale_pids)"
      # shellcheck disable=SC2086  # intentional word-splitting of PID list
      kill -9 $stale_pids 2>/dev/null || true
      sleep 0.5
    fi
  done
}

# Bootstrap a cluster.
# Args:
#   1: base_port
#   2: node_count
#   3: data_root          (will be created and removed on exit)
#   4: settle_time_secs   (pause after init before returning)
#   5: health_timeout_secs (default 60)
bootstrap_cluster() {
  local base_port=$1
  local node_count=$2
  local data_root=$3
  local settle_time=$4
  local health_timeout=${5:-60}

  _CLUSTER_DATA_ROOT="$data_root"
  mkdir -p "$data_root"

  kill_stale_listeners "$base_port" "$node_count"

  local blinding_key="deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
  local first_port=$base_port
  local first_addr="127.0.0.1:$first_port"

  log_info "Starting $node_count-node cluster (ports $base_port-$((base_port + node_count - 1)))..."

  local i port node_data
  for ((i=1; i<=node_count; i++)); do
    port=$((base_port + i - 1))
    node_data="$data_root/node$i"
    mkdir -p "$node_data"

    if [[ $i -eq 1 ]]; then
      RUST_LOG=info "$LEDGER_BINARY" \
        --listen "127.0.0.1:$port" \
        --data "$node_data" \
        --enable-grpc-reflection \
        --email-blinding-key "$blinding_key" \
        --log-format text \
        > "$data_root/node$i.log" 2>&1 &
    else
      RUST_LOG=info "$LEDGER_BINARY" \
        --listen "127.0.0.1:$port" \
        --data "$node_data" \
        --join "$first_addr" \
        --enable-grpc-reflection \
        --email-blinding-key "$blinding_key" \
        --log-format text \
        > "$data_root/node$i.log" 2>&1 &
    fi

    CLUSTER_PIDS+=("$!")
    log_info "  Node $i: PID $! port $port"
  done

  log_info "Waiting for cluster readiness (timeout: ${health_timeout}s)..."
  local elapsed=0
  local all_listening
  while [[ $elapsed -lt $health_timeout ]]; do
    all_listening=true
    for ((i=1; i<=node_count; i++)); do
      port=$((base_port + i - 1))
      if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
        all_listening=false
        break
      fi
    done

    if [[ "$all_listening" == "true" ]]; then
      log_success "All $node_count nodes listening"
      log_info "Initializing cluster via $first_addr..."
      if ! "$LEDGER_BINARY" init --host="$first_addr"; then
        log_error "Cluster initialization failed"
        dump_node_logs "$data_root" "$node_count" 20
        return 1
      fi
      log_success "Cluster initialized"
      log_info "Settling for ${settle_time}s..."
      sleep "$settle_time"
      break
    fi

    # Check for early crashes
    local idx
    for idx in "${!CLUSTER_PIDS[@]}"; do
      if ! kill -0 "${CLUSTER_PIDS[$idx]}" 2>/dev/null; then
        log_error "Node $((idx + 1)) (PID ${CLUSTER_PIDS[$idx]}) exited prematurely"
        dump_node_logs "$data_root" "$node_count" 20
        return 1
      fi
    done

    sleep 1
    elapsed=$((elapsed + 1))
  done

  if [[ $elapsed -ge $health_timeout ]]; then
    log_error "Cluster did not become ready within ${health_timeout}s"
    dump_node_logs "$data_root" "$node_count" 20
    return 1
  fi

  # Export endpoints
  CLUSTER_ENDPOINTS=""
  for ((i=1; i<=node_count; i++)); do
    port=$((base_port + i - 1))
    [[ $i -gt 1 ]] && CLUSTER_ENDPOINTS+=","
    CLUSTER_ENDPOINTS+="http://127.0.0.1:$port"
    export "LEDGER_NODE$i=http://127.0.0.1:$port"
  done
  export LEDGER_ENDPOINTS="$CLUSTER_ENDPOINTS"

  log_info "Cluster endpoints: $CLUSTER_ENDPOINTS"
}

# Provision a data region via `AdminService::ProvisionRegion`. Data regions
# are no longer auto-created at boot — `init` only brings up the GLOBAL
# region. Any RPC that writes to a data region (`InitiateEmailVerification`,
# `CompleteRegistration`, `Write`, etc.) requires the region to be
# explicitly provisioned first.
#
# Args:
#   1: region_enum_value (e.g. 10 for REGION_US_EAST_VA)
#   2: max_attempts      (default 30)
#
# Tries each listening cluster port on each attempt — `ProvisionRegion` is
# idempotent (`created = false` on a no-op) and can be served by any node.
provision_region() {
  local region=$1
  local max_attempts=${2:-30}
  if ! command -v grpcurl &>/dev/null; then
    log_error "grpcurl is required for provision_region (install: brew install grpcurl)"
    return 1
  fi
  if ! command -v jq &>/dev/null; then
    log_error "jq is required for provision_region (install: brew install jq)"
    return 1
  fi

  local node_count=${#CLUSTER_PIDS[@]}
  local base_port
  # Extract the FIRST endpoint's port from the exported $CLUSTER_ENDPOINTS
  # (e.g. "http://127.0.0.1:50051,http://127.0.0.1:50052,..."). The previous
  # `sed -n 's@.*://[^:]*:\([0-9]*\).*@\1@p'` regex was greedy on `.*://` and
  # silently captured the LAST endpoint's port — fine on a single-endpoint
  # string but broken when called after `bootstrap_cluster` exports a
  # comma-separated list, leaving `provision_region` looping over ports
  # past the cluster's range. Split on comma first, then on the last `:`.
  base_port=$(echo "$CLUSTER_ENDPOINTS" | cut -d, -f1 | sed 's@.*:@@')
  [[ -z "$base_port" ]] && { log_error "provision_region: cannot derive base port"; return 1; }

  local attempt
  local last_result=""
  for attempt in $(seq 1 "$max_attempts"); do
    local i
    for ((i=0; i<node_count; i++)); do
      local addr="127.0.0.1:$((base_port + i))"
      local result
      result=$(grpcurl -plaintext \
        -d "{\"region\": $region}" \
        "$addr" \
        ledger.v1.AdminService/ProvisionRegion 2>&1) || true
      last_result="$result"
      if echo "$result" | jq -e '.region' &>/dev/null; then
        log_success "Data region $region provisioned (attempt $attempt)"
        return 0
      fi
    done
    sleep 1
  done

  log_error "provision_region: failed after $max_attempts attempts (region=$region)"
  log_error "Last response: $last_result"
  return 1
}

# Tail the last N lines of each node's log.
# Args: data_root node_count lines
dump_node_logs() {
  local data_root=$1
  local node_count=$2
  local lines=$3
  local i
  for ((i=1; i<=node_count; i++)); do
    log_error "--- Node $i log (last $lines lines) ---"
    tail -"$lines" "$data_root/node$i.log" 2>/dev/null || true
  done
}
