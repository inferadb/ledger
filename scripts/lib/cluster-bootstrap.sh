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

# Populated by bootstrap_cluster; read by callers (and any `inferadb-ledger
# admin ...` invocation) to dial the cluster's wire (QUIC + TLS) listener.
LEDGER_TLS_CERT=""
LEDGER_TLS_KEY=""
LEDGER_TLS_SERVER_NAME="localhost"

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
  # Shell-driven smoke tests dial the cluster via `admin <subcommand>`,
  # which sends an empty `auth_payload`. The production `JwtAuthVerifier`
  # rejects that — so test builds enable `permissive-wire-auth`, which
  # swaps in the test stub `PermissiveVerifier`. Production release
  # builds must NOT use this feature.
  case "$profile" in
    debug)
      log_info "Building inferadb-ledger (debug + permissive-wire-auth)..."
      cargo +1.92 build -p inferadb-ledger-server --features permissive-wire-auth
      LEDGER_BINARY="$PWD/target/debug/inferadb-ledger"
      ;;
    release)
      log_info "Building inferadb-ledger (release + permissive-wire-auth)..."
      cargo +1.92 build --release -p inferadb-ledger-server --features permissive-wire-auth
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

# Generate a self-signed PEM cert + key pair under `$1/tls/` and export
# `LEDGER_TLS_CERT` / `LEDGER_TLS_KEY` / `LEDGER_TLS_SERVER_NAME`. The wire
# transport (QUIC) requires `--tls-cert` / `--tls-key`, and every
# `inferadb-ledger admin ...` invocation that dials the cluster needs the
# same cert as its trust anchor. SANs cover `localhost`, `127.0.0.1`, and
# `::1` so loopback dials by IP or name both verify.
#
# Uses the system `openssl` (BoringSSL/LibreSSL/OpenSSL — all support the
# `-subj` + `-addext` flags emitted here). Self-signed material is
# acceptable for local-cluster scripts; production deployments supply
# operator certs.
#
# Args:
#   1: tls_root  (e.g. "$data_root")
generate_tls_material() {
  local tls_root=$1
  local tls_dir="$tls_root/tls"
  mkdir -p "$tls_dir"

  if ! command -v openssl &>/dev/null; then
    log_error "openssl is required to generate test TLS material (install: brew install openssl)"
    return 1
  fi

  LEDGER_TLS_CERT="$tls_dir/cert.pem"
  LEDGER_TLS_KEY="$tls_dir/key.pem"

  # The wire transport (rustls / rustls-webpki) rejects certs with
  # `BasicConstraints: CA:TRUE` when used as the end-entity (it surfaces as
  # `error 46: invalid peer certificate: CaUsedAsEndEntity` during the QUIC
  # handshake). `openssl req -x509` defaults to a CA-marked self-signed cert,
  # so override `basicConstraints` and add explicit `extendedKeyUsage` to
  # keep the same material usable for both server-side bind and as the
  # client-side trust anchor under `--tls-cert`.
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "$LEDGER_TLS_KEY" \
    -out "$LEDGER_TLS_CERT" -days 30 \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1" \
    -addext "basicConstraints=CA:FALSE" \
    -addext "extendedKeyUsage=serverAuth,clientAuth" \
    >/dev/null 2>&1 || {
      log_error "openssl failed to generate self-signed cert in $tls_dir"
      return 1
    }

  chmod 600 "$LEDGER_TLS_KEY"
  export LEDGER_TLS_CERT LEDGER_TLS_KEY LEDGER_TLS_SERVER_NAME
  log_info "Generated test TLS material at $tls_dir"
}

# Returns 0 when something is bound to the supplied UDP port, 1 otherwise.
# The wire transport is QUIC, so the readiness signal is a UDP listener — the
# previous `nc -z` (TCP) probe never succeeds against the in-house wire stack.
# Args: port
port_is_listening() {
  local port=$1
  [[ -n "$(lsof -ti "udp:$port" 2>/dev/null || true)" ]]
}

# Kill any leftover processes bound to the listen ports in our range.
# Args: base_port node_count
kill_stale_listeners() {
  local base_port=$1
  local node_count=$2
  local i port stale_pids
  for ((i=1; i<=node_count; i++)); do
    port=$((base_port + i - 1))
    stale_pids=$(lsof -ti "udp:$port" 2>/dev/null || true)
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

  generate_tls_material "$data_root" || return 1

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
        --tls-cert "$LEDGER_TLS_CERT" \
        --tls-key "$LEDGER_TLS_KEY" \
        --tls-server-name "$LEDGER_TLS_SERVER_NAME" \
        --email-blinding-key "$blinding_key" \
        --log-format text \
        > "$data_root/node$i.log" 2>&1 &
    else
      RUST_LOG=info "$LEDGER_BINARY" \
        --listen "127.0.0.1:$port" \
        --data "$node_data" \
        --join "$first_addr" \
        --tls-cert "$LEDGER_TLS_CERT" \
        --tls-key "$LEDGER_TLS_KEY" \
        --tls-server-name "$LEDGER_TLS_SERVER_NAME" \
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
      if ! port_is_listening "$port"; then
        all_listening=false
        break
      fi
    done

    if [[ "$all_listening" == "true" ]]; then
      log_success "All $node_count nodes listening"
      log_info "Initializing cluster via $first_addr..."
      # TLS flags are top-level (before the `init` subcommand) on the
      # `inferadb-ledger` binary — clap rejects them when placed after the
      # subcommand.
      if ! "$LEDGER_BINARY" \
            --tls-cert "$LEDGER_TLS_CERT" \
            --tls-server-name "$LEDGER_TLS_SERVER_NAME" \
            init --host="$first_addr"; then
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
# carry an explicit residency contract (`requires_residency`, `retention_days`).
# Data regions are not auto-created at boot — `init` only brings up the
# GLOBAL region. Any RPC that writes to a data region
# (`InitiateEmailVerification`, `CompleteRegistration`, `Write`, etc.)
# requires the region to be explicitly provisioned first.
#
# Args:
#   1: region_name  (e.g. "us-east-va")
#   2: protected    ("true" or "false")
#   3: max_attempts (default 30)
#
# Tries each listening cluster port on each attempt — `ProvisionRegion` is
# idempotent (`created = false` on a no-op) and can be served by any node.
provision_region() {
  local region=$1
  local protected=$2
  local max_attempts=${3:-30}
  if [[ -z "$region" ]]; then
    log_error "provision_region: region name is required (e.g. us-east-va)"
    return 1
  fi
  if [[ "$protected" != "true" && "$protected" != "false" ]]; then
    log_error "provision_region: protected must be 'true' or 'false' (got: '$protected')"
    return 1
  fi
  if ! command -v jq &>/dev/null; then
    log_error "jq is required for provision_region (install: brew install jq)"
    return 1
  fi
  if [[ -z "${LEDGER_TLS_CERT:-}" ]]; then
    log_error "provision_region: LEDGER_TLS_CERT not set (call bootstrap_cluster first)"
    return 1
  fi

  local node_count=${#CLUSTER_PIDS[@]}
  local base_port
  base_port=$(echo "$CLUSTER_ENDPOINTS" | cut -d, -f1 | sed 's@.*:@@')
  [[ -z "$base_port" ]] && { log_error "provision_region: cannot derive base port"; return 1; }

  # `--protected` and `--requires-residency` are flag-style on the admin
  # CLI. The previous `requires_residency: $protected` payload mirrored the
  # `protected` flag verbatim, so preserve that linkage here.
  local protected_flag=()
  local residency_flag=()
  if [[ "$protected" == "true" ]]; then
    protected_flag=(--protected)
    residency_flag=(--requires-residency)
  fi

  local attempt
  local last_result=""
  for attempt in $(seq 1 "$max_attempts"); do
    local i
    for ((i=0; i<node_count; i++)); do
      local addr="127.0.0.1:$((base_port + i))"
      local result
      # TLS flags are top-level (before the `admin` subcommand). clap rejects
      # them when placed after the subcommand.
      # `${arr[@]+"${arr[@]}"}` is the canonical bash idiom for expanding a
       # potentially-empty array under `set -u`. Plain `"${arr[@]}"` would
       # trip the nounset check when `protected_flag` / `residency_flag`
       # remain empty (the unprotected, no-residency case).
      result=$("$LEDGER_BINARY" \
        --tls-cert "$LEDGER_TLS_CERT" \
        --tls-server-name "$LEDGER_TLS_SERVER_NAME" \
        admin provision-region \
        --host "$addr" \
        --name "$region" \
        --retention-days 90 \
        ${protected_flag[@]+"${protected_flag[@]}"} \
        ${residency_flag[@]+"${residency_flag[@]}"} \
        2>&1) || true
      last_result="$result"
      if echo "$result" | jq -e '.name' &>/dev/null; then
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
