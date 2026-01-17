#!/usr/bin/env bash
#
# Start a local 3-node Ledger cluster for development.
#
# Uses coordinated bootstrap: all 3 nodes discover each other via peer cache,
# exchange node info, and the node with the lowest Snowflake ID bootstraps
# the cluster while others wait to join.
#
# Usage:
#   ./scripts/start-cluster.sh         # Start cluster
#   ./scripts/start-cluster.sh stop    # Stop cluster
#   ./scripts/start-cluster.sh clean   # Stop and remove data
#   ./scripts/start-cluster.sh status  # Show running nodes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_ROOT="${LEDGER_DATA_ROOT:-/tmp/ledger-cluster}"
BINARY="${PROJECT_ROOT}/target/release/ledger"

# Node configuration (bash 3 compatible)
NODE_COUNT=3

get_port() {
    case "$1" in
        1) echo "50051" ;;
        2) echo "50052" ;;
        3) echo "50053" ;;
        *) echo "50051" ;;
    esac
}

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

build_if_needed() {
    if [[ ! -f "$BINARY" ]]; then
        log "Building ledger (release)..."
        cargo build --release --manifest-path "${PROJECT_ROOT}/Cargo.toml" -p inferadb-ledger-server
    fi
}

create_peer_cache() {
    local cache_file=$1
    shift
    local now
    now=$(date +%s)

    # Build peers array from remaining arguments
    local peers=""
    local first=true
    for addr in "$@"; do
        if [[ "$first" == "true" ]]; then
            first=false
        else
            peers="${peers},"
        fi
        peers="${peers}{\"addr\": \"${addr}\", \"priority\": 10, \"weight\": 100}"
    done

    cat > "$cache_file" << EOF
{
  "cached_at": ${now},
  "peers": [${peers}]
}
EOF
}

create_config() {
    local node_num=$1
    local port
    port=$(get_port "$node_num")
    local config_dir="${DATA_ROOT}/node-${node_num}"
    local config_file="${config_dir}/inferadb-ledger.toml"
    local cache_file="${config_dir}/peers.cache"

    mkdir -p "$config_dir"

    # Build list of all other node addresses for peer cache
    local peer_addrs=()
    for i in $(seq 1 $NODE_COUNT); do
        if [[ "$i" != "$node_num" ]]; then
            peer_addrs+=("127.0.0.1:$(get_port "$i")")
        fi
    done

    # Create peer cache with all other nodes
    create_peer_cache "$cache_file" "${peer_addrs[@]}"

    # All nodes use the same bootstrap config:
    # - min_cluster_size=3: wait for all nodes to discover each other
    # - Node IDs are auto-generated (Snowflake IDs)
    # - Lowest ID node will bootstrap, others wait to join
    cat > "$config_file" << EOF
# Auto-generated config for node ${node_num}
# Node ID is auto-generated (Snowflake ID) and persisted to data_dir/node_id
listen_addr = "127.0.0.1:${port}"
data_dir = "${config_dir}/data"

[bootstrap]
min_cluster_size = 3
bootstrap_timeout_secs = 30
poll_interval_secs = 1

[batching]
max_batch_size = 100
max_batch_delay_ms = 10

[discovery]
cached_peers_path = "${cache_file}"
EOF

    echo "$config_file"
}

start_node() {
    local node_num=$1
    local config_file
    config_file=$(create_config "$node_num")
    local log_file="${DATA_ROOT}/node-${node_num}/ledger.log"
    local pid_file="${DATA_ROOT}/node-${node_num}/ledger.pid"
    local port
    port=$(get_port "$node_num")

    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        log "Node $node_num already running (PID $(cat "$pid_file"))"
        return 0
    fi

    log "Starting node $node_num on port ${port}..."
    RUST_LOG="${RUST_LOG:-info}" "$BINARY" --config "$config_file" > "$log_file" 2>&1 &
    local pid=$!
    echo "$pid" > "$pid_file"
    log "Node $node_num started (PID $pid)"
}

stop_node() {
    local node_num=$1
    local pid_file="${DATA_ROOT}/node-${node_num}/ledger.pid"

    if [[ -f "$pid_file" ]]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            log "Stopping node $node_num (PID $pid)..."
            kill "$pid" 2>/dev/null || true
            # Wait for graceful shutdown
            local i=0
            while [[ $i -lt 10 ]]; do
                if ! kill -0 "$pid" 2>/dev/null; then
                    break
                fi
                sleep 0.5
                i=$((i + 1))
            done
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pid_file"
    fi
}

start_cluster() {
    build_if_needed
    mkdir -p "$DATA_ROOT"

    log "Starting ${NODE_COUNT}-node cluster with coordinated bootstrap..."
    log "Data directory: $DATA_ROOT"
    echo

    # Start all nodes simultaneously - they will coordinate via GetNodeInfo RPC
    # The node with the lowest Snowflake ID will bootstrap the cluster
    for node_num in $(seq 1 $NODE_COUNT); do
        start_node "$node_num"
    done

    echo
    log "Cluster starting! Nodes are coordinating bootstrap..."
    log "The node with the lowest Snowflake ID will become the initial leader."
    log "Logs: ${DATA_ROOT}/node-*/ledger.log"
    echo
    echo "Node endpoints:"
    for node_num in $(seq 1 $NODE_COUNT); do
        local port
        port=$(get_port "$node_num")
        echo "  Node $node_num: 127.0.0.1:${port}"
    done
    echo
    echo "Stop with: $0 stop"
}

stop_cluster() {
    log "Stopping cluster..."
    for node_num in $(seq 1 $NODE_COUNT); do
        stop_node "$node_num"
    done
    log "Cluster stopped"
}

clean_cluster() {
    stop_cluster
    if [[ -d "$DATA_ROOT" ]]; then
        log "Removing data directory: $DATA_ROOT"
        rm -rf "$DATA_ROOT"
    fi
    log "Cluster data cleaned"
}

status_cluster() {
    echo "Cluster status:"
    for node_num in $(seq 1 $NODE_COUNT); do
        local pid_file="${DATA_ROOT}/node-${node_num}/ledger.pid"
        local port
        port=$(get_port "$node_num")
        if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
            echo "  Node $node_num: running (PID $(cat "$pid_file")) on port ${port}"
        else
            echo "  Node $node_num: stopped"
        fi
    done
}

case "${1:-start}" in
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    clean)
        clean_cluster
        ;;
    status)
        status_cluster
        ;;
    restart)
        stop_cluster
        sleep 1
        start_cluster
        ;;
    *)
        echo "Usage: $0 {start|stop|clean|status|restart}"
        exit 1
        ;;
esac
