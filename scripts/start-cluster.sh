#!/usr/bin/env bash
#
# Start a local 3-node Ledger cluster for development.
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
NODE_IDS="1 2 3"

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
        cargo build --release --manifest-path "${PROJECT_ROOT}/Cargo.toml" -p ledger-server
    fi
}

create_config() {
    local node_id=$1
    local port
    port=$(get_port "$node_id")
    local config_dir="${DATA_ROOT}/node-${node_id}"
    local config_file="${config_dir}/ledger.toml"

    mkdir -p "$config_dir"

    # Build peers list (all nodes except self)
    local peers=""
    for peer_id in $NODE_IDS; do
        if [[ "$peer_id" != "$node_id" ]]; then
            local peer_port
            peer_port=$(get_port "$peer_id")
            peers="${peers}
[[peers]]
node_id = ${peer_id}
addr = \"127.0.0.1:${peer_port}\""
        fi
    done

    # Only node 1 bootstraps the cluster
    local bootstrap="false"
    if [[ "$node_id" == "1" ]]; then
        bootstrap="true"
    fi

    cat > "$config_file" << EOF
# Auto-generated config for node ${node_id}
node_id = ${node_id}
listen_addr = "127.0.0.1:${port}"
data_dir = "${config_dir}/data"
bootstrap = ${bootstrap}
${peers}

[batching]
max_batch_size = 100
max_batch_delay_ms = 10
EOF

    echo "$config_file"
}

start_node() {
    local node_id=$1
    local config_file
    config_file=$(create_config "$node_id")
    local log_file="${DATA_ROOT}/node-${node_id}/ledger.log"
    local pid_file="${DATA_ROOT}/node-${node_id}/ledger.pid"
    local port
    port=$(get_port "$node_id")

    if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        log "Node $node_id already running (PID $(cat "$pid_file"))"
        return 0
    fi

    log "Starting node $node_id on port ${port}..."
    RUST_LOG="${RUST_LOG:-info}" "$BINARY" --config "$config_file" > "$log_file" 2>&1 &
    local pid=$!
    echo "$pid" > "$pid_file"
    log "Node $node_id started (PID $pid)"
}

stop_node() {
    local node_id=$1
    local pid_file="${DATA_ROOT}/node-${node_id}/ledger.pid"

    if [[ -f "$pid_file" ]]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            log "Stopping node $node_id (PID $pid)..."
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

    log "Starting 3-node cluster..."
    log "Data directory: $DATA_ROOT"
    echo

    # Start nodes in order (node 1 first as bootstrap)
    for node_id in $NODE_IDS; do
        start_node "$node_id"
        # Small delay to let bootstrap node initialize first
        if [[ "$node_id" == "1" ]]; then
            sleep 1
        fi
    done

    echo
    log "Cluster started!"
    log "Logs: ${DATA_ROOT}/node-*/ledger.log"
    echo
    echo "Node endpoints:"
    for node_id in $NODE_IDS; do
        local port
        port=$(get_port "$node_id")
        echo "  Node $node_id: 127.0.0.1:${port}"
    done
    echo
    echo "Stop with: $0 stop"
}

stop_cluster() {
    log "Stopping cluster..."
    for node_id in $NODE_IDS; do
        stop_node "$node_id"
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
    for node_id in $NODE_IDS; do
        local pid_file="${DATA_ROOT}/node-${node_id}/ledger.pid"
        local port
        port=$(get_port "$node_id")
        if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
            echo "  Node $node_id: running (PID $(cat "$pid_file")) on port ${port}"
        else
            echo "  Node $node_id: stopped"
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
