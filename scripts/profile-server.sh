#!/usr/bin/env bash
# Server-under-load profiling orchestrator.
#
# Two modes:
#   sampling — run server under a sampling profiler (samply/flamegraph); output is
#              a standard CPU flamegraph showing what's on-CPU each sample interval.
#   spans    — run server with --features profiling + --flamegraph-spans; uses
#              tracing-flame to record span enter/exit events; output is a
#              semantic flamegraph showing logical-operation time.
#
# Usage: scripts/profile-server.sh <mode> <workload> [duration_secs]
#   mode:          sampling | spans
#   workload:      throughput-writes | mixed-rw | check-heavy
#   duration_secs: measured-phase duration (default 60)
#
# Environment variables:
#   PROFILER              samply (macOS default) | flamegraph (Linux default)
#   FLAMEGRAPH_FREQ       sample rate in Hz (default 997)
#   PROFILE_OUTPUT_DIR    output directory (default profiles/)
#   WARMUP_SECS           warmup duration in seconds (default 5)
#   PROFILE_METRICS_PATH  when set, passed as --metrics-json to the workload
#                         binary; the measured-phase metrics report is written
#                         to that path (consumed by scripts/profile-suite.sh)

set -euo pipefail

MODE="${1:-}"
WORKLOAD="${2:-throughput-writes}"
DURATION="${3:-60}"

# Platform-aware default profiler. samply on macOS has better symbol resolution
# for inlined async frames than dtrace; perf via cargo-flamegraph is rock-solid
# on Linux.
_OS="$(uname -s)"
case "$_OS" in
    Darwin) _DEFAULT_PROFILER="samply" ;;
    Linux)  _DEFAULT_PROFILER="flamegraph" ;;
    *)      _DEFAULT_PROFILER="flamegraph" ;;
esac

PROFILER="${PROFILER:-$_DEFAULT_PROFILER}"
FLAMEGRAPH_FREQ="${FLAMEGRAPH_FREQ:-997}"
PROFILE_OUTPUT_DIR="${PROFILE_OUTPUT_DIR:-profiles}"
WARMUP_SECS="${WARMUP_SECS:-5}"

# Spans mode: tracing-flame can balloon with verbose filters. Pin a reasonable
# default unless the operator explicitly sets RUST_LOG themselves.
SPANS_RUST_LOG="${RUST_LOG:-info,inferadb_ledger=debug}"

# Blinding key used by test clusters — a deterministic dev-only value. See
# scripts/lib/cluster-bootstrap.sh for the same constant.
BLINDING_KEY="deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

# --- pre-flight ---------------------------------------------------------------

case "$MODE" in
    sampling|spans) ;;
    "") echo "error: mode is required (sampling|spans)" >&2; exit 2 ;;
    *)  echo "error: unknown mode '$MODE' (expected sampling|spans)" >&2; exit 2 ;;
esac

./scripts/profile.sh doctor >/dev/null || {
    echo "error: profiling prerequisites missing (run: just doctor-profiling)" >&2
    exit 1
}

case "$WORKLOAD" in
    throughput-writes|mixed-rw|check-heavy) ;;
    *) echo "error: unknown workload '$WORKLOAD' (expected throughput-writes|mixed-rw|check-heavy)" >&2; exit 1 ;;
esac

if [[ "$MODE" == "spans" ]]; then
    command -v inferno-flamegraph >/dev/null 2>&1 || {
        echo "error: inferno-flamegraph not on PATH (required for spans mode)" >&2
        echo "hint:  cargo install inferno" >&2
        exit 1
    }
fi

# --- build ------------------------------------------------------------------

echo "==> building profile + server binaries with --profile profiling (mode=$MODE)"
cargo +1.92 build --profile profiling -p inferadb-ledger-profile
if [[ "$MODE" == "spans" ]]; then
    cargo +1.92 build --profile profiling --features profiling -p inferadb-ledger-server
else
    cargo +1.92 build --profile profiling -p inferadb-ledger-server
fi

SERVER_BIN="$PWD/target/profiling/inferadb-ledger"
PROFILE_BIN="$PWD/target/profiling/inferadb-ledger-profile"
test -x "$SERVER_BIN"  || { echo "error: server binary not found at $SERVER_BIN" >&2; exit 1; }
test -x "$PROFILE_BIN" || { echo "error: profile binary not found at $PROFILE_BIN" >&2; exit 1; }

# --- port allocation --------------------------------------------------------

# Ask the kernel for a free port via Python. Simple, portable, atomic.
find_free_port() {
    python3 -c '
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
'
}

PORT=$(find_free_port)
ENDPOINT="http://127.0.0.1:${PORT}"
echo "==> allocated port: $PORT"

# --- data dir ---------------------------------------------------------------

TS=$(date -u +%Y%m%dT%H%M%SZ)
DATA_DIR="$PWD/target/profiling-data/$TS"
mkdir -p "$DATA_DIR"
echo "==> data dir: $DATA_DIR"

# --- output naming ----------------------------------------------------------

mkdir -p "$PROFILE_OUTPUT_DIR"
if [[ "$MODE" == "spans" ]]; then
    # Spans mode output is always SVG, post-processed from folded-stacks.
    SPANS_FOLDED="$DATA_DIR/spans.folded"
    OUTPUT="$PROFILE_OUTPUT_DIR/server-spans-${WORKLOAD}-${TS}.svg"
    RAW_OUTPUT=""  # unused in spans mode
else
    case "$PROFILER" in
        flamegraph) EXT="svg" ;;
        samply)     EXT="json.gz" ;;
        *) echo "error: unknown PROFILER='$PROFILER'" >&2; exit 1 ;;
    esac
    OUTPUT="$PROFILE_OUTPUT_DIR/server-${WORKLOAD}-${TS}.${EXT}"
    RAW_OUTPUT="$DATA_DIR/capture.${EXT}"
fi

# --- cleanup trap -----------------------------------------------------------

# SERVER_PID is the wrapper bash spawned with `&` — in sampling mode that's
# samply/cargo-flamegraph, in spans mode it's the server itself. TARGET_PID
# is the actual inferadb-ledger process that should receive SIGINT for
# graceful shutdown. In spans mode they're equal; in sampling mode the
# wrapper doesn't reliably propagate SIGINT to its child, so we signal the
# child directly.
SERVER_PID=""
TARGET_PID=""
cleanup() {
    local exit_code=$?
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        if [[ -n "$TARGET_PID" ]] && kill -0 "$TARGET_PID" 2>/dev/null; then
            echo "==> cleanup: SIGINT server PID $TARGET_PID (wrapper $SERVER_PID)"
            kill -INT "$TARGET_PID" 2>/dev/null || true
        else
            # Target couldn't be resolved or has already exited; fall back
            # to signaling the wrapper.
            echo "==> cleanup: SIGINT wrapper PID $SERVER_PID"
            kill -INT "$SERVER_PID" 2>/dev/null || true
        fi
        # Give graceful shutdown up to 10s, then kill -9.
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            kill -0 "$SERVER_PID" 2>/dev/null || break
            sleep 1
        done
        kill -9 "$SERVER_PID" 2>/dev/null || true
    fi
    # Finalize outputs.
    if [[ "$MODE" == "spans" ]]; then
        if [[ -f "$SPANS_FOLDED" ]]; then
            echo "==> post-processing spans → SVG via inferno-flamegraph"
            inferno-flamegraph < "$SPANS_FOLDED" > "$OUTPUT" || {
                echo "warn: inferno-flamegraph failed; folded-stack file preserved at $SPANS_FOLDED" >&2
            }
            echo "==> wrote $OUTPUT"
        fi
    else
        if [[ -f "$RAW_OUTPUT" ]]; then
            mv "$RAW_OUTPUT" "$OUTPUT"
            echo "==> wrote $OUTPUT"
        fi
    fi
    rm -rf "$DATA_DIR"
    exit "$exit_code"
}
trap cleanup EXIT

# --- start server -----------------------------------------------------------

if [[ "$MODE" == "spans" ]]; then
    echo "==> starting server with --flamegraph-spans $SPANS_FOLDED (RUST_LOG=$SPANS_RUST_LOG)"
    RUST_LOG="$SPANS_RUST_LOG" "$SERVER_BIN" \
        --listen "127.0.0.1:${PORT}" \
        --data "$DATA_DIR/node" \
        --email-blinding-key "$BLINDING_KEY" \
        --log-format text \
        --flamegraph-spans "$SPANS_FOLDED" \
        > "$DATA_DIR/server.log" 2>&1 &
    SERVER_PID=$!
else
    echo "==> starting server under $PROFILER"
    case "$PROFILER" in
        flamegraph)
            # Use the standalone `flamegraph` wrapper (not `cargo flamegraph`).
            # `cargo flamegraph` wants to build a cargo target; our workspace
            # has multiple bin targets and cargo-flamegraph fails with an
            # ambiguous-target error. The standalone wrapper takes an already-
            # built binary path, which is what we want — we built it above.
            flamegraph \
                --freq "$FLAMEGRAPH_FREQ" \
                -o "$RAW_OUTPUT" \
                -- "$SERVER_BIN" \
                    --listen "127.0.0.1:${PORT}" \
                    --data "$DATA_DIR/node" \
                    --email-blinding-key "$BLINDING_KEY" \
                    --log-format text \
                > "$DATA_DIR/server.log" 2>&1 &
            SERVER_PID=$!
            ;;
        samply)
            samply record \
                --save-only \
                --output "$RAW_OUTPUT" \
                -- "$SERVER_BIN" \
                    --listen "127.0.0.1:${PORT}" \
                    --data "$DATA_DIR/node" \
                    --email-blinding-key "$BLINDING_KEY" \
                    --log-format text \
                > "$DATA_DIR/server.log" 2>&1 &
            SERVER_PID=$!
            ;;
    esac
fi

# --- wait for readiness -----------------------------------------------------

echo "==> waiting for port $PORT to accept connections"
for i in $(seq 1 60); do
    if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "error: server exited before accepting connections; last log lines:" >&2
        tail -30 "$DATA_DIR/server.log" >&2 || true
        exit 1
    fi
    sleep 1
    if [[ "$i" = "60" ]]; then
        echo "error: server did not listen on port $PORT within 60s" >&2
        exit 1
    fi
done
echo "==> server listening"

# --- resolve the actual server PID for shutdown signals ---------------------

# In sampling mode, $SERVER_PID is the wrapper (samply or cargo-flamegraph),
# which doesn't reliably propagate SIGINT to its child. Find the real
# inferadb-ledger process so graceful shutdown is triggered directly on it.
# In spans mode, no wrapper — $SERVER_PID already IS the server.
#
# `pgrep -P $wrapper` returns only direct children of the wrapper process.
# That's what we want: samply spawns the server as its one child. We can't
# use `pgrep -f` here because samply's own command line contains the server
# binary path (as its argv after `--`) and would match first.
if [[ "$MODE" == "spans" ]]; then
    TARGET_PID="$SERVER_PID"
else
    for _ in 1 2 3 4 5; do
        TARGET_PID=$(pgrep -P "$SERVER_PID" 2>/dev/null | head -1 || true)
        if [[ -n "$TARGET_PID" && "$TARGET_PID" != "$SERVER_PID" ]]; then
            break
        fi
        TARGET_PID=""
        sleep 1
    done
    if [[ -z "$TARGET_PID" ]]; then
        echo "error: could not locate inferadb-ledger PID under wrapper $SERVER_PID within 5s" >&2
        exit 1
    fi
    echo "==> wrapper PID: $SERVER_PID, server PID: $TARGET_PID"
fi

# --- bootstrap cluster -------------------------------------------------------

echo "==> running init subcommand"
"$SERVER_BIN" init --host="127.0.0.1:${PORT}" || {
    echo "error: init failed; last server log lines:" >&2
    tail -30 "$DATA_DIR/server.log" >&2 || true
    exit 1
}
# Brief settle before we start firing SDK traffic.
sleep 2

# --- warmup + measured phases -----------------------------------------------

echo "==> warmup (${WARMUP_SECS}s, preset=$WORKLOAD)"
"$PROFILE_BIN" "$WORKLOAD" \
    --endpoint "$ENDPOINT" \
    --duration "$WARMUP_SECS" \
    > "$DATA_DIR/warmup.log" 2>&1 || {
        echo "error: warmup failed; last log lines:" >&2
        tail -30 "$DATA_DIR/warmup.log" >&2 || true
        exit 1
    }

echo "==> measured phase (${DURATION}s, preset=$WORKLOAD)"
if [[ -n "${PROFILE_METRICS_PATH:-}" ]]; then
    "$PROFILE_BIN" "$WORKLOAD" \
        --endpoint "$ENDPOINT" \
        --duration "$DURATION" \
        --metrics-json "$PROFILE_METRICS_PATH" \
        2>&1 | tee "$DATA_DIR/measured.log" || true
else
    "$PROFILE_BIN" "$WORKLOAD" \
        --endpoint "$ENDPOINT" \
        --duration "$DURATION" \
        2>&1 | tee "$DATA_DIR/measured.log" || true
fi

# --- shutdown ---------------------------------------------------------------

echo "==> sending SIGINT to server PID $TARGET_PID (wrapper: $SERVER_PID)"
kill -INT "$TARGET_PID" 2>/dev/null || true
# Wait for the wrapper process (samply / cargo-flamegraph / bare server) to
# exit. In sampling mode the wrapper finalizes its output file after its
# child exits; in spans mode the FlushGuard drops during graceful shutdown.
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
TARGET_PID=""
