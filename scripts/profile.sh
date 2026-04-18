#!/usr/bin/env bash
# Shared profiling harness. Subcommands: doctor, bench, test.
#
# Environment variables:
#   PROFILER            samply (macOS default) | flamegraph (Linux default)
#   FLAMEGRAPH_FREQ     sample rate in Hz (default 997)
#   PROFILE_OUTPUT_DIR  output directory (default profiles/)
#
# Usage:
#   scripts/profile.sh doctor
#   scripts/profile.sh bench <crate> <bench-name>
#   scripts/profile.sh test  <crate> <test-name>

set -euo pipefail

# Platform-aware default profiler. Mirrors scripts/profile-server.sh: samply on
# macOS has better symbol resolution for inlined async frames than dtrace; perf
# via cargo-flamegraph is rock-solid on Linux.
case "$(uname -s)" in
    Darwin) _DEFAULT_PROFILER="samply" ;;
    Linux)  _DEFAULT_PROFILER="flamegraph" ;;
    *)      _DEFAULT_PROFILER="flamegraph" ;;
esac

PROFILER="${PROFILER:-$_DEFAULT_PROFILER}"
FLAMEGRAPH_FREQ="${FLAMEGRAPH_FREQ:-997}"
PROFILE_OUTPUT_DIR="${PROFILE_OUTPUT_DIR:-profiles}"

_ts() { date -u +%Y%m%dT%H%M%SZ; }

_ensure_out_dir() {
    mkdir -p "$PROFILE_OUTPUT_DIR"
}

_require_tool() {
    local name=$1
    if ! command -v "$name" >/dev/null 2>&1; then
        echo "error: '$name' not found on PATH" >&2
        case "$name" in
            cargo-flamegraph|flamegraph)
                echo "hint: cargo install flamegraph" >&2
                ;;
            samply)
                echo "hint: cargo install samply" >&2
                ;;
        esac
        return 1
    fi
}

doctor() {
    local os
    os=$(uname -s)
    echo "=== profiler ==="
    case "$PROFILER" in
        flamegraph)
            _require_tool cargo-flamegraph || return 1
            cargo flamegraph --version 2>/dev/null | head -1 || true
            ;;
        samply)
            _require_tool samply || return 1
            samply --version 2>/dev/null | head -1 || true
            ;;
        *)
            echo "error: unknown PROFILER='$PROFILER' (expected 'flamegraph' or 'samply')" >&2
            return 1
            ;;
    esac
    echo ""

    echo "=== platform ==="
    echo "os: $os"
    case "$os" in
        Darwin)
            if ! command -v dtrace >/dev/null 2>&1; then
                echo "warn: dtrace not on PATH — cargo-flamegraph on macOS requires dtrace" >&2
            else
                echo "dtrace: $(command -v dtrace)"
            fi
            echo "note: cargo-flamegraph spawns the target as a child, so sudo is not required"
            ;;
        Linux)
            if ! command -v perf >/dev/null 2>&1; then
                echo "warn: perf not on PATH — install linux-perf or similar" >&2
            else
                echo "perf: $(command -v perf)"
            fi
            if [[ -r /proc/sys/kernel/perf_event_paranoid ]]; then
                local paranoid
                paranoid=$(cat /proc/sys/kernel/perf_event_paranoid)
                echo "perf_event_paranoid: $paranoid"
                if (( paranoid > 1 )); then
                    echo "warn: perf_event_paranoid > 1 blocks unprivileged perf record" >&2
                    echo "fix:  sudo sysctl -w kernel.perf_event_paranoid=1" >&2
                fi
            fi
            ;;
        *)
            echo "warn: unsupported OS '$os' — profiling recipes are tested on macOS and Linux" >&2
            ;;
    esac
    echo ""

    echo "=== post-processing ==="
    if ! command -v inferno-flamegraph >/dev/null 2>&1; then
        echo "warn: inferno-flamegraph not on PATH — required for tracing-flame spans mode (profile-server-spans)" >&2
        echo "hint: cargo install inferno" >&2
    else
        echo "inferno-flamegraph: $(command -v inferno-flamegraph)"
    fi
    echo ""

    echo "=== output ==="
    echo "PROFILE_OUTPUT_DIR: $PROFILE_OUTPUT_DIR"
    echo "FLAMEGRAPH_FREQ:    $FLAMEGRAPH_FREQ"
    echo ""
    echo "doctor ok."
}

_profile_cmd() {
    local output=$1
    shift
    case "$PROFILER" in
        flamegraph)
            cargo +1.92 flamegraph \
                --profile profiling \
                --freq "$FLAMEGRAPH_FREQ" \
                -o "$output" \
                "$@"
            ;;
        samply)
            samply record \
                --save-only \
                --output "$output" \
                -- cargo +1.92 run --profile profiling "$@"
            ;;
        *)
            echo "error: unknown PROFILER='$PROFILER'" >&2
            return 1
            ;;
    esac
}

bench() {
    local crate=$1
    local bench=$2
    _ensure_out_dir
    local ext ts output
    case "$PROFILER" in
        flamegraph) ext="svg" ;;
        samply)     ext="json.gz" ;;
        *) echo "error: unknown PROFILER" >&2; return 1 ;;
    esac
    ts=$(_ts)
    output="$PROFILE_OUTPUT_DIR/bench-${crate}-${bench}-${ts}.${ext}"

    echo "==> profiling bench: crate=$crate bench=$bench output=$output"
    _profile_cmd "$output" \
        -p "inferadb-ledger-${crate}" --bench "$bench" \
        -- --profile-time 10
    echo "==> wrote $output"
}

test_one() {
    local crate=$1
    local name=$2
    _ensure_out_dir
    local ext ts output
    case "$PROFILER" in
        flamegraph) ext="svg" ;;
        samply)     ext="json.gz" ;;
        *) echo "error: unknown PROFILER" >&2; return 1 ;;
    esac
    ts=$(_ts)
    local name_flat
    name_flat=$(echo "$name" | tr '/:' '__')
    output="$PROFILE_OUTPUT_DIR/test-${crate}-${name_flat}-${ts}.${ext}"

    echo "==> profiling test: crate=$crate name=$name output=$output"
    _profile_cmd "$output" \
        -p "inferadb-ledger-${crate}" --test integration \
        -- "$name" --exact --nocapture --test-threads=1
    echo "==> wrote $output"
}

usage() {
    cat <<'EOF'
Usage:
  scripts/profile.sh doctor
  scripts/profile.sh bench <crate> <bench-name>
  scripts/profile.sh test  <crate> <test-name>

Env vars: PROFILER, FLAMEGRAPH_FREQ, PROFILE_OUTPUT_DIR.
EOF
}

main() {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi
    local sub=$1
    shift
    case "$sub" in
        doctor) doctor "$@" ;;
        bench)  [[ $# -eq 2 ]] || { usage; exit 1; }; bench "$@" ;;
        test)   [[ $# -eq 2 ]] || { usage; exit 1; }; test_one "$@" ;;
        -h|--help) usage ;;
        *) echo "error: unknown subcommand '$sub'" >&2; usage; exit 1 ;;
    esac
}

main "$@"
