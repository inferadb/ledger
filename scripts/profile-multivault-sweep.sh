#!/usr/bin/env bash
# Multivault throughput sweep: reproduce the P10 ceiling environment and
# scan throughput across a (concurrency × pool_size) matrix.
#
# Locks the workload to `concurrent-writes-multivault` with a shared client
# (the realistic SDK consumer shape) and walks a grid of concurrency and
# connection_pool_size values, running the standard profile-server
# orchestration for each. Aggregates throughput + latency percentiles into
# a markdown table so future optimizations can be measured against the
# same baseline.
#
# Output: profiles/multivault-sweep-<UTC-ts>/
#   - sweep.md         — aggregated throughput grid + latency table
#   - sweep.csv        — same data, machine-readable
#   - <c>x<p>/         — per-cell directory with metrics.json + run.log
#                        + the flamegraph capture profile-server produced
#
# Usage: scripts/profile-multivault-sweep.sh [duration_secs]
#   duration_secs: per-cell measured duration (default 30)
#
# Environment variables:
#   SWEEP_CONCURRENCY     space-separated list of concurrency values
#                         (default: "32 128 512 1024")
#   SWEEP_POOL_SIZE       space-separated list of pool_size values
#                         (default: "1 4")
#   VAULTS                vault fan-out (default 16; matches P10's setup)
#   PROFILER              samply | flamegraph (forwarded to profile-server)
#   FLAMEGRAPH_FREQ       sample rate Hz (forwarded)
#   WARMUP_SECS           warmup seconds (forwarded)
#   PROFILE_SUITE_REPORT  set to 0 to skip the aggregated sweep.md
#
# Notes
# -----
# * SHARED_CLIENT=true is hardcoded — the whole point of this sweep is to
#   measure the per-Channel parallelism story P10 closed. Per-task clients
#   sidestep the bottleneck by accident; we want the realistic shape.
# * Total runtime ≈ |concurrency| × |pool_size| × (duration + ~15s overhead).
#   Default matrix (4×2 = 8 cells × ~45s) ≈ 6 min on a warm cargo cache.

set -euo pipefail

DURATION="${1:-30}"
SWEEP_CONCURRENCY_LIST="${SWEEP_CONCURRENCY:-32 128 512 1024}"
SWEEP_POOL_SIZE_LIST="${SWEEP_POOL_SIZE:-1 4}"
SWEEP_VAULTS="${VAULTS:-16}"

# --- session dir --------------------------------------------------------------

TS=$(date -u +%Y%m%dT%H%M%SZ)
SESSION_DIR="profiles/multivault-sweep-$TS"
mkdir -p "$SESSION_DIR"
echo "==> sweep directory: $SESSION_DIR"

# --- record sweep config ------------------------------------------------------

GIT_SHA=$(git -C . rev-parse HEAD 2>/dev/null || echo "unknown")
PLATFORM=$(uname -s)
cat > "$SESSION_DIR/config.json" <<EOF
{
  "session_ts": "$TS",
  "duration_secs": $DURATION,
  "vaults": $SWEEP_VAULTS,
  "concurrency_grid": [$(echo "$SWEEP_CONCURRENCY_LIST" | tr ' ' '\n' | grep -v '^$' | paste -sd, -)],
  "pool_size_grid": [$(echo "$SWEEP_POOL_SIZE_LIST" | tr ' ' '\n' | grep -v '^$' | paste -sd, -)],
  "platform": "$PLATFORM",
  "git_sha": "$GIT_SHA",
  "shared_client": true
}
EOF

# --- run each cell ------------------------------------------------------------

CELL_COUNT=0
for concurrency in $SWEEP_CONCURRENCY_LIST; do
    for pool_size in $SWEEP_POOL_SIZE_LIST; do
        CELL_COUNT=$((CELL_COUNT + 1))
        cell_dir="$SESSION_DIR/c${concurrency}-p${pool_size}"
        mkdir -p "$cell_dir"
        echo ""
        echo "============================================================"
        echo "==> cell ${CELL_COUNT}: concurrency=$concurrency pool_size=$pool_size (${DURATION}s)"
        echo "============================================================"
        CONCURRENCY="$concurrency" \
        POOL_SIZE="$pool_size" \
        SHARED_CLIENT=true \
        VAULTS="$SWEEP_VAULTS" \
        PROFILE_OUTPUT_DIR="$cell_dir" \
        PROFILE_METRICS_PATH="$cell_dir/metrics.json" \
            ./scripts/profile-server.sh sampling concurrent-writes-multivault "$DURATION" \
            2>&1 | tee "$cell_dir/run.log" || {
                echo "warn: cell c${concurrency}-p${pool_size} failed; continuing" >&2
            }
    done
done

# --- aggregate ----------------------------------------------------------------

if [[ "${PROFILE_SUITE_REPORT:-1}" == "0" ]]; then
    echo ""
    echo "==> sweep complete (report skipped): $SESSION_DIR"
    exit 0
fi

echo ""
echo "==> aggregating results"

if ! command -v jq >/dev/null 2>&1; then
    echo "warn: jq not on PATH; skipping aggregated report (raw metrics.json files preserved)" >&2
    echo "==> sweep complete: $SESSION_DIR"
    exit 0
fi

CSV="$SESSION_DIR/sweep.csv"
MD="$SESSION_DIR/sweep.md"

echo "concurrency,pool_size,throughput_ops_s,p50_us,p95_us,p99_us,p999_us,errors,operations,elapsed_s" > "$CSV"

for concurrency in $SWEEP_CONCURRENCY_LIST; do
    for pool_size in $SWEEP_POOL_SIZE_LIST; do
        cell_dir="$SESSION_DIR/c${concurrency}-p${pool_size}"
        metrics="$cell_dir/metrics.json"
        if [[ ! -f "$metrics" ]]; then
            echo "$concurrency,$pool_size,FAILED,FAILED,FAILED,FAILED,FAILED,FAILED,FAILED,FAILED" >> "$CSV"
            continue
        fi
        # metrics.json fields are ns-precision latencies and ops/s throughput.
        # Convert to microseconds for human-friendly tables.
        jq -r --arg c "$concurrency" --arg p "$pool_size" '
            [
                $c,
                $p,
                (.throughput_ops_per_sec // .throughput // 0),
                ((.latency_ns.p50 // 0) / 1000),
                ((.latency_ns.p95 // 0) / 1000),
                ((.latency_ns.p99 // 0) / 1000),
                ((.latency_ns.p999 // 0) / 1000),
                (.errors // 0),
                (.operations // 0),
                (.elapsed_secs // .elapsed // 0)
            ] | @csv
        ' "$metrics" >> "$CSV" 2>/dev/null || {
            echo "$concurrency,$pool_size,PARSE_ERROR,PARSE_ERROR,PARSE_ERROR,PARSE_ERROR,PARSE_ERROR,PARSE_ERROR,PARSE_ERROR,PARSE_ERROR" >> "$CSV"
        }
    done
done

# Markdown report —————————————————————————————————————————————————————————
{
    echo "# Multivault throughput sweep"
    echo ""
    echo "- **Timestamp:** \`$TS\`"
    echo "- **Duration per cell:** \`${DURATION}s\`"
    echo "- **Vaults:** \`$SWEEP_VAULTS\`"
    echo "- **Workload:** \`concurrent-writes-multivault\` (shared client, fixed key space)"
    echo "- **Git SHA:** \`$GIT_SHA\`"
    echo ""
    echo "## Throughput grid (ops/s)"
    echo ""
    echo "Rows = concurrency, columns = connection_pool_size."
    echo ""
    # Header row.
    printf "| concurrency \\\\ pool_size |"
    for pool_size in $SWEEP_POOL_SIZE_LIST; do
        printf " %s |" "$pool_size"
    done
    echo ""
    printf "|---:|"
    for _ in $SWEEP_POOL_SIZE_LIST; do printf "---:|"; done
    echo ""
    # Data rows.
    for concurrency in $SWEEP_CONCURRENCY_LIST; do
        printf "| %s |" "$concurrency"
        for pool_size in $SWEEP_POOL_SIZE_LIST; do
            tput=$(awk -F, -v c="$concurrency" -v p="$pool_size" '$1==c && $2==p {print $3}' "$CSV")
            if [[ -z "$tput" || "$tput" == "FAILED" || "$tput" == "PARSE_ERROR" ]]; then
                printf " — |"
            else
                # Format with thousands comma when possible.
                printf " %s |" "$(printf "%.0f" "$tput" 2>/dev/null || echo "$tput")"
            fi
        done
        echo ""
    done
    echo ""
    echo "## Full results"
    echo ""
    echo "| concurrency | pool_size | ops/s | p50 (µs) | p95 (µs) | p99 (µs) | p999 (µs) | errors |"
    echo "|---:|---:|---:|---:|---:|---:|---:|---:|"
    tail -n +2 "$CSV" | while IFS=, read -r c p tput p50 p95 p99 p999 errs ops elapsed; do
        echo "| $c | $p | $tput | $p50 | $p95 | $p99 | $p999 | $errs |"
    done
    echo ""
    echo "## Reproducing the P10 ceiling"
    echo ""
    echo "P10 (2026-04-28) closed the per-tonic-Channel buffer ceiling by adding"
    echo "\`ClientConfig::connection_pool_size\`. Reference numbers from that session"
    echo "on \`concurrent-writes-multivault\` (16 vaults, shared client):"
    echo ""
    echo "| Mode | Concurrency | Pool size | Throughput |"
    echo "|---|---:|---:|---:|"
    echo "| Shared (pre-fix) | 1024 | 1 | ~25.0k |"
    echo "| Shared (post-fix) | 1024 | 4 | ~42.8k |"
    echo "| Per-task (no pool needed) | 1024 | 1 | ~41.8k |"
    echo ""
    echo "Use this sweep to verify the ceiling holds and to measure where future"
    echo "optimizations move the curve. The \`p1=1\` column is the historical"
    echo "single-Channel ceiling; raising \`pool_size\` should track linearly with"
    echo "concurrency until the server-side apply pipeline binds."
    echo ""
    echo "## Flamegraphs"
    echo ""
    for concurrency in $SWEEP_CONCURRENCY_LIST; do
        for pool_size in $SWEEP_POOL_SIZE_LIST; do
            cell="c${concurrency}-p${pool_size}"
            for fg in "$SESSION_DIR/$cell"/*.json.gz; do
                [[ -e "$fg" ]] || continue
                rel="${fg#"$SESSION_DIR/"}"
                echo "- \`$rel\`"
            done
        done
    done
} > "$MD"

echo ""
echo "==> sweep complete: $SESSION_DIR"
echo "==> markdown report: $MD"
echo "==> csv: $CSV"
