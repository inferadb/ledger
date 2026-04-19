#!/usr/bin/env bash
# Profile suite: run a canonical set of workloads end-to-end and emit a
# per-session directory with flamegraphs + per-workload metrics.json files +
# an aggregated summary.md.
#
# The six workloads cover the entity and relationship R/W hot paths:
#   throughput-writes    — pure entity write
#   entity-reads         — pure entity read (seeded)
#   relationship-writes  — pure relationship write
#   relationship-reads   — pure relationship check (seeded)
#   mixed-rw             — 70/30 entity writes/reads (mixed-shape baseline)
#   check-heavy          — 90/10 relationship check/write (mixed-shape baseline)
#
# Usage: scripts/profile-suite.sh [duration_secs] [mode] [workloads...]
#   duration_secs: per-workload measured duration (default 30)
#   mode:          sampling | spans (default sampling — faster)
#   workloads:     optional explicit list. Default: all six.
#
# Output: profiles/session-<UTC-ts>/
#
# Environment variables (all optional):
#   PROFILER                samply | flamegraph   (forwarded to profile-server.sh)
#   FLAMEGRAPH_FREQ         sample rate in Hz      (forwarded)
#   WARMUP_SECS             warmup seconds         (forwarded)
#   PROFILE_SUITE_REPORT    set to 0 to skip the aggregated summary.md

set -euo pipefail

DURATION="${1:-30}"
MODE="${2:-sampling}"
shift || true
shift || true
WORKLOADS=("$@")
if [[ ${#WORKLOADS[@]} -eq 0 ]]; then
    WORKLOADS=(
        throughput-writes
        entity-reads
        relationship-writes
        relationship-reads
        mixed-rw
        check-heavy
    )
fi

case "$MODE" in
    sampling|spans) ;;
    *) echo "error: unknown mode '$MODE' (expected sampling|spans)" >&2; exit 2 ;;
esac

# --- session dir --------------------------------------------------------------

TS=$(date -u +%Y%m%dT%H%M%SZ)
SESSION_DIR="profiles/session-$TS"
mkdir -p "$SESSION_DIR"
echo "==> session directory: $SESSION_DIR"

# --- record session config ----------------------------------------------------

GIT_SHA=$(git -C . rev-parse HEAD 2>/dev/null || echo "unknown")
PLATFORM=$(uname -s)
cat > "$SESSION_DIR/config.json" <<EOF
{
  "session_ts": "$TS",
  "duration_secs": $DURATION,
  "mode": "$MODE",
  "workloads": [$(printf '"%s",' "${WORKLOADS[@]}" | sed 's/,$//')],
  "platform": "$PLATFORM",
  "git_sha": "$GIT_SHA",
  "profiler": "${PROFILER:-$( [[ $PLATFORM == Darwin ]] && echo samply || echo flamegraph )}"
}
EOF

# --- run each workload --------------------------------------------------------

for workload in "${WORKLOADS[@]}"; do
    workload_dir="$SESSION_DIR/$workload"
    mkdir -p "$workload_dir"
    echo ""
    echo "============================================================"
    echo "==> $workload (${DURATION}s, mode=$MODE)"
    echo "============================================================"
    # Flamegraph output goes into the workload subdir via PROFILE_OUTPUT_DIR;
    # metrics JSON goes into the same subdir via PROFILE_METRICS_PATH.
    PROFILE_OUTPUT_DIR="$workload_dir" \
    PROFILE_METRICS_PATH="$workload_dir/metrics.json" \
        ./scripts/profile-server.sh "$MODE" "$workload" "$DURATION" \
        2>&1 | tee "$workload_dir/run.log" || {
            echo "warn: $workload failed; continuing with remaining workloads" >&2
        }
done

# --- generate summary ---------------------------------------------------------

if [[ "${PROFILE_SUITE_REPORT:-1}" != "0" ]]; then
    echo ""
    echo "==> generating summary report"
    if ! ./scripts/profile-suite-report.sh "$SESSION_DIR"; then
        echo "warn: report generation failed (missing jq?); session data preserved at $SESSION_DIR" >&2
    fi
fi

echo ""
echo "==> suite complete: $SESSION_DIR"
