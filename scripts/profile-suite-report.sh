#!/usr/bin/env bash
# Aggregate per-workload metrics.json files into a session summary markdown.
#
# Usage: scripts/profile-suite-report.sh <session-dir>
#
# Reads: $session-dir/config.json, $session-dir/*/metrics.json
# Writes: $session-dir/summary.md

set -euo pipefail

SESSION_DIR="${1:-}"
if [[ -z "$SESSION_DIR" ]]; then
    echo "error: usage: profile-suite-report.sh <session-dir>" >&2
    exit 2
fi
if [[ ! -d "$SESSION_DIR" ]]; then
    echo "error: session dir does not exist: $SESSION_DIR" >&2
    exit 1
fi

command -v jq >/dev/null 2>&1 || {
    echo "error: jq not on PATH (required for report aggregation)" >&2
    echo "hint: brew install jq  (or: apt install jq)" >&2
    exit 1
}

CONFIG="$SESSION_DIR/config.json"
SUMMARY="$SESSION_DIR/summary.md"

# --- header -------------------------------------------------------------------

{
    echo "# Profile session"
    echo ""
    if [[ -f "$CONFIG" ]]; then
        ts=$(jq -r '.session_ts // "unknown"' "$CONFIG")
        duration=$(jq -r '.duration_secs // "unknown"' "$CONFIG")
        mode=$(jq -r '.mode // "unknown"' "$CONFIG")
        platform=$(jq -r '.platform // "unknown"' "$CONFIG")
        profiler=$(jq -r '.profiler // "unknown"' "$CONFIG")
        git_sha=$(jq -r '.git_sha // "unknown"' "$CONFIG")
        echo "- **Timestamp:** \`$ts\`"
        echo "- **Duration per workload:** \`${duration}s\`"
        echo "- **Mode:** \`$mode\`"
        echo "- **Profiler:** \`$profiler\`"
        echo "- **Platform:** \`$platform\`"
        echo "- **Git SHA:** \`$git_sha\`"
    fi
    echo ""
    echo "## Workload metrics"
    echo ""
    echo "Latency percentiles in microseconds. Throughput in ops/s. Error counts over the full measured window."
    echo ""
    echo "| Workload | Throughput | Errors | p50 | p95 | p99 | p999 | max |"
    echo "|---|---:|---:|---:|---:|---:|---:|---:|"
} > "$SUMMARY"

# --- workload rows ------------------------------------------------------------

# Find every metrics.json in an immediate subdirectory of the session.
found_any=0
for metrics_file in "$SESSION_DIR"/*/metrics.json; do
    [[ -f "$metrics_file" ]] || continue
    found_any=1
    jq -r '
        def us: . / 1000 | tostring;
        "| \(.preset) | \(.throughput_ops_per_sec | tonumber | . * 10 | floor / 10) | \(.errors) | \(.latency_ns.p50_ns | us) | \(.latency_ns.p95_ns | us) | \(.latency_ns.p99_ns | us) | \(.latency_ns.p999_ns | us) | \(.latency_ns.max_ns | us) |"
    ' "$metrics_file" >> "$SUMMARY"
done

if [[ $found_any -eq 0 ]]; then
    echo "| _(no metrics.json files found in session subdirs)_ |  |  |  |  |  |  |  |" >> "$SUMMARY"
fi

# --- flamegraph index ---------------------------------------------------------

{
    echo ""
    echo "## Flamegraphs"
    echo ""
} >> "$SUMMARY"

fg_found=0
for fg in "$SESSION_DIR"/*/*.svg "$SESSION_DIR"/*/*.json.gz; do
    [[ -f "$fg" ]] || continue
    fg_found=1
    # Path relative to the session dir so the link is portable.
    rel="${fg#"$SESSION_DIR"/}"
    echo "- [\`$rel\`]($rel)" >> "$SUMMARY"
done
if [[ $fg_found -eq 0 ]]; then
    echo "_(no flamegraph artifacts found)_" >> "$SUMMARY"
fi

# --- footer -------------------------------------------------------------------

{
    echo ""
    echo "## How to read this"
    echo ""
    echo "- **p50 / p95 / p99 / p999** are end-to-end SDK-measured latencies. They include gRPC marshaling, network hop, server dispatch, and full operation execution."
    echo "- **Pure presets** (\`throughput-writes\`, \`entity-reads\`, \`relationship-writes\`, \`relationship-reads\`) isolate one hot path each — use them to spot path-specific regressions."
    echo "- **Mixed presets** (\`mixed-rw\`, \`check-heavy\`) exercise realistic load shape; use them to catch regressions that only surface under mixed pressure (e.g., write-write contention slowing reads)."
    echo "- **Flamegraphs** in each workload subdir show where CPU time was spent — open the \`.svg\` files in a browser and drill into the wide boxes."
    echo ""
    echo "To diff two sessions, \`diff\` the \`summary.md\` files."
} >> "$SUMMARY"

echo "wrote $SUMMARY"
