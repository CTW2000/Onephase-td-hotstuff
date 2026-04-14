#!/bin/bash
#
# weighted_experiment.sh — reusable runner for TD-HotStuff experiments with
# non-uniform (random or explicit) per-replica weights.
#
# Usage:
#   ./weighted_experiment.sh <n> [label]
#
# Required:
#   n        Number of replicas (>= 4, since TD-HS assumes n >= 3f+1 with f >= 1)
#
# Optional positional:
#   label    Human-readable label for this run (default: auto-generated)
#
# Optional environment variables:
#   WEIGHTS        Explicit comma-separated weights (overrides random gen).
#                  Length must equal n. Example: WEIGHTS="3,2,2,1,1"
#   MIN_WEIGHT     Minimum random weight (default: 1)
#   MAX_WEIGHT     Maximum random weight (default: 3)
#   SEED           RNG seed for reproducible random weights (default: $RANDOM)
#   DURATION       Benchmark duration in seconds (default: 30)
#   RESULTS_DIR    Output directory (default: ./weighted_experiment_results)
#
# Examples:
#   ./weighted_experiment.sh 10
#   SEED=42 ./weighted_experiment.sh 10 reproducible_run
#   WEIGHTS="3,3,3,2,2,2,1,1,1,1" ./weighted_experiment.sh 10 graduated
#   MAX_WEIGHT=5 DURATION=60 ./weighted_experiment.sh 7
#
# Output:
#   ${RESULTS_DIR}/<label>/
#     ├── results.log        throughput + latency summary
#     ├── weights.txt        the weight vector used
#     ├── stdout.log         benchmark stdout
#     └── node_<i>.log       per-node server logs
#
set -euo pipefail
cd "$(dirname "$0")"

# ── Parse args ────────────────────────────────────────────────────────────
if [ $# -lt 1 ]; then
  sed -n '3,/^set -e/p' "$0" | sed 's/^# \?//; /^set -e/d' >&2
  exit 1
fi

N="$1"
LABEL="${2:-}"

if ! [[ "$N" =~ ^[0-9]+$ ]] || [ "$N" -lt 4 ]; then
  echo "Error: n must be an integer >= 4 (got '$N')" >&2
  exit 1
fi

MIN_WEIGHT="${MIN_WEIGHT:-1}"
MAX_WEIGHT="${MAX_WEIGHT:-3}"
SEED="${SEED:-$RANDOM}"
DURATION="${DURATION:-30}"
RESULTS_DIR="${RESULTS_DIR:-./weighted_experiment_results}"

# ── Resolve weights (explicit or random) ──────────────────────────────────
if [ -n "${WEIGHTS:-}" ]; then
  # Validate user-supplied weights
  wcount=$(echo "$WEIGHTS" | awk -F',' '{print NF}')
  if [ "$wcount" -ne "$N" ]; then
    echo "Error: WEIGHTS has $wcount entries, expected $N" >&2
    exit 1
  fi
  FINAL_WEIGHTS="$WEIGHTS"
  SOURCE="explicit"
else
  # Generate random weights via python for portability
  FINAL_WEIGHTS=$(python3 -c "
import random
random.seed($SEED)
ws = [random.randint($MIN_WEIGHT, $MAX_WEIGHT) for _ in range($N)]
print(','.join(str(w) for w in ws))
")
  SOURCE="random seed=$SEED range=[$MIN_WEIGHT,$MAX_WEIGHT]"
fi

# Compute W and threshold for reporting
read TOTAL THRESHOLD <<<"$(python3 -c "
ws = [int(x) for x in '$FINAL_WEIGHTS'.split(',')]
W = sum(ws)
T = (2 * W) // 3 + 1
print(W, T)
")"

# Auto-generate label if not given
if [ -z "$LABEL" ]; then
  LABEL="n${N}_$(echo "$FINAL_WEIGHTS" | tr ',' '_')_seed${SEED}"
fi

OUT_DIR="${RESULTS_DIR}/${LABEL}"
mkdir -p "$OUT_DIR"

echo "========================================"
echo "  Weighted experiment: $LABEL"
echo "  n=$N  duration=${DURATION}s"
echo "  weights=[$FINAL_WEIGHTS]"
echo "  W=$TOTAL  threshold=$THRESHOLD  source=$SOURCE"
echo "  output=$OUT_DIR"
echo "========================================"

# Save weight vector so the run is self-describing
cat > "$OUT_DIR/weights.txt" <<EOF
n=$N
weights=$FINAL_WEIGHTS
total_weight=$TOTAL
threshold=$THRESHOLD
source=$SOURCE
duration_seconds=$DURATION
EOF

# ── Run the benchmark ─────────────────────────────────────────────────────
export TD_HS_WEIGHTS="$FINAL_WEIGHTS"
export BENCH_DURATION="$DURATION"

# Generate TD-HS config and local IP list (N replicas + 1 client)
python3 -c "
import local_experiment as le
info = le.get_protocol_info('TD-HS')
le.generate_config(info['config'], max_process_txn=info['max_process_txn'])
le.generate_local_conf($N)
"

# Clean any leftover processes from earlier runs
killall -9 kv_server_performance 2>/dev/null || true
killall -9 kv_service_tools 2>/dev/null || true
sleep 2

./performance_local/td_hotstuff_performance.sh \
  ./config/performance_local_gen.conf > "$OUT_DIR/stdout.log" 2>&1

# ── Collect artifacts ─────────────────────────────────────────────────────
if [ -f results.log ]; then
  cp results.log "$OUT_DIR/results.log"
fi

for logfile in resilientdb_app/*/kv_server_performance.log; do
  if [ -f "$logfile" ]; then
    idx=$(basename "$(dirname "$logfile")")
    cp "$logfile" "$OUT_DIR/node_${idx}.log"
  fi
done

# ── Print summary ─────────────────────────────────────────────────────────
echo ""
echo "=== Summary ==="
if [ -f "$OUT_DIR/results.log" ]; then
  cat "$OUT_DIR/results.log"
else
  echo "No results.log produced — check $OUT_DIR/stdout.log"
fi

echo ""
echo "TPS pattern (node 1, per 5s window):"
if [ -f "$OUT_DIR/node_1.log" ]; then
  grep -a "txn:" "$OUT_DIR/node_1.log" | grep -oP 'txn:\K[0-9]+' | tr '\n' ','
  echo ""
fi

echo ""
echo "Artifacts in: $OUT_DIR"
