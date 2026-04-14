#!/bin/bash
# Run 20 TD-HotStuff experiments with random weights + 5 baseline protocols, all at n=10.
set -e
cd "$(dirname "$0")"

DURATION="${DURATION:-20}"
N=10
RESULTS_ROOT="./protocol_comparison_results"
rm -rf "$RESULTS_ROOT"
mkdir -p "$RESULTS_ROOT/td_hs_random" "$RESULTS_ROOT/baselines"

# ── Part 1: 20 TD-HotStuff runs with random weights (seeds 1..20) ────────
echo "========================================"
echo "  Part 1: TD-HotStuff × 20 (random weights, n=$N)"
echo "========================================"

for seed in $(seq 1 20); do
  SEED=$seed \
  DURATION=$DURATION \
  RESULTS_DIR="$RESULTS_ROOT/td_hs_random" \
    ./weighted_experiment.sh "$N" "seed_${seed}" > /dev/null 2>&1
  # Print a compact one-liner summary for each run
  resdir="$RESULTS_ROOT/td_hs_random/seed_${seed}"
  if [ -f "$resdir/results.log" ]; then
    weights=$(grep "^weights=" "$resdir/weights.txt" | cut -d= -f2)
    avg=$(grep "average throughput" "$resdir/results.log" | awk -F: '{printf "%.0f", $2}')
    lat=$(grep "average latency" "$resdir/results.log" | awk -F: '{printf "%.4f", $2}')
    printf "  seed=%-3s weights=[%-30s] avg_tps=%-8s lat=%sms\n" "$seed" "$weights" "$avg" "$lat"
  else
    echo "  seed=$seed FAILED"
  fi
done

# ── Part 2: Baseline protocols at n=10 ───────────────────────────────────
echo ""
echo "========================================"
echo "  Part 2: Baseline protocols (n=$N)"
echo "========================================"

run_baseline() {
  local protocol="$1"
  local outdir="$RESULTS_ROOT/baselines/${protocol}"
  mkdir -p "$outdir"

  echo "--- $protocol ---"

  # Reuse local_experiment.py helpers but with n=$N instead of the default 4
  python3 -c "
import local_experiment as le
info = le.get_protocol_info('$protocol')
le.generate_config(info['config'], max_process_txn=info['max_process_txn'])
le.generate_local_conf($N)
import sys
sys.stdout.write(info['script'])
" > /tmp/_script_path

  # Unset the TD-HS weights env var so it doesn't leak between runs
  unset TD_HS_WEIGHTS

  killall -9 kv_server_performance 2>/dev/null || true
  killall -9 kv_service_tools 2>/dev/null || true
  # Also kill any protocol-specific binaries
  for bin in hs_performance hs1_performance hs2_performance slot_hs1_performance pbft_performance; do
    killall -9 "$bin" 2>/dev/null || true
  done
  sleep 2

  script_path=$(cat /tmp/_script_path)
  BENCH_DURATION=$DURATION $script_path ./config/performance_local_gen.conf \
    > "$outdir/stdout.log" 2>&1 || true

  if [ -f results.log ]; then
    cp results.log "$outdir/results.log"
  fi

  # Collect per-node logs
  for logfile in resilientdb_app/*/*.log; do
    if [ -f "$logfile" ]; then
      idx=$(basename "$(dirname "$logfile")")
      bn=$(basename "$logfile")
      cp "$logfile" "$outdir/node_${idx}_${bn}" 2>/dev/null || true
    fi
  done

  # Compact summary
  if [ -f "$outdir/results.log" ]; then
    avg=$(grep "average throughput" "$outdir/results.log" | awk -F: '{printf "%.0f", $2}')
    max=$(grep "max throughput" "$outdir/results.log" | awk -F: '{printf "%.0f", $2}')
    lat=$(grep "average latency" "$outdir/results.log" | awk -F: '{printf "%.4f", $2}')
    printf "    avg_tps=%-8s max_tps=%-8s lat=%sms\n" "$avg" "$max" "$lat"
  fi
}

for protocol in HS HS-1 HS-2 HS-1-SLOT PBFT; do
  run_baseline "$protocol"
done

echo ""
echo "========================================"
echo "  All runs complete. Results in: $RESULTS_ROOT"
echo "========================================"
