#!/bin/bash
# Local Leader Slowness Experiment
# Tests: Performance when leaders are artificially slow
# Varies: non_responsive_num in {0, 1}; timer_length in {10, 100} ms
# Protocols: HS, HS-2, HS-1, HS-1-SLOT

set -e
cd "$(dirname "$0")"

PROTOCOLS=("HS" "HS-2" "HS-1" "HS-1-SLOT")
SLOW_COUNTS=(0 1)
DELAYS=(10 100)
REPLICAS=4

mkdir -p plot_data_local/leader_slowness

echo "=========================================="
echo "  Local Leader Slowness Experiment"
echo "=========================================="

for delay in "${DELAYS[@]}"; do
  for protocol in "${PROTOCOLS[@]}"; do
    for num_slow in "${SLOW_COUNTS[@]}"; do
      echo ""
      echo ">>> Protocol: $protocol | SlowLeaders: $num_slow | Delay: ${delay}ms"
      echo "-------------------------------------------"

      cmd=$(python3 -c "
import local_experiment as le
info = le.get_protocol_info('$protocol')
le.generate_config(info['config'], max_process_txn=info['max_process_txn'], non_responsive_num=$num_slow, timer_length=$delay)
conf = le.generate_local_conf($REPLICAS)
print(info['script'] + ' ' + conf)
")

      eval "$cmd" || true

      if [ -f results.log ]; then
        cp results.log "plot_data_local/leader_slowness/${protocol}_delay${delay}_slow${num_slow}.log"
      fi
    done
  done
done

echo ""
echo "=========================================="
echo "  Leader Slowness Experiment Complete"
echo "  Results in: plot_data_local/leader_slowness/"
echo "=========================================="
