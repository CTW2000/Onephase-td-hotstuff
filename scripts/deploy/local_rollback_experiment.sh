#!/bin/bash
# Local Rollback Attack Experiment
# Tests: Performance under rollback attack (HS-1 and HS-1-SLOT only)
# Varies: rollback_num in {0, 1}; timer_length in {10, 100} ms

set -e
cd "$(dirname "$0")"

PROTOCOLS=("HS-1" "HS-1-SLOT")
FAULTY_COUNTS=(0 1)
DELAYS=(10 100)
REPLICAS=4

mkdir -p plot_data_local/rollback

echo "=========================================="
echo "  Local Rollback Attack Experiment"
echo "=========================================="

for delay in "${DELAYS[@]}"; do
  for protocol in "${PROTOCOLS[@]}"; do
    for num_faulty in "${FAULTY_COUNTS[@]}"; do
      echo ""
      echo ">>> Protocol: $protocol | Faulty: $num_faulty | Delay: ${delay}ms"
      echo "-------------------------------------------"

      cmd=$(python3 -c "
import local_experiment as le
info = le.get_protocol_info('$protocol')
le.generate_config(info['config'], max_process_txn=info['max_process_txn'], rollback_num=$num_faulty, timer_length=$delay)
conf = le.generate_local_conf($REPLICAS)
print(info['script'] + ' ' + conf)
")

      eval "$cmd" || true

      if [ -f results.log ]; then
        cp results.log "plot_data_local/rollback/${protocol}_delay${delay}_faulty${num_faulty}.log"
      fi
    done
  done
done

echo ""
echo "=========================================="
echo "  Rollback Experiment Complete"
echo "  Results in: plot_data_local/rollback/"
echo "=========================================="
