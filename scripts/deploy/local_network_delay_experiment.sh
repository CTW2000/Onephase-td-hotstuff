#!/bin/bash
# Local Network Delay Experiment
# Tests: Tolerance to artificial delay on a subset of replicas
# Varies: mean_network_delay in {1, 50} ms; network_delay_num in {0, 1, 2}
# Protocols: HS, HS-2, HS-1, HS-1-SLOT

set -e
cd "$(dirname "$0")"

PROTOCOLS=("HS" "HS-2" "HS-1" "HS-1-SLOT")
DELAYS=(1 50)
IMPACTED_COUNTS=(0 1 2)
REPLICAS=4

mkdir -p plot_data_local/network_delay

echo "=========================================="
echo "  Local Network Delay Experiment"
echo "=========================================="

for delay in "${DELAYS[@]}"; do
  for protocol in "${PROTOCOLS[@]}"; do
    for num_impacted in "${IMPACTED_COUNTS[@]}"; do
      echo ""
      echo ">>> Protocol: $protocol | Delay: ${delay}ms | Impacted: $num_impacted"
      echo "-------------------------------------------"

      cmd=$(python3 -c "
import local_experiment as le
info = le.get_protocol_info('$protocol')
le.generate_config(info['config'], max_process_txn=info['max_process_txn'], network_delay_num=$num_impacted, mean_network_delay=$delay)
conf = le.generate_local_conf($REPLICAS)
print(info['script'] + ' ' + conf)
")

      eval "$cmd" || true

      if [ -f results.log ]; then
        cp results.log "plot_data_local/network_delay/${protocol}_delay${delay}_impacted${num_impacted}.log"
      fi
    done
  done
done

echo ""
echo "=========================================="
echo "  Network Delay Experiment Complete"
echo "  Results in: plot_data_local/network_delay/"
echo "=========================================="
