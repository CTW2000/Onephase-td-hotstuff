#!/bin/bash
# Local Batching Experiment
# Tests: Effect of client batch size on throughput and latency
# Varies: clientBatchNum in {100, 500, 1000, 2000}
# Protocols: HS, HS-2, HS-1, HS-1-SLOT

set -e
cd "$(dirname "$0")"

PROTOCOLS=("HS" "HS-2" "HS-1" "HS-1-SLOT")
BATCH_SIZES=(100 500 1000 2000)
REPLICAS=4

mkdir -p plot_data_local/batching_throughput
mkdir -p plot_data_local/batching_latency

echo "=========================================="
echo "  Local Batching Experiment"
echo "=========================================="

for protocol in "${PROTOCOLS[@]}"; do
  for batch in "${BATCH_SIZES[@]}"; do
    echo ""
    echo ">>> Protocol: $protocol | BatchSize: $batch"
    echo "-------------------------------------------"

    cmd=$(python3 -c "
import local_experiment as le
info = le.get_protocol_info('$protocol')
le.generate_config(info['config'], max_process_txn=info['max_process_txn'], clientBatchNum=$batch, timer_length=100)
conf = le.generate_local_conf($REPLICAS)
print(info['script'] + ' ' + conf)
")

    eval "$cmd" || true

    if [ -f results.log ]; then
      cp results.log "plot_data_local/batching_throughput/${protocol}_${batch}.log"
    fi
  done
done

echo ""
echo "=========================================="
echo "  Batching Experiment Complete"
echo "  Results in: plot_data_local/batching_throughput/"
echo "=========================================="
