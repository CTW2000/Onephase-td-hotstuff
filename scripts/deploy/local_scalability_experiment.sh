#!/bin/bash
# Local Scalability Experiment
# Tests: How throughput/latency scale with replica count on localhost
# Varies: replica_number in {4, 7, 10}
# Protocols: HS, HS-2, HS-1, HS-1-SLOT

set +e
cd "$(dirname "$0")"

PROTOCOLS=("HS" "HS-2" "HS-1" "HS-1-SLOT")
REPLICA_COUNTS=(5 10 15)

mkdir -p plot_data_local/scalability_throughput
mkdir -p plot_data_local/scalability_latency

echo "=========================================="
echo "  Local Scalability Experiment"
echo "=========================================="

for protocol in "${PROTOCOLS[@]}"; do
  for n in "${REPLICA_COUNTS[@]}"; do
    echo ""
    echo ">>> Protocol: $protocol | Replicas: $n"
    echo "-------------------------------------------"

    # Generate config and local conf via Python
    cmd=$(python3 -c "
import local_experiment as le
info = le.get_protocol_info('$protocol')
le.generate_config(info['config'], max_process_txn=info['max_process_txn'])
conf = le.generate_local_conf($n)
print(info['script'] + ' ' + conf)
")

    eval "$cmd" || true

    # Extract results
    if [ -f results.log ]; then
      cp results.log "plot_data_local/scalability_throughput/${protocol}_${n}.log"
      echo "  -> Saved to plot_data_local/scalability_throughput/${protocol}_${n}.log"
    fi
  done
done

echo ""
echo "=========================================="
echo "  Scalability Experiment Complete"
echo "  Results in: plot_data_local/scalability_throughput/"
echo "=========================================="
