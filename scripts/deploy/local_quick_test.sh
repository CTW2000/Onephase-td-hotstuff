#!/bin/bash
# Quick local test — run a single protocol with 4 replicas for 40 seconds
# Usage: ./local_quick_test.sh [PROTOCOL]
# PROTOCOL: HS, HS-1, HS-2, HS-1-SLOT, PBFT (default: HS-1)

set -e
cd "$(dirname "$0")"

PROTOCOL="${1:-HS-1}"

echo "=========================================="
echo "  Quick Local Test: $PROTOCOL"
echo "=========================================="

cmd=$(python3 -c "
import local_experiment as le
info = le.get_protocol_info('$PROTOCOL')
le.generate_config(info['config'], max_process_txn=info['max_process_txn'])
conf = le.generate_local_conf(4)
print(info['script'] + ' ' + conf)
")

echo "Running: $cmd"
eval "$cmd"

echo ""
echo "=========================================="
echo "  Quick Test Complete"
echo "=========================================="
