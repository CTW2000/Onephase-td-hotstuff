#!/bin/bash
# Run all local experiments sequentially
# Results saved to plot_data_local/

set -e
cd "$(dirname "$0")"

echo "============================================================"
echo "  Running ALL Local Experiments"
echo "  This will take a while (~30+ minutes)"
echo "============================================================"
echo ""

echo "[1/6] Scalability Experiment..."
./local_scalability_experiment.sh
echo ""

echo "[2/6] Batching Experiment..."
./local_batching_experiment.sh
echo ""

echo "[3/6] Leader Slowness Experiment..."
./local_leader_slowness_experiment.sh
echo ""

echo "[4/6] Network Delay Experiment..."
./local_network_delay_experiment.sh
echo ""

echo "[5/6] Rollback Attack Experiment..."
./local_rollback_experiment.sh
echo ""

echo "[6/6] Tail-Forking Attack Experiment..."
./local_tailforking_experiment.sh
echo ""

echo "============================================================"
echo "  ALL Experiments Complete!"
echo "  Results in: plot_data_local/"
echo "============================================================"
ls -la plot_data_local/
