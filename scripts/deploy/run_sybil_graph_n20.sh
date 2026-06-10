#!/bin/bash
# TD-Hotstuff Sybil graph cluster experiment for n=20.
# TD_HS_SYBIL_GRAPH_ATTACK_IDS is used only by deploy_multi.sh to inject the
# replica-local TD_HS_SYBIL_GRAPH_ATTACK=1 flag into selected faulty processes.
# Clients, ordinary replicas, the adapter, and the plugin never receive the
# ID list or fixed reviewer list.

set -o pipefail
DEPLOY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DEPLOY_DIR"

. ./script/env.sh
. ./td_hotstuff_stable_env.sh

RESULT_DIR="$DEPLOY_DIR/experiment_results/sybil_graph_n20"
mkdir -p "$RESULT_DIR"

LOCAL_IP="10.10.131.205"
SERVERS="10.10.131.224 10.10.131.247 10.10.131.86 10.10.131.125 10.10.131.83"
SLEEP_TIME="${SLEEP_TIME:-90}"
N=20
MEAN_DELAY_MS=0

if [ -n "${SYBIL_GRAPH_COUNTS_OVERRIDE:-}" ]; then
  read -r -a SYBIL_GRAPH_COUNTS <<< "${SYBIL_GRAPH_COUNTS_OVERRIDE}"
else
  SYBIL_GRAPH_COUNTS=(0 8 10 12)
fi
if [ -n "${SYBIL_GRAPH_MODES_OVERRIDE:-}" ]; then
  read -r -a SYBIL_GRAPH_MODES <<< "${SYBIL_GRAPH_MODES_OVERRIDE}"
else
  SYBIL_GRAPH_MODES=(off on)
fi

CONFIG_FILES_TO_RESTORE=(
  "config/td_hotstuff.config"
  "config/performance.conf"
)
. ./script/generated_config_restore.sh
backup_generated_configs
trap restore_generated_configs EXIT

kill_nodes() {
  killall -9 kv_server_performance 2>/dev/null || true
  for ip in $SERVERS; do
    ssh -o StrictHostKeyChecking=no hyperchain@$ip "killall -9 kv_server_performance" 2>/dev/null &
  done
  wait
  sleep 1
}

cleanup_all() {
  killall -9 kv_server_performance 2>/dev/null || true
  rm -rf "$DEPLOY_DIR/resilientdb_app"
  for ip in $SERVERS; do
    ssh -o StrictHostKeyChecking=no hyperchain@$ip "killall -9 kv_server_performance; rm -rf ~/resilientdb_app" 2>/dev/null &
  done
  wait
  sleep 1
}

collect_logs() {
  rm -rf result_*_log result_*_reputation.jsonl result_*_qc_evidence.jsonl
  for ip in $SERVERS; do
    for node_id in $(ssh -o StrictHostKeyChecking=no hyperchain@$ip "ls ~/resilientdb_app/ 2>/dev/null | grep -E '^[0-9]+$'" 2>/dev/null); do
      scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/kv_server_performance.log result_${node_id}_log 2>/dev/null &
      scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/'td_hotstuff_reputation_node_'*.jsonl result_${node_id}_reputation.jsonl 2>/dev/null || true &
      scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/'td_hotstuff_qc_evidence_node_'*.jsonl result_${node_id}_qc_evidence.jsonl 2>/dev/null || true &
    done
  done
  for d in "$DEPLOY_DIR"/resilientdb_app/*/; do
    [ -d "$d" ] || continue
    local node_id
    node_id=$(basename "$d")
    if [ -f "$d/kv_server_performance.log" ]; then
      cp "$d/kv_server_performance.log" result_${node_id}_log 2>/dev/null &
      cp "$d"/td_hotstuff_reputation_node_*.jsonl result_${node_id}_reputation.jsonl 2>/dev/null || true &
      cp "$d"/td_hotstuff_qc_evidence_node_*.jsonl result_${node_id}_qc_evidence.jsonl 2>/dev/null || true &
    fi
  done
  wait
}

derive_sybil_reviewer_ids() {
  local bad_count="$1"
  local mode="${SYBIL_GRAPH_REVIEWER_MODE:-internal}"
  local group_size="${TD_HS_SYBIL_GRAPH_REVIEWER_GROUP_SIZE:-}"
  if [ -z "$group_size" ]; then
    if [ "$mode" = "clean" ]; then
      group_size=$((N - bad_count))
    else
      group_size=$bad_count
    fi
  fi
  local ids=""
  local chosen=0
  local start=1
  if [ "$mode" = "clean" ]; then
    start=$((bad_count + 1))
  fi
  for ((i=start; i<=N && chosen<group_size; i++)); do
    if [ -n "$ids" ]; then ids="${ids},${i}"; else ids="${i}"; fi
    chosen=$((chosen + 1))
  done
  if [ "$mode" = "clean" ] && [ "$chosen" -lt "$group_size" ]; then
    for ((i=1; i<=bad_count && chosen<group_size; i++)); do
      if [ -n "$ids" ]; then ids="${ids},${i}"; else ids="${i}"; fi
      chosen=$((chosen + 1))
    done
  fi
  printf '%s' "$ids"
}

derive_bad_node_ids() {
  local count="$1"
  local ids=""
  for ((i=1; i<=count; i++)); do
    if [ -n "$ids" ]; then
      ids="${ids},${i}"
    else
      ids="${i}"
    fi
  done
  printf '%s' "$ids"
}

run_single_experiment() {
  local mode="$1"
  local num_bad="$2"
  local bad_node_ids
  bad_node_ids=$(derive_bad_node_ids "$num_bad")
  local sybil_reviewer_ids
  sybil_reviewer_ids=$(derive_sybil_reviewer_ids "$num_bad")
  local sybil_graph_enabled=0
  if [ "$mode" = "on" ]; then
    sybil_graph_enabled=1
  fi
  local result_file="$RESULT_DIR/TD-Hotstuff_sybil_graph_${mode}_sybil${num_bad}.txt"

  echo ""
  echo "======================================================================"
  echo "  EXPERIMENT: SybilGraph=$mode sybil_bad=$num_bad ids=${bad_node_ids:-none} n=$N"
  echo "======================================================================"

  cleanup_all

  python3 -c "
from network_delay_experiment import generate_config, generate_performance_server_conf
generate_config(config_path='./config/td_hotstuff.config',
                max_process_txn=5,
                network_delay_num=0,
                mean_network_delay=$MEAN_DELAY_MS)
generate_performance_server_conf($N)
"

  export TEMPLATE_PATH=$PWD/config/td_hotstuff.config
  export server=//benchmark/protocols/td_hotstuff:kv_server_performance
  export TD_HS_REPUTATION_ENABLE=1
  export TD_HS_REPUTATION_PEERTRUST_ENABLE=0
  export TD_HS_REPUTATION_SYBIL_GRAPH_ENABLE="$sybil_graph_enabled"
  export TD_HS_REPUTATION_SYBIL_GRAPH_MIN_EDGES=1
  export TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_INCREMENT=30
  export TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_RECOVERY=5
  export TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_TRIGGER_SCORE=75
  export TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE=1
  export TD_HS_WEIGHT_UPDATE_ENABLE=1
  export TD_HS_STRONG_FAULT_ENABLE=0
  export TD_HS_REPUTATION_WINDOW_SIZE=64
  export TD_HS_REPUTATION_MIN_CANDIDATE_QCS=16
  export TD_HS_REPUTATION_MIN_LEADER_OPPORTUNITIES=1
  export TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS=64
  export TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY=1
  export TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS=32
  export TD_HS_LEADER_SELECTION_ENABLE=1
  export TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT=10
  export TD_HS_BENCHMARK_DYNAMIC_ROUTING_ENABLE=1
  export TD_HS_EVIDENCE_ENABLE=0
  export TD_HS_TIMEOUT_ENABLE=0
  export TD_HS_BAD_NODE_COUNT="$num_bad"
  export TD_HS_BAD_NODE_IDS="$bad_node_ids"
  if [ "$num_bad" -gt 0 ]; then
    export TD_HS_SYBIL_GRAPH_ATTACK_IDS="$bad_node_ids"
    export TD_HS_SYBIL_GRAPH_REVIEWER_IDS="$sybil_reviewer_ids"
  else
    unset TD_HS_SYBIL_GRAPH_ATTACK_IDS
    unset TD_HS_SYBIL_GRAPH_REVIEWER_IDS
  fi

  bash ./script/deploy_multi.sh "./config/performance.conf" 2>&1 | grep -E "(=== |Phase|deployed|started|ready|running)"

  echo "  Running benchmark client..."
  for((i=1;;i++)); do
    cf=$PWD/config_out/client${i}.config
    if [ ! -f "$cf" ]; then break; fi
    env -u TD_HS_SILENT_LEADER_IDS \
        -u TD_HS_SYBIL_GRAPH_ATTACK_IDS \
        -u TD_HS_SYBIL_GRAPH_REVIEWER_IDS \
        -u TD_HS_DOUBLE_PROPOSAL_IDS -u TD_HS_DOUBLE_VOTE_IDS \
        -u TD_HS_INVALID_QC_IDS \
        -u TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS \
        -u TD_HS_TIMEOUT_VOTE_EQUIVOCATION_IDS \
        -u TD_HS_INVALID_TC_PROPOSAL_IDS \
        -u TD_HS_BAD_NODE_IDS -u TD_HS_BAD_NODE_COUNT \
        ${BAZEL_WORKSPACE_PATH}/bazel-bin/benchmark/protocols/pbft/kv_service_tools "$cf" 2>/dev/null
  done

  echo "  Sleeping ${SLEEP_TIME}s..."
  sleep "$SLEEP_TIME"

  kill_nodes
  collect_logs

  local log_files
  log_files=$(ls result_*_log 2>/dev/null)
  if [ -n "$log_files" ]; then
    TD_HS_BAD_NODE_COUNT="$num_bad" TD_HS_BAD_NODE_IDS="$bad_node_ids" \
      TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT="${TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT}" \
      python3 performance/calculate_result.py $log_files > "$result_file" 2>&1
    local tps lat
    tps=$(grep "^[0-9]" "$result_file" | head -1)
    lat=$(grep "^[0-9]" "$result_file" | tail -1)
    echo "  >> Throughput: $tps txn/s | Latency: $lat s"
    local keep_dir="$RESULT_DIR/logs_TD-Hotstuff_sybil_graph_${mode}_sybil${num_bad}"
    rm -rf "$keep_dir"
    mkdir -p "$keep_dir"
    cp result_*_log "$keep_dir"/ 2>/dev/null || true
    cp result_*_reputation.jsonl "$keep_dir"/ 2>/dev/null || true
    cp result_*_qc_evidence.jsonl "$keep_dir"/ 2>/dev/null || true
    echo "  >> Logs preserved in: $keep_dir"
    rm -rf result_*_log result_*_reputation.jsonl result_*_qc_evidence.jsonl
  else
    echo "  >> NO LOG FILES COLLECTED"
    echo "0" > "$result_file"
    echo "0" >> "$result_file"
  fi

  unset TD_HS_SYBIL_GRAPH_ATTACK_IDS
  unset TD_HS_SYBIL_GRAPH_REVIEWER_IDS
}

echo "=== Building TD-Hotstuff benchmark binaries ==="
bazel build //benchmark/protocols/td_hotstuff:kv_server_performance \
            //benchmark/protocols/pbft:kv_service_tools 2>&1 | tail -5

for mode in "${SYBIL_GRAPH_MODES[@]}"; do
  for num_bad in "${SYBIL_GRAPH_COUNTS[@]}"; do
    run_single_experiment "$mode" "$num_bad"
  done
done

cleanup_all
echo ""
echo "======================================================================"
echo "  SYBIL_GRAPH CLIQUE EXPERIMENTS COMPLETE (n=$N)"
echo "  Results in: $RESULT_DIR/"
echo "======================================================================"
ls -la "$RESULT_DIR/"
