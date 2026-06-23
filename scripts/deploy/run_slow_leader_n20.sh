#!/bin/bash
# Slow Leader experiment for n=20 — all 5 protocols, sequential.
# Uniform clientBatchNum=100; 100ms leader timeout.

set -o pipefail
DEPLOY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DEPLOY_DIR"

. ./script/env.sh
. ./td_hotstuff_stable_env.sh

# Silent-leader experiments require the TD-Hotstuff timeout path; otherwise a
# silent leader can stall the view and the run measures a config mistake.
: "${TD_HS_SLOW_LEADER_TIMEOUT_ENABLE:=1}"
export TD_HS_TIMEOUT_ENABLE="$TD_HS_SLOW_LEADER_TIMEOUT_ENABLE"

RESULT_DIR="$DEPLOY_DIR/experiment_results/slow_leader_n20"
mkdir -p "$RESULT_DIR"

LOCAL_IP="10.10.131.205"
SERVERS="10.10.131.224 10.10.131.247 10.10.131.86 10.10.131.125 10.10.131.83"
SLEEP_TIME="${SLEEP_TIME:-150}"
if [ -n "${PROTOCOLS_OVERRIDE:-}" ]; then
  read -r -a PROTOCOLS <<< "${PROTOCOLS_OVERRIDE}"
else
  PROTOCOLS=("HS-1" "HS-2" "HS" "HS-1-SLOT" "TD-Hotstuff")
fi
if [ -n "${SLOW_COUNTS_OVERRIDE:-}" ]; then
  read -r -a SLOW_COUNTS <<< "${SLOW_COUNTS_OVERRIDE}"
else
  SLOW_COUNTS=(0 1 4 6)
fi
N=20
TIMER="${SLOW_LEADER_TIMER_MS:-100}"
export TD_HS_TIMEOUT_MS="$TIMER"

CONFIG_FILES_TO_RESTORE=(
  "config/hs1.config"
  "config/hs2.config"
  "config/hs.config"
  "config/slot_hs1.config"
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


derive_bad_node_ids() {
  local count="$1"
  local ids=""
  for ((bad_idx=0; bad_idx<count; bad_idx++)); do
    local bad_id=$((bad_idx * 3 + 1))
    if [ -n "$ids" ]; then
      ids="${ids},"
    fi
    ids="${ids}${bad_id}"
  done
  echo "$ids"
}

configure_td_hotstuff_reputation_pipeline() {
  export TD_HS_REPUTATION_ENABLE=1
  export TD_HS_WEIGHT_UPDATE_ENABLE=1
  export TD_HS_STRONG_FAULT_ENABLE=1
  export TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE=1
  export TD_HS_DOUBLE_VOTE_DETECT_ENABLE=1
  export TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE=1
  export TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE=1
  export TD_HS_CONFLICTING_QC_DETECT_ENABLE=1
  export TD_HS_STRONG_FAULT_TARGET_WEIGHT=1
  # Soft-fault experiments should show a conservative reputation curve.
  # Bad nodes start healthy, then lose recovery slowly after the attack begins.
  export TD_HS_REPUTATION_DECAY_PER_EPOCH="${TD_HS_SOFT_REPUTATION_DECAY_PER_EPOCH:-10}"
  export TD_HS_REPUTATION_MAX_RECOVERY_PER_EPOCH="${TD_HS_SOFT_REPUTATION_MAX_RECOVERY_PER_EPOCH:-10}"
  export TD_HS_REPUTATION_BONUS_PER_EPOCH=0
  export TD_HS_REPUTATION_WINDOW_SIZE="${TD_HS_SOFT_REPUTATION_WINDOW_SIZE:-256}"
  export TD_HS_REPUTATION_MIN_CANDIDATE_QCS="${TD_HS_SOFT_REPUTATION_MIN_CANDIDATE_QCS:-64}"
  export TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS="${TD_HS_SOFT_WEIGHT_UPDATE_EPOCH_VIEWS:-256}"
  export TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY="${TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY:-4}"
  export TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS="${TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS:-128}"
  export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS="${TD_HS_SLOW_LEADER_EMPTY_PROPOSAL_VIEWS:-20}"
  export TD_HS_SILENT_LEADER_START_VIEW="${TD_HS_SILENT_LEADER_START_VIEW:-${TD_HS_SOFT_ATTACK_START_VIEW:-8192}}"
  export TD_HS_LEADER_SELECTION_ENABLE=1
  export TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT=10
  export TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE=1
  export TD_HS_REPUTATION_AUDIT_JSONL_ENABLE=1
}

clear_td_hotstuff_reputation_pipeline() {
  unset TD_HS_REPUTATION_ENABLE TD_HS_WEIGHT_UPDATE_ENABLE
  unset TD_HS_STRONG_FAULT_ENABLE TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE
  unset TD_HS_DOUBLE_VOTE_DETECT_ENABLE
  unset TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE
  unset TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE
  unset TD_HS_CONFLICTING_QC_DETECT_ENABLE
  unset TD_HS_STRONG_FAULT_TARGET_WEIGHT
  unset TD_HS_REPUTATION_DECAY_PER_EPOCH
  unset TD_HS_REPUTATION_MAX_RECOVERY_PER_EPOCH
  unset TD_HS_REPUTATION_BONUS_PER_EPOCH
  unset TD_HS_REPUTATION_WINDOW_SIZE TD_HS_REPUTATION_MIN_CANDIDATE_QCS
  unset TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS
  unset TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY
  unset TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS
  unset TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS
  unset TD_HS_LEADER_SELECTION_ENABLE TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT
  unset TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE
  unset TD_HS_REPUTATION_AUDIT_JSONL_ENABLE
  unset TD_HS_SILENT_LEADER_START_VIEW
}

cleanup_all() {
  killall -9 kv_server_performance 2>/dev/null || true
  rm -rf $DEPLOY_DIR/resilientdb_app
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
      if [ "${KEEP_EXPERIMENT_LOGS:-0}" = "1" ]; then
        scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/'td_hotstuff_reputation_node_'*.jsonl result_${node_id}_reputation.jsonl 2>/dev/null || true &
        scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/'td_hotstuff_qc_evidence_node_'*.jsonl result_${node_id}_qc_evidence.jsonl 2>/dev/null || true &
      fi
    done
  done
  for d in $DEPLOY_DIR/resilientdb_app/*/; do
    local node_id=$(basename "$d")
    if [ -f "$d/kv_server_performance.log" ]; then
      cp "$d/kv_server_performance.log" result_${node_id}_log 2>/dev/null &
      if [ "${KEEP_EXPERIMENT_LOGS:-0}" = "1" ]; then
        cp "$d"/td_hotstuff_reputation_node_*.jsonl result_${node_id}_reputation.jsonl 2>/dev/null || true &
        cp "$d"/td_hotstuff_qc_evidence_node_*.jsonl result_${node_id}_qc_evidence.jsonl 2>/dev/null || true &
      fi
    fi
  done
  wait
}

run_single_experiment() {
  local protocol="$1"
  local experiment_name="$2"
  local result_file="$3"
  local config_file="$4"

  echo ""
  echo "======================================================================"
  echo "  EXPERIMENT: $experiment_name | Protocol: $protocol"
  echo "======================================================================"

  cleanup_all

  case "$protocol" in
    "HS-1")      export TEMPLATE_PATH=$PWD/config/hs1.config;      export server=//benchmark/protocols/hs1:kv_server_performance;;
    "HS-2")      export TEMPLATE_PATH=$PWD/config/hs2.config;      export server=//benchmark/protocols/hs2:kv_server_performance;;
    "HS")        export TEMPLATE_PATH=$PWD/config/hs.config;       export server=//benchmark/protocols/hs:kv_server_performance;;
    "HS-1-SLOT") export TEMPLATE_PATH=$PWD/config/slot_hs1.config; export server=//benchmark/protocols/slot_hs1:kv_server_performance;;
    "TD-Hotstuff"|"TD-HotStuff"|"TD-HS") export TEMPLATE_PATH=$PWD/config/td_hotstuff.config; export server=//benchmark/protocols/td_hotstuff:kv_server_performance;;
  esac

  bash ./script/deploy_multi.sh "$config_file" 2>&1 | grep -E "(=== |Phase|deployed|started|ready|running)"

  if [ "${BENCHMARK_PRE_START_SLEEP:-0}" -gt 0 ] 2>/dev/null; then
    echo "  Waiting ${BENCHMARK_PRE_START_SLEEP}s for benchmark client channels to settle..."
    sleep "$BENCHMARK_PRE_START_SLEEP"
  fi

  echo "  Running benchmark..."
  for((i=1;;i++)); do
    cf=$PWD/config_out/client${i}.config
    if [ ! -f "$cf" ]; then break; fi
    env -u TD_HS_SILENT_LEADER_IDS -u TD_HS_DOUBLE_PROPOSAL_IDS -u TD_HS_DOUBLE_VOTE_IDS -u TD_HS_INVALID_QC_IDS \
        -u TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS \
        \
        \
        -u TD_HS_BAD_NODE_IDS -u TD_HS_BAD_NODE_COUNT \
        ${BAZEL_WORKSPACE_PATH}/bazel-bin/benchmark/protocols/pbft/kv_service_tools "$cf" 2>/dev/null
  done

  echo "  Sleeping ${SLEEP_TIME}s..."
  sleep $SLEEP_TIME

  kill_nodes
  collect_logs

  local log_files=$(ls result_*_log 2>/dev/null)
  if [ -n "$log_files" ]; then
    local bad_node_ids=""
    for ((bad_idx=0; bad_idx<num_slow; bad_idx++)); do
      local bad_id=$((bad_idx * 3 + 1))
      if [ -n "$bad_node_ids" ]; then
        bad_node_ids="${bad_node_ids},"
      fi
      bad_node_ids="${bad_node_ids}${bad_id}"
    done
    TD_HS_BAD_NODE_COUNT="${num_slow:-0}" TD_HS_BAD_NODE_IDS="$bad_node_ids" \
      python3 performance/calculate_result.py $log_files > "$result_file" 2>&1
    if [ "${KEEP_EXPERIMENT_LOGS:-0}" = "1" ]; then
      local log_dir="${result_file%.txt}_logs"
      rm -rf "$log_dir"
      mkdir -p "$log_dir"
      cp result_* "$log_dir"/ 2>/dev/null || true
    fi
    local tps=$(grep "^[0-9]" "$result_file" | head -1)
    local lat=$(grep "^[0-9]" "$result_file" | tail -1)
    echo "  >> Throughput: $tps txn/s | Latency: $lat s"
    rm -rf result_*_log result_*_reputation.jsonl result_*_qc_evidence.jsonl
  else
    echo "  >> NO LOG FILES COLLECTED"
    echo "0" > "$result_file"
    echo "0" >> "$result_file"
  fi
}

echo "=== Building all protocol binaries ==="
if ! bazel build //benchmark/protocols/hs1:kv_server_performance \
            //benchmark/protocols/hs2:kv_server_performance \
            //benchmark/protocols/hs:kv_server_performance \
            //benchmark/protocols/slot_hs1:kv_server_performance \
            //benchmark/protocols/td_hotstuff:kv_server_performance \
            //benchmark/protocols/pbft:kv_service_tools 2>&1 | tail -5; then
  echo "ERROR: build failed; aborting experiment"
  exit 1
fi

echo ""
echo "######################################################################"
echo "# SLOW LEADER EXPERIMENT | n=$N | timer=${TIMER}ms"
echo "######################################################################"

for num_slow in "${SLOW_COUNTS[@]}"; do
  for proto in "${PROTOCOLS[@]}"; do
    case "$proto" in
      "HS-1")      mpt=3; cfg="hs1";;
      "HS-2")      mpt=4; cfg="hs2";;
      "HS")        mpt=5; cfg="hs";;
      "HS-1-SLOT") mpt=3; cfg="slot_hs1";;
      "TD-Hotstuff"|"TD-HotStuff"|"TD-HS") mpt=5; cfg="td_hotstuff";;
    esac

    config_non_responsive_num=$num_slow
    if [[ "$proto" == "TD-Hotstuff" || "$proto" == "TD-HotStuff" || "$proto" == "TD-HS" ]]; then
      configure_td_hotstuff_reputation_pipeline
      # TD-Hotstuff silent-leader experiments use per-node
      # TD_HS_SILENT_LEADER=1 injection. Keep generic network-delay replicas
      # disabled so the benchmark measures only silent leaders, not slow voters.
      config_non_responsive_num=0
    else
      clear_td_hotstuff_reputation_pipeline
    fi

    python3 -c "
from leader_slowness_experiment import generate_config, generate_performance_server_conf
generate_config(config_path='./config/${cfg}.config',
                max_process_txn=$mpt, non_responsive_num=$config_non_responsive_num, timer_length=$TIMER)
generate_performance_server_conf($N)
"
    actual_slow=$(grep non_responsive_num ./config/${cfg}.config | grep -o '[0-9]\+')
    if [ "$actual_slow" != "$config_non_responsive_num" ]; then
      echo "ERROR: ${cfg}.config has non_responsive_num=$actual_slow, expected $config_non_responsive_num — ABORTING"
      exit 1
    fi

    if [[ "$proto" == "TD-Hotstuff" || "$proto" == "TD-HotStuff" || "$proto" == "TD-HS" ]]; then
      if [ "$num_slow" -gt 0 ]; then
        export TD_HS_SILENT_LEADER_IDS="$(derive_bad_node_ids "$num_slow")"
      else
        unset TD_HS_SILENT_LEADER_IDS
      fi
    else
      unset TD_HS_SILENT_LEADER_IDS
    fi

    result_file="$RESULT_DIR/${proto}_slow${num_slow}.txt"
    run_single_experiment "$proto" "SlowLeader slow=$num_slow timer=${TIMER}ms n=$N" "$result_file" "./config/performance.conf"
    unset TD_HS_SILENT_LEADER_IDS
  done
done

cleanup_all
echo ""
echo "======================================================================"
echo "  SLOW LEADER EXPERIMENTS COMPLETE (n=$N)"
echo "  Results in: $RESULT_DIR/"
echo "======================================================================"
ls -la "$RESULT_DIR/"
