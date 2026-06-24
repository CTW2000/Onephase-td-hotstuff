#!/bin/bash
# Shared TD-Hotstuff strong-fault V2 experiment runner for n=20.
# The attack ID env var is launcher-only metadata. deploy_multi.sh converts it
# to one replica-local attack flag and scrubs it from shared/client env.

set -o pipefail
DEPLOY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DEPLOY_DIR"

. ./script/env.sh
. ./td_hotstuff_stable_env.sh

MODE_NAME="${MODE_NAME:?MODE_NAME is required}"
RESULT_NAME="${RESULT_NAME:?RESULT_NAME is required}"
ATTACK_IDS_ENV="${ATTACK_IDS_ENV:?ATTACK_IDS_ENV is required}"
DETECT_ENV="${DETECT_ENV:?DETECT_ENV is required}"
COUNTS="${COUNTS:-0 1 3}"

RESULT_DIR="$DEPLOY_DIR/experiment_results/${RESULT_NAME}_n20"
mkdir -p "$RESULT_DIR"

SERVERS="10.10.131.224 10.10.131.247 10.10.131.86 10.10.131.125 10.10.131.83"
SLEEP_TIME="${SLEEP_TIME:-90}"
N=20
MEAN_DELAY_MS=0

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
  local collect_timeout="${TD_HS_EXPERIMENT_LOG_COLLECTION_TIMEOUT_SECONDS:-15}s"
  for ip in $SERVERS; do
    for node_id in $(timeout "$collect_timeout" ssh -o BatchMode=yes -o StrictHostKeyChecking=no hyperchain@$ip "ls ~/resilientdb_app/ 2>/dev/null | grep -E '^[0-9]+$'" 2>/dev/null || true); do
      timeout "$collect_timeout" scp -o BatchMode=yes -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/kv_server_performance.log result_${node_id}_log 2>/dev/null || true &
      timeout "$collect_timeout" scp -o BatchMode=yes -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/'td_hotstuff_reputation_node_'*.jsonl result_${node_id}_reputation.jsonl 2>/dev/null || true &
      timeout "$collect_timeout" scp -o BatchMode=yes -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/'td_hotstuff_qc_evidence_node_'*.jsonl result_${node_id}_qc_evidence.jsonl 2>/dev/null || true &
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

client_load_started() {
  local client_log="$DEPLOY_DIR/resilientdb_app/$((N + 1))/kv_server_performance.log"
  if [ -f "$client_log" ] && grep -Eq "munmap_chunk|invalid pointer|double free|corrupted" "$client_log"; then
    return 1
  fi
  BENCHMARK_START_MIN_CLIENT_CALLS="${BENCHMARK_START_MIN_CLIENT_CALLS:-1000}" \
    python3 - "$DEPLOY_DIR/resilientdb_app" <<'PYCHECK'
import os
import re
import sys

min_calls = int(os.environ.get("BENCHMARK_START_MIN_CLIENT_CALLS", "1000"))
root = sys.argv[1]
for dirpath, _, filenames in os.walk(root):
    if "kv_server_performance.log" not in filenames:
        continue
    path = os.path.join(dirpath, "kv_server_performance.log")
    text = open(path, errors="ignore").read()
    if re.search(r"req client latency:\s*(?!-?nan)([0-9]+(?:\.[0-9]+)?)", text):
        sys.exit(0)
    for match in re.finditer(r"client call:([0-9]+)", text):
        if int(match.group(1)) >= min_calls:
            sys.exit(0)
sys.exit(1)
PYCHECK
}

start_benchmark_clients() {
  local max_attempts="${BENCHMARK_START_ATTEMPTS:-3}"
  local wait_seconds="${BENCHMARK_START_WAIT_SECONDS:-45}"
  local pre_start_sleep="${BENCHMARK_PRE_START_SLEEP:-20}"
  if [ "$pre_start_sleep" -gt 0 ]; then
    echo "  Waiting ${pre_start_sleep}s for benchmark client channels to settle..."
    sleep "$pre_start_sleep"
  fi
  local attempt
  for ((attempt=1; attempt<=max_attempts; attempt++)); do
    echo "  Starting benchmark clients (attempt ${attempt}/${max_attempts})..."
    for((i=1;;i++)); do
      cf=$PWD/config_out/client${i}.config
      if [ ! -f "$cf" ]; then break; fi
      env -u TD_HS_SILENT_LEADER_IDS \
          -u TD_HS_DOUBLE_PROPOSAL_IDS -u TD_HS_DOUBLE_VOTE_IDS \
          -u TD_HS_INVALID_QC_IDS \
          -u TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS \
          \
          \
          -u TD_HS_BAD_NODE_IDS -u TD_HS_BAD_NODE_COUNT \
          ${BAZEL_WORKSPACE_PATH}/bazel-bin/benchmark/protocols/pbft/kv_service_tools "$cf" \
          > /tmp/td_hs_kv_service_tools.out 2>&1
      local tool_rc=$?
      if [ -n "${BENCHMARK_TOOL_LOG:-}" ]; then
        {
          echo "attempt=${attempt} config=${cf} rc=${tool_rc}"
          sed 's/^/  /' /tmp/td_hs_kv_service_tools.out
        } >> "$BENCHMARK_TOOL_LOG"
      fi
    done

    local second
    for ((second=1; second<=wait_seconds; second++)); do
      sleep 1
      if client_load_started; then
        echo "  Benchmark client load is running."
        return 0
      fi
    done
    echo "  Benchmark client load did not start after ${wait_seconds}s."
  done
  return 1
}

result_has_valid_throughput() {
  local result_file="$1"
  python3 - "$result_file" <<'PYCHECK'
import math
import os
import re
import sys

text = open(sys.argv[1], errors="ignore").read()
numbers = [line.strip() for line in text.splitlines()
           if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", line.strip())]
if len(numbers) < 2:
    sys.exit(1)
try:
    throughput = float(numbers[-2])
except ValueError:
    sys.exit(1)
if not math.isfinite(throughput) or throughput <= 0:
    sys.exit(1)
min_stable_tps = float(os.environ.get("BENCHMARK_MIN_STABLE_TPS", "0") or "0")
if min_stable_tps > 0 and throughput < min_stable_tps:
    print(
        f"stable throughput {throughput} below BENCHMARK_MIN_STABLE_TPS "
        f"{min_stable_tps}",
        file=sys.stderr,
    )
    sys.exit(1)
match = re.search(r"stable positive throughput samples:\s*([0-9]+)", text)
if match and int(match.group(1)) == 0:
    sys.exit(1)
sys.exit(0)
PYCHECK
}

derive_bad_node_ids() {
  local count="$1"
  if [ -n "${STRONG_FAULT_BAD_NODE_IDS_OVERRIDE:-}" ]; then
    printf '%s' "${STRONG_FAULT_BAD_NODE_IDS_OVERRIDE}"
    return
  fi
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

node_ip_for_id() {
  local node_id="$1"
  if [ "$node_id" -lt 1 ] || [ "$node_id" -gt "$N" ]; then
    return 1
  fi
  local ips=($SERVERS)
  local idx=$(( (node_id - 1) / 4 ))
  printf '%s' "${ips[$idx]}"
}

arm_weight_update_vote_equivocation_nodes() {
  local ids_csv="$1"
  if [ "$ATTACK_IDS_ENV" != "TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS" ] ||
     [ -z "$ids_csv" ]; then
    return 0
  fi
  local ids=()
  IFS=',' read -r -a ids <<< "$ids_csv"
  local node_id ip
  for node_id in "${ids[@]}"; do
    ip=$(node_ip_for_id "$node_id") || continue
    ssh -o StrictHostKeyChecking=no hyperchain@$ip \
      "touch /home/hyperchain/resilientdb_app/${node_id}/td_hs_wue_trigger" \
      2>/dev/null &
  done
  wait
  echo "  Armed WUE trigger for nodes: $ids_csv"
}

run_single_experiment() {
  local num_bad="$1"
  local bad_node_ids
  bad_node_ids=$(derive_bad_node_ids "$num_bad")
  local result_file="$RESULT_DIR/TD-Hotstuff_${RESULT_NAME}${num_bad}.txt"
  local max_cell_attempts="${BENCHMARK_CELL_ATTEMPTS:-2}"
  local cell_attempt

  echo ""
  echo "======================================================================"
  echo "  EXPERIMENT: ${MODE_NAME} bad=$num_bad ids=${bad_node_ids:-none} n=$N"
  echo "======================================================================"

  for ((cell_attempt=1; cell_attempt<=max_cell_attempts; cell_attempt++)); do
    if [ "$max_cell_attempts" -gt 1 ]; then
      echo "  Cell deployment attempt ${cell_attempt}/${max_cell_attempts}"
    fi

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
  export TD_HS_WEIGHT_UPDATE_ENABLE=1
  export TD_HS_STRONG_FAULT_ENABLE=1
  export TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE=1
  export TD_HS_DOUBLE_VOTE_DETECT_ENABLE=1
  export TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE=1
  export TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE=1
  export TD_HS_CONFLICTING_QC_DETECT_ENABLE=1
  if [ "${TD_HS_ALL_DETECTORS_ENABLE:-0}" = "1" ]; then
    td_hs_enable_all_detectors
  else
    export "$DETECT_ENV=1"
  fi
  export TD_HS_STRONG_FAULT_TARGET_WEIGHT=1
  export TD_HS_STRONG_FAULT_ATTACK_START_VIEW="${TD_HS_STRONG_FAULT_ATTACK_START_VIEW:-1024}"
  export TD_HS_REPUTATION_WINDOW_SIZE="${TD_HS_REPUTATION_WINDOW_SIZE:-64}"
  export TD_HS_REPUTATION_MIN_CANDIDATE_QCS="${TD_HS_REPUTATION_MIN_CANDIDATE_QCS:-16}"
  export TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS="${TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS:-64}"
  export TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY="${TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY:-4}"
  export TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS="${TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS:-128}"
  export TD_HS_LEADER_SELECTION_ENABLE=1
  export TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT=10
  export TD_HS_BENCHMARK_DYNAMIC_ROUTING_ENABLE=1
  export TD_HS_BENCHMARK_RETRY_ENABLE="${TD_HS_BENCHMARK_RETRY_ENABLE:-1}"
  export TD_HS_BENCHMARK_REQUEST_TIMEOUT_MS="${TD_HS_BENCHMARK_REQUEST_TIMEOUT_MS:-100}"
  export TD_HS_REPUTATION_AUDIT_JSONL_ENABLE="${TD_HS_REPUTATION_AUDIT_JSONL_ENABLE:-0}"
  export TD_HS_EVIDENCE_ENABLE=0
  export TD_HS_TIMEOUT_ENABLE=1
  export TD_HS_TIMEOUT_MS=200
  export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS="${TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS:-20}"
  export TD_HS_BAD_NODE_COUNT="$num_bad"
  export TD_HS_BAD_NODE_IDS="$bad_node_ids"
  if [ "$num_bad" -gt 0 ]; then
    export "$ATTACK_IDS_ENV=$bad_node_ids"
  else
    unset "$ATTACK_IDS_ENV"
  fi
  if [ "${TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_CANDIDATE_ON_ATTACK:-0}" = "1" ]; then
    if [ "$num_bad" -gt 0 ]; then
      export TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_CANDIDATE=1
    else
      unset TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_CANDIDATE
    fi
  fi

  local deploy_log="$RESULT_DIR/deploy_${RESULT_NAME}${num_bad}_attempt${cell_attempt}.log"
  if ! bash ./script/deploy_multi.sh "./config/performance.conf" > "$deploy_log" 2>&1; then
    grep -E "(=== |Phase|deployed|started|ready|running|error|failed)" \
      "$deploy_log" || true
    echo "  >> Deploy failed; stopping instead of starting benchmark clients."
    cleanup_all
    if [ "$cell_attempt" -lt "$max_cell_attempts" ]; then
      echo "  >> Retrying full cell deployment after deploy failure."
      continue
    fi
    return 1
  fi
  grep -E "(=== |Phase|deployed|started|ready|running)" "$deploy_log" || true

  echo "  Running benchmark client..."
  export BENCHMARK_TOOL_LOG="$RESULT_DIR/benchmark_start_${RESULT_NAME}${num_bad}_attempt${cell_attempt}.log"
  : > "$BENCHMARK_TOOL_LOG"
  if ! start_benchmark_clients; then
    echo "  >> Benchmark client failed to enter sustained load."
    if [ "${BENCHMARK_REQUIRE_START_GATE:-0}" = "1" ]; then
      kill_nodes
      collect_logs
      local fail_dir="$RESULT_DIR/logs_TD-Hotstuff_${RESULT_NAME}${num_bad}_attempt${cell_attempt}_benchmark_start_failed"
      rm -rf "$fail_dir"
      mkdir -p "$fail_dir"
      cp result_* "$fail_dir"/ 2>/dev/null || true
      cp "$BENCHMARK_TOOL_LOG" "$fail_dir"/ 2>/dev/null || true
      if [ "$cell_attempt" -lt "$max_cell_attempts" ]; then
        echo "  >> Retrying full cell deployment after benchmark-start failure."
        continue
      fi
      return 1
    fi
    echo "  >> Continuing; final result parser will reject unhealthy throughput."
  else
    arm_weight_update_vote_equivocation_nodes "$bad_node_ids"
  fi

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
    if ! result_has_valid_throughput "$result_file"; then
      echo "  >> Invalid throughput result; stopping instead of accepting an unhealthy row."
      local invalid_dir="$RESULT_DIR/logs_TD-Hotstuff_${RESULT_NAME}${num_bad}_invalid"
      rm -rf "$invalid_dir"
      mkdir -p "$invalid_dir"
      cp result_* "$invalid_dir"/ 2>/dev/null || true
      cp "$BENCHMARK_TOOL_LOG" "$invalid_dir"/ 2>/dev/null || true
      if [ "$cell_attempt" -lt "$max_cell_attempts" ]; then
        echo "  >> Retrying full cell deployment after invalid result."
        continue
      fi
      return 1
    fi
    local keep_dir="$RESULT_DIR/logs_TD-Hotstuff_${RESULT_NAME}${num_bad}"
    rm -rf "$keep_dir"
    if [ "${KEEP_EXPERIMENT_LOGS:-1}" = "1" ]; then
      mkdir -p "$keep_dir"
      cp result_*_log "$keep_dir"/ 2>/dev/null || true
      cp result_*_reputation.jsonl "$keep_dir"/ 2>/dev/null || true
      cp result_*_qc_evidence.jsonl "$keep_dir"/ 2>/dev/null || true
      cp "$BENCHMARK_TOOL_LOG" "$keep_dir"/ 2>/dev/null || true
      echo "  >> Logs preserved in: $keep_dir"
    else
      echo "  >> Logs discarded (KEEP_EXPERIMENT_LOGS=0)"
    fi
    rm -rf result_*_log result_*_reputation.jsonl result_*_qc_evidence.jsonl
    unset "$ATTACK_IDS_ENV"
    return 0
  else
    echo "  >> NO LOG FILES COLLECTED"
    echo "0" > "$result_file"
    echo "0" >> "$result_file"
    if [ "$cell_attempt" -lt "$max_cell_attempts" ]; then
      echo "  >> Retrying full cell deployment after missing logs."
      continue
    fi
    return 1
  fi
  done

  unset "$ATTACK_IDS_ENV"
  return 1
}

echo "=== Building TD-Hotstuff benchmark binaries ==="
bazel build //benchmark/protocols/td_hotstuff:kv_server_performance \
            //benchmark/protocols/pbft:kv_service_tools 2>&1 | tail -5

read -r -a RUN_COUNTS <<< "$COUNTS"
for num_bad in "${RUN_COUNTS[@]}"; do
  run_single_experiment "$num_bad" || exit 1
done

cleanup_all
echo ""
echo "======================================================================"
echo "  ${MODE_NAME} EXPERIMENTS COMPLETE (n=$N)"
echo "  Results in: $RESULT_DIR/"
echo "======================================================================"
ls -la "$RESULT_DIR/"
