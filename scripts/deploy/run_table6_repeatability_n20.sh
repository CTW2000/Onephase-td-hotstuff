#!/bin/bash
# TD-Hotstuff Table 6 repeatability matrix.
# Runs pipeline-off and full-pipeline repetitions for:
# baseline, silent leader, slow voter, double proposal, double vote, invalid QC.

set -o pipefail

DEPLOY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DEPLOY_DIR"

. ./script/env.sh
. ./td_hotstuff_stable_env.sh

LOCAL_IP="10.10.131.205"
SERVERS="10.10.131.224 10.10.131.247 10.10.131.86 10.10.131.125 10.10.131.83"
N=20

REPEAT_COUNT="${REPEAT_COUNT:-5}"
START_REPEAT="${START_REPEAT:-1}"
TABLE6_MODES="${TABLE6_MODES:-off full}"
TABLE6_ATTACKS="${TABLE6_ATTACKS:-baseline silent_leader slow_voter double_proposal double_vote invalid_qc}"
TABLE6_COUNTS="${TABLE6_COUNTS:-1 2 3 4}"
BASELINE_SLEEP_TIME="${BASELINE_SLEEP_TIME:-90}"
DEFAULT_SLEEP_TIME="${DEFAULT_SLEEP_TIME:-90}"
SLOW_LEADER_SLEEP_TIME="${SLOW_LEADER_SLEEP_TIME:-150}"
SLOW_VOTER_SLEEP_TIME="${SLOW_VOTER_SLEEP_TIME:-150}"
TABLE6_CASE_RETRY_LIMIT="${TABLE6_CASE_RETRY_LIMIT:-2}"
MEAN_DELAY_MS="${MEAN_DELAY_MS:-10}"
RESULT_ROOT="${RESULT_ROOT:-$DEPLOY_DIR/experiment_results/table6_repeatability_n20}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d_%H%M%S)}"
OUT_DIR="$RESULT_ROOT/$RUN_ID"
SUMMARY_CSV="$OUT_DIR/summary.csv"

CONFIG_FILES_TO_RESTORE=(
  "config/td_hotstuff.config"
  "config/performance.conf"
)
. ./script/generated_config_restore.sh
backup_generated_configs
trap 'cleanup_all; restore_generated_configs' EXIT

mkdir -p "$OUT_DIR/results"

if [ ! -f "$SUMMARY_CSV" ]; then
  echo "repeat,mode,experiment,bad_count,bad_node_ids,result_file,throughput,latency,bad_voting_weights,bad_leader_weights,honest_weight_min,honest_weight_avg,honest_weight_max,bad_strong_fault_counts,bad_penalty_points,leader_context_mismatch,candidate_digest_mismatch,bad_timeout" > "$SUMMARY_CSV"
fi

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
      if [ "${KEEP_TABLE6_CASE_LOGS:-0}" = "1" ]; then
        scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/'td_hotstuff_reputation_node_'*.jsonl result_${node_id}_reputation.jsonl 2>/dev/null || true &
        scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/'td_hotstuff_qc_evidence_node_'*.jsonl result_${node_id}_qc_evidence.jsonl 2>/dev/null || true &
      fi
    done
  done
  for d in "$DEPLOY_DIR"/resilientdb_app/*/; do
    [ -d "$d" ] || continue
    local node_id
    node_id=$(basename "$d")
    if [ -f "$d/kv_server_performance.log" ]; then
      cp "$d/kv_server_performance.log" result_${node_id}_log 2>/dev/null &
      if [ "${KEEP_TABLE6_CASE_LOGS:-0}" = "1" ]; then
        cp "$d"/td_hotstuff_reputation_node_*.jsonl result_${node_id}_reputation.jsonl 2>/dev/null || true &
        cp "$d"/td_hotstuff_qc_evidence_node_*.jsonl result_${node_id}_qc_evidence.jsonl 2>/dev/null || true &
      fi
    fi
  done
  wait
}

derive_contiguous_ids() {
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

derive_silent_leader_ids() {
  local count="$1"
  local ids=""
  for ((bad_idx=0; bad_idx<count; bad_idx++)); do
    local bad_id=$((bad_idx * 3 + 1))
    if [ -n "$ids" ]; then
      ids="${ids},"
    fi
    ids="${ids}${bad_id}"
  done
  printf '%s' "$ids"
}

clear_attack_env() {
  unset TD_HS_SILENT_LEADER_IDS TD_HS_SLOW_VOTE_IDS
  unset TD_HS_DOUBLE_PROPOSAL_IDS TD_HS_DOUBLE_VOTE_IDS TD_HS_INVALID_QC_IDS
  unset TD_HS_LOW_DIVERSITY_QC_IDS TD_HS_LOW_DIVERSITY_TARGET_IDS
  unset TD_HS_LOW_DIVERSITY_REVIEWER_IDS TD_HS_LOW_DIVERSITY_MIN_AVAILABLE_SIGNERS
  unset TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS
  unset TD_HS_BAD_NODE_IDS TD_HS_BAD_NODE_COUNT
}

clear_pipeline_env() {
  unset TD_HS_REPUTATION_ENABLE TD_HS_WEIGHT_UPDATE_ENABLE
  unset TD_HS_STRONG_FAULT_ENABLE TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE
  unset TD_HS_DOUBLE_VOTE_DETECT_ENABLE TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE
  unset TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE
  unset TD_HS_CONFLICTING_QC_DETECT_ENABLE
  unset TD_HS_STRONG_FAULT_TARGET_WEIGHT
  unset TD_HS_REPUTATION_DECAY_PER_EPOCH TD_HS_REPUTATION_MAX_RECOVERY_PER_EPOCH
  unset TD_HS_REPUTATION_BONUS_PER_EPOCH TD_HS_REPUTATION_WINDOW_SIZE
  unset TD_HS_REPUTATION_MIN_CANDIDATE_QCS TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES
  unset TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY
  unset TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS
  unset TD_HS_LEADER_SELECTION_ENABLE TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT
  unset TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE TD_HS_REPUTATION_AUDIT_JSONL_ENABLE
  unset TD_HS_SLOW_VOTE_DELAY_US TD_HS_EVIDENCE_ENABLE
  unset TD_HS_TIMEOUT_ENABLE TD_HS_TIMEOUT_MS TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS
}

configure_pipeline() {
  local mode="$1"
  local experiment="$2"
  clear_pipeline_env
  export TD_HS_BENCHMARK_DYNAMIC_ROUTING_ENABLE=1
  export TD_HS_BENCHMARK_RETRY_ENABLE=1
  export TD_HS_BENCHMARK_REQUEST_TIMEOUT_MS=100
  export TD_HS_REQUEST_VIEW_LOOKAHEAD=20
  export TD_HS_TX_FORWARD_LOOKAHEAD=4
  if [ "$mode" = "off" ]; then
    case "$experiment" in
      silent_leader)
        export TD_HS_TIMEOUT_ENABLE=1
        export TD_HS_TIMEOUT_MS=100
        export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS="${TD_HS_SLOW_LEADER_EMPTY_PROPOSAL_VIEWS:-20}"
        ;;
      double_proposal|double_vote|invalid_qc)
        export TD_HS_TIMEOUT_ENABLE=1
        export TD_HS_TIMEOUT_MS=200
        export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS=20
        ;;
      *)
        export TD_HS_TIMEOUT_ENABLE=0
        export TD_HS_TIMEOUT_MS=120
        export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS=2
        ;;
    esac
    return
  fi

  export TD_HS_REPUTATION_ENABLE=1
  export TD_HS_WEIGHT_UPDATE_ENABLE=1
  export TD_HS_STRONG_FAULT_ENABLE=1
  export TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE=1
  export TD_HS_DOUBLE_VOTE_DETECT_ENABLE=1
  export TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE=1
  export TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE=1
  export TD_HS_CONFLICTING_QC_DETECT_ENABLE=1
  export TD_HS_STRONG_FAULT_TARGET_WEIGHT=1
  export TD_HS_REPUTATION_WINDOW_SIZE=64
  export TD_HS_REPUTATION_MIN_CANDIDATE_QCS=16
  export TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS=64
  export TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY="${TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY:-4}"
  export TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS="${TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS:-128}"
  export TD_HS_LEADER_SELECTION_ENABLE=1
  export TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT=10
  export TD_HS_REPUTATION_AUDIT_JSONL_ENABLE=1
  export TD_HS_EVIDENCE_ENABLE=0

  case "$experiment" in
    silent_leader)
      export TD_HS_REPUTATION_DECAY_PER_EPOCH=99
      export TD_HS_REPUTATION_MAX_RECOVERY_PER_EPOCH=99
      export TD_HS_REPUTATION_BONUS_PER_EPOCH=0
      export TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE=1
      export TD_HS_TIMEOUT_ENABLE=1
      export TD_HS_TIMEOUT_MS=100
      export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS="${TD_HS_SLOW_LEADER_EMPTY_PROPOSAL_VIEWS:-20}"
      ;;
    slow_voter)
      export TD_HS_REPUTATION_DECAY_PER_EPOCH=99
      export TD_HS_REPUTATION_MAX_RECOVERY_PER_EPOCH=99
      export TD_HS_REPUTATION_BONUS_PER_EPOCH=0
      export TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE=1
      export TD_HS_SLOW_VOTE_DELAY_US="${TD_HS_SLOW_VOTE_DELAY_US:-$((MEAN_DELAY_MS * 1000))}"
      export TD_HS_TIMEOUT_ENABLE=0
      export TD_HS_TIMEOUT_MS=120
      export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS=2
      ;;
    double_vote)
      export TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES=1000000
      export TD_HS_TIMEOUT_ENABLE=1
      export TD_HS_TIMEOUT_MS=200
      export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS=20
      ;;
    double_proposal|invalid_qc)
      export TD_HS_TIMEOUT_ENABLE=1
      export TD_HS_TIMEOUT_MS=200
      export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS=20
      ;;
    *)
      export TD_HS_TIMEOUT_ENABLE=0
      export TD_HS_TIMEOUT_MS=120
      export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS=2
      ;;
  esac
}

generate_td_config() {
  local experiment="$1"
  python3 - "$experiment" "$MEAN_DELAY_MS" <<'PY'
import sys
experiment = sys.argv[1]
mean_delay = int(sys.argv[2])
if experiment == "silent_leader":
    from leader_slowness_experiment import generate_config, generate_performance_server_conf
    generate_config(config_path="./config/td_hotstuff.config",
                    max_process_txn=5,
                    non_responsive_num=0,
                    timer_length=100)
else:
    from network_delay_experiment import generate_config, generate_performance_server_conf
    generate_config(config_path="./config/td_hotstuff.config",
                    max_process_txn=5,
                    network_delay_num=0,
                    mean_network_delay=0 if experiment != "slow_voter" else mean_delay)
generate_performance_server_conf(20)
PY
}

set_attack_env() {
  local experiment="$1"
  local bad_count="$2"
  local ids="$3"
  clear_attack_env
  export TD_HS_BAD_NODE_COUNT="$bad_count"
  export TD_HS_BAD_NODE_IDS="$ids"
  if [ "$bad_count" -le 0 ]; then
    return
  fi
  case "$experiment" in
    silent_leader) export TD_HS_SILENT_LEADER_IDS="$ids" ;;
    slow_voter) export TD_HS_SLOW_VOTE_IDS="$ids" ;;
    double_proposal) export TD_HS_DOUBLE_PROPOSAL_IDS="$ids" ;;
    double_vote) export TD_HS_DOUBLE_VOTE_IDS="$ids" ;;
    invalid_qc) export TD_HS_INVALID_QC_IDS="$ids" ;;
  esac
}

bad_ids_for_case() {
  local experiment="$1"
  local bad_count="$2"
  if [ "$bad_count" -le 0 ]; then
    printf ''
  elif [ "$experiment" = "silent_leader" ]; then
    derive_silent_leader_ids "$bad_count"
  else
    derive_contiguous_ids "$bad_count"
  fi
}

sleep_time_for_case() {
  local experiment="$1"
  if [ "$experiment" = "baseline" ]; then
    echo "$BASELINE_SLEEP_TIME"
  elif [ "$experiment" = "silent_leader" ]; then
    echo "$SLOW_LEADER_SLEEP_TIME"
  elif [ "$experiment" = "slow_voter" ]; then
    echo "$SLOW_VOTER_SLEEP_TIME"
  else
    echo "$DEFAULT_SLEEP_TIME"
  fi
}

append_summary_row() {
  local repeat="$1"
  local mode="$2"
  local experiment="$3"
  local bad_count="$4"
  local bad_ids="$5"
  local result_file="$6"
  python3 - "$SUMMARY_CSV" "$repeat" "$mode" "$experiment" "$bad_count" "$bad_ids" "$result_file" <<'PY'
import csv
import re
import sys
summary, repeat, mode, experiment, bad_count, bad_ids, result_file = sys.argv[1:]
text = open(result_file, errors="ignore").read()
numbers = [line.strip() for line in text.splitlines() if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", line.strip())]
throughput = numbers[-2] if len(numbers) >= 2 else (numbers[0] if numbers else "")
latency = numbers[-1] if len(numbers) >= 2 else ""

def match(pattern):
    m = re.search(pattern, text)
    return m.group(1).strip() if m else ""

honest = match(r"honest final weights min/avg/max:\s*([^\n]+)")
honest_min = honest_avg = honest_max = ""
if honest:
    parts = honest.split()
    if len(parts) >= 3:
      honest_min, honest_avg, honest_max = parts[:3]

def counter(name):
    m = re.search(rf"{re.escape(name)}\s*[:=]\s*([0-9]+)", text)
    return m.group(1) if m else "0"

row = {
    "repeat": repeat,
    "mode": mode,
    "experiment": experiment,
    "bad_count": bad_count,
    "bad_node_ids": bad_ids,
    "result_file": result_file,
    "throughput": throughput,
    "latency": latency,
    "bad_voting_weights": match(r"bad node final weights:\s*([^\n]+)"),
    "bad_leader_weights": match(r"bad node final leader weights:\s*([^\n]+)"),
    "honest_weight_min": honest_min,
    "honest_weight_avg": honest_avg,
    "honest_weight_max": honest_max,
    "bad_strong_fault_counts": match(r"bad node strong fault counts:\s*([^\n]+)"),
    "bad_penalty_points": match(r"bad node penalty points:\s*([^\n]+)"),
    "leader_context_mismatch": counter("leader_context_mismatch") or counter("proposal leader context mismatch"),
    "candidate_digest_mismatch": counter("candidate_digest_mismatch") or counter("candidate digest mismatch"),
    "bad_timeout": counter("bad_timeout_cert") or counter("bad timeout"),
}
with open(summary, "a", newline="") as fp:
    writer = csv.DictWriter(fp, fieldnames=list(row))
    writer.writerow(row)
PY
}

result_needs_retry() {
  local result_file="$1"
  [ -s "$result_file" ] || return 0
  python3 - "$result_file" <<'PYCHECK'
import math
import re
import sys

text = open(sys.argv[1], errors="ignore").read()
numbers = [line.strip() for line in text.splitlines()
           if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", line.strip())]
if not numbers:
    sys.exit(0)
throughput = numbers[-2] if len(numbers) >= 2 else numbers[0]
try:
    throughput_value = float(throughput)
except ValueError:
    sys.exit(0)
if not math.isfinite(throughput_value) or throughput_value <= 0:
    sys.exit(0)
match = re.search(r"stable positive throughput samples:\s*([0-9]+)", text)
if match and int(match.group(1)) == 0:
    sys.exit(0)
honest = re.search(r"honest final weights min/avg/max:\s*([0-9.]+)", text)
if honest:
    try:
        if float(honest.group(1)) < 100.0:
            sys.exit(0)
    except ValueError:
        sys.exit(0)
experiment = None
for name in ("silent_leader", "slow_voter", "double_proposal",
             "double_vote", "invalid_qc",
             "weight_update_vote_equivocation"):
    if f"_{name}_" in sys.argv[1]:
        experiment = name
        break
def parse_weight_list(label):
    match = re.search(label + r":\s*([0-9,]+)", text)
    if not match:
        return None
    try:
        return [int(part) for part in match.group(1).split(",") if part]
    except ValueError:
        return None
bad_weights = parse_weight_list("bad node final weights")
bad_leader_weights = parse_weight_list("bad node final leader weights")
if experiment == "silent_leader":
    if bad_weights is not None and any(weight != 100 for weight in bad_weights):
        sys.exit(0)
    if bad_leader_weights is not None and any(weight != 1 for weight in bad_leader_weights):
        sys.exit(0)
elif experiment in ("slow_voter", "double_proposal", "double_vote",
                    "invalid_qc", "weight_update_vote_equivocation"):
    if bad_weights is not None and any(weight != 1 for weight in bad_weights):
        sys.exit(0)
    if experiment != "slow_voter" and bad_leader_weights is not None and \
            any(weight != 1 for weight in bad_leader_weights):
        sys.exit(0)
sys.exit(1)
PYCHECK
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
      env -u TD_HS_SILENT_LEADER_IDS -u TD_HS_SLOW_VOTE_IDS \
          -u TD_HS_DOUBLE_PROPOSAL_IDS -u TD_HS_DOUBLE_VOTE_IDS \
          -u TD_HS_INVALID_QC_IDS -u TD_HS_LOW_DIVERSITY_QC_IDS \
          -u TD_HS_LOW_DIVERSITY_TARGET_IDS -u TD_HS_LOW_DIVERSITY_REVIEWER_IDS \
          -u TD_HS_LOW_DIVERSITY_MIN_AVAILABLE_SIGNERS \
          -u TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS \
          \
          -u TD_HS_BAD_NODE_IDS -u TD_HS_BAD_NODE_COUNT \
          ${BAZEL_WORKSPACE_PATH}/bazel-bin/benchmark/protocols/pbft/kv_service_tools "$cf" 2>/dev/null
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

run_case() {
  local repeat="$1"
  local mode="$2"
  local experiment="$3"
  local bad_count="$4"
  local ids
  ids=$(bad_ids_for_case "$experiment" "$bad_count")
  local result_file="$OUT_DIR/results/r${repeat}_${mode}_${experiment}_${bad_count}.txt"
  if [ -s "$result_file" ] && grep -Eq '^[0-9]+([.][0-9]+)?$' "$result_file"; then
    if result_needs_retry "$result_file"; then
      echo "Existing result invalid; rerunning r=$repeat mode=$mode experiment=$experiment bad=$bad_count"
    else
      echo "SKIP existing r=$repeat mode=$mode experiment=$experiment bad=$bad_count"
      append_summary_row "$repeat" "$mode" "$experiment" "$bad_count" "$ids" "$result_file"
      return
    fi
  fi

  local max_attempts=$((TABLE6_CASE_RETRY_LIMIT + 1))
  local attempt
  for ((attempt=1; attempt<=max_attempts; attempt++)); do
    echo ""
    echo "======================================================================"
    echo "TABLE6 repeat=$repeat/$REPEAT_COUNT mode=$mode experiment=$experiment bad=$bad_count ids=${ids:-none} attempt=$attempt/$max_attempts"
    echo "======================================================================"
    cleanup_all
    generate_td_config "$experiment"
    configure_pipeline "$mode" "$experiment"
    set_attack_env "$experiment" "$bad_count" "$ids"
    export TEMPLATE_PATH="$PWD/config/td_hotstuff.config"
    export server=//benchmark/protocols/td_hotstuff:kv_server_performance

    bash ./script/deploy_multi.sh "./config/performance.conf" 2>&1 | grep -E "(=== |Phase|deployed|started|ready|running)"

    echo "  Running benchmark client..."
    if ! start_benchmark_clients; then
      echo "  >> Benchmark client failed to enter sustained load."
      kill_nodes
      collect_logs
      echo "benchmark client failed to enter sustained load" > "$result_file"
      echo "0" >> "$result_file"
      echo "0" >> "$result_file"
      if [ "${KEEP_TABLE6_CASE_LOGS:-0}" = "1" ]; then
        local start_fail_log_dir="$OUT_DIR/logs/r${repeat}_${mode}_${experiment}_${bad_count}_benchmark_start_failed_attempt${attempt}"
        mkdir -p "$start_fail_log_dir"
        cp result_* "$start_fail_log_dir"/ 2>/dev/null || true
      fi
      rm -rf result_*_log result_*_reputation.jsonl result_*_qc_evidence.jsonl
      clear_attack_env
      clear_pipeline_env
      if [ "$attempt" -lt "$max_attempts" ]; then
        continue
      fi
      return 1
    fi

    local sleep_time
    sleep_time=$(sleep_time_for_case "$experiment")
    echo "  Sleeping ${sleep_time}s..."
    sleep "$sleep_time"

    kill_nodes
    collect_logs

    local log_files
    log_files=$(ls result_*_log 2>/dev/null)
    if [ -n "$log_files" ]; then
      TD_HS_BAD_NODE_COUNT="$bad_count" TD_HS_BAD_NODE_IDS="$ids" \
        TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT="${TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT:-10}" \
        python3 performance/calculate_result.py $log_files > "$result_file" 2>&1
      local tps lat
      tps=$(grep "^[0-9]" "$result_file" | head -1)
      lat=$(grep "^[0-9]" "$result_file" | tail -1)
      echo "  >> Throughput: $tps txn/s | Latency: $lat s"
      if result_needs_retry "$result_file" && [ "$attempt" -lt "$max_attempts" ]; then
        echo "  >> Invalid result; retrying case (attempt $attempt/$max_attempts)"
        if [ "${KEEP_TABLE6_CASE_LOGS:-0}" = "1" ]; then
          local retry_log_dir="$OUT_DIR/logs/r${repeat}_${mode}_${experiment}_${bad_count}_invalid_attempt${attempt}"
          mkdir -p "$retry_log_dir"
          cp result_* "$retry_log_dir"/ 2>/dev/null || true
        fi
        rm -rf result_*_log result_*_reputation.jsonl result_*_qc_evidence.jsonl
        clear_attack_env
        clear_pipeline_env
        continue
      fi
      if result_needs_retry "$result_file"; then
        echo "  >> Invalid result after final attempt; stopping instead of accepting an unhealthy row."
        if [ "${KEEP_TABLE6_CASE_LOGS:-0}" = "1" ]; then
          local final_invalid_log_dir="$OUT_DIR/logs/r${repeat}_${mode}_${experiment}_${bad_count}_final_invalid"
          mkdir -p "$final_invalid_log_dir"
          cp result_* "$final_invalid_log_dir"/ 2>/dev/null || true
        fi
        rm -rf result_*_log result_*_reputation.jsonl result_*_qc_evidence.jsonl
        clear_attack_env
        clear_pipeline_env
        return 1
      fi
      append_summary_row "$repeat" "$mode" "$experiment" "$bad_count" "$ids" "$result_file"
      if [ "${KEEP_TABLE6_CASE_LOGS:-0}" = "1" ]; then
        local case_log_dir="$OUT_DIR/logs/r${repeat}_${mode}_${experiment}_${bad_count}"
        mkdir -p "$case_log_dir"
        cp result_* "$case_log_dir"/ 2>/dev/null || true
      fi
      rm -rf result_*_log result_*_reputation.jsonl result_*_qc_evidence.jsonl
      clear_attack_env
      clear_pipeline_env
      return
    fi

    echo "  >> NO LOG FILES COLLECTED"
    echo "0" > "$result_file"
    echo "0" >> "$result_file"
    if [ "$attempt" -lt "$max_attempts" ]; then
      echo "  >> Missing logs; retrying case (attempt $attempt/$max_attempts)"
      clear_attack_env
      clear_pipeline_env
      continue
    fi
    clear_attack_env
    clear_pipeline_env
    return 1
  done
}

write_aggregate_report() {
  python3 - "$SUMMARY_CSV" "$OUT_DIR/aggregate_report.md" <<'PY'
import csv
import math
import statistics
import sys
from collections import defaultdict

summary_csv, report_path = sys.argv[1:]
rows = []
with open(summary_csv, newline="") as fp:
    for row in csv.DictReader(fp):
        try:
            row["_tps"] = float(row["throughput"])
        except Exception:
            row["_tps"] = math.nan
        try:
            row["_lat"] = float(row["latency"])
        except Exception:
            row["_lat"] = math.nan
        rows.append(row)

groups = defaultdict(list)
for row in rows:
    groups[(row["mode"], row["experiment"], row["bad_count"])].append(row)

def stats(values):
    values = [v for v in values if not math.isnan(v)]
    if not values:
        return ("", "", "", "", "")
    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    return (f"{mean:.2f}", f"{stdev:.2f}", f"{min(values):.2f}", f"{max(values):.2f}", str(len(values)))

with open(report_path, "w") as out:
    out.write("# Table 6 Repeatability Aggregate\\n\\n")
    out.write("| Mode | Experiment | Bad | n | TPS mean | TPS sd | TPS min | TPS max | Lat mean | Lat sd | Bad voting weights | Bad leader weights | Honest weights |\\n")
    out.write("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|\\n")
    for key in sorted(groups):
        group = groups[key]
        mode, experiment, bad = key
        t_mean, t_sd, t_min, t_max, n = stats([r["_tps"] for r in group])
        l_mean, l_sd, _, _, _ = stats([r["_lat"] for r in group])
        last = group[-1]
        honest = "/".join([last["honest_weight_min"], last["honest_weight_avg"], last["honest_weight_max"]]).strip("/")
        out.write(f"| {mode} | {experiment} | {bad} | {n} | {t_mean} | {t_sd} | {t_min} | {t_max} | {l_mean} | {l_sd} | {last['bad_voting_weights']} | {last['bad_leader_weights']} | {honest} |\\n")
print(report_path)
PY
}

echo "=== Building TD-Hotstuff benchmark binaries ==="
bazel build //benchmark/protocols/td_hotstuff:kv_server_performance \
            //benchmark/protocols/pbft:kv_service_tools 2>&1 | tail -5

for ((repeat=START_REPEAT; repeat<=REPEAT_COUNT; repeat++)); do
  for mode in $TABLE6_MODES; do
    for experiment in $TABLE6_ATTACKS; do
      if [ "$experiment" = "baseline" ]; then
        if ! run_case "$repeat" "$mode" "$experiment" 0; then
          write_aggregate_report
          exit 1
        fi
      else
        for bad_count in $TABLE6_COUNTS; do
          if ! run_case "$repeat" "$mode" "$experiment" "$bad_count"; then
            write_aggregate_report
            exit 1
          fi
        done
      fi
    done
  done
  write_aggregate_report
done

write_aggregate_report
cleanup_all

echo ""
echo "======================================================================"
echo "TABLE 6 REPEATABILITY COMPLETE"
echo "Run dir: $OUT_DIR"
echo "Summary CSV: $SUMMARY_CSV"
echo "Aggregate report: $OUT_DIR/aggregate_report.md"
echo "======================================================================"
