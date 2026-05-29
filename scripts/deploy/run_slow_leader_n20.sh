#!/bin/bash
# Slow Leader experiment for n=20 — all 5 protocols, sequential.
# Uniform clientBatchNum=100; 100ms leader timeout.

set -o pipefail
DEPLOY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DEPLOY_DIR"

. ./script/env.sh

RESULT_DIR="$DEPLOY_DIR/experiment_results/slow_leader_n20"
mkdir -p "$RESULT_DIR"

LOCAL_IP="10.10.131.205"
SERVERS="10.10.131.224 10.10.131.247 10.10.131.86 10.10.131.125 10.10.131.83"
SLEEP_TIME=25
PROTOCOLS=("HS-1" "HS-2" "HS" "HS-1-SLOT" "TD-Hotstuff")
SLOW_COUNTS=(0 1 4 6)
N=20
TIMER=100

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
  rm -rf $DEPLOY_DIR/resilientdb_app
  for ip in $SERVERS; do
    ssh -o StrictHostKeyChecking=no hyperchain@$ip "killall -9 kv_server_performance; rm -rf ~/resilientdb_app" 2>/dev/null &
  done
  wait
  sleep 1
}

collect_logs() {
  rm -rf result_*_log
  for ip in $SERVERS; do
    for node_id in $(ssh -o StrictHostKeyChecking=no hyperchain@$ip "ls ~/resilientdb_app/ 2>/dev/null | grep -E '^[0-9]+$'" 2>/dev/null); do
      scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/kv_server_performance.log result_${node_id}_log 2>/dev/null &
    done
  done
  for d in $DEPLOY_DIR/resilientdb_app/*/; do
    local node_id=$(basename "$d")
    if [ -f "$d/kv_server_performance.log" ]; then
      cp "$d/kv_server_performance.log" result_${node_id}_log 2>/dev/null &
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

  echo "  Running benchmark..."
  for((i=1;;i++)); do
    cf=$PWD/config_out/client${i}.config
    if [ ! -f "$cf" ]; then break; fi
    ${BAZEL_WORKSPACE_PATH}/bazel-bin/benchmark/protocols/pbft/kv_service_tools "$cf" 2>/dev/null
  done

  echo "  Sleeping ${SLEEP_TIME}s..."
  sleep $SLEEP_TIME

  kill_nodes
  collect_logs

  local log_files=$(ls result_*_log 2>/dev/null)
  if [ -n "$log_files" ]; then
    python3 performance/calculate_result.py $log_files > "$result_file" 2>&1
    local tps=$(grep "^[0-9]" "$result_file" | head -1)
    local lat=$(grep "^[0-9]" "$result_file" | tail -1)
    echo "  >> Throughput: $tps txn/s | Latency: $lat s"
    rm -rf result_*_log
  else
    echo "  >> NO LOG FILES COLLECTED"
    echo "0" > "$result_file"
    echo "0" >> "$result_file"
  fi
}

echo "=== Building all protocol binaries ==="
bazel build //benchmark/protocols/hs1:kv_server_performance \
            //benchmark/protocols/hs2:kv_server_performance \
            //benchmark/protocols/hs:kv_server_performance \
            //benchmark/protocols/slot_hs1:kv_server_performance \
            //benchmark/protocols/td_hotstuff:kv_server_performance \
            //benchmark/protocols/pbft:kv_service_tools 2>&1 | tail -5

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

    python3 -c "
from leader_slowness_experiment import generate_config, generate_performance_server_conf
generate_config(config_path='./config/${cfg}.config',
                max_process_txn=$mpt, non_responsive_num=$num_slow, timer_length=$TIMER)
generate_performance_server_conf($N)
"
    actual_slow=$(grep non_responsive_num ./config/${cfg}.config | grep -o '[0-9]\+')
    if [ "$actual_slow" != "$num_slow" ]; then
      echo "ERROR: ${cfg}.config has non_responsive_num=$actual_slow, expected $num_slow — ABORTING"
      exit 1
    fi

    result_file="$RESULT_DIR/${proto}_slow${num_slow}.txt"
    run_single_experiment "$proto" "SlowLeader slow=$num_slow timer=${TIMER}ms n=$N" "$result_file" "./config/performance.conf"
  done
done

cleanup_all
echo ""
echo "======================================================================"
echo "  SLOW LEADER EXPERIMENTS COMPLETE (n=$N)"
echo "  Results in: $RESULT_DIR/"
echo "======================================================================"
ls -la "$RESULT_DIR/"
