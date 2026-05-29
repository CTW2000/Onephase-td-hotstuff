#!/bin/bash
# Two attack experiments for n=20, all 5 protocols, sequential.
#   Attack 1: Rollback (rollback_num)
#   Attack 2: Combined fork_tail + non_responsive (fork + slow leader)
# Runs Attack 1 fully, then Attack 2. NO parallel experiments.

set -o pipefail
DEPLOY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DEPLOY_DIR"

. ./script/env.sh

RESULT_DIR_A1="$DEPLOY_DIR/experiment_results/rollback_n20"
RESULT_DIR_A2="$DEPLOY_DIR/experiment_results/combined_fork_slow_n20"
mkdir -p "$RESULT_DIR_A1" "$RESULT_DIR_A2"

LOCAL_IP="10.10.131.205"
SERVERS="10.10.131.224 10.10.131.247 10.10.131.86 10.10.131.125 10.10.131.83"
SLEEP_TIME=25
PROTOCOLS=("HS-1" "HS-2" "HS" "HS-1-SLOT" "TD-Hotstuff")
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

########################################################################
# ATTACK 1: ROLLBACK
########################################################################
echo ""
echo "######################################################################"
echo "# ATTACK 1: ROLLBACK | n=$N | timer=${TIMER}ms"
echo "# Note: HS-2 and HS don't implement rollback injection — they will"
echo "# stay at baseline, which itself is the data point."
echo "######################################################################"

ROLLBACK_COUNTS=(0 1 4 6)

for num_rb in "${ROLLBACK_COUNTS[@]}"; do
  for proto in "${PROTOCOLS[@]}"; do
    case "$proto" in
      "HS-1")      mpt=3; cfg="hs1";;
      "HS-2")      mpt=4; cfg="hs2";;
      "HS")        mpt=5; cfg="hs";;
      "HS-1-SLOT") mpt=3; cfg="slot_hs1";;
      "TD-Hotstuff"|"TD-HotStuff"|"TD-HS") mpt=5; cfg="td_hotstuff";;
    esac

    python3 -c "
from rollback_experiment import generate_config, generate_performance_server_conf
generate_config(config_path='./config/${cfg}.config',
                max_process_txn=$mpt, rollback_num=$num_rb, timer_length=$TIMER)
generate_performance_server_conf($N)
"
    actual_rb=$(grep rollback_num ./config/${cfg}.config | grep -o '[0-9]\+')
    if [ "$actual_rb" != "$num_rb" ]; then
      echo "ERROR: ${cfg}.config has rollback_num=$actual_rb, expected $num_rb — ABORTING"
      exit 1
    fi

    result_file="$RESULT_DIR_A1/${proto}_rb${num_rb}.txt"
    run_single_experiment "$proto" "Rollback rb=$num_rb n=$N" "$result_file" "./config/performance.conf"
  done
done

########################################################################
# ATTACK 2: COMBINED FORK + SLOW LEADER
########################################################################
echo ""
echo "######################################################################"
echo "# ATTACK 2: COMBINED fork + slow_leader | n=$N | timer=${TIMER}ms"
echo "######################################################################"

# (fork, slow) pairs
COMBINED=("0 0" "1 1" "2 2" "4 4")

for combo in "${COMBINED[@]}"; do
  read -r num_fork num_slow <<< "$combo"
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
                max_process_txn=$mpt,
                non_responsive_num=$num_slow,
                fork_tail_num=$num_fork,
                timer_length=$TIMER)
generate_performance_server_conf($N)
"
    # Verify both fault params wrote successfully
    actual_fork=$(grep fork_tail_num ./config/${cfg}.config | grep -o '[0-9]\+')
    actual_slow=$(grep non_responsive_num ./config/${cfg}.config | grep -o '[0-9]\+')
    if [ "$actual_fork" != "$num_fork" ] || [ "$actual_slow" != "$num_slow" ]; then
      echo "ERROR: ${cfg}.config fork=$actual_fork (want $num_fork) slow=$actual_slow (want $num_slow) — ABORTING"
      exit 1
    fi

    result_file="$RESULT_DIR_A2/${proto}_fork${num_fork}_slow${num_slow}.txt"
    run_single_experiment "$proto" "Combined fork=$num_fork slow=$num_slow n=$N" "$result_file" "./config/performance.conf"
  done
done

cleanup_all
echo ""
echo "======================================================================"
echo "  ALL ATTACK EXPERIMENTS COMPLETE (n=$N)"
echo "  Attack 1 (Rollback): $RESULT_DIR_A1/"
echo "  Attack 2 (Combined): $RESULT_DIR_A2/"
echo "======================================================================"
ls -la "$RESULT_DIR_A1/" "$RESULT_DIR_A2/"
