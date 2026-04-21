#!/bin/bash
# Master experiment runner for all protocols with n≤20
# Runs experiments sequentially, collects results into experiment_results/

set -o pipefail
DEPLOY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DEPLOY_DIR"

# Set BAZEL_WORKSPACE_PATH (needed for kv_service_tools path)
. ./script/env.sh

RESULT_DIR="$DEPLOY_DIR/experiment_results"
rm -rf "$RESULT_DIR"
mkdir -p "$RESULT_DIR"

LOCAL_IP="10.10.131.205"
SERVERS="10.10.131.224 10.10.131.247 10.10.131.86 10.10.131.125 10.10.131.83"
SLEEP_TIME=25
PROTOCOLS=("HS-1" "HS-2" "HS" "HS-1-SLOT")

kill_nodes() {
  # Only kill processes, keep log files for collection
  killall -9 kv_server_performance 2>/dev/null || true
  for ip in $SERVERS; do
    ssh -o StrictHostKeyChecking=no hyperchain@$ip "killall -9 kv_server_performance" 2>/dev/null &
  done
  wait
  sleep 1
}

cleanup_all() {
  # Kill processes AND remove old logs on ALL servers
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
  # Local node logs
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
  local perf_script="$4"
  local config_file="$5"

  echo ""
  echo "======================================================================"
  echo "  EXPERIMENT: $experiment_name | Protocol: $protocol"
  echo "======================================================================"

  # Clean ALL servers before each experiment
  cleanup_all

  # Deploy
  export server="$perf_script"
  case "$protocol" in
    "HS-1")    export TEMPLATE_PATH=$PWD/config/hs1.config; export server=//benchmark/protocols/hs1:kv_server_performance;;
    "HS-2")    export TEMPLATE_PATH=$PWD/config/hs2.config; export server=//benchmark/protocols/hs2:kv_server_performance;;
    "HS")      export TEMPLATE_PATH=$PWD/config/hs.config; export server=//benchmark/protocols/hs:kv_server_performance;;
    "HS-1-SLOT") export TEMPLATE_PATH=$PWD/config/slot_hs1.config; export server=//benchmark/protocols/slot_hs1:kv_server_performance;;
  esac

  bash ./script/deploy_multi.sh "$config_file" 2>&1 | grep -E "(=== |Phase|deployed|started|ready|running)"

  # Run benchmark
  echo "  Running benchmark..."
  for((i=1;;i++)); do
    cf=$PWD/config_out/client${i}.config
    if [ ! -f "$cf" ]; then break; fi
    ${BAZEL_WORKSPACE_PATH}/bazel-bin/benchmark/protocols/pbft/kv_service_tools "$cf" 2>/dev/null
  done

  echo "  Sleeping ${SLEEP_TIME}s..."
  sleep $SLEEP_TIME

  # Kill nodes (keep logs), then collect
  kill_nodes
  collect_logs

  # Calculate results
  local log_files=$(ls result_*_log 2>/dev/null)
  if [ -n "$log_files" ]; then
    python3 performance/calculate_result.py $log_files > "$result_file" 2>&1
    # Extract throughput and latency
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

# Build all protocol binaries first
echo "=== Building all protocol binaries ==="
bazel build //benchmark/protocols/hs1:kv_server_performance \
            //benchmark/protocols/hs2:kv_server_performance \
            //benchmark/protocols/hs:kv_server_performance \
            //benchmark/protocols/slot_hs1:kv_server_performance \
            //benchmark/protocols/pbft:kv_service_tools 2>&1 | tail -5

########################################################################
# EXPERIMENT 1: SCALABILITY (n = 4, 10, 16, 19)
########################################################################
echo ""
echo "######################################################################"
echo "# EXPERIMENT 1: SCALABILITY"
echo "######################################################################"

SCALE_NS=(4 10 16 19)
mkdir -p "$RESULT_DIR/scalability"

for n in "${SCALE_NS[@]}"; do
  for proto in "${PROTOCOLS[@]}"; do
    # Generate config
    case "$proto" in
      "HS-1")    mpt=3; cfg="hs1";;
      "HS-2")    mpt=4; cfg="hs2";;
      "HS")      mpt=5; cfg="hs";;
      "HS-1-SLOT") mpt=3; cfg="slot_hs1";;
    esac

    python3 -c "
from scalability_experiment import generate_config, generate_performance_server_conf
generate_config(config_path='./config/${cfg}.config', max_process_txn=$mpt)
generate_performance_server_conf($n)
"
    result_file="$RESULT_DIR/scalability/${proto}_n${n}.txt"
    run_single_experiment "$proto" "Scalability n=$n" "$result_file" "" "./config/performance.conf"
  done
done

########################################################################
# EXPERIMENT 2: LEADER SLOWNESS (100ms timeout, n=19)
########################################################################
echo ""
echo "######################################################################"
echo "# EXPERIMENT 2: LEADER SLOWNESS (100ms)"
echo "######################################################################"

SLOW_COUNTS=(0 1 4 6)
mkdir -p "$RESULT_DIR/leader_slowness"

for num_slow in "${SLOW_COUNTS[@]}"; do
  for proto in "${PROTOCOLS[@]}"; do
    case "$proto" in
      "HS-1")    mpt=3; cfg="hs1";;
      "HS-2")    mpt=4; cfg="hs2";;
      "HS")      mpt=5; cfg="hs";;
      "HS-1-SLOT") mpt=3; cfg="slot_hs1";;
    esac

    python3 -c "
from leader_slowness_experiment import generate_config, generate_performance_server_conf
generate_config(config_path='./config/${cfg}.config',
                max_process_txn=$mpt, non_responsive_num=$num_slow, timer_length=100)
generate_performance_server_conf(19)
"
    result_file="$RESULT_DIR/leader_slowness/${proto}_slow${num_slow}.txt"
    run_single_experiment "$proto" "LeaderSlowness slow=$num_slow timer=100ms" "$result_file" "" "./config/performance.conf"
  done
done

########################################################################
# EXPERIMENT 3: TAIL FORKING (100ms timeout, n=19)
########################################################################
echo ""
echo "######################################################################"
echo "# EXPERIMENT 3: TAIL FORKING (100ms)"
echo "######################################################################"

FORK_COUNTS=(0 1 4)
mkdir -p "$RESULT_DIR/tail_forking"

for num_fork in "${FORK_COUNTS[@]}"; do
  for proto in "${PROTOCOLS[@]}"; do
    case "$proto" in
      "HS-1")    mpt=3; cfg="hs1";;
      "HS-2")    mpt=4; cfg="hs2";;
      "HS")      mpt=5; cfg="hs";;
      "HS-1-SLOT") mpt=3; cfg="slot_hs1";;
    esac

    python3 -c "
from tail_forking_experiment import generate_config, generate_performance_server_conf
generate_config(config_path='./config/${cfg}.config',
                max_process_txn=$mpt, fork_tail_num=$num_fork, timer_length=100)
generate_performance_server_conf(19)
"
    result_file="$RESULT_DIR/tail_forking/${proto}_fork${num_fork}.txt"
    run_single_experiment "$proto" "TailForking fork=$num_fork timer=100ms" "$result_file" "" "./config/performance.conf"
  done
done

########################################################################
# EXPERIMENT 4: ROLLBACK (100ms timeout, n=19, HS-1 and HS-1-SLOT only)
########################################################################
echo ""
echo "######################################################################"
echo "# EXPERIMENT 4: ROLLBACK (100ms)"
echo "######################################################################"

ROLLBACK_COUNTS=(0 1 4)
ROLLBACK_PROTOS=("HS-1" "HS-1-SLOT")
mkdir -p "$RESULT_DIR/rollback"

for num_rb in "${ROLLBACK_COUNTS[@]}"; do
  for proto in "${ROLLBACK_PROTOS[@]}"; do
    mpt=3

    case "$proto" in
      "HS-1")    cfg="hs1";;
      "HS-1-SLOT") cfg="slot_hs1";;
    esac

    python3 -c "
from rollback_experiment import generate_config, generate_performance_server_conf
generate_config(config_path='./config/${cfg}.config',
                max_process_txn=$mpt, rollback_num=$num_rb, timer_length=100)
generate_performance_server_conf(19)
"
    result_file="$RESULT_DIR/rollback/${proto}_rb${num_rb}.txt"
    run_single_experiment "$proto" "Rollback rb=$num_rb timer=100ms" "$result_file" "" "./config/performance.conf"
  done
done

########################################################################
# FINAL CLEANUP
########################################################################
cleanup_all
echo ""
echo "======================================================================"
echo "  ALL EXPERIMENTS COMPLETE"
echo "  Results saved in: $RESULT_DIR/"
echo "======================================================================"
ls -R "$RESULT_DIR/"
