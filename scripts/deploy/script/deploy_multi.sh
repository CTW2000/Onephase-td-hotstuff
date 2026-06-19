set -e

# load environment parameters
. ./script/env.sh

# load ip list
. ./script/load_config.sh $1

USER_NAME="hyperchain"
LOCAL_IP="10.10.131.205"

script_path=${BAZEL_WORKSPACE_PATH}/scripts

if [[ -z $server ]]; then
  server=//service/kv:kv_service
fi

if [[ -z $client_num ]]; then
  client_num=1
fi

server_path=`echo "$server" | sed 's/:/\//g'`
server_path=${server_path:1}
server_name=`echo "$server" | awk -F':' '{print $NF}'`
server_bin=${server_name}

local_server_env=(env -u TD_HS_SILENT_LEADER_IDS -u TD_HS_SLOW_VOTE_IDS -u TD_HS_PEERTRUST_CLIQUE_IDS -u TD_HS_PEERTRUST_CLIQUE_TARGET_IDS -u TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS -u TD_HS_SYBIL_GRAPH_ATTACK_IDS -u TD_HS_SYBIL_GRAPH_REVIEWER_IDS -u TD_HS_DOUBLE_PROPOSAL_IDS -u TD_HS_DOUBLE_VOTE_IDS -u TD_HS_LOW_DIVERSITY_QC_IDS -u TD_HS_LOW_DIVERSITY_TARGET_IDS -u TD_HS_LOW_DIVERSITY_REVIEWER_IDS -u TD_HS_LOW_DIVERSITY_MIN_AVAILABLE_SIGNERS -u TD_HS_LOW_DIVERSITY_QC_TRACE -u TD_HS_INVALID_QC_IDS -u TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS -u TD_HS_BAD_NODE_IDS -u TD_HS_BAD_NODE_COUNT)
remote_server_env=""
td_env_names=(
  TD_HS_WEIGHTS
  TD_HS_EVIDENCE_ENABLE
  TD_HS_EVIDENCE_OUTPUT_DIR
  TD_HS_EVIDENCE_QUEUE_CAPACITY
  TD_HS_REPUTATION_ENABLE
  TD_HS_REPUTATION_WINDOW_SIZE
  TD_HS_REPUTATION_OUTPUT_DIR
  TD_HS_REPUTATION_QUEUE_CAPACITY
  TD_HS_REPUTATION_ADAPTER_QUEUE_CAPACITY
  TD_HS_REPUTATION_AUDIT_JSONL_ENABLE
  TD_HS_REPUTATION_AUDIT_JSONL_PATH
  TD_HS_REPUTATION_MAX_DELTA
  TD_HS_REPUTATION_MIN_CANDIDATE_QCS
  TD_HS_REPUTATION_DECAY_PER_EPOCH
  TD_HS_REPUTATION_MAX_RECOVERY_PER_EPOCH
  TD_HS_REPUTATION_BONUS_PER_EPOCH
  TD_HS_REPUTATION_VOTE_BETA_DECAY_PER_MILLE
  TD_HS_REPUTATION_MULTIPLICATIVE_WEIGHT_ENABLE
  TD_HS_REPUTATION_STAKE_TAU_PER_MILLE
  TD_HS_REPUTATION_STAKE_MIN_PER_MILLE
  TD_HS_REPUTATION_STAKE_MAX_PER_MILLE
  TD_HS_REPUTATION_IDENTITY_MIN_PER_MILLE
  TD_HS_REPUTATION_IDENTITY_MAX_PER_MILLE
  TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE
  TD_HS_REPUTATION_PEERTRUST_ENABLE
  TD_HS_REPUTATION_SYBIL_GRAPH_ENABLE
  TD_HS_REPUTATION_SYBIL_GRAPH_ITERATIONS
  TD_HS_REPUTATION_SYBIL_GRAPH_MAX_DISCOUNT
  TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_INCREMENT
  TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_RECOVERY
  TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_MAX
  TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_TRIGGER_SCORE
  TD_HS_REPUTATION_SYBIL_GRAPH_SEED_MIN_REPUTATION
  TD_HS_REPUTATION_SYBIL_GRAPH_MIN_EDGES
  TD_HS_REPUTATION_MIN_WEIGHT
  TD_HS_REPUTATION_MAX_WEIGHT
  TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES
  TD_HS_REPUTATION_MIN_LEADER_OPPORTUNITIES
  TD_HS_STRONG_FAULT_ENABLE
  TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE
  TD_HS_DOUBLE_VOTE_DETECT_ENABLE
  TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE
  TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE


  TD_HS_CONFLICTING_QC_DETECT_ENABLE
  TD_HS_STRONG_FAULT_TARGET_WEIGHT
  TD_HS_WEIGHT_UPDATE_ENABLE
  TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_CANDIDATE
  TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_INITIAL_VERSION_ONLY
  TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_MIN_START_VIEW
  TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW
  TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_ON_CANDIDATE
  TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS
  TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY
  TD_HS_WEIGHT_UPDATE_TRACE
  TD_HS_ROUND_FLOW_TRACE
  TD_HS_LEADER_SELECTION_ENABLE
  TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT
  TD_HS_BENCHMARK_DYNAMIC_ROUTING_ENABLE
  TD_HS_BENCHMARK_RETRY_ENABLE
  TD_HS_BENCHMARK_REQUEST_TIMEOUT_MS
  TD_HS_REQUEST_VIEW_LOOKAHEAD
  TD_HS_TX_FORWARD_LOOKAHEAD
  TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS
  TD_HS_QC_DIVERSITY_ENABLE
  TD_HS_QC_SIGNER_COOLDOWN_ROUNDS
  TD_HS_QC_DIVERSITY_GRACE_US
  TD_HS_PEERTRUST_QC_TRACE
  TD_HS_TIMEOUT_ENABLE
  TD_HS_TIMEOUT_MS
  TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS
  TD_HS_SLOW_VOTE_DELAY_US
)
for env_name in "${td_env_names[@]}"; do
  env_value="${!env_name:-}"
  if [ -n "${env_value}" ]; then
    local_server_env+=("${env_name}=${env_value}")
    env_value_escaped=$(printf "%q" "${env_value}")
    remote_server_env="${remote_server_env}${env_name}=${env_value_escaped} "
  fi
done
if [ -n "${remote_server_env}" ]; then
  remote_server_env="env ${remote_server_env}"
fi

is_silent_leader_node() {
  local node_id="$1"
  local raw_ids=",${TD_HS_SILENT_LEADER_IDS:-},"
  [[ "$raw_ids" == *",${node_id},"* ]]
}

is_slow_vote_node() {
  local node_id="$1"
  local raw_ids=",${TD_HS_SLOW_VOTE_IDS:-},"
  [[ "$raw_ids" == *",${node_id},"* ]]
}

is_peertrust_clique_node() {
  local node_id="$1"
  local raw_ids=",${TD_HS_PEERTRUST_CLIQUE_IDS:-},"
  [[ "$raw_ids" == *",${node_id},"* ]]
}

is_sybil_graph_attack_node() {
  local node_id="$1"
  local raw_ids=",${TD_HS_SYBIL_GRAPH_ATTACK_IDS:-},"
  [[ "$raw_ids" == *",${node_id},"* ]]
}

is_double_proposal_node() {
  local node_id="$1"
  local raw_ids=",${TD_HS_DOUBLE_PROPOSAL_IDS:-},"
  [[ "$raw_ids" == *",${node_id},"* ]]
}

is_double_vote_node() {
  local node_id="$1"
  local raw_ids=",${TD_HS_DOUBLE_VOTE_IDS:-},"
  [[ "$raw_ids" == *",${node_id},"* ]]
}

is_low_diversity_qc_node() {
  local node_id="$1"
  local raw_ids=",${TD_HS_LOW_DIVERSITY_QC_IDS:-},"
  [[ "$raw_ids" == *",${node_id},"* ]]
}

is_invalid_qc_node() {
  local node_id="$1"
  local raw_ids=",${TD_HS_INVALID_QC_IDS:-},"
  [[ "$raw_ids" == *",${node_id},"* ]]
}

is_weight_update_vote_equivocation_node() {
  local node_id="$1"
  local raw_ids=",${TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS:-},"
  [[ "$raw_ids" == *",${node_id},"* ]]
}

remote_env_for_node() {
  local node_id="$1"
  local env_prefix="${remote_server_env}"
  if is_silent_leader_node "$node_id"; then
    if [ -n "${env_prefix}" ]; then
      env_prefix="${env_prefix}TD_HS_SILENT_LEADER=1 "
    else
      env_prefix="env TD_HS_SILENT_LEADER=1 "
    fi
  fi
  if is_slow_vote_node "$node_id"; then
    if [ -n "${env_prefix}" ]; then
      env_prefix="${env_prefix}TD_HS_SLOW_VOTE=1 "
    else
      env_prefix="env TD_HS_SLOW_VOTE=1 "
    fi
  fi
  if is_peertrust_clique_node "$node_id"; then
    if [ -n "${env_prefix}" ]; then
      env_prefix="${env_prefix}TD_HS_PEERTRUST_CLIQUE=1 "
    else
      env_prefix="env TD_HS_PEERTRUST_CLIQUE=1 "
    fi
    if [ -n "${TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS:-}" ]; then
      signer_ids_escaped=$(printf "%q" "${TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS}")
      env_prefix="${env_prefix}TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS=${signer_ids_escaped} "
    fi
    if [ -n "${TD_HS_PEERTRUST_CLIQUE_TARGET_IDS:-}" ]; then
      target_ids_escaped=$(printf "%q" "${TD_HS_PEERTRUST_CLIQUE_TARGET_IDS}")
      env_prefix="${env_prefix}TD_HS_PEERTRUST_CLIQUE_TARGET_IDS=${target_ids_escaped} "
    fi
  fi
  if is_sybil_graph_attack_node "$node_id"; then
    if [ -n "${env_prefix}" ]; then
      env_prefix="${env_prefix}TD_HS_SYBIL_GRAPH_ATTACK=1 "
    else
      env_prefix="env TD_HS_SYBIL_GRAPH_ATTACK=1 "
    fi
    if [ -n "${TD_HS_SYBIL_GRAPH_REVIEWER_IDS:-}" ]; then
      reviewer_ids_escaped=$(printf "%q" "${TD_HS_SYBIL_GRAPH_REVIEWER_IDS}")
      env_prefix="${env_prefix}TD_HS_SYBIL_GRAPH_REVIEWER_IDS=${reviewer_ids_escaped} "
    fi
  fi
  if is_double_proposal_node "$node_id"; then
    if [ -n "${env_prefix}" ]; then
      env_prefix="${env_prefix}TD_HS_DOUBLE_PROPOSAL=1 "
    else
      env_prefix="env TD_HS_DOUBLE_PROPOSAL=1 "
    fi
  fi
  if is_double_vote_node "$node_id"; then
    if [ -n "${env_prefix}" ]; then
      env_prefix="${env_prefix}TD_HS_DOUBLE_VOTE=1 "
    else
      env_prefix="env TD_HS_DOUBLE_VOTE=1 "
    fi
  fi
  if is_low_diversity_qc_node "$node_id"; then
    if [ -n "${env_prefix}" ]; then
      env_prefix="${env_prefix}TD_HS_LOW_DIVERSITY_QC=1 "
    else
      env_prefix="env TD_HS_LOW_DIVERSITY_QC=1 "
    fi
    if [ -n "${TD_HS_LOW_DIVERSITY_TARGET_IDS:-}" ]; then
      low_diversity_targets_escaped=$(printf "%q" "${TD_HS_LOW_DIVERSITY_TARGET_IDS}")
      env_prefix="${env_prefix}TD_HS_LOW_DIVERSITY_TARGET_IDS=${low_diversity_targets_escaped} "
    fi
    if [ -n "${TD_HS_LOW_DIVERSITY_REVIEWER_IDS:-}" ]; then
      low_diversity_reviewers_escaped=$(printf "%q" "${TD_HS_LOW_DIVERSITY_REVIEWER_IDS}")
      env_prefix="${env_prefix}TD_HS_LOW_DIVERSITY_REVIEWER_IDS=${low_diversity_reviewers_escaped} "
    fi
    if [ -n "${TD_HS_LOW_DIVERSITY_MIN_AVAILABLE_SIGNERS:-}" ]; then
      low_diversity_min_available_escaped=$(printf "%q" "${TD_HS_LOW_DIVERSITY_MIN_AVAILABLE_SIGNERS}")
      env_prefix="${env_prefix}TD_HS_LOW_DIVERSITY_MIN_AVAILABLE_SIGNERS=${low_diversity_min_available_escaped} "
    fi
    if [ -n "${TD_HS_LOW_DIVERSITY_QC_TRACE:-}" ]; then
      low_diversity_trace_escaped=$(printf "%q" "${TD_HS_LOW_DIVERSITY_QC_TRACE}")
      env_prefix="${env_prefix}TD_HS_LOW_DIVERSITY_QC_TRACE=${low_diversity_trace_escaped} "
    fi
  fi
  if is_invalid_qc_node "$node_id"; then
    if [ -n "${env_prefix}" ]; then
      env_prefix="${env_prefix}TD_HS_INVALID_QC=1 "
    else
      env_prefix="env TD_HS_INVALID_QC=1 "
    fi
  fi
  if is_weight_update_vote_equivocation_node "$node_id"; then
    if [ -n "${env_prefix}" ]; then
      env_prefix="${env_prefix}TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION=1 "
    else
      env_prefix="env TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION=1 "
    fi
    env_prefix="${env_prefix}TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE=/home/hyperchain/resilientdb_app/${node_id}/td_hs_wue_trigger "
  fi
  printf '%s' "${env_prefix}"
}

bin_path=${BAZEL_WORKSPACE_PATH}/bazel-bin/${server_path}
output_path=${script_path}/deploy/config_out
output_key_path=${output_path}/cert
output_cert_path=${output_key_path}
admin_key_path=${script_path}/deploy/data/cert

rm -rf ${output_path}
mkdir -p ${output_path}

deploy_iplist=${iplist[@]}

echo "=== Multi-Node Deploy ==="
echo "server:${server_bin} | nodes:${#iplist[@]} | ips:${deploy_iplist[@]}"

cd ${script_path}
deploy/script/generate_key.sh ${BAZEL_WORKSPACE_PATH} ${output_key_path} ${#iplist[@]}
deploy/script/generate_config.sh ${BAZEL_WORKSPACE_PATH} ${output_key_path} ${output_cert_path} ${output_path} ${admin_key_path} ${client_num} ${deploy_iplist[@]}

bazel build ${server}
if [ $? != 0 ]; then
  echo "Compile ${server} failed"
  exit 1
fi

# ---- Group nodes by IP ----
declare -A ip_node_list
declare -A ip_seen
idx=1
for ip in ${deploy_iplist[@]}; do
  ip_node_list[$ip]="${ip_node_list[$ip]} $idx"
  ip_seen[$ip]=1
  ((idx++))
done
unique_ips=(${!ip_seen[@]})

# ---- Phase 1: Kill old processes ----
echo "Phase 1: Kill old processes..."
(
  for ip in "${unique_ips[@]}"; do
    if [ "$ip" == "$LOCAL_IP" ]; then
      killall -9 ${server_bin} 2>/dev/null || true
    else
      ssh -i ${key} -n -o BatchMode=yes -o StrictHostKeyChecking=no $USER_NAME@${ip} "killall -9 ${server_bin}" 2>/dev/null || true &
    fi
  done
  wait
)
sleep 1

# ---- Phase 2: Deploy files ----
echo "Phase 2: Deploy files..."
(
  for ip in "${unique_ips[@]}"; do
    nodes=(${ip_node_list[$ip]})
    if [ "$ip" == "$LOCAL_IP" ]; then
      rm -rf ${script_path}/deploy/resilientdb_app
      for n in "${nodes[@]}"; do
        node_dir="${script_path}/deploy/resilientdb_app/${n}"
        mkdir -p "${node_dir}"
        cp ${bin_path} "${node_dir}/"
        cp ${output_path}/server.config "${node_dir}/"
        cp -r ${output_path}/cert "${node_dir}/"
      done
    else
      (
        dir_cmds="rm -rf ~/resilientdb_app"
        for n in "${nodes[@]}"; do
          dir_cmds="${dir_cmds} && mkdir -p ~/resilientdb_app/${n}"
        done
        ssh -i ${key} -n -o BatchMode=yes -o StrictHostKeyChecking=no $USER_NAME@${ip} "${dir_cmds}"
        for n in "${nodes[@]}"; do
          scp -i ${key} -o StrictHostKeyChecking=no -r \
            ${bin_path} ${output_path}/server.config ${output_path}/cert \
            $USER_NAME@${ip}:~/resilientdb_app/${n}/ > /dev/null 2>&1
        done
      ) &
    fi
  done
  wait
)
echo "Files deployed."

# ---- Phase 3: Start nodes (in subshell so background SSHs don't leak) ----
echo "Phase 3: Start nodes..."
(
  for ip in "${unique_ips[@]}"; do
    nodes=(${ip_node_list[$ip]})
    grafana_port=8090
    if [ "$ip" == "$LOCAL_IP" ]; then
      for n in "${nodes[@]}"; do
        node_dir="${script_path}/deploy/resilientdb_app/${n}"
        # setsid -f: new session + fork → fully detached from shell job control
        node_local_env=("${local_server_env[@]}")
        if is_silent_leader_node "$n"; then
          if [ ${#node_local_env[@]} -eq 0 ]; then
            node_local_env=(env)
          fi
          node_local_env+=("TD_HS_SILENT_LEADER=1")
        fi
        if is_slow_vote_node "$n"; then
          if [ ${#node_local_env[@]} -eq 0 ]; then
            node_local_env=(env)
          fi
          node_local_env+=("TD_HS_SLOW_VOTE=1")
        fi
        if is_peertrust_clique_node "$n"; then
          if [ ${#node_local_env[@]} -eq 0 ]; then
            node_local_env=(env)
          fi
          node_local_env+=("TD_HS_PEERTRUST_CLIQUE=1")
          if [ -n "${TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS:-}" ]; then
            node_local_env+=("TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS=${TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS}")
          fi
          if [ -n "${TD_HS_PEERTRUST_CLIQUE_TARGET_IDS:-}" ]; then
            node_local_env+=("TD_HS_PEERTRUST_CLIQUE_TARGET_IDS=${TD_HS_PEERTRUST_CLIQUE_TARGET_IDS}")
          fi
        fi
        if is_sybil_graph_attack_node "$n"; then
          if [ ${#node_local_env[@]} -eq 0 ]; then
            node_local_env=(env)
          fi
          node_local_env+=("TD_HS_SYBIL_GRAPH_ATTACK=1")
          if [ -n "${TD_HS_SYBIL_GRAPH_REVIEWER_IDS:-}" ]; then
            node_local_env+=("TD_HS_SYBIL_GRAPH_REVIEWER_IDS=${TD_HS_SYBIL_GRAPH_REVIEWER_IDS}")
          fi
        fi
        if is_double_proposal_node "$n"; then
          if [ ${#node_local_env[@]} -eq 0 ]; then
            node_local_env=(env)
          fi
          node_local_env+=("TD_HS_DOUBLE_PROPOSAL=1")
        fi
        if is_double_vote_node "$n"; then
          if [ ${#node_local_env[@]} -eq 0 ]; then
            node_local_env=(env)
          fi
          node_local_env+=("TD_HS_DOUBLE_VOTE=1")
        fi
        if is_low_diversity_qc_node "$n"; then
          if [ ${#node_local_env[@]} -eq 0 ]; then
            node_local_env=(env)
          fi
          node_local_env+=("TD_HS_LOW_DIVERSITY_QC=1")
          if [ -n "${TD_HS_LOW_DIVERSITY_TARGET_IDS:-}" ]; then
            node_local_env+=("TD_HS_LOW_DIVERSITY_TARGET_IDS=${TD_HS_LOW_DIVERSITY_TARGET_IDS}")
          fi
          if [ -n "${TD_HS_LOW_DIVERSITY_REVIEWER_IDS:-}" ]; then
            node_local_env+=("TD_HS_LOW_DIVERSITY_REVIEWER_IDS=${TD_HS_LOW_DIVERSITY_REVIEWER_IDS}")
          fi
          if [ -n "${TD_HS_LOW_DIVERSITY_MIN_AVAILABLE_SIGNERS:-}" ]; then
            node_local_env+=("TD_HS_LOW_DIVERSITY_MIN_AVAILABLE_SIGNERS=${TD_HS_LOW_DIVERSITY_MIN_AVAILABLE_SIGNERS}")
          fi
          if [ -n "${TD_HS_LOW_DIVERSITY_QC_TRACE:-}" ]; then
            node_local_env+=("TD_HS_LOW_DIVERSITY_QC_TRACE=${TD_HS_LOW_DIVERSITY_QC_TRACE}")
          fi
        fi
        if is_invalid_qc_node "$n"; then
          if [ ${#node_local_env[@]} -eq 0 ]; then
            node_local_env=(env)
          fi
          node_local_env+=("TD_HS_INVALID_QC=1")
        fi
        if is_weight_update_vote_equivocation_node "$n"; then
          if [ ${#node_local_env[@]} -eq 0 ]; then
            node_local_env=(env)
          fi
          node_local_env+=("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION=1")
          node_local_env+=("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE=${DEPLOY_DIR}/resilientdb_app/${n}/td_hs_wue_trigger")
        fi
        (cd "${node_dir}" && "${node_local_env[@]}" setsid -f ./${server_bin} server.config cert/node_${n}.key.pri cert/cert_${n}.cert 0.0.0.0:${grafana_port} > ${server_bin}.log 2>&1 < /dev/null)
        ((grafana_port++))
      done
    else
      # Build startup script content
      start_cmds=""
      for n in "${nodes[@]}"; do
        node_remote_env=$(remote_env_for_node "$n")
        start_cmds="${start_cmds}cd ~/resilientdb_app/${n} && ${node_remote_env}nohup ./${server_bin} server.config cert/node_${n}.key.pri cert/cert_${n}.cert 0.0.0.0:${grafana_port} > ${server_bin}.log 2>&1 & "
        ((grafana_port++))
      done
      # Use timeout to prevent SSH from hanging forever
      timeout 10 ssh -i ${key} -n -o BatchMode=yes -o StrictHostKeyChecking=no $USER_NAME@${ip} "${start_cmds}" 2>/dev/null || true &
    fi
  done
  wait
)
echo "Nodes started."

# ---- Phase 4: Readiness check (clean subshell) ----
echo "Phase 4: Readiness check..."
expected_size=${#iplist[@]}
(
  for ip in "${unique_ips[@]}"; do
    nodes=(${ip_node_list[$ip]})
    for n in "${nodes[@]}"; do
      (
        for attempt in $(seq 1 120); do
          if [ "$ip" == "$LOCAL_IP" ]; then
            resp=$(grep "receive public size:${expected_size}" ${script_path}/deploy/resilientdb_app/${n}/${server_bin}.log 2>/dev/null || true)
          else
            resp=$(ssh -i ${key} -n -o BatchMode=yes -o StrictHostKeyChecking=no $USER_NAME@${ip} \
              "grep 'receive public size:${expected_size}' ~/resilientdb_app/${n}/${server_bin}.log 2>/dev/null" 2>/dev/null || true)
          fi
          if [ -n "$resp" ]; then
            echo "  Node ${n}@${ip} ready"
            break
          fi
          sleep 1
        done
      ) &
    done
  done
  wait
)
echo "=== All ${#iplist[@]} nodes running ==="
