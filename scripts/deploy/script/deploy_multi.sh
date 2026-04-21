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
        (cd "${node_dir}" && setsid -f ./${server_bin} server.config cert/node_${n}.key.pri cert/cert_${n}.cert 0.0.0.0:${grafana_port} > ${server_bin}.log 2>&1 < /dev/null)
        ((grafana_port++))
      done
    else
      # Build startup script content
      start_cmds=""
      for n in "${nodes[@]}"; do
        start_cmds="${start_cmds}cd ~/resilientdb_app/${n} && nohup ./${server_bin} server.config cert/node_${n}.key.pri cert/cert_${n}.cert 0.0.0.0:${grafana_port} > ${server_bin}.log 2>&1 & "
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
