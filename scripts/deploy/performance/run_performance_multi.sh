. ./script/env.sh

./script/deploy_multi.sh $1

. ./script/load_config.sh $1

USER_NAME="hyperchain"
LOCAL_IP="10.10.131.205"

server_name=`echo "$server" | awk -F':' '{print $NF}'`
server_bin=${server_name}

bazel build //benchmark/protocols/pbft:kv_service_tools

for((i=1;;i++))
do
  config_file=$PWD/config_out/client${i}.config
  if [ ! -f "$config_file" ]; then
    break;
  fi
  echo "get cofigfile:"$config_file
  ${BAZEL_WORKSPACE_PATH}/bazel-bin/benchmark/protocols/pbft/kv_service_tools $config_file
done

sleep 40

echo "benchmark done"

# ---- Group nodes by IP ----
declare -A ip_node_list
idx=1
for ip in ${iplist[@]}; do
  ip_node_list[$ip]="${ip_node_list[$ip]} $idx"
  ((idx++))
done
unique_ips=(${!ip_node_list[@]})

# ---- Kill all nodes ----
echo "Killing nodes..."
for ip in "${unique_ips[@]}"; do
  if [ "$ip" == "$LOCAL_IP" ]; then
    killall -9 ${server_bin} 2>/dev/null || true
  else
    ssh -i ${key} -n -o BatchMode=yes -o StrictHostKeyChecking=no $USER_NAME@${ip} "killall -9 ${server_bin}" 2>/dev/null &
  fi
done
wait

# ---- Collect logs ----
echo "Collecting logs..."
rm -rf result_*_log

for ip in "${unique_ips[@]}"; do
  nodes=(${ip_node_list[$ip]})
  for n in "${nodes[@]}"; do
    if [ "$ip" == "$LOCAL_IP" ]; then
      cp ${BAZEL_WORKSPACE_PATH}/scripts/deploy/resilientdb_app/${n}/${server_bin}.log result_${n}_log 2>/dev/null &
    else
      scp -i ${key} -o StrictHostKeyChecking=no \
        $USER_NAME@${ip}:~/resilientdb_app/${n}/${server_bin}.log result_${n}_log 2>/dev/null &
    fi
  done
done
wait

echo "Calculating results..."
python3 performance/calculate_result.py `ls result_*_log 2>/dev/null` > results.log

rm -rf result_*_log
echo "=== Results ==="
cat $TEMPLATE_PATH
cat results.log
