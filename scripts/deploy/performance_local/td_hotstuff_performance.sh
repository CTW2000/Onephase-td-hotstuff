export server=//benchmark/protocols/td_hotstuff:kv_server_performance
export TEMPLATE_PATH=$PWD/config/td_hotstuff.config
export performance=true

./performance_local/run_performance.sh $*
