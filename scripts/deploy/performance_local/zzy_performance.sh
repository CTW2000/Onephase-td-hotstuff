export server=//benchmark/protocols/zzy:kv_server_performance
export TEMPLATE_PATH=$PWD/config/zzy.config
export performance=true

./performance_local/run_performance.sh $*
