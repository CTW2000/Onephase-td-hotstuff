export server=//benchmark/protocols/hs1:kv_server_performance
export TEMPLATE_PATH=$PWD/config/hs1.config
export performance=true

./performance_local/run_performance.sh $*
