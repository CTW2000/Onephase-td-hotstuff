export server=//benchmark/protocols/hs2:kv_server_performance
export TEMPLATE_PATH=$PWD/config/hs2.config
export performance=true

./performance_local/run_performance.sh $*
