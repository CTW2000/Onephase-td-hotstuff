export server=//benchmark/protocols/hs:kv_server_performance
export TEMPLATE_PATH=$PWD/config/hs.config
export performance=true

./performance_local/run_performance.sh $*
