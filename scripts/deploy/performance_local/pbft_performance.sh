export server=//benchmark/protocols/pbft:kv_server_performance
export TEMPLATE_PATH=$PWD/config/pbft.config
export performance=true

./performance_local/run_performance.sh $*
