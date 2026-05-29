#!/usr/bin/env bash
set -euo pipefail

export server=//benchmark/protocols/hs:kv_server_performance
export TEMPLATE_PATH=${TEMPLATE_PATH:-$PWD/config/hs.config}
export key=${key:-/home/hyperchain/.ssh/id_rsa}

bash ./performance/run_performance_multi.sh "$@"
