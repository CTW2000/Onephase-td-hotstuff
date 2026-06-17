#!/bin/bash

td_hs_remove_result_artifacts() {
  rm -rf result_*_log result_*_reputation.jsonl result_*_qc_evidence.jsonl
}

td_hs_collect_logs() {
  local deploy_dir="$1"
  local servers="$2"
  td_hs_remove_result_artifacts
  for ip in $servers; do
    for node_id in $(ssh -o StrictHostKeyChecking=no hyperchain@$ip "ls ~/resilientdb_app/ 2>/dev/null | grep -E '^[0-9]+$'" 2>/dev/null); do
      scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/kv_server_performance.log result_${node_id}_log 2>/dev/null &
      scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/'td_hotstuff_reputation_node_'*.jsonl result_${node_id}_reputation.jsonl 2>/dev/null || true &
      scp -o StrictHostKeyChecking=no hyperchain@$ip:~/resilientdb_app/$node_id/'td_hotstuff_qc_evidence_node_'*.jsonl result_${node_id}_qc_evidence.jsonl 2>/dev/null || true &
    done
  done
  for d in "$deploy_dir"/resilientdb_app/*/; do
    [ -d "$d" ] || continue
    local node_id
    node_id=$(basename "$d")
    if [ -f "$d/kv_server_performance.log" ]; then
      cp "$d/kv_server_performance.log" result_${node_id}_log 2>/dev/null &
      cp "$d"/td_hotstuff_reputation_node_*.jsonl result_${node_id}_reputation.jsonl 2>/dev/null || true &
      cp "$d"/td_hotstuff_qc_evidence_node_*.jsonl result_${node_id}_qc_evidence.jsonl 2>/dev/null || true &
    fi
  done
  wait
}

td_hs_client_load_started() {
  local app_root="$1"
  local client_node_id="$2"
  local client_log="$app_root/${client_node_id}/kv_server_performance.log"
  if [ -f "$client_log" ] && grep -Eq "munmap_chunk|invalid pointer|double free|corrupted" "$client_log"; then
    return 1
  fi
  BENCHMARK_START_MIN_CLIENT_CALLS="${BENCHMARK_START_MIN_CLIENT_CALLS:-1000}" \
    python3 - "$app_root" <<'PYCHECK'
import os
import re
import sys

min_calls = int(os.environ.get("BENCHMARK_START_MIN_CLIENT_CALLS", "1000"))
root = sys.argv[1]
for dirpath, _, filenames in os.walk(root):
    if "kv_server_performance.log" not in filenames:
        continue
    path = os.path.join(dirpath, "kv_server_performance.log")
    text = open(path, errors="ignore").read()
    if re.search(r"req client latency:\s*(?!-?nan)([0-9]+(?:\.[0-9]+)?)", text):
        sys.exit(0)
    for match in re.finditer(r"client call:([0-9]+)", text):
        if int(match.group(1)) >= min_calls:
            sys.exit(0)
sys.exit(1)
PYCHECK
}

td_hs_start_benchmark_clients() {
  local config_dir="$1"
  local client_tool="$2"
  shift 2
  local max_attempts="${BENCHMARK_START_ATTEMPTS:-3}"
  local wait_seconds="${BENCHMARK_START_WAIT_SECONDS:-45}"
  local pre_start_sleep="${BENCHMARK_PRE_START_SLEEP:-20}"
  if [ "$pre_start_sleep" -gt 0 ]; then
    echo "  Waiting ${pre_start_sleep}s for benchmark client channels to settle..."
    sleep "$pre_start_sleep"
  fi
  local attempt
  for ((attempt=1; attempt<=max_attempts; attempt++)); do
    echo "  Starting benchmark clients (attempt ${attempt}/${max_attempts})..."
    for((i=1;;i++)); do
      cf=$config_dir/client${i}.config
      if [ ! -f "$cf" ]; then break; fi
      env "$@" "$client_tool" "$cf" > /tmp/td_hs_kv_service_tools.out 2>&1
      local tool_rc=$?
      if [ -n "${BENCHMARK_TOOL_LOG:-}" ]; then
        {
          echo "attempt=${attempt} config=${cf} rc=${tool_rc}"
          sed 's/^/  /' /tmp/td_hs_kv_service_tools.out
        } >> "$BENCHMARK_TOOL_LOG"
      fi
    done

    local second
    for ((second=1; second<=wait_seconds; second++)); do
      sleep 1
      if td_hs_client_load_started "$DEPLOY_DIR/resilientdb_app" "$((N + 1))"; then
        echo "  Benchmark client load is running."
        return 0
      fi
    done
    echo "  Benchmark client load did not start after ${wait_seconds}s."
  done
  return 1
}

td_hs_result_has_valid_throughput() {
  local result_file="$1"
  python3 - "$result_file" <<'PYCHECK'
import math
import os
import re
import sys

text = open(sys.argv[1], errors="ignore").read()
numbers = [line.strip() for line in text.splitlines()
           if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", line.strip())]
if len(numbers) < 2:
    sys.exit(1)
try:
    throughput = float(numbers[-2])
except ValueError:
    sys.exit(1)
if not math.isfinite(throughput) or throughput <= 0:
    sys.exit(1)
min_stable_tps = float(os.environ.get("BENCHMARK_MIN_STABLE_TPS", "0") or "0")
if min_stable_tps > 0 and throughput < min_stable_tps:
    print(
        f"stable throughput {throughput} below BENCHMARK_MIN_STABLE_TPS "
        f"{min_stable_tps}",
        file=sys.stderr,
    )
    sys.exit(1)
match = re.search(r"stable positive throughput samples:\s*([0-9]+)", text)
if match and int(match.group(1)) == 0:
    sys.exit(1)
sys.exit(0)
PYCHECK
}

td_hs_copy_success_artifacts() {
  local dest="$1"
  mkdir -p "$dest"
  cp result_*_log "$dest"/ 2>/dev/null || true
  if [ "${TD_HS_KEEP_FULL_JSONL_LOGS:-0}" = "1" ]; then
    cp result_*_reputation.jsonl "$dest"/ 2>/dev/null || true
    cp result_*_qc_evidence.jsonl "$dest"/ 2>/dev/null || true
  fi
}
