# TD-Hotstuff Experiment Best Practices

This file is a living playbook. Update it when a better experiment setting, parser check, or debugging lesson is learned. It is not immutable philosophy; `AGENTS.md` holds the durable project rules.

## Why Long Experiment Iterations Happen

A full TD-Hotstuff experiment is expensive because it touches many layers, not just one binary:

- Build and test the server checkout.
- Deploy binaries, configs, certificates, and scripts across six servers and twenty replica processes.
- Start replicas and wait for readiness on all hosts.
- Run the benchmark client long enough to include warmup, weight/reputation windows, certification, activation, and post-activation behavior.
- Collect logs and reputation JSONL from all remote hosts.
- Parse throughput, latency, active weights, leader eligibility, activation timing, and protocol mismatch counters.
- If a result looks too good or too bad, run control experiments to separate consensus behavior from benchmark/client/config artifacts.

The slowest part is usually not editing code. It is repeated distributed experiment cycles after a hidden pipeline bug appears. To save time, run one small pilot experiment before launching a full matrix.

## Pre-Experiment Integrity Checklist

Run this checklist before trusting any result:

- Operate on the server checkout on `10.10.131.205`.
- Verify branch, HEAD, and working-tree status from the server checkout.
- If the experiment depends on code changes, rebuild the benchmark binary before deployment.
- Verify all replica server checkouts are synced when their source trees matter for the run.
- Confirm reusable defaults come from shared scripts such as `scripts/deploy/td_hotstuff_stable_env.sh`, not one-off shell state left from a previous experiment.
- Confirm experiment-specific attack settings are transient and restored after the run.
- Capture runtime env snapshots for client and replica processes.
- Confirm bad-node metadata is not visible to normal client or replica processes.
- Confirm the client does not route using a bad-node oracle. It may use real responses or active protocol-visible state only.
- Confirm result parsers use bad-node IDs only after logs are collected.

## Fake-Control Red Flags

Treat a result as invalid or diagnostic-only if any of these happen:

- All replicas receive a bad-node list and use it to skip, timeout, route, vote, or select leaders.
- The benchmark client knows the bad-node IDs before the run and avoids them directly.
- A parser reports post-exclusion performance, but the live consensus never activated the exclusion through a certificate.
- A manual config sets bad nodes to minimum weight and the result is reported as if the plugin discovered that by itself.
- Timeout or leader-selection logic checks a test-only bad-node list instead of using normal protocol state.
- Reputation computation infers leader failure from a stale default schedule while dynamic leader selection is active.

Manual controls are still useful, but label them clearly. For example, “manual-min bad leaders” can prove the consensus path can run fast once bad leaders are ineligible, but it does not prove the reputation pipeline can make them ineligible.

## Recommended Staged Workflow

1. Run targeted unit tests for the code touched.
2. Build `//benchmark/protocols/td_hotstuff:kv_server_performance`.
3. Run one pilot experiment only:
   - If the hot path, evidence recorder, plugin worker, or client routing changed, start with no-Byzantine 20 replicas + 1 client.
   - If the change is specifically about Byzantine handling, start with one representative Byzantine case such as 3 bad nodes.
4. Parse the pilot before running more cases. Check throughput, latency, active weights, leader eligibility, activation logs, protocol mismatches, and env isolation.
5. Continue to the full matrix only after the pilot is clean.
6. If the pilot fails, do not run more experiments. Find the failing layer first.

## Result Summary Requirements

A useful experiment summary should include:

- Run label and exact code version.
- Key env values and config files used.
- Whether bad-node env was isolated correctly.
- Throughput and latency.
- Throughput before and after the first activation where all bad nodes are below the leader eligibility threshold, when applicable.
- Final active voting weights.
- Final active leader weights and eligible leader set.
- Activation count and activation views.
- Protocol mismatch counters: proposal invalid, leader mismatch, leader context mismatch, invalid timeout cert, verify failures.
- Whether the result is a real mechanism result or a diagnostic control.

## Current Lessons Learned

- Server-side work is the source of truth. Local Codex copies can drift and create misleading conclusions.
- Normal no-Byzantine 20-replica + 1-client throughput should usually be around `82000+`. If it is much lower, debug the benchmark/client/deploy pipeline before running a Byzantine matrix.
- The client should not know bad nodes. Dynamic routing must be based on real observed responses or protocol-visible state, not bad-node IDs.
- Preserve learned healthy ingress across benchmark retry release; clearing it can make the client repeatedly fall back to stale predicted leaders.
- Do not infer dynamic leader opportunities from `DefaultLeaderForView` once live leader selection is enabled.
- A timeout certificate is real consensus evidence, but it can still be noisy. Use it carefully for reputation; do not let ordinary benchmark/request starvation punish honest leaders.
- Manual-min weight experiments are valuable controls. They can prove whether the system would recover after exclusion, but they do not prove the plugin caused exclusion.

## Useful Environment Notes

- `TD_HS_SILENT_LEADER_IDS` may be used by the launcher to decide which processes receive local fault injection, but it must not be exported to all replicas or clients.
- `TD_HS_SILENT_LEADER=1` should appear only in the faulty replica process that is being simulated as silent.
- `TD_HS_BAD_NODE_IDS` and `TD_HS_BAD_NODE_COUNT` are post-run parser metadata only. They should be scrubbed from live client and normal replica processes.
- Reusable benchmark and reputation defaults should stay in shared env files and remain overrideable. Avoid hard-coding settings for a single experiment unless the script restores them.
