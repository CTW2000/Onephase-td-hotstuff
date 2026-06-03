## Project operation philosophy

- Treat the server checkout as the source of truth for this project. Operate code, configs, deploy scripts, experiment scripts, and benchmark runs in `10.10.131.205:/usr/TD-hotstuff/Onephase-td-hotstuff` unless the user explicitly asks otherwise.
- Do not let a local Codex checkout become a parallel working tree. If a local copy is used only for inspection, immediately sync the server checkout and verify file hashes before running experiments.
- Before every experiment, verify the server-side code/config version and baseline experiment config from the server checkout. Do not rely on local state when judging performance.
- Keep changes reviewable as server-side git diffs and do not commit unless the user explicitly asks for a commit.
- Keep experiment-specific attack settings transient. Experiment runners that generate configs such as slow-vote or slow-leader settings must restore shared baseline config files before exit, and reusable environment defaults should stay overrideable instead of being hard-coded for one run.

Codex 改代码，但不提交 commit；我在 Cursor 的 Git 面板里审查 diff，再决定接受或丢弃。



## End-to-end pipeline checks

- When adding a new function or changing existing behavior, check the whole live pipeline before experiments: consensus protocol logic, reputation plugin logic, client request routing, benchmark deployment scripts, and metric parsing/summary code.
- Do not assume a performance drop is caused by the new protocol idea until the client pipeline, benchmark routing, generated configs, environment propagation, and result parser have been checked against the active leader/weight/timeout rules.
- In normal no-Byzantine 20-replica + 1-client experiments, throughput should recover to the normal baseline, usually around `82000+`, unless the new mechanism intentionally adds unavoidable work. If no-Byzantine throughput stays far below that level, treat it as a bug or pipeline mismatch and find the source before running larger Byzantine experiments.
- Before interpreting Byzantine results, verify that metric logic reports both aggregate and post-activation behavior, including latency, throughput, protocol mismatch counters, active weights, leader eligibility, and benchmark routing decisions.

## Server version synchronization

- For any change that can affect build output, runtime behavior, deployment scripts, experiment configs, or dependencies, keep all six server checkouts synchronized before redeploying or running experiments.
- Verify every server reports the same branch and commit before deployment, for example with `git branch --show-current` and `git rev-parse --short HEAD`.
- Current paths: controller `10.10.131.205:/usr/TD-hotstuff/Onephase-td-hotstuff`; replica checkouts `~/Onephase-td-hotstuff` on `10.10.131.224`, `10.10.131.247`, `10.10.131.86`, `10.10.131.125`, and `10.10.131.83`.
- Replicas do not strictly need the full source checkout to run after deployment because the deploy script copies binaries, configs, and certificates into `~/resilientdb_app`. Still, keeping the replica checkouts synced prevents debugging/redeploy mistakes and makes version checks deterministic.
- Documentation-only changes do not require redeployment. Sync the checkout only when the documentation itself should be available on every server.
- Do not commit automatically; leave diffs for Cursor review unless the user explicitly asks to commit.
