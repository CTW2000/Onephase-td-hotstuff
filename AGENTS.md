## Project operation philosophy

- Treat the server checkout as the source of truth for this project. Operate code, configs, deploy scripts, experiment scripts, and benchmark runs in `10.10.131.205:/usr/TD-hotstuff/Onephase-td-hotstuff` unless the user explicitly asks otherwise.
- Do not let a local Codex checkout become a parallel working tree. If a local copy is used only for inspection, immediately sync the server checkout and verify file hashes before running experiments.
- Before every experiment, verify the server-side code/config version and baseline experiment config from the server checkout. Do not rely on local state when judging performance.
- Keep changes reviewable as server-side git diffs and do not commit unless the user explicitly asks for a commit.
- Keep experiment-specific attack settings transient. Experiment runners that generate configs such as slow-vote or slow-leader settings must restore shared baseline config files before exit, and reusable environment defaults should stay overrideable instead of being hard-coded for one run.

Codex 改代码，但不提交 commit；我在 Cursor 的 Git 面板里审查 diff，再决定接受或丢弃。



## Server version synchronization

- For any change that can affect build output, runtime behavior, deployment scripts, experiment configs, or dependencies, keep all six server checkouts synchronized before redeploying or running experiments.
- Verify every server reports the same branch and commit before deployment, for example with `git branch --show-current` and `git rev-parse --short HEAD`.
- Current paths: controller `10.10.131.205:/usr/TD-hotstuff/Onephase-td-hotstuff`; replica checkouts `~/Onephase-td-hotstuff` on `10.10.131.224`, `10.10.131.247`, `10.10.131.86`, `10.10.131.125`, and `10.10.131.83`.
- Replicas do not strictly need the full source checkout to run after deployment because the deploy script copies binaries, configs, and certificates into `~/resilientdb_app`. Still, keeping the replica checkouts synced prevents debugging/redeploy mistakes and makes version checks deterministic.
- Documentation-only changes do not require redeployment. Sync the checkout only when the documentation itself should be available on every server.
- Do not commit automatically; leave diffs for Cursor review unless the user explicitly asks to commit.
