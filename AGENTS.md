# Onephase-td-hotstuff — Project Memory

> Maintained for Codex sessions. Keep it short, factual, and current.
> **Rule:** update this file in place whenever project layout, protocols, experiment set, or baseline numbers change. Prune stale facts — do not accrete.

## What this project is

Apache ResilientDB fork implementing **HotStuff-1** (one-phase Byzantine consensus, [ACM TODS 10.1145/3725308](https://dl.acm.org/doi/10.1145/3725308)) plus baselines and our own **TD-HotStuff** protocol (HS-1-SLOT + weighted QC). Evaluation harness covers scalability, batching, and four Byzantine fault scenarios.

- Language / build: **C++ / Bazel** (`MODULE.bazel`, `WORKSPACE`)
- Repo root on server: `/usr/ctw/Onephase-td-hotstuff` (host `10.10.131.205`, user `hyperchain`)
- Current branch: **`origin`** — contains the n=20 baseline work merged with weighted-QC. Tip: `c46ab36 Merge weighted-QC into origin`. Upstream: `origin/origin` on GitHub (`https://github.com/CTW2000/Onephase-td-hotstuff.git`).
- Other branches: `main`, `dynamicTimeout`, `epochManager`, `weighted-QC`.
- Commit identity: git `user.name` is unset; all previous commits authored as `Ubuntu <1043386498@qq.com>`. Use `git -c user.name=Ubuntu commit …` when committing through scripts.

## Cluster

6 Ubuntu 22.04 servers, all user `hyperchain` (credentials stored locally, not in this repo — ask the maintainer):

| Role | IP | Hostname | Replicas |
|---|---|---|---|
| Controller / client | `10.10.131.205` | `web3-root` | — (also local-mode benchmark host) |
| Server 1 | `10.10.131.224` | `root-3` | 4 |
| Server 2 | `10.10.131.247` | `root-2` | 4 |
| Server 3 | `10.10.131.86`  | `root-5` | 4 |
| Server 4 | `10.10.131.125` | `root-1` | 4 |
| Server 5 | `10.10.131.83`  | `root-4` | 4 |

Total distributed replicas: 20 (5 × 4). Local-mode (127.0.0.1 multi-process) runs all replicas on `10.10.131.205`.

Local SSH aliases: `hyperchain-205`, `hyperchain-node{1..5}`. **Known mismatch:** the `hyperchain-node{N}` aliases do not line up with the servers' own hostnames `root-N`; fix before using the numeric alias.

## Protocols

| Label | Meaning | Code path |
|---|---|---|
| **HS-1** | HotStuff-1 (1-phase) — paper contribution | `platform/consensus/ordering/hs1/` |
| **HS-1-SLOT** | Slot-based HotStuff-1 — paper contribution | `platform/consensus/ordering/slot_hs1/` |
| **TD-HS** | TD-HotStuff (HS-1-SLOT + **weighted QC**) — **our contribution** | `platform/consensus/ordering/td_hotstuff/` |
| **HS-2** | HotStuff-2 (2-phase) — baseline | `platform/consensus/ordering/hs2/` |
| **HS** | HotStuff (3-phase) — baseline | `platform/consensus/ordering/hs/` |
| **PBFT** | PBFT — baseline | `platform/consensus/ordering/pbft/` |

### TD-HotStuff delta (vs HS-1-SLOT)

- `proto/proposal.proto` — `Certificate` gains `int32 weight = 7` (signer's trust weight).
- `algorithm/td_hotstuff.{h,cpp}` — ctor takes `const std::vector<int>& weights` (1-based, `weights_[id-1]`); `total_weight_ = Σ weights`, `weight_threshold_ = ⌊2W/3⌋ + 1`; `ReceiveCertificate` dedupes by signer, accumulates weight per (view, hash), and forms the QC the instant weight crosses the threshold. Uniform weights `[1,1,…,1]` reproduce `2f+1` quorum semantics exactly — degenerate-case parity with HS-1-SLOT.
- `framework/consensus.cpp` — weights injected via env var **`TD_HS_WEIGHTS`** (comma-separated ints). Unset → uniform `[1,1,…,1]` default. Weight vector + threshold logged on startup (grep `TD-HotStuff weights:`).
- Fault-injection hooks inherited from HS-1-SLOT unchanged (fork/rollback/slow-leader at `id < 3k && id%3 == 1`).

## Experiment harness

**Primary workflow: `scripts/experiment_manager` Python package.** Older ad-hoc `run_*_n20.sh` scripts have been removed.

```
cd scripts
python3 -m experiment_manager list         # show experiments, suites, protocols
python3 -m experiment_manager run <name>   # run one experiment or suite
  --protocols HS-1 TD-HS …                 # restrict protocols (default: all)
  --mode local | remote                    # local loopback vs. AWS / 5-server cluster
  --results-dir ./experiment_results
  --checkpoint ./experiment_checkpoint.json
python3 -m experiment_manager resume       # resume from checkpoint
python3 -m experiment_manager status       # show checkpoint status
python3 -m experiment_manager reset        # clear failed entries
```

Experiments / suites (from `scripts/experiment_manager/experiments/registry.py`):

| Experiment | Suite | Default sweep | Code |
|---|---|---|---|
| `scalability` | simple | n ∈ {5, 10, 15} | `experiments/scalability.py` |
| `batching` | simple | clientBatchNum sweep | `experiments/batching.py` |
| `leader_slowness` | byzantine | # slow leaders sweep | `experiments/leader_slowness.py` |
| `network_delay` | byzantine | delay tier sweep | `experiments/network_delay.py` |
| `rollback` | byzantine | # rollback replicas sweep | `experiments/rollback.py` |
| `tail_forking` | byzantine | # forkers sweep | `experiments/tail_forking.py` |
| `weighted` | weighted | TD-HS non-uniform weight vectors | `experiments/weighted.py` |
| `protocol_comparison` | weighted | TD-HS random-weight seeds vs. all baselines | `experiments/protocol_comparison.py` |

Suites: `simple` (scalability + batching), `byzantine` (the four Byzantine experiments), `all`, `full` (all + weighted experiments).

Results → `<results-dir>/<experiment>/<run_id>.log`. Checkpoint JSON records status, throughput/latency, and baseline snapshots.

### ⚠️ Known bug — local runner reuses stale `results.log`

As of 2026-04-21 the `local` mode of `experiment_manager run scalability` reports byte-identical numbers for every n in the sweep and finishes each run in ~2 s instead of the expected 40 s. Root cause: the subprocess's exit code isn't checked, and if the `deploy_local.sh` step fails silently, the parser re-reads the previous run's `scripts/deploy/results.log`. A flagged follow-up task exists to fix the runner (add returncode + mtime guards). **Until that lands, do not trust any local-mode sweep that produces suspiciously-identical numbers — verify with `scripts/deploy/local_quick_test.sh <PROTOCOL>` as a ground truth.**

### Baseline sanity check (performed after merge)

- `scripts/deploy/local_quick_test.sh TD-HS` (4 replicas local, 40 s): **avg throughput 174 K tx/s, avg latency 1.40 ms** — sensible for local-loopback, confirms TD-HotStuff binary and the default uniform-weight code path work end-to-end.
- `python3 -m experiment_manager list` loads cleanly and enumerates all 8 experiments, 4 suites, 6 protocols.
- `python3 -m experiment_manager run scalability --protocols TD-HS --mode local`: pipeline executes, writes checkpoint, all three runs reported "PASSED". Numbers themselves affected by the stale-results bug above, so they do not constitute a real baseline — only a merge-correctness smoke test.

### Reference baseline (distributed, 5-server × 4-node)

Kept at [`.codex/BASELINE_REPORT.md`](.codex/BASELINE_REPORT.md). That file holds the n=20 reference numbers produced before this merge; they still stand for HS-1, HS-1-SLOT, HS-2, HS under the six experiment types we ran at n=20. Future distributed experiments via `experiment_manager run ... --mode remote` must match or beat those numbers — the regression gates there are the acceptance bar.

## Repo layout changes landed by the merge

New on this branch (`c46ab36`):
- `platform/consensus/ordering/td_hotstuff/` + `benchmark/protocols/td_hotstuff/` + `scripts/deploy/config/td_hotstuff.config` + `scripts/deploy/performance/td_hotstuff_performance.sh` + `scripts/deploy/performance_local/td_hotstuff_performance.sh`
- `scripts/experiment_manager/` — 22-file Python package
- `scripts/deploy/local_*` — local (non-AWS) experiment helpers: `local_experiment.py`, `local_quick_test.sh`, `local_scalability_experiment.sh`, `local_batching_experiment.sh`, `local_leader_slowness_experiment.sh`, `local_network_delay_experiment.sh`, `local_rollback_experiment.sh`, `local_tailforking_experiment.sh`, `local_run_all_experiments.sh`
- `scripts/deploy/run_protocol_comparison.sh` — TD-HS random-weight vs. baseline sweep

Removed on this branch (from the n=20 ad-hoc work):
- `scripts/deploy/experiment_results/` (raw data); baseline numbers migrated to `.codex/BASELINE_REPORT.md`
- `scripts/deploy/experiment_full_log.txt`

## Update protocol for this file

1. Change code → re-run the affected experiments via `experiment_manager` (preferred) or the `local_*` shortcut scripts.
2. Update `.codex/BASELINE_REPORT.md` in place with new tables; do not append old and new side-by-side.
3. If protocol roster, experiment registry, or cluster layout changed, update the tables above **in place**.
4. Keep this file under ~200 lines. If it grows, move detail into `.codex/` sub-docs and link.
