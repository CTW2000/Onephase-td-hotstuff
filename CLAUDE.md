# Onephase-td-hotstuff — Project Memory

> Maintained for Claude Code sessions. Keep it short, factual, and current.
> **Rule:** update this file in place whenever project layout, protocols, experiment set, or baseline numbers change. Prune stale facts — do not accrete.

## What this project is

Apache ResilientDB fork implementing **HotStuff-1** (one-phase Byzantine consensus, [ACM TODS 10.1145/3725308](https://dl.acm.org/doi/10.1145/3725308)) plus baselines, used as the reproducibility artifact for the paper **and** as the harness for our own cluster experiments.

- Language / build: **C++ / Bazel** (`MODULE.bazel`, `WORKSPACE`)
- Repo root on server: `/usr/ctw/Onephase-td-hotstuff` (host `10.10.131.205`, user `hyperchain`)
- Git: detached on `origin` ≈ `origin/main`, tip `0c4e549`. Local uncommitted edits to `scripts/deploy/config/{hs,hs1,hs2}.config` + `kv_performance_server.conf`.
- Other branches: `main`, `dynamicTimeout`, `epochManager`, `weighted-QC`.

## Cluster (our baseline setup)

6 Ubuntu 22.04 servers, all reachable with user `hyperchain` (credentials stored locally, not in this repo — ask the maintainer):

| Role | IP | Hostname | Replicas |
|---|---|---|---|
| Controller / client | `10.10.131.205` | `web3-root` | — |
| Server 1 | `10.10.131.224` | `root-3` | 4 |
| Server 2 | `10.10.131.247` | `root-2` | 4 |
| Server 3 | `10.10.131.86`  | `root-5` | 4 |
| Server 4 | `10.10.131.125` | `root-1` | 4 |
| Server 5 | `10.10.131.83`  | `root-4` | 4 |

**Replica count:** 20 (5 × 4). This is the `n=20` that every `run_*_n20.sh` assumes. Scalability sweeps run n=2 / 10 / 20.

Local SSH aliases in `~/.ssh/config`: `hyperchain-205`, `hyperchain-node{1..5}`. **Known mismatch:** the `hyperchain-node{N}` alias does not line up with the server's own hostname `root-N`; fix before using the numeric alias.

## Protocols under evaluation

| Label | Role | Code path |
|---|---|---|
| **HS-1** | HotStuff-1 (1-phase) — paper contribution | `platform/consensus/ordering/hs1/` |
| **HS-1-SLOT** | Slot-based HotStuff-1 — paper contribution | `platform/consensus/ordering/slot_hs1/` |
| **HS-2** | HotStuff-2 (2-phase) — baseline | `platform/consensus/ordering/hs2/` |
| **HS** | HotStuff (3-phase) — baseline | `platform/consensus/ordering/hs/` |
| compiled but not in eval: `pbft`, `poe`, `geo_pbft`, `zzy` | — | `platform/consensus/ordering/*` |

Each protocol has `algorithm/` (state machine + proposal manager), `framework/` (ResilientDB integration, performance manager), `proto/` (wire format).

Fault-injection convention across experiments: at parameter value `k`, the malicious / slow replicas are those with `id < 3k && id%3 == 1`. So `k=1 → {1}`, `k=4 → {1,4,7,10}`, `k=6 → {1,4,7,10,13,16}`. Defined in `platform/consensus/ordering/*/algorithm/*.cpp` (fork hook) and `platform/consensus/ordering/common/algorithm/protocol_base.cpp:40-61` (network-delay hook).

## Experiment set (our n=20 baseline, the reference for future work)

All drivers live under `scripts/deploy/`. Runs are launched by `run_*_n20.sh`, which invoke the Python configurators then the per-protocol performance scripts on the 5 servers. Per-run output lives in `scripts/deploy/experiment_results/<exp>_n20/<proto>_<param>.txt`, with the last two lines of each file giving `average throughput:` (tx/s) and `average latency:` (sec).

| Experiment | Driver | Parameters tested | Protocols |
|---|---|---|---|
| Scalability | `run_scalability_n2_10_20.sh` | n ∈ {2, 10, 20} | all 4 |
| Tail-Forking | `run_fork_tail_n20.sh` | fork ∈ {0, 1, 4}, timer=100 ms | all 4 |
| Slow Leader | `run_slow_leader_n20.sh` | slow ∈ {0, 1, 4, 6}, timer=100 ms | all 4 |
| Slow Voter | `run_slow_vote_n20.sh` | `network_delay_num` ∈ {0,1,4,6}, delay=10 ms | all 4 |
| Rollback | `run_attacks_n20.sh` (attack 1) | rb ∈ {0, 1, 4, 6}, timer=100 ms | HS-1 + HS-1-SLOT only (HS / HS-2 have no hook) |
| Combined Fork + Slow | `run_attacks_n20.sh` (attack 2) | (fork, slow) ∈ {(0,0),(1,1),(2,2),(4,4)} | all 4 |

All runs use `clientBatchNum = 100`, uniform across protocols.

### Known-bad older runs — do not use

- `experiment_results/EXPERIMENT_REPORT.md` (n=19)
- `experiment_results/scalability/`, `experiment_results/tail_forking/`, `experiment_results/leader_slowness/`, `experiment_results/rollback/`

Those runs happened before the config-permission fix and with `clientBatchNum=10000` baked into root-owned configs, so HS-2 / HS / HS-1-SLOT appeared immune to every fault. Keep them only for the bug-fix postmortem; never treat them as data.

### Paper's AWS runs — do not use as our baseline

`scripts/deploy/latex_plot_data/*.data` hold the paper's AWS c4.4xlarge results at n=31. They are the paper's numbers, not ours. Leave them alone for reproducibility documentation; our baseline is the n=20 data in `.claude/BASELINE_REPORT.md`.

## Baseline results

**Full consolidated baseline:** [`.claude/BASELINE_REPORT.md`](.claude/BASELINE_REPORT.md). Treat those numbers as the **reference baseline** for all future commits. Any perf-affecting change must re-run the relevant `run_*_n20.sh` and update the report in place.

Headline (detailed numbers and regression gates in the report):
- **Fault-free, n=20**: HS-1-SLOT 3.05 ms ≤ HS-1 3.32 ms < HS-2 4.44 ms < HS 5.52 ms latency; throughput clustered at 88–95 K tx/s.
- **Slow leader (flagship result)**: at 30% faulty, HS-1 / HS-2 / HS drop −96%; HS-1-SLOT drops −30%. HS-1-SLOT retains 66 K tx/s and 3.58 ms latency, ~21× better throughput and ~26× lower latency than HS-1.
- **Tail-forking (20% faulty)**: HS-1-SLOT flat; HS-1 / HS-2 / HS lose ~20%.
- **Rollback (30% faulty)**: HS-1-SLOT ±1%; HS-1 −32%. (HS, HS-2 have no rollback hook implemented yet.)
- **Slow-voter (30% faulty)**: everyone suffers (vote-path bottleneck); HS-1-SLOT still leads by 14 pp and 2–3× on latency.
- **Combined fork + slow at (4,4)**: chain protocols −96%; HS-1-SLOT −18% (graceful linear degradation).

## Update protocol for this file

1. Change code → re-run the affected `run_*_n20.sh` on the server.
2. `scp -r hyperchain@10.10.131.205:/usr/ctw/Onephase-td-hotstuff/scripts/deploy/experiment_results ...` locally.
3. Regenerate [`.claude/BASELINE_REPORT.md`](.claude/BASELINE_REPORT.md) by editing the tables **in place**. Do not append new runs next to old ones.
4. If the protocol roster, driver set, or cluster layout changed, update the tables above **in place** too.
5. Keep this file under ~200 lines. If it grows, move detail into `.claude/` sub-docs and link.
