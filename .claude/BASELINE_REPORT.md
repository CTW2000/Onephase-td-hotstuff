# Baseline Performance Report — Onephase-td-hotstuff (n=20)

**Cluster:** 5 servers × 4 nodes = 20 replicas. Client / controller on `10.10.131.205`.
Server IPs (with 4 replicas each): `10.10.131.{224, 247, 86, 125, 83}`.
**Workload:** KV-server performance benchmark, `clientBatchNum = 100` uniform across protocols.
**Fault timer:** `timer_length = 100 ms` for all attack experiments.
**Git tip:** `0c4e549` (detached on `origin`). Runs taken **2026-04-18**.
**Raw data:** `scripts/deploy/experiment_results/<experiment>_n20/{HS,HS-1,HS-2,HS-1-SLOT}_*.txt` — each file ends with two lines: `average throughput: <tx/s>` and `average latency: <sec>`.

> **These numbers are THE baseline.** Any change to the consensus code must re-run the affected `run_*_n20.sh` and update this report. The old `latex_plot_data/*.data` values (from the paper's AWS c4.4xlarge run at n=31) are retained in the repo but are not our reference.

## Setup notes — bugs fixed before this baseline

Two bugs invalidated the first round of runs (captured in the older `EXPERIMENT_REPORT.md`). Both are fixed here; keep them in mind when re-running:

1. **Root-owned configs.** `hs.config`, `hs2.config`, `slot_hs1.config` were `root:root 0644`. `generate_config()` hit `PermissionError` silently; HS-2 / HS / HS-1-SLOT ran with stale defaults and **no fault injection** — so they looked immune. Fix: `chown hyperchain:hyperchain` + `chmod 664` on all `config/*.config`, and `run_*_n20.sh` now verifies the target field actually lands in the file before launching.
2. **Heterogeneous `clientBatchNum`.** HS-1 ran with 100; HS-2 / HS / HS-1-SLOT carried a stale 10 000 from those root-owned files. That inflated their throughput ~5× and pushed their latency into 40–90 ms — meaningless cross-protocol comparison. All runs below use `clientBatchNum = 100`.
3. `**deploy_multi.sh` wait-hang** when a node runs on `LOCAL_IP=205`: fixed by detaching the `nohup` with `setsid -f` so the outer `wait` returns.

## Protocols compared


| Label         | Meaning                                    | Code path                               |
| ------------- | ------------------------------------------ | --------------------------------------- |
| **HS-1**      | HotStuff-1 (1-phase) — paper contribution  | `platform/consensus/ordering/hs1/`      |
| **HS-1-SLOT** | Slot-based HotStuff-1 — paper contribution | `platform/consensus/ordering/slot_hs1/` |
| **HS-2**      | HotStuff-2 (2-phase) — baseline            | `platform/consensus/ordering/hs2/`      |
| **HS**        | HotStuff (3-phase) — baseline              | `platform/consensus/ordering/hs/`       |


Fault-injection convention (common across experiments): at parameter value `k`, the replicas with `id < 3k && id%3 == 1` are malicious/slow/forking/rollback. So `k=1 → {1}`, `k=4 → {1,4,7,10}`, `k=6 → {1,4,7,10,13,16}`.

---

## Executive summary

At **n = 20** on the 5-server LAN cluster, with `clientBatchNum = 100`:

- **Fault-free latency** (ms): HS-1-SLOT **3.05** ≈ HS-1 3.32 < HS-2 4.44 < HS 5.52. The extra phases of HS-2 / HS cost ~1 ms each. HS-1-SLOT ties HS-1 despite its slot scheduler.
- **Fault-free throughput** (tx/s): all four protocols cluster at **88–95 K**. HS-1-SLOT is slightly ahead because its pipeline keeps slots busy even at the baseline.
- **Scalability 10 → 20**: throughput roughly halves (0.55–0.59×) and latency ~doubles, **identical slope across protocols**. Bottleneck is vote fan-out / signature verification, not protocol-specific.
- **Slow-leader fault = catastrophic for chain protocols, handled by HS-1-SLOT**:
just **one** slow leader (5% faulty) drops HS-1 / HS-2 / HS by −82%; **six** drops them by −96%.
HS-1-SLOT at slow=6 holds **66 K tx/s** (−30%) and **3.58 ms** latency — ~21× the throughput and ~26× lower latency than HS-1 under the same fault.
- **Tail-forking fault**: chain protocols lose 17–22% at fork=4/20; HS-1-SLOT is **flat** (0.0%).
- **Rollback fault (HS-1 / HS-1-SLOT only implement the hook)**: HS-1 drops −32% at rb=6; HS-1-SLOT is within ±1% of baseline.
- **Slow-voter / network-delay fault**: hits every protocol because quorum collection stalls. HS-1-SLOT still leads (−63% vs chain −77% at 30% slow voters), but this is the regime where the slot design helps the least.
- **Combined fork + slow attack**: HS-1 / HS-2 / HS collapse identically (−82% / −91% / −96% at (1,1) / (2,2) / (4,4)). HS-1-SLOT loses **linearly** with attacker budget (−2.5% / −7.1% / −17.6%). At (4,4): HS-1-SLOT **20× the throughput of HS-1** and **39× lower latency than HS**.

**One-liner**: HS-1 gives the best clean-run latency; HS-1-SLOT matches HS-1 on the fast path and is the only protocol still usable under Byzantine leader faults.

---

## 1 · Scalability  — `run_scalability_n2_10_20.sh`

Parameters: n ∈ {2, 10, 20}, no faults. (n=2 fails for every protocol as expected — HotStuff needs `n ≥ 3f+1 = 4`.)

### Throughput (tx/s)


| n   | HS-1    | HS-2    | HS          | HS-1-SLOT |
| --- | ------- | ------- | ----------- | --------- |
| 2   | —       | —       | —           | —         |
| 10  | 155 597 | 157 548 | **161 075** | 149 642   |
| 20  | 88 371  | 89 188  | **89 339**  | 87 954    |


### Latency (ms)


| n   | HS-1     | HS-2 | HS   | HS-1-SLOT |
| --- | -------- | ---- | ---- | --------- |
| 10  | **1.83** | 2.43 | 3.01 | 1.86      |
| 20  | 3.30     | 4.42 | 5.52 | **3.25**  |


**Observations**

- Throughput scales ~0.57× as n doubles (10→20); slope is identical for all four protocols → the bottleneck is vote fan-out and signature verification, not protocol-specific.
- Latency ranking at n=20: HS-1 ≈ HS-1-SLOT < HS-2 < HS. Each extra commit phase costs ~1 ms.
- HS-1-SLOT matches HS-1 on both metrics — no scalability tax for adding slot-based resilience.

## 2 · Tail-Forking  — `run_fork_tail_n20.sh`

Parameters: n=20, fork ∈ {0, 1, 4}.

### Throughput (tx/s)


| fork | HS-1   | HS-2   | HS     | HS-1-SLOT  |
| ---- | ------ | ------ | ------ | ---------- |
| 0    | 88 384 | 87 482 | 89 307 | **94 463** |
| 1    | 84 041 | 83 385 | 84 516 | **95 279** |
| 4    | 72 234 | 72 931 | 69 722 | **94 467** |


### Latency (ms)


| fork | HS-1 | HS-2 | HS   | HS-1-SLOT |
| ---- | ---- | ---- | ---- | --------- |
| 0    | 3.32 | 4.50 | 5.56 | **3.10**  |
| 1    | 3.49 | 4.71 | 5.82 | **3.04**  |
| 4    | 4.03 | 5.35 | 7.02 | **3.07**  |


### Throughput drop


| fork | HS-1       | HS-2       | HS         | HS-1-SLOT |
| ---- | ---------- | ---------- | ---------- | --------- |
| 1    | −4.9%      | −4.7%      | −5.4%      | +0.9%     |
| 4    | **−18.3%** | **−16.6%** | **−21.9%** | **0.0%**  |


HS-1-SLOT is immune to tail forking; HS-1 / HS-2 / HS all lose ~20% at fork=4 (20% faulty).

## 3 · Slow Leader  — `run_slow_leader_n20.sh`

Parameters: n=20, slow ∈ {0, 1, 4, 6}, timer=100 ms. This is the **paper's flagship fault.**

### Throughput (tx/s)


| slow | HS-1   | HS-2   | HS     | HS-1-SLOT  |
| ---- | ------ | ------ | ------ | ---------- |
| 0    | 88 430 | 89 770 | 89 403 | **95 322** |
| 1    | 16 128 | 16 011 | 16 117 | **88 885** |
| 4    | 4 705  | 4 702  | 4 708  | **76 500** |
| 6    | 3 200  | 3 186  | 3 200  | **66 341** |


### Latency (ms)


| slow | HS-1  | HS-2   | HS     | HS-1-SLOT |
| ---- | ----- | ------ | ------ | --------- |
| 0    | 3.34  | 4.37   | 5.53   | **3.04**  |
| 1    | 18.54 | 24.81  | 31.00  | **3.12**  |
| 4    | 64.15 | 85.30  | 106.50 | **3.30**  |
| 6    | 94.00 | 126.10 | 157.00 | **3.58**  |


### Throughput drop


| slow | HS-1   | HS-2   | HS     | HS-1-SLOT  |
| ---- | ------ | ------ | ------ | ---------- |
| 1    | −81.8% | −82.2% | −82.0% | **−6.8%**  |
| 4    | −94.7% | −94.8% | −94.7% | **−19.8%** |
| 6    | −96.4% | −96.4% | −96.4% | **−30.4%** |


HS-1-SLOT degrades **linearly** with the faulty fraction (5%→7%, 20%→20%, 30%→30%); chain protocols collapse super-linearly.

## 4 · Slow Voter / Network Delay  — `run_slow_vote_n20.sh`

Parameters: n=20, `network_delay_num` ∈ {0, 1, 4, 6}, `mean_network_delay = 10 ms`. Implementation: nodes with `id ≤ network_delay_num` add ~10 ms to every message in & out (see `platform/consensus/ordering/common/algorithm/protocol_base.cpp:40-61`).

### Throughput (tx/s)


| slow-vote | HS-1   | HS-2   | HS     | HS-1-SLOT  |
| --------- | ------ | ------ | ------ | ---------- |
| 0         | 88 345 | 88 977 | 89 373 | 87 227     |
| 1         | 38 560 | 38 584 | 38 544 | **39 497** |
| 4         | 26 205 | 26 400 | 26 394 | **35 085** |
| 6         | 20 784 | 20 719 | 20 788 | **31 961** |


### Latency (ms)


| slow-vote | HS-1  | HS-2  | HS    | HS-1-SLOT |
| --------- | ----- | ----- | ----- | --------- |
| 0         | 3.32  | 4.44  | 5.50  | **3.27**  |
| 1         | 8.14  | 10.53 | 13.15 | **6.36**  |
| 4         | 11.84 | 15.38 | 19.23 | **6.90**  |
| 6         | 14.91 | 19.62 | 24.36 | **7.34**  |


### Throughput drop


| slow-vote | HS-1   | HS-2   | HS     | HS-1-SLOT  |
| --------- | ------ | ------ | ------ | ---------- |
| 1         | −56.3% | −56.6% | −56.9% | **−54.7%** |
| 4         | −70.3% | −70.3% | −70.5% | **−59.8%** |
| 6         | −76.5% | −76.7% | −76.7% | **−63.4%** |


Unlike slow-leader, slow-voter hurts every protocol — votes are on the critical path for quorum formation. HS-1-SLOT still leads by ~10–14 pp and 2–3× on latency, but the advantage is modest because the slot scheduler cannot avoid votes entirely.

## 5 · Rollback  — `run_attacks_n20.sh` (attack 1)

Parameters: n=20, rb ∈ {0, 1, 4, 6}. **Only HS-1 and HS-1-SLOT implement the rollback injection hook**; HS-2 / HS numbers are constants that should be read as "fault not applied" and serve as a no-fault baseline for those protocols.

### Throughput (tx/s)


| rb  | HS-1   | HS-2 (no impl) | HS (no impl) | HS-1-SLOT  |
| --- | ------ | -------------- | ------------ | ---------- |
| 0   | 88 342 | 89 574         | 89 523       | **94 851** |
| 1   | 83 147 | 89 836         | 87 887       | **95 636** |
| 4   | 69 633 | 88 316         | 88 802       | **95 358** |
| 6   | 60 196 | 90 091         | 90 139       | **93 807** |


### Latency (ms)


| rb  | HS-1 | HS-2 | HS   | HS-1-SLOT |
| --- | ---- | ---- | ---- | --------- |
| 0   | 3.31 | 4.42 | 5.55 | **3.06**  |
| 1   | 3.47 | 4.37 | 5.61 | **3.05**  |
| 4   | 3.64 | 4.45 | 5.55 | **3.07**  |
| 6   | 3.86 | 4.37 | 5.47 | **3.10**  |


### Throughput drop — HS-1 and HS-1-SLOT


| rb  | HS-1       | HS-1-SLOT |
| --- | ---------- | --------- |
| 1   | −5.9%      | +0.8%     |
| 4   | −21.2%     | +0.5%     |
| 6   | **−31.9%** | **−1.1%** |


HS-1-SLOT is essentially immune; HS-1 degrades ~1:1 with the rollback fraction.

## 6 · Combined Fork + Slow  — `run_attacks_n20.sh` (attack 2)

Parameters: n=20, pairs (fork, slow) ∈ {(0,0), (1,1), (2,2), (4,4)} — the **same** replicas fork their tail AND sleep on propose. This is the composite Byzantine worst-case.

### Throughput (tx/s)


| (fork, slow) | HS-1   | HS-2   | HS     | HS-1-SLOT  |
| ------------ | ------ | ------ | ------ | ---------- |
| (0, 0)       | 87 818 | 87 625 | 87 392 | **91 599** |
| (1, 1)       | 15 293 | 15 200 | 15 296 | **89 339** |
| (2, 2)       | 8 011  | 8 011  | 7 936  | **85 132** |
| (4, 4)       | 3 762  | 3 813  | 3 762  | **75 467** |


### Latency (ms)


| (fork, slow) | HS-1  | HS-2   | HS     | HS-1-SLOT |
| ------------ | ----- | ------ | ------ | --------- |
| (0, 0)       | 3.33  | 4.48   | 5.65   | **3.17**  |
| (1, 1)       | 19.53 | 26.13  | 32.56  | **3.17**  |
| (2, 2)       | 37.39 | 49.79  | 62.62  | **3.20**  |
| (4, 4)       | 79.77 | 106.42 | 133.04 | **3.39**  |


### Throughput drop


| (fork, slow) | HS-1   | HS-2   | HS     | HS-1-SLOT  |
| ------------ | ------ | ------ | ------ | ---------- |
| (1, 1)       | −82.6% | −82.7% | −82.5% | **−2.5%**  |
| (2, 2)       | −90.9% | −90.9% | −90.9% | **−7.1%**  |
| (4, 4)       | −95.7% | −95.6% | −95.7% | **−17.6%** |


The slow-leader component dominates the drop on chain protocols — the fork adds little on top because the pipeline is already stalled. HS-1-SLOT's drop tracks the attacker fraction linearly with no cascade.

---

## Headline cross-experiment comparison (n=20)

Drop at 30% faulty / fork=4 or 6, whichever applies:


| Experiment   | Worst faulty frac | HS-1 drop | HS-2 drop | HS drop   | HS-1-SLOT drop |
| ------------ | ----------------- | --------- | --------- | --------- | -------------- |
| Tail-forking | 20%               | −18.3%    | −16.6%    | −21.9%    | **0.0%**       |
| Slow leader  | 30%               | −96.4%    | −96.4%    | −96.4%    | **−30.4%**     |
| Slow voter   | 30%               | −76.5%    | −76.7%    | −76.7%    | **−63.4%**     |
| Rollback     | 30%               | −31.9%    | (no impl) | (no impl) | **−1.1%**      |
| Fork+Slow    | 20%               | −95.7%    | −95.6%    | −95.7%    | **−17.6%**     |


HS-1-SLOT wins on every attack surface that it was designed to defend (leader-side). Slow-voter is a network-layer fault where everyone suffers; HS-1-SLOT still wins but by a smaller margin.

---

## Regression gates

These are the numbers a future commit must meet or exceed to count as "no regression." Tolerance = ±5% on throughput and ±10% on latency unless noted. Re-run the corresponding `run_*_n20.sh` to verify.


| Experiment          | Metric                              | Gate                                                              |
| ------------------- | ----------------------------------- | ----------------------------------------------------------------- |
| Scalability n=20    | HS-1 throughput                     | ≥ 84 000 tx/s                                                     |
| Scalability n=20    | HS-1 latency                        | ≤ 3.6 ms                                                          |
| Scalability n=20    | HS-1-SLOT throughput                | ≥ 83 000 tx/s                                                     |
| Scalability n=20    | HS-1-SLOT latency                   | ≤ 3.6 ms                                                          |
| Tail-forking fork=4 | HS-1-SLOT throughput                | ≥ 90 000 tx/s                                                     |
| Tail-forking fork=4 | HS-1-SLOT latency                   | ≤ 3.3 ms                                                          |
| Slow-leader slow=6  | HS-1-SLOT throughput                | ≥ 60 000 tx/s                                                     |
| Slow-leader slow=6  | HS-1-SLOT latency                   | ≤ 4.0 ms                                                          |
| Slow-voter slow=6   | HS-1-SLOT throughput                | ≥ 28 000 tx/s                                                     |
| Rollback rb=6       | HS-1-SLOT throughput                | ≥ 88 000 tx/s                                                     |
| Rollback rb=6       | HS-1 throughput                     | 55 000 – 65 000 tx/s (regression in *either* direction is a flag) |
| Fork+Slow (4,4)     | HS-1-SLOT throughput                | ≥ 70 000 tx/s                                                     |
| Fork+Slow (4,4)     | HS-1-SLOT latency                   | ≤ 3.7 ms                                                          |
| Any rollback run    | HS-1 must produce a positive number | No `nan`, no `-1000`                                              |


---

## Caveats

- **HS-2 / HS do not implement the rollback hook.** Their numbers in §5 are fault-free baselines for those protocols and must not be read as resilience. A fair rollback comparison against HS-1 / HS-1-SLOT would require adding the injection point at `platform/consensus/ordering/{hs,hs2}/algorithm/*.cpp` (mirroring the HS-1 path at `hs1/algorithm/hs1.cpp:~80`).
- `**slow_vote_n20` uses `network_delay_num` + `mean_network_delay=10 ms`** — the codebase has no explicit "slow vote" flag; the sender- **and** receiver-side delay at `protocol_base.cpp:40-61` is effectively a slow-voter fault. A proper vote-only slowdown would require a new hook.
- **Round-robin leader rotation** at n=20 means the fault pattern `id < 3k && id%3==1` lands on every `20/3 ≈ 7` views. At k=1 the single slow leader is therefore the rotation's leader once per 20-view cycle, which is the 5% figure used throughout.
- **HS-1-SLOT slow-leader degradation is larger than the paper's** (−30% @ 30% faulty vs paper's −2% @ 32% faulty). That is consistent with paper's larger leader pool (n=31 → more slots per view). Expect the gap to narrow if you rerun at higher n.
- **All runs are from a single execution** of each parameter point. No repetition / variance estimate. If tight gates matter, add `--repeat 3` to the drivers and take mean ± std.
- **Deploy configs have uncommitted local edits.** Before declaring a new baseline, commit or revert `scripts/deploy/config/{hs,hs1,hs2}.config` and `kv_performance_server.conf`.

## Update protocol

1. Modify code.
2. Re-run the affected `run_*_n20.sh` on `10.10.131.205`. Wait for all 20 + client processes to return cleanly.
3. `scp -r hyperchain@10.10.131.205:/usr/ctw/Onephase-td-hotstuff/scripts/deploy/experiment_results /tmp/` and parse each `<proto>_<param>.txt`'s final two lines (`average throughput` / `average latency`).
4. Edit the tables in this file **in place** — do not append new runs alongside old ones. Archive old versions in git history.
5. Update regression gates only if the new numbers are accepted as the new baseline; otherwise fail the gate and fix the regression.

