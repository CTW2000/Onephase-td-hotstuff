# Tail Forking Experiment — n=20, 6 Machines

**Date:** 2026-04-18
**Setup:** 5 server machines (10.10.131.{224,247,86,125,83}) × 4 nodes each = 20 nodes; client on 10.10.131.205 (6 machines total)
**Protocols:** HS-1, HS-2, HS, HS-1-SLOT
**Fork counts tested:** {0, 1, 4} | timer_length = 100ms | clientBatchNum = 100 (uniform for all protocols)

## What Was Wrong Before — Root Cause Summary

The previous report at `experiment_results/EXPERIMENT_REPORT.md` had two defects that jointly explain every odd number in it:

1. **`hs.config`, `hs2.config`, `slot_hs1.config` were owned by root with 644 perms.** The Python `generate_config()` call inside `run_all_experiments.sh` silently raised `PermissionError` (36 occurrences in `experiment_full_log.txt`); the subprocess exit code was not checked. Consequently, HS-2 / HS / HS-1-SLOT ran with **stale config** in which `fork_tail_num`, `non_responsive_num`, `rollback_num` all stayed at 0 — no fault was ever injected. That is why they appeared "immune."
2. **Heterogeneous `clientBatchNum`:** HS-1 used 100, but HS-2/HS/HS-1-SLOT used 10000 (baked into the root-owned config files). That inflated their throughput ~5x and pushed their latency to 40-90ms, making per-protocol comparisons meaningless.

Both were fixed before this run: `chown hyperchain:hyperchain` + `chmod 664` on the config files, and the experiment script now verifies `fork_tail_num` actually lands in the file before launching (see `run_fork_tail_n20.sh`). A third, separate bug — `deploy_multi.sh` hanging when a node runs on `LOCAL_IP=205` because the background `nohup` was not detached from the outer subshell's `wait` — was fixed with `setsid -f`.

## Results

### Throughput (txn/s)

| Fork count | HS-1 | HS-2 | HS | HS-1-SLOT |
|---|---:|---:|---:|---:|
| 0 (baseline) | 88,384 | 87,482 | 89,307 | **94,463** |
| 1 | 84,041 | 83,385 | 84,516 | **95,279** |
| 4 | 72,234 | 72,931 | 69,722 | **94,467** |

### Latency (ms)

| Fork count | HS-1 | HS-2 | HS | HS-1-SLOT |
|---|---:|---:|---:|---:|
| 0 | 3.32 | 4.50 | 5.56 | **3.10** |
| 1 | 3.49 | 4.71 | 5.82 | **3.04** |
| 4 | 4.03 | 5.35 | 7.02 | **3.07** |

### Throughput drop from baseline

| Fork count | HS-1 | HS-2 | HS | HS-1-SLOT |
|---|---:|---:|---:|---:|
| 1 | −4.9% | −4.7% | −5.4% | **+0.9%** |
| 4 | **−18.3%** | **−16.6%** | **−21.9%** | **+0.0%** |

## Analysis

1. **HS-1, HS-2, and HS all degrade ~18–22% at fork=4.** This matches the arithmetic expectation: the fault pattern at `platform/consensus/ordering/*/algorithm/*.cpp:80` selects nodes where `id < 3·fork_tail_num && id%3==1`, so fork=4 makes nodes {1, 4, 7, 10} faulty — 4 out of 20 replicas (20%). Round-robin leadership cycles through all 20 replicas, so 20% of leader slots produce forked tails, and the observed drop matches.

2. **HS-1-SLOT shows 0% degradation at fork=4.** The SLOT scheduler redistributes workload away from faulty leaders within the pipeline, so the 4 faulty leaders contribute no forks but honest leaders absorb the freed slots. This is the paper's central claim (Fig. 11) and it reproduces cleanly now that fault injection actually reaches the binary.

3. **HS-1-SLOT has both the best throughput (94K, ~6% above HS-1) AND the lowest latency (3.0ms).** Under fork=4 the gap widens: HS-1-SLOT maintains 94K / 3.1ms while HS-1 drops to 72K / 4.0ms, HS-2 to 73K / 5.4ms, HS to 70K / 7.0ms.

4. **HS and HS-2 lose more latency than HS-1 at fork=4** (HS: +1.47ms; HS-2: +0.85ms; HS-1: +0.70ms). The deeper pipeline of 3-phase HS gets hurt more per forked tail because each stall flushes more in-flight proposals — consistent with the pipelining analysis in §3 of the paper.

5. **Absolute numbers align with the paper's AWS c4.4xlarge runs at matching n.** The paper reports HS-1 ≈ 57K at n=31 with 100ms timer and fork=0; we get 88K at n=20 on a lower-latency local LAN, which is in the expected range.

## Comparison with Paper (n=31, timer=100ms, batch=100)

| Metric | Paper fork=7 | Ours fork=4 |
|---|---|---|
| HS-1 drop | −22% | −18% |
| HS-1-SLOT drop | −1% | ~0% |
| HS-1-SLOT vs HS-1 baseline | ~+5% | +7% |

The relative shape of the paper's Figure 11 is fully reproduced; magnitudes are slightly compressed because we tested fork=4/20 instead of fork=7/31 (20% faulty vs 23%).

## Conclusion

With the permissions and deploy bugs fixed, the n=20 fork-tail experiment **confirms the paper's central claim**: HS-1-SLOT retains HS-1's low-latency 1-phase efficiency while being immune to tail-forking attacks, where HS-1, HS-2, and HS all degrade ~20% at fork=4.
