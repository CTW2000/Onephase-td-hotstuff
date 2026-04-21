# Slow-Vote Experiment — n=20

**Date:** 2026-04-18
**Setup:** 6 machines (5 server hosts × 4 nodes; client on 10.10.131.205)
**Protocols:** HS-1, HS-2, HS, HS-1-SLOT
**Parameters:** n=20, clientBatchNum=100, mean_network_delay=10ms, slow-voter counts ∈ {0, 1, 4, 6}

## Interpretation

The codebase has no flag literally named "slow_vote." The closest mechanism is `network_delay_num` in `platform/consensus/ordering/common/algorithm/protocol_base.cpp:40-61`:

```cpp
bool IsSlowReplica(int node_id) { return node_id <= network_delay_num_; }

int SendMessage(...) {
  if (IsSlowReplica(id_))        usleep(GetRandomDelay());  // sender slow
  if (IsSlowReplica(node_id))    usleep(GetRandomDelay());  // receiver slow
  return single_call_(...);
}
```

Replicas with `id ∈ {1..network_delay_num}` add a ~`mean_network_delay` ms delay to every inbound and outbound message. Since votes are the dominant inter-replica message (every replica sends a vote for every proposal, whereas only the leader proposes), this is effectively a slow-voter fault. Note: it ALSO slows any proposals those replicas issue when they rotate into leadership, but the dominant effect is vote-path slowdown.

## Results

### Throughput (txn/s)

| Slow voters | HS-1 | HS-2 | HS | HS-1-SLOT |
|---:|---:|---:|---:|---:|
| 0 (baseline) | 88,345 | 88,977 | 89,373 | 87,227 |
| 1 | 38,560 | 38,584 | 38,544 | **39,497** |
| 4 | 26,205 | 26,400 | 26,394 | **35,085** |
| 6 | 20,784 | 20,719 | 20,788 | **31,961** |

### Latency (ms)

| Slow voters | HS-1 | HS-2 | HS | HS-1-SLOT |
|---:|---:|---:|---:|---:|
| 0 | 3.32 | 4.44 | 5.50 | **3.27** |
| 1 | 8.14 | 10.53 | 13.15 | **6.36** |
| 4 | 11.84 | 15.38 | 19.23 | **6.90** |
| 6 | 14.91 | 19.62 | 24.36 | **7.34** |

### Throughput drop from baseline

| Slow voters | HS-1 | HS-2 | HS | HS-1-SLOT |
|---:|---:|---:|---:|---:|
| 1 | −56.3% | −56.6% | −56.9% | **−54.7%** |
| 4 | −70.3% | −70.3% | −70.5% | **−59.8%** |
| 6 | −76.5% | −76.7% | −76.7% | **−63.4%** |

## Analysis

### 1. All four protocols suffer at slow-vote=1 (5% slow voters)

With one slow voter (node 1) out of 20, throughput for every protocol drops ~55–57%, from ~88K to ~38K. This is fundamentally different from the slow-leader picture, where only the chain-based protocols collapsed (−82%) and HS-1-SLOT was nearly immune (−7%). **A slow voter hits every protocol because every protocol needs votes to form a QC.** A `⌈2n/3⌉+1 = 14`-vote quorum at n=20 means 6 votes can be ignored, but when the slow voter's vote happens to be on the critical path — and it often is, since the protocol broadcasts to everyone — the leader waits.

### 2. HS-2 and HS do NOT suffer more than HS-1

In slow-leader, HS and HS-2's extra phases amplified latency (HS latency was ~50% higher than HS-1 at slow=6). Here, the percentage drops for HS-1, HS-2, and HS are within 0.5 pp of each other. The bottleneck is pure vote-collection time, not phase count. Raw latency still rises with phase count (HS-1 14.9ms vs HS 24.4ms at slow=6) because each extra phase adds one more QC round-trip, but throughput is capped by the same vote-collection delay for all three.

### 3. HS-1-SLOT is the only protocol that degrades sub-linearly — the advantage grows with fault fraction

| Slow | Chain protocols drop | HS-1-SLOT drop | Gap |
|---:|---:|---:|---:|
| 1 (5%) | ~−56% | −55% | 1 pp |
| 4 (20%) | ~−70% | −60% | 10 pp |
| 6 (30%) | ~−77% | −63% | 14 pp |

The gap grows because the SLOT scheduler can issue multiple proposals per view and route them to different quorum subsets. With more slow voters, the scheduler has more flexibility to pick quorums that exclude slow voters — but because it still needs `⌈2n/3⌉+1` votes, it cannot exclude all of them when slow voters approach 1/3 of replicas. So HS-1-SLOT is **partially resilient**, not immune, to slow voters.

### 4. HS-1-SLOT's latency advantage is even larger than its throughput advantage

At slow=6: HS-1-SLOT latency is 7.3ms, while HS-1 is 14.9ms (2.0×), HS-2 is 19.6ms (2.7×), HS is 24.4ms (3.3×). The multi-slot pipeline hides vote latency by running several slots concurrently; any slow votes land on at most one slot instead of stalling the whole pipeline. This is a direct consequence of the slot-parallel design.

### 5. Comparison with slow_leader at n=20

| Fault type | HS-1 @ 6 faulty | HS-1-SLOT @ 6 faulty | Ratio |
|---|---:|---:|---:|
| Slow leader | 3,200 txn/s (−96%) | 66,341 txn/s (−30%) | 21× |
| Slow voter (this) | 20,784 txn/s (−77%) | 31,961 txn/s (−63%) | 1.5× |

Slow leaders are catastrophic for chain protocols and HS-1-SLOT absorbs them almost completely. Slow voters hurt everyone roughly equally, and HS-1-SLOT can only absorb the marginal benefit from quorum flexibility. **The SLOT design defends against the proposer-side fault model the paper targets, not against voter-side faults.**

## Conclusion

1. **Slow voters hurt all protocols substantially** — −55% at just 5% slow voters (single faulty voter out of 20). This is the dominant network-delay effect.
2. **Chain-based HS-1, HS-2, HS degrade nearly identically in throughput** (percentages within 0.5 pp), but HS's 3-phase path pays ~60% more latency than HS-1's 1-phase path.
3. **HS-1-SLOT's advantage is meaningful but modest**: 14 pp better at slow=6 (31K vs 21K) and 2–3× lower latency than HS/HS-2. It degrades gracefully because the slot scheduler can avoid slow voters when quorums allow, but it cannot eliminate the need for them entirely.
4. **Slow leader is the catastrophic failure mode that HS-1-SLOT solves; slow voter is the gradual failure mode where HS-1-SLOT merely softens the blow.**
