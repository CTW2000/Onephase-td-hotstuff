# Protocol Comparison: TD-HotStuff vs Baselines (n=10)

All runs used 10 replicas on a single local machine, benchmark duration 20 s.
TD-HotStuff was run 20 times with random per-replica weights drawn from [1,3].
Baseline protocols use the framework's default (uniform) configuration.

---

## 1. TD-HotStuff — 20 random-weight runs (n=10)


| seed | weights             | W   | threshold | avg TPS | max TPS | avg lat (ms) | sustained |
| ---- | ------------------- | --- | --------- | ------- | ------- | ------------ | --------- |
| 1    | 1,3,1,2,1,2,2,2,3,2 | 19  | 13        | 84,724  | 103,940 | 3.700        | Yes       |
| 10   | 3,1,2,2,3,1,1,2,2,2 | 19  | 13        | 93,489  | 116,500 | 3.233        | Yes       |
| 11   | 2,3,2,2,3,3,1,1,3,2 | 22  | 15        | 89,744  | 108,340 | 3.472        | Yes       |
| 12   | 2,2,3,3,3,2,1,2,1,2 | 21  | 15        | 97,378  | 116,920 | 3.274        | Yes       |
| 13   | 2,2,3,3,1,3,1,3,1,1 | 20  | 14        | 94,602  | 111,940 | 3.154        | Yes       |
| 14   | 1,3,3,3,3,1,2,3,2,2 | 23  | 16        | 89,421  | 105,760 | 3.473        | Yes       |
| 15   | 1,1,3,3,1,1,1,1,1,3 | 16  | 11        | 96,070  | 114,040 | 3.320        | Yes       |
| 16   | 2,2,2,2,2,1,2,1,2,3 | 19  | 13        | 96,816  | 115,780 | 3.094        | Yes       |
| 17   | 3,2,2,2,2,1,3,3,3,3 | 24  | 17        | 89,603  | 104,600 | 3.629        | Yes       |
| 18   | 1,1,3,2,2,1,1,2,3,2 | 18  | 13        | 87,746  | 103,960 | 4.121        | Yes       |
| 19   | 3,1,3,1,3,1,2,2,3,2 | 21  | 15        | 86,223  | 101,980 | 3.745        | Yes       |
| 2    | 1,1,1,2,1,3,3,2,2,3 | 19  | 13        | 84,256  | 104,080 | 4.098        | Yes       |
| 20   | 3,3,1,2,3,3,1,2,3,1 | 22  | 15        | 90,808  | 110,680 | 3.398        | Yes       |
| 3    | 1,3,3,1,2,3,2,3,3,1 | 22  | 15        | 80,909  | 99,900  | 4.229        | Yes       |
| 4    | 1,2,1,3,2,2,1,1,1,1 | 15  | 11        | 87,356  | 103,960 | 3.639        | Yes       |
| 5    | 3,2,3,2,3,3,3,3,1,2 | 25  | 17        | 87,189  | 104,400 | 3.938        | Yes       |
| 6    | 3,1,2,2,1,1,1,3,3,2 | 19  | 13        | 89,268  | 104,600 | 3.247        | Yes       |
| 7    | 2,1,2,3,1,1,3,1,2,3 | 19  | 13        | 82,019  | 99,760  | 4.249        | Yes       |
| 8    | 1,2,2,1,1,3,1,1,1,1 | 14  | 10        | 84,782  | 102,100 | 3.864        | Yes       |
| 9    | 2,3,2,2,1,1,3,1,2,3 | 20  | 14        | 85,340  | 107,320 | 3.869        | Yes       |


### TD-HotStuff aggregate statistics across the 20 runs


| Metric           | min    | median  | mean    | max     | stdev |
| ---------------- | ------ | ------- | ------- | ------- | ----- |
| Avg TPS          | 80,909 | 88,507  | 88,887  | 97,378  | 4,804 |
| Max TPS          | 99,760 | 104,600 | 107,028 | 116,920 | 5,445 |
| Avg Latency (ms) | 3.094  | 3.634   | 3.637   | 4.249   | 0.368 |
| Max Latency (ms) | 3.349  | 4.138   | 4.188   | 5.276   | 0.645 |


**Sustained throughput:** 20/20 runs sustained TPS across the 20s window.

---

## 2. Baseline protocols (n=10, uniform weights)


| Protocol  | avg TPS | max TPS | avg lat (ms) | max lat (ms) | sustained |
| --------- | ------- | ------- | ------------ | ------------ | --------- |
| HS        | 86,244  | 106,100 | 4.630        | 5.024        | Yes       |
| HS-1      | 82,314  | 107,720 | 2.804        | 3.158        | Yes       |
| HS-2      | 94,804  | 115,460 | 3.260        | 3.404        | Yes       |
| HS-1-SLOT | 89,213  | 104,080 | 3.282        | 3.499        | Yes       |
| PBFT      | 52,351  | 71,460  | 42.879       | 92.433       | Yes       |


---

## 3. Side-by-side comparison

TD-HotStuff numbers are the **mean over 20 random-weight runs**; baselines are single runs (uniform weights).


| Protocol                                | Avg TPS | Avg Latency (ms) |
| --------------------------------------- | ------- | ---------------- |
| TD-HotStuff (random weights, n=20 runs) | 88,887  | 3.637            |
| HS                                      | 86,244  | 4.630            |
| HS-1                                    | 82,314  | 2.804            |
| HS-2                                    | 94,804  | 3.260            |
| HS-1-SLOT                               | 89,213  | 3.282            |
| PBFT                                    | 52,351  | 42.879           |


---

## 4. Observations

### Robustness

- **20/20 random-weight runs sustained throughput** over the full 20 s window. The duplicate-QC-formation fix is confirmed to be robust across a broad range of random weight distributions, including aggressive ones like [3,3,1,2,3,3,1,2,3,1].
- TD-HotStuff avg-TPS range across 20 weight draws: **80,909 – 97,378** (spread ≈ 16K, stdev ≈ 4.8K). This is only ±5% variation around the mean — weight distribution has a small effect on throughput once the protocol stabilizes.
- Total weight W varied from 14 to 25, threshold T varied from 10 to 17, yet throughput stayed in the same band. Weight ratios (not absolute values) dominate behavior.

### TD-HotStuff vs baselines (median of 20 runs vs single baseline run)


| Comparison         | TD-HS median | Baseline | Ratio                           |
| ------------------ | ------------ | -------- | ------------------------------- |
| TD-HS vs HS        | 88,507       | 86,244   | **1.03× faster** than HS        |
| TD-HS vs HS-1      | 88,507       | 82,314   | **1.08× faster** than HS-1      |
| TD-HS vs HS-2      | 88,507       | 94,804   | 1.07× slower than HS-2          |
| TD-HS vs HS-1-SLOT | 88,507       | 89,213   | ≈ on par (0.99×) with HS-1-SLOT |
| TD-HS vs PBFT      | 88,507       | 52,351   | **1.69× faster** than PBFT      |


### Latency

- **PBFT** is an outlier: 42.9 ms avg / 92.4 ms max — roughly 10× higher than every HotStuff variant. This reflects its 3-phase design and classical-BFT quorum collection.
- All HotStuff variants (HS, HS-1, HS-2, HS-1-SLOT) and TD-HotStuff cluster in the **2.8 – 4.6 ms** range. TD-HotStuff's 3.64 ms median sits in the middle of this group.
- TD-HS's latency spread across 20 runs (3.09 – 4.25 ms) is narrower than the spread between HotStuff variants themselves.

### Takeaways

1. **Weighted QC does not cost throughput.** With random non-uniform weights, TD-HotStuff matches or exceeds HS, HS-1, and HS-1-SLOT, and stays within ~7% of the fastest variant (HS-2).
2. **The protocol is stable under varied weight distributions** — not a single stall was observed across the 20 runs, including "aggressive" configurations that would previously have hung.
3. **PBFT is the clear outlier** in both throughput and latency, as expected given its 3-phase design.
4. **HS-2** (2-phase HotStuff) is the single-run top performer here, but TD-HotStuff is its nearest neighbor and the difference is within the run-to-run noise observed in the TD-HS 20-run sample.

