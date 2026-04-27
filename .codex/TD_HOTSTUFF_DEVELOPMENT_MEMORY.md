# TD-HotStuff Development Memory

This note captures the working philosophy for future TD-HotStuff development.
Every new Codex session should read it after `AGENTS.md` and before protocol, experiment, or validation work.
It is guidance for implementation and review, not protocol code.

## Core Philosophy

- Do not write code only for one experiment, one attack, or one observed symptom. Every fix must be correct for the general protocol case.
- Preserve the HS-1-SLOT safety foundation: Prefix Speculation Rule, No-Gap Rule, SafeSlot, carry-block handling, and normal rollback behavior must not be weakened by weights, reputation, quarantine, or leader election.
- Strong protocol actions must be driven by deterministic or certified evidence. Local observation alone remains suspicion and must not directly change reputation, voting weight, quarantine state, or leader eligibility.
- Keep safety and accounting separate. Weighted QCs and HotStuff certificates decide consensus progress; receipts protect fairness/accounting and must not form QCs or lower quorum thresholds.
- Keep reputation and Byzantine penalty separate. Reputation reflects long-term certified behavior; direct Byzantine evidence updates penalty/weight effects.
- Never activate new weights before the EpochChangeBlock is committed. The epoch transition must be certified with the old epoch weight and quarantine context.
- Quarantine is a temporary view-level brake. It may reduce effective signer weight, but it must never reduce the epoch quorum threshold.
- Every signature, certificate, proposal, vote, receipt, and leader-selection decision must bind the full context: epoch, weight version, quarantine version, leader weight root, leader parameter version, randomness reference, view, slot, certificate type, and parent/carry context where relevant.
- All epoch recomputation must be deterministic across replicas. Use fixed-point integer arithmetic, canonical sorting/deduplication, deterministic rounding, and stable root construction.
- Treat small validator sets as first-class cases. Constants such as node/domain caps must be valid for `n=5`, `n=15`, `n=20`, and larger deployments, not only for the large-validator ideal case.
- Prefer common mechanisms over special-case patches. If an experiment fails, identify whether the failure is in consensus logic, evidence collection, result parsing, workload setup, or deployment before changing protocol behavior.
- Every protocol change should include a clear threat review: what attacker opportunity it closes, what new attacker opportunity it might open, and what evidence is required to trigger it.
- Experiments are validation evidence, not the design itself. Passing one attack scenario is not enough; the code must remain correct under normal, weighted, rollback, tail-forking, leader-slowness, network-delay, batching, scalability, and mixed-fault situations.

## Implementation Watchpoints

- `CapNormalize` must handle validator counts and domain layouts where configured caps are too small to allocate 100% total weight. The implementation needs a deterministic relaxation or validation rule, not an ad hoc case.
- Weighted QC verification must count unique signers by certified weight, not signature count, and must reject duplicated signers or mixed contexts.
- Evidence roots must be computed from the same committed/certified cutoff at all replicas; late evidence should be assigned to the next epoch by a deterministic rule.
- Leader election fallback must be deterministic and must handle `total_L = 0` without splitting replicas.
- Receipt storage and validation must be bounded and canonicalized so fairness evidence cannot become an unbounded DoS path.
- Fast quarantine can preserve safety while hurting liveness if too much weight is removed. Only deterministic severe evidence should remove voting weight quickly.
- Optional threshold encryption changes the fast path and should remain optional unless its extra decryption step is explicitly intended and measured.
