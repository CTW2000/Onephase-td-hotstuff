# Develop Philosophy

These rules are part of the framework design. Read them before changing code.

## Core Boundary

- Consensus verifies protocol artifacts and advances the protocol.
- The adapter extracts compact, authenticated evidence from consensus artifacts.
- The plugin computes reputation, Byzantine classifications, and weight results.
- Consensus only certifies and activates the plugin result; it must not become the reputation engine.

## Timeout Rule

- Timeout certificates and view changes are normal consensus-path artifacts.
- Do not turn timeout evidence into Byzantine punishment or leader-outcome penalty.
- Silent leaders lose recovery only through leader score, based on successful proposals.

## Evidence Rule

- Strong-fault penalties require authenticated protocol evidence.
- Consensus may reject invalid artifacts, but the plugin classifies Byzantine behavior.
- Bad-node IDs are experiment-launch controls only; they must not leak into normal replicas, clients, consensus judgment, or plugin computation.

## Role Separation

- Vote score and leader score are computed independently from their own evidence.
- The plugin combines those independent scores into the node's final reputation/weight through the normal certified pipeline.
- When a role-specific score changes, preserve the evidence attribution in audit output so experiments can explain whether the loss came from voter behavior, leader behavior, PeerTrust/Sybil context, or strong-fault evidence.

## Performance Rule

- Keep the consensus hot path thin.
- Prefer adapter/plugin/runtime changes before consensus changes.
- After any pipeline change, verify the no-Byzantine baseline before trusting attack results.

## Full-Pipeline Experiment Rule

- Main experiments must enable the full implemented reputation pipeline.
- Do not disable bonus, recovery, PeerTrust, SybilGraph, strong-fault detectors, or certified weight updates in a main experiment to make a result easier to interpret.
- If a run disables a mechanism, call it an ablation or diagnostic control and keep it separate from paper conclusions about the full design.
- Validators should start from low certified reputation/weight and earn influence slowly through long correct behavior.
- Reaching maximum weight should be difficult; conservative growth and diminishing bonus are part of the design, not a test artifact.
