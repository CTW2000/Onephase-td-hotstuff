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

- Voter behavior affects voting weight.
- Leader behavior affects leader weight.
- Do not let slow-voter decay automatically remove leader eligibility when leader behavior is healthy.

## Performance Rule

- Keep the consensus hot path thin.
- Prefer adapter/plugin/runtime changes before consensus changes.
- After any pipeline change, verify the no-Byzantine baseline before trusting attack results.
