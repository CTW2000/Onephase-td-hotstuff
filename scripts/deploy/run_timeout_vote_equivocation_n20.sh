#!/bin/bash
# TD-Hotstuff TimeoutVote equivocation strong-fault experiment for n=20.
# TD_HS_TIMEOUT_VOTE_EQUIVOCATION_IDS is converted by deploy_multi.sh into
# replica-local TD_HS_TIMEOUT_VOTE_EQUIVOCATION=1 only.

set -e
DEPLOY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DEPLOY_DIR"

if [ -n "${TIMEOUT_VOTE_EQUIVOCATION_COUNTS_OVERRIDE:-}" ]; then
  COUNTS="$TIMEOUT_VOTE_EQUIVOCATION_COUNTS_OVERRIDE"
else
  COUNTS="0 1 3"
fi

env -u TD_HS_SILENT_LEADER_IDS -u TD_HS_UNFAIR_LEADER_IDS \
    -u TD_HS_DOUBLE_PROPOSAL_IDS -u TD_HS_DOUBLE_VOTE_IDS \
    -u TD_HS_INVALID_QC_IDS \
    -u TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS \
    -u TD_HS_TIMEOUT_VOTE_EQUIVOCATION_IDS \
    -u TD_HS_INVALID_TC_PROPOSAL_IDS \
    MODE_NAME="TimeoutVoteEquivocation" \
    RESULT_NAME="timeout_vote_equivocation" \
    ATTACK_IDS_ENV="TD_HS_TIMEOUT_VOTE_EQUIVOCATION_IDS" \
    DETECT_ENV="TD_HS_TIMEOUT_VOTE_EQUIVOCATION_DETECT_ENABLE" \
    COUNTS="$COUNTS" \
    ./run_strong_fault_v2_n20.sh
