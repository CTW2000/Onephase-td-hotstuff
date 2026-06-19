#pragma once

#include <vector>

#include "platform/consensus/reputation/reputation_types.h"

namespace resdb {
namespace consensus {
namespace reputation {

std::vector<StrongFaultRecord> DetectDoubleProposalFaults(
    const std::vector<SignedProposalEvidence>& signed_proposal_evidence);

std::vector<StrongFaultRecord> DetectDoubleVoteFaults(
    const std::vector<SignedVoteEvidence>& signed_vote_evidence);

std::vector<StrongFaultRecord> DetectInvalidQcProposalFaults(
    const std::vector<InvalidQcProposalEvidence>& invalid_qc_proposal_evidence);

std::vector<StrongFaultRecord> DetectWeightUpdateVoteEquivocationFaults(
    const std::vector<SignedWeightUpdateVoteEvidence>&
        signed_weight_update_vote_evidence);

std::vector<StrongFaultRecord> DetectConflictingQcFaults(
    const std::vector<VerifiedQcArtifactEvidence>& verified_qc_artifact_evidence,
    int total_replicas);

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
