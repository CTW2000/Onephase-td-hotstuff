#pragma once

#include <memory>
#include <string>

#include "common/crypto/signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"

namespace resdb {
namespace td_hotstuff {

std::unique_ptr<WeightUpdateVote>
MakeWeightUpdateVoteForCandidateDigestForExperiment(
    const CandidateWeightUpdate& candidate, const std::string& candidate_digest,
    int node_id, SignatureVerifier* verifier);

std::unique_ptr<WeightUpdateVote> MakeConflictingWeightUpdateVoteForExperiment(
    const WeightUpdateVote& vote, int node_id, SignatureVerifier* verifier);

}  // namespace td_hotstuff
}  // namespace resdb
