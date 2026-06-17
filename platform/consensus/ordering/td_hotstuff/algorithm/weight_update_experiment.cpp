#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_experiment.h"

#include <string>

#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_controller.h"
#include "platform/consensus/reputation/reputation_utils.h"

namespace resdb {
namespace td_hotstuff {

std::unique_ptr<WeightUpdateVote>
MakeWeightUpdateVoteForCandidateDigestForExperiment(
    const CandidateWeightUpdate& candidate, const std::string& candidate_digest,
    int node_id, SignatureVerifier* verifier) {
  if (verifier == nullptr || node_id <= 0 || candidate_digest.empty() ||
      candidate.old_weight_root().empty() || candidate.activation_view() <= 0) {
    return nullptr;
  }
  std::unique_ptr<WeightUpdateVote> vote = std::make_unique<WeightUpdateVote>();
  vote->set_signer(node_id);
  vote->set_old_weight_root(candidate.old_weight_root());
  vote->set_old_weight_version(candidate.old_weight_version());
  vote->set_activation_view(candidate.activation_view());
  vote->set_candidate_digest(candidate_digest);
  auto signature_or = verifier->SignMessage(WeightUpdateVotePayload(*vote));
  if (!signature_or.ok()) {
    return nullptr;
  }
  *vote->mutable_signature() = *signature_or;
  return vote;
}

std::unique_ptr<WeightUpdateVote> MakeConflictingWeightUpdateVoteForExperiment(
    const WeightUpdateVote& vote, int node_id, SignatureVerifier* verifier) {
  if (verifier == nullptr || vote.candidate_digest().empty()) {
    return nullptr;
  }
  std::unique_ptr<WeightUpdateVote> conflicting =
      std::make_unique<WeightUpdateVote>(vote);
  const std::string material =
      vote.candidate_digest() + "|td_hotstuff_weight_update_vote_equivocation|" +
      std::to_string(node_id) + "|" + std::to_string(vote.old_weight_version()) +
      "|" + std::to_string(vote.activation_view());
  std::string digest = resdb::consensus::reputation::HashHex(material);
  if (digest == vote.candidate_digest()) {
    digest = resdb::consensus::reputation::HashHex(material + "|retry");
  }
  conflicting->set_candidate_digest(digest);
  conflicting->clear_signature();
  auto signature_or =
      verifier->SignMessage(WeightUpdateVotePayload(*conflicting));
  if (!signature_or.ok()) {
    return nullptr;
  }
  *conflicting->mutable_signature() = *signature_or;
  return conflicting;
}

}  // namespace td_hotstuff
}  // namespace resdb
