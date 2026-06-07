#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/crypto/signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"

namespace resdb {
namespace td_hotstuff {

struct ExperimentFaultConfig {
  bool silent_leader = false;
  bool unfair_leader = false;
  bool double_proposal = false;
  bool double_vote = false;
  bool invalid_qc = false;
  bool weight_update_vote_equivocation = false;
  bool timeout_vote_equivocation = false;
  bool invalid_tc_proposal = false;
  int unfair_leader_signer_group_size = 0;
  std::vector<int> peertrust_clique_reviewer_ids;
};

ExperimentFaultConfig ExperimentFaultConfigFromEnv(int total_replicas);

std::unique_ptr<Proposal> BuildConflictingProposalForExperiment(
    const Proposal& base_proposal, SignatureVerifier* verifier);

std::unique_ptr<Proposal> BuildInvalidQcProposalForExperiment(
    const Proposal& base_proposal, SignatureVerifier* verifier,
    int total_replicas);

std::unique_ptr<Proposal> BuildInvalidTcProposalForExperiment(
    const Proposal& base_proposal, SignatureVerifier* verifier);

std::unique_ptr<Certificate> BuildConflictingVoteForExperiment(
    const Proposal& proposal, int signer_id, SignatureVerifier* verifier);

std::unique_ptr<WeightUpdateVote> BuildWeightUpdateVoteForExperiment(
    int validator_id, std::string candidate_digest, std::string old_weight_root,
    uint64_t old_weight_version, int activation_view,
    SignatureVerifier* verifier);

std::unique_ptr<WeightUpdateVote> BuildConflictingWeightUpdateVoteForExperiment(
    const WeightUpdateVote& base_vote, std::string conflicting_candidate_digest,
    SignatureVerifier* verifier);

std::unique_ptr<TimeoutVote> BuildConflictingTimeoutVoteForExperiment(
    const TimeoutVote& base_vote, SignatureVerifier* verifier);

}  // namespace td_hotstuff
}  // namespace resdb
