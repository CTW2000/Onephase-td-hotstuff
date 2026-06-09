#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/crypto/signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"
#include "platform/consensus/reputation/reputation_types.h"

namespace resdb {
namespace td_hotstuff {

CandidateWeightUpdate ToCandidateWeightUpdate(
    const resdb::consensus::reputation::ReputationCandidate& candidate);
std::string WeightUpdateVotePayload(const WeightUpdateVote& vote);

class WeightUpdateController {
 public:
  WeightUpdateController(
      int node_id, int total_replicas,
      std::shared_ptr<WeightSchedule> weight_schedule,
      SignatureVerifier* verifier,
      std::shared_ptr<LeaderSelectionSchedule> leader_schedule = nullptr);

  bool AddLocalCandidate(
      const resdb::consensus::reputation::ReputationCandidate& candidate);
  std::unique_ptr<WeightUpdateVote> HandleCandidate(
      const CandidateWeightUpdate& candidate);
  std::unique_ptr<WeightUpdateCert> HandleVote(const WeightUpdateVote& vote);
  bool HandleCert(const WeightUpdateCert& cert);
  bool ActivateReady(int current_view);

 private:
  struct CandidateKey {
    std::string old_weight_root;
    uint64_t old_weight_version = 0;
    int window_start = 0;
    int window_end = 0;
    int activation_view = 0;
    std::string candidate_digest;

    bool operator<(const CandidateKey& other) const {
      return std::tie(old_weight_root, old_weight_version, window_start,
                      window_end, activation_view, candidate_digest) <
             std::tie(other.old_weight_root, other.old_weight_version,
                      other.window_start, other.window_end,
                      other.activation_view, other.candidate_digest);
    }
  };

  struct VoteBucket {
    CandidateWeightUpdate candidate;
    std::map<int, WeightUpdateVote> votes;
  };

  CandidateKey KeyForCandidate(const CandidateWeightUpdate& candidate) const;
  bool ValidateCandidateStructure(const CandidateWeightUpdate& candidate) const;
  bool VerifyVoteForCandidate(const WeightUpdateVote& vote,
                              const CandidateWeightUpdate& candidate) const;
  std::vector<int64_t> CandidateWeights(
      const CandidateWeightUpdate& candidate) const;
  std::vector<int64_t> CandidateLeaderWeights(
      const CandidateWeightUpdate& candidate) const;
  int64_t VoteWeight(const std::map<int, WeightUpdateVote>& votes) const;
  std::unique_ptr<WeightUpdateCert> MaybeFormCert(VoteBucket* bucket) const;

  const int node_id_;
  const int total_replicas_;
  std::shared_ptr<WeightSchedule> weight_schedule_;
  std::shared_ptr<LeaderSelectionSchedule> leader_schedule_;
  SignatureVerifier* verifier_ = nullptr;
  mutable std::mutex mutex_;

  std::map<CandidateKey, CandidateWeightUpdate> local_candidates_;
  std::map<std::string, CandidateWeightUpdate> candidates_by_digest_;
  std::map<std::string, VoteBucket> vote_buckets_;
  std::map<std::string, WeightUpdateCert> pending_certs_;
  std::set<std::string> voted_digests_;
};

}  // namespace td_hotstuff
}  // namespace resdb
