#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <memory>
#include <set>
#include <string>
#include <tuple>
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
  bool AddLocalCandidateAndMaybeCert(
      const resdb::consensus::reputation::ReputationCandidate& candidate,
      std::unique_ptr<WeightUpdateCert>* cert);
  std::unique_ptr<WeightUpdateCert> AddLocalCandidateAndMaybeCert(
      const resdb::consensus::reputation::ReputationCandidate& candidate);
  bool ObserveCandidate(const CandidateWeightUpdate& candidate,
                        std::unique_ptr<WeightUpdateCert>* cert = nullptr);
  std::unique_ptr<WeightUpdateVote> HandleCandidate(
      const CandidateWeightUpdate& candidate);
  std::unique_ptr<WeightUpdateCert> HandleVote(const WeightUpdateVote& vote);
  bool HandleCert(const WeightUpdateCert& cert);
  std::unique_ptr<WeightUpdateCert> LatestCertForProposal(int view) const;
  bool ActivateReady(int current_view);
  int EarliestPendingActivationView() const;

 private:
  struct CandidateKey {
    std::string old_weight_root;
    uint64_t old_weight_version = 0;
    int activation_view = 0;
    std::string candidate_digest;

    bool operator<(const CandidateKey& other) const {
      return std::tie(old_weight_root, old_weight_version, activation_view,
                      candidate_digest) <
             std::tie(other.old_weight_root, other.old_weight_version,
                      other.activation_view, other.candidate_digest);
    }
  };

  struct VoteBucket {
    CandidateWeightUpdate candidate;
    std::map<int, WeightUpdateVote> votes;
  };

  struct VoteScopeKey {
    std::string old_weight_root;
    uint64_t old_weight_version = 0;

    bool operator<(const VoteScopeKey& other) const {
      return std::tie(old_weight_root, old_weight_version) <
             std::tie(other.old_weight_root, other.old_weight_version);
    }
  };

  CandidateKey KeyForCandidate(const CandidateWeightUpdate& candidate) const;
  bool ValidateCandidateStructure(const CandidateWeightUpdate& candidate) const;
  bool VerifyVoteForCandidate(const WeightUpdateVote& vote,
                              const CandidateWeightUpdate& candidate) const;
  std::vector<int64_t> CandidateWeights(
      const CandidateWeightUpdate& candidate) const;
  std::vector<int64_t> CandidateLeaderWeights(
      const CandidateWeightUpdate& candidate) const;
  int64_t VoteWeight(const std::map<int, WeightUpdateVote>& votes,
                     uint64_t old_weight_version) const;
  void AbsorbPendingVotesLocked(const std::string& digest,
                                const CandidateWeightUpdate& candidate);
  std::unique_ptr<WeightUpdateCert> MaybeFormCert(VoteBucket* bucket);
  bool StageCandidateSchedule(const CandidateWeightUpdate& candidate) const;
  void PruneStaleStateLocked();

  const int node_id_;
  const int total_replicas_;
  std::shared_ptr<WeightSchedule> weight_schedule_;
  std::shared_ptr<LeaderSelectionSchedule> leader_schedule_;
  SignatureVerifier* verifier_ = nullptr;
  mutable std::mutex mutex_;

  std::map<CandidateKey, CandidateWeightUpdate> local_candidates_;
  std::map<std::string, CandidateWeightUpdate> candidates_by_digest_;
  std::map<std::string, VoteBucket> vote_buckets_;
  std::map<std::string, std::map<int, WeightUpdateVote>> pending_votes_by_digest_;
  std::map<std::string, WeightUpdateCert> pending_certs_;
  std::map<std::string, WeightUpdateCert> recent_certs_;
  std::map<VoteScopeKey, std::string> accepted_cert_by_scope_;
  std::map<VoteScopeKey, std::string> voted_candidate_by_scope_;
  std::map<std::string, uint64_t> emitted_cert_version_by_digest_;
};

}  // namespace td_hotstuff
}  // namespace resdb
