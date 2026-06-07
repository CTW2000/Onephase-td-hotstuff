#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/crypto/signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/vote_score_reputation_plugin.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"

namespace resdb {
namespace td_hotstuff {

struct WeightUpdateConfig {
  bool enabled = false;
  int epoch_views = 4096;
  int activation_epoch_delay = 2;
};

struct WeightSnapshot {
  std::vector<int64_t> weights;
  std::string weight_root;
  uint64_t weight_version = 0;
  int64_t quorum_weight = 0;
  int current_view = 0;
  std::vector<int64_t> leader_weights;
  std::string leader_weight_root;
  uint64_t leader_weight_version = 0;
};

struct WeightPluginOutboundMessages {
  std::vector<CandidateWeightUpdate> candidates;
  std::vector<WeightUpdateVote> votes;
  std::vector<WeightUpdateCert> certs;
};

struct InstallableWeightUpdate {
  int activation_view = 0;
  std::string old_weight_root;
  uint64_t old_weight_version = 0;
  std::vector<int64_t> next_weights;
  std::vector<int64_t> leader_weights;
  std::string leader_weight_root;
  uint64_t leader_params_version = 1;
  std::string leader_randomness_ref;
  WeightUpdateCert cert;
};

WeightUpdateConfig WeightUpdateConfigFromEnv();
int ComputeWeightUpdateActivationView(int end_qc_view, int epoch_views,
                                      int activation_epoch_delay);
WeightSnapshot MakeWeightSnapshot(const WeightSchedule& schedule,
                                  int current_view);

std::vector<int64_t> CandidateNextWeights(
    const CandidateWeightUpdate& update);
std::vector<int64_t> CandidateLeaderWeights(
    const CandidateWeightUpdate& update);
CandidateWeightUpdate BuildCandidateWeightUpdate(
    const VoteScoreCandidate& candidate);
std::string WeightUpdateVotePayload(const CandidateWeightUpdate& update,
                                    int validator_id);
std::string WeightUpdateVotePayload(const WeightUpdateVote& vote);

class WeightUpdateManager {
 public:
  WeightUpdateManager(int node_id, int total_replicas,
                      SignatureVerifier* verifier,
                      WeightUpdateConfig config);

  bool enabled() const { return config_.enabled; }
  const WeightUpdateConfig& config() const { return config_; }

  void AddLocalCandidates(std::vector<VoteScoreCandidate> candidates,
                          const WeightSnapshot& snapshot);
  void HandleCandidate(const CandidateWeightUpdate& update,
                       const WeightSnapshot& snapshot);
  void HandleVote(const WeightUpdateVote& vote,
                  const WeightSnapshot& snapshot);
  void HandleCert(const WeightUpdateCert& cert,
                  const WeightSnapshot& snapshot);
  WeightPluginOutboundMessages DrainOutboundMessages(
      int current_view, const WeightSnapshot& snapshot);
  std::vector<InstallableWeightUpdate> TakeInstallableUpdates(
      int current_view, const WeightSnapshot& snapshot);
  void OnWeightsActivated(const WeightSnapshot& snapshot);

  bool VerifyCandidate(const CandidateWeightUpdate& update,
                       const VoteScoreCandidate& local_candidate,
                       const WeightSchedule& schedule,
                       std::string* error) const;
  std::unique_ptr<WeightUpdateVote> CreateVote(
      const CandidateWeightUpdate& update,
      const VoteScoreCandidate& local_candidate,
      const WeightSchedule& schedule) const;
  bool VerifyVote(const WeightUpdateVote& vote,
                  const CandidateWeightUpdate& update,
                  std::string* error) const;
  bool VerifyVoteEvidence(const WeightUpdateVote& vote,
                          std::string* error) const;
  bool VerifyCert(const WeightUpdateCert& cert,
                  const WeightSchedule& schedule,
                  std::string* error) const;

 private:
  bool ValidateCandidateFields(const CandidateWeightUpdate& update,
                               const WeightSnapshot& snapshot,
                               std::string* error) const;
  bool VerifyCandidateWithSnapshot(const CandidateWeightUpdate& update,
                                   const VoteScoreCandidate& local_candidate,
                                   const WeightSnapshot& snapshot,
                                   std::string* error) const;
  std::unique_ptr<WeightUpdateVote> CreateVoteWithSnapshot(
      const CandidateWeightUpdate& update,
      const VoteScoreCandidate& local_candidate,
      const WeightSnapshot& snapshot) const;
  bool VerifyCertWithSnapshot(const WeightUpdateCert& cert,
                              const WeightSnapshot& snapshot,
                              std::string* error) const;
  void TryVoteForCandidate(const CandidateWeightUpdate& candidate,
                           const WeightSnapshot& snapshot);
  void AddWeightUpdateVote(const WeightUpdateVote& vote,
                           const WeightSnapshot& snapshot);
  void TryFormWeightUpdateCert(const std::string& candidate_digest,
                               const WeightSnapshot& snapshot);
  void ClearPluginState();

  int node_id_;
  int total_replicas_;
  SignatureVerifier* verifier_;
  WeightUpdateConfig config_;

  std::map<std::string, VoteScoreCandidate> local_weight_candidates_;
  std::map<std::string, CandidateWeightUpdate> weight_update_candidates_;
  std::map<std::string, std::map<int, WeightUpdateVote>> weight_update_votes_;
  std::map<int, WeightUpdateCert> pending_weight_update_certs_;
  std::set<std::string> voted_weight_candidate_digests_;
  std::set<std::string> voted_weight_candidate_schedule_keys_;
  std::set<std::string> broadcast_candidate_digests_;
  std::set<std::string> broadcast_weight_cert_digests_;
  WeightPluginOutboundMessages outbound_messages_;
};

}  // namespace td_hotstuff
}  // namespace resdb
