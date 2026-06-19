#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "common/crypto/signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/adapter/td_hotstuff_reputation_adapter.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_controller.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"
#include "platform/consensus/reputation/reputation_types.h"

namespace resdb {
namespace td_hotstuff {

class CertifiedWeightUpdatePipeline {
 public:
  struct Callbacks {
    std::function<int()> current_view;
    std::function<void(const CandidateWeightUpdate&)> broadcast_candidate;
    std::function<void(const WeightUpdateVote&)> broadcast_vote;
    std::function<void(const WeightUpdateCert&)> broadcast_cert;
  };

  CertifiedWeightUpdatePipeline(
      int node_id, int total_replicas,
      std::shared_ptr<WeightSchedule> weight_schedule,
      std::shared_ptr<LeaderSelectionSchedule> leader_schedule,
      SignatureVerifier* verifier,
      TdHotstuffReputationAdapter* reputation_adapter,
      Callbacks callbacks = {});

  CertifiedWeightUpdatePipeline(const CertifiedWeightUpdatePipeline&) = delete;
  CertifiedWeightUpdatePipeline& operator=(
      const CertifiedWeightUpdatePipeline&) = delete;

  void DrainCompletedCandidates();
  void DrainCompletedCandidatesForTesting(
      std::vector<resdb::consensus::reputation::ReputationCandidate>
          candidates);
  bool ReceiveCandidate(std::unique_ptr<CandidateWeightUpdate> candidate);
  bool ReceiveVote(std::unique_ptr<WeightUpdateVote> vote);
  bool ReceiveCert(std::unique_ptr<WeightUpdateCert> cert);
  std::unique_ptr<WeightUpdateCert> LatestCertForProposal(int view) const;
  void ProcessCertFromProposal(const WeightUpdateCert& cert, int view);
  void ActivateReady();
  void ActivateReady(int view);
  void MaybeActivateReadyAfterViewAdvance();

 private:
  int CurrentView() const;
  void BroadcastCandidate(const CandidateWeightUpdate& candidate) const;
  void BroadcastVote(const WeightUpdateVote& vote) const;
  void BroadcastCert(const WeightUpdateCert& cert) const;
  void DrainCompletedCandidates(
      std::vector<resdb::consensus::reputation::ReputationCandidate>
          candidates);
  void RefreshPendingActivationView();
  bool MaybeMakeSignedWeightUpdateVoteEvidenceSnapshot(
      const WeightUpdateVote& vote,
      TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot* snapshot) const;
  void MaybeBroadcastSyntheticWeightUpdateVoteEquivocationForExperiment();
  void BroadcastConflictingWeightUpdateVoteForExperiment(
      const WeightUpdateVote& vote);
  void MaybeBroadcastConflictingWeightUpdateVoteForExperiment(
      const WeightUpdateVote& vote);
  void MaybeBroadcastRemoteCandidateWeightUpdateVoteEquivocationForExperiment(
      const CandidateWeightUpdate& candidate);

  const int node_id_;
  const int total_replicas_;
  std::shared_ptr<WeightSchedule> weight_schedule_;
  std::shared_ptr<LeaderSelectionSchedule> leader_schedule_;
  SignatureVerifier* verifier_ = nullptr;
  TdHotstuffReputationAdapter* reputation_adapter_ = nullptr;
  Callbacks callbacks_;
  WeightUpdateController controller_;
  bool candidate_inflight_ = false;
  uint64_t candidate_inflight_version_ = 0;
  std::mutex experiment_vote_mutex_;
  std::set<std::string> experiment_vote_digests_;
  std::atomic<bool> synthetic_weight_update_vote_equivocation_done_{false};
  std::atomic<int> next_pending_activation_view_{0};
};

}  // namespace td_hotstuff
}  // namespace resdb
