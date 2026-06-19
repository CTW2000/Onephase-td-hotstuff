#include "platform/consensus/ordering/td_hotstuff/algorithm/certified_weight_update_pipeline.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <tuple>
#include <utility>

#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_experiment.h"
#include "platform/consensus/reputation/reputation_utils.h"

namespace resdb {
namespace td_hotstuff {
namespace {

bool EnvFlagEnabled(const char* name) {
  const char* raw = std::getenv(name);
  if (raw == nullptr) {
    return false;
  }
  const std::string value(raw);
  return value == "1" || value == "true" || value == "TRUE" ||
         value == "yes" || value == "YES" || value == "on" ||
         value == "ON";
}

bool WeightUpdateTraceEnabled() {
  static const bool enabled = EnvFlagEnabled("TD_HS_WEIGHT_UPDATE_TRACE");
  return enabled;
}

bool WeightUpdateVoteEquivocationForExperimentEnabled() {
  if (!EnvFlagEnabled("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION")) {
    return false;
  }
  const char* trigger_file =
      std::getenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE");
  if (trigger_file == nullptr || trigger_file[0] == '\0') {
    return true;
  }
  return std::ifstream(trigger_file).good();
}

bool WeightUpdateVoteEquivocationOnCandidateForExperimentEnabled() {
  return EnvFlagEnabled("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_ON_CANDIDATE");
}

int WeightUpdateVoteEquivocationStartViewForExperiment() {
  const char* raw =
      std::getenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW");
  if (raw == nullptr) {
    return 4096;
  }
  const int value = std::atoi(raw);
  return value < 0 ? 0 : value;
}

std::string JoinInt64Vector(const std::vector<int64_t>& values) {
  std::ostringstream oss;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      oss << ',';
    }
    oss << values[i];
  }
  return oss.str();
}

}  // namespace

CertifiedWeightUpdatePipeline::CertifiedWeightUpdatePipeline(
    int node_id, int total_replicas,
    std::shared_ptr<WeightSchedule> weight_schedule,
    std::shared_ptr<LeaderSelectionSchedule> leader_schedule,
    SignatureVerifier* verifier,
    TdHotstuffReputationAdapter* reputation_adapter,
    Callbacks callbacks)
    : node_id_(node_id),
      total_replicas_(total_replicas),
      weight_schedule_(std::move(weight_schedule)),
      leader_schedule_(std::move(leader_schedule)),
      verifier_(verifier),
      reputation_adapter_(reputation_adapter),
      callbacks_(std::move(callbacks)),
      controller_(node_id_, total_replicas_, weight_schedule_, verifier_,
                  leader_schedule_) {}

int CertifiedWeightUpdatePipeline::CurrentView() const {
  return callbacks_.current_view == nullptr ? 0 : callbacks_.current_view();
}

void CertifiedWeightUpdatePipeline::BroadcastCandidate(
    const CandidateWeightUpdate& candidate) const {
  if (callbacks_.broadcast_candidate != nullptr) {
    callbacks_.broadcast_candidate(candidate);
  }
}

void CertifiedWeightUpdatePipeline::BroadcastVote(
    const WeightUpdateVote& vote) const {
  if (callbacks_.broadcast_vote != nullptr) {
    callbacks_.broadcast_vote(vote);
  }
}

void CertifiedWeightUpdatePipeline::BroadcastCert(
    const WeightUpdateCert& cert) const {
  if (callbacks_.broadcast_cert != nullptr) {
    callbacks_.broadcast_cert(cert);
  }
}

void CertifiedWeightUpdatePipeline::DrainCompletedCandidates() {
  if (reputation_adapter_ == nullptr) {
    return;
  }
  DrainCompletedCandidates(reputation_adapter_->TakeCompletedCandidates());
}

void CertifiedWeightUpdatePipeline::DrainCompletedCandidatesForTesting(
    std::vector<resdb::consensus::reputation::ReputationCandidate>
        candidates) {
  DrainCompletedCandidates(std::move(candidates));
}

void CertifiedWeightUpdatePipeline::DrainCompletedCandidates(
    std::vector<resdb::consensus::reputation::ReputationCandidate>
        candidates) {
  if (weight_schedule_ == nullptr) {
    return;
  }
  const uint64_t active_version = weight_schedule_->ActiveWeightVersion();
  if (candidate_inflight_ && candidate_inflight_version_ == active_version) {
    return;
  }
  if (candidate_inflight_ && candidate_inflight_version_ != active_version) {
    candidate_inflight_ = false;
  }
  std::sort(candidates.begin(), candidates.end(),
            [](const auto& lhs, const auto& rhs) {
              return std::tie(lhs.old_weight_version, lhs.activation_view,
                              lhs.start_view, lhs.end_view,
                              lhs.candidate_digest_hex) <
                     std::tie(rhs.old_weight_version, rhs.activation_view,
                              rhs.start_view, rhs.end_view,
                              rhs.candidate_digest_hex);
            });
  for (const auto& candidate : candidates) {
    if (candidate.old_weight_version != active_version) {
      continue;
    }
    CandidateWeightUpdate message = ToCandidateWeightUpdate(candidate);
    std::unique_ptr<WeightUpdateCert> buffered_cert;
    if (!controller_.AddLocalCandidateAndMaybeCert(candidate, &buffered_cert)) {
      continue;
    }
    if (WeightUpdateTraceEnabled()) {
      LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
                 << " drain_candidate digest=" << message.candidate_digest()
                 << " activation=" << message.activation_view();
    }
    if (buffered_cert != nullptr) {
      if (WeightUpdateTraceEnabled()) {
        LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
                   << " buffered_cert digest="
                   << buffered_cert->candidate().candidate_digest();
      }
      if (controller_.HandleCert(*buffered_cert)) {
        RefreshPendingActivationView();
        candidate_inflight_ = true;
        candidate_inflight_version_ = active_version;
      }
      BroadcastCert(*buffered_cert);
      ActivateReady();
      return;
    }
    BroadcastCandidate(message);
    std::unique_ptr<WeightUpdateVote> vote =
        controller_.HandleCandidate(message);
    if (vote == nullptr) {
      candidate_inflight_ = false;
      continue;
    }
    if (WeightUpdateTraceEnabled()) {
      LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
                 << " local_vote digest=" << vote->candidate_digest()
                 << " signer=" << vote->signer();
    }
    candidate_inflight_ = true;
    candidate_inflight_version_ = active_version;
    std::unique_ptr<WeightUpdateCert> cert = controller_.HandleVote(*vote);
    BroadcastVote(*vote);
    MaybeBroadcastSyntheticWeightUpdateVoteEquivocationForExperiment();
    MaybeBroadcastConflictingWeightUpdateVoteForExperiment(*vote);
    if (cert != nullptr) {
      if (WeightUpdateTraceEnabled()) {
        LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
                   << " local_cert digest="
                   << cert->candidate().candidate_digest();
      }
      controller_.HandleCert(*cert);
      RefreshPendingActivationView();
      BroadcastCert(*cert);
      ActivateReady();
    }
    return;
  }
}

void CertifiedWeightUpdatePipeline::RefreshPendingActivationView() {
  next_pending_activation_view_.store(
      controller_.EarliestPendingActivationView(), std::memory_order_release);
}

void CertifiedWeightUpdatePipeline::MaybeActivateReadyAfterViewAdvance() {
  MaybeBroadcastSyntheticWeightUpdateVoteEquivocationForExperiment();
  const int pending_view =
      next_pending_activation_view_.load(std::memory_order_acquire);
  if (pending_view <= 0) {
    return;
  }
  const int current_view = CurrentView();
  if (current_view >= pending_view) {
    ActivateReady(current_view);
  }
}

void CertifiedWeightUpdatePipeline::ActivateReady() {
  ActivateReady(CurrentView());
}

void CertifiedWeightUpdatePipeline::ActivateReady(int view) {
  if (weight_schedule_ == nullptr) {
    return;
  }
  int pending_view =
      next_pending_activation_view_.load(std::memory_order_acquire);
  if (pending_view <= 0) {
    RefreshPendingActivationView();
    pending_view =
        next_pending_activation_view_.load(std::memory_order_acquire);
  }
  if (pending_view <= 0 || view < pending_view) {
    return;
  }
  if (controller_.ActivateReady(view) && reputation_adapter_ != nullptr) {
    resdb::consensus::reputation::ReputationWeightSnapshot snapshot;
    snapshot.weights = weight_schedule_->ActiveWeights();
    snapshot.weight_root_hex = weight_schedule_->ActiveWeightRoot();
    snapshot.weight_version = weight_schedule_->ActiveWeightVersion();
    if (leader_schedule_ != nullptr) {
      snapshot.leader_selection_enabled = leader_schedule_->enabled();
      snapshot.leader_weights = leader_schedule_->ActiveLeaderWeights();
      snapshot.leader_weight_root_hex =
          leader_schedule_->ActiveLeaderWeightRoot();
      snapshot.leader_weight_version = leader_schedule_->ActiveLeaderVersion();
      snapshot.leader_eligible_min_weight =
          leader_schedule_->ActiveEligibleMinWeight();
    }
    const std::vector<int64_t> active_leader_weights =
        leader_schedule_ == nullptr ? snapshot.weights
                                    : leader_schedule_->ActiveLeaderWeights();
    LOG(INFO) << "TD-Hotstuff activated certified weights view:" << view
              << " weight_version:" << snapshot.weight_version
              << " active_weights:[" << JoinInt64Vector(snapshot.weights)
              << "]"
              << " leader_weights:[" << JoinInt64Vector(active_leader_weights)
              << "]";
    std::cout << "activated TD-Hotstuff weight update version:"
              << snapshot.weight_version << " view:" << view
              << " active_weights:[" << JoinInt64Vector(snapshot.weights)
              << "]"
              << " leader_weights:[" << JoinInt64Vector(active_leader_weights)
              << "]" << std::endl;
    reputation_adapter_->UpdateActiveWeights(std::move(snapshot));
    candidate_inflight_ = false;
  }
  RefreshPendingActivationView();
}

std::unique_ptr<WeightUpdateCert>
CertifiedWeightUpdatePipeline::LatestCertForProposal(int view) const {
  return controller_.LatestCertForProposal(view);
}

void CertifiedWeightUpdatePipeline::ProcessCertFromProposal(
    const WeightUpdateCert& cert, int view) {
  controller_.HandleCert(cert);
  if (WeightUpdateTraceEnabled()) {
    LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
               << " proposal_cert digest="
               << cert.candidate().candidate_digest() << " view=" << view;
  }
  RefreshPendingActivationView();
  ActivateReady(view);
}

bool CertifiedWeightUpdatePipeline::ReceiveCandidate(
    std::unique_ptr<CandidateWeightUpdate> candidate) {
  if (candidate == nullptr) {
    return false;
  }
  std::unique_ptr<WeightUpdateCert> observed_cert;
  if (!controller_.ObserveCandidate(*candidate, &observed_cert)) {
    if (WeightUpdateTraceEnabled()) {
      LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
                 << " observe_candidate_reject digest="
                 << candidate->candidate_digest();
    }
    return false;
  }
  if (WeightUpdateTraceEnabled()) {
    LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
               << " observe_candidate digest="
               << candidate->candidate_digest();
  }
  if (observed_cert != nullptr) {
    if (WeightUpdateTraceEnabled()) {
      LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
                 << " observed_cert digest="
                 << observed_cert->candidate().candidate_digest();
    }
    if (controller_.HandleCert(*observed_cert)) {
      RefreshPendingActivationView();
      BroadcastCert(*observed_cert);
    }
    ActivateReady();
    return true;
  }
  std::unique_ptr<WeightUpdateVote> vote =
      controller_.HandleCandidate(*candidate);
  if (vote == nullptr) {
    if (WeightUpdateTraceEnabled()) {
      LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
                 << " no_local_vote_for_candidate digest="
                 << candidate->candidate_digest();
    }
    MaybeBroadcastRemoteCandidateWeightUpdateVoteEquivocationForExperiment(
        *candidate);
    return true;
  }
  if (WeightUpdateTraceEnabled()) {
    LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
               << " remote_candidate_local_vote digest="
               << vote->candidate_digest();
  }
  std::unique_ptr<WeightUpdateCert> cert = controller_.HandleVote(*vote);
  BroadcastVote(*vote);
  MaybeBroadcastConflictingWeightUpdateVoteForExperiment(*vote);
  if (cert != nullptr) {
    if (WeightUpdateTraceEnabled()) {
      LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
                 << " remote_candidate_cert digest="
                 << cert->candidate().candidate_digest();
    }
    if (controller_.HandleCert(*cert)) {
      RefreshPendingActivationView();
    }
    BroadcastCert(*cert);
    ActivateReady();
  }
  return true;
}

bool CertifiedWeightUpdatePipeline::MaybeMakeSignedWeightUpdateVoteEvidenceSnapshot(
    const WeightUpdateVote& vote,
    TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot* snapshot) const {
  const bool adapter_available = reputation_adapter_ != nullptr;
  const bool wants_evidence =
      adapter_available &&
      reputation_adapter_->WantsSignedWeightUpdateVoteEvidence();
  const bool basic_valid =
      snapshot != nullptr && adapter_available && wants_evidence &&
      verifier_ != nullptr && vote.signer() > 0 &&
      !vote.old_weight_root().empty() && vote.activation_view() > 0 &&
      !vote.candidate_digest().empty();
  const bool persistent_fault =
      basic_valid &&
      reputation_adapter_->IsPersistentStrongFaultValidator(vote.signer());
  const bool signature_valid =
      basic_valid && !persistent_fault &&
      verifier_->VerifyMessage(WeightUpdateVotePayload(vote), vote.signature());
  if (WeightUpdateTraceEnabled()) {
    LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
               << " wue_evidence_gate signer=" << vote.signer()
               << " wants=" << (wants_evidence ? 1 : 0)
               << " basic=" << (basic_valid ? 1 : 0)
               << " persistent=" << (persistent_fault ? 1 : 0)
               << " signature=" << (signature_valid ? 1 : 0)
               << " view=" << CurrentView()
               << " activation=" << vote.activation_view()
               << " digest=" << vote.candidate_digest();
  }
  if (!basic_valid || persistent_fault || !signature_valid) {
    return false;
  }
  snapshot->local_node_id = node_id_;
  snapshot->total_replicas = total_replicas_;
  snapshot->view = CurrentView();
  snapshot->validator_id = vote.signer();
  snapshot->old_weight_root = vote.old_weight_root();
  snapshot->old_weight_version = vote.old_weight_version();
  snapshot->activation_view = vote.activation_view();
  snapshot->candidate_digest = vote.candidate_digest();
  snapshot->signature_verified = true;
  snapshot->active_weight_root = vote.old_weight_root();
  snapshot->active_weight_version = vote.old_weight_version();
  return true;
}

bool CertifiedWeightUpdatePipeline::ReceiveVote(
    std::unique_ptr<WeightUpdateVote> vote) {
  if (vote == nullptr) {
    return false;
  }
  TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot evidence_snapshot;
  const bool has_evidence_snapshot =
      MaybeMakeSignedWeightUpdateVoteEvidenceSnapshot(*vote, &evidence_snapshot);
  if (has_evidence_snapshot && reputation_adapter_ != nullptr) {
    const bool recorded = reputation_adapter_->TryRecordSignedWeightUpdateVote(
        std::move(evidence_snapshot));
    if (WeightUpdateTraceEnabled()) {
      LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
                 << " wue_evidence_recorded signer=" << vote->signer()
                 << " recorded=" << (recorded ? 1 : 0)
                 << " digest=" << vote->candidate_digest();
    }
  }
  std::unique_ptr<WeightUpdateCert> cert = controller_.HandleVote(*vote);
  if (WeightUpdateTraceEnabled()) {
    LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
               << " receive_vote signer=" << vote->signer()
               << " digest=" << vote->candidate_digest()
               << " cert=" << (cert == nullptr ? 0 : 1);
  }
  if (cert != nullptr) {
    if (controller_.HandleCert(*cert)) {
      RefreshPendingActivationView();
    }
    BroadcastCert(*cert);
    ActivateReady();
  }
  return true;
}

bool CertifiedWeightUpdatePipeline::ReceiveCert(
    std::unique_ptr<WeightUpdateCert> cert) {
  if (cert == nullptr) {
    return false;
  }
  const bool accepted = controller_.HandleCert(*cert);
  if (WeightUpdateTraceEnabled()) {
    LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
               << " receive_cert digest="
               << cert->candidate().candidate_digest()
               << " accepted=" << (accepted ? 1 : 0);
  }
  if (accepted) {
    RefreshPendingActivationView();
    BroadcastCert(*cert);
    ActivateReady();
  }
  return accepted;
}

void CertifiedWeightUpdatePipeline::MaybeBroadcastConflictingWeightUpdateVoteForExperiment(
    const WeightUpdateVote& vote) {
  if (!WeightUpdateVoteEquivocationForExperimentEnabled() ||
      !WeightUpdateVoteEquivocationOnCandidateForExperimentEnabled()) {
    return;
  }
  BroadcastConflictingWeightUpdateVoteForExperiment(vote);
}

void CertifiedWeightUpdatePipeline::
    BroadcastConflictingWeightUpdateVoteForExperiment(
        const WeightUpdateVote& vote) {
  std::unique_ptr<WeightUpdateVote> conflicting =
      MakeConflictingWeightUpdateVoteForExperiment(vote, node_id_, verifier_);
  if (conflicting == nullptr) {
    LOG(ERROR) << "failed to build TD-Hotstuff conflicting weight update vote";
    return;
  }
  BroadcastVote(*conflicting);
}

void CertifiedWeightUpdatePipeline::
    MaybeBroadcastSyntheticWeightUpdateVoteEquivocationForExperiment() {
  if (synthetic_weight_update_vote_equivocation_done_.load(
          std::memory_order_acquire)) {
    return;
  }
  const int current_view = CurrentView();
  if (current_view <
      WeightUpdateVoteEquivocationStartViewForExperiment()) {
    return;
  }
  if (!WeightUpdateVoteEquivocationForExperimentEnabled() ||
      verifier_ == nullptr || weight_schedule_ == nullptr) {
    return;
  }
  const std::string old_root = weight_schedule_->ActiveWeightRoot();
  if (old_root.empty()) {
    return;
  }
  const uint64_t old_version = weight_schedule_->ActiveWeightVersion();
  const int activation_view =
      WeightUpdateVoteEquivocationStartViewForExperiment() + 1;
  const std::string key = "synthetic|" + old_root + "|" +
                          std::to_string(old_version) + "|" +
                          std::to_string(activation_view);
  {
    std::lock_guard<std::mutex> lk(experiment_vote_mutex_);
    if (!experiment_vote_digests_.insert(key).second) {
      synthetic_weight_update_vote_equivocation_done_.store(
          true, std::memory_order_release);
      return;
    }
  }

  WeightUpdateVote vote;
  vote.set_signer(node_id_);
  vote.set_old_weight_root(old_root);
  vote.set_old_weight_version(old_version);
  vote.set_activation_view(activation_view);
  vote.set_candidate_digest(
      resdb::consensus::reputation::HashHex(key + "|candidate"));
  auto signature_or = verifier_->SignMessage(WeightUpdateVotePayload(vote));
  if (!signature_or.ok()) {
    LOG(ERROR) << "failed to sign TD-Hotstuff synthetic weight update vote for "
                  "WUE";
    return;
  }
  *vote.mutable_signature() = *signature_or;
  BroadcastVote(vote);
  BroadcastConflictingWeightUpdateVoteForExperiment(vote);
  synthetic_weight_update_vote_equivocation_done_.store(
      true, std::memory_order_release);
}

void CertifiedWeightUpdatePipeline::
    MaybeBroadcastRemoteCandidateWeightUpdateVoteEquivocationForExperiment(
        const CandidateWeightUpdate& candidate) {
  if (!WeightUpdateVoteEquivocationForExperimentEnabled() ||
      !WeightUpdateVoteEquivocationOnCandidateForExperimentEnabled() ||
      verifier_ == nullptr || candidate.candidate_digest().empty()) {
    return;
  }
  const std::string key =
      candidate.old_weight_root() + "|" +
      std::to_string(candidate.old_weight_version()) + "|" +
      std::to_string(candidate.activation_view()) + "|" +
      candidate.candidate_digest();
  {
    std::lock_guard<std::mutex> lk(experiment_vote_mutex_);
    if (!experiment_vote_digests_.insert(key).second) {
      return;
    }
  }
  std::unique_ptr<WeightUpdateVote> vote =
      MakeWeightUpdateVoteForCandidateDigestForExperiment(
          candidate, candidate.candidate_digest(), node_id_, verifier_);
  if (vote == nullptr) {
    LOG(ERROR) << "failed to build TD-Hotstuff remote-candidate "
                  "weight update vote for WUE";
    return;
  }
  if (WeightUpdateTraceEnabled()) {
    LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
               << " remote_candidate_experiment_vote digest="
               << vote->candidate_digest();
  }
  BroadcastVote(*vote);
  MaybeBroadcastConflictingWeightUpdateVoteForExperiment(*vote);
}

}  // namespace td_hotstuff
}  // namespace resdb
