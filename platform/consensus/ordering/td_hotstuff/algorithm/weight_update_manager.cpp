#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_manager.h"

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <set>
#include <sstream>
#include <utility>

#include <glog/logging.h>

#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr const char* kEnableEnv = "TD_HS_WEIGHT_UPDATE_ENABLE";
constexpr const char* kEpochViewsEnv = "TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS";
constexpr const char* kActivationDelayEnv =
    "TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY";
constexpr const char* kProtocolId = "td_hotstuff_weight_update_v1";
constexpr const char* kQuorumRuleId = "old-weight-2f-plus-1";

int PositiveIntFromEnv(const char* env_name, int default_value) {
  const char* raw = std::getenv(env_name);
  if (raw == nullptr || std::string(raw).empty()) {
    return default_value;
  }
  try {
    const int value = std::stoi(raw);
    return value > 0 ? value : default_value;
  } catch (const std::exception&) {
    LOG(WARNING) << "invalid " << env_name << ":" << raw
                 << ", use default:" << default_value;
    return default_value;
  }
}

bool SetError(std::string* error, const std::string& message) {
  if (error != nullptr) {
    *error = message;
  }
  return false;
}

bool ValidWeight(int64_t weight) { return weight >= 1 && weight <= 100; }

bool CandidateChangesWeights(const CandidateWeightUpdate& update,
                             const WeightSnapshot& snapshot) {
  if (update.validators_size() != static_cast<int>(snapshot.weights.size())) {
    return true;
  }
  for (int i = 0; i < update.validators_size(); ++i) {
    if (update.validators(i).next_weight() != snapshot.weights[i]) {
      return true;
    }
  }
  return false;
}

bool CandidateChangesLeaderProfile(const CandidateWeightUpdate& update,
                                   const WeightSnapshot& snapshot) {
  if (snapshot.leader_weights.empty() ||
      update.leader_validators_size() !=
          static_cast<int>(snapshot.leader_weights.size())) {
    return false;
  }
  for (int i = 0; i < update.leader_validators_size(); ++i) {
    if (update.leader_validators(i).leader_weight() !=
        snapshot.leader_weights[i]) {
      return true;
    }
  }
  return false;
}

bool CandidateHasCanonicalEpochWindow(const CandidateWeightUpdate& update,
                                      int epoch_views) {
  if (epoch_views <= 0 || update.event_count() == 0 ||
      update.event_count() > static_cast<uint64_t>(epoch_views)) {
    return false;
  }
  return update.end_qc_view() > 0 &&
         update.end_qc_view() % epoch_views == 0 &&
         update.start_qc_view() == update.end_qc_view() - epoch_views + 1;
}

bool CandidateShouldEnterConsensus(const CandidateWeightUpdate& update,
                                   const WeightSnapshot& snapshot,
                                   int epoch_views) {
  return CandidateHasCanonicalEpochWindow(update, epoch_views) &&
         (CandidateChangesWeights(update, snapshot) ||
          CandidateChangesLeaderProfile(update, snapshot));
}

int CandidateBroadcaster(const CandidateWeightUpdate& update,
                         int total_replicas) {
  if (total_replicas <= 0) {
    return 0;
  }
  return static_cast<int>(update.window_index() % total_replicas) + 1;
}

WeightSnapshot SnapshotForScheduleView(const WeightSchedule& schedule,
                                       int current_view) {
  WeightSnapshot snapshot;
  snapshot.weights = schedule.ActiveWeights();
  snapshot.weight_root = schedule.ActiveWeightRoot();
  snapshot.weight_version = schedule.ActiveWeightVersion();
  snapshot.quorum_weight = CalculateWeightQuorum(snapshot.weights);
  snapshot.current_view = current_view;
  snapshot.leader_weights = snapshot.weights;
  snapshot.leader_weight_root = LeaderWeightRootHex(snapshot.leader_weights);
  snapshot.leader_weight_version = snapshot.weight_version;
  return snapshot;
}

}  // namespace

WeightUpdateConfig WeightUpdateConfigFromEnv() {
  WeightUpdateConfig config;
  const char* enabled = std::getenv(kEnableEnv);
  config.enabled = enabled != nullptr && std::string(enabled) == "1";
  config.epoch_views = PositiveIntFromEnv(kEpochViewsEnv, 4096);
  config.activation_epoch_delay = PositiveIntFromEnv(kActivationDelayEnv, 2);
  return config;
}

int ComputeWeightUpdateActivationView(int end_qc_view, int epoch_views,
                                      int activation_epoch_delay) {
  if (end_qc_view <= 0 || epoch_views <= 0) {
    return 0;
  }
  const int delay = std::max(activation_epoch_delay, 1);
  // The evidence window ends with a QC at the epoch boundary. That QC was
  // formed under the old schedule, so the new schedule starts at the first
  // following view.
  return ((end_qc_view / epoch_views) + delay) * epoch_views + 1;
}

WeightSnapshot MakeWeightSnapshot(const WeightSchedule& schedule,
                                  int current_view) {
  return SnapshotForScheduleView(schedule, current_view);
}

std::vector<int64_t> CandidateNextWeights(
    const CandidateWeightUpdate& update) {
  std::vector<int64_t> weights;
  weights.reserve(update.validators_size());
  for (const CandidateValidatorWeight& validator : update.validators()) {
    weights.push_back(validator.next_weight());
  }
  return weights;
}

std::vector<int64_t> CandidateLeaderWeights(
    const CandidateWeightUpdate& update) {
  std::vector<int64_t> weights;
  weights.reserve(update.leader_validators_size());
  for (const CandidateLeaderWeight& validator : update.leader_validators()) {
    weights.push_back(validator.leader_weight());
  }
  return weights;
}

CandidateWeightUpdate BuildCandidateWeightUpdate(
    const VoteScoreCandidate& candidate) {
  CandidateWeightUpdate update;
  update.set_protocol_id(kProtocolId);
  update.set_old_weight_version(candidate.old_weight_version);
  update.set_old_weight_root(candidate.old_weight_root_hex);
  update.set_activation_view(candidate.activation_view);
  update.set_total_replicas(candidate.total_replicas);
  update.set_window_index(candidate.window_index);
  update.set_start_qc_view(candidate.start_qc_view);
  update.set_end_qc_view(candidate.end_qc_view);
  update.set_event_count(candidate.event_count);
  update.set_metric_root(candidate.metric_root_hex);
  update.set_next_weight_root(candidate.next_weight_root_hex);
  update.set_candidate_digest(candidate.candidate_digest_hex);
  update.set_leader_weight_root(candidate.leader_weight_root_hex);
  update.set_leader_params_version(candidate.leader_params_version);
  update.set_leader_randomness_ref(candidate.leader_randomness_ref);
  for (size_t i = 0; i < candidate.next_weights.size(); ++i) {
    CandidateValidatorWeight* validator = update.add_validators();
    validator->set_validator_id(static_cast<int>(i + 1));
    validator->set_next_weight(candidate.next_weights[i]);
  }
  for (size_t i = 0; i < candidate.leader_weights.size(); ++i) {
    CandidateLeaderWeight* validator = update.add_leader_validators();
    validator->set_validator_id(static_cast<int>(i + 1));
    validator->set_leader_weight(candidate.leader_weights[i]);
  }
  return update;
}

std::string WeightUpdateVotePayload(const CandidateWeightUpdate& update,
                                    int validator_id) {
  std::ostringstream out;
  out << "td_hotstuff_weight_update_vote_v1|"
      << update.candidate_digest() << '|' << validator_id << '|'
      << update.old_weight_root() << '|' << update.old_weight_version() << '|'
      << update.activation_view();
  return out.str();
}

WeightUpdateManager::WeightUpdateManager(int node_id, int total_replicas,
                                         SignatureVerifier* verifier,
                                         WeightUpdateConfig config)
    : node_id_(node_id),
      total_replicas_(total_replicas),
      verifier_(verifier),
      config_(config) {}

void WeightUpdateManager::AddLocalCandidates(
    std::vector<VoteScoreCandidate> candidates,
    const WeightSnapshot& snapshot) {
  if (!config_.enabled) {
    return;
  }
  for (const VoteScoreCandidate& local_candidate : candidates) {
    CandidateWeightUpdate update = BuildCandidateWeightUpdate(local_candidate);
    std::string error;
    if (!ValidateCandidateFields(update, snapshot, &error)) {
      LOG(WARNING) << "skip local weight update candidate:" << error;
      continue;
    }
    if (update.activation_view() <= snapshot.current_view) {
      LOG(WARNING) << "skip stale local weight update candidate activation:"
                   << update.activation_view() << " current:"
                   << snapshot.current_view;
      continue;
    }
    if (!CandidateShouldEnterConsensus(update, snapshot, config_.epoch_views)) {
      continue;
    }
    const std::string candidate_digest = update.candidate_digest();
    if (!local_weight_candidates_.emplace(candidate_digest, local_candidate)
             .second) {
      continue;
    }
    if (CandidateBroadcaster(update, total_replicas_) == node_id_) {
      weight_update_candidates_[candidate_digest] = update;
      if (broadcast_candidate_digests_.insert(candidate_digest).second) {
        outbound_messages_.candidates.push_back(update);
      }
    }
  }
  for (const auto& entry : weight_update_candidates_) {
    TryVoteForCandidate(entry.second, snapshot);
  }
}

void WeightUpdateManager::HandleCandidate(
    const CandidateWeightUpdate& update, const WeightSnapshot& snapshot) {
  if (!config_.enabled || update.candidate_digest().empty()) {
    return;
  }
  std::string error;
  if (!ValidateCandidateFields(update, snapshot, &error)) {
    LOG(WARNING) << "reject weight update candidate:" << error;
    return;
  }
  if (!CandidateShouldEnterConsensus(update, snapshot, config_.epoch_views)) {
    return;
  }
  weight_update_candidates_[update.candidate_digest()] = update;
  TryVoteForCandidate(update, snapshot);
}

void WeightUpdateManager::HandleVote(const WeightUpdateVote& vote,
                                     const WeightSnapshot& snapshot) {
  if (!config_.enabled) {
    return;
  }
  AddWeightUpdateVote(vote, snapshot);
}

void WeightUpdateManager::HandleCert(const WeightUpdateCert& cert,
                                     const WeightSnapshot& snapshot) {
  if (!config_.enabled) {
    return;
  }
  std::string error;
  if (!VerifyCertWithSnapshot(cert, snapshot, &error)) {
    LOG(WARNING) << "reject weight update cert:" << error;
    return;
  }
  pending_weight_update_certs_[cert.candidate_update().activation_view()] = cert;
  const std::string& candidate_digest =
      cert.candidate_update().candidate_digest();
  if (broadcast_weight_cert_digests_.insert(candidate_digest).second) {
    outbound_messages_.certs.push_back(cert);
  }
}

WeightPluginOutboundMessages WeightUpdateManager::DrainOutboundMessages(
    int current_view, const WeightSnapshot& snapshot) {
  if (!config_.enabled) {
    return WeightPluginOutboundMessages();
  }
  (void)current_view;
  (void)snapshot;
  WeightPluginOutboundMessages messages;
  messages.candidates.swap(outbound_messages_.candidates);
  messages.votes.swap(outbound_messages_.votes);
  messages.certs.swap(outbound_messages_.certs);
  return messages;
}

std::vector<InstallableWeightUpdate>
WeightUpdateManager::TakeInstallableUpdates(
    int current_view, const WeightSnapshot& snapshot) {
  std::vector<InstallableWeightUpdate> updates;
  if (!config_.enabled) {
    return updates;
  }
  WeightSnapshot effective_snapshot = snapshot;
  effective_snapshot.current_view = current_view;
  for (auto it = pending_weight_update_certs_.begin();
       it != pending_weight_update_certs_.end();) {
    if (it->first > current_view) {
      ++it;
      continue;
    }
    std::string error;
    if (!VerifyCertWithSnapshot(it->second, effective_snapshot, &error)) {
      LOG(WARNING) << "drop invalid pending weight update cert:" << error;
      it = pending_weight_update_certs_.erase(it);
      continue;
    }
    const CandidateWeightUpdate& update = it->second.candidate_update();
    InstallableWeightUpdate installable;
    installable.activation_view = update.activation_view();
    installable.old_weight_root = update.old_weight_root();
    installable.old_weight_version = update.old_weight_version();
    installable.next_weights = CandidateNextWeights(update);
    installable.leader_weights = CandidateLeaderWeights(update);
    installable.leader_weight_root = update.leader_weight_root();
    installable.leader_params_version = update.leader_params_version();
    installable.leader_randomness_ref = update.leader_randomness_ref();
    installable.cert = it->second;
    updates.push_back(installable);
    it = pending_weight_update_certs_.erase(it);
  }
  return updates;
}

void WeightUpdateManager::OnWeightsActivated(const WeightSnapshot&) {
  ClearPluginState();
}

bool WeightUpdateManager::VerifyCandidate(
    const CandidateWeightUpdate& update,
    const VoteScoreCandidate& local_candidate,
    const WeightSchedule& schedule, std::string* error) const {
  return VerifyCandidateWithSnapshot(update, local_candidate,
                                     MakeWeightSnapshot(schedule,
                                                        update.activation_view() - 1),
                                     error);
}

std::unique_ptr<WeightUpdateVote> WeightUpdateManager::CreateVote(
    const CandidateWeightUpdate& update,
    const VoteScoreCandidate& local_candidate,
    const WeightSchedule& schedule) const {
  return CreateVoteWithSnapshot(update, local_candidate,
                                MakeWeightSnapshot(schedule,
                                                   update.activation_view() - 1));
}

bool WeightUpdateManager::VerifyVote(const WeightUpdateVote& vote,
                                     const CandidateWeightUpdate& update,
                                     std::string* error) const {
  if (vote.candidate_digest() != update.candidate_digest() ||
      vote.old_weight_root() != update.old_weight_root() ||
      vote.old_weight_version() != update.old_weight_version() ||
      vote.activation_view() != update.activation_view()) {
    return SetError(error, "vote fields do not bind candidate");
  }
  if (vote.validator_id() != vote.signature().node_id()) {
    return SetError(error, "vote signer mismatch");
  }
  if (vote.validator_id() < 1 || vote.validator_id() > total_replicas_) {
    return SetError(error, "vote signer out of range");
  }
  if (!verifier_->VerifyMessage(
          WeightUpdateVotePayload(update, vote.validator_id()),
          vote.signature())) {
    return SetError(error, "vote signature invalid");
  }
  return true;
}

bool WeightUpdateManager::VerifyCert(const WeightUpdateCert& cert,
                                     const WeightSchedule& schedule,
                                     std::string* error) const {
  return VerifyCertWithSnapshot(
      cert, MakeWeightSnapshot(schedule,
                               cert.candidate_update().activation_view() - 1),
      error);
}

bool WeightUpdateManager::ValidateCandidateFields(
    const CandidateWeightUpdate& update, const WeightSnapshot& snapshot,
    std::string* error) const {
  if (!config_.enabled) {
    return SetError(error, "weight update disabled");
  }
  if (update.protocol_id() != kProtocolId) {
    return SetError(error, "protocol id mismatch");
  }
  if (update.total_replicas() != total_replicas_ ||
      update.validators_size() != total_replicas_) {
    return SetError(error, "replica count mismatch");
  }
  if (update.old_weight_root() != snapshot.weight_root ||
      update.old_weight_version() !=
          static_cast<int64_t>(snapshot.weight_version)) {
    return SetError(error, "old weight root/version mismatch");
  }
  if (update.activation_view() <= update.end_qc_view()) {
    return SetError(error, "activation view is not after evidence window");
  }
  std::vector<int64_t> next_weights;
  next_weights.reserve(update.validators_size());
  for (int i = 0; i < update.validators_size(); ++i) {
    const CandidateValidatorWeight& validator = update.validators(i);
    if (validator.validator_id() != i + 1) {
      return SetError(error, "validator order mismatch");
    }
    if (!ValidWeight(validator.next_weight())) {
      return SetError(error, "candidate weight out of range");
    }
    next_weights.push_back(validator.next_weight());
  }
  if (update.next_weight_root() != WeightRootHex(next_weights)) {
    return SetError(error, "next weight root mismatch");
  }
  if (update.leader_validators_size() != total_replicas_) {
    return SetError(error, "leader replica count mismatch");
  }
  std::vector<int64_t> leader_weights;
  leader_weights.reserve(update.leader_validators_size());
  for (int i = 0; i < update.leader_validators_size(); ++i) {
    const CandidateLeaderWeight& validator = update.leader_validators(i);
    if (validator.validator_id() != i + 1) {
      return SetError(error, "leader validator order mismatch");
    }
    if (!ValidWeight(validator.leader_weight())) {
      return SetError(error, "leader weight out of range");
    }
    leader_weights.push_back(validator.leader_weight());
  }
  if (update.leader_weight_root() != LeaderWeightRootHex(leader_weights)) {
    return SetError(error, "leader weight root mismatch");
  }
  if (leader_weights != snapshot.weights) {
    return SetError(error, "leader weights must match old weights");
  }
  if (update.leader_params_version() != 1 ||
      update.leader_randomness_ref().empty()) {
    return SetError(error, "leader params mismatch");
  }
  const std::string expected_digest = VoteScoreCandidateDigest(
      update.total_replicas(), update.window_index(), update.start_qc_view(),
      update.end_qc_view(), update.event_count(), update.old_weight_root(),
      update.old_weight_version(), update.activation_view(),
      update.metric_root(), update.next_weight_root(), next_weights,
      update.leader_weight_root(), update.leader_params_version(),
      update.leader_randomness_ref(), leader_weights);
  if (update.candidate_digest() != expected_digest) {
    return SetError(error, "candidate digest mismatch");
  }
  return true;
}

bool WeightUpdateManager::VerifyCandidateWithSnapshot(
    const CandidateWeightUpdate& update,
    const VoteScoreCandidate& local_candidate,
    const WeightSnapshot& snapshot, std::string* error) const {
  if (!ValidateCandidateFields(update, snapshot, error)) {
    return false;
  }
  if (!CandidateShouldEnterConsensus(update, snapshot, config_.epoch_views)) {
    return SetError(error, "candidate not eligible for consensus");
  }
  if (update.candidate_digest() != local_candidate.candidate_digest_hex) {
    return SetError(error, "local recomputation mismatch");
  }
  if (update.next_weight_root() != local_candidate.next_weight_root_hex ||
      update.leader_weight_root() != local_candidate.leader_weight_root_hex ||
      update.leader_params_version() !=
          local_candidate.leader_params_version ||
      update.leader_randomness_ref() !=
          local_candidate.leader_randomness_ref ||
      update.activation_view() != local_candidate.activation_view ||
      update.old_weight_root() != local_candidate.old_weight_root_hex ||
      update.old_weight_version() !=
          static_cast<int64_t>(local_candidate.old_weight_version)) {
    return SetError(error, "candidate fields do not match local recomputation");
  }
  return true;
}

std::unique_ptr<WeightUpdateVote>
WeightUpdateManager::CreateVoteWithSnapshot(
    const CandidateWeightUpdate& update,
    const VoteScoreCandidate& local_candidate,
    const WeightSnapshot& snapshot) const {
  std::string error;
  if (!VerifyCandidateWithSnapshot(update, local_candidate, snapshot, &error)) {
    LOG(WARNING) << "skip weight update vote:" << error;
    return nullptr;
  }
  auto vote = std::make_unique<WeightUpdateVote>();
  vote->set_candidate_digest(update.candidate_digest());
  vote->set_validator_id(node_id_);
  vote->set_old_weight_root(update.old_weight_root());
  vote->set_old_weight_version(update.old_weight_version());
  vote->set_activation_view(update.activation_view());
  const std::string payload = WeightUpdateVotePayload(update, node_id_);
  auto signature = verifier_->SignMessage(payload);
  if (!signature.ok()) {
    LOG(ERROR) << "sign weight update vote failed";
    return nullptr;
  }
  *vote->mutable_signature() = *signature;
  return vote;
}

bool WeightUpdateManager::VerifyCertWithSnapshot(
    const WeightUpdateCert& cert, const WeightSnapshot& snapshot,
    std::string* error) const {
  if (cert.quorum_rule_id() != kQuorumRuleId) {
    return SetError(error, "quorum rule mismatch");
  }
  const CandidateWeightUpdate& update = cert.candidate_update();
  if (!ValidateCandidateFields(update, snapshot, error)) {
    return false;
  }
  if (!CandidateShouldEnterConsensus(update, snapshot, config_.epoch_views)) {
    return SetError(error, "candidate not eligible for consensus");
  }
  std::set<int> signers;
  int64_t signer_weight = 0;
  for (const WeightUpdateVote& vote : cert.votes()) {
    if (!VerifyVote(vote, update, error)) {
      return false;
    }
    if (!signers.insert(vote.validator_id()).second) {
      return SetError(error, "duplicate weight update vote");
    }
    if (vote.validator_id() < 1 ||
        vote.validator_id() > static_cast<int>(snapshot.weights.size())) {
      return SetError(error, "cert signer out of range");
    }
    signer_weight += snapshot.weights[vote.validator_id() - 1];
  }
  std::vector<int> signer_list(signers.begin(), signers.end());
  if (cert.signer_bitmap() != BuildSignerBitmap(signer_list, total_replicas_)) {
    return SetError(error, "cert signer bitmap mismatch");
  }
  if (signer_weight < snapshot.quorum_weight) {
    std::ostringstream out;
    out << "weight update cert weight below quorum:" << signer_weight
        << " quorum:" << snapshot.quorum_weight;
    return SetError(error, out.str());
  }
  return true;
}

void WeightUpdateManager::TryVoteForCandidate(
    const CandidateWeightUpdate& candidate, const WeightSnapshot& snapshot) {
  auto local_it = local_weight_candidates_.find(candidate.candidate_digest());
  if (local_it == local_weight_candidates_.end() ||
      voted_weight_candidate_digests_.count(candidate.candidate_digest()) > 0) {
    return;
  }
  std::unique_ptr<WeightUpdateVote> vote =
      CreateVoteWithSnapshot(candidate, local_it->second, snapshot);
  if (vote == nullptr) {
    return;
  }
  voted_weight_candidate_digests_.insert(candidate.candidate_digest());
  AddWeightUpdateVote(*vote, snapshot);
  outbound_messages_.votes.push_back(*vote);
}

void WeightUpdateManager::AddWeightUpdateVote(
    const WeightUpdateVote& vote, const WeightSnapshot& snapshot) {
  auto candidate_it = weight_update_candidates_.find(vote.candidate_digest());
  if (candidate_it == weight_update_candidates_.end()) {
    return;
  }
  std::string error;
  if (!VerifyVote(vote, candidate_it->second, &error)) {
    LOG(WARNING) << "reject weight update vote:" << error;
    return;
  }
  weight_update_votes_[vote.candidate_digest()][vote.validator_id()] = vote;
  TryFormWeightUpdateCert(vote.candidate_digest(), snapshot);
}

void WeightUpdateManager::TryFormWeightUpdateCert(
    const std::string& candidate_digest, const WeightSnapshot& snapshot) {
  if (broadcast_weight_cert_digests_.count(candidate_digest) > 0) {
    return;
  }
  auto candidate_it = weight_update_candidates_.find(candidate_digest);
  auto votes_it = weight_update_votes_.find(candidate_digest);
  if (candidate_it == weight_update_candidates_.end() ||
      votes_it == weight_update_votes_.end()) {
    return;
  }
  WeightUpdateCert cert;
  *cert.mutable_candidate_update() = candidate_it->second;
  std::vector<int> signers;
  for (const auto& entry : votes_it->second) {
    signers.push_back(entry.first);
    *cert.add_votes() = entry.second;
  }
  cert.set_signer_bitmap(BuildSignerBitmap(signers, total_replicas_));
  cert.set_quorum_rule_id(kQuorumRuleId);
  std::string error;
  if (!VerifyCertWithSnapshot(cert, snapshot, &error)) {
    return;
  }
  broadcast_weight_cert_digests_.insert(candidate_digest);
  pending_weight_update_certs_[cert.candidate_update().activation_view()] = cert;
  outbound_messages_.certs.push_back(cert);
}

void WeightUpdateManager::ClearPluginState() {
  local_weight_candidates_.clear();
  weight_update_candidates_.clear();
  weight_update_votes_.clear();
  pending_weight_update_certs_.clear();
  voted_weight_candidate_digests_.clear();
  broadcast_candidate_digests_.clear();
  broadcast_weight_cert_digests_.clear();
  outbound_messages_.candidates.clear();
  outbound_messages_.votes.clear();
  outbound_messages_.certs.clear();
}

}  // namespace td_hotstuff
}  // namespace resdb
