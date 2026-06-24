#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_controller.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>

#include "platform/consensus/reputation/reputation_roots.h"
#include "platform/consensus/reputation/reputation_utils.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr const char* kQuorumRuleId = "old_weight_quorum_v1";
constexpr int64_t kMinCandidateWeight = 1;
constexpr int64_t kMaxCandidateWeight = 100;
constexpr size_t kMaxLocalCandidates = 128;
constexpr size_t kMaxCandidatesByDigest = 128;
constexpr size_t kMaxVoteBuckets = 128;
constexpr size_t kMaxPendingVoteDigests = 128;
constexpr size_t kMaxPendingCerts = 64;
constexpr size_t kMaxRecentCerts = 64;
constexpr size_t kMaxAcceptedCerts = 128;
constexpr size_t kMaxVotedScopes = 128;
constexpr size_t kMaxEmittedCerts = 128;

template <typename Map, typename Predicate>
void EraseIf(Map* values, Predicate predicate) {
  if (values == nullptr) {
    return;
  }
  for (auto it = values->begin(); it != values->end();) {
    if (predicate(*it)) {
      it = values->erase(it);
    } else {
      ++it;
    }
  }
}

template <typename Map>
void TrimMapToSize(Map* values, size_t limit) {
  if (values == nullptr) {
    return;
  }
  while (values->size() > limit) {
    values->erase(values->begin());
  }
}

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

bool AllowNoOpCandidateForExperiment() {
  return EnvFlagEnabled("TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_CANDIDATE");
}

bool AllowNoOpCandidateInitialVersionOnlyForExperiment() {
  return EnvFlagEnabled(
      "TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_INITIAL_VERSION_ONLY");
}

int AllowNoOpCandidateMinStartViewForExperiment() {
  const char* raw =
      std::getenv("TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_MIN_START_VIEW");
  if (raw == nullptr) {
    return 0;
  }
  return std::max(0, std::atoi(raw));
}

bool WeightUpdateTraceEnabled() {
  static const bool enabled = EnvFlagEnabled("TD_HS_WEIGHT_UPDATE_TRACE");
  return enabled;
}

int WeightUpdateCertPiggybackLeadViews() {
  const char* raw =
      std::getenv("TD_HS_WEIGHT_UPDATE_CERT_PIGGYBACK_LEAD_VIEWS");
  if (raw == nullptr || std::string(raw).empty()) {
    return 64;
  }
  return std::max(0, std::atoi(raw));
}

std::string BuildBitmap(const std::vector<int>& signers, int total_replicas) {
  std::string bitmap((total_replicas + 7) / 8, '\0');
  for (int signer : signers) {
    if (signer < 1 || signer > total_replicas) {
      continue;
    }
    const int bit = signer - 1;
    bitmap[bit / 8] =
        static_cast<char>(bitmap[bit / 8] | (1 << (bit % 8)));
  }
  return bitmap;
}

std::vector<int> BitmapSigners(const std::string& bitmap, int total_replicas) {
  std::vector<int> signers;
  for (int signer = 1; signer <= total_replicas; ++signer) {
    const int bit = signer - 1;
    if (bit / 8 >= static_cast<int>(bitmap.size())) {
      continue;
    }
    if ((static_cast<unsigned char>(bitmap[bit / 8]) & (1 << (bit % 8))) != 0) {
      signers.push_back(signer);
    }
  }
  return signers;
}

std::string DigestForCandidateParts(
    const CandidateWeightUpdate& candidate,
    const std::vector<int64_t>& weights,
    const std::vector<int64_t>& leader_weights,
    const std::vector<int>& leader_epoch_leaders) {
  return resdb::consensus::reputation::ReputationCandidateDigest(
      candidate.next_weights_size(), candidate.window_index(),
      candidate.window_start(), candidate.window_end(),
      /*event_count=*/0, candidate.old_weight_root(),
      candidate.old_weight_version(), candidate.activation_view(),
      candidate.metric_root(), candidate.reputation_root(),
      candidate.next_weight_root(), weights, candidate.strong_fault_root(),
      candidate.penalty_root(), leader_weights, candidate.leader_weight_root(),
      candidate.leader_eligible_min_weight(),
      candidate.leader_selection_version(), candidate.leader_epoch_start_view(),
      candidate.leader_epoch_views(), leader_epoch_leaders,
      candidate.leader_schedule_root());
}

}  // namespace

void WeightUpdateController::PruneStaleStateLocked() {
  if (weight_schedule_ == nullptr) {
    return;
  }
  const uint64_t active_version = weight_schedule_->ActiveWeightVersion();
  EraseIf(&local_candidates_, [active_version](const auto& entry) {
    return entry.first.old_weight_version < active_version;
  });
  EraseIf(&candidates_by_digest_, [active_version](const auto& entry) {
    return entry.second.old_weight_version() < active_version;
  });
  EraseIf(&vote_buckets_, [active_version](const auto& entry) {
    const CandidateWeightUpdate& candidate = entry.second.candidate;
    return !candidate.candidate_digest().empty() &&
           candidate.old_weight_version() < active_version;
  });
  EraseIf(&pending_certs_, [active_version](const auto& entry) {
    return entry.second.candidate().old_weight_version() < active_version;
  });
  EraseIf(&recent_certs_, [active_version](const auto& entry) {
    return entry.second.candidate().old_weight_version() + 1 < active_version;
  });
  EraseIf(&accepted_cert_by_scope_, [active_version](const auto& entry) {
    return entry.first.old_weight_version < active_version;
  });
  EraseIf(&voted_candidate_by_scope_, [active_version](const auto& entry) {
    return entry.first.old_weight_version < active_version;
  });
  EraseIf(&emitted_cert_version_by_digest_,
          [active_version](const auto& entry) {
            return entry.second < active_version;
          });
  TrimMapToSize(&local_candidates_, kMaxLocalCandidates);
  TrimMapToSize(&candidates_by_digest_, kMaxCandidatesByDigest);
  TrimMapToSize(&vote_buckets_, kMaxVoteBuckets);
  TrimMapToSize(&pending_votes_by_digest_, kMaxPendingVoteDigests);
  TrimMapToSize(&pending_certs_, kMaxPendingCerts);
  TrimMapToSize(&recent_certs_, kMaxRecentCerts);
  TrimMapToSize(&accepted_cert_by_scope_, kMaxAcceptedCerts);
  TrimMapToSize(&voted_candidate_by_scope_, kMaxVotedScopes);
  TrimMapToSize(&emitted_cert_version_by_digest_, kMaxEmittedCerts);
}

std::vector<int> CandidateLeaderEpochLeaders(
    const CandidateWeightUpdate& candidate);

CandidateWeightUpdate ToCandidateWeightUpdate(
    const resdb::consensus::reputation::ReputationCandidate& candidate) {
  CandidateWeightUpdate message;
  message.set_algorithm(candidate.algorithm);
  message.set_window_index(candidate.window_index);
  message.set_window_start(candidate.start_view);
  message.set_window_end(candidate.end_view);
  message.set_old_weight_root(candidate.old_weight_root_hex);
  message.set_old_weight_version(candidate.old_weight_version);
  message.set_activation_view(candidate.activation_view);
  message.set_metric_root(candidate.metric_root_hex);
  message.set_reputation_root(candidate.reputation_root_hex);
  message.set_strong_fault_root(candidate.strong_fault_root_hex);
  message.set_penalty_root(candidate.penalty_root_hex);
  message.set_next_weight_root(candidate.next_weight_root_hex);
  message.set_candidate_digest(candidate.candidate_digest_hex);
  message.set_leader_weight_root(candidate.leader_weight_root_hex);
  message.set_leader_selection_version(candidate.leader_selection_version);
  message.set_leader_eligible_min_weight(candidate.leader_eligible_min_weight);
  message.set_leader_epoch_start_view(candidate.leader_epoch_start_view);
  message.set_leader_epoch_views(candidate.leader_epoch_views);
  message.set_leader_schedule_root(candidate.leader_schedule_root_hex);
  for (size_t i = 0; i < candidate.next_weights.size(); ++i) {
    CandidateWeight* weight = message.add_next_weights();
    weight->set_validator_id(static_cast<int>(i) + 1);
    weight->set_weight(candidate.next_weights[i]);
  }
  for (size_t i = 0; i < candidate.leader_weights.size(); ++i) {
    CandidateWeight* weight = message.add_leader_weights();
    weight->set_validator_id(static_cast<int>(i) + 1);
    weight->set_weight(candidate.leader_weights[i]);
  }
  for (int leader : candidate.leader_epoch_leaders) {
    message.add_leader_epoch_leaders(leader);
  }
  return message;
}

std::string WeightUpdateVotePayload(const WeightUpdateVote& vote) {
  std::ostringstream out;
  out << "td_hotstuff_weight_update_vote_v1|" << vote.signer() << '|'
      << vote.old_weight_root() << '|' << vote.old_weight_version() << '|'
      << vote.activation_view() << '|' << vote.candidate_digest();
  return out.str();
}

WeightUpdateController::WeightUpdateController(
    int node_id, int total_replicas,
    std::shared_ptr<WeightSchedule> weight_schedule, SignatureVerifier* verifier,
    std::shared_ptr<LeaderSelectionSchedule> leader_schedule)
    : node_id_(node_id),
      total_replicas_(total_replicas),
      weight_schedule_(std::move(weight_schedule)),
      leader_schedule_(std::move(leader_schedule)),
      verifier_(verifier) {}

bool WeightUpdateController::AddLocalCandidate(
    const resdb::consensus::reputation::ReputationCandidate& candidate) {
  return AddLocalCandidateAndMaybeCert(candidate, nullptr);
}

std::unique_ptr<WeightUpdateCert>
WeightUpdateController::AddLocalCandidateAndMaybeCert(
    const resdb::consensus::reputation::ReputationCandidate& candidate) {
  std::unique_ptr<WeightUpdateCert> cert;
  if (!AddLocalCandidateAndMaybeCert(candidate, &cert)) {
    return nullptr;
  }
  return cert;
}

bool WeightUpdateController::AddLocalCandidateAndMaybeCert(
    const resdb::consensus::reputation::ReputationCandidate& candidate,
    std::unique_ptr<WeightUpdateCert>* cert) {
  std::lock_guard<std::mutex> lk(mutex_);
  PruneStaleStateLocked();
  CandidateWeightUpdate message = ToCandidateWeightUpdate(candidate);
  if (!ValidateCandidateStructure(message)) {
    LOG(WARNING) << "local reputation candidate failed structure guard";
    return false;
  }
  if (cert != nullptr) {
    cert->reset();
  }
  const CandidateKey key = KeyForCandidate(message);
  const std::string digest = message.candidate_digest();
  local_candidates_[key] = message;
  candidates_by_digest_[digest] = message;
  AbsorbPendingVotesLocked(digest, candidates_by_digest_[digest]);
  if (cert != nullptr) {
    auto bucket_it = vote_buckets_.find(digest);
    if (bucket_it != vote_buckets_.end()) {
      *cert = MaybeFormCert(&bucket_it->second);
    }
  }
  return true;
}

bool WeightUpdateController::ObserveCandidate(
    const CandidateWeightUpdate& candidate,
    std::unique_ptr<WeightUpdateCert>* cert) {
  std::lock_guard<std::mutex> lk(mutex_);
  PruneStaleStateLocked();
  if (!ValidateCandidateStructure(candidate)) {
    return false;
  }
  if (cert != nullptr) {
    cert->reset();
  }
  const std::string digest = candidate.candidate_digest();
  candidates_by_digest_[digest] = candidate;
  AbsorbPendingVotesLocked(digest, candidates_by_digest_[digest]);
  if (cert != nullptr) {
    auto bucket_it = vote_buckets_.find(digest);
    if (bucket_it != vote_buckets_.end()) {
      *cert = MaybeFormCert(&bucket_it->second);
    }
  }
  return true;
}

std::unique_ptr<WeightUpdateVote> WeightUpdateController::HandleCandidate(
    const CandidateWeightUpdate& candidate) {
  std::lock_guard<std::mutex> lk(mutex_);
  PruneStaleStateLocked();
  if (verifier_ == nullptr || !ValidateCandidateStructure(candidate)) {
    return nullptr;
  }
  const CandidateKey key = KeyForCandidate(candidate);
  auto it = local_candidates_.find(key);
  if (it == local_candidates_.end()) {
    return nullptr;
  }
  const VoteScopeKey vote_scope{candidate.old_weight_root(),
                                candidate.old_weight_version()};
  auto voted_it = voted_candidate_by_scope_.find(vote_scope);
  if (voted_it != voted_candidate_by_scope_.end()) {
    return nullptr;
  }
  candidates_by_digest_[candidate.candidate_digest()] = candidate;
  AbsorbPendingVotesLocked(candidate.candidate_digest(),
                           candidates_by_digest_[candidate.candidate_digest()]);
  std::unique_ptr<WeightUpdateVote> vote = std::make_unique<WeightUpdateVote>();
  vote->set_signer(node_id_);
  vote->set_old_weight_root(candidate.old_weight_root());
  vote->set_old_weight_version(candidate.old_weight_version());
  vote->set_activation_view(candidate.activation_view());
  vote->set_candidate_digest(candidate.candidate_digest());
  auto signature_or = verifier_->SignMessage(WeightUpdateVotePayload(*vote));
  if (!signature_or.ok()) {
    return nullptr;
  }
  *vote->mutable_signature() = *signature_or;
  voted_candidate_by_scope_[vote_scope] = candidate.candidate_digest();
  return vote;
}

std::unique_ptr<WeightUpdateCert> WeightUpdateController::HandleVote(
    const WeightUpdateVote& vote) {
  std::lock_guard<std::mutex> lk(mutex_);
  PruneStaleStateLocked();
  auto candidate_it = candidates_by_digest_.find(vote.candidate_digest());
  if (candidate_it == candidates_by_digest_.end()) {
    if (vote.signer() >= 1 && vote.signer() <= total_replicas_ &&
        !vote.candidate_digest().empty()) {
      pending_votes_by_digest_[vote.candidate_digest()][vote.signer()] = vote;
    }
    return nullptr;
  }
  if (!VerifyVoteForCandidate(vote, candidate_it->second)) {
    return nullptr;
  }
  VoteBucket& bucket = vote_buckets_[vote.candidate_digest()];
  if (bucket.candidate.candidate_digest().empty()) {
    bucket.candidate = candidate_it->second;
  }
  bucket.votes.emplace(vote.signer(), vote);
  return MaybeFormCert(&bucket);
}

bool WeightUpdateController::HandleCert(const WeightUpdateCert& cert) {
  std::lock_guard<std::mutex> lk(mutex_);
  PruneStaleStateLocked();
  auto reject = [&](const char* reason) {
    if (WeightUpdateTraceEnabled()) {
      const CandidateWeightUpdate& candidate = cert.candidate();
      LOG(ERROR) << "[WeightUpdateTrace] node=" << node_id_
                 << " handle_cert_reject reason=" << reason
                 << " digest=" << candidate.candidate_digest()
                 << " old_version=" << candidate.old_weight_version()
                 << " active_version="
                 << (weight_schedule_ == nullptr
                         ? 0
                         : weight_schedule_->ActiveWeightVersion())
                 << " activation=" << candidate.activation_view();
    }
    return false;
  };
  if (!ValidateCandidateStructure(cert.candidate())) {
    return reject("candidate_structure");
  }
  const std::string& digest = cert.candidate().candidate_digest();
  const VoteScopeKey cert_scope{cert.candidate().old_weight_root(),
                                cert.candidate().old_weight_version()};
  auto accepted_it = accepted_cert_by_scope_.find(cert_scope);
  if (accepted_it != accepted_cert_by_scope_.end()) {
    return reject(accepted_it->second == digest ? "duplicate"
                                                : "duplicate_scope");
  }
  std::map<int, WeightUpdateVote> votes;
  for (const WeightUpdateVote& vote : cert.votes()) {
    if (!VerifyVoteForCandidate(vote, cert.candidate())) {
      return reject("vote_invalid");
    }
    votes.emplace(vote.signer(), vote);
  }
  if (VoteWeight(votes, cert.candidate().old_weight_version()) <
      weight_schedule_->QuorumWeightForVersion(
          cert.candidate().old_weight_version())) {
    return reject("quorum_weight");
  }
  std::vector<int> signers;
  for (const auto& entry : votes) {
    signers.push_back(entry.first);
  }
  if (cert.signer_bitmap() != BuildBitmap(signers, total_replicas_)) {
    return reject("signer_bitmap");
  }
  if (!StageCandidateSchedule(cert.candidate())) {
    return reject("stage_candidate");
  }
  accepted_cert_by_scope_[cert_scope] = digest;
  pending_certs_[digest] = cert;
  recent_certs_[digest] = cert;
  candidates_by_digest_[digest] = cert.candidate();
  emitted_cert_version_by_digest_[digest] =
      cert.candidate().old_weight_version();
  return true;
}

std::unique_ptr<WeightUpdateCert> WeightUpdateController::LatestCertForProposal(
    int view) const {
  std::lock_guard<std::mutex> lk(mutex_);
  const WeightUpdateCert* best = nullptr;
  const int piggyback_lead_views = WeightUpdateCertPiggybackLeadViews();
  auto consider = [&](const WeightUpdateCert& cert) {
    const CandidateWeightUpdate& candidate = cert.candidate();
    const int activation_view = candidate.activation_view();
    if (view < activation_view &&
        view + piggyback_lead_views < activation_view) {
      return;
    }
    // Piggyback long enough for lagging replicas to learn the cert near the
    // epoch boundary, but do not carry an already-activated cert for the full
    // leader epoch on every proposal.
    if (view > activation_view + piggyback_lead_views) {
      return;
    }
    if (best == nullptr || activation_view > best->candidate().activation_view()) {
      best = &cert;
    }
  };
  for (const auto& entry : pending_certs_) {
    consider(entry.second);
  }
  for (const auto& entry : recent_certs_) {
    consider(entry.second);
  }
  if (best == nullptr) {
    return nullptr;
  }
  return std::make_unique<WeightUpdateCert>(*best);
}

bool WeightUpdateController::ActivateReady(int current_view) {
  std::lock_guard<std::mutex> lk(mutex_);
  PruneStaleStateLocked();
  bool activated = false;
  for (auto it = pending_certs_.begin(); it != pending_certs_.end();) {
    const CandidateWeightUpdate& candidate = it->second.candidate();
    const int effective_activation_view = candidate.activation_view();
    if (effective_activation_view > current_view) {
      ++it;
      continue;
    }
    const uint64_t before_version = weight_schedule_->ActiveWeightVersion();
    if (StageCandidateSchedule(candidate) &&
        weight_schedule_->ActivateUpTo(current_view) &&
        (leader_schedule_ == nullptr || leader_schedule_->ActivateUpTo(current_view) ||
         !leader_schedule_->enabled()) &&
        weight_schedule_->ActiveWeightVersion() != before_version) {
      activated = true;
      it = pending_certs_.erase(it);
    } else {
      ++it;
    }
  }
  return activated;
}

int WeightUpdateController::EarliestPendingActivationView() const {
  std::lock_guard<std::mutex> lk(mutex_);
  int earliest = 0;
  for (const auto& entry : pending_certs_) {
    const int activation_view = entry.second.candidate().activation_view();
    if (activation_view <= 0) {
      continue;
    }
    if (earliest == 0 || activation_view < earliest) {
      earliest = activation_view;
    }
  }
  return earliest;
}

WeightUpdateController::CandidateKey WeightUpdateController::KeyForCandidate(
    const CandidateWeightUpdate& candidate) const {
  CandidateKey key;
  key.old_weight_root = candidate.old_weight_root();
  key.old_weight_version = candidate.old_weight_version();
  key.activation_view = candidate.activation_view();
  key.candidate_digest = candidate.candidate_digest();
  return key;
}

bool WeightUpdateController::ValidateCandidateStructure(
    const CandidateWeightUpdate& candidate) const {
  if (weight_schedule_ == nullptr || candidate.next_weights_size() != total_replicas_ ||
      candidate.leader_weights_size() != total_replicas_) {
    return false;
  }
  if (candidate.old_weight_root() != weight_schedule_->ActiveWeightRoot() ||
      candidate.old_weight_version() != weight_schedule_->ActiveWeightVersion()) {
    return false;
  }
  if (candidate.window_end() <= candidate.window_start() ||
      candidate.activation_view() < candidate.window_end()) {
    return false;
  }
  std::vector<int64_t> weights;
  weights.reserve(total_replicas_);
  for (int i = 0; i < candidate.next_weights_size(); ++i) {
    const CandidateWeight& weight = candidate.next_weights(i);
    if (weight.validator_id() != i + 1 || weight.weight() < kMinCandidateWeight ||
        weight.weight() > kMaxCandidateWeight) {
      return false;
    }
    weights.push_back(weight.weight());
  }
  std::vector<int64_t> leader_weights;
  leader_weights.reserve(total_replicas_);
  for (int i = 0; i < candidate.leader_weights_size(); ++i) {
    const CandidateWeight& weight = candidate.leader_weights(i);
    if (weight.validator_id() != i + 1 || weight.weight() < kMinCandidateWeight ||
        weight.weight() > kMaxCandidateWeight) {
      return false;
    }
    leader_weights.push_back(weight.weight());
  }
  if (candidate.leader_selection_version() != 1 ||
      candidate.leader_eligible_min_weight() < kMinCandidateWeight ||
      candidate.leader_eligible_min_weight() > kMaxCandidateWeight) {
    return false;
  }
  if (candidate.next_weight_root() !=
      resdb::consensus::reputation::WeightRootHex(weights)) {
    return false;
  }
  if (candidate.leader_weight_root() !=
      resdb::consensus::reputation::LeaderWeightRootHex(
          leader_weights, candidate.leader_eligible_min_weight(),
          candidate.leader_selection_version())) {
    return false;
  }
  const std::vector<int> leader_epoch_leaders =
      CandidateLeaderEpochLeaders(candidate);
  if (candidate.leader_epoch_start_view() != candidate.activation_view() ||
      candidate.leader_epoch_views() <= 0 ||
      leader_epoch_leaders.size() !=
          static_cast<size_t>(candidate.leader_epoch_views())) {
    return false;
  }
  for (int leader : leader_epoch_leaders) {
    if (leader < 1 || leader > total_replicas_) {
      return false;
    }
  }
  const std::vector<int> expected_epoch_leaders =
      resdb::consensus::reputation::BuildLeaderEpochSchedule(
          total_replicas_, leader_weights,
          candidate.leader_eligible_min_weight(),
          candidate.leader_epoch_start_view(), candidate.leader_epoch_views());
  if (leader_epoch_leaders != expected_epoch_leaders) {
    return false;
  }
  if (candidate.leader_schedule_root() !=
      resdb::consensus::reputation::LeaderScheduleRootHex(
          candidate.leader_selection_version(),
          candidate.leader_eligible_min_weight(),
          candidate.leader_weight_root(), candidate.leader_epoch_start_view(),
          candidate.leader_epoch_views(), leader_epoch_leaders)) {
    return false;
  }
  if (weights == weight_schedule_->ActiveWeights()) {
    bool leader_profile_changed = false;
    if (leader_schedule_ != nullptr && leader_schedule_->enabled()) {
      const std::vector<int64_t> active_leader_weights =
          leader_schedule_->ActiveLeaderWeights();
      leader_profile_changed =
          leader_weights != active_leader_weights ||
          candidate.leader_eligible_min_weight() !=
              leader_schedule_->ActiveEligibleMinWeight() ||
          candidate.leader_weight_root() !=
              leader_schedule_->ActiveLeaderWeightRoot();
    }
    if (!leader_profile_changed) {
      if (!AllowNoOpCandidateForExperiment()) {
        return false;
      }
      const int min_start_view = AllowNoOpCandidateMinStartViewForExperiment();
      if (candidate.window_start() < min_start_view) {
        return false;
      }
      if (AllowNoOpCandidateInitialVersionOnlyForExperiment() &&
          candidate.old_weight_version() != 0) {
        return false;
      }
    }
  }
  if (DigestForCandidateParts(candidate, weights, leader_weights,
                              leader_epoch_leaders) !=
      candidate.candidate_digest()) {
    return false;
  }
  return true;
}

bool WeightUpdateController::VerifyVoteForCandidate(
    const WeightUpdateVote& vote, const CandidateWeightUpdate& candidate) const {
  if (verifier_ == nullptr || vote.signer() < 1 || vote.signer() > total_replicas_) {
    return false;
  }
  if (vote.old_weight_root() != candidate.old_weight_root() ||
      vote.old_weight_version() != candidate.old_weight_version() ||
      vote.activation_view() != candidate.activation_view() ||
      vote.candidate_digest() != candidate.candidate_digest()) {
    return false;
  }
  return verifier_->VerifyMessage(WeightUpdateVotePayload(vote), vote.signature());
}

std::vector<int64_t> WeightUpdateController::CandidateWeights(
    const CandidateWeightUpdate& candidate) const {
  std::vector<int64_t> weights;
  weights.reserve(candidate.next_weights_size());
  for (const CandidateWeight& weight : candidate.next_weights()) {
    weights.push_back(weight.weight());
  }
  return weights;
}

std::vector<int64_t> WeightUpdateController::CandidateLeaderWeights(
    const CandidateWeightUpdate& candidate) const {
  std::vector<int64_t> weights;
  weights.reserve(candidate.leader_weights_size());
  for (const CandidateWeight& weight : candidate.leader_weights()) {
    weights.push_back(weight.weight());
  }
  return weights;
}

std::vector<int> CandidateLeaderEpochLeaders(
    const CandidateWeightUpdate& candidate) {
  std::vector<int> leaders;
  leaders.reserve(candidate.leader_epoch_leaders_size());
  for (int leader : candidate.leader_epoch_leaders()) {
    leaders.push_back(leader);
  }
  return leaders;
}

int64_t WeightUpdateController::VoteWeight(
    const std::map<int, WeightUpdateVote>& votes,
    uint64_t old_weight_version) const {
  if (weight_schedule_ == nullptr) {
    return 0;
  }
  int64_t total = 0;
  for (const auto& entry : votes) {
    total += weight_schedule_->WeightForSignerInVersion(entry.first,
                                                        old_weight_version);
  }
  return total;
}


void WeightUpdateController::AbsorbPendingVotesLocked(
    const std::string& digest, const CandidateWeightUpdate& candidate) {
  auto pending_it = pending_votes_by_digest_.find(digest);
  if (pending_it == pending_votes_by_digest_.end()) {
    return;
  }
  VoteBucket& bucket = vote_buckets_[digest];
  if (bucket.candidate.candidate_digest().empty()) {
    bucket.candidate = candidate;
  }
  for (const auto& entry : pending_it->second) {
    const WeightUpdateVote& vote = entry.second;
    if (VerifyVoteForCandidate(vote, candidate)) {
      bucket.votes.emplace(vote.signer(), vote);
    }
  }
  pending_votes_by_digest_.erase(pending_it);
}

std::unique_ptr<WeightUpdateCert> WeightUpdateController::MaybeFormCert(
    VoteBucket* bucket) {
  if (bucket == nullptr ||
      VoteWeight(bucket->votes, bucket->candidate.old_weight_version()) <
          weight_schedule_->QuorumWeightForVersion(
              bucket->candidate.old_weight_version())) {
    return nullptr;
  }
  const std::string& digest = bucket->candidate.candidate_digest();
  if (digest.empty()) {
    return nullptr;
  }
  if (emitted_cert_version_by_digest_.find(digest) !=
      emitted_cert_version_by_digest_.end()) {
    return nullptr;
  }
  emitted_cert_version_by_digest_[digest] =
      bucket->candidate.old_weight_version();
  std::vector<int> signers;
  signers.reserve(bucket->votes.size());
  std::unique_ptr<WeightUpdateCert> cert = std::make_unique<WeightUpdateCert>();
  *cert->mutable_candidate() = bucket->candidate;
  cert->set_quorum_rule_id(kQuorumRuleId);
  for (const auto& entry : bucket->votes) {
    signers.push_back(entry.first);
    *cert->add_votes() = entry.second;
  }
  cert->set_signer_bitmap(BuildBitmap(signers, total_replicas_));
  return cert;
}

bool WeightUpdateController::StageCandidateSchedule(
    const CandidateWeightUpdate& candidate) const {
  const std::vector<int64_t> weights = CandidateWeights(candidate);
  const std::vector<int64_t> leader_weights = CandidateLeaderWeights(candidate);
  const std::vector<int> leader_epoch_leaders =
      CandidateLeaderEpochLeaders(candidate);
  return weight_schedule_->ScheduleUpdate(
             candidate.activation_view(), weights, candidate.old_weight_root(),
             candidate.old_weight_version()) &&
         (leader_schedule_ == nullptr ||
          leader_schedule_->ScheduleEpochUpdate(
              candidate.activation_view(), candidate.old_weight_version() + 1,
              candidate.leader_weight_root(), leader_weights,
              candidate.leader_eligible_min_weight(),
              candidate.leader_epoch_start_view(), candidate.leader_epoch_views(),
              leader_epoch_leaders, candidate.leader_schedule_root()));
}

}  // namespace td_hotstuff
}  // namespace resdb
