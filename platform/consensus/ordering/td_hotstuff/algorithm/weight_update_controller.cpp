#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_controller.h"

#include <glog/logging.h>

#include <algorithm>
#include <sstream>
#include <tuple>
#include <utility>

#include "platform/consensus/reputation/reputation_roots.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr const char* kQuorumRuleId = "old_weight_quorum_v1";
constexpr int64_t kMinCandidateWeight = 1;
constexpr int64_t kMaxCandidateWeight = 100;

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

std::string DigestForCandidateParts(const CandidateWeightUpdate& candidate,
                                    const std::vector<int64_t>& weights) {
  return resdb::consensus::reputation::ReputationCandidateDigest(
      candidate.next_weights_size(), candidate.window_index(),
      candidate.window_start(), candidate.window_end(),
      /*event_count=*/0, candidate.old_weight_root(),
      candidate.old_weight_version(), candidate.activation_view(),
      candidate.metric_root(), candidate.reputation_root(),
      candidate.next_weight_root(), weights, candidate.strong_fault_root(),
      candidate.penalty_root());
}

}  // namespace

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
  for (size_t i = 0; i < candidate.next_weights.size(); ++i) {
    CandidateWeight* weight = message.add_next_weights();
    weight->set_validator_id(static_cast<int>(i) + 1);
    weight->set_weight(candidate.next_weights[i]);
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
    int node_id, int total_replicas, std::shared_ptr<WeightSchedule> weight_schedule,
    SignatureVerifier* verifier)
    : node_id_(node_id),
      total_replicas_(total_replicas),
      weight_schedule_(std::move(weight_schedule)),
      verifier_(verifier) {}

bool WeightUpdateController::AddLocalCandidate(
    const resdb::consensus::reputation::ReputationCandidate& candidate) {
  std::lock_guard<std::mutex> lk(mutex_);
  CandidateWeightUpdate message = ToCandidateWeightUpdate(candidate);
  if (!ValidateCandidateStructure(message)) {
    LOG(WARNING) << "local reputation candidate failed structure guard";
    return false;
  }
  const CandidateKey key = KeyForCandidate(message);
  local_candidates_[key] = message;
  candidates_by_digest_[message.candidate_digest()] = std::move(message);
  return true;
}

std::unique_ptr<WeightUpdateVote> WeightUpdateController::HandleCandidate(
    const CandidateWeightUpdate& candidate) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (verifier_ == nullptr || !ValidateCandidateStructure(candidate)) {
    return nullptr;
  }
  const CandidateKey key = KeyForCandidate(candidate);
  auto it = local_candidates_.find(key);
  if (it == local_candidates_.end()) {
    return nullptr;
  }
  if (voted_digests_.find(candidate.candidate_digest()) != voted_digests_.end()) {
    return nullptr;
  }
  candidates_by_digest_[candidate.candidate_digest()] = candidate;
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
  voted_digests_.insert(candidate.candidate_digest());
  return vote;
}

std::unique_ptr<WeightUpdateCert> WeightUpdateController::HandleVote(
    const WeightUpdateVote& vote) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto candidate_it = candidates_by_digest_.find(vote.candidate_digest());
  if (candidate_it == candidates_by_digest_.end()) {
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
  if (!ValidateCandidateStructure(cert.candidate())) {
    return false;
  }
  std::map<int, WeightUpdateVote> votes;
  for (const WeightUpdateVote& vote : cert.votes()) {
    if (!VerifyVoteForCandidate(vote, cert.candidate())) {
      return false;
    }
    votes.emplace(vote.signer(), vote);
  }
  if (VoteWeight(votes) < weight_schedule_->QuorumWeightForView(0)) {
    return false;
  }
  std::vector<int> signers;
  for (const auto& entry : votes) {
    signers.push_back(entry.first);
  }
  if (cert.signer_bitmap() != BuildBitmap(signers, total_replicas_)) {
    return false;
  }
  pending_certs_[cert.candidate().candidate_digest()] = cert;
  candidates_by_digest_[cert.candidate().candidate_digest()] = cert.candidate();
  return true;
}

bool WeightUpdateController::ActivateReady(int current_view) {
  std::lock_guard<std::mutex> lk(mutex_);
  bool activated = false;
  for (auto it = pending_certs_.begin(); it != pending_certs_.end();) {
    const CandidateWeightUpdate& candidate = it->second.candidate();
    if (candidate.activation_view() >= current_view) {
      ++it;
      continue;
    }
    const uint64_t before_version = weight_schedule_->ActiveWeightVersion();
    if (weight_schedule_->ScheduleUpdate(
            candidate.activation_view(), CandidateWeights(candidate),
            candidate.old_weight_root(), candidate.old_weight_version()) &&
        weight_schedule_->ActivateUpTo(current_view) &&
        weight_schedule_->ActiveWeightVersion() != before_version) {
      activated = true;
      it = pending_certs_.erase(it);
    } else {
      ++it;
    }
  }
  return activated;
}

WeightUpdateController::CandidateKey WeightUpdateController::KeyForCandidate(
    const CandidateWeightUpdate& candidate) const {
  CandidateKey key;
  key.old_weight_root = candidate.old_weight_root();
  key.old_weight_version = candidate.old_weight_version();
  key.window_start = candidate.window_start();
  key.window_end = candidate.window_end();
  key.activation_view = candidate.activation_view();
  key.candidate_digest = candidate.candidate_digest();
  return key;
}

bool WeightUpdateController::ValidateCandidateStructure(
    const CandidateWeightUpdate& candidate) const {
  if (weight_schedule_ == nullptr || candidate.next_weights_size() != total_replicas_) {
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
  if (candidate.next_weight_root() !=
      resdb::consensus::reputation::WeightRootHex(weights)) {
    return false;
  }
  if (weights == weight_schedule_->ActiveWeights()) {
    return false;
  }
  if (DigestForCandidateParts(candidate, weights) != candidate.candidate_digest()) {
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

int64_t WeightUpdateController::VoteWeight(
    const std::map<int, WeightUpdateVote>& votes) const {
  int64_t total = 0;
  for (const auto& entry : votes) {
    total += weight_schedule_->WeightForSigner(entry.first, 0);
  }
  return total;
}

std::unique_ptr<WeightUpdateCert> WeightUpdateController::MaybeFormCert(
    VoteBucket* bucket) const {
  if (bucket == nullptr ||
      VoteWeight(bucket->votes) < weight_schedule_->QuorumWeightForView(0)) {
    return nullptr;
  }
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

}  // namespace td_hotstuff
}  // namespace resdb
