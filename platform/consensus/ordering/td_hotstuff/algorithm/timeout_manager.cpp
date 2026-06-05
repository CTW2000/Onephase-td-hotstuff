#include "platform/consensus/ordering/td_hotstuff/algorithm/timeout_manager.h"

#include <algorithm>
#include <cstdlib>
#include <sstream>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace resdb {
namespace td_hotstuff {
namespace {

void SetError(std::string* error, const std::string& message) {
  if (error != nullptr) {
    *error = message;
  }
}

std::string BuildBitmap(const std::vector<int>& signers, int total_replicas) {
  if (total_replicas <= 0) {
    return "";
  }
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

bool BetterHighQc(const QC& candidate, const QC& current) {
  if (candidate.view() != current.view()) {
    return candidate.view() > current.view();
  }
  return candidate.hash() > current.hash();
}

std::vector<int> SignersFromVotes(
    const std::map<int, TimeoutVote>& votes_by_signer) {
  std::vector<int> signers;
  signers.reserve(votes_by_signer.size());
  for (const auto& entry : votes_by_signer) {
    signers.push_back(entry.first);
  }
  return signers;
}

}  // namespace

TimeoutConfig TimeoutConfigFromEnv() {
  TimeoutConfig config;
  const char* enabled = std::getenv("TD_HS_TIMEOUT_ENABLE");
  config.enabled = enabled != nullptr && std::string(enabled) == "1";

  const char* timeout_ms = std::getenv("TD_HS_TIMEOUT_MS");
  if (timeout_ms != nullptr && std::string(timeout_ms).size() > 0) {
    config.timeout_ms = std::max(1, std::stoi(timeout_ms));
  }
  return config;
}

bool ShouldTimeoutView(int current_view, int last_valid_proposal_view) {
  return current_view > 0 && last_valid_proposal_view < current_view;
}

TimeoutManager::TimeoutManager(
    int node_id, int total_replicas, SignatureVerifier* verifier,
    std::shared_ptr<WeightSchedule> weight_schedule)
    : node_id_(node_id),
      total_replicas_(total_replicas),
      verifier_(verifier),
      weight_schedule_(std::move(weight_schedule)),
      certificate_verifier_(total_replicas, verifier, weight_schedule_) {}

std::unique_ptr<TimeoutVote> TimeoutManager::CreateTimeoutVote(
    int view, const QC& high_qc) {
  if (verifier_ == nullptr || view <= 0 || node_id_ <= 0) {
    return nullptr;
  }
  auto vote = std::make_unique<TimeoutVote>();
  vote->set_view(view);
  vote->set_signer(node_id_);
  *vote->mutable_high_qc() = high_qc;

  auto signature_or = verifier_->SignMessage(TimeoutVotePayload(*vote));
  if (!signature_or.ok()) {
    LOG(ERROR) << "failed to sign TD-Hotstuff timeout vote, view:" << view;
    return nullptr;
  }
  *vote->mutable_signature() = *signature_or;
  return vote;
}

std::unique_ptr<TimeoutCert> TimeoutManager::AddVote(const TimeoutVote& vote) {
  std::string error;
  if (!VerifyTimeoutVote(vote, &error)) {
    LOG(ERROR) << "reject TD-Hotstuff timeout vote: " << error;
    return nullptr;
  }

  auto& votes_for_view = votes_by_view_[vote.view()];
  if (votes_for_view.find(vote.signer()) != votes_for_view.end()) {
    return nullptr;
  }
  votes_for_view[vote.signer()] = vote;

  int64_t total_weight = 0;
  for (const auto& entry : votes_for_view) {
    total_weight += WeightForSigner(entry.first, vote.view());
  }
  if (total_weight < QuorumWeightForView(vote.view()) ||
      formed_cert_views_.find(vote.view()) != formed_cert_views_.end()) {
    return nullptr;
  }
  formed_cert_views_.insert(vote.view());
  return std::make_unique<TimeoutCert>(BuildCertForView(vote.view()));
}

bool TimeoutManager::VerifyTimeoutVote(const TimeoutVote& vote,
                                       std::string* error) const {
  if (verifier_ == nullptr) {
    SetError(error, "missing verifier");
    return false;
  }
  if (vote.view() <= 0) {
    SetError(error, "invalid timeout view");
    return false;
  }
  if (vote.signer() != vote.signature().node_id()) {
    SetError(error, "signer mismatch");
    return false;
  }
  if (WeightForSigner(vote.signer(), vote.view()) <= 0) {
    SetError(error, "signer has no active weight");
    return false;
  }
  return certificate_verifier_.VerifyTimeoutVote(vote, error);
}

bool TimeoutManager::VerifyTimeoutCert(const TimeoutCert& cert,
                                       std::string* error) const {
  return certificate_verifier_.VerifyTimeoutCert(cert, error);
}

void TimeoutManager::ResetBelowView(int view) {
  for (auto it = votes_by_view_.begin(); it != votes_by_view_.end();) {
    if (it->first < view) {
      it = votes_by_view_.erase(it);
    } else {
      ++it;
    }
  }
  for (auto it = formed_cert_views_.begin(); it != formed_cert_views_.end();) {
    if (*it < view) {
      it = formed_cert_views_.erase(it);
    } else {
      ++it;
    }
  }
}

bool TimeoutManager::VerifyHighQc(const QC& qc, int timeout_view,
                                  std::string* error) const {
  if (qc.hash().empty()) {
    return true;
  }
  if (qc.view() >= timeout_view) {
    SetError(error, "timeout high qc is not lower than timeout view");
    return false;
  }
  return certificate_verifier_.VerifyQC(qc, error);
}

int64_t TimeoutManager::WeightForSigner(int signer, int view) const {
  if (weight_schedule_ != nullptr) {
    return weight_schedule_->WeightForSigner(signer, view);
  }
  return signer >= 1 && signer <= total_replicas_ ? 1 : 0;
}

int64_t TimeoutManager::QuorumWeightForView(int view) const {
  if (weight_schedule_ != nullptr) {
    return weight_schedule_->QuorumWeightForView(view);
  }
  return total_replicas_ * 2 / 3 + 1;
}

TimeoutCert TimeoutManager::BuildCertForView(int view) const {
  TimeoutCert cert;
  cert.set_view(view);
  cert.set_quorum_rule_id(kTimeoutQuorumRuleId);

  auto it = votes_by_view_.find(view);
  if (it == votes_by_view_.end()) {
    return cert;
  }
  const std::vector<int> signers = SignersFromVotes(it->second);
  cert.set_signer_bitmap(BuildBitmap(signers, total_replicas_));

  QC best_high_qc;
  for (const auto& entry : it->second) {
    const TimeoutVote& vote = entry.second;
    *cert.add_votes() = vote;
    if (BetterHighQc(vote.high_qc(), best_high_qc)) {
      best_high_qc = vote.high_qc();
    }
  }
  *cert.mutable_high_qc() = best_high_qc;
  return cert;
}

}  // namespace td_hotstuff
}  // namespace resdb
