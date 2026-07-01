#include "platform/consensus/reputation/soft_reputation.h"

#include <algorithm>

#include "platform/consensus/reputation/reputation_utils.h"

namespace resdb {
namespace consensus {
namespace reputation {
namespace {

constexpr uint64_t kLeaderDirichletCounterScale = 1000;

int BoundLeaderScore(int value) {
  return std::max(0, std::min(100, value));
}

uint64_t ScaledLeaderAlpha(int alpha) {
  return static_cast<uint64_t>(std::max(0, alpha)) *
         kLeaderDirichletCounterScale;
}

}  // namespace

int VoteScore(uint64_t inclusions, uint64_t opportunities) {
  const uint64_t numerator = 100 * (1 + inclusions);
  const uint64_t denominator = 2 + opportunities;
  return std::max(0, std::min(100, RoundedDivide(numerator, denominator)));
}

// LeaderCertifiedScore removed (audit fix): Dirichlet leader scoring is now the
// sole leader-score path; the old certified-ratio fallback no longer exists.

int LeaderDirichletScore(const LeaderDirichletCounter& counter,
                         const ReputationConfig& config) {
  const uint64_t commit =
      counter.commit + ScaledLeaderAlpha(config.leader_dirichlet_alpha_commit);
  const uint64_t certify_only = counter.certify_only +
                                ScaledLeaderAlpha(
                                    config.leader_dirichlet_alpha_certify_only);
  const uint64_t timeout =
      counter.timeout + ScaledLeaderAlpha(config.leader_dirichlet_alpha_timeout);
  const uint64_t total = commit + certify_only + timeout;
  if (total == 0) {
    return 100;
  }
  const uint64_t numerator =
      commit * 100 +
      certify_only * static_cast<uint64_t>(BoundLeaderScore(
                         config.leader_certify_only_score)) +
      timeout * static_cast<uint64_t>(BoundLeaderScore(config.leader_timeout_score));
  return BoundLeaderScore(static_cast<int>(RoundedDivide(numerator, total)));
}

uint64_t FairExpectedSignerOpportunities(uint64_t selected_signer_slots,
                                         int total_replicas,
                                         uint64_t event_count) {
  if (selected_signer_slots == 0 || total_replicas <= 0 || event_count == 0) {
    return 0;
  }
  const uint64_t expected = static_cast<uint64_t>(
      RoundedDivide(selected_signer_slots, static_cast<uint64_t>(total_replicas)));
  return std::max<uint64_t>(1, std::min<uint64_t>(event_count, expected));
}

int RecoveryCreditForScore(int score, int max_recovery_per_epoch) {
  if (max_recovery_per_epoch <= 0) {
    return 0;
  }
  constexpr int kNoRecoveryBelow = 10;
  constexpr int kFullRecoveryAt = 30;
  const int bounded_score = std::max(0, std::min(100, score));
  if (bounded_score >= kFullRecoveryAt) {
    return max_recovery_per_epoch;
  }
  if (bounded_score < kNoRecoveryBelow) {
    return 0;
  }
  return RoundedDivide(static_cast<uint64_t>(bounded_score - kNoRecoveryBelow) *
                           static_cast<uint64_t>(max_recovery_per_epoch),
                       kFullRecoveryAt - kNoRecoveryBelow);
}

bool HasNearFairInclusion(uint64_t inclusions, uint64_t opportunities) {
  if (opportunities == 0 || inclusions == 0) {
    return false;
  }
  return RoundedDivide(inclusions * 100, opportunities) >= 75;
}

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
