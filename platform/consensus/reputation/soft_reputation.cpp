#include "platform/consensus/reputation/soft_reputation.h"

#include <algorithm>

#include "platform/consensus/reputation/reputation_utils.h"

namespace resdb {
namespace consensus {
namespace reputation {

int VoteScore(uint64_t inclusions, uint64_t opportunities) {
  const uint64_t numerator = 100 * (1 + inclusions);
  const uint64_t denominator = 2 + opportunities;
  return std::max(0, std::min(100, RoundedDivide(numerator, denominator)));
}

int LeaderCertifiedScore(uint64_t certified_count,
                         uint64_t leader_opportunities) {
  if (leader_opportunities == 0) {
    return 100;
  }
  return VoteScore(certified_count, leader_opportunities);
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
  constexpr int kNoRecoveryBelow = 30;
  constexpr int kFullRecoveryAt = 67;
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
