#pragma once

#include <cstdint>

namespace resdb {
namespace consensus {
namespace reputation {

int VoteScore(uint64_t inclusions, uint64_t opportunities);
int LeaderCertifiedScore(uint64_t certified_count,
                         uint64_t leader_opportunities);
uint64_t FairExpectedSignerOpportunities(uint64_t selected_signer_slots,
                                         int total_replicas,
                                         uint64_t event_count);
int RecoveryCreditForScore(int score, int max_recovery_per_epoch);
bool HasNearFairInclusion(uint64_t inclusions, uint64_t opportunities);

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
