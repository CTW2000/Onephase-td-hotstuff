#pragma once

#include <cstdint>

#include "platform/consensus/reputation/reputation_types.h"

namespace resdb {
namespace consensus {
namespace reputation {

int VoteScore(uint64_t inclusions, uint64_t opportunities);
int LeaderDirichletScore(const LeaderDirichletCounter& counter,
                         const ReputationConfig& config);
uint64_t FairExpectedSignerOpportunities(uint64_t selected_signer_slots,
                                         int total_replicas,
                                         uint64_t event_count);
int RecoveryCreditForScore(int score, int max_recovery_per_epoch);
bool HasNearFairInclusion(uint64_t inclusions, uint64_t opportunities);

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
