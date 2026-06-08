#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "platform/consensus/reputation/reputation_types.h"

namespace resdb {
namespace consensus {
namespace reputation {

std::string WeightRootHex(const std::vector<int64_t>& weights);
void RecomputeReputationCandidateRoots(ReputationCandidate* candidate);
std::string ReputationCandidateDigest(
    int total_replicas, uint64_t window_index, int start_view, int end_view,
    uint64_t event_count, const std::string& old_weight_root_hex,
    uint64_t old_weight_version, int activation_view,
    const std::string& metric_root_hex,
    const std::string& reputation_root_hex,
    const std::string& next_weight_root_hex,
    const std::vector<int64_t>& next_weights,
    const std::string& strong_fault_root_hex = "",
    const std::string& penalty_root_hex = "");

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
