#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "platform/consensus/reputation/reputation_types.h"

namespace resdb {
namespace consensus {
namespace reputation {

std::string WeightRootHex(const std::vector<int64_t>& weights);
std::string LeaderWeightRootHex(const std::vector<int64_t>& weights,
                                int64_t eligible_min_weight,
                                int leader_selection_version);
std::vector<int> BuildLeaderEpochSchedule(
    int total_replicas, const std::vector<int64_t>& leader_weights,
    int64_t eligible_min_weight, int epoch_start_view, int epoch_views);
std::string LeaderScheduleRootHex(
    int leader_selection_version, int64_t eligible_min_weight,
    const std::string& leader_weight_root_hex, int epoch_start_view,
    int epoch_views, const std::vector<int>& leader_epoch_leaders);
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
    const std::string& penalty_root_hex = "",
    const std::vector<int64_t>& leader_weights = {},
    const std::string& leader_weight_root_hex = "",
    int64_t leader_eligible_min_weight = 10,
    int leader_selection_version = 1,
    int leader_epoch_start_view = 0,
    int leader_epoch_views = 0,
    const std::vector<int>& leader_epoch_leaders = std::vector<int>(),
    const std::string& leader_schedule_root_hex = "");

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
