#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace resdb {
namespace td_hotstuff {

std::vector<int64_t> NormalizeWeightPoints(
    const std::vector<int64_t>& weights, int total_replicas);
int64_t CalculateWeightQuorum(const std::vector<int64_t>& weights);
std::string WeightRootHex(const std::vector<int64_t>& weights);
std::string HashHexForTesting(const std::string& data);

std::vector<int64_t> NormalizeLeaderWeights(
    const std::vector<int64_t>& weights, int total_replicas);
std::string LeaderWeightRootHex(const std::vector<int64_t>& leader_weights);
std::string LeaderRandomnessRefHex(const std::string& old_weight_root,
                                   uint64_t old_weight_version,
                                   int activation_view,
                                   const std::string& leader_weight_root);

}  // namespace td_hotstuff
}  // namespace resdb
