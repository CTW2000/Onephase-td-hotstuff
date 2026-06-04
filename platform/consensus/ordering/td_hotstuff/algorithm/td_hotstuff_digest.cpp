#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff_digest.h"

#include <algorithm>
#include <iomanip>
#include <sstream>

#include "common/crypto/hash.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr int64_t kMinWeight = 1;
constexpr int64_t kMaxWeight = 100;

std::string HexEncode(const std::string& data) {
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (unsigned char ch : data) {
    out << std::setw(2) << static_cast<int>(ch);
  }
  return out.str();
}

std::string WeightCanonical(const std::vector<int64_t>& weights) {
  std::ostringstream out;
  out << "td_hotstuff_weight_root_v1|" << weights.size();
  for (int64_t weight : weights) {
    out << '|' << weight;
  }
  return out.str();
}

}  // namespace

std::vector<int64_t> NormalizeWeightPoints(const std::vector<int64_t>& weights,
                                           int total_replicas) {
  std::vector<int64_t> normalized(std::max(total_replicas, 0), kMinWeight);
  for (int i = 0; i < total_replicas && i < static_cast<int>(weights.size());
       ++i) {
    normalized[i] = std::max(kMinWeight, std::min(kMaxWeight, weights[i]));
  }
  return normalized;
}

int64_t CalculateWeightQuorum(const std::vector<int64_t>& weights) {
  int64_t total_weight = 0;
  for (int64_t weight : weights) {
    total_weight += std::max<int64_t>(weight, kMinWeight);
  }
  return total_weight * 2 / 3 + 1;
}

std::string WeightRootHex(const std::vector<int64_t>& weights) {
  return HexEncode(resdb::utils::CalculateSHA256Hash(WeightCanonical(weights)));
}

std::string HashHexForTesting(const std::string& data) {
  return HexEncode(resdb::utils::CalculateSHA256Hash(data));
}

std::vector<int64_t> NormalizeLeaderWeights(const std::vector<int64_t>& weights,
                                            int total_replicas) {
  return NormalizeWeightPoints(weights, total_replicas);
}

std::string LeaderWeightRootHex(const std::vector<int64_t>& leader_weights) {
  std::ostringstream out;
  out << "td_hotstuff_leader_weight_root_v1|" << leader_weights.size();
  for (size_t i = 0; i < leader_weights.size(); ++i) {
    out << '|' << (i + 1) << ':' << leader_weights[i];
  }
  return HashHexForTesting(out.str());
}

std::string LeaderRandomnessRefHex(const std::string& old_weight_root,
                                   uint64_t old_weight_version,
                                   int activation_view,
                                   const std::string& leader_weight_root) {
  std::ostringstream out;
  out << "td_hotstuff_leader_randomness_ref_v1|" << old_weight_root << '|'
      << old_weight_version << '|' << activation_view << '|'
      << leader_weight_root;
  return HashHexForTesting(out.str());
}

}  // namespace td_hotstuff
}  // namespace resdb
