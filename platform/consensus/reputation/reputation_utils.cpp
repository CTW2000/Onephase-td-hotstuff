#include "platform/consensus/reputation/reputation_utils.h"

#include <algorithm>

#include "common/crypto/hash.h"

namespace resdb {
namespace consensus {
namespace reputation {
namespace {

constexpr int64_t kMinWeight = 1;
constexpr int64_t kMaxWeight = 100;

}  // namespace

std::string HexEncodeBytes(const std::string& data) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(data.size() * 2);
  for (unsigned char ch : data) {
    hex.push_back(kHex[ch >> 4]);
    hex.push_back(kHex[ch & 0x0f]);
  }
  return hex;
}

std::string HashHex(const std::string& data) {
  return HexEncodeBytes(utils::CalculateSHA256Hash(data));
}

int64_t ClampWeight(int64_t weight, const ReputationConfig& config) {
  return std::max<int64_t>(config.min_weight,
                           std::min<int64_t>(config.max_weight, weight));
}

int64_t ClampWeight(int64_t weight) {
  return std::max<int64_t>(kMinWeight, std::min<int64_t>(kMaxWeight, weight));
}

std::vector<int64_t> NormalizeWeights(const std::vector<int64_t>& weights,
                                      int total_replicas) {
  std::vector<int64_t> normalized(std::max(total_replicas, 0), kMinWeight);
  for (int i = 0; i < total_replicas && i < static_cast<int>(weights.size());
       ++i) {
    normalized[i] = ClampWeight(weights[i]);
  }
  return normalized;
}

int RoundedDivide(uint64_t numerator, uint64_t denominator) {
  if (denominator == 0) {
    return 0;
  }
  return static_cast<int>((numerator + denominator / 2) / denominator);
}
std::vector<bool> SignerMask(const std::vector<int>& signers,
                             int total_replicas) {
  std::vector<bool> mask(std::max(total_replicas, 0), false);
  for (int signer : signers) {
    if (signer >= 1 && signer <= total_replicas) {
      mask[signer - 1] = true;
    }
  }
  return mask;
}

int WeightedEffectiveDiversityScore(const std::vector<int>& signers,
                                    const std::vector<int64_t>& weights,
                                    int total_replicas) {
  if (signers.empty() || total_replicas <= 0) {
    return 0;
  }
  int64_t sum_weight = 0;
  int64_t square_sum = 0;
  for (int signer : signers) {
    if (signer < 1 || signer > total_replicas ||
        signer > static_cast<int>(weights.size())) {
      continue;
    }
    const int64_t weight = std::max<int64_t>(1, weights[signer - 1]);
    sum_weight += weight;
    square_sum += weight * weight;
  }
  if (sum_weight <= 0 || square_sum <= 0) {
    return 0;
  }
  const int target_effective_signers =
      std::max(1, std::min(total_replicas, (total_replicas * 2) / 3 + 1));
  const uint64_t numerator = static_cast<uint64_t>(sum_weight * sum_weight) * 100;
  const uint64_t denominator = static_cast<uint64_t>(square_sum) *
                               static_cast<uint64_t>(target_effective_signers);
  return std::max(0, std::min(100, RoundedDivide(numerator, denominator)));
}

int SignerFrequencyBalanceScore(const std::vector<uint64_t>& counts,
                                const std::set<int>& eligible_signers) {
  if (counts.empty() || eligible_signers.empty()) {
    return 100;
  }
  uint64_t total_selected = 0;
  uint64_t min_selected = UINT64_MAX;
  for (int signer : eligible_signers) {
    if (signer < 1 || signer > static_cast<int>(counts.size())) {
      continue;
    }
    const uint64_t selected = counts[signer - 1];
    total_selected += selected;
    min_selected = std::min(min_selected, selected);
  }
  if (total_selected == 0 || min_selected == UINT64_MAX) {
    return 100;
  }
  const int average_selected =
      RoundedDivide(total_selected, static_cast<uint64_t>(eligible_signers.size()));
  if (average_selected <= 0) {
    return 100;
  }
  return std::max(
      0, std::min(100,
                  RoundedDivide(min_selected * 100,
                                static_cast<uint64_t>(average_selected))));
}

int WeightedSignerVariationScore(const std::vector<int>& previous_signers,
                                 const std::vector<int>& current_signers,
                                 const std::vector<int64_t>& weights,
                                 int total_replicas) {
  if (previous_signers.empty() || current_signers.empty() ||
      total_replicas <= 0) {
    return 100;
  }
  const std::vector<bool> previous = SignerMask(previous_signers, total_replicas);
  const std::vector<bool> current = SignerMask(current_signers, total_replicas);
  int64_t intersection_weight = 0;
  int64_t union_weight = 0;
  for (int i = 0; i < total_replicas; ++i) {
    const bool in_previous = previous[i];
    const bool in_current = current[i];
    if (!in_previous && !in_current) {
      continue;
    }
    const int64_t weight = i < static_cast<int>(weights.size())
                               ? std::max<int64_t>(1, weights[i])
                               : 1;
    union_weight += weight;
    if (in_previous && in_current) {
      intersection_weight += weight;
    }
  }
  if (union_weight <= 0) {
    return 100;
  }
  const int repeated_score = RoundedDivide(
      static_cast<uint64_t>(intersection_weight) * 100,
      static_cast<uint64_t>(union_weight));
  return std::max(0, std::min(100, 100 - repeated_score));
}

int ReviewerCredibilityScore(const std::vector<int>& signers,
                             const std::vector<int64_t>& weights,
                             int total_replicas) {
  if (signers.empty() || weights.empty() || total_replicas <= 0) {
    return 100;
  }
  int64_t max_weight = 0;
  for (int i = 0; i < total_replicas && i < static_cast<int>(weights.size());
       ++i) {
    max_weight = std::max<int64_t>(max_weight, weights[i]);
  }
  if (max_weight <= 0) {
    return 100;
  }
  uint64_t credibility_sum = 0;
  uint64_t credibility_count = 0;
  for (int signer : signers) {
    if (signer < 1 || signer > total_replicas ||
        signer > static_cast<int>(weights.size())) {
      continue;
    }
    credibility_sum += RoundedDivide(
        static_cast<uint64_t>(std::max<int64_t>(1, weights[signer - 1])) *
            100,
        static_cast<uint64_t>(max_weight));
    ++credibility_count;
  }
  if (credibility_count == 0) {
    return 100;
  }
  return std::max(0, std::min(100,
                              RoundedDivide(credibility_sum,
                                            credibility_count)));
}


int WeightedJaccardPercent(const std::set<int>& lhs, const std::set<int>& rhs,
                           const std::vector<int64_t>& weights,
                           int total_replicas) {
  if (lhs.empty() || rhs.empty() || total_replicas <= 0) {
    return 0;
  }
  int64_t intersection_weight = 0;
  int64_t union_weight = 0;
  for (int validator_id = 1; validator_id <= total_replicas; ++validator_id) {
    const bool in_lhs = lhs.find(validator_id) != lhs.end();
    const bool in_rhs = rhs.find(validator_id) != rhs.end();
    if (!in_lhs && !in_rhs) {
      continue;
    }
    const int64_t weight =
        validator_id <= static_cast<int>(weights.size())
            ? std::max<int64_t>(1, weights[validator_id - 1])
            : 1;
    union_weight += weight;
    if (in_lhs && in_rhs) {
      intersection_weight += weight;
    }
  }
  if (union_weight <= 0) {
    return 0;
  }
  return std::max(0, std::min(100,
                              RoundedDivide(
                                  static_cast<uint64_t>(intersection_weight) *
                                      100,
                                  static_cast<uint64_t>(union_weight))));
}

std::vector<int> SetToOrderedVector(const std::set<int>& values) {
  return std::vector<int>(values.begin(), values.end());
}

int ClampScore(int score) { return std::max(0, std::min(100, score)); }
std::vector<int> DecodeSignerBitmap(const std::string& signer_bitmap,
                                    int total_replicas) {
  std::vector<int> signers;
  for (int validator_id = 1; validator_id <= total_replicas; ++validator_id) {
    const int bit = validator_id - 1;
    const size_t byte_index = static_cast<size_t>(bit / 8);
    if (byte_index >= signer_bitmap.size()) {
      continue;
    }
    const unsigned char byte =
        static_cast<unsigned char>(signer_bitmap[byte_index]);
    if ((byte & (1 << (bit % 8))) != 0) {
      signers.push_back(validator_id);
    }
  }
  return signers;
}

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
