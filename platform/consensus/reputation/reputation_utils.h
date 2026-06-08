#pragma once

#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "platform/consensus/reputation/reputation_types.h"

namespace resdb {
namespace consensus {
namespace reputation {

std::string HexEncodeBytes(const std::string& data);
std::string HashHex(const std::string& data);
int64_t ClampWeight(int64_t weight, const ReputationConfig& config);
int64_t ClampWeight(int64_t weight);
std::vector<int64_t> NormalizeWeights(const std::vector<int64_t>& weights,
                                      int total_replicas);
int RoundedDivide(uint64_t numerator, uint64_t denominator);
std::vector<int> DecodeSignerBitmap(const std::string& signer_bitmap,
                                    int total_replicas);
std::vector<bool> SignerMask(const std::vector<int>& signers,
                             int total_replicas);
int WeightedEffectiveDiversityScore(const std::vector<int>& signers,
                                    const std::vector<int64_t>& weights,
                                    int total_replicas);
int SignerFrequencyBalanceScore(const std::vector<uint64_t>& counts,
                                const std::set<int>& eligible_signers);
int WeightedSignerVariationScore(const std::vector<int>& previous_signers,
                                 const std::vector<int>& current_signers,
                                 const std::vector<int64_t>& weights,
                                 int total_replicas);
int ReviewerCredibilityScore(const std::vector<int>& signers,
                             const std::vector<int64_t>& weights,
                             int total_replicas);
int WeightedJaccardPercent(const std::set<int>& lhs, const std::set<int>& rhs,
                           const std::vector<int64_t>& weights,
                           int total_replicas);
std::vector<int> SetToOrderedVector(const std::set<int>& values);
int ClampScore(int score);

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
