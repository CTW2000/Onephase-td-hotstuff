#include "platform/consensus/reputation/reputation_algorithm.h"

#include <algorithm>
#include <map>
#include <numeric>
#include <set>
#include <sstream>
#include <tuple>

#include "common/crypto/hash.h"

namespace resdb {
namespace consensus {
namespace reputation {
namespace {

constexpr const char* kAlgorithmBayesV4 = "bayes_v4";
constexpr int64_t kMinWeight = 1;
constexpr int64_t kMaxWeight = 100;

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

int AutoSybilGraphIterations(int total_replicas,
                             const ReputationConfig& config) {
  if (config.sybil_graph_iterations > 0) {
    return config.sybil_graph_iterations;
  }
  int iterations = 1;
  int size = std::max(1, total_replicas);
  while (size > 1) {
    size = (size + 1) / 2;
    ++iterations;
  }
  return std::max(1, iterations);
}

void AddBoundedUndirectedGraphEdge(
    std::vector<std::map<int, uint64_t>>* graph, int lhs, int rhs) {
  if (graph == nullptr || lhs == rhs || lhs < 0 || rhs < 0 ||
      lhs >= static_cast<int>(graph->size()) ||
      rhs >= static_cast<int>(graph->size())) {
    return;
  }
  constexpr uint64_t kMaxEvidenceWeightPerPair = 16;
  uint64_t& lhs_weight = (*graph)[lhs][rhs];
  uint64_t& rhs_weight = (*graph)[rhs][lhs];
  lhs_weight = std::min<uint64_t>(kMaxEvidenceWeightPerPair, lhs_weight + 1);
  rhs_weight = std::min<uint64_t>(kMaxEvidenceWeightPerPair, rhs_weight + 1);
}

uint64_t GraphDegree(const std::map<int, uint64_t>& edges) {
  uint64_t degree = 0;
  for (const auto& edge : edges) {
    degree += edge.second;
  }
  return degree;
}

struct SybilGraphAudit {
  std::vector<int> rank_score;
  std::vector<int> cut_score;
  std::vector<int> graph_score;
  std::vector<uint64_t> graph_degree;
  std::vector<int> seed_trust_score;
  uint64_t edge_evidence_count = 0;
};

SybilGraphAudit ComputeSybilGraphAudit(
    int total_replicas, const std::vector<MetricEvidence>& ordered_events,
    const std::vector<int64_t>& weights,
    const std::vector<std::set<int>>& unique_signers_by_leader,
    const std::vector<uint64_t>& diversity_count,
    const std::vector<std::set<int>>& unique_available_signers_by_leader,
    const std::vector<uint64_t>& available_signer_evidence_count,
    const ReputationConfig& config) {
  SybilGraphAudit audit;
  const int n = std::max(total_replicas, 0);
  audit.rank_score.assign(n, 100);
  audit.cut_score.assign(n, 100);
  audit.graph_score.assign(n, 100);
  audit.graph_degree.assign(n, 0);
  audit.seed_trust_score.assign(n, 100);
  if (!config.sybil_graph_enabled || n == 0) {
    return audit;
  }

  std::vector<std::map<int, uint64_t>> graph(n);
  for (const MetricEvidence& event : ordered_events) {
    if (event.outcome_class != OutcomeClass::kCertified ||
        event.signer_bitmap.empty() || event.leader_id < 1 ||
        event.leader_id > n) {
      continue;
    }
    const int leader_idx = event.leader_id - 1;
    const std::vector<int> signers =
        DecodeSignerBitmap(event.signer_bitmap, total_replicas);
    for (int signer : signers) {
      if (signer < 1 || signer > n || signer == event.leader_id) {
        continue;
      }
      AddBoundedUndirectedGraphEdge(&graph, leader_idx, signer - 1);
      ++audit.edge_evidence_count;
    }
  }
  if (audit.edge_evidence_count < config.sybil_graph_min_edges) {
    return audit;
  }

  int64_t max_weight = 1;
  for (int64_t weight : weights) {
    max_weight = std::max<int64_t>(max_weight, weight);
  }
  uint64_t total_degree = 0;
  for (int i = 0; i < n; ++i) {
    audit.graph_degree[i] = GraphDegree(graph[i]);
    total_degree += audit.graph_degree[i];
    const int64_t weight = i < static_cast<int>(weights.size())
                               ? std::max<int64_t>(1, weights[i])
                               : 1;
    audit.seed_trust_score[i] = ClampScore(
        RoundedDivide(static_cast<uint64_t>(weight) * 100,
                      static_cast<uint64_t>(max_weight)));
  }
  if (total_degree == 0) {
    return audit;
  }

  constexpr uint64_t kTrustScale = 1000000;
  std::vector<uint64_t> trust(n, kTrustScale);
  for (int i = 0; i < n; ++i) {
    trust[i] = std::max<uint64_t>(
        1, static_cast<uint64_t>(audit.seed_trust_score[i]) * kTrustScale);
  }
  const int iterations = AutoSybilGraphIterations(total_replicas, config);
  for (int iter = 0; iter < iterations; ++iter) {
    std::vector<uint64_t> next(n, 0);
    for (int i = 0; i < n; ++i) {
      const uint64_t degree = audit.graph_degree[i];
      if (degree == 0) {
        next[i] += trust[i];
        continue;
      }
      for (const auto& edge : graph[i]) {
        next[edge.first] += (trust[i] * edge.second) / degree;
      }
    }
    trust.swap(next);
  }

  std::vector<uint64_t> rank_values;
  rank_values.reserve(n);
  for (int i = 0; i < n; ++i) {
    const uint64_t degree = std::max<uint64_t>(1, audit.graph_degree[i]);
    rank_values.push_back(trust[i] / degree);
  }
  std::vector<uint64_t> sorted_rank_values = rank_values;
  std::sort(sorted_rank_values.begin(), sorted_rank_values.end());
  const uint64_t median_rank =
      std::max<uint64_t>(1, sorted_rank_values[sorted_rank_values.size() / 2]);

  for (int i = 0; i < n; ++i) {
    audit.rank_score[i] = ClampScore(RoundedDivide(rank_values[i] * 100,
                                                   median_rank));
    int leader_reviewer_coverage_score = 100;
    int max_leader_reviewer_overlap = 0;
    const bool has_available_signer_evidence =
        i < static_cast<int>(available_signer_evidence_count.size()) &&
        available_signer_evidence_count[i] > 0;
    const int available_reviewer_coverage_score =
        has_available_signer_evidence &&
                i < static_cast<int>(unique_available_signers_by_leader.size()) &&
                n > 0
            ? ClampScore(RoundedDivide(
                  unique_available_signers_by_leader[i].size() * 100,
                  static_cast<uint64_t>(n)))
            : 100;
    if (i < static_cast<int>(unique_signers_by_leader.size()) &&
        i < static_cast<int>(diversity_count.size()) &&
        diversity_count[i] > 0 && !unique_signers_by_leader[i].empty()) {
      leader_reviewer_coverage_score =
          ClampScore(RoundedDivide(unique_signers_by_leader[i].size() * 100,
                                   static_cast<uint64_t>(std::max(1, n))));
      for (int other = 0;
           other < static_cast<int>(unique_signers_by_leader.size()); ++other) {
        if (other == i || other >= static_cast<int>(diversity_count.size()) ||
            diversity_count[other] == 0) {
          continue;
        }
        max_leader_reviewer_overlap = std::max(
            max_leader_reviewer_overlap,
            WeightedJaccardPercent(unique_signers_by_leader[i],
                                   unique_signers_by_leader[other], weights,
                                   total_replicas));
      }
    }
    int cut_score = 100;
    const bool narrow_public_reviewer_target =
        has_available_signer_evidence &&
        available_reviewer_coverage_score < 95;
    const bool narrow_unknown_target =
        !has_available_signer_evidence && leader_reviewer_coverage_score < 95;
    if (audit.graph_degree[i] > 0 &&
        (narrow_public_reviewer_target || narrow_unknown_target) &&
        max_leader_reviewer_overlap >= 67) {
      const int cluster_score = std::min(
          std::min(leader_reviewer_coverage_score,
                   available_reviewer_coverage_score),
          std::max(0, 100 - max_leader_reviewer_overlap));
      cut_score = std::max(
          0, std::min(100, 100 - std::min(config.sybil_graph_max_discount,
                                          100 - cluster_score)));
    }
    audit.cut_score[i] = cut_score;
    // SybilRank is useful audit context, but in V1 it is too assumption-heavy
    // to reduce weights alone. Only a concrete evidence-derived cut/cluster
    // signal can make the advisory graph score non-neutral.
    audit.graph_score[i] = audit.cut_score[i] < 100
                               ? ClampScore(std::min(audit.rank_score[i],
                                                     audit.cut_score[i]))
                               : 100;
  }
  return audit;
}

std::string WeightRootHex(const std::vector<int64_t>& weights) {
  std::ostringstream out;
  out << "protocol_neutral_weight_root_v1";
  for (size_t i = 0; i < weights.size(); ++i) {
    out << '|' << (i + 1) << ':' << weights[i];
  }
  return HashHex(out.str());
}

std::string MetricCanonical(const ReputationCandidate& candidate) {
  std::ostringstream out;
  out << "protocol_neutral_reputation_metric_v4|" << candidate.algorithm << '|'
      << candidate.total_replicas << '|' << candidate.window_index << '|'
      << candidate.start_view << '|' << candidate.end_view << '|'
      << candidate.event_count;
  for (const auto& validator : candidate.validators) {
    out << '|' << validator.validator_id << ':' << validator.opportunities << ':'
        << validator.inclusions << ':' << validator.vote_score << ':'
        << validator.leader_certified_count << ':'
        << validator.leader_opportunity_count << ':'
        << validator.leader_score << ':' << validator.leader_diversity_score
        << ':' << validator.peertrust_score << ':'
        << validator.reviewer_credibility_score << ':'
        << validator.transaction_context_score << ':'
        << validator.community_context_score << ':'
        << validator.reviewer_entropy_score << ':'
        << validator.cross_leader_independence_score << ':'
        << validator.reviewer_overuse_score << ':'
        << validator.peertrust_leader_debt << ':'
        << validator.peertrust_debt_delta << ':' << validator.feedback_count;
    out << ':' << validator.sybil_rank_score << ':' << validator.sybil_cut_score
        << ':' << validator.sybil_graph_score << ':'
        << validator.sybil_graph_debt << ':'
        << validator.sybil_graph_debt_delta << ':' << validator.graph_degree
        << ':' << validator.seed_trust_score;
  }
  return out.str();
}

std::string ReputationCanonical(const ReputationCandidate& candidate) {
  std::ostringstream out;
  out << "protocol_neutral_reputation_state_v4|" << candidate.algorithm << '|'
      << candidate.total_replicas << '|' << candidate.window_index;
  for (const auto& validator : candidate.validators) {
    out << '|' << validator.validator_id << ':' << validator.reputation_score
        << ':' << validator.decay_applied << ':' << validator.recovery_credit
        << ':' << validator.bonus_credit << ':' << validator.current_weight
        << ':' << validator.next_weight << ':' << validator.strong_fault_count
        << ':' << validator.penalty_points << ':' << validator.peertrust_score
        << ':' << validator.reviewer_credibility_score << ':'
        << validator.transaction_context_score << ':'
        << validator.community_context_score << ':'
        << validator.reviewer_entropy_score << ':'
        << validator.cross_leader_independence_score << ':'
        << validator.reviewer_overuse_score << ':' << validator.feedback_count;
    out << ':' << validator.sybil_rank_score << ':' << validator.sybil_cut_score
        << ':' << validator.sybil_graph_score << ':'
        << validator.sybil_graph_debt << ':'
        << validator.sybil_graph_debt_delta << ':' << validator.graph_degree
        << ':' << validator.seed_trust_score;
  }
  return out.str();
}

std::string StrongFaultCanonical(const ReputationCandidate& candidate) {
  std::ostringstream out;
  out << "protocol_neutral_strong_fault_v1|" << candidate.algorithm << '|'
      << candidate.total_replicas << '|' << candidate.window_index;
  for (const StrongFaultRecord& fault : candidate.strong_faults) {
    out << '|' << static_cast<int>(fault.type) << ':' << fault.validator_id
        << ':' << fault.view_or_round << ':' << fault.slot_or_height << ':'
        << fault.first_artifact_digest << ':' << fault.second_artifact_digest;
  }
  return out.str();
}

std::string PenaltyCanonical(const ReputationCandidate& candidate) {
  std::ostringstream out;
  out << "protocol_neutral_penalty_v1|" << candidate.algorithm << '|'
      << candidate.total_replicas << '|' << candidate.window_index;
  for (const ValidatorReputation& validator : candidate.validators) {
    out << '|' << validator.validator_id << ':' << validator.strong_fault_count
        << ':' << validator.penalty_points << ':' << validator.next_weight;
  }
  return out.str();
}

std::string CandidateCanonicalFromParts(
    int total_replicas, int start_view, int end_view,
    const std::string& old_weight_root_hex, uint64_t old_weight_version,
    int activation_view, const std::string& metric_root_hex,
    const std::string& reputation_root_hex,
    const std::string& next_weight_root_hex,
    const std::vector<int64_t>& next_weights,
    const std::string& strong_fault_root_hex,
    const std::string& penalty_root_hex) {
  (void)metric_root_hex;
  (void)reputation_root_hex;
  std::ostringstream out;
  out << "protocol_neutral_reputation_decision_v4|" << total_replicas << '|'
      << start_view << '|' << end_view << '|' << old_weight_root_hex << '|'
      << old_weight_version << '|' << activation_view << '|'
      << next_weight_root_hex << '|' << strong_fault_root_hex << '|'
      << penalty_root_hex;
  for (size_t i = 0; i < next_weights.size(); ++i) {
    out << '|' << (i + 1) << ':' << next_weights[i];
  }
  return out.str();
}

std::string StrongFaultTypeName(StrongFaultType type) {
  switch (type) {
    case StrongFaultType::kDoubleProposal:
      return "double_proposal";
    case StrongFaultType::kDoubleVote:
      return "double_vote";
    case StrongFaultType::kInvalidQcProposal:
      return "invalid_qc_proposal";
    case StrongFaultType::kWeightUpdateVoteEquivocation:
      return "weight_update_vote_equivocation";
    case StrongFaultType::kTimeoutVoteEquivocation:
      return "timeout_vote_equivocation";
    case StrongFaultType::kInvalidTcProposal:
      return "invalid_tc_proposal";
    case StrongFaultType::kConflictingQc:
      return "conflicting_qc";
    case StrongFaultType::kUnknown:
      return "unknown";
  }
  return "unknown";
}

std::vector<StrongFaultRecord> SummarizeFaultedValidators(
    const std::vector<StrongFaultRecord>& faults) {
  std::map<std::pair<int, int>, StrongFaultRecord> summary;
  for (const StrongFaultRecord& fault : faults) {
    if (fault.type == StrongFaultType::kUnknown || fault.validator_id <= 0) {
      continue;
    }
    const std::pair<int, int> key{static_cast<int>(fault.type),
                                  fault.validator_id};
    if (summary.find(key) != summary.end()) {
      continue;
    }
    StrongFaultRecord summarized;
    summarized.type = fault.type;
    summarized.validator_id = fault.validator_id;
    summarized.view_or_round = 0;
    summarized.slot_or_height = 0;
    summarized.first_artifact_digest = StrongFaultTypeName(fault.type);
    summarized.second_artifact_digest =
        "validator-" + std::to_string(fault.validator_id);
    summary.emplace(key, std::move(summarized));
  }
  std::vector<StrongFaultRecord> output;
  output.reserve(summary.size());
  for (auto& item : summary) {
    output.push_back(std::move(item.second));
  }
  return output;
}

}  // namespace

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

std::vector<StrongFaultRecord> DetectDoubleProposalFaults(
    const std::vector<SignedProposalEvidence>& signed_proposal_evidence) {
  std::vector<SignedProposalEvidence> ordered = signed_proposal_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const SignedProposalEvidence& lhs,
               const SignedProposalEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.weight_version,
                              lhs.leader_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.proposal_hash) <
                     std::tie(rhs.protocol_id, rhs.weight_version,
                              rhs.leader_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.proposal_hash);
            });

  using ArtifactKey = std::tuple<std::string, uint64_t, int, int, int>;
  std::map<ArtifactKey, SignedProposalEvidence> first_artifact_by_key;
  std::set<ArtifactKey> emitted_keys;
  std::vector<StrongFaultRecord> faults;
  for (const SignedProposalEvidence& artifact : ordered) {
    if (!artifact.signature_verified || artifact.protocol_id.empty() ||
        artifact.leader_id <= 0 || artifact.view_or_round <= 0 ||
        artifact.proposal_hash.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.weight_version,
                          artifact.leader_id, artifact.view_or_round,
                          artifact.slot_or_height};
    auto inserted = first_artifact_by_key.emplace(key, artifact);
    const SignedProposalEvidence& first = inserted.first->second;
    if (inserted.second || first.proposal_hash == artifact.proposal_hash) {
      continue;
    }
    if (!emitted_keys.insert(key).second) {
      continue;
    }
    StrongFaultRecord fault;
    fault.type = StrongFaultType::kDoubleProposal;
    fault.validator_id = artifact.leader_id;
    fault.view_or_round = artifact.view_or_round;
    fault.slot_or_height = artifact.slot_or_height;
    fault.first_artifact_digest =
        std::min(first.proposal_hash, artifact.proposal_hash);
    fault.second_artifact_digest =
        std::max(first.proposal_hash, artifact.proposal_hash);
    faults.push_back(std::move(fault));
  }
  return faults;
}

std::vector<StrongFaultRecord> DetectDoubleVoteFaults(
    const std::vector<SignedVoteEvidence>& signed_vote_evidence) {
  std::vector<SignedVoteEvidence> ordered = signed_vote_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const SignedVoteEvidence& lhs,
               const SignedVoteEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.weight_version,
                              lhs.signer_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.proposal_hash) <
                     std::tie(rhs.protocol_id, rhs.weight_version,
                              rhs.signer_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.proposal_hash);
            });

  using ArtifactKey = std::tuple<std::string, uint64_t, int, int, int>;
  std::map<ArtifactKey, SignedVoteEvidence> first_artifact_by_key;
  std::set<ArtifactKey> emitted_keys;
  std::vector<StrongFaultRecord> faults;
  for (const SignedVoteEvidence& artifact : ordered) {
    if (!artifact.signature_verified || artifact.protocol_id.empty() ||
        artifact.signer_id <= 0 || artifact.view_or_round <= 0 ||
        artifact.proposal_hash.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.weight_version,
                          artifact.signer_id, artifact.view_or_round,
                          artifact.slot_or_height};
    auto inserted = first_artifact_by_key.emplace(key, artifact);
    const SignedVoteEvidence& first = inserted.first->second;
    if (inserted.second || first.proposal_hash == artifact.proposal_hash) {
      continue;
    }
    if (!emitted_keys.insert(key).second) {
      continue;
    }
    StrongFaultRecord fault;
    fault.type = StrongFaultType::kDoubleVote;
    fault.validator_id = artifact.signer_id;
    fault.view_or_round = artifact.view_or_round;
    fault.slot_or_height = artifact.slot_or_height;
    fault.first_artifact_digest =
        std::min(first.proposal_hash, artifact.proposal_hash);
    fault.second_artifact_digest =
        std::max(first.proposal_hash, artifact.proposal_hash);
    faults.push_back(std::move(fault));
  }
  return faults;
}

std::vector<StrongFaultRecord> DetectInvalidQcProposalFaults(
    const std::vector<InvalidQcProposalEvidence>& invalid_qc_proposal_evidence) {
  std::vector<InvalidQcProposalEvidence> ordered =
      invalid_qc_proposal_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const InvalidQcProposalEvidence& lhs,
               const InvalidQcProposalEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.weight_version,
                              lhs.leader_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.proposal_hash,
                              lhs.invalid_reason) <
                     std::tie(rhs.protocol_id, rhs.weight_version,
                              rhs.leader_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.proposal_hash,
                              rhs.invalid_reason);
            });

  using ArtifactKey =
      std::tuple<std::string, uint64_t, int, int, int, std::string>;
  std::set<ArtifactKey> emitted_keys;
  std::vector<StrongFaultRecord> faults;
  for (const InvalidQcProposalEvidence& artifact : ordered) {
    if (!artifact.proposal_signature_verified || artifact.qc_verified ||
        artifact.protocol_id.empty() || artifact.leader_id <= 0 ||
        artifact.view_or_round <= 0 || artifact.proposal_hash.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.weight_version,
                          artifact.leader_id, artifact.view_or_round,
                          artifact.slot_or_height, artifact.proposal_hash};
    if (!emitted_keys.insert(key).second) {
      continue;
    }
    StrongFaultRecord fault;
    fault.type = StrongFaultType::kInvalidQcProposal;
    fault.validator_id = artifact.leader_id;
    fault.view_or_round = artifact.view_or_round;
    fault.slot_or_height = artifact.slot_or_height;
    fault.first_artifact_digest = artifact.proposal_hash;
    fault.second_artifact_digest =
        artifact.invalid_reason.empty() ? "invalid_qc" : artifact.invalid_reason;
    faults.push_back(std::move(fault));
  }
  return faults;
}

std::vector<StrongFaultRecord> DetectWeightUpdateVoteEquivocationFaults(
    const std::vector<SignedWeightUpdateVoteEvidence>&
        signed_weight_update_vote_evidence) {
  std::vector<SignedWeightUpdateVoteEvidence> ordered =
      signed_weight_update_vote_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const SignedWeightUpdateVoteEvidence& lhs,
               const SignedWeightUpdateVoteEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.old_weight_root,
                              lhs.old_weight_version, lhs.activation_view,
                              lhs.validator_id, lhs.candidate_digest) <
                     std::tie(rhs.protocol_id, rhs.old_weight_root,
                              rhs.old_weight_version, rhs.activation_view,
                              rhs.validator_id, rhs.candidate_digest);
            });

  using ArtifactKey = std::tuple<std::string, std::string, uint64_t, int, int>;
  std::map<ArtifactKey, SignedWeightUpdateVoteEvidence> first_artifact_by_key;
  std::set<ArtifactKey> emitted_keys;
  std::vector<StrongFaultRecord> faults;
  for (const SignedWeightUpdateVoteEvidence& artifact : ordered) {
    if (!artifact.signature_verified || artifact.protocol_id.empty() ||
        artifact.validator_id <= 0 || artifact.old_weight_root.empty() ||
        artifact.activation_view <= 0 || artifact.candidate_digest.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.old_weight_root,
                          artifact.old_weight_version, artifact.activation_view,
                          artifact.validator_id};
    auto inserted = first_artifact_by_key.emplace(key, artifact);
    const SignedWeightUpdateVoteEvidence& first = inserted.first->second;
    if (inserted.second ||
        first.candidate_digest == artifact.candidate_digest) {
      continue;
    }
    if (!emitted_keys.insert(key).second) {
      continue;
    }
    StrongFaultRecord fault;
    fault.type = StrongFaultType::kWeightUpdateVoteEquivocation;
    fault.validator_id = artifact.validator_id;
    fault.view_or_round = artifact.activation_view;
    fault.slot_or_height = 0;
    fault.first_artifact_digest =
        std::min(first.candidate_digest, artifact.candidate_digest);
    fault.second_artifact_digest =
        std::max(first.candidate_digest, artifact.candidate_digest);
    faults.push_back(std::move(fault));
  }
  return faults;
}

std::vector<StrongFaultRecord> DetectTimeoutVoteEquivocationFaults(
    const std::vector<SignedTimeoutVoteEvidence>& signed_timeout_vote_evidence) {
  std::vector<SignedTimeoutVoteEvidence> ordered = signed_timeout_vote_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const SignedTimeoutVoteEvidence& lhs,
               const SignedTimeoutVoteEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.weight_version,
                              lhs.signer_id, lhs.view_or_round,
                              lhs.high_qc_digest) <
                     std::tie(rhs.protocol_id, rhs.weight_version,
                              rhs.signer_id, rhs.view_or_round,
                              rhs.high_qc_digest);
            });

  using ArtifactKey = std::tuple<std::string, uint64_t, int, int>;
  std::map<ArtifactKey, SignedTimeoutVoteEvidence> first_artifact_by_key;
  std::set<ArtifactKey> emitted_keys;
  std::vector<StrongFaultRecord> faults;
  for (const SignedTimeoutVoteEvidence& artifact : ordered) {
    if (!artifact.signature_verified || artifact.protocol_id.empty() ||
        artifact.signer_id <= 0 || artifact.view_or_round <= 0 ||
        artifact.high_qc_digest.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.weight_version,
                          artifact.signer_id, artifact.view_or_round};
    auto inserted = first_artifact_by_key.emplace(key, artifact);
    const SignedTimeoutVoteEvidence& first = inserted.first->second;
    if (inserted.second || first.high_qc_digest == artifact.high_qc_digest) {
      continue;
    }
    if (!emitted_keys.insert(key).second) {
      continue;
    }
    StrongFaultRecord fault;
    fault.type = StrongFaultType::kTimeoutVoteEquivocation;
    fault.validator_id = artifact.signer_id;
    fault.view_or_round = artifact.view_or_round;
    fault.slot_or_height = 0;
    fault.first_artifact_digest =
        std::min(first.high_qc_digest, artifact.high_qc_digest);
    fault.second_artifact_digest =
        std::max(first.high_qc_digest, artifact.high_qc_digest);
    faults.push_back(std::move(fault));
  }
  return faults;
}

std::vector<StrongFaultRecord> DetectInvalidTcProposalFaults(
    const std::vector<InvalidTcProposalEvidence>& invalid_tc_proposal_evidence) {
  std::vector<InvalidTcProposalEvidence> ordered = invalid_tc_proposal_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const InvalidTcProposalEvidence& lhs,
               const InvalidTcProposalEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.weight_version,
                              lhs.leader_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.proposal_hash,
                              lhs.invalid_reason) <
                     std::tie(rhs.protocol_id, rhs.weight_version,
                              rhs.leader_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.proposal_hash,
                              rhs.invalid_reason);
            });

  using ArtifactKey =
      std::tuple<std::string, uint64_t, int, int, int, std::string>;
  std::set<ArtifactKey> emitted_keys;
  std::vector<StrongFaultRecord> faults;
  for (const InvalidTcProposalEvidence& artifact : ordered) {
    if (!artifact.proposal_signature_verified ||
        artifact.timeout_cert_verified || artifact.protocol_id.empty() ||
        artifact.leader_id <= 0 || artifact.view_or_round <= 0 ||
        artifact.proposal_hash.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.weight_version,
                          artifact.leader_id, artifact.view_or_round,
                          artifact.slot_or_height, artifact.proposal_hash};
    if (!emitted_keys.insert(key).second) {
      continue;
    }
    StrongFaultRecord fault;
    fault.type = StrongFaultType::kInvalidTcProposal;
    fault.validator_id = artifact.leader_id;
    fault.view_or_round = artifact.view_or_round;
    fault.slot_or_height = artifact.slot_or_height;
    fault.first_artifact_digest = artifact.proposal_hash;
    fault.second_artifact_digest =
        artifact.invalid_reason.empty() ? "invalid_tc" : artifact.invalid_reason;
    faults.push_back(std::move(fault));
  }
  return faults;
}

std::vector<StrongFaultRecord> DetectConflictingQcFaults(
    const std::vector<VerifiedQcArtifactEvidence>& verified_qc_artifact_evidence,
    int total_replicas) {
  std::vector<VerifiedQcArtifactEvidence> ordered =
      verified_qc_artifact_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const VerifiedQcArtifactEvidence& lhs,
               const VerifiedQcArtifactEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.weight_version,
                              lhs.view_or_round, lhs.slot_or_height,
                              lhs.qc_hash) <
                     std::tie(rhs.protocol_id, rhs.weight_version,
                              rhs.view_or_round, rhs.slot_or_height,
                              rhs.qc_hash);
            });

  using ArtifactKey = std::tuple<std::string, uint64_t, int, int>;
  std::map<ArtifactKey, VerifiedQcArtifactEvidence> first_artifact_by_key;
  std::set<std::tuple<ArtifactKey, int>> emitted_validators;
  std::vector<StrongFaultRecord> faults;
  for (const VerifiedQcArtifactEvidence& artifact : ordered) {
    if (!artifact.qc_verified || artifact.protocol_id.empty() ||
        artifact.view_or_round <= 0 || artifact.qc_hash.empty() ||
        artifact.signer_bitmap.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.weight_version,
                          artifact.view_or_round, artifact.slot_or_height};
    auto inserted = first_artifact_by_key.emplace(key, artifact);
    const VerifiedQcArtifactEvidence& first = inserted.first->second;
    if (inserted.second || first.qc_hash == artifact.qc_hash) {
      continue;
    }

    const std::vector<int> first_signers =
        DecodeSignerBitmap(first.signer_bitmap, total_replicas);
    const std::vector<int> second_signers =
        DecodeSignerBitmap(artifact.signer_bitmap, total_replicas);
    std::set<int> first_signer_set(first_signers.begin(), first_signers.end());
    for (int signer : second_signers) {
      if (first_signer_set.find(signer) == first_signer_set.end()) {
        continue;
      }
      if (!emitted_validators.insert(std::make_tuple(key, signer)).second) {
        continue;
      }
      StrongFaultRecord fault;
      fault.type = StrongFaultType::kConflictingQc;
      fault.validator_id = signer;
      fault.view_or_round = artifact.view_or_round;
      fault.slot_or_height = artifact.slot_or_height;
      fault.first_artifact_digest = std::min(first.qc_hash, artifact.qc_hash);
      fault.second_artifact_digest = std::max(first.qc_hash, artifact.qc_hash);
      faults.push_back(std::move(fault));
    }
  }
  std::sort(faults.begin(), faults.end(),
            [](const StrongFaultRecord& lhs, const StrongFaultRecord& rhs) {
              return std::tie(lhs.validator_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.first_artifact_digest,
                              lhs.second_artifact_digest) <
                     std::tie(rhs.validator_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.first_artifact_digest,
                              rhs.second_artifact_digest);
            });
  return faults;
}

ReputationCandidate ComputeReputationCandidate(
    int node_id, int total_replicas, uint64_t window_index,
    const std::vector<MetricEvidence>& evidence,
    const std::vector<int64_t>& current_weights,
    const ReputationConfig& config,
    const std::string& old_weight_root_hex, uint64_t old_weight_version,
    int activation_view,
    const std::vector<SignedProposalEvidence>& signed_proposal_evidence,
    const std::vector<SignedVoteEvidence>& signed_vote_evidence,
    const std::vector<InvalidQcProposalEvidence>& invalid_qc_proposal_evidence,
    const std::vector<SignedWeightUpdateVoteEvidence>&
        signed_weight_update_vote_evidence,
    const std::vector<SignedTimeoutVoteEvidence>& signed_timeout_vote_evidence,
    const std::vector<InvalidTcProposalEvidence>& invalid_tc_proposal_evidence,
    const std::vector<VerifiedQcArtifactEvidence>&
        verified_qc_artifact_evidence,
    const std::vector<int>& prior_peertrust_leader_debt,
    const std::vector<int>& prior_sybil_graph_debt) {
  ReputationCandidate candidate;
  candidate.algorithm = kAlgorithmBayesV4;
  candidate.local_node_id = node_id;
  candidate.total_replicas = total_replicas;
  candidate.window_index = window_index;
  candidate.event_count = evidence.size();
  if (!evidence.empty()) {
    candidate.start_view = evidence.front().view_or_round;
    candidate.end_view = evidence.back().view_or_round;
  }

  const std::vector<int64_t> weights =
      NormalizeWeights(current_weights, total_replicas);
  candidate.old_weight_root_hex =
      old_weight_root_hex.empty() ? WeightRootHex(weights) : old_weight_root_hex;
  candidate.old_weight_version = old_weight_version;
  candidate.activation_view = activation_view;
  candidate.validators.resize(std::max(total_replicas, 0));
  for (int i = 0; i < total_replicas; ++i) {
    ValidatorReputation& validator = candidate.validators[i];
    validator.validator_id = i + 1;
    validator.current_weight = weights[i];
    validator.next_weight = weights[i];
    if (config.peertrust_enabled &&
        i < static_cast<int>(prior_peertrust_leader_debt.size())) {
      validator.peertrust_leader_debt = std::max(
          0, std::min(config.peertrust_debt_max,
                      prior_peertrust_leader_debt[i]));
    }
    if (config.sybil_graph_enabled &&
        i < static_cast<int>(prior_sybil_graph_debt.size())) {
      validator.sybil_graph_debt = std::max(
          0, std::min(config.sybil_graph_debt_max, prior_sybil_graph_debt[i]));
    }
  }

  std::vector<uint64_t> diversity_sum(std::max(total_replicas, 0), 0);
  std::vector<uint64_t> diversity_count(std::max(total_replicas, 0), 0);
  std::vector<uint64_t> signer_set_coverage_sum(
      std::max(total_replicas, 0), 0);
  std::vector<uint64_t> available_signer_set_coverage_sum(
      std::max(total_replicas, 0), 0);
  std::vector<uint64_t> available_signer_evidence_count(
      std::max(total_replicas, 0), 0);
  std::vector<std::set<int>> unique_signers_by_leader(
      std::max(total_replicas, 0));
  std::vector<std::set<int>> unique_available_signers_by_leader(
      std::max(total_replicas, 0));
  std::vector<std::vector<uint64_t>> signer_frequency_by_leader(
      std::max(total_replicas, 0),
      std::vector<uint64_t>(std::max(total_replicas, 0), 0));
  std::vector<uint64_t> variation_sum(std::max(total_replicas, 0), 0);
  std::vector<uint64_t> variation_count(std::max(total_replicas, 0), 0);
  std::vector<std::vector<int>> previous_signers_by_leader(
      std::max(total_replicas, 0));
  std::vector<uint64_t> peertrust_reviewer_credibility_sum(
      std::max(total_replicas, 0), 0);
  std::vector<uint64_t> peertrust_feedback_count(
      std::max(total_replicas, 0), 0);
  std::vector<int> peertrust_reviewer_entropy_scores(
      std::max(total_replicas, 0), 100);
  std::vector<int> peertrust_cross_leader_independence_scores(
      std::max(total_replicas, 0), 100);
  std::vector<int> peertrust_reviewer_overuse_scores(
      std::max(total_replicas, 0), 100);
  std::vector<int> peertrust_community_context_scores(
      std::max(total_replicas, 0), 100);

  std::vector<MetricEvidence> ordered_events = evidence;
  std::sort(ordered_events.begin(), ordered_events.end(),
            [](const MetricEvidence& lhs, const MetricEvidence& rhs) {
              if (lhs.view_or_round != rhs.view_or_round) {
                return lhs.view_or_round < rhs.view_or_round;
              }
              return lhs.artifact_digest < rhs.artifact_digest;
            });

  uint64_t legacy_selected_signer_slots = 0;
  uint64_t legacy_certificate_event_count = 0;
  uint64_t certificate_event_count = 0;
  std::vector<uint64_t> signer_opportunity_count(
      std::max(total_replicas, 0), 0);
  std::set<std::pair<int, std::string>> seen_certificates;
  std::set<std::pair<int, int>> seen_leader_opportunities;
  for (const MetricEvidence& event : ordered_events) {
    const int leader = event.leader_id;
    const int diversity_leader =
        event.collector_id > 0 ? event.collector_id : leader;

    if (event.outcome_class == OutcomeClass::kTimeoutOrViewChange &&
        leader >= 1 && leader <= total_replicas) {
      if (seen_leader_opportunities
              .insert({event.view_or_round, leader})
              .second) {
        ++candidate.validators[leader - 1].leader_opportunity_count;
      }
    }

    if (event.outcome_class != OutcomeClass::kCertified ||
        event.signer_bitmap.empty()) {
      continue;
    }
    const std::string certificate_key =
        event.artifact_digest.empty()
            ? "view-" + std::to_string(event.view_or_round)
            : event.artifact_digest;
    if (!seen_certificates.insert({event.view_or_round, certificate_key}).second) {
      continue;
    }

    const std::vector<int> signers =
        DecodeSignerBitmap(event.signer_bitmap, total_replicas);
    const bool has_available_signer_evidence =
        !event.available_signer_bitmap.empty();
    std::vector<int> available_signers =
        has_available_signer_evidence
            ? DecodeSignerBitmap(event.available_signer_bitmap, total_replicas)
            : signers;
    if (available_signers.empty()) {
      available_signers = signers;
    }
    ++certificate_event_count;
    if (has_available_signer_evidence) {
      for (int signer : available_signers) {
        if (signer >= 1 && signer <= total_replicas) {
          ++signer_opportunity_count[signer - 1];
        }
      }
    } else {
      legacy_selected_signer_slots += signers.size();
      ++legacy_certificate_event_count;
    }
    for (int signer : signers) {
      if (signer >= 1 && signer <= total_replicas) {
        ++candidate.validators[signer - 1].inclusions;
      }
    }
    if (leader >= 1 && leader <= total_replicas) {
      ValidatorReputation& leader_score = candidate.validators[leader - 1];
      if (seen_leader_opportunities
              .insert({event.view_or_round, leader})
              .second) {
        ++leader_score.leader_opportunity_count;
      }
      ++leader_score.leader_certified_count;
      const int leader_idx = leader - 1;
      peertrust_reviewer_credibility_sum[leader_idx] +=
          ReviewerCredibilityScore(signers, weights, total_replicas);
      ++peertrust_feedback_count[leader_idx];
    }

    if (diversity_leader < 1 || diversity_leader > total_replicas) {
      continue;
    }
    const int diversity_idx = diversity_leader - 1;
    diversity_sum[diversity_idx] +=
        WeightedEffectiveDiversityScore(signers, weights, total_replicas);
    signer_set_coverage_sum[diversity_idx] += std::min(
        100, RoundedDivide(signers.size() * 100,
                           static_cast<uint64_t>(total_replicas)));
    available_signer_set_coverage_sum[diversity_idx] += std::min(
        100, RoundedDivide(available_signers.size() * 100,
                           static_cast<uint64_t>(total_replicas)));
    if (has_available_signer_evidence) {
      ++available_signer_evidence_count[diversity_idx];
    }
    for (int signer : signers) {
      if (signer >= 1 && signer <= total_replicas) {
        unique_signers_by_leader[diversity_idx].insert(signer);
        ++signer_frequency_by_leader[diversity_idx][signer - 1];
      }
    }
    for (int signer : available_signers) {
      if (signer >= 1 && signer <= total_replicas) {
        unique_available_signers_by_leader[diversity_idx].insert(signer);
      }
    }
    ++diversity_count[diversity_idx];
    if (!previous_signers_by_leader[diversity_idx].empty()) {
      variation_sum[diversity_idx] += WeightedSignerVariationScore(
          previous_signers_by_leader[diversity_idx], signers, weights,
          total_replicas);
      ++variation_count[diversity_idx];
    }
    previous_signers_by_leader[diversity_idx] = signers;
  }

  const uint64_t fair_opportunities = FairExpectedSignerOpportunities(
      legacy_selected_signer_slots, total_replicas,
      legacy_certificate_event_count);
  for (ValidatorReputation& validator : candidate.validators) {
    const int idx = validator.validator_id - 1;
    const uint64_t direct_opportunities =
        idx >= 0 && idx < static_cast<int>(signer_opportunity_count.size())
            ? signer_opportunity_count[idx]
            : 0;
    validator.opportunities = direct_opportunities + fair_opportunities;
  }

  int64_t total_current_weight = 0;
  for (const ValidatorReputation& validator : candidate.validators) {
    total_current_weight += validator.current_weight;
  }
  const int64_t mean_current_weight =
      candidate.validators.empty()
          ? 0
          : RoundedDivide(static_cast<uint64_t>(total_current_weight),
                          candidate.validators.size());

  std::vector<int> raw_leader_diversity_scores(candidate.validators.size(), 100);
  std::vector<int> public_target_coverage_scores(candidate.validators.size(), 100);
  std::vector<int> diversity_baseline_samples;
  diversity_baseline_samples.reserve(candidate.validators.size());
  for (const ValidatorReputation& validator : candidate.validators) {
    const int idx = validator.validator_id - 1;
    const int diversity_score =
        idx >= 0 && idx < static_cast<int>(diversity_count.size()) &&
                diversity_count[idx] > 0
            ? RoundedDivide(diversity_sum[idx], diversity_count[idx])
            : 100;
    const bool has_repeated_leader_signer_group =
        idx >= 0 && idx < static_cast<int>(variation_count.size()) &&
        variation_count[idx] > 0;
    const int variation_score =
        has_repeated_leader_signer_group
            ? RoundedDivide(variation_sum[idx], variation_count[idx])
            : 100;
    const int signer_coverage_score =
        idx >= 0 && idx < static_cast<int>(unique_signers_by_leader.size()) &&
                diversity_count[idx] > 0 && total_replicas > 0
            ? RoundedDivide(unique_signers_by_leader[idx].size() * 100,
                            static_cast<uint64_t>(total_replicas))
            : 100;
    const int average_signer_set_coverage_score =
        idx >= 0 && idx < static_cast<int>(signer_set_coverage_sum.size()) &&
                diversity_count[idx] > 0
            ? RoundedDivide(signer_set_coverage_sum[idx], diversity_count[idx])
            : 100;
    const bool has_public_available_signer_evidence =
        idx >= 0 &&
        idx < static_cast<int>(available_signer_evidence_count.size()) &&
        available_signer_evidence_count[idx] > 0;
    const int available_signer_coverage_score =
        idx >= 0 &&
                idx < static_cast<int>(unique_available_signers_by_leader.size()) &&
                has_public_available_signer_evidence && total_replicas > 0
            ? RoundedDivide(unique_available_signers_by_leader[idx].size() * 100,
                            static_cast<uint64_t>(total_replicas))
            : signer_coverage_score;
    const int average_available_signer_set_coverage_score =
        idx >= 0 &&
                idx < static_cast<int>(available_signer_set_coverage_sum.size()) &&
                diversity_count[idx] > 0
            ? RoundedDivide(available_signer_set_coverage_sum[idx],
                            diversity_count[idx])
            : average_signer_set_coverage_score;
    const bool had_broader_public_choice =
        !has_public_available_signer_evidence ||
        available_signer_coverage_score > signer_coverage_score + 10 ||
        average_available_signer_set_coverage_score >
            average_signer_set_coverage_score + 10;
    const int frequency_balance_score =
        idx >= 0 && idx < static_cast<int>(signer_frequency_by_leader.size()) &&
                has_public_available_signer_evidence
            ? SignerFrequencyBalanceScore(
                  signer_frequency_by_leader[idx],
                  unique_available_signers_by_leader[idx])
            : 100;
    int raw_diversity_score = diversity_score;
    const bool repeated_narrow_choice =
        has_repeated_leader_signer_group && variation_score < 50 &&
        average_signer_set_coverage_score < 95 &&
        (!has_public_available_signer_evidence || signer_coverage_score < 95);
    const int target_coverage_score =
        has_public_available_signer_evidence
            ? std::min(available_signer_coverage_score,
                       average_available_signer_set_coverage_score)
            : 100;
    const bool narrow_public_target =
        has_public_available_signer_evidence && target_coverage_score < 95;
    const bool unbalanced_public_frequency =
        has_public_available_signer_evidence && had_broader_public_choice &&
        frequency_balance_score < 67;
    const int target_coverage_recovery_score =
        has_public_available_signer_evidence
            ? (target_coverage_score >= 95
                   ? 100
                   : std::max(
                         0, std::min(100,
                                     RoundedDivide(
                                         static_cast<uint64_t>(std::max(
                                             0, target_coverage_score - 67)) *
                                             100,
                                         28))))
            : 100;
    if (has_public_available_signer_evidence && !had_broader_public_choice &&
        !narrow_public_target) {
      raw_diversity_score = 100;
    } else if (narrow_public_target) {
      raw_diversity_score = target_coverage_recovery_score;
    } else if (repeated_narrow_choice || unbalanced_public_frequency) {
      const int public_choice_gap =
          has_public_available_signer_evidence
              ? std::max(0,
                         std::max(available_signer_coverage_score -
                                      signer_coverage_score,
                                  average_available_signer_set_coverage_score -
                                      average_signer_set_coverage_score))
              : 0;
      const int public_choice_score =
          has_public_available_signer_evidence
              ? std::max(0, 100 - public_choice_gap * 2)
              : 100;
      const int concentration_score = std::min(
          std::min(diversity_score, public_choice_score),
          std::min(std::min(signer_coverage_score,
                            average_signer_set_coverage_score),
                   frequency_balance_score));
      const int movement_score =
          repeated_narrow_choice
              ? std::min(variation_score, frequency_balance_score)
              : frequency_balance_score;
      raw_diversity_score = std::max(
          0, std::min(100,
                      RoundedDivide(concentration_score + movement_score, 2)));
    }
    raw_leader_diversity_scores[idx] = raw_diversity_score;
    public_target_coverage_scores[idx] = target_coverage_score;
    if (validator.leader_opportunity_count >= config.min_leader_opportunities &&
        diversity_count[idx] > 0) {
      diversity_baseline_samples.push_back(raw_diversity_score);
    }
  }


  std::vector<int> leaders_per_reviewer(std::max(total_replicas, 0), 0);
  for (int idx = 0; idx < static_cast<int>(unique_signers_by_leader.size());
       ++idx) {
    if (diversity_count[idx] == 0) {
      continue;
    }
    for (int signer : unique_signers_by_leader[idx]) {
      if (signer >= 1 && signer <= total_replicas) {
        ++leaders_per_reviewer[signer - 1];
      }
    }
  }
  for (int idx = 0; idx < static_cast<int>(candidate.validators.size());
       ++idx) {
    if (diversity_count[idx] == 0) {
      continue;
    }
    const int available_coverage_score =
        !unique_available_signers_by_leader[idx].empty() && total_replicas > 0
            ? RoundedDivide(unique_available_signers_by_leader[idx].size() *
                                100,
                            static_cast<uint64_t>(total_replicas))
            : 100;
    const int signer_coverage_score =
        !unique_signers_by_leader[idx].empty() && total_replicas > 0
            ? RoundedDivide(unique_signers_by_leader[idx].size() * 100,
                            static_cast<uint64_t>(total_replicas))
            : 100;
    const bool has_broad_available_reviewer_set =
        !unique_available_signers_by_leader[idx].empty() &&
        available_coverage_score >= 95;
    const int reviewer_coverage_score =
        has_broad_available_reviewer_set
            ? 100
            : std::min(available_coverage_score, signer_coverage_score);
    peertrust_reviewer_entropy_scores[idx] =
        has_broad_available_reviewer_set
            ? 100
            : WeightedEffectiveDiversityScore(
                  SetToOrderedVector(unique_signers_by_leader[idx]), weights,
                  total_replicas);

    int max_overlap_score = 0;
    for (int other = 0;
         other < static_cast<int>(unique_signers_by_leader.size()); ++other) {
      if (other == idx || diversity_count[other] == 0) {
        continue;
      }
      max_overlap_score = std::max(
          max_overlap_score,
          WeightedJaccardPercent(unique_signers_by_leader[idx],
                                 unique_signers_by_leader[other], weights,
                                 total_replicas));
    }
    peertrust_cross_leader_independence_scores[idx] =
        has_broad_available_reviewer_set || signer_coverage_score >= 95
            ? 100
            : std::max(0, 100 - max_overlap_score);

    int max_reviewer_overuse = 1;
    for (int signer : unique_signers_by_leader[idx]) {
      if (signer >= 1 && signer <= total_replicas) {
        max_reviewer_overuse =
            std::max(max_reviewer_overuse, leaders_per_reviewer[signer - 1]);
      }
    }
    peertrust_reviewer_overuse_scores[idx] =
        has_broad_available_reviewer_set || signer_coverage_score >= 95
            ? 100
            : std::max(0, std::min(100,
                                  RoundedDivide(100,
                                                static_cast<uint64_t>(
                                                    max_reviewer_overuse))));
    peertrust_community_context_scores[idx] = std::min(
        std::min(reviewer_coverage_score,
                 peertrust_reviewer_entropy_scores[idx]),
        std::min(peertrust_cross_leader_independence_scores[idx],
                 peertrust_reviewer_overuse_scores[idx]));
  }

  std::vector<int> peertrust_community_baseline_samples;
  peertrust_community_baseline_samples.reserve(candidate.validators.size());
  for (const ValidatorReputation& validator : candidate.validators) {
    const int idx = validator.validator_id - 1;
    if (idx >= 0 &&
        idx < static_cast<int>(peertrust_feedback_count.size()) &&
        peertrust_feedback_count[idx] > 0 &&
        idx < static_cast<int>(peertrust_community_context_scores.size())) {
      peertrust_community_baseline_samples.push_back(
          peertrust_community_context_scores[idx]);
    }
  }
  int peertrust_community_baseline_score = 100;
  if (!peertrust_community_baseline_samples.empty()) {
    std::sort(peertrust_community_baseline_samples.begin(),
              peertrust_community_baseline_samples.end());
    peertrust_community_baseline_score =
        peertrust_community_baseline_samples
            [peertrust_community_baseline_samples.size() / 2];
  }
  constexpr int kPeerTrustCommunityOutlierDeadband = 10;

  const SybilGraphAudit sybil_graph_audit = ComputeSybilGraphAudit(
      total_replicas, ordered_events, weights, unique_signers_by_leader,
      diversity_count, unique_available_signers_by_leader,
      available_signer_evidence_count, config);

  int leader_diversity_baseline_score = 100;
  if (!diversity_baseline_samples.empty()) {
    std::sort(diversity_baseline_samples.begin(),
              diversity_baseline_samples.end());
    leader_diversity_baseline_score =
        diversity_baseline_samples[diversity_baseline_samples.size() / 2];
  }
  constexpr int kLeaderDiversityOutlierDeadband = 10;

  for (ValidatorReputation& validator : candidate.validators) {
    validator.vote_score = VoteScore(validator.inclusions,
                                     validator.opportunities);
    const int idx = validator.validator_id - 1;
    const int raw_leader_diversity_score =
        idx >= 0 && idx < static_cast<int>(raw_leader_diversity_scores.size())
            ? raw_leader_diversity_scores[idx]
            : 100;
    const bool low_diversity_outlier =
        raw_leader_diversity_score + kLeaderDiversityOutlierDeadband <
        leader_diversity_baseline_score;
    const bool has_public_available_signer_evidence =
        idx >= 0 &&
        idx < static_cast<int>(available_signer_evidence_count.size()) &&
        available_signer_evidence_count[idx] > 0;
    const bool has_narrow_public_target =
        idx >= 0 && idx < static_cast<int>(public_target_coverage_scores.size()) &&
        public_target_coverage_scores[idx] < 95;
    if (has_public_available_signer_evidence && has_narrow_public_target) {
      validator.leader_diversity_score = raw_leader_diversity_score;
    } else if (has_public_available_signer_evidence &&
               diversity_baseline_samples.size() > 1) {
      validator.leader_diversity_score =
          low_diversity_outlier && leader_diversity_baseline_score > 0
              ? std::max(0, std::min(100,
                                     RoundedDivide(
                                         raw_leader_diversity_score * 100,
                                         leader_diversity_baseline_score)))
              : 100;
    } else {
      validator.leader_diversity_score = raw_leader_diversity_score;
    }

    const uint64_t leader_opportunities = validator.leader_opportunity_count;
    validator.leader_score = LeaderCertifiedScore(
        validator.leader_certified_count, leader_opportunities);
    if (config.peertrust_enabled && idx >= 0 &&
        idx < static_cast<int>(peertrust_feedback_count.size()) &&
        peertrust_feedback_count[idx] > 0) {
      validator.feedback_count = peertrust_feedback_count[idx];
      validator.reviewer_credibility_score = std::max(
          0, std::min(100,
                      RoundedDivide(peertrust_reviewer_credibility_sum[idx],
                                    peertrust_feedback_count[idx])));
      validator.transaction_context_score = 100;
      validator.reviewer_entropy_score =
          idx >= 0 && idx < static_cast<int>(peertrust_reviewer_entropy_scores.size())
              ? peertrust_reviewer_entropy_scores[idx]
              : 100;
      validator.cross_leader_independence_score =
          idx >= 0 &&
                  idx < static_cast<int>(
                            peertrust_cross_leader_independence_scores.size())
              ? peertrust_cross_leader_independence_scores[idx]
              : 100;
      validator.reviewer_overuse_score =
          idx >= 0 && idx < static_cast<int>(peertrust_reviewer_overuse_scores.size())
              ? peertrust_reviewer_overuse_scores[idx]
              : 100;
      const int raw_peertrust_community_score =
          idx >= 0 &&
                  idx < static_cast<int>(peertrust_community_context_scores.size())
              ? peertrust_community_context_scores[idx]
              : 100;
      const bool low_peertrust_community_outlier =
          raw_peertrust_community_score + kPeerTrustCommunityOutlierDeadband <
          peertrust_community_baseline_score;
      const int relative_peertrust_community_score =
          low_peertrust_community_outlier &&
                  peertrust_community_baseline_score > 0
              ? std::max(0, std::min(100,
                                     RoundedDivide(
                                         raw_peertrust_community_score * 100,
                                         peertrust_community_baseline_score)))
              : 100;
      validator.community_context_score = std::min(
          validator.leader_diversity_score, relative_peertrust_community_score);
      validator.peertrust_score = std::max(
          0, std::min(100,
                      RoundedDivide(
                          static_cast<uint64_t>(
                              validator.reviewer_credibility_score) *
                              static_cast<uint64_t>(
                                  validator.transaction_context_score) *
                              static_cast<uint64_t>(
                                  validator.community_context_score),
                          10000)));
    } else {
      validator.peertrust_score = 100;
      validator.reviewer_credibility_score = 100;
      validator.transaction_context_score = 100;
      validator.community_context_score = 100;
      validator.reviewer_entropy_score = 100;
      validator.cross_leader_independence_score = 100;
      validator.reviewer_overuse_score = 100;
      validator.feedback_count = 0;
    }

    const int previous_peertrust_debt = validator.peertrust_leader_debt;
    if (config.peertrust_enabled) {
      int next_peertrust_debt = previous_peertrust_debt;
      if (validator.feedback_count > 0) {
        const bool low_peertrust =
            validator.peertrust_score < config.peertrust_debt_trigger_score ||
            validator.community_context_score <
                config.peertrust_debt_trigger_score;
        const bool broad_good_peertrust =
            validator.peertrust_score >= 95 &&
            validator.community_context_score >= 95;
        if (low_peertrust) {
          next_peertrust_debt += config.peertrust_debt_increment;
        } else if (broad_good_peertrust) {
          next_peertrust_debt -= config.peertrust_debt_recovery;
        }
      }
      next_peertrust_debt = std::max(
          0, std::min(config.peertrust_debt_max, next_peertrust_debt));
      validator.peertrust_leader_debt = next_peertrust_debt;
      validator.peertrust_debt_delta =
          next_peertrust_debt - previous_peertrust_debt;
    } else {
      validator.peertrust_leader_debt = 0;
      validator.peertrust_debt_delta = 0;
    }
    if (config.sybil_graph_enabled && idx >= 0 &&
        idx < static_cast<int>(sybil_graph_audit.graph_score.size())) {
      validator.sybil_rank_score = sybil_graph_audit.rank_score[idx];
      validator.sybil_cut_score = sybil_graph_audit.cut_score[idx];
      validator.sybil_graph_score = sybil_graph_audit.graph_score[idx];
      validator.graph_degree = sybil_graph_audit.graph_degree[idx];
      validator.seed_trust_score = sybil_graph_audit.seed_trust_score[idx];
      const int previous_sybil_graph_debt = validator.sybil_graph_debt;
      int next_sybil_graph_debt = previous_sybil_graph_debt;
      if (validator.graph_degree > 0 &&
          validator.sybil_cut_score <
              config.sybil_graph_debt_trigger_score) {
        next_sybil_graph_debt += config.sybil_graph_debt_increment;
      } else if (leader_opportunities >= config.min_leader_opportunities &&
                 validator.graph_degree > 0 &&
                 validator.sybil_graph_score >= 95) {
        next_sybil_graph_debt -= config.sybil_graph_debt_recovery;
      }
      next_sybil_graph_debt = std::max(
          0, std::min(config.sybil_graph_debt_max, next_sybil_graph_debt));
      validator.sybil_graph_debt = next_sybil_graph_debt;
      validator.sybil_graph_debt_delta =
          next_sybil_graph_debt - previous_sybil_graph_debt;
    } else {
      validator.sybil_rank_score = 100;
      validator.sybil_cut_score = 100;
      validator.sybil_graph_score = 100;
      validator.sybil_graph_debt = 0;
      validator.sybil_graph_debt_delta = 0;
      validator.graph_degree = 0;
      validator.seed_trust_score = 100;
    }

    int recovery_score = validator.vote_score;
    if (validator.inclusions > 0) {
      recovery_score = std::max(recovery_score, 67);
    }
    const bool has_strong_leader_failure_signal =
        leader_opportunities > 0 && validator.leader_certified_count == 0 &&
        validator.leader_score < 50;
    if (config.leader_recovery_enabled &&
        (leader_opportunities >= config.min_leader_opportunities ||
         has_strong_leader_failure_signal)) {
      recovery_score = std::min(recovery_score, validator.leader_score);
      if (leader_opportunities >= config.min_leader_opportunities &&
          has_narrow_public_target) {
        recovery_score =
            std::min(recovery_score, validator.leader_diversity_score);
      }
    }
    if (config.peertrust_enabled &&
        (leader_opportunities >= config.min_leader_opportunities ||
         validator.peertrust_leader_debt > 0)) {
      if (leader_opportunities >= config.min_leader_opportunities) {
        recovery_score = std::min(recovery_score, validator.peertrust_score);
      }
      const int debt_gate_score =
          std::max(0, 100 - validator.peertrust_leader_debt);
      recovery_score = std::min(recovery_score, debt_gate_score);
    }
    if (config.sybil_graph_enabled &&
        (validator.graph_degree > 0 || validator.sybil_graph_debt > 0)) {
      if (validator.graph_degree > 0 &&
          validator.sybil_cut_score <
              config.sybil_graph_debt_trigger_score) {
        recovery_score = std::min(recovery_score,
                                  validator.sybil_graph_score);
      }
      const int debt_gate_score =
          std::max(0, 100 - validator.sybil_graph_debt);
      recovery_score = std::min(recovery_score, debt_gate_score);
    }

    const bool validator_has_enough_decay_evidence =
        validator.opportunities >= config.min_decay_opportunities;
    const bool near_fair_vote =
        HasNearFairInclusion(validator.inclusions, validator.opportunities);
    const int64_t available_decay =
        std::max<int64_t>(0, validator.current_weight - config.min_weight);
    validator.decay_applied =
        validator_has_enough_decay_evidence
            ? static_cast<int>(
                  std::min<int64_t>(available_decay, config.decay_per_epoch))
            : 0;
    validator.recovery_credit = std::min(
        validator.decay_applied,
        RecoveryCreditForScore(recovery_score, config.max_recovery_per_epoch));
    const bool below_mean_weight =
        validator.current_weight < mean_current_weight;
    const bool earns_bonus =
        validator_has_enough_decay_evidence && below_mean_weight &&
        ((recovery_score >= 95 && validator.vote_score >= 95) ||
         (near_fair_vote && recovery_score >= 67));
    validator.bonus_credit = earns_bonus ? config.bonus_per_epoch : 0;
    validator.reputation_score =
        validator.recovery_credit >= validator.decay_applied &&
                validator.bonus_credit > 0
            ? 100
            : std::max(0, std::min(100, recovery_score));
    validator.next_weight = ClampWeight(
        validator.current_weight - validator.decay_applied +
            validator.recovery_credit + validator.bonus_credit,
        config);
  }

  if (config.strong_fault_enabled) {
    if (config.double_proposal_detection_enabled) {
      std::vector<StrongFaultRecord> proposal_faults =
          DetectDoubleProposalFaults(signed_proposal_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     proposal_faults.begin(),
                                     proposal_faults.end());
    }
    if (config.double_vote_detection_enabled) {
      std::vector<StrongFaultRecord> vote_faults =
          DetectDoubleVoteFaults(signed_vote_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     vote_faults.begin(), vote_faults.end());
    }
    if (config.invalid_qc_proposal_detection_enabled) {
      std::vector<StrongFaultRecord> invalid_qc_faults =
          DetectInvalidQcProposalFaults(invalid_qc_proposal_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     invalid_qc_faults.begin(),
                                     invalid_qc_faults.end());
    }
    if (config.weight_update_vote_equivocation_detection_enabled) {
      std::vector<StrongFaultRecord> weight_vote_faults =
          DetectWeightUpdateVoteEquivocationFaults(
              signed_weight_update_vote_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     weight_vote_faults.begin(),
                                     weight_vote_faults.end());
    }
    if (config.timeout_vote_equivocation_detection_enabled) {
      std::vector<StrongFaultRecord> timeout_vote_faults =
          DetectTimeoutVoteEquivocationFaults(signed_timeout_vote_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     timeout_vote_faults.begin(),
                                     timeout_vote_faults.end());
    }
    if (config.invalid_tc_proposal_detection_enabled) {
      std::vector<StrongFaultRecord> invalid_tc_faults =
          DetectInvalidTcProposalFaults(invalid_tc_proposal_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     invalid_tc_faults.begin(),
                                     invalid_tc_faults.end());
    }
    if (config.conflicting_qc_detection_enabled) {
      std::vector<StrongFaultRecord> conflicting_qc_faults =
          DetectConflictingQcFaults(verified_qc_artifact_evidence,
                                    total_replicas);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     conflicting_qc_faults.begin(),
                                     conflicting_qc_faults.end());
    }
    candidate.strong_faults =
        SummarizeFaultedValidators(candidate.strong_faults);
    std::sort(candidate.strong_faults.begin(), candidate.strong_faults.end(),
              [](const StrongFaultRecord& lhs, const StrongFaultRecord& rhs) {
                return std::tie(lhs.validator_id, lhs.view_or_round,
                                lhs.slot_or_height, lhs.type,
                                lhs.first_artifact_digest,
                                lhs.second_artifact_digest) <
                       std::tie(rhs.validator_id, rhs.view_or_round,
                                rhs.slot_or_height, rhs.type,
                                rhs.first_artifact_digest,
                                rhs.second_artifact_digest);
              });
    std::set<int> faulted_validators;
    for (const StrongFaultRecord& fault : candidate.strong_faults) {
      if (fault.validator_id < 1 || fault.validator_id > total_replicas) {
        continue;
      }
      ValidatorReputation& validator =
          candidate.validators[fault.validator_id - 1];
      ++validator.strong_fault_count;
      faulted_validators.insert(fault.validator_id);
    }
    const int64_t penalty_weight =
        ClampWeight(config.strong_fault_target_weight, config);
    for (int validator_id : faulted_validators) {
      ValidatorReputation& validator = candidate.validators[validator_id - 1];
      validator.recovery_credit = 0;
      validator.bonus_credit = 0;
      validator.reputation_score = 0;
      validator.penalty_points =
          std::max<int64_t>(0, validator.next_weight - penalty_weight);
      validator.next_weight = penalty_weight;
    }
  }

  RecomputeReputationCandidateRoots(&candidate);
  return candidate;
}

void RecomputeReputationCandidateRoots(ReputationCandidate* candidate) {
  if (candidate == nullptr) {
    return;
  }
  candidate->next_weights.clear();
  candidate->next_weights.reserve(candidate->validators.size());
  for (const ValidatorReputation& validator : candidate->validators) {
    candidate->next_weights.push_back(validator.next_weight);
  }
  candidate->metric_root_hex = HashHex(MetricCanonical(*candidate));
  candidate->reputation_root_hex = HashHex(ReputationCanonical(*candidate));
  candidate->strong_fault_root_hex = HashHex(StrongFaultCanonical(*candidate));
  candidate->penalty_root_hex = HashHex(PenaltyCanonical(*candidate));
  candidate->next_weight_root_hex = WeightRootHex(candidate->next_weights);
  candidate->candidate_digest_hex = ReputationCandidateDigest(
      candidate->total_replicas, candidate->window_index, candidate->start_view,
      candidate->end_view, candidate->event_count,
      candidate->old_weight_root_hex, candidate->old_weight_version,
      candidate->activation_view, candidate->metric_root_hex,
      candidate->reputation_root_hex, candidate->next_weight_root_hex,
      candidate->next_weights, candidate->strong_fault_root_hex,
      candidate->penalty_root_hex);
}

std::string ReputationCandidateDigest(
    int total_replicas, uint64_t window_index, int start_view, int end_view,
    uint64_t event_count, const std::string& old_weight_root_hex,
    uint64_t old_weight_version, int activation_view,
    const std::string& metric_root_hex,
    const std::string& reputation_root_hex,
    const std::string& next_weight_root_hex,
    const std::vector<int64_t>& next_weights,
    const std::string& strong_fault_root_hex,
    const std::string& penalty_root_hex) {
  (void)window_index;
  (void)event_count;
  return HashHex(CandidateCanonicalFromParts(
      total_replicas, start_view, end_view, old_weight_root_hex,
      old_weight_version, activation_view, metric_root_hex,
      reputation_root_hex, next_weight_root_hex, next_weights,
      strong_fault_root_hex, penalty_root_hex));
}

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
