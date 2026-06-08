#include "platform/consensus/reputation/sybil_graph.h"

#include <algorithm>
#include <map>

#include "platform/consensus/reputation/reputation_utils.h"

namespace resdb {
namespace consensus {
namespace reputation {
namespace {

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

}  // namespace

SybilGraphAudit ComputeSybilGraphAudit(
    int total_replicas, const std::vector<CertifiedSignerEvidence>& certified_events,
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
  for (const CertifiedSignerEvidence& event : certified_events) {
    if (event.signer_bitmap.empty() || event.leader_id < 1 ||
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

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
