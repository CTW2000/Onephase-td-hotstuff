#pragma once

#include <cstdint>
#include <set>
#include <vector>

#include "platform/consensus/reputation/reputation_types.h"

namespace resdb {
namespace consensus {
namespace reputation {

struct SybilGraphAudit {
  std::vector<int> rank_score;
  std::vector<int> cut_score;
  std::vector<int> graph_score;
  std::vector<uint64_t> graph_degree;
  std::vector<int> seed_trust_score;
  uint64_t edge_evidence_count = 0;
};

SybilGraphAudit ComputeSybilGraphAudit(
    int total_replicas, const std::vector<CertifiedSignerEvidence>& certified_events,
    const std::vector<int64_t>& weights,
    const std::vector<std::set<int>>& unique_signers_by_leader,
    const std::vector<uint64_t>& diversity_count,
    const std::vector<std::set<int>>& unique_available_signers_by_leader,
    const std::vector<uint64_t>& available_signer_evidence_count,
    const ReputationConfig& config);

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
