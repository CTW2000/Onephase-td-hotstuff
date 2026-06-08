#include "platform/consensus/reputation/reputation_roots.h"

#include <sstream>

#include "platform/consensus/reputation/reputation_utils.h"

namespace resdb {
namespace consensus {
namespace reputation {
namespace {

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
    const std::string& penalty_root_hex,
    const std::vector<int64_t>& leader_weights,
    const std::string& leader_weight_root_hex,
    int64_t leader_eligible_min_weight,
    int leader_selection_version) {
  (void)metric_root_hex;
  (void)reputation_root_hex;
  std::ostringstream out;
  out << "protocol_neutral_reputation_decision_v4|" << total_replicas << '|'
      << start_view << '|' << end_view << '|' << old_weight_root_hex << '|'
      << old_weight_version << '|' << activation_view << '|'
      << next_weight_root_hex << '|' << strong_fault_root_hex << '|'
      << penalty_root_hex << "|leader:" << leader_selection_version << '|'
      << leader_eligible_min_weight << '|' << leader_weight_root_hex;
  for (size_t i = 0; i < next_weights.size(); ++i) {
    out << "|w" << (i + 1) << ':' << next_weights[i];
  }
  for (size_t i = 0; i < leader_weights.size(); ++i) {
    out << "|l" << (i + 1) << ':' << leader_weights[i];
  }
  return out.str();
}

}  // namespace

std::string WeightRootHex(const std::vector<int64_t>& weights) {
  std::ostringstream out;
  out << "protocol_neutral_weight_root_v1";
  for (size_t i = 0; i < weights.size(); ++i) {
    out << '|' << (i + 1) << ':' << weights[i];
  }
  return HashHex(out.str());
}

std::string LeaderWeightRootHex(const std::vector<int64_t>& weights,
                                int64_t eligible_min_weight,
                                int leader_selection_version) {
  std::ostringstream out;
  out << "protocol_neutral_leader_weight_root_v1|"
      << leader_selection_version << '|' << eligible_min_weight;
  for (size_t i = 0; i < weights.size(); ++i) {
    out << '|' << (i + 1) << ':' << weights[i];
  }
  return HashHex(out.str());
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
  if (candidate->leader_weights.empty()) {
    candidate->leader_weights = candidate->next_weights;
  }
  if (candidate->leader_eligible_min_weight <= 0) {
    candidate->leader_eligible_min_weight = 10;
  }
  if (candidate->leader_selection_version <= 0) {
    candidate->leader_selection_version = 1;
  }
  candidate->leader_weight_root_hex = LeaderWeightRootHex(
      candidate->leader_weights, candidate->leader_eligible_min_weight,
      candidate->leader_selection_version);
  candidate->candidate_digest_hex = ReputationCandidateDigest(
      candidate->total_replicas, candidate->window_index, candidate->start_view,
      candidate->end_view, candidate->event_count,
      candidate->old_weight_root_hex, candidate->old_weight_version,
      candidate->activation_view, candidate->metric_root_hex,
      candidate->reputation_root_hex, candidate->next_weight_root_hex,
      candidate->next_weights, candidate->strong_fault_root_hex,
      candidate->penalty_root_hex, candidate->leader_weights,
      candidate->leader_weight_root_hex, candidate->leader_eligible_min_weight,
      candidate->leader_selection_version);
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
    const std::string& penalty_root_hex,
    const std::vector<int64_t>& leader_weights,
    const std::string& leader_weight_root_hex,
    int64_t leader_eligible_min_weight,
    int leader_selection_version) {
  (void)window_index;
  (void)event_count;
  return HashHex(CandidateCanonicalFromParts(
      total_replicas, start_view, end_view, old_weight_root_hex,
      old_weight_version, activation_view, metric_root_hex,
      reputation_root_hex, next_weight_root_hex, next_weights,
      strong_fault_root_hex, penalty_root_hex, leader_weights,
      leader_weight_root_hex, leader_eligible_min_weight,
      leader_selection_version));
}

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
