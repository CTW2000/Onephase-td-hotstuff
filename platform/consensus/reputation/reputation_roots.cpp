#include "platform/consensus/reputation/reputation_roots.h"

#include <algorithm>
#include <numeric>
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
        << validator.vote_beta_success << ':' << validator.vote_beta_failure
        << ':' << validator.stake_factor_per_mille << ':'
        << validator.stake_power_factor_per_mille << ':'
        << validator.identity_factor_per_mille << ':'
        << validator.reputation_factor_per_mille << ':'
        << validator.direct_penalty_factor_per_mille << ':'
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
        << ':' << validator.next_weight << ':' << validator.vote_beta_success
        << ':' << validator.vote_beta_failure << ':'
        << validator.stake_factor_per_mille << ':'
        << validator.stake_power_factor_per_mille << ':'
        << validator.identity_factor_per_mille << ':'
        << validator.reputation_factor_per_mille << ':'
        << validator.direct_penalty_factor_per_mille << ':'
        << validator.strong_fault_count
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
    int leader_selection_version,
    int leader_epoch_start_view,
    int leader_epoch_views,
    const std::vector<int>& leader_epoch_leaders,
    const std::string& leader_schedule_root_hex) {
  (void)metric_root_hex;
  (void)reputation_root_hex;
  std::ostringstream out;
  out << "protocol_neutral_reputation_decision_v4|" << total_replicas << '|'
      << start_view << '|' << end_view << '|' << old_weight_root_hex << '|'
      << old_weight_version << '|' << activation_view << '|'
      << next_weight_root_hex << '|' << strong_fault_root_hex << '|'
      << penalty_root_hex << "|leader:" << leader_selection_version << '|'
      << leader_eligible_min_weight << '|' << leader_weight_root_hex
      << "|leader_epoch:" << leader_epoch_start_view << '|'
      << leader_epoch_views << '|' << leader_schedule_root_hex;
  for (size_t i = 0; i < next_weights.size(); ++i) {
    out << "|w" << (i + 1) << ':' << next_weights[i];
  }
  for (size_t i = 0; i < leader_weights.size(); ++i) {
    out << "|l" << (i + 1) << ':' << leader_weights[i];
  }
  for (size_t i = 0; i < leader_epoch_leaders.size(); ++i) {
    out << "|e" << (leader_epoch_start_view + static_cast<int>(i)) << ':'
        << leader_epoch_leaders[i];
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

std::vector<int> BuildLeaderEpochSchedule(
    int total_replicas, const std::vector<int64_t>& leader_weights,
    int64_t eligible_min_weight, int epoch_start_view, int epoch_views) {
  if (total_replicas <= 0 || epoch_views <= 0) {
    return {};
  }
  std::vector<int64_t> normalized(static_cast<size_t>(total_replicas), 1);
  for (int i = 0; i < total_replicas && i < static_cast<int>(leader_weights.size()); ++i) {
    normalized[i] = std::max<int64_t>(1, std::min<int64_t>(100, leader_weights[i]));
  }
  const int64_t threshold = std::max<int64_t>(1, eligible_min_weight);
  std::vector<int64_t> effective(normalized.size(), 0);
  for (size_t i = 0; i < normalized.size(); ++i) {
    if (normalized[i] >= threshold) {
      effective[i] = normalized[i];
    }
  }
  int64_t total = std::accumulate(effective.begin(), effective.end(), int64_t{0});
  if (total <= 0) {
    effective = normalized;
    total = std::accumulate(effective.begin(), effective.end(), int64_t{0});
  }

  std::vector<int> cycle;
  if (total <= 0) {
    for (int id = 1; id <= total_replicas; ++id) {
      cycle.push_back(id);
    }
  } else {
    std::vector<int64_t> current(effective.size(), 0);
    cycle.reserve(static_cast<size_t>(total));
    for (int64_t step = 0; step < total; ++step) {
      int best = -1;
      for (size_t i = 0; i < effective.size(); ++i) {
        if (effective[i] <= 0) {
          continue;
        }
        current[i] += effective[i];
        if (best < 0 || current[i] > current[best] ||
            (current[i] == current[best] && i < static_cast<size_t>(best))) {
          best = static_cast<int>(i);
        }
      }
      if (best < 0) {
        break;
      }
      cycle.push_back(best + 1);
      current[best] -= total;
    }
  }
  if (cycle.empty()) {
    for (int id = 1; id <= total_replicas; ++id) {
      cycle.push_back(id);
    }
  }

  std::vector<int> epoch;
  epoch.reserve(static_cast<size_t>(epoch_views));
  for (int offset = 0; offset < epoch_views; ++offset) {
    int idx = (epoch_start_view + offset) % static_cast<int>(cycle.size());
    if (idx < 0) {
      idx += static_cast<int>(cycle.size());
    }
    epoch.push_back(cycle[static_cast<size_t>(idx)]);
  }
  return epoch;
}

std::string LeaderScheduleRootHex(
    int leader_selection_version, int64_t eligible_min_weight,
    const std::string& leader_weight_root_hex, int epoch_start_view,
    int epoch_views, const std::vector<int>& leader_epoch_leaders) {
  std::ostringstream out;
  out << "protocol_neutral_leader_schedule_root_v1|"
      << leader_selection_version << '|' << eligible_min_weight << '|'
      << leader_weight_root_hex << '|' << epoch_start_view << '|'
      << epoch_views;
  for (size_t i = 0; i < leader_epoch_leaders.size(); ++i) {
    out << '|' << (epoch_start_view + static_cast<int>(i)) << ':'
        << leader_epoch_leaders[i];
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
  if (candidate->leader_epoch_start_view <= 0) {
    candidate->leader_epoch_start_view = candidate->activation_view;
  }
  if (candidate->leader_epoch_views <= 0) {
    candidate->leader_epoch_views =
        std::max(1, candidate->end_view > candidate->start_view
                        ? candidate->end_view - candidate->start_view
                        : candidate->total_replicas);
  }
  candidate->leader_epoch_leaders = BuildLeaderEpochSchedule(
      candidate->total_replicas, candidate->leader_weights,
      candidate->leader_eligible_min_weight, candidate->leader_epoch_start_view,
      candidate->leader_epoch_views);
  candidate->leader_schedule_root_hex = LeaderScheduleRootHex(
      candidate->leader_selection_version, candidate->leader_eligible_min_weight,
      candidate->leader_weight_root_hex, candidate->leader_epoch_start_view,
      candidate->leader_epoch_views, candidate->leader_epoch_leaders);
  candidate->candidate_digest_hex = ReputationCandidateDigest(
      candidate->total_replicas, candidate->window_index, candidate->start_view,
      candidate->end_view, candidate->event_count,
      candidate->old_weight_root_hex, candidate->old_weight_version,
      candidate->activation_view, candidate->metric_root_hex,
      candidate->reputation_root_hex, candidate->next_weight_root_hex,
      candidate->next_weights, candidate->strong_fault_root_hex,
      candidate->penalty_root_hex, candidate->leader_weights,
      candidate->leader_weight_root_hex, candidate->leader_eligible_min_weight,
      candidate->leader_selection_version, candidate->leader_epoch_start_view,
      candidate->leader_epoch_views, candidate->leader_epoch_leaders,
      candidate->leader_schedule_root_hex);
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
    int leader_selection_version,
    int leader_epoch_start_view,
    int leader_epoch_views,
    const std::vector<int>& leader_epoch_leaders,
    const std::string& leader_schedule_root_hex) {
  (void)window_index;
  (void)event_count;
  return HashHex(CandidateCanonicalFromParts(
      total_replicas, start_view, end_view, old_weight_root_hex,
      old_weight_version, activation_view, metric_root_hex,
      reputation_root_hex, next_weight_root_hex, next_weights,
      strong_fault_root_hex, penalty_root_hex, leader_weights,
      leader_weight_root_hex, leader_eligible_min_weight,
      leader_selection_version, leader_epoch_start_view, leader_epoch_views,
      leader_epoch_leaders, leader_schedule_root_hex));
}

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
