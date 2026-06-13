#include "platform/consensus/reputation/reputation_algorithm.h"

#include <algorithm>
#include <map>
#include <numeric>
#include <set>
#include <tuple>
#include <utility>

#include "platform/consensus/reputation/reputation_roots.h"
#include "platform/consensus/reputation/reputation_utils.h"
#include "platform/consensus/reputation/soft_reputation.h"
#include "platform/consensus/reputation/strong_fault_detector.h"
#include "platform/consensus/reputation/sybil_graph.h"

namespace resdb {
namespace consensus {
namespace reputation {
namespace {

constexpr const char* kAlgorithmBayesV4 = "bayes_v4";
constexpr int64_t kCarryoverDecayWeightGap = 10;
constexpr int kHealthyCatchUpScore = 40;
constexpr int kDebtNoRecoveryBelow = 30;
constexpr uint64_t kMinNoCertifiedLeaderOpportunities = 3;
constexpr uint64_t kVoteBetaCounterScale = 1000;

struct CoreEvidenceEvent {
  int view_or_round = 0;
  int leader_id = 0;
  std::string artifact_digest;
  std::string signer_bitmap;
  std::string available_signer_bitmap;
  OutcomeClass outcome_class = OutcomeClass::kNone;
};

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

int VoteBetaDecayPerMille(const ReputationConfig& config) {
  return std::max(0, std::min(1000, config.vote_beta_decay_per_mille));
}

uint64_t ScaledObservationCount(uint64_t count) {
  return count * kVoteBetaCounterScale;
}

uint64_t DecayVoteBetaCounter(uint64_t counter, const ReputationConfig& config) {
  const int decay_per_mille = VoteBetaDecayPerMille(config);
  if (counter == 0 || decay_per_mille == 0) {
    return 0;
  }
  return (counter * static_cast<uint64_t>(decay_per_mille) +
          kVoteBetaCounterScale / 2) /
         kVoteBetaCounterScale;
}

int VoteScoreFromBetaCounters(uint64_t success, uint64_t failure) {
  const uint64_t numerator = 100 * (kVoteBetaCounterScale + success);
  const uint64_t denominator =
      2 * kVoteBetaCounterScale + success + failure;
  return std::max(0, std::min(100, RoundedDivide(numerator, denominator)));
}

void UpdateVoteBetaCounters(ValidatorReputation* validator,
                            const ReputationConfig& config) {
  if (validator == nullptr) {
    return;
  }
  const uint64_t misses =
      validator->opportunities > validator->inclusions
          ? validator->opportunities - validator->inclusions
          : 0;
  validator->vote_beta_success =
      DecayVoteBetaCounter(validator->vote_beta_success, config) +
      ScaledObservationCount(validator->inclusions);
  validator->vote_beta_failure =
      DecayVoteBetaCounter(validator->vote_beta_failure, config) +
      ScaledObservationCount(misses);
  validator->vote_score = VoteScoreFromBetaCounters(
      validator->vote_beta_success, validator->vote_beta_failure);
}

int CoreRecoveryCreditForScore(int score, int max_recovery_per_epoch) {
  if (max_recovery_per_epoch <= 0) {
    return 0;
  }
  constexpr int kNoRecoveryBelow = 10;
  constexpr int kFullRecoveryAt = 30;
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

bool HasLeaderRecoveryEvidence(const ValidatorReputation& validator,
                               const ReputationConfig& config) {
  if (!config.leader_recovery_enabled) {
    return false;
  }
  const bool has_repeated_uncertified_leader_opportunities =
      validator.leader_opportunity_count >= kMinNoCertifiedLeaderOpportunities &&
      validator.leader_certified_count == 0 && validator.leader_score < 50;
  return validator.leader_score < 95 &&
         (validator.leader_opportunity_count >= config.min_leader_opportunities ||
          has_repeated_uncertified_leader_opportunities);
}

bool IsLeaderWeightIneligible(const ValidatorReputation& validator,
                              const ReputationConfig& config,
                              int64_t current_leader_weight) {
  if (validator.strong_fault_count > 0) {
    return true;
  }
  if (!config.leader_recovery_enabled) {
    return validator.next_weight <= config.leader_eligible_min_weight;
  }
  const bool has_good_leader_reentry_evidence =
      validator.leader_opportunity_count >= config.min_leader_opportunities &&
      validator.leader_score >= 95;
  if (current_leader_weight <= config.leader_eligible_min_weight &&
      !has_good_leader_reentry_evidence) {
    return true;
  }
  return HasLeaderRecoveryEvidence(validator, config) &&
         validator.next_weight <= config.leader_eligible_min_weight;
}

int LeaderSelectionScore(const ValidatorReputation& validator,
                         const ReputationConfig& config) {
  static_cast<void>(validator);
  static_cast<void>(config);
  int score = 100;
  // PeerTrust debt is intentionally audit/plugin-side state for now. Applying
  // it directly to the active consensus leader schedule requires a synchronized
  // activation protocol, otherwise validators can switch expected leaders at
  // different local views.
  return std::max(0, std::min(100, score));
}

std::vector<int64_t> LeaderWeightsForCandidate(
    const std::vector<ValidatorReputation>& validators,
    const ReputationConfig& config,
    const std::vector<int64_t>& current_leader_weights) {
  std::vector<int64_t> leader_weights;
  leader_weights.reserve(validators.size());
  bool all_validators_eligible = !validators.empty();
  for (size_t i = 0; i < validators.size(); ++i) {
    const ValidatorReputation& validator = validators[i];
    const int64_t current_leader_weight =
        i < current_leader_weights.size() ? current_leader_weights[i]
                                          : config.max_weight;
    if (IsLeaderWeightIneligible(validator, config, current_leader_weight)) {
      all_validators_eligible = false;
      break;
    }
    if (LeaderSelectionScore(validator, config) < 100) {
      all_validators_eligible = false;
      break;
    }
  }
  const int64_t equal_leader_weight = ClampWeight(config.max_weight, config);
  const int64_t ineligible_leader_weight = ClampWeight(config.min_weight, config);
  for (size_t i = 0; i < validators.size(); ++i) {
    const ValidatorReputation& validator = validators[i];
    const int64_t current_leader_weight =
        i < current_leader_weights.size() ? current_leader_weights[i]
                                          : config.max_weight;
    if (all_validators_eligible) {
      leader_weights.push_back(equal_leader_weight);
    } else if (IsLeaderWeightIneligible(validator, config,
                                        current_leader_weight)) {
      leader_weights.push_back(ineligible_leader_weight);
    } else if (LeaderSelectionScore(validator, config) < 100) {
      leader_weights.push_back(ClampWeight(LeaderSelectionScore(validator, config),
                                           config));
    } else if (config.leader_recovery_enabled) {
      leader_weights.push_back(equal_leader_weight);
    } else {
      leader_weights.push_back(validator.next_weight);
    }
  }
  return leader_weights;
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


bool HasStrongFaultEvidence(const ReputationWindowInput& input) {
  return !input.signed_proposal_evidence.empty() ||
         !input.signed_vote_evidence.empty() ||
         !input.invalid_qc_proposal_evidence.empty() ||
         !input.signed_weight_update_vote_evidence.empty() ||
         !input.signed_timeout_vote_evidence.empty() ||
         !input.invalid_tc_proposal_evidence.empty() ||
         !input.verified_qc_artifact_evidence.empty();
}

bool UseCoreOnlyFastPath(const ReputationWindowInput& input,
                         const ReputationConfig& config) {
  return !config.leader_recovery_enabled && !config.peertrust_enabled &&
         !config.sybil_graph_enabled && !config.strong_fault_enabled &&
         !HasStrongFaultEvidence(input);
}

uint64_t QuorumAvailableSignerThreshold(int total_replicas) {
  if (total_replicas <= 0) {
    return 0;
  }
  return static_cast<uint64_t>(
      std::min(total_replicas, (total_replicas * 2) / 3 + 1));
}

uint64_t WindowFairAvailableSignerThreshold(int total_replicas) {
  if (total_replicas <= 0) {
    return 0;
  }
  return static_cast<uint64_t>(
      std::min(total_replicas, (total_replicas * 4 + 4) / 5));
}

bool HasQuorumAvailableSignerCoverage(
    const std::vector<int>& available_signers, int total_replicas) {
  const uint64_t threshold = QuorumAvailableSignerThreshold(total_replicas);
  return threshold > 0 && available_signers.size() >= threshold;
}

bool HasWindowFairAvailableSignerCoverage(
    const std::set<int>& available_signers, int total_replicas) {
  const uint64_t threshold = WindowFairAvailableSignerThreshold(total_replicas);
  return threshold > 0 && available_signers.size() >= threshold;
}

void ComputeCoreOnlyReputation(const std::vector<CoreEvidenceEvent>& ordered_events,
                               const std::vector<int64_t>& weights,
                               const ReputationConfig& config,
                               bool count_certified_leaders_as_opportunities,
                               ReputationCandidate* candidate) {
  if (candidate == nullptr) {
    return;
  }
  const int total_replicas = candidate->total_replicas;
  uint64_t legacy_selected_signer_slots = 0;
  uint64_t legacy_certificate_event_count = 0;
  uint64_t broad_available_signer_slots = 0;
  uint64_t broad_available_event_count = 0;
  std::vector<uint64_t> signer_opportunity_count(
      std::max(total_replicas, 0), 0);
  std::vector<uint64_t> available_not_selected_count(
      std::max(total_replicas, 0), 0);
  std::set<int> window_quorum_available_signers;
  std::set<std::pair<int, std::string>> seen_certificates;
  std::set<std::pair<int, int>> seen_leader_opportunities;

  for (const CoreEvidenceEvent& event : ordered_events) {
    const int leader = event.leader_id;
    if (event.outcome_class != OutcomeClass::kCertified ||
        event.signer_bitmap.empty()) {
      continue;
    }
    const std::string certificate_key =
        event.artifact_digest.empty()
            ? "view-" + std::to_string(event.view_or_round)
            : event.artifact_digest;
    if (!seen_certificates.insert({event.view_or_round, certificate_key})
             .second) {
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
    std::set<int> signer_set(signers.begin(), signers.end());
    if (has_available_signer_evidence) {
      if (HasQuorumAvailableSignerCoverage(available_signers,
                                           total_replicas)) {
        broad_available_signer_slots += available_signers.size();
        ++broad_available_event_count;
        for (int signer : available_signers) {
          if (signer >= 1 && signer <= total_replicas) {
            window_quorum_available_signers.insert(signer);
          }
        }
      }
      for (int signer : signers) {
        if (signer >= 1 && signer <= total_replicas) {
          ++candidate->validators[signer - 1].inclusions;
          ++signer_opportunity_count[signer - 1];
        }
      }
      for (int signer : available_signers) {
        if (signer >= 1 && signer <= total_replicas &&
            signer_set.find(signer) == signer_set.end()) {
          ++available_not_selected_count[signer - 1];
        }
      }
    } else {
      legacy_selected_signer_slots += signers.size();
      ++legacy_certificate_event_count;
      for (int signer : signers) {
        if (signer >= 1 && signer <= total_replicas) {
          ++candidate->validators[signer - 1].inclusions;
        }
      }
    }
    if (leader >= 1 && leader <= total_replicas) {
      ValidatorReputation& leader_score = candidate->validators[leader - 1];
      if (count_certified_leaders_as_opportunities &&
          seen_leader_opportunities.insert({event.view_or_round, leader})
              .second) {
        ++leader_score.leader_opportunity_count;
      }
      ++leader_score.leader_certified_count;
    }
  }

  const uint64_t fair_opportunities = FairExpectedSignerOpportunities(
      legacy_selected_signer_slots, total_replicas,
      legacy_certificate_event_count);
  const uint64_t broad_available_fair_opportunities =
      HasWindowFairAvailableSignerCoverage(window_quorum_available_signers,
                                           total_replicas)
          ? FairExpectedSignerOpportunities(broad_available_signer_slots,
                                            total_replicas,
                                            broad_available_event_count)
          : 0;
  for (ValidatorReputation& validator : candidate->validators) {
    const int idx = validator.validator_id - 1;
    const uint64_t direct_opportunities =
        idx >= 0 && idx < static_cast<int>(signer_opportunity_count.size())
            ? signer_opportunity_count[idx]
            : 0;
    const uint64_t excused_available_not_selected =
        idx >= 0 &&
                idx < static_cast<int>(available_not_selected_count.size())
            ? available_not_selected_count[idx]
            : 0;
    const uint64_t adjusted_broad_available_fair_opportunities =
        broad_available_fair_opportunities > excused_available_not_selected
            ? broad_available_fair_opportunities -
                  excused_available_not_selected
            : 0;
    validator.opportunities =
        std::max(direct_opportunities,
                 adjusted_broad_available_fair_opportunities) +
        fair_opportunities;
  }

  int64_t total_current_weight = 0;
  for (const ValidatorReputation& validator : candidate->validators) {
    total_current_weight += validator.current_weight;
  }
  const int64_t mean_current_weight =
      candidate->validators.empty()
          ? 0
          : RoundedDivide(static_cast<uint64_t>(total_current_weight),
                          candidate->validators.size());

  for (ValidatorReputation& validator : candidate->validators) {
    UpdateVoteBetaCounters(&validator, config);
    validator.leader_score = LeaderCertifiedScore(
        validator.leader_certified_count, validator.leader_opportunity_count);
    int recovery_score = validator.vote_score;
    const bool carryover_decay =
        validator.opportunities == 0 &&
        validator.current_weight + kCarryoverDecayWeightGap <
            mean_current_weight;
    const bool validator_has_enough_decay_evidence =
        validator.opportunities >= config.min_decay_opportunities ||
        carryover_decay;
    const bool near_fair_vote =
        HasNearFairInclusion(validator.inclusions, validator.opportunities);
    const int64_t available_decay =
        std::max<int64_t>(0, validator.current_weight - config.min_weight);
    validator.decay_applied =
        validator_has_enough_decay_evidence
            ? static_cast<int>(
                  std::min<int64_t>(available_decay, config.decay_per_epoch))
            : 0;
    validator.recovery_credit =
        carryover_decay
            ? 0
            : std::min(validator.decay_applied,
                       CoreRecoveryCreditForScore(
                           recovery_score, config.max_recovery_per_epoch));
    const bool below_mean_weight = validator.current_weight < mean_current_weight;
    const bool fully_recovered_decay =
        validator.decay_applied > 0 &&
        validator.recovery_credit >= validator.decay_applied;
    const bool has_recovery_debt =
        validator.peertrust_leader_debt > 0 || validator.sybil_graph_debt > 0;
    const bool healthy_catchup_score =
        !has_recovery_debt && fully_recovered_decay &&
        validator.vote_score >= kHealthyCatchUpScore &&
        recovery_score >= kHealthyCatchUpScore;
    const bool earns_bonus =
        validator_has_enough_decay_evidence && below_mean_weight &&
        ((recovery_score >= 95 && validator.vote_score >= 95) ||
         (near_fair_vote && recovery_score >= 67) || healthy_catchup_score);
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
}

}  // namespace

ReputationCandidate ComputeReputationCandidate(
    const ReputationWindowInput& input, const ReputationConfig& config) {

  const int total_replicas = input.total_replicas;

  ReputationCandidate candidate;
  candidate.algorithm = kAlgorithmBayesV4;
  candidate.local_node_id = input.local_node_id;
  candidate.total_replicas = total_replicas;
  candidate.window_index = input.window_index;

  std::vector<CoreEvidenceEvent> ordered_events;
  ordered_events.reserve(input.certified_signer_evidence.size() +
                         input.leader_outcome_evidence.size());
  for (const CertifiedSignerEvidence& event : input.certified_signer_evidence) {
    CoreEvidenceEvent core;
    core.view_or_round = event.view_or_round;
    core.leader_id = event.leader_id;
    core.artifact_digest = event.artifact_digest;
    core.signer_bitmap = event.signer_bitmap;
    core.available_signer_bitmap = event.available_signer_bitmap;
    core.outcome_class = OutcomeClass::kCertified;
    ordered_events.push_back(std::move(core));
  }
  for (const LeaderOutcomeEvidence& event : input.leader_outcome_evidence) {
    CoreEvidenceEvent core;
    core.view_or_round = event.view_or_round;
    core.leader_id = event.leader_id;
    core.artifact_digest = event.artifact_digest;
    core.outcome_class = event.outcome_class;
    ordered_events.push_back(std::move(core));
  }
  std::sort(ordered_events.begin(), ordered_events.end(),
            [](const CoreEvidenceEvent& lhs, const CoreEvidenceEvent& rhs) {
              if (lhs.view_or_round != rhs.view_or_round) {
                return lhs.view_or_round < rhs.view_or_round;
              }
              return lhs.artifact_digest < rhs.artifact_digest;
            });
  candidate.event_count = ordered_events.size();
  if (!ordered_events.empty()) {
    candidate.start_view = ordered_events.front().view_or_round;
    candidate.end_view = ordered_events.back().view_or_round;
  }

  const std::vector<int64_t> weights =
      NormalizeWeights(input.current_weights, total_replicas);
  candidate.old_weight_root_hex =
      input.old_weight_root_hex.empty() ? WeightRootHex(weights) : input.old_weight_root_hex;
  candidate.old_weight_version = input.old_weight_version;
  candidate.activation_view = input.activation_view;
  candidate.validators.resize(std::max(total_replicas, 0));
  const bool has_scheduled_leader_counts =
      input.scheduled_leader_counts.size() >=
      static_cast<size_t>(std::max(total_replicas, 0));
  for (int i = 0; i < total_replicas; ++i) {
    ValidatorReputation& validator = candidate.validators[i];
    validator.validator_id = i + 1;
    validator.current_weight = weights[i];
    validator.next_weight = weights[i];
    if (has_scheduled_leader_counts) {
      validator.leader_opportunity_count = input.scheduled_leader_counts[i];
    }
    if (i < static_cast<int>(input.prior_vote_beta_counters.size())) {
      validator.vote_beta_success = input.prior_vote_beta_counters[i].success;
      validator.vote_beta_failure = input.prior_vote_beta_counters[i].failure;
    }
    if (config.peertrust_enabled &&
        i < static_cast<int>(input.prior_peertrust_leader_debt.size())) {
      validator.peertrust_leader_debt = std::max(
          0, std::min(config.peertrust_debt_max,
                      input.prior_peertrust_leader_debt[i]));
    }
    if (config.sybil_graph_enabled &&
        i < static_cast<int>(input.prior_sybil_graph_debt.size())) {
      validator.sybil_graph_debt = std::max(
          0, std::min(config.sybil_graph_debt_max, input.prior_sybil_graph_debt[i]));
    }
  }

  if (UseCoreOnlyFastPath(input, config)) {
    ComputeCoreOnlyReputation(ordered_events, weights, config,
                              !has_scheduled_leader_counts, &candidate);
    candidate.leader_weights =
        LeaderWeightsForCandidate(candidate.validators, config,
                                  input.current_leader_weights);
    candidate.leader_selection_version = 1;
    candidate.leader_eligible_min_weight = config.leader_eligible_min_weight;
    RecomputeReputationCandidateRoots(&candidate);
    return candidate;
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
  std::vector<bool> peertrust_has_credible_public_choice(
      std::max(total_replicas, 0), false);

  uint64_t legacy_selected_signer_slots = 0;
  uint64_t legacy_certificate_event_count = 0;
  uint64_t broad_available_signer_slots = 0;
  uint64_t broad_available_event_count = 0;
  uint64_t certificate_event_count = 0;
  std::vector<uint64_t> signer_opportunity_count(
      std::max(total_replicas, 0), 0);
  std::vector<uint64_t> available_not_selected_count(
      std::max(total_replicas, 0), 0);
  std::set<int> window_quorum_available_signers;
  std::set<std::pair<int, std::string>> seen_certificates;
  std::set<std::pair<int, int>> seen_leader_opportunities;
  for (const CoreEvidenceEvent& event : ordered_events) {
    const int leader = event.leader_id;
    const int diversity_leader = leader;

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
    std::set<int> signer_set(signers.begin(), signers.end());
    if (has_available_signer_evidence) {
      if (HasQuorumAvailableSignerCoverage(available_signers,
                                           total_replicas)) {
        broad_available_signer_slots += available_signers.size();
        ++broad_available_event_count;
        for (int signer : available_signers) {
          if (signer >= 1 && signer <= total_replicas) {
            window_quorum_available_signers.insert(signer);
          }
        }
      }
      for (int signer : signers) {
        if (signer >= 1 && signer <= total_replicas) {
          ++signer_opportunity_count[signer - 1];
          ++candidate.validators[signer - 1].inclusions;
        }
      }
      for (int signer : available_signers) {
        if (signer >= 1 && signer <= total_replicas &&
            signer_set.find(signer) == signer_set.end()) {
          ++available_not_selected_count[signer - 1];
        }
      }
    } else {
      legacy_selected_signer_slots += signers.size();
      ++legacy_certificate_event_count;
      for (int signer : signers) {
        if (signer >= 1 && signer <= total_replicas) {
          ++candidate.validators[signer - 1].inclusions;
        }
      }
    }
    if (leader >= 1 && leader <= total_replicas) {
      ValidatorReputation& leader_score = candidate.validators[leader - 1];
      if (!has_scheduled_leader_counts &&
          seen_leader_opportunities
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
  const uint64_t broad_available_fair_opportunities =
      HasWindowFairAvailableSignerCoverage(window_quorum_available_signers,
                                           total_replicas)
          ? FairExpectedSignerOpportunities(broad_available_signer_slots,
                                            total_replicas,
                                            broad_available_event_count)
          : 0;
  for (ValidatorReputation& validator : candidate.validators) {
    const int idx = validator.validator_id - 1;
    const uint64_t direct_opportunities =
        idx >= 0 && idx < static_cast<int>(signer_opportunity_count.size())
            ? signer_opportunity_count[idx]
            : 0;
    const uint64_t excused_available_not_selected =
        idx >= 0 &&
                idx < static_cast<int>(available_not_selected_count.size())
            ? available_not_selected_count[idx]
            : 0;
    const uint64_t adjusted_broad_available_fair_opportunities =
        broad_available_fair_opportunities > excused_available_not_selected
            ? broad_available_fair_opportunities -
                  excused_available_not_selected
            : 0;
    validator.opportunities =
        std::max(direct_opportunities,
                 adjusted_broad_available_fair_opportunities) +
        fair_opportunities;
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
    const bool selected_narrower_than_available =
        has_broad_available_reviewer_set &&
        signer_coverage_score + 10 < available_coverage_score;
    peertrust_has_credible_public_choice[idx] =
        selected_narrower_than_available;
    const int reviewer_coverage_score =
        selected_narrower_than_available
            ? signer_coverage_score
            : (has_broad_available_reviewer_set
                   ? 100
                   : std::min(available_coverage_score,
                              signer_coverage_score));
    if (!peertrust_has_credible_public_choice[idx]) {
      peertrust_reviewer_entropy_scores[idx] = 100;
      peertrust_cross_leader_independence_scores[idx] = 100;
      peertrust_reviewer_overuse_scores[idx] = 100;
      peertrust_community_context_scores[idx] = 100;
      continue;
    }
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
    // A collusive reviewer clique can occupy many leader slots, so compare
    // against the healthiest observed community context instead of a median.
    peertrust_community_baseline_score =
        peertrust_community_baseline_samples.back();
  }
  constexpr int kPeerTrustCommunityOutlierDeadband = 10;

  const SybilGraphAudit sybil_graph_audit = ComputeSybilGraphAudit(
      total_replicas, input.certified_signer_evidence, weights, unique_signers_by_leader,
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
    UpdateVoteBetaCounters(&validator, config);
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
    const bool has_credible_peertrust_evidence =
        idx >= 0 &&
        idx < static_cast<int>(peertrust_has_credible_public_choice.size()) &&
        peertrust_has_credible_public_choice[idx];
    if (config.peertrust_enabled && idx >= 0 &&
        idx < static_cast<int>(peertrust_feedback_count.size()) &&
        peertrust_feedback_count[idx] > 0) {
      validator.feedback_count = peertrust_feedback_count[idx];
      validator.reviewer_credibility_score =
          has_credible_peertrust_evidence
              ? std::max(0, std::min(100,
                                     RoundedDivide(
                                         peertrust_reviewer_credibility_sum[idx],
                                         peertrust_feedback_count[idx])))
              : 100;
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
      const int relative_leader_diversity_score =
          has_public_available_signer_evidence &&
                  diversity_baseline_samples.size() > 1
              ? (low_diversity_outlier && leader_diversity_baseline_score > 0
                     ? std::max(0, std::min(100,
                                            RoundedDivide(
                                                raw_leader_diversity_score * 100,
                                                leader_diversity_baseline_score)))
                     : 100)
              : validator.leader_diversity_score;
      if (has_credible_peertrust_evidence) {
        validator.community_context_score = std::min(
            relative_leader_diversity_score, relative_peertrust_community_score);
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
        validator.community_context_score = 100;
        validator.peertrust_score = 100;
      }
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
    bool low_peertrust = false;
    bool broad_good_peertrust = false;
    if (config.peertrust_enabled) {
      int next_peertrust_debt = previous_peertrust_debt;
      if (validator.feedback_count > 0) {
        low_peertrust =
            validator.peertrust_score < config.peertrust_debt_trigger_score ||
            validator.community_context_score <
                config.peertrust_debt_trigger_score;
        broad_good_peertrust =
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
    const bool has_repeated_uncertified_leader_opportunities =
        leader_opportunities >= kMinNoCertifiedLeaderOpportunities &&
        validator.leader_certified_count == 0 && validator.leader_score < 50;
    if (config.leader_recovery_enabled &&
        (leader_opportunities >= config.min_leader_opportunities ||
         has_repeated_uncertified_leader_opportunities)) {
      recovery_score = std::min(recovery_score, validator.leader_score);
      if (has_narrow_public_target) {
        recovery_score =
            std::min(recovery_score, validator.leader_diversity_score);
      }
    }
    if (config.peertrust_enabled &&
        (validator.feedback_count > 0 ||
         validator.peertrust_leader_debt > 0)) {
      if (validator.feedback_count > 0) {
        recovery_score = std::min(recovery_score, validator.peertrust_score);
      }
      const int debt_gate_score =
          std::max(0, 100 - validator.peertrust_leader_debt);
      recovery_score = std::min(recovery_score, debt_gate_score);
      if (validator.peertrust_leader_debt > 0 &&
          debt_gate_score < kDebtNoRecoveryBelow) {
        recovery_score = 0;
      }
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
      if (validator.sybil_graph_debt > 0 &&
          debt_gate_score < kDebtNoRecoveryBelow) {
        recovery_score = 0;
      }
    }

    const bool carryover_decay =
        validator.opportunities == 0 &&
        validator.current_weight + kCarryoverDecayWeightGap <
            mean_current_weight;
    const bool validator_has_enough_decay_evidence =
        validator.opportunities >= config.min_decay_opportunities ||
        carryover_decay;
    const bool near_fair_vote =
        HasNearFairInclusion(validator.inclusions, validator.opportunities);
    const int64_t available_decay =
        std::max<int64_t>(0, validator.current_weight - config.min_weight);
    validator.decay_applied =
        validator_has_enough_decay_evidence
            ? static_cast<int>(
                  std::min<int64_t>(available_decay, config.decay_per_epoch))
            : 0;
    validator.recovery_credit =
        carryover_decay
            ? 0
            : std::min(validator.decay_applied,
                       RecoveryCreditForScore(
                           recovery_score, config.max_recovery_per_epoch));
    const bool below_mean_weight =
        validator.current_weight < mean_current_weight;
    const bool fully_recovered_decay =
        validator.decay_applied > 0 &&
        validator.recovery_credit >= validator.decay_applied;
    const bool has_recovery_debt =
        validator.peertrust_leader_debt > 0 || validator.sybil_graph_debt > 0;
    const bool healthy_catchup_score =
        !has_recovery_debt && fully_recovered_decay &&
        validator.vote_score >= kHealthyCatchUpScore &&
        recovery_score >= kHealthyCatchUpScore;
    const bool at_leader_reentry_boundary =
        config.leader_recovery_enabled && below_mean_weight &&
        validator.current_weight <= config.leader_eligible_min_weight;
    const bool has_good_leader_reentry_evidence =
        leader_opportunities >= config.min_leader_opportunities &&
        validator.leader_score >= 95;
    const bool leader_reentry_bonus_allowed =
        !at_leader_reentry_boundary || has_good_leader_reentry_evidence;
    const int64_t peertrust_soft_floor = std::max<int64_t>(
        config.min_weight,
        std::min<int64_t>(config.peertrust_soft_min_weight, config.max_weight));
    const bool peertrust_floor_recovery_guard =
        config.peertrust_enabled &&
        validator.current_weight <= peertrust_soft_floor &&
        validator.current_weight + validator.decay_applied >=
            peertrust_soft_floor &&
        (validator.feedback_count > 0 || validator.peertrust_leader_debt > 0) &&
        !(broad_good_peertrust && validator.leader_diversity_score >= 95);
    const bool earns_bonus =
        leader_reentry_bonus_allowed && validator_has_enough_decay_evidence &&
        !peertrust_floor_recovery_guard &&
        below_mean_weight &&
        ((recovery_score >= 95 && validator.vote_score >= 95) ||
         (near_fair_vote && recovery_score >= 67) || healthy_catchup_score);
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
    const bool peertrust_floor_applies =
        low_peertrust || validator.peertrust_leader_debt > 0 ||
        previous_peertrust_debt > 0;
    const bool peertrust_soft_floor_eligible =
        config.peertrust_enabled &&
        validator.vote_score >= kHealthyCatchUpScore &&
        validator.sybil_graph_debt == 0;
    if (peertrust_soft_floor_eligible && low_peertrust &&
        validator.current_weight > peertrust_soft_floor &&
        validator.next_weight > peertrust_soft_floor) {
      validator.next_weight = peertrust_soft_floor;
    }
    if (peertrust_floor_applies && peertrust_soft_floor_eligible &&
        validator.current_weight + validator.decay_applied >=
            peertrust_soft_floor &&
        validator.next_weight < peertrust_soft_floor) {
      validator.next_weight = peertrust_soft_floor;
    }
  }

  if (config.strong_fault_enabled) {
    if (config.double_proposal_detection_enabled) {
      std::vector<StrongFaultRecord> proposal_faults =
          DetectDoubleProposalFaults(input.signed_proposal_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     proposal_faults.begin(),
                                     proposal_faults.end());
    }
    if (config.double_vote_detection_enabled) {
      std::vector<StrongFaultRecord> vote_faults =
          DetectDoubleVoteFaults(input.signed_vote_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     vote_faults.begin(), vote_faults.end());
    }
    if (config.invalid_qc_proposal_detection_enabled) {
      std::vector<StrongFaultRecord> invalid_qc_faults =
          DetectInvalidQcProposalFaults(input.invalid_qc_proposal_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     invalid_qc_faults.begin(),
                                     invalid_qc_faults.end());
    }
    if (config.weight_update_vote_equivocation_detection_enabled) {
      std::vector<StrongFaultRecord> weight_vote_faults =
          DetectWeightUpdateVoteEquivocationFaults(
              input.signed_weight_update_vote_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     weight_vote_faults.begin(),
                                     weight_vote_faults.end());
    }
    if (config.timeout_vote_equivocation_detection_enabled) {
      std::vector<StrongFaultRecord> timeout_vote_faults =
          DetectTimeoutVoteEquivocationFaults(input.signed_timeout_vote_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     timeout_vote_faults.begin(),
                                     timeout_vote_faults.end());
    }
    if (config.invalid_tc_proposal_detection_enabled) {
      std::vector<StrongFaultRecord> invalid_tc_faults =
          DetectInvalidTcProposalFaults(input.invalid_tc_proposal_evidence);
      candidate.strong_faults.insert(candidate.strong_faults.end(),
                                     invalid_tc_faults.begin(),
                                     invalid_tc_faults.end());
    }
    if (config.conflicting_qc_detection_enabled) {
      std::vector<StrongFaultRecord> conflicting_qc_faults =
          DetectConflictingQcFaults(input.verified_qc_artifact_evidence,
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

  candidate.leader_weights = LeaderWeightsForCandidate(
      candidate.validators, config, input.current_leader_weights);
  candidate.leader_selection_version = 1;
  candidate.leader_eligible_min_weight = config.leader_eligible_min_weight;

  RecomputeReputationCandidateRoots(&candidate);
  return candidate;
}


}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
