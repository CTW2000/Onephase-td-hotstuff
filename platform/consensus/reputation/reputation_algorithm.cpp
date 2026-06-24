#include "platform/consensus/reputation/reputation_algorithm.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <numeric>
#include <set>
#include <tuple>
#include <utility>

#include "platform/consensus/reputation/reputation_roots.h"
#include "platform/consensus/reputation/reputation_utils.h"
#include "platform/consensus/reputation/soft_reputation.h"
#include "platform/consensus/reputation/strong_fault_detector.h"

namespace resdb {
namespace consensus {
namespace reputation {
namespace {

constexpr const char* kAlgorithmBayesV4 = "bayes_v4";
constexpr int64_t kCarryoverDecayWeightGap = 10;
constexpr int kHealthyCatchUpScore = 40;
constexpr int kLeaderDiversityGateThreshold = 50;
constexpr int kLeaderSoftFaultScoreThreshold = 70;
constexpr int kDebtNoRecoveryBelow = 30;
constexpr uint64_t kMinNoCertifiedLeaderOpportunities = 3;
constexpr uint64_t kMinFirstWindowSoftDecayOpportunities = 4;
constexpr uint64_t kVoteBetaCounterScale = 1000;
constexpr int kFactorScale = 1000;
constexpr int kFormulaFullReputationScore = kHealthyCatchUpScore;
constexpr uint64_t kFourFactorDenominator = 1000000000000ULL;

struct CoreEvidenceEvent {
  int view_or_round = 0;
  int leader_id = 0;
  std::string artifact_digest;
  std::string signer_bitmap;
  std::string available_signer_bitmap;
  OutcomeClass outcome_class = OutcomeClass::kNone;
};

int LeaderSignerDiversityGateScore(int weighted_effective_signer_score,
                                   int repeated_signer_concentration_score,
                                   int weighted_jaccard_variation_score) {
  // F_div is a soft recovery gate built from the paper's signer-diversity
  // terms. Low scores reduce recovery credit gradually; they do not classify a
  // Byzantine fault or directly rewrite leader weights.
  return std::max(
      0, std::min(100,
                  std::min(weighted_effective_signer_score,
                           std::min(repeated_signer_concentration_score,
                                    weighted_jaccard_variation_score))));
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
    case StrongFaultType::kConflictingQc:
      return "conflicting_qc";
    case StrongFaultType::kUnknown:
      return "unknown";
  }
  return "unknown";
}

int ClampPermilleFactor(int value) {
  return std::max(0, std::min(10000, value));
}

std::pair<int, int> NormalizedFactorRange(int min_per_mille,
                                          int max_per_mille) {
  int min_factor = ClampPermilleFactor(min_per_mille);
  int max_factor = ClampPermilleFactor(max_per_mille);
  if (min_factor <= 0) {
    min_factor = kFactorScale;
  }
  if (max_factor <= 0) {
    max_factor = kFactorScale;
  }
  if (min_factor > max_factor) {
    std::swap(min_factor, max_factor);
  }
  return {min_factor, max_factor};
}

uint64_t Mix64(uint64_t value) {
  value ^= value >> 30;
  value *= 0xbf58476d1ce4e5b9ULL;
  value ^= value >> 27;
  value *= 0x94d049bb133111ebULL;
  value ^= value >> 31;
  return value;
}

int DeterministicPermilleFactor(int validator_id, uint64_t seed,
                                int min_per_mille, int max_per_mille) {
  const auto range = NormalizedFactorRange(min_per_mille, max_per_mille);
  const int span = range.second - range.first + 1;
  if (span <= 1) {
    return range.first;
  }
  const uint64_t mixed = Mix64(seed ^ (static_cast<uint64_t>(validator_id) *
                                      0x9e3779b97f4a7c15ULL));
  return range.first + static_cast<int>(mixed % static_cast<uint64_t>(span));
}

bool HasConfiguredFactorRange(int min_per_mille, int max_per_mille) {
  return min_per_mille > 0 || max_per_mille > 0;
}

int FactorFromInputOrDefault(const std::vector<int>& input_factors,
                             int validator_index, uint64_t seed,
                             int min_per_mille, int max_per_mille,
                             int fallback_per_mille) {
  if (validator_index >= 0 &&
      validator_index < static_cast<int>(input_factors.size()) &&
      input_factors[validator_index] > 0) {
    return ClampPermilleFactor(input_factors[validator_index]);
  }
  if (!HasConfiguredFactorRange(min_per_mille, max_per_mille)) {
    return ClampPermilleFactor(fallback_per_mille);
  }
  return DeterministicPermilleFactor(validator_index + 1, seed, min_per_mille,
                                    max_per_mille);
}


int StakePowerFactorPerMille(int stake_factor_per_mille,
                             int tau_per_mille) {
  const int bounded_stake = ClampPermilleFactor(stake_factor_per_mille);
  if (tau_per_mille <= 0) {
    return kFactorScale;
  }
  if (tau_per_mille == kFactorScale) {
    return bounded_stake;
  }
  const long double stake =
      static_cast<long double>(bounded_stake) / kFactorScale;
  const long double tau = static_cast<long double>(tau_per_mille) / kFactorScale;
  const long double scaled = std::pow(stake, tau) * kFactorScale;
  if (!(scaled >= 0.0L)) {
    return 0;
  }
  if (scaled > 10000.0L) {
    return 10000;
  }
  return static_cast<int>(scaled + 0.5L);
}

int WeightRatioFactorPerMille(int64_t numerator_weight,
                                int64_t denominator_weight) {
  if (denominator_weight <= 0) {
    return kFactorScale;
  }
  return ClampPermilleFactor(static_cast<int>(RoundedDivide(
      static_cast<uint64_t>(std::max<int64_t>(0, numerator_weight)) *
          kFactorScale,
      static_cast<uint64_t>(denominator_weight))));
}

int ScoreFactorPerMille(int recovery_score) {
  const int bounded_score = std::max(0, std::min(100, recovery_score));
  if (bounded_score >= kFormulaFullReputationScore) {
    return kFactorScale;
  }
  return bounded_score * 10;
}

int FormulaReputationFactorPerMille(int recovery_score,
                                    int64_t current_weight,
                                    int64_t max_weight,
                                    int64_t smoothed_next_weight,
                                    bool force_score_factor) {
  (void)current_weight;
  const int score_factor = ScoreFactorPerMille(recovery_score);
  const int smooth_factor = WeightRatioFactorPerMille(smoothed_next_weight,
                                                      max_weight);
  if (force_score_factor) {
    return std::min(score_factor, smooth_factor);
  }
  return smooth_factor;
}

int64_t MultiplicativeWeightFromFactorValues(
    const ReputationConfig& config, int stake_power_factor_per_mille,
    int identity_factor_per_mille, int reputation_factor_per_mille,
    int direct_penalty_factor_per_mille) {
  unsigned __int128 numerator =
      static_cast<unsigned __int128>(std::max<int64_t>(0, config.max_weight));
  numerator *= static_cast<uint64_t>(
      ClampPermilleFactor(stake_power_factor_per_mille));
  numerator *=
      static_cast<uint64_t>(ClampPermilleFactor(identity_factor_per_mille));
  numerator *=
      static_cast<uint64_t>(ClampPermilleFactor(reputation_factor_per_mille));
  numerator *= static_cast<uint64_t>(
      ClampPermilleFactor(direct_penalty_factor_per_mille));
  numerator += kFourFactorDenominator / 2;
  const unsigned __int128 raw = numerator / kFourFactorDenominator;
  const int64_t bounded_raw =
      raw > static_cast<unsigned __int128>(std::numeric_limits<int64_t>::max())
          ? std::numeric_limits<int64_t>::max()
          : static_cast<int64_t>(raw);
  return ClampWeight(bounded_raw, config);
}

int64_t MultiplicativeWeightFromFactors(
    const ValidatorReputation& validator, const ReputationConfig& config) {
  return MultiplicativeWeightFromFactorValues(
      config, validator.stake_power_factor_per_mille,
      validator.identity_factor_per_mille,
      validator.reputation_factor_per_mille,
      validator.direct_penalty_factor_per_mille);
}

int64_t WeightWithFixedFactorsForBehaviorWeight(
    const ValidatorReputation& validator, const ReputationConfig& config,
    int64_t behavior_weight, int direct_penalty_factor_per_mille) {
  const int64_t clamped_behavior_weight = ClampWeight(behavior_weight, config);
  if (!config.multiplicative_weight_formula_enabled) {
    return clamped_behavior_weight;
  }
  return MultiplicativeWeightFromFactorValues(
      config, validator.stake_power_factor_per_mille,
      validator.identity_factor_per_mille,
      WeightRatioFactorPerMille(clamped_behavior_weight, config.max_weight),
      direct_penalty_factor_per_mille);
}

int DirectPenaltyFactorForWeight(int64_t pre_penalty_weight,
                                 int64_t penalty_weight) {
  if (pre_penalty_weight <= 0) {
    return kFactorScale;
  }
  const int64_t bounded_penalty = std::max<int64_t>(0, penalty_weight);
  return ClampPermilleFactor(static_cast<int>(RoundedDivide(
      static_cast<uint64_t>(bounded_penalty) * kFactorScale,
      static_cast<uint64_t>(pre_penalty_weight))));
}

void InitializeFormulaFactors(ValidatorReputation* validator, int index,
                              const ReputationWindowInput& input,
                              const ReputationConfig& config) {
  if (validator == nullptr) {
    return;
  }
  validator->stake_factor_per_mille = FactorFromInputOrDefault(
      input.stake_factors_per_mille, index, config.stake_factor_seed,
      config.stake_factor_min_per_mille, config.stake_factor_max_per_mille,
      kFactorScale);
  validator->stake_power_factor_per_mille = StakePowerFactorPerMille(
      validator->stake_factor_per_mille, config.stake_exponent_tau_per_mille);
  validator->identity_factor_per_mille = FactorFromInputOrDefault(
      input.identity_factors_per_mille, index, config.identity_factor_seed,
      config.identity_factor_min_per_mille,
      config.identity_factor_max_per_mille, kFactorScale);
  validator->reputation_factor_per_mille = kFactorScale;
  validator->direct_penalty_factor_per_mille = kFactorScale;
}

void MaybeApplyMultiplicativeWeightFormula(
    ValidatorReputation* validator, const ReputationConfig& config,
    int recovery_score, int64_t smoothed_next_weight,
    bool force_score_factor) {
  if (validator == nullptr) {
    return;
  }
  validator->reputation_factor_per_mille = FormulaReputationFactorPerMille(
      recovery_score, validator->current_weight, config.max_weight,
      smoothed_next_weight, force_score_factor);
  validator->direct_penalty_factor_per_mille = kFactorScale;
  if (config.multiplicative_weight_formula_enabled) {
    validator->next_weight = MultiplicativeWeightFromFactors(*validator, config);
  }
}

int BoundedReputationScore(int score) {
  return std::max(0, std::min(100, score));
}

int VoteReputationScore(const ValidatorReputation& validator) {
  return BoundedReputationScore(validator.vote_score);
}

int64_t ReputationTargetWeightForScore(int score,
                                       const ReputationConfig& config) {
  const int bounded_score = std::max(0, std::min(100, score));
  const int64_t min_weight = ClampWeight(config.min_weight, config);
  const int64_t max_weight = ClampWeight(config.max_weight, config);
  if (bounded_score <= 0 || max_weight <= min_weight) {
    return min_weight;
  }
  if (bounded_score >= 100) {
    return max_weight;
  }
  const int64_t range = max_weight - min_weight;
  const int64_t curved =
      (range * bounded_score * bounded_score + 5000) / 10000;
  return ClampWeight(min_weight + curved, config);
}

int DiminishingBonusCreditForScore(int score, int64_t current_weight,
                                   int64_t target_weight,
                                   const ReputationConfig& config,
                                   uint64_t vote_opportunities,
                                   uint64_t leader_opportunities) {
  if (target_weight <= current_weight || config.bonus_per_epoch <= 0) {
    return 0;
  }
  const int bounded_score = std::max(0, std::min(100, score));
  if (bounded_score < kHealthyCatchUpScore) {
    return 0;
  }
  const int64_t min_weight = ClampWeight(config.min_weight, config);
  const int64_t max_weight = ClampWeight(config.max_weight, config);
  const int64_t range = std::max<int64_t>(1, max_weight - min_weight);
  const int64_t progress =
      ((ClampWeight(current_weight, config) - min_weight) * 100) / range;
  if ((progress >= 90 && bounded_score < 100) ||
      (progress >= 80 && bounded_score < 98) ||
      (progress >= 70 && bounded_score < 95)) {
    return 0;
  }
  if (progress >= 90) {
    const uint64_t required =
        std::max<uint64_t>(config.min_decay_opportunities, 1) * 4;
    if (vote_opportunities + leader_opportunities < required) {
      return 0;
    }
  }
  const int64_t headroom = target_weight - current_weight;
  const int64_t raw =
      (static_cast<int64_t>(config.bonus_per_epoch) * bounded_score *
           std::max<int64_t>(1, max_weight - current_weight) +
       100 * range - 1) /
      (100 * range);
  return static_cast<int>(std::max<int64_t>(1, std::min(headroom, raw)));
}


int LeaderRecoveryCreditForScore(int score, int max_recovery_per_epoch) {
  if (max_recovery_per_epoch <= 0) {
    return 0;
  }
  const int bounded_score = std::max(0, std::min(100, score));
  if (bounded_score >= 95) {
    return max_recovery_per_epoch;
  }
  return static_cast<int>((static_cast<int64_t>(bounded_score) *
                           max_recovery_per_epoch) /
                          100);
}

struct BehaviorWeightUpdate {
  int64_t target_weight = 0;
  int decay_applied = 0;
  int recovery_credit = 0;
  int bonus_credit = 0;
  int64_t next_weight = 0;
};

BehaviorWeightUpdate ComputeBehaviorWeightUpdate(
    int score, int64_t current_weight, const ReputationConfig& config,
    bool has_behavior_evidence, bool carryover_decay,
    uint64_t primary_opportunities, uint64_t secondary_opportunities,
    bool apply_recovery_credit) {
  BehaviorWeightUpdate update;
  const int bounded_score = std::max(0, std::min(100, score));
  const int64_t current = ClampWeight(current_weight, config);
  update.target_weight = ReputationTargetWeightForScore(bounded_score, config);
  if (current >= config.max_weight && bounded_score >= 90) {
    update.target_weight = ClampWeight(config.max_weight, config);
  }
  update.next_weight = current;
  if (!has_behavior_evidence) {
    return update;
  }
  if (current > update.target_weight) {
    const int64_t available_decay =
        std::max<int64_t>(0, current - config.min_weight);
    update.decay_applied = static_cast<int>(std::min<int64_t>(
        available_decay,
        std::min<int64_t>(config.decay_per_epoch,
                          current - update.target_weight)));
    if (apply_recovery_credit) {
      update.recovery_credit = std::min(
          update.decay_applied,
          LeaderRecoveryCreditForScore(bounded_score,
                                       config.max_recovery_per_epoch));
    }
  } else if (current < update.target_weight && !carryover_decay) {
    update.bonus_credit = DiminishingBonusCreditForScore(
        bounded_score, current, update.target_weight, config,
        primary_opportunities, secondary_opportunities);
  }
  update.next_weight = ClampWeight(current - update.decay_applied +
                                       update.recovery_credit +
                                       update.bonus_credit,
                                   config);
  return update;
}

int VoteBetaDecayPerMille(const ReputationConfig& config) {
  return std::max(0, std::min(1000, config.vote_beta_decay_per_mille));
}

int LeaderDirichletDecayPerMille(const ReputationConfig& config) {
  return std::max(0, std::min(1000, config.leader_dirichlet_decay_per_mille));
}

uint64_t ScaledObservationCount(uint64_t count) {
  return count * kVoteBetaCounterScale;
}

uint64_t DecayLeaderDirichletCounter(uint64_t counter,
                                     const ReputationConfig& config) {
  const int decay_per_mille = LeaderDirichletDecayPerMille(config);
  if (counter == 0 || decay_per_mille == 0) {
    return 0;
  }
  return (counter * static_cast<uint64_t>(decay_per_mille) +
          kVoteBetaCounterScale / 2) /
         kVoteBetaCounterScale;
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
  const uint64_t decayed_success =
      DecayVoteBetaCounter(validator->vote_beta_success, config);
  const uint64_t decayed_failure =
      DecayVoteBetaCounter(validator->vote_beta_failure, config);
  validator->vote_beta_success =
      decayed_success + ScaledObservationCount(validator->inclusions);
  validator->vote_beta_failure = decayed_failure + ScaledObservationCount(misses);
  if (validator->inclusions > 0 && misses == 0 && decayed_failure == 0) {
    validator->vote_score = 100;
    return;
  }
  validator->vote_score = VoteScoreFromBetaCounters(
      validator->vote_beta_success, validator->vote_beta_failure);
}

int UpdateLeaderDirichletCounters(
    ValidatorReputation* validator, const ReputationConfig& config,
    uint64_t certify_only_observations, uint64_t timeout_observations) {
  if (validator == nullptr) {
    return 100;
  }
  const uint64_t commit_observations = validator->leader_certified_count;
  uint64_t timeout_total = timeout_observations;
  const uint64_t observed_outcomes =
      commit_observations + certify_only_observations + timeout_total;
  if (validator->leader_opportunity_count > observed_outcomes) {
    timeout_total += validator->leader_opportunity_count - observed_outcomes;
  }
  validator->leader_dirichlet_commit =
      DecayLeaderDirichletCounter(validator->leader_dirichlet_commit, config) +
      ScaledObservationCount(commit_observations);
  validator->leader_dirichlet_certify_only =
      DecayLeaderDirichletCounter(validator->leader_dirichlet_certify_only,
                                  config) +
      ScaledObservationCount(certify_only_observations);
  validator->leader_dirichlet_timeout =
      DecayLeaderDirichletCounter(validator->leader_dirichlet_timeout, config) +
      ScaledObservationCount(timeout_total);
  LeaderDirichletCounter counter;
  counter.commit = validator->leader_dirichlet_commit;
  counter.certify_only = validator->leader_dirichlet_certify_only;
  counter.timeout = validator->leader_dirichlet_timeout;
  if (validator->leader_opportunity_count > 0 &&
      validator->leader_certified_count ==
          validator->leader_opportunity_count &&
      counter.certify_only == 0 && counter.timeout == 0) {
    return 100;
  }
  return LeaderDirichletScore(counter, config);
}

void CountLeaderOutcomeOpportunity(
    int view_or_round, int leader, bool has_scheduled_leader_counts,
    std::set<std::pair<int, int>>* seen_leader_opportunities,
    ReputationCandidate* candidate) {
  if (candidate == nullptr || seen_leader_opportunities == nullptr ||
      leader < 1 || leader > candidate->total_replicas) {
    return;
  }
  if (!has_scheduled_leader_counts &&
      seen_leader_opportunities->insert({view_or_round, leader}).second) {
    ++candidate->validators[leader - 1].leader_opportunity_count;
  }
}

bool HasLeaderRecoveryEvidence(const ValidatorReputation& validator,
                               const ReputationConfig& config) {
  if (!config.leader_recovery_enabled) {
    return false;
  }
  const bool has_repeated_uncertified_leader_opportunities =
      validator.leader_opportunity_count >= kMinNoCertifiedLeaderOpportunities &&
      validator.leader_certified_count == 0 && validator.leader_score < 50;
  return validator.leader_score < kLeaderSoftFaultScoreThreshold &&
         (validator.leader_opportunity_count >= config.min_leader_opportunities ||
          has_repeated_uncertified_leader_opportunities);
}

bool HasDirectLeaderProfileCap(const ValidatorReputation& validator,
                               const ReputationConfig& config) {
  if (!config.leader_recovery_enabled || validator.strong_fault_count > 0) {
    return false;
  }
  return validator.leader_certified_count > 0 &&
         validator.leader_diversity_score < kLeaderDiversityGateThreshold;
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
  return !HasDirectLeaderProfileCap(validator, config) &&
         HasLeaderRecoveryEvidence(validator, config) &&
         validator.next_weight <= config.leader_eligible_min_weight;
}

int LeaderSelectionScore(const ValidatorReputation& validator,
                         const ReputationConfig& config) {
  if (!config.leader_recovery_enabled) {
    return 100;
  }
  const bool has_repeated_uncertified_leader_opportunities =
      validator.leader_opportunity_count >= kMinNoCertifiedLeaderOpportunities &&
      validator.leader_certified_count == 0 && validator.leader_score < 50;
  const bool has_low_diversity_leader_evidence =
      validator.leader_certified_count > 0 &&
      validator.leader_diversity_score < kLeaderDiversityGateThreshold;
  const bool has_leader_score_evidence =
      validator.leader_opportunity_count >= config.min_leader_opportunities ||
      has_repeated_uncertified_leader_opportunities ||
      has_low_diversity_leader_evidence;
  if (!has_leader_score_evidence) {
    return 100;
  }

  int score = validator.leader_score;
  if (!HasDirectLeaderProfileCap(validator, config) &&
      score >= kLeaderSoftFaultScoreThreshold) {
    return 100;
  }
  if (HasDirectLeaderProfileCap(validator, config)) {
    score = std::max<int>(score, static_cast<int>(std::max<int64_t>(
                                     config.min_weight,
                                     std::min<int64_t>(
                                         config.leader_diversity_soft_min_weight,
                                         config.max_weight))));
  }
  return std::max(0, std::min(100, score));
}

bool HasLeaderBehaviorWeightEvidence(const ValidatorReputation& validator,
                                     const ReputationConfig& config) {
  if (!config.leader_recovery_enabled) {
    return false;
  }
  if (HasDirectLeaderProfileCap(validator, config)) {
    return true;
  }
  if (validator.leader_opportunity_count >= kMinNoCertifiedLeaderOpportunities &&
      validator.leader_certified_count == 0) {
    return true;
  }
  if (validator.leader_opportunity_count == 0 &&
      validator.leader_certified_count == 0) {
    return false;
  }
  return LeaderSelectionScore(validator, config) >= 100;
}

BehaviorWeightUpdate LeaderBehaviorWeightUpdateForScore(
    const ValidatorReputation& validator, const ReputationConfig& config,
    int64_t current_leader_weight) {
  return ComputeBehaviorWeightUpdate(
      LeaderSelectionScore(validator, config), current_leader_weight, config,
      HasLeaderBehaviorWeightEvidence(validator, config),
      /*carryover_decay=*/false,
      /*primary_opportunities=*/0, validator.leader_opportunity_count,
      /*apply_recovery_credit=*/true);
}

std::vector<int64_t> LeaderWeightsForCandidate(
    const std::vector<ValidatorReputation>& validators,
    const ReputationConfig& config,
    const std::vector<int64_t>& current_leader_weights) {
  std::vector<int64_t> leader_weights;
  leader_weights.reserve(validators.size());
  const int64_t ineligible_leader_weight = ClampWeight(config.min_weight, config);
  for (size_t i = 0; i < validators.size(); ++i) {
    const ValidatorReputation& validator = validators[i];
    const int64_t current_leader_weight =
        i < current_leader_weights.size() ? current_leader_weights[i]
                                          : validator.current_weight;
    int64_t leader_weight = config.leader_recovery_enabled
                                ? ClampWeight(current_leader_weight, config)
                                : ClampWeight(validator.next_weight, config);
    if (IsLeaderWeightIneligible(validator, config, current_leader_weight)) {
      leader_weights.push_back(ineligible_leader_weight);
      continue;
    }
    if (config.leader_recovery_enabled) {
      const BehaviorWeightUpdate leader_update =
          LeaderBehaviorWeightUpdateForScore(validator, config,
                                             current_leader_weight);
      leader_weight = leader_update.next_weight;
    }
    if (HasDirectLeaderProfileCap(validator, config)) {
      const int64_t leader_profile_cap = ClampWeight(
          LeaderSelectionScore(validator, config), config);
      leader_weight = std::min<int64_t>(leader_weight, leader_profile_cap);
      if (validator.vote_score >= kHealthyCatchUpScore &&
          current_leader_weight >= leader_profile_cap) {
        leader_weight = std::max<int64_t>(leader_weight, leader_profile_cap);
      }
    }
    if (config.leader_recovery_enabled) {
      leader_weight = WeightWithFixedFactorsForBehaviorWeight(
          validator, config, leader_weight, kFactorScale);
    }
    leader_weights.push_back(ClampWeight(leader_weight, config));
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
         !input.verified_qc_artifact_evidence.empty();
}

bool UseCoreOnlyFastPath(const ReputationWindowInput& input,
                         const ReputationConfig& config) {
  return !config.multiplicative_weight_formula_enabled &&
         !config.leader_recovery_enabled && !config.peertrust_enabled &&
         !config.strong_fault_enabled &&
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

bool HasBroadAvailableSignerCoverage(
    const std::vector<int>& available_signers, int total_replicas) {
  const uint64_t threshold = WindowFairAvailableSignerThreshold(total_replicas);
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
  std::vector<uint64_t> quorum_unavailable_count(
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
      const bool quorum_available_signer_evidence =
          HasQuorumAvailableSignerCoverage(available_signers, total_replicas);
      const bool broad_available_signer_evidence =
          HasBroadAvailableSignerCoverage(available_signers, total_replicas);
      if (quorum_available_signer_evidence) {
        const std::set<int> available_set(available_signers.begin(),
                                          available_signers.end());
        for (int validator_id = 1; validator_id <= total_replicas;
             ++validator_id) {
          if (available_set.find(validator_id) == available_set.end()) {
            ++quorum_unavailable_count[validator_id - 1];
          }
        }
      }
      if (broad_available_signer_evidence) {
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
      if (broad_available_signer_evidence) {
        for (int signer : available_signers) {
          if (signer >= 1 && signer <= total_replicas &&
              signer_set.find(signer) == signer_set.end()) {
            ++available_not_selected_count[signer - 1];
          }
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
    const uint64_t quorum_unavailable_samples =
        idx >= 0 && idx < static_cast<int>(quorum_unavailable_count.size())
            ? quorum_unavailable_count[idx]
            : 0;
    const uint64_t quorum_participation_samples =
        validator.inclusions + quorum_unavailable_samples;
    const bool near_zero_quorum_participation =
        quorum_unavailable_samples > 0 && quorum_participation_samples > 0 &&
        validator.inclusions * 100 <= quorum_participation_samples * 10;
    const uint64_t quorum_unavailable_opportunities =
        broad_available_fair_opportunities == 0 &&
                near_zero_quorum_participation
            ? quorum_unavailable_samples
            : 0;
    validator.opportunities =
        std::max(std::max(direct_opportunities,
                          adjusted_broad_available_fair_opportunities),
                 quorum_unavailable_opportunities) +
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
    const bool had_vote_history =
        validator.vote_beta_success > 0 || validator.vote_beta_failure > 0;
    const bool had_leader_history =
        validator.leader_dirichlet_commit > 0 ||
        validator.leader_dirichlet_certify_only > 0 ||
        validator.leader_dirichlet_timeout > 0;
    UpdateVoteBetaCounters(&validator, config);
    const int dirichlet_leader_score =
        UpdateLeaderDirichletCounters(&validator, config, 0, 0);
    validator.leader_score =
        config.leader_dirichlet_scoring_enabled
            ? dirichlet_leader_score
            : LeaderCertifiedScore(validator.leader_certified_count,
                                   validator.leader_opportunity_count);
    if (!had_leader_history && validator.current_weight >= config.max_weight &&
        validator.leader_opportunity_count <
            kMinFirstWindowSoftDecayOpportunities) {
      validator.leader_score = 100;
    }
    int recovery_score = VoteReputationScore(validator);
    const bool carryover_decay =
        validator.opportunities == 0 &&
        validator.current_weight + kCarryoverDecayWeightGap <
            mean_current_weight;
    const bool near_fair_vote =
        HasNearFairInclusion(validator.inclusions, validator.opportunities);
    const bool first_window_partial_vote_sample =
        !had_vote_history && validator.current_weight >= config.max_weight &&
        validator.opportunities >= config.min_decay_opportunities &&
        validator.inclusions > 0 && !near_fair_vote;
    const bool first_window_sparse_max_weight_sample =
        !had_vote_history && validator.current_weight >= config.max_weight &&
        validator.opportunities < kMinFirstWindowSoftDecayOpportunities;
    const bool validator_has_enough_decay_evidence =
        (validator.opportunities >= config.min_decay_opportunities ||
         carryover_decay) &&
        !first_window_partial_vote_sample &&
        !first_window_sparse_max_weight_sample;
    const int64_t available_decay =
        std::max<int64_t>(0, validator.current_weight - config.min_weight);
    int64_t target_weight = ReputationTargetWeightForScore(recovery_score, config);
    if (validator.current_weight >= config.max_weight && recovery_score >= 90) {
      target_weight = ClampWeight(config.max_weight, config);
    }
    validator.decay_applied = 0;
    validator.recovery_credit = 0;
    validator.bonus_credit = 0;
    if (validator_has_enough_decay_evidence &&
        validator.current_weight > target_weight) {
      validator.decay_applied = static_cast<int>(std::min<int64_t>(
          available_decay,
          std::min<int64_t>(config.decay_per_epoch,
                            validator.current_weight - target_weight)));
    } else if (validator_has_enough_decay_evidence &&
               validator.current_weight < target_weight && !carryover_decay) {
      validator.bonus_credit = DiminishingBonusCreditForScore(
          recovery_score, validator.current_weight, target_weight, config,
          validator.opportunities, validator.leader_opportunity_count);
    }
    validator.reputation_score = std::max(0, std::min(100, recovery_score));
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
  candidate.leader_epoch_start_view = input.activation_view;
  candidate.leader_epoch_views = input.leader_epoch_views;
  candidate.validators.resize(std::max(total_replicas, 0));
  const bool has_scheduled_leader_counts =
      input.scheduled_leader_counts.size() >=
      static_cast<size_t>(std::max(total_replicas, 0));
  for (int i = 0; i < total_replicas; ++i) {
    ValidatorReputation& validator = candidate.validators[i];
    validator.validator_id = i + 1;
    validator.current_weight = weights[i];
    validator.next_weight = weights[i];
    InitializeFormulaFactors(&validator, i, input, config);
    if (has_scheduled_leader_counts) {
      validator.leader_opportunity_count = input.scheduled_leader_counts[i];
    }
    if (i < static_cast<int>(input.prior_vote_beta_counters.size())) {
      validator.vote_beta_success = input.prior_vote_beta_counters[i].success;
      validator.vote_beta_failure = input.prior_vote_beta_counters[i].failure;
    }
    if (i < static_cast<int>(input.prior_leader_dirichlet_counters.size())) {
      validator.leader_dirichlet_commit =
          input.prior_leader_dirichlet_counters[i].commit;
      validator.leader_dirichlet_certify_only =
          input.prior_leader_dirichlet_counters[i].certify_only;
      validator.leader_dirichlet_timeout =
          input.prior_leader_dirichlet_counters[i].timeout;
    }
    if (config.peertrust_enabled &&
        i < static_cast<int>(input.prior_peertrust_leader_debt.size())) {
      validator.peertrust_leader_debt = std::max(
          0, std::min(config.peertrust_debt_max,
                      input.prior_peertrust_leader_debt[i]));
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
  std::vector<uint64_t> leader_certify_only_observations(
      std::max(total_replicas, 0), 0);
  std::vector<uint64_t> leader_timeout_observations(
      std::max(total_replicas, 0), 0);

  uint64_t legacy_selected_signer_slots = 0;
  uint64_t legacy_certificate_event_count = 0;
  uint64_t broad_available_signer_slots = 0;
  uint64_t broad_available_event_count = 0;
  uint64_t certificate_event_count = 0;
  std::vector<uint64_t> signer_opportunity_count(
      std::max(total_replicas, 0), 0);
  std::vector<uint64_t> available_not_selected_count(
      std::max(total_replicas, 0), 0);
  std::vector<uint64_t> quorum_unavailable_count(
      std::max(total_replicas, 0), 0);
  std::set<int> window_quorum_available_signers;
  std::set<std::pair<int, std::string>> seen_certificates;
  std::set<std::pair<int, int>> seen_leader_opportunities;
  for (const CoreEvidenceEvent& event : ordered_events) {
    const int leader = event.leader_id;
    const int diversity_leader = leader;

    if (leader >= 1 && leader <= total_replicas &&
        event.signer_bitmap.empty()) {
      if (event.outcome_class == OutcomeClass::kCommitted) {
        CountLeaderOutcomeOpportunity(event.view_or_round, leader,
                                      has_scheduled_leader_counts,
                                      &seen_leader_opportunities, &candidate);
        ++candidate.validators[leader - 1].leader_certified_count;
        continue;
      }
      if (event.outcome_class == OutcomeClass::kCertifyOnly) {
        CountLeaderOutcomeOpportunity(event.view_or_round, leader,
                                      has_scheduled_leader_counts,
                                      &seen_leader_opportunities, &candidate);
        ++leader_certify_only_observations[leader - 1];
        continue;
      }
      if (event.outcome_class == OutcomeClass::kTimeoutOrViewChange) {
        if (config.leader_timeout_outcome_enabled) {
          CountLeaderOutcomeOpportunity(event.view_or_round, leader,
                                        has_scheduled_leader_counts,
                                        &seen_leader_opportunities, &candidate);
          ++leader_timeout_observations[leader - 1];
        }
        continue;
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
    std::set<int> signer_set(signers.begin(), signers.end());
    if (has_available_signer_evidence) {
      const bool quorum_available_signer_evidence =
          HasQuorumAvailableSignerCoverage(available_signers, total_replicas);
      const bool broad_available_signer_evidence =
          HasBroadAvailableSignerCoverage(available_signers, total_replicas);
      if (quorum_available_signer_evidence) {
        const std::set<int> available_set(available_signers.begin(),
                                          available_signers.end());
        for (int validator_id = 1; validator_id <= total_replicas;
             ++validator_id) {
          if (available_set.find(validator_id) == available_set.end()) {
            ++quorum_unavailable_count[validator_id - 1];
          }
        }
      }
      if (broad_available_signer_evidence) {
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
      if (broad_available_signer_evidence) {
        for (int signer : available_signers) {
          if (signer >= 1 && signer <= total_replicas &&
              signer_set.find(signer) == signer_set.end()) {
            ++available_not_selected_count[signer - 1];
          }
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

  const uint64_t fair_opportunities =
      config.multiplicative_weight_formula_enabled
          ? 0
          : FairExpectedSignerOpportunities(legacy_selected_signer_slots,
                                            total_replicas,
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
    const uint64_t quorum_unavailable_samples =
        idx >= 0 && idx < static_cast<int>(quorum_unavailable_count.size())
            ? quorum_unavailable_count[idx]
            : 0;
    const uint64_t quorum_participation_samples =
        validator.inclusions + quorum_unavailable_samples;
    const bool near_zero_quorum_participation =
        quorum_unavailable_samples > 0 && quorum_participation_samples > 0 &&
        validator.inclusions * 100 <= quorum_participation_samples * 10;
    const uint64_t quorum_unavailable_opportunities =
        broad_available_fair_opportunities == 0 &&
                near_zero_quorum_participation
            ? quorum_unavailable_samples
            : 0;
    validator.opportunities =
        std::max(std::max(direct_opportunities,
                          adjusted_broad_available_fair_opportunities),
                 quorum_unavailable_opportunities) +
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
  std::vector<bool> credible_available_signer_evidence(
      candidate.validators.size(), false);
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
    const int target_coverage_score =
        has_public_available_signer_evidence
            ? std::min(available_signer_coverage_score,
                       average_available_signer_set_coverage_score)
            : 100;
    const bool has_credible_available_signer_evidence =
        has_public_available_signer_evidence && target_coverage_score >= 85;
    const bool narrow_public_target =
        has_credible_available_signer_evidence && target_coverage_score < 95;
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
    int raw_diversity_score = diversity_score;
    if (has_credible_available_signer_evidence && had_broader_public_choice) {
      const int weighted_jaccard_variation_score =
          has_repeated_leader_signer_group ? variation_score : 100;
      raw_diversity_score = LeaderSignerDiversityGateScore(
          diversity_score, frequency_balance_score,
          weighted_jaccard_variation_score);
    } else if (narrow_public_target) {
      raw_diversity_score = target_coverage_recovery_score;
    } else if (has_public_available_signer_evidence) {
      raw_diversity_score = 100;
    }
    raw_leader_diversity_scores[idx] = raw_diversity_score;
    public_target_coverage_scores[idx] = target_coverage_score;
    credible_available_signer_evidence[idx] =
        has_credible_available_signer_evidence;
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
    const int average_signer_coverage_score =
        idx < static_cast<int>(signer_set_coverage_sum.size()) &&
                diversity_count[idx] > 0
            ? RoundedDivide(signer_set_coverage_sum[idx],
                            diversity_count[idx])
            : signer_coverage_score;
    const int average_available_coverage_score =
        idx < static_cast<int>(available_signer_set_coverage_sum.size()) &&
                diversity_count[idx] > 0
            ? RoundedDivide(available_signer_set_coverage_sum[idx],
                            diversity_count[idx])
            : available_coverage_score;
    const bool has_broad_available_reviewer_set =
        !unique_available_signers_by_leader[idx].empty() &&
        available_coverage_score >= 95;
    const int frequency_balance_score =
        idx >= 0 && idx < static_cast<int>(signer_frequency_by_leader.size()) &&
                has_broad_available_reviewer_set
            ? SignerFrequencyBalanceScore(
                  signer_frequency_by_leader[idx],
                  unique_available_signers_by_leader[idx])
            : 100;
    const bool unique_selected_narrower_than_available =
        has_broad_available_reviewer_set &&
        signer_coverage_score + 10 < available_coverage_score;
    const bool per_qc_selected_narrower_than_available =
        has_broad_available_reviewer_set &&
        average_signer_coverage_score + 10 < average_available_coverage_score;
    const bool reviewer_frequency_concentrated = frequency_balance_score < 90;
    const bool selected_narrower_than_available =
        unique_selected_narrower_than_available ||
        (per_qc_selected_narrower_than_available &&
         reviewer_frequency_concentrated);
    peertrust_has_credible_public_choice[idx] =
        selected_narrower_than_available;
    const int reviewer_coverage_score =
        selected_narrower_than_available
            ? (unique_selected_narrower_than_available
                   ? signer_coverage_score
                   : frequency_balance_score)
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
    const int unique_reviewer_entropy_score = WeightedEffectiveDiversityScore(
        SetToOrderedVector(unique_signers_by_leader[idx]), weights,
        total_replicas);
    peertrust_reviewer_entropy_scores[idx] =
        has_broad_available_reviewer_set
            ? std::min(unique_reviewer_entropy_score, frequency_balance_score)
            : unique_reviewer_entropy_score;

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
        signer_coverage_score >= 95 ? 100
                                    : std::max(0, 100 - max_overlap_score);

    int max_reviewer_overuse = 1;
    for (int signer : unique_signers_by_leader[idx]) {
      if (signer >= 1 && signer <= total_replicas) {
        max_reviewer_overuse =
            std::max(max_reviewer_overuse, leaders_per_reviewer[signer - 1]);
      }
    }
    peertrust_reviewer_overuse_scores[idx] =
        signer_coverage_score >= 95
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

  int leader_diversity_baseline_score = 100;
  if (!diversity_baseline_samples.empty()) {
    std::sort(diversity_baseline_samples.begin(),
              diversity_baseline_samples.end());
    leader_diversity_baseline_score =
        diversity_baseline_samples[diversity_baseline_samples.size() / 2];
  }
  constexpr int kLeaderDiversityOutlierDeadband = 10;

  for (ValidatorReputation& validator : candidate.validators) {
    const bool had_vote_history =
        validator.vote_beta_success > 0 || validator.vote_beta_failure > 0;
    const bool had_leader_history =
        validator.leader_dirichlet_commit > 0 ||
        validator.leader_dirichlet_certify_only > 0 ||
        validator.leader_dirichlet_timeout > 0;
    UpdateVoteBetaCounters(&validator, config);
    const int idx = validator.validator_id - 1;
    const int raw_leader_diversity_score =
        idx >= 0 && idx < static_cast<int>(raw_leader_diversity_scores.size())
            ? raw_leader_diversity_scores[idx]
            : 100;
    const bool low_diversity_outlier =
        raw_leader_diversity_score + kLeaderDiversityOutlierDeadband <
        leader_diversity_baseline_score;
    const bool has_credible_available_signer_evidence =
        idx >= 0 &&
        idx < static_cast<int>(credible_available_signer_evidence.size()) &&
        credible_available_signer_evidence[idx];
    if (has_credible_available_signer_evidence &&
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
    const uint64_t certify_only_observations =
        idx >= 0 && idx < static_cast<int>(leader_certify_only_observations.size())
            ? leader_certify_only_observations[idx]
            : 0;
    const uint64_t timeout_observations =
        idx >= 0 && idx < static_cast<int>(leader_timeout_observations.size())
            ? leader_timeout_observations[idx]
            : 0;
    const int dirichlet_leader_score = UpdateLeaderDirichletCounters(
        &validator, config, certify_only_observations, timeout_observations);
    const int certified_leader_score = LeaderCertifiedScore(
        validator.leader_certified_count, leader_opportunities);
    const int raw_leader_score = config.leader_dirichlet_scoring_enabled
                                     ? dirichlet_leader_score
                                     : certified_leader_score;
    const bool has_enough_leader_opportunities =
        leader_opportunities >= config.min_leader_opportunities;
    const bool has_repeated_uncertified_leader_opportunities =
        leader_opportunities >= kMinNoCertifiedLeaderOpportunities &&
        validator.leader_certified_count == 0 && raw_leader_score < 50;
    const bool first_window_sparse_max_weight_leader_sample =
        !has_scheduled_leader_counts && !had_leader_history &&
        validator.current_weight >= config.max_weight &&
        leader_opportunities < kMinFirstWindowSoftDecayOpportunities;
    const bool has_leader_recovery_evidence =
        config.leader_recovery_enabled &&
        !first_window_sparse_max_weight_leader_sample &&
        (has_enough_leader_opportunities ||
         has_repeated_uncertified_leader_opportunities);
    validator.leader_score =
        has_leader_recovery_evidence ? raw_leader_score : 100;
    const bool low_leader_diversity_gate =
        config.leader_recovery_enabled && has_credible_available_signer_evidence &&
        validator.leader_certified_count > 0 &&
        validator.leader_diversity_score < kLeaderDiversityGateThreshold;
    if (low_leader_diversity_gate) {
      const int leader_diversity_soft_cap = static_cast<int>(
          std::max<int64_t>(config.min_weight,
                            std::min<int64_t>(
                                config.leader_diversity_soft_min_weight,
                                config.max_weight)));
      validator.leader_score =
          std::min(validator.leader_score, leader_diversity_soft_cap);
    }
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
          has_credible_available_signer_evidence &&
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
            validator.peertrust_score <= config.peertrust_debt_trigger_score ||
            validator.community_context_score <=
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
    int recovery_score = VoteReputationScore(validator);
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

    const bool carryover_decay =
        validator.opportunities == 0 &&
        validator.current_weight + kCarryoverDecayWeightGap <
            mean_current_weight;
    const bool near_fair_vote =
        HasNearFairInclusion(validator.inclusions, validator.opportunities);
    const bool first_window_partial_vote_sample =
        !had_vote_history && validator.current_weight >= config.max_weight &&
        validator.opportunities >= config.min_decay_opportunities &&
        validator.inclusions > 0 && !near_fair_vote;
    const bool first_window_sparse_max_weight_sample =
        !had_vote_history && validator.current_weight >= config.max_weight &&
        validator.opportunities < kMinFirstWindowSoftDecayOpportunities;
    const bool validator_has_enough_decay_evidence =
        (validator.opportunities >= config.min_decay_opportunities ||
         carryover_decay) &&
        !first_window_partial_vote_sample &&
        !first_window_sparse_max_weight_sample;
    const int64_t available_decay =
        std::max<int64_t>(0, validator.current_weight - config.min_weight);
    const int64_t peertrust_soft_floor = std::max<int64_t>(
        config.min_weight,
        std::min<int64_t>(config.peertrust_soft_min_weight, config.max_weight));
    const bool peertrust_floor_applies =
        low_peertrust || validator.peertrust_leader_debt > 0 ||
        previous_peertrust_debt > 0;
    const bool peertrust_soft_floor_eligible =
        config.peertrust_enabled && validator.vote_score >= kHealthyCatchUpScore;
    int64_t target_weight = ReputationTargetWeightForScore(recovery_score, config);
    if (peertrust_floor_applies && peertrust_soft_floor_eligible) {
      target_weight = std::max<int64_t>(target_weight, peertrust_soft_floor);
    }
    if (validator.current_weight >= config.max_weight && recovery_score >= 90) {
      target_weight = ClampWeight(config.max_weight, config);
    }
    const bool has_reputation_evidence = validator_has_enough_decay_evidence;
    validator.decay_applied = 0;
    validator.recovery_credit = 0;
    validator.bonus_credit = 0;
    if (has_reputation_evidence && validator.current_weight > target_weight) {
      validator.decay_applied = static_cast<int>(std::min<int64_t>(
          available_decay,
          std::min<int64_t>(config.decay_per_epoch,
                            validator.current_weight - target_weight)));
    } else if (has_reputation_evidence &&
               validator.current_weight < target_weight && !carryover_decay) {
      validator.bonus_credit = DiminishingBonusCreditForScore(
          recovery_score, validator.current_weight, target_weight, config,
          validator.opportunities, leader_opportunities);
    }
    validator.reputation_score = std::max(0, std::min(100, recovery_score));
    const int64_t smoothed_next_weight = ClampWeight(
        validator.current_weight - validator.decay_applied +
            validator.recovery_credit + validator.bonus_credit,
        config);
    validator.next_weight = smoothed_next_weight;
    const bool force_score_factor = false;
    MaybeApplyMultiplicativeWeightFormula(
        &validator, config, recovery_score, smoothed_next_weight,
        force_score_factor);
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
      const int64_t pre_penalty_weight = validator.next_weight;
      validator.recovery_credit = 0;
      validator.bonus_credit = 0;
      validator.reputation_score = 0;
      validator.reputation_factor_per_mille = 0;
      validator.direct_penalty_factor_per_mille = DirectPenaltyFactorForWeight(
          pre_penalty_weight, penalty_weight);
      validator.penalty_points =
          std::max<int64_t>(0, pre_penalty_weight - penalty_weight);
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
