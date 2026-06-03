#include "platform/consensus/ordering/td_hotstuff/algorithm/vote_score_reputation_plugin.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>

#include <glog/logging.h>

#include "common/crypto/hash.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr const char* kEnableEnv = "TD_HS_REPUTATION_ENABLE";
constexpr const char* kWindowSizeEnv = "TD_HS_REPUTATION_WINDOW_SIZE";
constexpr const char* kOutputDirEnv = "TD_HS_REPUTATION_OUTPUT_DIR";
constexpr const char* kQueueCapacityEnv = "TD_HS_REPUTATION_QUEUE_CAPACITY";
constexpr const char* kMaxDeltaEnv = "TD_HS_REPUTATION_MAX_DELTA";
constexpr const char* kDecayPerEpochEnv = "TD_HS_REPUTATION_DECAY_PER_EPOCH";
constexpr const char* kMaxRecoveryPerEpochEnv =
    "TD_HS_REPUTATION_MAX_RECOVERY_PER_EPOCH";
constexpr const char* kBonusPerEpochEnv = "TD_HS_REPUTATION_BONUS_PER_EPOCH";
constexpr const char* kMinWeightEnv = "TD_HS_REPUTATION_MIN_WEIGHT";
constexpr const char* kMaxWeightEnv = "TD_HS_REPUTATION_MAX_WEIGHT";
constexpr const char* kMinDecayOpportunitiesEnv =
    "TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES";
constexpr const char* kLeaderMissPenaltyEnableEnv =
    "TD_HS_REPUTATION_LEADER_MISS_PENALTY_ENABLE";
constexpr const char* kAlgorithmBayesV3 = "bayes_v3";
constexpr const char* kWeightUpdateEpochViewsEnv = "TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS";
constexpr const char* kWeightUpdateActivationDelayEnv = "TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY";
constexpr size_t kDefaultWindowSize = 4096;
constexpr size_t kDefaultQueueCapacity = 65536;
constexpr int kDefaultDecayPerEpoch = 3;
constexpr int kDefaultMaxRecoveryPerEpoch = 3;
constexpr int kDefaultBonusPerEpoch = 1;
constexpr int kDefaultMinDecayOpportunities = 8;
constexpr int64_t kMinWeight = 1;
constexpr int64_t kMaxWeight = 100;

bool EnvFlagEnabled(const char* env_name) {
  const char* enabled = std::getenv(env_name);
  return enabled != nullptr && std::string(enabled) == "1";
}

bool ReputationEnabledFromEnv() {
  return EnvFlagEnabled(kEnableEnv);
}

size_t SizeFromEnv(const char* env_name, size_t default_value) {
  const char* raw_value = std::getenv(env_name);
  if (raw_value == nullptr || std::string(raw_value).empty()) {
    return default_value;
  }
  try {
    const size_t value = std::stoull(raw_value);
    return value == 0 ? default_value : value;
  } catch (const std::exception&) {
    LOG(WARNING) << "invalid " << env_name << ":" << raw_value
                 << ", use default:" << default_value;
    return default_value;
  }
}

int IntFromEnv(const char* env_name, int default_value) {
  const char* raw_value = std::getenv(env_name);
  if (raw_value == nullptr || std::string(raw_value).empty()) {
    return default_value;
  }
  try {
    const int value = std::stoi(raw_value);
    return value < 0 ? default_value : value;
  } catch (const std::exception&) {
    LOG(WARNING) << "invalid " << env_name << ":" << raw_value
                 << ", use default:" << default_value;
    return default_value;
  }
}

int IntFromEnvInRange(const char* env_name, int default_value, int min_value,
                      int max_value) {
  const int value = IntFromEnv(env_name, default_value);
  if (value < min_value || value > max_value) {
    LOG(WARNING) << "invalid " << env_name << ":" << value
                 << ", use default:" << default_value;
    return default_value;
  }
  return value;
}

ReputationRecoveryConfig RecoveryConfigFromValues(
    int decay_per_epoch, int max_recovery_per_epoch, int bonus_per_epoch,
    int64_t min_weight, int64_t max_weight, uint64_t min_decay_opportunities) {
  ReputationRecoveryConfig config;
  config.decay_per_epoch = std::max(0, decay_per_epoch);
  config.max_recovery_per_epoch = std::max(0, max_recovery_per_epoch);
  config.bonus_per_epoch = std::max(0, bonus_per_epoch);
  config.min_decay_opportunities =
      std::max<uint64_t>(1, min_decay_opportunities);
  config.min_weight = std::max<int64_t>(kMinWeight, min_weight);
  config.max_weight = std::min<int64_t>(kMaxWeight, max_weight);
  if (config.max_weight < config.min_weight) {
    config.min_weight = kMinWeight;
    config.max_weight = kMaxWeight;
  }
  return config;
}

ReputationRecoveryConfig RecoveryConfigForCompute(int max_delta) {
  const int decay = max_delta > 0 ? max_delta : kDefaultDecayPerEpoch;
  return RecoveryConfigFromValues(decay, decay, kDefaultBonusPerEpoch,
                                  kMinWeight, kMaxWeight,
                                  /*min_decay_opportunities=*/1);
}

ReputationRecoveryConfig RecoveryConfigFromEnv(int legacy_default_decay) {
  const int legacy_delta = IntFromEnv(
      kMaxDeltaEnv, legacy_default_decay > 0 ? legacy_default_decay
                                             : kDefaultDecayPerEpoch);
  const int decay = IntFromEnvInRange(kDecayPerEpochEnv, legacy_delta, 0, 100);
  const int recovery = IntFromEnvInRange(
      kMaxRecoveryPerEpochEnv, kDefaultMaxRecoveryPerEpoch, 0, 100);
  const int bonus =
      IntFromEnvInRange(kBonusPerEpochEnv, kDefaultBonusPerEpoch, 0, 100);
  const int min_weight =
      IntFromEnvInRange(kMinWeightEnv, static_cast<int>(kMinWeight), 1, 100);
  const int max_weight =
      IntFromEnvInRange(kMaxWeightEnv, static_cast<int>(kMaxWeight), 1, 100);
  const int min_decay_opportunities = IntFromEnvInRange(
      kMinDecayOpportunitiesEnv, kDefaultMinDecayOpportunities, 1, 1000000);
  ReputationRecoveryConfig config = RecoveryConfigFromValues(
      decay, recovery, bonus, min_weight, max_weight,
      min_decay_opportunities);
  config.penalize_missing_leader =
      EnvFlagEnabled(kLeaderMissPenaltyEnableEnv);
  return config;
}

std::string OutputDirFromEnv() {
  const char* output_dir = std::getenv(kOutputDirEnv);
  if (output_dir == nullptr || std::string(output_dir).empty()) {
    return ".";
  }
  return output_dir;
}

std::string OutputPath(const std::string& output_dir, int node_id) {
  std::filesystem::path path(output_dir);
  path /= "td_hotstuff_reputation_node_" + std::to_string(node_id) + ".jsonl";
  return path.string();
}

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

int64_t ClampWeight(int64_t weight) {
  return std::max<int64_t>(kMinWeight, std::min<int64_t>(kMaxWeight, weight));
}

int64_t ClampWeight(int64_t weight, const ReputationRecoveryConfig& config) {
  return std::max<int64_t>(config.min_weight,
                           std::min<int64_t>(config.max_weight, weight));
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
  const uint64_t expected =
      static_cast<uint64_t>(RoundedDivide(selected_signer_slots,
                                          static_cast<uint64_t>(total_replicas)));
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
  const int target_effective_signers = std::max(
      1, std::min(total_replicas, (total_replicas * 2) / 3 + 1));
  const uint64_t numerator = static_cast<uint64_t>(sum_weight * sum_weight) * 100;
  const uint64_t denominator = static_cast<uint64_t>(square_sum) *
                               static_cast<uint64_t>(target_effective_signers);
  return std::max(0, std::min(100, RoundedDivide(numerator, denominator)));
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

std::string MetricCanonical(const VoteScoreCandidate& candidate) {
  std::ostringstream out;
  out << "td_hotstuff_reputation_metric_v3|" << candidate.algorithm << '|'
      << candidate.total_replicas << '|' << candidate.window_index << '|'
      << candidate.start_qc_view << '|' << candidate.end_qc_view << '|'
      << candidate.event_count;
  for (const auto& validator : candidate.validators) {
    out << '|' << validator.validator_id << ':' << validator.opportunities << ':'
        << validator.inclusions << ':' << validator.vote_score << ':'
        << validator.leader_certified_count << ':' << validator.leader_score << ':'
        << validator.leader_diversity_score << ':'
        << validator.reputation_score << ':' << validator.decay_applied << ':'
        << validator.recovery_credit << ':' << validator.bonus_credit << ':'
        << validator.current_weight << ':' << validator.next_weight;
  }
  return out.str();
}

std::string CandidateCanonicalFromParts(
    int total_replicas, uint64_t window_index, int start_qc_view,
    int end_qc_view, uint64_t event_count,
    const std::string& old_weight_root_hex, uint64_t old_weight_version,
    int activation_view, const std::string& metric_root_hex,
    const std::string& next_weight_root_hex,
    const std::vector<int64_t>& next_weights,
    const std::string& leader_weight_root_hex,
    uint64_t leader_params_version,
    const std::string& leader_randomness_ref,
    const std::vector<int64_t>& leader_weights) {
  (void)window_index;
  (void)event_count;
  (void)metric_root_hex;
  std::ostringstream out;
  out << "td_hotstuff_reputation_candidate_v3|" << total_replicas << '|'
      << start_qc_view << '|' << end_qc_view << '|' << old_weight_root_hex
      << '|' << old_weight_version << '|' << activation_view << '|'
      << next_weight_root_hex << '|' << leader_weight_root_hex << '|'
      << leader_params_version << '|' << leader_randomness_ref;
  for (size_t i = 0; i < next_weights.size(); ++i) {
    out << '|' << (i + 1) << ':' << next_weights[i];
  }
  out << "|leader";
  for (size_t i = 0; i < leader_weights.size(); ++i) {
    out << '|' << (i + 1) << ':' << leader_weights[i];
  }
  return out.str();
}

int ActivationViewForWindow(int end_qc_view, size_t epoch_views,
                            size_t activation_epoch_delay) {
  if (end_qc_view <= 0 || epoch_views == 0) {
    return 0;
  }
  const size_t delay = std::max<size_t>(activation_epoch_delay, 1);
  // A QC at the epoch boundary was formed under the old schedule. Make
  // activation the first following view so delayed boundary QCs verify against
  // the same schedule that produced them.
  return static_cast<int>(((static_cast<size_t>(end_qc_view) / epoch_views) +
                           delay) * epoch_views + 1);
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
    const unsigned char byte = static_cast<unsigned char>(signer_bitmap[byte_index]);
    if ((byte & (1 << (bit % 8))) != 0) {
      signers.push_back(validator_id);
    }
  }
  return signers;
}

VoteScoreCandidate ComputeBayesianReputationCandidate(
    int node_id, int total_replicas, uint64_t window_index,
    const std::vector<ReputationQcEvent>& events,
    const std::vector<int64_t>& current_weights, int max_delta,
    const std::string& old_weight_root_hex, uint64_t old_weight_version,
    int activation_view) {
  return ComputeBayesianReputationCandidateWithConfig(
      node_id, total_replicas, window_index, events, current_weights,
      RecoveryConfigForCompute(max_delta), old_weight_root_hex,
      old_weight_version, activation_view);
}

VoteScoreCandidate ComputeBayesianReputationCandidateWithConfig(
    int node_id, int total_replicas, uint64_t window_index,
    const std::vector<ReputationQcEvent>& events,
    const std::vector<int64_t>& current_weights,
    const ReputationRecoveryConfig& recovery_config,
    const std::string& old_weight_root_hex, uint64_t old_weight_version,
    int activation_view) {
  VoteScoreCandidate candidate;
  candidate.algorithm = kAlgorithmBayesV3;
  candidate.local_node_id = node_id;
  candidate.total_replicas = total_replicas;
  candidate.window_index = window_index;
  candidate.event_count = events.size();
  if (!events.empty()) {
    candidate.start_qc_view = events.front().qc_view;
    candidate.end_qc_view = events.back().qc_view;
  }

  const std::vector<int64_t> weights = NormalizeWeights(current_weights, total_replicas);
  candidate.old_weight_root_hex = old_weight_root_hex.empty()
                                      ? WeightRootHex(weights)
                                      : old_weight_root_hex;
  candidate.old_weight_version = old_weight_version;
  candidate.activation_view = activation_view;
  candidate.validators.resize(std::max(total_replicas, 0));
  for (int i = 0; i < total_replicas; ++i) {
    ValidatorVoteScore& validator = candidate.validators[i];
    validator.validator_id = i + 1;
    validator.current_weight = weights[i];
    validator.next_weight = weights[i];
  }

  std::vector<uint64_t> diversity_sum(std::max(total_replicas, 0), 0);
  std::vector<uint64_t> diversity_count(std::max(total_replicas, 0), 0);
  std::vector<uint64_t> variation_sum(std::max(total_replicas, 0), 0);
  std::vector<uint64_t> variation_count(std::max(total_replicas, 0), 0);
  std::vector<std::vector<int>> previous_signers_by_leader(
      std::max(total_replicas, 0));

  std::vector<ReputationQcEvent> ordered_events = events;
  std::sort(ordered_events.begin(), ordered_events.end(),
            [](const ReputationQcEvent& lhs, const ReputationQcEvent& rhs) {
              return lhs.qc_view < rhs.qc_view;
            });

  uint64_t selected_signer_slots = 0;
  for (const ReputationQcEvent& event : ordered_events) {
    std::vector<int> signers = DecodeSignerBitmap(event.signer_bitmap,
                                                  total_replicas);
    selected_signer_slots += signers.size();
    for (int signer : signers) {
      if (signer >= 1 && signer <= total_replicas) {
        ++candidate.validators[signer - 1].inclusions;
      }
    }

    const int leader = event.leader_id;
    if (leader >= 1 && leader <= total_replicas) {
      ValidatorVoteScore& leader_score = candidate.validators[leader - 1];
      ++leader_score.leader_certified_count;
      diversity_sum[leader - 1] += WeightedEffectiveDiversityScore(
          signers, weights, total_replicas);
      ++diversity_count[leader - 1];
      if (!previous_signers_by_leader[leader - 1].empty()) {
        variation_sum[leader - 1] += WeightedSignerVariationScore(
            previous_signers_by_leader[leader - 1], signers, weights,
            total_replicas);
        ++variation_count[leader - 1];
      }
      previous_signers_by_leader[leader - 1] = std::move(signers);
    }
  }

  const uint64_t fair_opportunities = FairExpectedSignerOpportunities(
      selected_signer_slots, total_replicas, ordered_events.size());
  const bool has_enough_decay_evidence =
      fair_opportunities >= recovery_config.min_decay_opportunities;
  std::vector<uint64_t> leader_opportunities_by_validator(
      std::max(total_replicas, 0), 0);
  if (recovery_config.penalize_missing_leader && total_replicas > 0 &&
      candidate.start_qc_view > 0 &&
      candidate.end_qc_view >= candidate.start_qc_view) {
    for (int view = candidate.start_qc_view; view <= candidate.end_qc_view;
         ++view) {
      const int scheduled_leader = DefaultLeaderForView(view, total_replicas);
      if (scheduled_leader >= 1 && scheduled_leader <= total_replicas) {
        ++leader_opportunities_by_validator[scheduled_leader - 1];
      }
    }
  }
  for (ValidatorVoteScore& validator : candidate.validators) {
    validator.opportunities = fair_opportunities;
  }
  int64_t total_current_weight = 0;
  for (const ValidatorVoteScore& validator : candidate.validators) {
    total_current_weight += validator.current_weight;
  }
  const int64_t mean_current_weight =
      candidate.validators.empty()
          ? 0
          : RoundedDivide(static_cast<uint64_t>(total_current_weight),
                          candidate.validators.size());

  for (ValidatorVoteScore& validator : candidate.validators) {
    validator.vote_score = VoteScore(validator.inclusions,
                                     validator.opportunities);
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
    if (!has_repeated_leader_signer_group) {
      validator.leader_diversity_score = 100;
    } else if (diversity_score >= 90) {
      validator.leader_diversity_score = diversity_score;
    } else {
      validator.leader_diversity_score = std::max(
          0, std::min(100,
                      RoundedDivide(diversity_score + variation_score, 2)));
    }
    uint64_t leader_opportunities = validator.leader_certified_count;
    if (recovery_config.penalize_missing_leader &&
        validator.leader_certified_count == 0) {
      leader_opportunities =
          idx >= 0 &&
                  idx < static_cast<int>(
                            leader_opportunities_by_validator.size())
              ? leader_opportunities_by_validator[idx]
              : 0;
    }
    validator.leader_score = std::min(
        LeaderCertifiedScore(validator.leader_certified_count,
                             leader_opportunities),
        validator.leader_diversity_score);
    if (validator.leader_certified_count > 0) {
      validator.leader_score = std::max(validator.leader_score, 67);
    }

    int recovery_score = validator.vote_score;
    if (validator.inclusions > 0) {
      recovery_score = std::max(recovery_score, 67);
    }
    if (leader_opportunities > 0 && validator.leader_score < 67) {
      recovery_score = std::min(recovery_score, validator.leader_score);
    }
    const bool near_fair_vote = HasNearFairInclusion(
        validator.inclusions, validator.opportunities);
    const int64_t available_decay =
        std::max<int64_t>(0,
                          validator.current_weight -
                              recovery_config.min_weight);
    validator.decay_applied =
        has_enough_decay_evidence
            ? static_cast<int>(std::min<int64_t>(
                  available_decay, recovery_config.decay_per_epoch))
            : 0;
    validator.recovery_credit = std::min(
        validator.decay_applied,
        RecoveryCreditForScore(recovery_score,
                               recovery_config.max_recovery_per_epoch));
    const bool earns_bonus =
        has_enough_decay_evidence &&
        ((recovery_score >= 95 && validator.vote_score >= 95) ||
         (near_fair_vote && recovery_score >= 67 &&
          validator.current_weight < mean_current_weight));
    validator.bonus_credit =
        earns_bonus ? recovery_config.bonus_per_epoch : 0;
    validator.reputation_score =
        validator.recovery_credit >= validator.decay_applied &&
                validator.bonus_credit > 0
            ? 100
            : std::max(0, std::min(100, recovery_score));
    validator.next_weight = ClampWeight(
        validator.current_weight - validator.decay_applied +
            validator.recovery_credit + validator.bonus_credit,
        recovery_config);
  }

  RecomputeVoteScoreCandidateRoots(&candidate);
  return candidate;
}

void RecomputeVoteScoreCandidateRoots(VoteScoreCandidate* candidate) {
  if (candidate == nullptr) {
    return;
  }
  candidate->next_weights.clear();
  candidate->next_weights.reserve(candidate->validators.size());
  for (const ValidatorVoteScore& validator : candidate->validators) {
    candidate->next_weights.push_back(validator.next_weight);
  }
  candidate->leader_weights.clear();
  candidate->leader_weights.reserve(candidate->validators.size());
  for (const ValidatorVoteScore& validator : candidate->validators) {
    candidate->leader_weights.push_back(validator.current_weight);
  }
  candidate->metric_root_hex = HashHex(MetricCanonical(*candidate));
  candidate->next_weight_root_hex = WeightRootHex(candidate->next_weights);
  candidate->leader_weight_root_hex =
      LeaderWeightRootHex(candidate->leader_weights);
  candidate->leader_params_version = 1;
  candidate->leader_randomness_ref = LeaderRandomnessRefHex(
      candidate->old_weight_root_hex, candidate->old_weight_version,
      candidate->activation_view, candidate->leader_weight_root_hex);
  candidate->candidate_digest_hex = VoteScoreCandidateDigest(
      candidate->total_replicas, candidate->window_index,
      candidate->start_qc_view, candidate->end_qc_view,
      candidate->event_count, candidate->old_weight_root_hex,
      candidate->old_weight_version, candidate->activation_view,
      candidate->metric_root_hex, candidate->next_weight_root_hex,
      candidate->next_weights, candidate->leader_weight_root_hex,
      candidate->leader_params_version, candidate->leader_randomness_ref,
      candidate->leader_weights);
}

std::string VoteScoreCandidateDigest(
    int total_replicas, uint64_t window_index, int start_qc_view,
    int end_qc_view, uint64_t event_count,
    const std::string& old_weight_root_hex, uint64_t old_weight_version,
    int activation_view, const std::string& metric_root_hex,
    const std::string& next_weight_root_hex,
    const std::vector<int64_t>& next_weights,
    const std::string& leader_weight_root_hex,
    uint64_t leader_params_version,
    const std::string& leader_randomness_ref,
    const std::vector<int64_t>& leader_weights) {
  return HashHex(CandidateCanonicalFromParts(
      total_replicas, window_index, start_qc_view, end_qc_view, event_count,
      old_weight_root_hex, old_weight_version, activation_view, metric_root_hex,
      next_weight_root_hex, next_weights, leader_weight_root_hex,
      leader_params_version, leader_randomness_ref, leader_weights));
}

std::string VoteScoreCandidateToJson(const VoteScoreCandidate& candidate) {
  std::ostringstream out;
  out << "{\"schema\":\"td_hotstuff_reputation_bayes_v3\""
      << ",\"algorithm\":\"" << candidate.algorithm << "\""
      << ",\"local_node_id\":" << candidate.local_node_id
      << ",\"total_replicas\":" << candidate.total_replicas
      << ",\"window_index\":" << candidate.window_index
      << ",\"start_qc_view\":" << candidate.start_qc_view
      << ",\"end_qc_view\":" << candidate.end_qc_view
      << ",\"event_count\":" << candidate.event_count
      << ",\"old_weight_root\":\"" << candidate.old_weight_root_hex << "\""
      << ",\"old_weight_version\":" << candidate.old_weight_version
      << ",\"activation_view\":" << candidate.activation_view
      << ",\"metric_root\":\"" << candidate.metric_root_hex << "\""
      << ",\"next_weight_root\":\"" << candidate.next_weight_root_hex << "\""
      << ",\"leader_weight_root\":\"" << candidate.leader_weight_root_hex
      << "\""
      << ",\"leader_params_version\":" << candidate.leader_params_version
      << ",\"leader_randomness_ref\":\"" << candidate.leader_randomness_ref
      << "\""
      << ",\"candidate_digest\":\"" << candidate.candidate_digest_hex << "\""
      << ",\"next_weights\":[";
  for (size_t i = 0; i < candidate.next_weights.size(); ++i) {
    if (i != 0) {
      out << ',';
    }
    out << candidate.next_weights[i];
  }
  out << ']'
      << ",\"leader_weights\":[";
  for (size_t i = 0; i < candidate.leader_weights.size(); ++i) {
    if (i != 0) {
      out << ',';
    }
    out << candidate.leader_weights[i];
  }
  out << ']'
      << ",\"validators\":[";
  for (size_t i = 0; i < candidate.validators.size(); ++i) {
    if (i != 0) {
      out << ',';
    }
    const ValidatorVoteScore& validator = candidate.validators[i];
    out << "{\"validator_id\":" << validator.validator_id
        << ",\"opportunities\":" << validator.opportunities
        << ",\"inclusions\":" << validator.inclusions
        << ",\"vote_score\":" << validator.vote_score
        << ",\"leader_certified_count\":"
        << validator.leader_certified_count
        << ",\"leader_score\":" << validator.leader_score
        << ",\"leader_diversity_score\":"
        << validator.leader_diversity_score
        << ",\"reputation_score\":" << validator.reputation_score
        << ",\"decay_applied\":" << validator.decay_applied
        << ",\"recovery_credit\":" << validator.recovery_credit
        << ",\"bonus_credit\":" << validator.bonus_credit
        << ",\"current_weight\":" << validator.current_weight
        << ",\"next_weight\":" << validator.next_weight << '}';
  }
  out << "]}";
  return out.str();
}

AsyncVoteScoreReputationPlugin::AsyncVoteScoreReputationPlugin(
    int node_id, int total_replicas, std::vector<int64_t> current_weights,
    std::string output_dir, size_t window_size, size_t queue_capacity,
    int max_delta)
    : node_id_(node_id),
      total_replicas_(total_replicas),
      current_weights_(NormalizeWeights(current_weights, total_replicas)),
      old_weight_root_hex_(WeightRootHex(current_weights_)),
      old_weight_version_(0),
      epoch_views_(SizeFromEnv(kWeightUpdateEpochViewsEnv, kDefaultWindowSize)),
      activation_epoch_delay_(SizeFromEnv(kWeightUpdateActivationDelayEnv, 2)),
      output_dir_(std::move(output_dir)),
      output_path_(OutputPath(output_dir_, node_id_)),
      window_size_(window_size == 0 ? kDefaultWindowSize : window_size),
      queue_capacity_(queue_capacity == 0 ? kDefaultQueueCapacity
                                          : queue_capacity),
      recovery_config_(RecoveryConfigFromEnv(max_delta)) {}

AsyncVoteScoreReputationPlugin::~AsyncVoteScoreReputationPlugin() { Stop(); }

std::unique_ptr<AsyncVoteScoreReputationPlugin>
AsyncVoteScoreReputationPlugin::CreateFromEnv(
    int node_id, int total_replicas,
    const std::vector<int64_t>& current_weights) {
  if (!ReputationEnabledFromEnv()) {
    return nullptr;
  }
  auto plugin = std::make_unique<AsyncVoteScoreReputationPlugin>(
      node_id, total_replicas, current_weights, OutputDirFromEnv(),
      SizeFromEnv(kWindowSizeEnv, kDefaultWindowSize),
      SizeFromEnv(kQueueCapacityEnv, kDefaultQueueCapacity),
      IntFromEnv(kMaxDeltaEnv, kDefaultDecayPerEpoch));
  plugin->Start();
  return plugin;
}

void AsyncVoteScoreReputationPlugin::Start() {
  if (!enabled_ || started_) {
    return;
  }
  stopping_.store(false);
  started_ = true;
  worker_ = std::thread(&AsyncVoteScoreReputationPlugin::WorkerLoop, this);
}

void AsyncVoteScoreReputationPlugin::Stop() {
  if (!started_) {
    return;
  }
  stopping_.store(true);
  cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
  started_ = false;
}

std::vector<VoteScoreCandidate>
AsyncVoteScoreReputationPlugin::TakeCompletedCandidates() {
  std::vector<VoteScoreCandidate> candidates;
  std::unique_lock<std::mutex> lock(mutex_);
  while (!completed_candidates_.empty()) {
    candidates.push_back(std::move(completed_candidates_.front()));
    completed_candidates_.pop_front();
  }
  return candidates;
}

void AsyncVoteScoreReputationPlugin::UpdateCurrentWeights(
    std::vector<int64_t> current_weights, std::string old_weight_root_hex,
    uint64_t old_weight_version) {
  std::unique_lock<std::mutex> lock(mutex_);
  current_weights_ = NormalizeWeights(current_weights, total_replicas_);
  old_weight_root_hex_ = std::move(old_weight_root_hex);
  old_weight_version_ = old_weight_version;
  current_window_.clear();
  completed_candidates_.clear();
}

bool AsyncVoteScoreReputationPlugin::RecordQc(
    int qc_view, const std::string& qc_hash,
    const std::string& signer_bitmap) {
  return RecordQc(qc_view, qc_hash, signer_bitmap, /*leader_id=*/0,
                  /*weight_version=*/0, /*active_weight_root=*/"");
}

bool AsyncVoteScoreReputationPlugin::RecordQc(
    int qc_view, const std::string& qc_hash,
    const std::string& signer_bitmap, int leader_id, uint64_t weight_version,
    std::string active_weight_root) {
  if (!enabled_ || qc_hash.empty()) {
    return false;
  }
  ReputationQcEvent event;
  event.qc_view = qc_view;
  event.qc_hash = qc_hash;
  event.signer_bitmap = signer_bitmap;
  event.leader_id = leader_id;
  event.weight_version = weight_version;
  event.active_weight_root = std::move(active_weight_root);

  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(qc_view);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(qc_view);
    return false;
  }
  queue_.push_back(std::move(event));
  cv_.notify_one();
  return true;
}

void AsyncVoteScoreReputationPlugin::DropRecord(int qc_view) {
  const uint64_t dropped = dropped_count_.fetch_add(1) + 1;
  if (dropped == 1 || dropped % 1000 == 0) {
    LOG(WARNING) << "dropped TD-Hotstuff reputation QC record, node:"
                 << node_id_ << " view:" << qc_view
                 << " dropped_count:" << dropped;
  }
}

void AsyncVoteScoreReputationPlugin::ProcessEvent(
    const ReputationQcEvent& event, std::ofstream& output) {
  std::vector<ReputationQcEvent> window;
  std::vector<int64_t> current_weights;
  std::string old_weight_root_hex;
  uint64_t old_weight_version = 0;
  uint64_t window_index = 0;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (event.weight_version != old_weight_version_ ||
        (!event.active_weight_root.empty() &&
         event.active_weight_root != old_weight_root_hex_)) {
      return;
    }
    if (!current_window_.empty() && epoch_views_ > 0 && event.qc_view > 0) {
      const size_t current_epoch =
          (static_cast<size_t>(current_window_.front().qc_view - 1) /
           epoch_views_);
      const size_t event_epoch =
          (static_cast<size_t>(event.qc_view - 1) / epoch_views_);
      if (event_epoch != current_epoch) {
        window = std::move(current_window_);
        current_window_.clear();
        window_index = window_index_++;
        current_weights = current_weights_;
        old_weight_root_hex = old_weight_root_hex_;
        old_weight_version = old_weight_version_;
      }
    }
    current_window_.push_back(event);
    if (window.empty() && current_window_.size() >= window_size_) {
      window = std::move(current_window_);
      current_window_.clear();
      window_index = window_index_++;
      current_weights = current_weights_;
      old_weight_root_hex = old_weight_root_hex_;
      old_weight_version = old_weight_version_;
    }
  }
  FlushWindow(output, std::move(window), window_index, std::move(current_weights),
              std::move(old_weight_root_hex), old_weight_version);
}

void AsyncVoteScoreReputationPlugin::FlushWindow(
    std::ofstream& output, std::vector<ReputationQcEvent> window,
    uint64_t window_index, std::vector<int64_t> current_weights,
    std::string old_weight_root_hex, uint64_t old_weight_version) {
  if (window.empty()) {
    return;
  }
  int evidence_end_view = window.back().qc_view;
  int window_end_view = evidence_end_view;
  if (epoch_views_ > 0 && evidence_end_view > 0) {
    window_end_view = static_cast<int>(
        ((static_cast<size_t>(evidence_end_view - 1) / epoch_views_) + 1) *
        epoch_views_);
  }
  const int activation_view = ActivationViewForWindow(
      window_end_view, epoch_views_, activation_epoch_delay_);
  VoteScoreCandidate candidate = ComputeBayesianReputationCandidateWithConfig(
      node_id_, total_replicas_, window_index, window, current_weights,
      recovery_config_, old_weight_root_hex, old_weight_version,
      activation_view);
  if (epoch_views_ > 0 && window_end_view > 0) {
    candidate.start_qc_view = window_end_view - static_cast<int>(epoch_views_) + 1;
    candidate.end_qc_view = window_end_view;
    candidate.activation_view = activation_view;
    RecomputeVoteScoreCandidateRoots(&candidate);
  }
  output << VoteScoreCandidateToJson(candidate) << '\n';
  output.flush();
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (completed_candidates_.size() >= queue_capacity_) {
      completed_candidates_.pop_front();
    }
    completed_candidates_.push_back(candidate);
  }
}

void AsyncVoteScoreReputationPlugin::WorkerLoop() {
  std::filesystem::create_directories(output_dir_);
  std::ofstream output(output_path_, std::ios::app);
  if (!output.is_open()) {
    LOG(ERROR) << "open TD-Hotstuff reputation output fail:" << output_path_;
    return;
  }

  while (true) {
    ReputationQcEvent event;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
        return stopping_.load() || !queue_.empty();
      });
      if (queue_.empty()) {
        if (stopping_.load()) {
          break;
        }
        output.flush();
        continue;
      }
      event = std::move(queue_.front());
      queue_.pop_front();
    }
    ProcessEvent(event, output);
  }

  std::vector<ReputationQcEvent> window;
  std::vector<int64_t> current_weights;
  std::string old_weight_root_hex;
  uint64_t old_weight_version = 0;
  uint64_t window_index = 0;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!current_window_.empty()) {
      window = std::move(current_window_);
      current_window_.clear();
      window_index = window_index_++;
      current_weights = current_weights_;
      old_weight_root_hex = old_weight_root_hex_;
      old_weight_version = old_weight_version_;
    }
  }
  FlushWindow(output, std::move(window), window_index, std::move(current_weights),
              std::move(old_weight_root_hex), old_weight_version);
  output.flush();
}

}  // namespace td_hotstuff
}  // namespace resdb
