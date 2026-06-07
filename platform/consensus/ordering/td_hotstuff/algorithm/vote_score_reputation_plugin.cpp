#include "platform/consensus/ordering/td_hotstuff/algorithm/vote_score_reputation_plugin.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <utility>

#include <glog/logging.h>

#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff_digest.h"

namespace resdb {
namespace td_hotstuff {
namespace reputation = ::resdb::consensus::reputation;
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
constexpr const char* kMinLeaderOpportunitiesEnv =
    "TD_HS_REPUTATION_MIN_LEADER_OPPORTUNITIES";
constexpr const char* kLeaderRecoveryEnableEnv =
    "TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE";
constexpr const char* kPeerTrustEnableEnv =
    "TD_HS_REPUTATION_PEERTRUST_ENABLE";
constexpr const char* kPeerTrustDebtIncrementEnv =
    "TD_HS_REPUTATION_PEERTRUST_DEBT_INCREMENT";
constexpr const char* kPeerTrustDebtRecoveryEnv =
    "TD_HS_REPUTATION_PEERTRUST_DEBT_RECOVERY";
constexpr const char* kPeerTrustDebtMaxEnv =
    "TD_HS_REPUTATION_PEERTRUST_DEBT_MAX";
constexpr const char* kPeerTrustDebtTriggerScoreEnv =
    "TD_HS_REPUTATION_PEERTRUST_DEBT_TRIGGER_SCORE";
constexpr const char* kSybilGraphEnableEnv =
    "TD_HS_REPUTATION_SYBIL_GRAPH_ENABLE";
constexpr const char* kSybilGraphIterationsEnv =
    "TD_HS_REPUTATION_SYBIL_GRAPH_ITERATIONS";
constexpr const char* kSybilGraphMaxDiscountEnv =
    "TD_HS_REPUTATION_SYBIL_GRAPH_MAX_DISCOUNT";
constexpr const char* kSybilGraphDebtIncrementEnv =
    "TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_INCREMENT";
constexpr const char* kSybilGraphDebtRecoveryEnv =
    "TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_RECOVERY";
constexpr const char* kSybilGraphDebtMaxEnv =
    "TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_MAX";
constexpr const char* kSybilGraphDebtTriggerScoreEnv =
    "TD_HS_REPUTATION_SYBIL_GRAPH_DEBT_TRIGGER_SCORE";
constexpr const char* kSybilGraphSeedMinReputationEnv =
    "TD_HS_REPUTATION_SYBIL_GRAPH_SEED_MIN_REPUTATION";
constexpr const char* kSybilGraphMinEdgesEnv =
    "TD_HS_REPUTATION_SYBIL_GRAPH_MIN_EDGES";
constexpr const char* kStrongFaultEnableEnv = "TD_HS_STRONG_FAULT_ENABLE";
constexpr const char* kDoubleProposalDetectEnableEnv =
    "TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE";
constexpr const char* kDoubleVoteDetectEnableEnv =
    "TD_HS_DOUBLE_VOTE_DETECT_ENABLE";
constexpr const char* kInvalidQcProposalDetectEnableEnv =
    "TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE";
constexpr const char* kWeightUpdateVoteEquivocationDetectEnableEnv =
    "TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE";
constexpr const char* kTimeoutVoteEquivocationDetectEnableEnv =
    "TD_HS_TIMEOUT_VOTE_EQUIVOCATION_DETECT_ENABLE";
constexpr const char* kInvalidTcProposalDetectEnableEnv =
    "TD_HS_INVALID_TC_PROPOSAL_DETECT_ENABLE";
constexpr const char* kConflictingQcDetectEnableEnv =
    "TD_HS_CONFLICTING_QC_DETECT_ENABLE";
constexpr const char* kStrongFaultTargetWeightEnv =
    "TD_HS_STRONG_FAULT_TARGET_WEIGHT";
constexpr const char* kMinCandidateQcsEnv =
    "TD_HS_REPUTATION_MIN_CANDIDATE_QCS";
constexpr const char* kWeightUpdateEpochViewsEnv =
    "TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS";
constexpr const char* kWeightUpdateActivationDelayEnv =
    "TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY";
constexpr size_t kDefaultWindowSize = 4096;
constexpr size_t kDefaultQueueCapacity = 65536;
constexpr size_t kFlushEveryCandidates = 16;
constexpr int kDefaultDecayPerEpoch = 3;
constexpr int kDefaultMaxRecoveryPerEpoch = 3;
constexpr int kDefaultBonusPerEpoch = 1;
constexpr int kDefaultMinDecayOpportunities = 8;
constexpr int kDefaultMinLeaderOpportunities = 8;
constexpr int kDefaultPeerTrustDebtIncrement = 20;
constexpr int kDefaultPeerTrustDebtRecovery = 5;
constexpr int kDefaultPeerTrustDebtMax = 95;
constexpr int kDefaultPeerTrustDebtTriggerScore = 67;
constexpr int kDefaultSybilGraphIterations = 0;
constexpr int kDefaultSybilGraphMaxDiscount = 40;
constexpr int kDefaultSybilGraphDebtIncrement = 20;
constexpr int kDefaultSybilGraphDebtRecovery = 5;
constexpr int kDefaultSybilGraphDebtMax = 95;
constexpr int kDefaultSybilGraphDebtTriggerScore = 67;
constexpr int kDefaultSybilGraphSeedMinReputation = 67;
constexpr int kDefaultSybilGraphMinEdges = 1;
constexpr int64_t kMinWeight = 1;
constexpr int64_t kMaxWeight = 100;

bool EnvFlagEnabled(const char* env_name) {
  const char* enabled = std::getenv(env_name);
  return enabled != nullptr && std::string(enabled) == "1";
}

bool ReputationEnabledFromEnv() { return EnvFlagEnabled(kEnableEnv); }

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

size_t CompleteWindowQcThreshold(size_t window_size, size_t epoch_views) {
  const size_t expected_qcs =
      epoch_views == 0 ? window_size : std::min(window_size, epoch_views);
  if (expected_qcs <= 1) {
    return 1;
  }
  return std::max<size_t>(1, (expected_qcs * 3 + 3) / 4);
}

size_t MinCandidateQcCountFromEnv(size_t window_size, size_t epoch_views) {
  const size_t default_value =
      CompleteWindowQcThreshold(window_size, epoch_views);
  const size_t configured = SizeFromEnv(kMinCandidateQcsEnv, default_value);
  const size_t max_reasonable =
      std::max<size_t>(1, epoch_views == 0 ? window_size
                                           : std::min(window_size, epoch_views));
  return std::max<size_t>(1, std::min(configured, max_reasonable));
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

ReputationRecoveryConfig RecoveryConfigFromValues(
    int decay_per_epoch, int max_recovery_per_epoch, int bonus_per_epoch,
    int64_t min_weight, int64_t max_weight, uint64_t min_decay_opportunities,
    uint64_t min_leader_opportunities = kDefaultMinLeaderOpportunities) {
  ReputationRecoveryConfig config;
  config.decay_per_epoch = std::max(0, decay_per_epoch);
  config.max_recovery_per_epoch = std::max(0, max_recovery_per_epoch);
  config.bonus_per_epoch = std::max(0, bonus_per_epoch);
  config.min_decay_opportunities =
      std::max<uint64_t>(1, min_decay_opportunities);
  config.min_leader_opportunities =
      std::max<uint64_t>(1, min_leader_opportunities);
  config.min_weight = std::max<int64_t>(kMinWeight, min_weight);
  config.max_weight = std::min<int64_t>(kMaxWeight, max_weight);
  config.leader_recovery_enabled = false;
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
                                  /*min_decay_opportunities=*/1,
                                  kDefaultMinLeaderOpportunities);
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
  const int min_leader_opportunities = IntFromEnvInRange(
      kMinLeaderOpportunitiesEnv, kDefaultMinLeaderOpportunities, 1, 1000000);
  const bool strong_fault_enabled = EnvFlagEnabled(kStrongFaultEnableEnv);
  ReputationRecoveryConfig config = RecoveryConfigFromValues(
      decay, recovery, bonus, min_weight, max_weight,
      min_decay_opportunities, min_leader_opportunities);
  config.leader_recovery_enabled = EnvFlagEnabled(kLeaderRecoveryEnableEnv);
  config.peertrust_enabled = EnvFlagEnabled(kPeerTrustEnableEnv);
  config.peertrust_debt_increment = IntFromEnvInRange(
      kPeerTrustDebtIncrementEnv, kDefaultPeerTrustDebtIncrement, 0, 100);
  config.peertrust_debt_recovery = IntFromEnvInRange(
      kPeerTrustDebtRecoveryEnv, kDefaultPeerTrustDebtRecovery, 0, 100);
  config.peertrust_debt_max = IntFromEnvInRange(
      kPeerTrustDebtMaxEnv, kDefaultPeerTrustDebtMax, 0, 100);
  config.peertrust_debt_trigger_score = IntFromEnvInRange(
      kPeerTrustDebtTriggerScoreEnv, kDefaultPeerTrustDebtTriggerScore, 0,
      100);
  config.sybil_graph_enabled = EnvFlagEnabled(kSybilGraphEnableEnv);
  config.sybil_graph_iterations = IntFromEnvInRange(
      kSybilGraphIterationsEnv, kDefaultSybilGraphIterations, 0, 1000);
  config.sybil_graph_max_discount = IntFromEnvInRange(
      kSybilGraphMaxDiscountEnv, kDefaultSybilGraphMaxDiscount, 0, 100);
  config.sybil_graph_debt_increment = IntFromEnvInRange(
      kSybilGraphDebtIncrementEnv, kDefaultSybilGraphDebtIncrement, 0, 100);
  config.sybil_graph_debt_recovery = IntFromEnvInRange(
      kSybilGraphDebtRecoveryEnv, kDefaultSybilGraphDebtRecovery, 0, 100);
  config.sybil_graph_debt_max = IntFromEnvInRange(
      kSybilGraphDebtMaxEnv, kDefaultSybilGraphDebtMax, 0, 100);
  config.sybil_graph_debt_trigger_score = IntFromEnvInRange(
      kSybilGraphDebtTriggerScoreEnv,
      kDefaultSybilGraphDebtTriggerScore, 0, 100);
  config.sybil_graph_seed_min_reputation = IntFromEnvInRange(
      kSybilGraphSeedMinReputationEnv,
      kDefaultSybilGraphSeedMinReputation, 0, 100);
  config.sybil_graph_min_edges = static_cast<uint64_t>(IntFromEnvInRange(
      kSybilGraphMinEdgesEnv, kDefaultSybilGraphMinEdges, 0, 1000000));
  config.strong_fault_enabled = strong_fault_enabled;
  config.double_proposal_detection_enabled =
      strong_fault_enabled &&
      (std::getenv(kDoubleProposalDetectEnableEnv) == nullptr ||
       EnvFlagEnabled(kDoubleProposalDetectEnableEnv));
  config.double_vote_detection_enabled =
      strong_fault_enabled &&
      (std::getenv(kDoubleVoteDetectEnableEnv) == nullptr ||
       EnvFlagEnabled(kDoubleVoteDetectEnableEnv));
  config.invalid_qc_proposal_detection_enabled =
      strong_fault_enabled &&
      (std::getenv(kInvalidQcProposalDetectEnableEnv) == nullptr ||
       EnvFlagEnabled(kInvalidQcProposalDetectEnableEnv));
  config.weight_update_vote_equivocation_detection_enabled =
      strong_fault_enabled &&
      (std::getenv(kWeightUpdateVoteEquivocationDetectEnableEnv) == nullptr ||
       EnvFlagEnabled(kWeightUpdateVoteEquivocationDetectEnableEnv));
  config.timeout_vote_equivocation_detection_enabled =
      strong_fault_enabled &&
      (std::getenv(kTimeoutVoteEquivocationDetectEnableEnv) == nullptr ||
       EnvFlagEnabled(kTimeoutVoteEquivocationDetectEnableEnv));
  config.invalid_tc_proposal_detection_enabled =
      strong_fault_enabled &&
      (std::getenv(kInvalidTcProposalDetectEnableEnv) == nullptr ||
       EnvFlagEnabled(kInvalidTcProposalDetectEnableEnv));
  config.conflicting_qc_detection_enabled =
      strong_fault_enabled &&
      (std::getenv(kConflictingQcDetectEnableEnv) == nullptr ||
       EnvFlagEnabled(kConflictingQcDetectEnableEnv));
  config.strong_fault_target_weight =
      IntFromEnvInRange(kStrongFaultTargetWeightEnv, 1, 1, 100);
  return config;
}

reputation::ReputationConfig ToNeutralConfig(
    const ReputationRecoveryConfig& config) {
  reputation::ReputationConfig neutral;
  neutral.decay_per_epoch = config.decay_per_epoch;
  neutral.max_recovery_per_epoch = config.max_recovery_per_epoch;
  neutral.bonus_per_epoch = config.bonus_per_epoch;
  neutral.min_weight = config.min_weight;
  neutral.max_weight = config.max_weight;
  neutral.min_decay_opportunities = config.min_decay_opportunities;
  neutral.min_leader_opportunities = config.min_leader_opportunities;
  neutral.leader_eligible_min_weight = config.leader_eligible_min_weight;
  neutral.leader_recovery_enabled = config.leader_recovery_enabled;
  neutral.peertrust_enabled = config.peertrust_enabled;
  neutral.peertrust_debt_increment = config.peertrust_debt_increment;
  neutral.peertrust_debt_recovery = config.peertrust_debt_recovery;
  neutral.peertrust_debt_max = config.peertrust_debt_max;
  neutral.peertrust_debt_trigger_score = config.peertrust_debt_trigger_score;
  neutral.sybil_graph_enabled = config.sybil_graph_enabled;
  neutral.sybil_graph_iterations = config.sybil_graph_iterations;
  neutral.sybil_graph_max_discount = config.sybil_graph_max_discount;
  neutral.sybil_graph_debt_increment = config.sybil_graph_debt_increment;
  neutral.sybil_graph_debt_recovery = config.sybil_graph_debt_recovery;
  neutral.sybil_graph_debt_max = config.sybil_graph_debt_max;
  neutral.sybil_graph_debt_trigger_score =
      config.sybil_graph_debt_trigger_score;
  neutral.sybil_graph_seed_min_reputation =
      config.sybil_graph_seed_min_reputation;
  neutral.sybil_graph_min_edges = config.sybil_graph_min_edges;
  neutral.strong_fault_enabled = config.strong_fault_enabled;
  neutral.double_proposal_detection_enabled =
      config.double_proposal_detection_enabled;
  neutral.double_vote_detection_enabled =
      config.double_vote_detection_enabled;
  neutral.invalid_qc_proposal_detection_enabled =
      config.invalid_qc_proposal_detection_enabled;
  neutral.weight_update_vote_equivocation_detection_enabled =
      config.weight_update_vote_equivocation_detection_enabled;
  neutral.timeout_vote_equivocation_detection_enabled =
      config.timeout_vote_equivocation_detection_enabled;
  neutral.invalid_tc_proposal_detection_enabled =
      config.invalid_tc_proposal_detection_enabled;
  neutral.conflicting_qc_detection_enabled =
      config.conflicting_qc_detection_enabled;
  neutral.strong_fault_target_weight = config.strong_fault_target_weight;
  return neutral;
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

bool HasDirectStrongFaultEvidence(const std::vector<ReputationQcEvent>& window) {
  for (const ReputationQcEvent& event : window) {
    if (event.invalid_qc_proposal_artifact ||
        event.invalid_tc_proposal_artifact) {
      return true;
    }
  }
  return false;
}

bool IsDirectStrongFaultEvidence(const ReputationQcEvent& event) {
  return event.invalid_qc_proposal_artifact ||
         event.invalid_tc_proposal_artifact;
}

bool HasDoubleProposalEvidence(const std::vector<ReputationQcEvent>& window) {
  std::map<std::string, std::string> hash_by_key;
  for (const ReputationQcEvent& event : window) {
    if (!event.signed_proposal_artifact || !event.proposal_signature_verified ||
        event.protocol_id.empty() || event.leader_id <= 0 ||
        event.qc_view <= 0 || event.proposal_hash.empty()) {
      continue;
    }
    std::ostringstream key;
    key << event.protocol_id << '|' << event.weight_version << '|'
        << event.leader_id << '|' << event.qc_view << '|'
        << event.proposal_slot;
    auto [it, inserted] = hash_by_key.emplace(key.str(), event.proposal_hash);
    if (!inserted && it->second != event.proposal_hash) {
      return true;
    }
  }
  return false;
}

bool HasDoubleVoteEvidence(const std::vector<ReputationQcEvent>& window) {
  std::map<std::string, std::string> hash_by_key;
  for (const ReputationQcEvent& event : window) {
    if (!event.signed_vote_artifact || !event.vote_signature_verified ||
        event.protocol_id.empty() || event.vote_signer_id <= 0 ||
        event.qc_view <= 0 || event.vote_proposal_hash.empty()) {
      continue;
    }
    std::ostringstream key;
    key << event.protocol_id << '|' << event.weight_version << '|'
        << event.vote_signer_id << '|' << event.qc_view << '|'
        << event.proposal_slot;
    auto [it, inserted] = hash_by_key.emplace(key.str(),
                                              event.vote_proposal_hash);
    if (!inserted && it->second != event.vote_proposal_hash) {
      return true;
    }
  }
  return false;
}

bool HasWeightUpdateVoteEquivocationEvidence(
    const std::vector<ReputationQcEvent>& window) {
  std::map<std::string, std::string> digest_by_key;
  for (const ReputationQcEvent& event : window) {
    if (!event.weight_update_vote_artifact ||
        !event.vote_signature_verified || event.vote_signer_id <= 0 ||
        event.old_weight_root.empty() || event.candidate_digest.empty()) {
      continue;
    }
    std::ostringstream key;
    key << event.vote_signer_id << '|' << event.old_weight_root << '|'
        << event.old_weight_version << '|' << event.activation_view;
    auto [it, inserted] = digest_by_key.emplace(key.str(),
                                                event.candidate_digest);
    if (!inserted && it->second != event.candidate_digest) {
      return true;
    }
  }
  return false;
}

bool HasSparseStrongFaultEvidence(const std::vector<ReputationQcEvent>& window) {
  return HasDirectStrongFaultEvidence(window) ||
         HasDoubleProposalEvidence(window) ||
         HasDoubleVoteEvidence(window) ||
         HasWeightUpdateVoteEquivocationEvidence(window);
}

int ActivationViewForWindow(int end_qc_view, size_t epoch_views,
                            size_t activation_epoch_delay) {
  if (end_qc_view <= 0 || epoch_views == 0) {
    return 0;
  }
  // A QC at the epoch boundary was formed under the old schedule. Make
  // activation the first following view so delayed boundary QCs verify against
  // the same schedule that produced them.
  return static_cast<int>(((static_cast<size_t>(end_qc_view) / epoch_views) +
                           activation_epoch_delay) *
                              epoch_views +
                          1);
}

ValidatorVoteScore ToTdValidator(
    const reputation::ValidatorReputation& validator) {
  ValidatorVoteScore td;
  td.validator_id = validator.validator_id;
  td.opportunities = validator.opportunities;
  td.inclusions = validator.inclusions;
  td.vote_score = validator.vote_score;
  td.leader_certified_count = validator.leader_certified_count;
  td.leader_opportunity_count = validator.leader_opportunity_count;
  td.leader_score = validator.leader_score;
  td.leader_diversity_score = validator.leader_diversity_score;
  td.peertrust_score = validator.peertrust_score;
  td.reviewer_credibility_score = validator.reviewer_credibility_score;
  td.transaction_context_score = validator.transaction_context_score;
  td.community_context_score = validator.community_context_score;
  td.reviewer_entropy_score = validator.reviewer_entropy_score;
  td.cross_leader_independence_score = validator.cross_leader_independence_score;
  td.reviewer_overuse_score = validator.reviewer_overuse_score;
  td.peertrust_leader_debt = validator.peertrust_leader_debt;
  td.peertrust_debt_delta = validator.peertrust_debt_delta;
  td.feedback_count = validator.feedback_count;
  td.sybil_rank_score = validator.sybil_rank_score;
  td.sybil_cut_score = validator.sybil_cut_score;
  td.sybil_graph_score = validator.sybil_graph_score;
  td.sybil_graph_debt = validator.sybil_graph_debt;
  td.sybil_graph_debt_delta = validator.sybil_graph_debt_delta;
  td.graph_degree = validator.graph_degree;
  td.seed_trust_score = validator.seed_trust_score;
  td.reputation_score = validator.reputation_score;
  td.decay_applied = validator.decay_applied;
  td.recovery_credit = validator.recovery_credit;
  td.bonus_credit = validator.bonus_credit;
  td.strong_fault_count = validator.strong_fault_count;
  td.penalty_points = validator.penalty_points;
  td.current_weight = validator.current_weight;
  td.next_weight = validator.next_weight;
  return td;
}

reputation::ValidatorReputation ToNeutralValidator(
    const ValidatorVoteScore& validator) {
  reputation::ValidatorReputation neutral;
  neutral.validator_id = validator.validator_id;
  neutral.opportunities = validator.opportunities;
  neutral.inclusions = validator.inclusions;
  neutral.vote_score = validator.vote_score;
  neutral.leader_certified_count = validator.leader_certified_count;
  neutral.leader_opportunity_count = validator.leader_opportunity_count;
  neutral.leader_score = validator.leader_score;
  neutral.leader_diversity_score = validator.leader_diversity_score;
  neutral.peertrust_score = validator.peertrust_score;
  neutral.reviewer_credibility_score = validator.reviewer_credibility_score;
  neutral.transaction_context_score = validator.transaction_context_score;
  neutral.community_context_score = validator.community_context_score;
  neutral.reviewer_entropy_score = validator.reviewer_entropy_score;
  neutral.cross_leader_independence_score = validator.cross_leader_independence_score;
  neutral.reviewer_overuse_score = validator.reviewer_overuse_score;
  neutral.peertrust_leader_debt = validator.peertrust_leader_debt;
  neutral.peertrust_debt_delta = validator.peertrust_debt_delta;
  neutral.feedback_count = validator.feedback_count;
  neutral.sybil_rank_score = validator.sybil_rank_score;
  neutral.sybil_cut_score = validator.sybil_cut_score;
  neutral.sybil_graph_score = validator.sybil_graph_score;
  neutral.sybil_graph_debt = validator.sybil_graph_debt;
  neutral.sybil_graph_debt_delta = validator.sybil_graph_debt_delta;
  neutral.graph_degree = validator.graph_degree;
  neutral.seed_trust_score = validator.seed_trust_score;
  neutral.reputation_score = validator.reputation_score;
  neutral.decay_applied = validator.decay_applied;
  neutral.recovery_credit = validator.recovery_credit;
  neutral.bonus_credit = validator.bonus_credit;
  neutral.strong_fault_count = validator.strong_fault_count;
  neutral.penalty_points = validator.penalty_points;
  neutral.current_weight = validator.current_weight;
  neutral.next_weight = validator.next_weight;
  return neutral;
}

VoteScoreCandidate ToTdCandidate(
    const reputation::ReputationCandidate& candidate) {
  VoteScoreCandidate td;
  td.algorithm = candidate.algorithm;
  td.local_node_id = candidate.local_node_id;
  td.total_replicas = candidate.total_replicas;
  td.window_index = candidate.window_index;
  td.start_qc_view = candidate.start_view;
  td.end_qc_view = candidate.end_view;
  td.event_count = candidate.event_count;
  td.old_weight_root_hex = candidate.old_weight_root_hex;
  td.old_weight_version = candidate.old_weight_version;
  td.activation_view = candidate.activation_view;
  td.next_weights = candidate.next_weights;
  td.strong_faults = candidate.strong_faults;
  td.metric_root_hex = candidate.metric_root_hex;
  td.reputation_root_hex = candidate.reputation_root_hex;
  td.strong_fault_root_hex = candidate.strong_fault_root_hex;
  td.penalty_root_hex = candidate.penalty_root_hex;
  td.next_weight_root_hex = WeightRootHex(td.next_weights);
  td.validators.reserve(candidate.validators.size());
  for (const auto& validator : candidate.validators) {
    td.validators.push_back(ToTdValidator(validator));
  }
  td.candidate_digest_hex = VoteScoreCandidateDigest(
      td.total_replicas, td.window_index, td.start_qc_view, td.end_qc_view,
      td.event_count, td.old_weight_root_hex, td.old_weight_version,
      td.activation_view, td.metric_root_hex, td.next_weight_root_hex,
      td.next_weights, /*leader_weight_root_hex=*/"",
      /*leader_params_version=*/0, /*leader_randomness_ref=*/"",
      /*leader_weights=*/{}, td.reputation_root_hex,
      td.strong_fault_root_hex, td.penalty_root_hex);
  return td;
}

reputation::ReputationCandidate ToNeutralCandidate(
    const VoteScoreCandidate& candidate) {
  reputation::ReputationCandidate neutral;
  neutral.algorithm = candidate.algorithm;
  neutral.local_node_id = candidate.local_node_id;
  neutral.total_replicas = candidate.total_replicas;
  neutral.window_index = candidate.window_index;
  neutral.start_view = candidate.start_qc_view;
  neutral.end_view = candidate.end_qc_view;
  neutral.event_count = candidate.event_count;
  neutral.old_weight_root_hex = candidate.old_weight_root_hex;
  neutral.old_weight_version = candidate.old_weight_version;
  neutral.activation_view = candidate.activation_view;
  neutral.next_weights = candidate.next_weights;
  neutral.strong_faults = candidate.strong_faults;
  neutral.metric_root_hex = candidate.metric_root_hex;
  neutral.reputation_root_hex = candidate.reputation_root_hex;
  neutral.strong_fault_root_hex = candidate.strong_fault_root_hex;
  neutral.penalty_root_hex = candidate.penalty_root_hex;
  neutral.next_weight_root_hex = candidate.next_weight_root_hex;
  neutral.candidate_digest_hex = candidate.candidate_digest_hex;
  neutral.validators.reserve(candidate.validators.size());
  for (const auto& validator : candidate.validators) {
    neutral.validators.push_back(ToNeutralValidator(validator));
  }
  return neutral;
}

}  // namespace

std::vector<int> DecodeSignerBitmap(const std::string& signer_bitmap,
                                    int total_replicas) {
  return reputation::DecodeSignerBitmap(signer_bitmap, total_replicas);
}

reputation::MetricEvidence ToMetricEvidence(const ReputationQcEvent& event) {
  reputation::MetricEvidence evidence;
  evidence.view_or_round = event.qc_view;
  evidence.leader_id = event.leader_id;
  evidence.collector_id = event.qc_collector_id;
  evidence.artifact_digest = event.qc_hash;
  evidence.signer_bitmap = event.signer_bitmap;
  evidence.available_signer_bitmap = event.available_signer_bitmap;
  evidence.active_weight_root = event.active_weight_root;
  evidence.weight_version = event.weight_version;
  evidence.leader_eligible_min_weight = event.leader_eligible_min_weight;
  if (!event.qc_hash.empty()) {
    evidence.artifact_family = reputation::ArtifactFamily::kQc;
    evidence.outcome_class = reputation::OutcomeClass::kCertified;
  } else if (event.leader_opportunity) {
    evidence.artifact_family = reputation::ArtifactFamily::kTimeout;
    evidence.outcome_class = reputation::OutcomeClass::kTimeoutOrViewChange;
  }
  return evidence;
}

reputation::SignedProposalEvidence ToSignedProposalEvidence(
    const ReputationQcEvent& event) {
  reputation::SignedProposalEvidence artifact;
  artifact.protocol_id = event.protocol_id;
  artifact.leader_id = event.leader_id;
  artifact.view_or_round = event.qc_view;
  artifact.slot_or_height = event.proposal_slot;
  artifact.proposal_hash = event.proposal_hash;
  artifact.signature_verified = event.proposal_signature_verified;
  artifact.active_weight_root = event.active_weight_root;
  artifact.weight_version = event.weight_version;
  return artifact;
}

reputation::SignedVoteEvidence ToSignedVoteEvidence(
    const ReputationQcEvent& event) {
  reputation::SignedVoteEvidence artifact;
  artifact.protocol_id = event.protocol_id;
  artifact.signer_id = event.vote_signer_id;
  artifact.view_or_round = event.qc_view;
  artifact.slot_or_height = event.proposal_slot;
  artifact.proposal_hash = event.vote_proposal_hash;
  artifact.signature_verified = event.vote_signature_verified;
  artifact.active_weight_root = event.active_weight_root;
  artifact.weight_version = event.weight_version;
  return artifact;
}

reputation::InvalidQcProposalEvidence ToInvalidQcProposalEvidence(
    const ReputationQcEvent& event) {
  reputation::InvalidQcProposalEvidence artifact;
  artifact.protocol_id = event.protocol_id;
  artifact.leader_id = event.leader_id;
  artifact.view_or_round = event.qc_view;
  artifact.slot_or_height = event.proposal_slot;
  artifact.proposal_hash = event.proposal_hash;
  artifact.proposal_signature_verified = event.proposal_signature_verified;
  artifact.qc_verified = event.qc_verified;
  artifact.invalid_reason = event.invalid_reason;
  artifact.active_weight_root = event.active_weight_root;
  artifact.weight_version = event.weight_version;
  return artifact;
}

reputation::SignedWeightUpdateVoteEvidence ToSignedWeightUpdateVoteEvidence(
    const ReputationQcEvent& event) {
  reputation::SignedWeightUpdateVoteEvidence artifact;
  artifact.protocol_id = event.protocol_id;
  artifact.validator_id = event.vote_signer_id;
  artifact.old_weight_root = event.old_weight_root;
  artifact.old_weight_version = event.old_weight_version;
  artifact.activation_view = event.activation_view;
  artifact.candidate_digest = event.candidate_digest;
  artifact.signature_verified = event.vote_signature_verified;
  artifact.active_weight_root = event.active_weight_root;
  artifact.weight_version = event.weight_version;
  return artifact;
}

reputation::SignedTimeoutVoteEvidence ToSignedTimeoutVoteEvidence(
    const ReputationQcEvent& event) {
  reputation::SignedTimeoutVoteEvidence artifact;
  artifact.protocol_id = event.protocol_id;
  artifact.signer_id = event.vote_signer_id;
  artifact.view_or_round = event.qc_view;
  artifact.high_qc_digest = event.high_qc_digest;
  artifact.signature_verified = event.vote_signature_verified;
  artifact.active_weight_root = event.active_weight_root;
  artifact.weight_version = event.weight_version;
  return artifact;
}

reputation::InvalidTcProposalEvidence ToInvalidTcProposalEvidence(
    const ReputationQcEvent& event) {
  reputation::InvalidTcProposalEvidence artifact;
  artifact.protocol_id = event.protocol_id;
  artifact.leader_id = event.leader_id;
  artifact.view_or_round = event.qc_view;
  artifact.slot_or_height = event.proposal_slot;
  artifact.proposal_hash = event.proposal_hash;
  artifact.proposal_signature_verified = event.proposal_signature_verified;
  artifact.timeout_cert_verified = event.timeout_cert_verified;
  artifact.invalid_reason = event.invalid_reason;
  artifact.active_weight_root = event.active_weight_root;
  artifact.weight_version = event.weight_version;
  return artifact;
}

reputation::VerifiedQcArtifactEvidence ToVerifiedQcArtifactEvidence(
    const ReputationQcEvent& event) {
  reputation::VerifiedQcArtifactEvidence artifact;
  artifact.protocol_id = event.protocol_id;
  artifact.view_or_round = event.qc_view;
  artifact.slot_or_height = event.proposal_slot;
  artifact.qc_hash = event.qc_hash;
  artifact.signer_bitmap = event.signer_bitmap;
  artifact.qc_verified = event.qc_verified;
  artifact.active_weight_root = event.active_weight_root;
  artifact.weight_version = event.weight_version;
  return artifact;
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
    int activation_view,
    const std::vector<int>& prior_peertrust_leader_debt,
    const std::vector<int>& prior_sybil_graph_debt) {
  std::vector<reputation::MetricEvidence> evidence;
  std::vector<reputation::SignedProposalEvidence> signed_proposal_evidence;
  std::vector<reputation::SignedVoteEvidence> signed_vote_evidence;
  std::vector<reputation::InvalidQcProposalEvidence>
      invalid_qc_proposal_evidence;
  std::vector<reputation::SignedWeightUpdateVoteEvidence>
      signed_weight_update_vote_evidence;
  std::vector<reputation::SignedTimeoutVoteEvidence>
      signed_timeout_vote_evidence;
  std::vector<reputation::InvalidTcProposalEvidence>
      invalid_tc_proposal_evidence;
  std::vector<reputation::VerifiedQcArtifactEvidence>
      verified_qc_artifact_evidence;
  evidence.reserve(events.size());
  for (const ReputationQcEvent& event : events) {
    if (event.signed_proposal_artifact) {
      signed_proposal_evidence.push_back(ToSignedProposalEvidence(event));
    } else if (event.signed_vote_artifact) {
      signed_vote_evidence.push_back(ToSignedVoteEvidence(event));
    } else if (event.invalid_qc_proposal_artifact) {
      invalid_qc_proposal_evidence.push_back(ToInvalidQcProposalEvidence(event));
    } else if (event.weight_update_vote_artifact) {
      signed_weight_update_vote_evidence.push_back(
          ToSignedWeightUpdateVoteEvidence(event));
    } else if (event.timeout_vote_artifact) {
      signed_timeout_vote_evidence.push_back(ToSignedTimeoutVoteEvidence(event));
    } else if (event.invalid_tc_proposal_artifact) {
      invalid_tc_proposal_evidence.push_back(ToInvalidTcProposalEvidence(event));
    } else if (event.verified_qc_artifact) {
      verified_qc_artifact_evidence.push_back(ToVerifiedQcArtifactEvidence(event));
    } else {
      evidence.push_back(ToMetricEvidence(event));
    }
  }
  const std::vector<int64_t> weights =
      NormalizeWeights(current_weights, total_replicas);
  const std::string effective_old_weight_root =
      old_weight_root_hex.empty() ? WeightRootHex(weights) : old_weight_root_hex;
  return ToTdCandidate(reputation::ComputeReputationCandidate(
      node_id, total_replicas, window_index, evidence, weights,
      ToNeutralConfig(recovery_config), effective_old_weight_root,
      old_weight_version, activation_view, signed_proposal_evidence,
      signed_vote_evidence, invalid_qc_proposal_evidence,
      signed_weight_update_vote_evidence, signed_timeout_vote_evidence,
      invalid_tc_proposal_evidence, verified_qc_artifact_evidence,
      prior_peertrust_leader_debt, prior_sybil_graph_debt));
}

void RecomputeVoteScoreCandidateRoots(VoteScoreCandidate* candidate) {
  if (candidate == nullptr) {
    return;
  }
  reputation::ReputationCandidate neutral = ToNeutralCandidate(*candidate);
  reputation::RecomputeReputationCandidateRoots(&neutral);
  candidate->next_weights = neutral.next_weights;
  candidate->metric_root_hex = neutral.metric_root_hex;
  candidate->reputation_root_hex = neutral.reputation_root_hex;
  candidate->strong_fault_root_hex = neutral.strong_fault_root_hex;
  candidate->penalty_root_hex = neutral.penalty_root_hex;
  candidate->next_weight_root_hex = WeightRootHex(candidate->next_weights);
  candidate->leader_weights.clear();
  candidate->leader_weight_root_hex.clear();
  candidate->leader_params_version = 0;
  candidate->leader_randomness_ref.clear();
  candidate->candidate_digest_hex = VoteScoreCandidateDigest(
      candidate->total_replicas, candidate->window_index,
      candidate->start_qc_view, candidate->end_qc_view,
      candidate->event_count, candidate->old_weight_root_hex,
      candidate->old_weight_version, candidate->activation_view,
      candidate->metric_root_hex, candidate->next_weight_root_hex,
      candidate->next_weights, /*leader_weight_root_hex=*/"",
      /*leader_params_version=*/0, /*leader_randomness_ref=*/"",
      /*leader_weights=*/{}, candidate->reputation_root_hex,
      candidate->strong_fault_root_hex, candidate->penalty_root_hex);
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
    const std::vector<int64_t>& leader_weights,
    const std::string& reputation_root_hex,
    const std::string& strong_fault_root_hex,
    const std::string& penalty_root_hex) {
  (void)leader_weight_root_hex;
  (void)leader_params_version;
  (void)leader_randomness_ref;
  (void)leader_weights;
  return reputation::ReputationCandidateDigest(
      total_replicas, window_index, start_qc_view, end_qc_view, event_count,
      old_weight_root_hex, old_weight_version, activation_view, metric_root_hex,
      reputation_root_hex, next_weight_root_hex, next_weights,
      strong_fault_root_hex, penalty_root_hex);
}

std::string VoteScoreCandidateToJson(const VoteScoreCandidate& candidate) {
  std::ostringstream out;
  out << "{\"schema\":\"td_hotstuff_reputation_bayes_v4\""
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
      << ",\"reputation_root\":\"" << candidate.reputation_root_hex << "\""
      << ",\"strong_fault_root\":\"" << candidate.strong_fault_root_hex << "\""
      << ",\"penalty_root\":\"" << candidate.penalty_root_hex << "\""
      << ",\"next_weight_root\":\"" << candidate.next_weight_root_hex << "\""
      << ",\"candidate_digest\":\"" << candidate.candidate_digest_hex << "\""
      << ",\"next_weights\":[";
  for (size_t i = 0; i < candidate.next_weights.size(); ++i) {
    if (i != 0) {
      out << ',';
    }
    out << candidate.next_weights[i];
  }
  out << "],\"validators\":[";
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
        << ",\"leader_opportunity_count\":"
        << validator.leader_opportunity_count
        << ",\"leader_score\":" << validator.leader_score
        << ",\"leader_diversity_score\":"
        << validator.leader_diversity_score
        << ",\"peertrust_score\":" << validator.peertrust_score
        << ",\"reviewer_credibility_score\":"
        << validator.reviewer_credibility_score
        << ",\"transaction_context_score\":"
        << validator.transaction_context_score
        << ",\"community_context_score\":"
        << validator.community_context_score
        << ",\"reviewer_entropy_score\":"
        << validator.reviewer_entropy_score
        << ",\"cross_leader_independence_score\":"
        << validator.cross_leader_independence_score
        << ",\"reviewer_overuse_score\":"
        << validator.reviewer_overuse_score
        << ",\"peertrust_leader_debt\":"
        << validator.peertrust_leader_debt
        << ",\"peertrust_debt_delta\":"
        << validator.peertrust_debt_delta
        << ",\"feedback_count\":" << validator.feedback_count
        << ",\"sybil_rank_score\":" << validator.sybil_rank_score
        << ",\"sybil_cut_score\":" << validator.sybil_cut_score
        << ",\"sybil_graph_score\":" << validator.sybil_graph_score
        << ",\"sybil_graph_debt\":" << validator.sybil_graph_debt
        << ",\"sybil_graph_debt_delta\":"
        << validator.sybil_graph_debt_delta
        << ",\"graph_degree\":" << validator.graph_degree
        << ",\"seed_trust_score\":" << validator.seed_trust_score
        << ",\"reputation_score\":" << validator.reputation_score
        << ",\"decay_applied\":" << validator.decay_applied
        << ",\"recovery_credit\":" << validator.recovery_credit
        << ",\"bonus_credit\":" << validator.bonus_credit
        << ",\"strong_fault_count\":" << validator.strong_fault_count
        << ",\"penalty_points\":" << validator.penalty_points
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
      activation_epoch_delay_(
          static_cast<size_t>(IntFromEnv(kWeightUpdateActivationDelayEnv, 2))),
      output_dir_(std::move(output_dir)),
      output_path_(OutputPath(output_dir_, node_id_)),
      window_size_(window_size == 0 ? kDefaultWindowSize : window_size),
      min_candidate_qc_count_(
          MinCandidateQcCountFromEnv(window_size_, epoch_views_)),
      queue_capacity_(queue_capacity == 0 ? kDefaultQueueCapacity
                                          : queue_capacity),
      recovery_config_(RecoveryConfigFromEnv(max_delta)),
      cumulative_strong_fault_counts_(std::max(total_replicas, 0), 0),
      peertrust_leader_debt_(std::max(total_replicas, 0), 0),
      sybil_graph_debt_(std::max(total_replicas, 0), 0) {}

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
  current_window_qc_count_ = 0;
  completed_candidates_.clear();
  if (peertrust_leader_debt_.size() !=
      static_cast<size_t>(std::max(total_replicas_, 0))) {
    peertrust_leader_debt_.assign(std::max(total_replicas_, 0), 0);
  }
  if (sybil_graph_debt_.size() !=
      static_cast<size_t>(std::max(total_replicas_, 0))) {
    sybil_graph_debt_.assign(std::max(total_replicas_, 0), 0);
  }
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
    std::string active_weight_root, int64_t leader_eligible_min_weight,
    std::string available_signer_bitmap, int qc_collector_id) {
  if (!enabled_ || qc_hash.empty()) {
    return false;
  }
  ReputationQcEvent event;
  event.qc_view = qc_view;
  event.qc_hash = qc_hash;
  event.signer_bitmap = signer_bitmap;
  event.available_signer_bitmap = std::move(available_signer_bitmap);
  event.leader_id = leader_id;
  event.qc_collector_id = qc_collector_id;
  event.weight_version = weight_version;
  event.active_weight_root = std::move(active_weight_root);
  event.leader_eligible_min_weight = leader_eligible_min_weight;

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
  return true;
}

bool AsyncVoteScoreReputationPlugin::RecordLeaderOpportunity(
    int view, int leader_id, uint64_t weight_version,
    std::string active_weight_root, int64_t leader_eligible_min_weight) {
  if (!enabled_ || view <= 0 || leader_id <= 0) {
    return false;
  }
  ReputationQcEvent event;
  event.qc_view = view;
  event.leader_id = leader_id;
  event.leader_opportunity = true;
  event.weight_version = weight_version;
  event.active_weight_root = std::move(active_weight_root);
  event.leader_eligible_min_weight = leader_eligible_min_weight;

  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(view);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(view);
    return false;
  }
  queue_.push_back(std::move(event));
  return true;
}

bool AsyncVoteScoreReputationPlugin::RecordSignedProposalArtifact(
    const reputation::SignedProposalEvidence& artifact) {
  if (!enabled_ || !recovery_config_.strong_fault_enabled ||
      !recovery_config_.double_proposal_detection_enabled ||
      artifact.protocol_id.empty() || artifact.leader_id <= 0 ||
      artifact.view_or_round <= 0 || artifact.proposal_hash.empty()) {
    return false;
  }
  ReputationQcEvent event;
  event.signed_proposal_artifact = true;
  event.protocol_id = artifact.protocol_id;
  event.qc_view = artifact.view_or_round;
  event.leader_id = artifact.leader_id;
  event.proposal_slot = artifact.slot_or_height;
  event.proposal_hash = artifact.proposal_hash;
  event.proposal_signature_verified = artifact.signature_verified;
  event.weight_version = artifact.weight_version;
  event.active_weight_root = artifact.active_weight_root;

  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  queue_.push_back(std::move(event));
  return true;
}

bool AsyncVoteScoreReputationPlugin::RecordSignedVoteArtifact(
    const reputation::SignedVoteEvidence& artifact) {
  if (!enabled_ || !recovery_config_.strong_fault_enabled ||
      !recovery_config_.double_vote_detection_enabled ||
      artifact.protocol_id.empty() || artifact.signer_id <= 0 ||
      artifact.view_or_round <= 0 || artifact.proposal_hash.empty()) {
    return false;
  }
  ReputationQcEvent event;
  event.signed_vote_artifact = true;
  event.protocol_id = artifact.protocol_id;
  event.qc_view = artifact.view_or_round;
  event.proposal_slot = artifact.slot_or_height;
  event.vote_signer_id = artifact.signer_id;
  event.vote_proposal_hash = artifact.proposal_hash;
  event.vote_signature_verified = artifact.signature_verified;
  event.weight_version = artifact.weight_version;
  event.active_weight_root = artifact.active_weight_root;

  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  queue_.push_back(std::move(event));
  return true;
}

bool AsyncVoteScoreReputationPlugin::RecordInvalidQcProposalArtifact(
    const reputation::InvalidQcProposalEvidence& artifact) {
  if (!enabled_ || !recovery_config_.strong_fault_enabled ||
      !recovery_config_.invalid_qc_proposal_detection_enabled ||
      artifact.protocol_id.empty() || artifact.leader_id <= 0 ||
      artifact.view_or_round <= 0 || artifact.proposal_hash.empty()) {
    return false;
  }
  ReputationQcEvent event;
  event.invalid_qc_proposal_artifact = true;
  event.protocol_id = artifact.protocol_id;
  event.qc_view = artifact.view_or_round;
  event.leader_id = artifact.leader_id;
  event.proposal_slot = artifact.slot_or_height;
  event.proposal_hash = artifact.proposal_hash;
  event.proposal_signature_verified = artifact.proposal_signature_verified;
  event.qc_verified = artifact.qc_verified;
  event.invalid_reason = artifact.invalid_reason;
  event.weight_version = artifact.weight_version;
  event.active_weight_root = artifact.active_weight_root;

  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  queue_.push_back(std::move(event));
  return true;
}

bool AsyncVoteScoreReputationPlugin::RecordSignedWeightUpdateVoteArtifact(
    const reputation::SignedWeightUpdateVoteEvidence& artifact) {
  if (!enabled_ || !recovery_config_.strong_fault_enabled ||
      !recovery_config_.weight_update_vote_equivocation_detection_enabled ||
      artifact.protocol_id.empty() || artifact.validator_id <= 0 ||
      artifact.old_weight_root.empty() || artifact.activation_view <= 0 ||
      artifact.candidate_digest.empty()) {
    return false;
  }
  ReputationQcEvent event;
  event.weight_update_vote_artifact = true;
  event.protocol_id = artifact.protocol_id;
  event.qc_view = artifact.activation_view;
  event.vote_signer_id = artifact.validator_id;
  event.old_weight_root = artifact.old_weight_root;
  event.old_weight_version = artifact.old_weight_version;
  event.activation_view = artifact.activation_view;
  event.candidate_digest = artifact.candidate_digest;
  event.vote_signature_verified = artifact.signature_verified;
  event.weight_version = artifact.weight_version;
  event.active_weight_root = artifact.active_weight_root;

  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(artifact.activation_view);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(artifact.activation_view);
    return false;
  }
  queue_.push_back(std::move(event));
  return true;
}

bool AsyncVoteScoreReputationPlugin::RecordSignedTimeoutVoteArtifact(
    const reputation::SignedTimeoutVoteEvidence& artifact) {
  if (!enabled_ || !recovery_config_.strong_fault_enabled ||
      !recovery_config_.timeout_vote_equivocation_detection_enabled ||
      artifact.protocol_id.empty() || artifact.signer_id <= 0 ||
      artifact.view_or_round <= 0 || artifact.high_qc_digest.empty()) {
    return false;
  }
  ReputationQcEvent event;
  event.timeout_vote_artifact = true;
  event.protocol_id = artifact.protocol_id;
  event.qc_view = artifact.view_or_round;
  event.vote_signer_id = artifact.signer_id;
  event.high_qc_digest = artifact.high_qc_digest;
  event.vote_signature_verified = artifact.signature_verified;
  event.weight_version = artifact.weight_version;
  event.active_weight_root = artifact.active_weight_root;

  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  queue_.push_back(std::move(event));
  return true;
}

bool AsyncVoteScoreReputationPlugin::RecordInvalidTcProposalArtifact(
    const reputation::InvalidTcProposalEvidence& artifact) {
  if (!enabled_ || !recovery_config_.strong_fault_enabled ||
      !recovery_config_.invalid_tc_proposal_detection_enabled ||
      artifact.protocol_id.empty() || artifact.leader_id <= 0 ||
      artifact.view_or_round <= 0 || artifact.proposal_hash.empty()) {
    return false;
  }
  ReputationQcEvent event;
  event.invalid_tc_proposal_artifact = true;
  event.protocol_id = artifact.protocol_id;
  event.qc_view = artifact.view_or_round;
  event.leader_id = artifact.leader_id;
  event.proposal_slot = artifact.slot_or_height;
  event.proposal_hash = artifact.proposal_hash;
  event.proposal_signature_verified = artifact.proposal_signature_verified;
  event.timeout_cert_verified = artifact.timeout_cert_verified;
  event.invalid_reason = artifact.invalid_reason;
  event.weight_version = artifact.weight_version;
  event.active_weight_root = artifact.active_weight_root;

  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  queue_.push_back(std::move(event));
  return true;
}

bool AsyncVoteScoreReputationPlugin::RecordVerifiedQcArtifact(
    const reputation::VerifiedQcArtifactEvidence& artifact) {
  if (!enabled_ || !recovery_config_.strong_fault_enabled ||
      !recovery_config_.conflicting_qc_detection_enabled ||
      artifact.protocol_id.empty() || artifact.view_or_round <= 0 ||
      artifact.qc_hash.empty() || artifact.signer_bitmap.empty()) {
    return false;
  }
  ReputationQcEvent event;
  event.verified_qc_artifact = true;
  event.protocol_id = artifact.protocol_id;
  event.qc_view = artifact.view_or_round;
  event.proposal_slot = artifact.slot_or_height;
  event.qc_hash = artifact.qc_hash;
  event.signer_bitmap = artifact.signer_bitmap;
  event.qc_verified = artifact.qc_verified;
  event.weight_version = artifact.weight_version;
  event.active_weight_root = artifact.active_weight_root;

  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(artifact.view_or_round);
    return false;
  }
  queue_.push_back(std::move(event));
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
  std::vector<int> peertrust_leader_debt;
  std::vector<int> sybil_graph_debt;
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
        current_window_qc_count_ = 0;
        window_index = window_index_++;
        current_weights = current_weights_;
        old_weight_root_hex = old_weight_root_hex_;
        old_weight_version = old_weight_version_;
        peertrust_leader_debt = peertrust_leader_debt_;
        sybil_graph_debt = sybil_graph_debt_;
      }
    }
    current_window_.push_back(event);
    if (!event.qc_hash.empty()) {
      ++current_window_qc_count_;
    }
    if (window.empty() && current_window_qc_count_ >= window_size_) {
      window = std::move(current_window_);
      current_window_.clear();
      current_window_qc_count_ = 0;
      window_index = window_index_++;
      current_weights = current_weights_;
      old_weight_root_hex = old_weight_root_hex_;
      old_weight_version = old_weight_version_;
      peertrust_leader_debt = peertrust_leader_debt_;
      sybil_graph_debt = sybil_graph_debt_;
    }
    if (window.empty() && recovery_config_.strong_fault_enabled &&
        HasSparseStrongFaultEvidence(current_window_)) {
      window = std::move(current_window_);
      current_window_.clear();
      current_window_qc_count_ = 0;
      window_index = window_index_++;
      current_weights = current_weights_;
      old_weight_root_hex = old_weight_root_hex_;
      old_weight_version = old_weight_version_;
      peertrust_leader_debt = peertrust_leader_debt_;
      sybil_graph_debt = sybil_graph_debt_;
    }
  }
  FlushWindow(output, std::move(window), window_index, std::move(current_weights),
              std::move(old_weight_root_hex), old_weight_version,
              std::move(peertrust_leader_debt), std::move(sybil_graph_debt));
}

void AsyncVoteScoreReputationPlugin::FlushWindow(
    std::ofstream& output, std::vector<ReputationQcEvent> window,
    uint64_t window_index, std::vector<int64_t> current_weights,
    std::string old_weight_root_hex, uint64_t old_weight_version,
    std::vector<int> peertrust_leader_debt,
    std::vector<int> sybil_graph_debt) {
  if (window.empty()) {
    return;
  }
  std::set<std::pair<int, std::string>> unique_qcs;
  for (const ReputationQcEvent& event : window) {
    if (!event.qc_hash.empty()) {
      unique_qcs.insert({event.qc_view, event.qc_hash});
    }
  }
  const uint64_t qc_record_count = unique_qcs.size();
  const bool sparse_direct_strong_fault_window =
      recovery_config_.strong_fault_enabled && qc_record_count < min_candidate_qc_count_ &&
      HasSparseStrongFaultEvidence(window);
  if (qc_record_count < min_candidate_qc_count_ &&
      !sparse_direct_strong_fault_window) {
    return;
  }
  int observed_end_view = 0;
  for (const ReputationQcEvent& event : window) {
    if (event.qc_hash.empty() || event.qc_view <= 0) {
      continue;
    }
    observed_end_view = std::max(observed_end_view, event.qc_view);
  }
  const int evidence_end_view =
      observed_end_view > 0 ? observed_end_view : window.back().qc_view;
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
      activation_view, peertrust_leader_debt, sybil_graph_debt);

  bool applied_cumulative_strong_fault = false;
  if (recovery_config_.strong_fault_enabled) {
    if (cumulative_strong_fault_counts_.size() !=
        static_cast<size_t>(std::max(total_replicas_, 0))) {
      cumulative_strong_fault_counts_.assign(std::max(total_replicas_, 0), 0);
    }
    for (const reputation::StrongFaultRecord& fault :
         candidate.strong_faults) {
      if (fault.validator_id < 1 || fault.validator_id > total_replicas_) {
        continue;
      }
      ++cumulative_strong_fault_counts_[fault.validator_id - 1];
    }

    const int64_t target_weight = std::max<int64_t>(
        recovery_config_.min_weight,
        std::min<int64_t>(recovery_config_.max_weight,
                          recovery_config_.strong_fault_target_weight));
    for (size_t i = 0; i < candidate.validators.size() &&
                       i < cumulative_strong_fault_counts_.size();
         ++i) {
      const uint64_t fault_count = cumulative_strong_fault_counts_[i];
      if (fault_count == 0) {
        if (sparse_direct_strong_fault_window && i < current_weights.size()) {
          ValidatorVoteScore& validator = candidate.validators[i];
          validator.next_weight = current_weights[i];
          if (i < candidate.next_weights.size()) {
            candidate.next_weights[i] = current_weights[i];
          }
        }
        continue;
      }
      ValidatorVoteScore& validator = candidate.validators[i];
      validator.strong_fault_count =
          std::max<uint64_t>(validator.strong_fault_count, fault_count);
      validator.recovery_credit = 0;
      validator.bonus_credit = 0;
      validator.reputation_score = 0;
      validator.penalty_points = std::max<int64_t>(
          validator.penalty_points,
          std::max<int64_t>(0, validator.next_weight - target_weight));
      validator.next_weight = target_weight;
      if (i < candidate.next_weights.size()) {
        candidate.next_weights[i] = target_weight;
      }
      applied_cumulative_strong_fault = true;
    }
  }

  if (epoch_views_ > 0 && window_end_view > 0) {
    candidate.start_qc_view = window_end_view - static_cast<int>(epoch_views_) + 1;
    candidate.end_qc_view = window_end_view;
    candidate.event_count = sparse_direct_strong_fault_window ? 0 : qc_record_count;
    candidate.activation_view = activation_view;
    RecomputeVoteScoreCandidateRoots(&candidate);
  } else if (applied_cumulative_strong_fault) {
    RecomputeVoteScoreCandidateRoots(&candidate);
  }
  if (recovery_config_.peertrust_enabled) {
    std::vector<int> next_peertrust_debt(
        static_cast<size_t>(std::max(total_replicas_, 0)), 0);
    for (const ValidatorVoteScore& validator : candidate.validators) {
      if (validator.validator_id >= 1 &&
          validator.validator_id <= total_replicas_) {
        next_peertrust_debt[validator.validator_id - 1] =
            validator.peertrust_leader_debt;
      }
    }
    std::unique_lock<std::mutex> lock(mutex_);
    peertrust_leader_debt_ = std::move(next_peertrust_debt);
  }
  if (recovery_config_.sybil_graph_enabled) {
    std::vector<int> next_sybil_graph_debt(
        static_cast<size_t>(std::max(total_replicas_, 0)), 0);
    for (const ValidatorVoteScore& validator : candidate.validators) {
      if (validator.validator_id >= 1 &&
          validator.validator_id <= total_replicas_) {
        next_sybil_graph_debt[validator.validator_id - 1] =
            validator.sybil_graph_debt;
      }
    }
    std::unique_lock<std::mutex> lock(mutex_);
    sybil_graph_debt_ = std::move(next_sybil_graph_debt);
  }

  output << VoteScoreCandidateToJson(candidate) << '\n';
  if (++output_records_since_flush_ >= kFlushEveryCandidates) {
    output.flush();
    output_records_since_flush_ = 0;
  }
  const bool changes_weights = candidate.next_weights != current_weights;
  if (changes_weights) {
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
  std::vector<int> peertrust_leader_debt;
  std::vector<int> sybil_graph_debt;
  uint64_t window_index = 0;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!current_window_.empty()) {
      window = std::move(current_window_);
      current_window_.clear();
      current_window_qc_count_ = 0;
      window_index = window_index_++;
      current_weights = current_weights_;
      old_weight_root_hex = old_weight_root_hex_;
      old_weight_version = old_weight_version_;
      peertrust_leader_debt = peertrust_leader_debt_;
      sybil_graph_debt = sybil_graph_debt_;
    }
  }
  FlushWindow(output, std::move(window), window_index, std::move(current_weights),
              std::move(old_weight_root_hex), old_weight_version,
              std::move(peertrust_leader_debt), std::move(sybil_graph_debt));
  output.flush();
}

}  // namespace td_hotstuff
}  // namespace resdb
