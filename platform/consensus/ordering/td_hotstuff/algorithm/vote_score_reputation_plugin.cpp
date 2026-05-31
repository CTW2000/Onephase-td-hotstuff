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
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr const char* kEnableEnv = "TD_HS_REPUTATION_ENABLE";
constexpr const char* kWindowSizeEnv = "TD_HS_REPUTATION_WINDOW_SIZE";
constexpr const char* kOutputDirEnv = "TD_HS_REPUTATION_OUTPUT_DIR";
constexpr const char* kQueueCapacityEnv = "TD_HS_REPUTATION_QUEUE_CAPACITY";
constexpr const char* kMaxDeltaEnv = "TD_HS_REPUTATION_MAX_DELTA";
constexpr const char* kAlgorithmBayesV2 = "bayes_v2";
constexpr const char* kWeightUpdateEpochViewsEnv = "TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS";
constexpr const char* kWeightUpdateActivationDelayEnv = "TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY";
constexpr size_t kDefaultWindowSize = 4096;
constexpr size_t kDefaultQueueCapacity = 65536;
constexpr int kDefaultMaxDelta = 1;
constexpr int64_t kMinWeight = 1;
constexpr int64_t kMaxWeight = 100;

bool ReputationEnabledFromEnv() {
  const char* enabled = std::getenv(kEnableEnv);
  return enabled != nullptr && std::string(enabled) == "1";
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

int ClampDelta(int delta, int max_delta) {
  const int cap = std::max(max_delta, 0);
  return std::max(-cap, std::min(cap, delta));
}

int DefaultLeaderForView(int view, int total_replicas) {
  if (view <= 0 || total_replicas <= 0) {
    return 0;
  }
  return (view % total_replicas) + 1;
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
  out << "td_hotstuff_reputation_metric_v2|" << candidate.algorithm << '|'
      << candidate.total_replicas << '|' << candidate.window_index << '|'
      << candidate.start_qc_view << '|' << candidate.end_qc_view << '|'
      << candidate.event_count;
  for (const auto& validator : candidate.validators) {
    out << '|' << validator.validator_id << ':' << validator.opportunities << ':'
        << validator.inclusions << ':' << validator.vote_score << ':'
        << validator.leader_certified_count << ':'
        << validator.leader_gap_count << ':' << validator.leader_score << ':'
        << validator.leader_diversity_score << ':'
        << validator.reputation_score;
  }
  return out.str();
}

std::string CandidateCanonicalFromParts(
    int total_replicas, uint64_t window_index, int start_qc_view,
    int end_qc_view, uint64_t event_count,
    const std::string& old_weight_root_hex, uint64_t old_weight_version,
    int activation_view, const std::string& metric_root_hex,
    const std::string& next_weight_root_hex,
    const std::vector<int64_t>& next_weights) {
  std::ostringstream out;
  out << "td_hotstuff_reputation_candidate_v1|" << total_replicas << '|'
      << window_index << '|' << start_qc_view << '|' << end_qc_view << '|'
      << event_count << '|' << old_weight_root_hex << '|'
      << old_weight_version << '|' << activation_view << '|' << metric_root_hex
      << '|' << next_weight_root_hex;
  for (size_t i = 0; i < next_weights.size(); ++i) {
    out << '|' << (i + 1) << ':' << next_weights[i];
  }
  return out.str();
}

int ActivationViewForWindow(int end_qc_view, size_t epoch_views,
                            size_t activation_epoch_delay) {
  if (end_qc_view <= 0 || epoch_views == 0) {
    return 0;
  }
  const size_t delay = std::max<size_t>(activation_epoch_delay, 1);
  return static_cast<int>(((static_cast<size_t>(end_qc_view) / epoch_views) +
                           delay) * epoch_views);
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
  VoteScoreCandidate candidate;
  candidate.algorithm = kAlgorithmBayesV2;
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
    validator.opportunities = events.size();
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

  for (const ReputationQcEvent& event : ordered_events) {
    std::vector<int> signers = DecodeSignerBitmap(event.signer_bitmap,
                                                  total_replicas);
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

  for (size_t i = 1; i < ordered_events.size(); ++i) {
    const int previous_view = ordered_events[i - 1].qc_view;
    const int current_view = ordered_events[i].qc_view;
    if (current_view <= previous_view + 1) {
      continue;
    }
    for (int missing_view = previous_view + 1; missing_view < current_view;
         ++missing_view) {
      const int leader = DefaultLeaderForView(missing_view, total_replicas);
      if (leader >= 1 && leader <= total_replicas) {
        ++candidate.validators[leader - 1].leader_gap_count;
      }
    }
  }

  for (ValidatorVoteScore& validator : candidate.validators) {
    validator.vote_score = VoteScore(validator.inclusions,
                                     validator.opportunities);
    const int idx = validator.validator_id - 1;
    const int diversity_score =
        idx >= 0 && idx < static_cast<int>(diversity_count.size()) &&
                diversity_count[idx] > 0
            ? RoundedDivide(diversity_sum[idx], diversity_count[idx])
            : 100;
    const int variation_score =
        idx >= 0 && idx < static_cast<int>(variation_count.size()) &&
                variation_count[idx] > 0
            ? RoundedDivide(variation_sum[idx], variation_count[idx])
            : 100;
    validator.leader_diversity_score = std::max(
        0, std::min(100, RoundedDivide(diversity_score + variation_score, 2)));
    validator.leader_score = validator.leader_diversity_score;
    const int gap_penalty = static_cast<int>(std::min<uint64_t>(
        100, validator.leader_gap_count * 10));
    validator.reputation_score = std::max(0, 100 - gap_penalty);

    if (validator.leader_gap_count > validator.leader_certified_count) {
      const int delta = ClampDelta(-static_cast<int>(validator.leader_gap_count -
                                                     validator.leader_certified_count),
                                   max_delta);
      validator.next_weight = ClampWeight(validator.current_weight + delta);
    } else {
      validator.next_weight = ClampWeight(validator.current_weight);
    }
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
  candidate->metric_root_hex = HashHex(MetricCanonical(*candidate));
  candidate->next_weight_root_hex = WeightRootHex(candidate->next_weights);
  candidate->candidate_digest_hex = VoteScoreCandidateDigest(
      candidate->total_replicas, candidate->window_index,
      candidate->start_qc_view, candidate->end_qc_view,
      candidate->event_count, candidate->old_weight_root_hex,
      candidate->old_weight_version, candidate->activation_view,
      candidate->metric_root_hex, candidate->next_weight_root_hex,
      candidate->next_weights);
}

std::string VoteScoreCandidateDigest(
    int total_replicas, uint64_t window_index, int start_qc_view,
    int end_qc_view, uint64_t event_count,
    const std::string& old_weight_root_hex, uint64_t old_weight_version,
    int activation_view, const std::string& metric_root_hex,
    const std::string& next_weight_root_hex,
    const std::vector<int64_t>& next_weights) {
  return HashHex(CandidateCanonicalFromParts(
      total_replicas, window_index, start_qc_view, end_qc_view, event_count,
      old_weight_root_hex, old_weight_version, activation_view, metric_root_hex,
      next_weight_root_hex, next_weights));
}

std::string VoteScoreCandidateToJson(const VoteScoreCandidate& candidate) {
  std::ostringstream out;
  out << "{\"schema\":\"td_hotstuff_reputation_bayes_v2\""
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
      << ",\"candidate_digest\":\"" << candidate.candidate_digest_hex << "\""
      << ",\"next_weights\":[";
  for (size_t i = 0; i < candidate.next_weights.size(); ++i) {
    if (i != 0) {
      out << ',';
    }
    out << candidate.next_weights[i];
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
        << ",\"leader_gap_count\":" << validator.leader_gap_count
        << ",\"leader_score\":" << validator.leader_score
        << ",\"leader_diversity_score\":"
        << validator.leader_diversity_score
        << ",\"reputation_score\":" << validator.reputation_score
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
      activation_epoch_delay_(SizeFromEnv(kWeightUpdateActivationDelayEnv, 1)),
      output_dir_(std::move(output_dir)),
      output_path_(OutputPath(output_dir_, node_id_)),
      window_size_(window_size == 0 ? kDefaultWindowSize : window_size),
      queue_capacity_(queue_capacity == 0 ? kDefaultQueueCapacity
                                          : queue_capacity),
      max_delta_(std::max(max_delta, 0)) {}

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
      IntFromEnv(kMaxDeltaEnv, kDefaultMaxDelta));
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
    current_window_.push_back(event);
    if (current_window_.size() < window_size_) {
      return;
    }
    window = std::move(current_window_);
    current_window_.clear();
    window_index = window_index_++;
    current_weights = current_weights_;
    old_weight_root_hex = old_weight_root_hex_;
    old_weight_version = old_weight_version_;
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
  const int activation_view = ActivationViewForWindow(
      window.back().qc_view, epoch_views_, activation_epoch_delay_);
  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      node_id_, total_replicas_, window_index, window, current_weights,
      max_delta_, old_weight_root_hex, old_weight_version, activation_view);
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
