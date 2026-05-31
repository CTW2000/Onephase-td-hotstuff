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

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr const char* kEnableEnv = "TD_HS_REPUTATION_ENABLE";
constexpr const char* kWindowSizeEnv = "TD_HS_REPUTATION_WINDOW_SIZE";
constexpr const char* kOutputDirEnv = "TD_HS_REPUTATION_OUTPUT_DIR";
constexpr const char* kQueueCapacityEnv = "TD_HS_REPUTATION_QUEUE_CAPACITY";
constexpr const char* kMaxDeltaEnv = "TD_HS_REPUTATION_MAX_DELTA";
constexpr size_t kDefaultWindowSize = 4096;
constexpr size_t kDefaultQueueCapacity = 65536;
constexpr int kDefaultMaxDelta = 2;
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

int MeanVoteScore(const std::vector<ValidatorVoteScore>& validators) {
  if (validators.empty()) {
    return 0;
  }
  uint64_t sum = 0;
  for (const auto& validator : validators) {
    sum += validator.vote_score;
  }
  return RoundedDivide(sum, validators.size());
}

int ClampDelta(int delta, int max_delta) {
  const int cap = std::max(max_delta, 0);
  return std::max(-cap, std::min(cap, delta));
}

std::string MetricCanonical(const VoteScoreCandidate& candidate) {
  std::ostringstream out;
  out << "td_hotstuff_reputation_vote_metric_v1|" << candidate.node_id << '|'
      << candidate.total_replicas << '|' << candidate.window_index << '|'
      << candidate.start_qc_view << '|' << candidate.end_qc_view << '|'
      << candidate.event_count;
  for (const auto& validator : candidate.validators) {
    out << '|' << validator.validator_id << ':' << validator.opportunities << ':'
        << validator.inclusions << ':' << validator.vote_score;
  }
  return out.str();
}

std::string WeightCanonical(const VoteScoreCandidate& candidate) {
  std::ostringstream out;
  out << "td_hotstuff_reputation_next_weight_v1|" << candidate.node_id << '|'
      << candidate.total_replicas << '|' << candidate.window_index;
  for (const auto& validator : candidate.validators) {
    out << '|' << validator.validator_id << ':' << validator.current_weight << ':'
        << validator.next_weight;
  }
  return out.str();
}

std::string CandidateCanonical(const VoteScoreCandidate& candidate) {
  std::ostringstream out;
  out << "td_hotstuff_reputation_candidate_v1|" << candidate.node_id << '|'
      << candidate.total_replicas << '|' << candidate.window_index << '|'
      << candidate.start_qc_view << '|' << candidate.end_qc_view << '|'
      << candidate.event_count << '|' << candidate.metric_root_hex << '|'
      << candidate.next_weight_root_hex;
  return out.str();
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

VoteScoreCandidate ComputeVoteScoreCandidate(
    int node_id, int total_replicas, uint64_t window_index,
    const std::vector<ReputationQcEvent>& events,
    const std::vector<int64_t>& current_weights, int max_delta) {
  VoteScoreCandidate candidate;
  candidate.node_id = node_id;
  candidate.total_replicas = total_replicas;
  candidate.window_index = window_index;
  candidate.event_count = events.size();
  if (!events.empty()) {
    candidate.start_qc_view = events.front().qc_view;
    candidate.end_qc_view = events.back().qc_view;
  }

  const std::vector<int64_t> weights = NormalizeWeights(current_weights, total_replicas);
  candidate.validators.resize(std::max(total_replicas, 0));
  for (int i = 0; i < total_replicas; ++i) {
    ValidatorVoteScore& validator = candidate.validators[i];
    validator.validator_id = i + 1;
    validator.opportunities = events.size();
    validator.current_weight = weights[i];
  }

  for (const ReputationQcEvent& event : events) {
    for (int signer : DecodeSignerBitmap(event.signer_bitmap, total_replicas)) {
      if (signer >= 1 && signer <= total_replicas) {
        ++candidate.validators[signer - 1].inclusions;
      }
    }
  }

  for (ValidatorVoteScore& validator : candidate.validators) {
    validator.vote_score = VoteScore(validator.inclusions, validator.opportunities);
  }

  const int mean_vote_score = MeanVoteScore(candidate.validators);
  for (ValidatorVoteScore& validator : candidate.validators) {
    const int raw_delta = (validator.vote_score - mean_vote_score) / 20;
    const int delta = ClampDelta(raw_delta, max_delta);
    validator.next_weight = ClampWeight(validator.current_weight + delta);
  }

  candidate.metric_root_hex = HashHex(MetricCanonical(candidate));
  candidate.next_weight_root_hex = HashHex(WeightCanonical(candidate));
  candidate.candidate_digest_hex = HashHex(CandidateCanonical(candidate));
  return candidate;
}

std::string VoteScoreCandidateToJson(const VoteScoreCandidate& candidate) {
  std::ostringstream out;
  out << "{\"schema\":\"td_hotstuff_reputation_vote_score_v1\""
      << ",\"node_id\":" << candidate.node_id
      << ",\"total_replicas\":" << candidate.total_replicas
      << ",\"window_index\":" << candidate.window_index
      << ",\"start_qc_view\":" << candidate.start_qc_view
      << ",\"end_qc_view\":" << candidate.end_qc_view
      << ",\"event_count\":" << candidate.event_count
      << ",\"metric_root\":\"" << candidate.metric_root_hex << "\""
      << ",\"next_weight_root\":\"" << candidate.next_weight_root_hex << "\""
      << ",\"candidate_digest\":\"" << candidate.candidate_digest_hex << "\""
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

bool AsyncVoteScoreReputationPlugin::RecordQc(
    int qc_view, const std::string& qc_hash,
    const std::string& signer_bitmap) {
  if (!enabled_ || qc_hash.empty()) {
    return false;
  }
  ReputationQcEvent event;
  event.qc_view = qc_view;
  event.qc_hash = qc_hash;
  event.signer_bitmap = signer_bitmap;

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
  current_window_.push_back(event);
  if (current_window_.size() >= window_size_) {
    FlushWindow(output);
  }
}

void AsyncVoteScoreReputationPlugin::FlushWindow(std::ofstream& output) {
  if (current_window_.empty()) {
    return;
  }
  const VoteScoreCandidate candidate = ComputeVoteScoreCandidate(
      node_id_, total_replicas_, window_index_, current_window_, current_weights_,
      max_delta_);
  output << VoteScoreCandidateToJson(candidate) << '\n';
  output.flush();
  current_window_.clear();
  ++window_index_;
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

  FlushWindow(output);
  output.flush();
}

}  // namespace td_hotstuff
}  // namespace resdb
