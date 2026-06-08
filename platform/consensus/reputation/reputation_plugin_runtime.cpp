#include "platform/consensus/reputation/reputation_plugin_runtime.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <sstream>
#include <utility>

#include "platform/consensus/reputation/reputation_roots.h"

namespace resdb {
namespace consensus {
namespace reputation {
namespace {

std::vector<int64_t> DefaultWeights(int total_replicas) {
  return std::vector<int64_t>(std::max(total_replicas, 0), 1);
}

std::string WeightRootOrDefault(const std::vector<int64_t>& weights,
                                const std::string& root) {
  return root.empty() ? WeightRootHex(weights) : root;
}

std::string AuditWeightsJson(const std::vector<int64_t>& values) {
  std::ostringstream out;
  out << '[';
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      out << ',';
    }
    out << values[i];
  }
  out << ']';
  return out.str();
}

}  // namespace

struct ReputationPluginRuntime::RuntimeEvent {
  enum class Type { kEvidence, kWatermark };
  Type type = Type::kEvidence;
  CertifiedSignerEvidenceRecord evidence;
  int watermark = 0;
};

struct ReputationPluginRuntime::WindowKey {
  uint64_t window_index = 0;
  std::string weight_root_hex;
  uint64_t weight_version = 0;

  bool operator<(const WindowKey& other) const {
    return std::tie(window_index, weight_root_hex, weight_version) <
           std::tie(other.window_index, other.weight_root_hex,
                    other.weight_version);
  }
};

struct ReputationPluginRuntime::WindowBuffer {
  int start_view = 0;
  int end_view = 0;
  ReputationWeightSnapshot snapshot;
  std::vector<CertifiedSignerEvidenceRecord> evidence;
  std::set<std::tuple<int, int, std::string>> seen;
};

ReputationCandidateKey ReputationCandidateKey::FromCandidate(
    const ReputationCandidate& candidate) {
  ReputationCandidateKey key;
  key.old_weight_root_hex = candidate.old_weight_root_hex;
  key.old_weight_version = candidate.old_weight_version;
  key.start_view = candidate.start_view;
  key.end_view = candidate.end_view;
  key.activation_view = candidate.activation_view;
  key.candidate_digest_hex = candidate.candidate_digest_hex;
  return key;
}

ReputationPluginRuntime::ReputationPluginRuntime(
    ReputationRuntimeOptions options)
    : options_(std::move(options)) {
  options_.window_size_views = std::max<size_t>(1, options_.window_size_views);
  options_.queue_capacity = std::max<size_t>(1, options_.queue_capacity);
  options_.activation_delay_windows = std::max(1, options_.activation_delay_windows);
  if (options_.total_replicas <= 0 && !options_.initial_weights.empty()) {
    options_.total_replicas = static_cast<int>(options_.initial_weights.size());
  }
  if (options_.initial_weights.empty()) {
    options_.initial_weights = DefaultWeights(options_.total_replicas);
  }
  active_snapshot_.weights = options_.initial_weights;
  active_snapshot_.weight_root_hex =
      WeightRootOrDefault(active_snapshot_.weights, options_.initial_weight_root);
  active_snapshot_.weight_version = options_.initial_weight_version;
  known_weights_[{active_snapshot_.weight_root_hex,
                  active_snapshot_.weight_version}] = active_snapshot_.weights;
  if (options_.audit_jsonl_enabled && !options_.audit_jsonl_path.empty()) {
    audit_file_.open(options_.audit_jsonl_path, std::ios::out | std::ios::app);
    if (!audit_file_.is_open()) {
      LOG(WARNING) << "failed to open reputation runtime audit path:"
                   << options_.audit_jsonl_path;
    }
  }
}

ReputationPluginRuntime::~ReputationPluginRuntime() { Stop(); }

void ReputationPluginRuntime::Start() {
  if (!options_.enabled) {
    return;
  }
  std::lock_guard<std::mutex> lk(mutex_);
  if (started_) {
    return;
  }
  stop_ = false;
  started_ = true;
  worker_ = std::thread(&ReputationPluginRuntime::WorkerLoop, this);
}

void ReputationPluginRuntime::Stop() {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!started_) {
      pending_.clear();
      return;
    }
    stop_ = true;
  }
  cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
  std::lock_guard<std::mutex> lk(mutex_);
  started_ = false;
  stop_ = false;
  pending_.clear();
}

bool ReputationPluginRuntime::RecordEvidence(
    CertifiedSignerEvidenceRecord evidence) {
  RuntimeEvent event;
  event.type = RuntimeEvent::Type::kEvidence;
  event.evidence = std::move(evidence);
  return Enqueue(std::move(event));
}

bool ReputationPluginRuntime::AdvanceWatermark(int view_or_round) {
  RuntimeEvent event;
  event.type = RuntimeEvent::Type::kWatermark;
  event.watermark = view_or_round;
  return Enqueue(std::move(event));
}

bool ReputationPluginRuntime::Enqueue(RuntimeEvent event) {
  if (!options_.enabled) {
    return false;
  }
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (stop_ || pending_.size() >= options_.queue_capacity) {
      dropped_count_.fetch_add(1);
      LOG_EVERY_N(WARNING, 1000)
          << "reputation plugin runtime queue full, dropped:"
          << dropped_count_.load();
      return false;
    }
    pending_.push_back(std::move(event));
    queued_count_.fetch_add(1);
  }
  cv_.notify_one();
  return true;
}

void ReputationPluginRuntime::UpdateActiveWeights(
    ReputationWeightSnapshot snapshot) {
  if (snapshot.weights.empty()) {
    snapshot.weights = DefaultWeights(options_.total_replicas);
  }
  snapshot.weight_root_hex =
      WeightRootOrDefault(snapshot.weights, snapshot.weight_root_hex);
  std::lock_guard<std::mutex> lk(mutex_);
  active_snapshot_ = std::move(snapshot);
  known_weights_[{active_snapshot_.weight_root_hex,
                  active_snapshot_.weight_version}] = active_snapshot_.weights;
}

std::vector<ReputationCandidate>
ReputationPluginRuntime::TakeCompletedCandidates() {
  std::deque<ReputationCandidate> drained;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    drained.swap(completed_);
  }
  std::vector<ReputationCandidate> output;
  output.reserve(drained.size());
  while (!drained.empty()) {
    output.push_back(std::move(drained.front()));
    drained.pop_front();
  }
  return output;
}

std::optional<ReputationCandidate> ReputationPluginRuntime::FindLocalCandidate(
    const ReputationCandidateKey& key) const {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = completed_index_.find(key);
  if (it == completed_index_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void ReputationPluginRuntime::WorkerLoop() {
  while (true) {
    RuntimeEvent event;
    bool has_event = false;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      cv_.wait(lk, [this]() { return stop_ || !pending_.empty(); });
      if (!pending_.empty()) {
        event = std::move(pending_.front());
        pending_.pop_front();
        has_event = true;
      } else if (stop_) {
        break;
      }
    }
    if (!has_event) {
      continue;
    }
    if (event.type == RuntimeEvent::Type::kEvidence) {
      ProcessEvidence(std::move(event.evidence));
    } else {
      ProcessWatermark(event.watermark);
    }
  }
}

ReputationWeightSnapshot ReputationPluginRuntime::SnapshotForEvidence(
    const CertifiedSignerEvidenceRecord& evidence) const {
  ReputationWeightSnapshot snapshot;
  snapshot.weight_root_hex = evidence.weight_root_hex.empty()
                                 ? active_snapshot_.weight_root_hex
                                 : evidence.weight_root_hex;
  snapshot.weight_version = evidence.weight_version;
  if (!evidence.active_weights.empty()) {
    snapshot.weights = evidence.active_weights;
  } else {
    auto it = known_weights_.find({snapshot.weight_root_hex,
                                   snapshot.weight_version});
    if (it != known_weights_.end()) {
      snapshot.weights = it->second;
    } else {
      snapshot.weights = active_snapshot_.weights;
    }
  }
  return snapshot;
}

void ReputationPluginRuntime::ProcessEvidence(
    CertifiedSignerEvidenceRecord evidence) {
  if (evidence.view_or_round < 0) {
    return;
  }
  const uint64_t window_index =
      static_cast<uint64_t>(evidence.view_or_round) / options_.window_size_views;
  const int window_start =
      static_cast<int>(window_index * options_.window_size_views);
  const int window_end =
      static_cast<int>(window_start + options_.window_size_views);
  ReputationWeightSnapshot snapshot = SnapshotForEvidence(evidence);
  WindowKey key;
  key.window_index = window_index;
  key.weight_root_hex = snapshot.weight_root_hex;
  key.weight_version = snapshot.weight_version;

  std::lock_guard<std::mutex> lk(mutex_);
  WindowBuffer& buffer = windows_[key];
  if (buffer.evidence.empty()) {
    buffer.start_view = window_start;
    buffer.end_view = window_end;
    buffer.snapshot = std::move(snapshot);
  }
  const auto dedupe_key = std::make_tuple(
      evidence.view_or_round, evidence.slot_or_height, evidence.artifact_digest);
  if (!buffer.seen.insert(dedupe_key).second) {
    return;
  }
  buffer.evidence.push_back(std::move(evidence));
}

void ReputationPluginRuntime::ProcessWatermark(int view_or_round) {
  std::vector<std::pair<WindowKey, WindowBuffer>> ready;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    for (auto it = windows_.begin(); it != windows_.end();) {
      if (view_or_round >= it->second.end_view) {
        ready.emplace_back(it->first, std::move(it->second));
        it = windows_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto& item : ready) {
    FinalizeWindow(item.first, &item.second);
  }
}

void ReputationPluginRuntime::FinalizeWindow(const WindowKey& key,
                                             WindowBuffer* buffer) {
  if (buffer == nullptr || buffer->evidence.empty()) {
    return;
  }
  std::sort(buffer->evidence.begin(), buffer->evidence.end(),
            [](const CertifiedSignerEvidenceRecord& lhs,
               const CertifiedSignerEvidenceRecord& rhs) {
              return std::tie(lhs.view_or_round, lhs.slot_or_height,
                              lhs.artifact_digest) <
                     std::tie(rhs.view_or_round, rhs.slot_or_height,
                              rhs.artifact_digest);
            });

  ReputationWindowInput input;
  input.total_replicas = options_.total_replicas;
  input.window_index = key.window_index;
  input.current_weights = buffer->snapshot.weights;
  input.old_weight_root_hex = buffer->snapshot.weight_root_hex;
  input.old_weight_version = buffer->snapshot.weight_version;
  input.activation_view = buffer->end_view +
                          options_.activation_delay_windows *
                              static_cast<int>(options_.window_size_views);
  input.certified_signer_evidence.reserve(buffer->evidence.size());
  for (const CertifiedSignerEvidenceRecord& record : buffer->evidence) {
    CertifiedSignerEvidence evidence;
    evidence.view_or_round = record.view_or_round;
    evidence.leader_id = record.leader_id;
    evidence.artifact_digest = record.artifact_digest;
    evidence.signer_bitmap = record.signer_bitmap;
    evidence.available_signer_bitmap = record.available_signer_bitmap;
    input.certified_signer_evidence.push_back(std::move(evidence));
  }

  ReputationCandidate candidate = ComputeReputationCandidate(input, options_.config);
  candidate.window_index = key.window_index;
  candidate.start_view = buffer->start_view;
  candidate.end_view = buffer->end_view;
  candidate.event_count = input.certified_signer_evidence.size();
  candidate.activation_view = input.activation_view;
  candidate.old_weight_root_hex = input.old_weight_root_hex;
  candidate.old_weight_version = input.old_weight_version;
  RecomputeReputationCandidateRoots(&candidate);
  WriteAudit(candidate);
  PushCompleted(std::move(candidate));
  computed_window_count_.fetch_add(1);
}

void ReputationPluginRuntime::PushCompleted(ReputationCandidate candidate) {
  const ReputationCandidateKey key = ReputationCandidateKey::FromCandidate(candidate);
  std::lock_guard<std::mutex> lk(mutex_);
  completed_index_[key] = candidate;
  completed_.push_back(std::move(candidate));
}

void ReputationPluginRuntime::WriteAudit(const ReputationCandidate& candidate) {
  if (!audit_file_.is_open()) {
    return;
  }
  audit_file_ << "{\"algorithm\":\"" << candidate.algorithm << "\",";
  audit_file_ << "\"window_index\":" << candidate.window_index << ',';
  audit_file_ << "\"start_view\":" << candidate.start_view << ',';
  audit_file_ << "\"end_view\":" << candidate.end_view << ',';
  audit_file_ << "\"event_count\":" << candidate.event_count << ',';
  audit_file_ << "\"old_weight_version\":" << candidate.old_weight_version << ',';
  audit_file_ << "\"old_weight_root\":\"" << candidate.old_weight_root_hex << "\",";
  audit_file_ << "\"activation_view\":" << candidate.activation_view << ',';
  audit_file_ << "\"candidate_digest\":\"" << candidate.candidate_digest_hex << "\",";
  audit_file_ << "\"next_weights\":" << AuditWeightsJson(candidate.next_weights);
  audit_file_ << "}\n";
  audit_file_.flush();
}

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
