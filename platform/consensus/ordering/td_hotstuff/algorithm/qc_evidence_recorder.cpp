#include "platform/consensus/ordering/td_hotstuff/algorithm/qc_evidence_recorder.h"

#include <memory>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>

#include <glog/logging.h>

#include "platform/consensus/ordering/td_hotstuff/algorithm/vote_score_reputation_plugin.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr const char* kEnableEnv = "TD_HS_EVIDENCE_ENABLE";
constexpr const char* kOutputDirEnv = "TD_HS_EVIDENCE_OUTPUT_DIR";
constexpr const char* kQueueCapacityEnv = "TD_HS_EVIDENCE_QUEUE_CAPACITY";
constexpr size_t kDefaultQueueCapacity = 65536;
constexpr size_t kFlushEveryRecords = 64;

bool EvidenceEnabledFromEnv() {
  const char* enabled = std::getenv(kEnableEnv);
  return enabled != nullptr && std::string(enabled) == "1";
}

size_t QueueCapacityFromEnv() {
  const char* raw_capacity = std::getenv(kQueueCapacityEnv);
  if (raw_capacity == nullptr || std::string(raw_capacity).empty()) {
    return kDefaultQueueCapacity;
  }
  try {
    const size_t capacity = std::stoull(raw_capacity);
    return capacity == 0 ? kDefaultQueueCapacity : capacity;
  } catch (const std::exception&) {
    LOG(WARNING) << "invalid " << kQueueCapacityEnv << ":" << raw_capacity
                 << ", use default:" << kDefaultQueueCapacity;
    return kDefaultQueueCapacity;
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
  path /= "td_hotstuff_qc_evidence_node_" + std::to_string(node_id) + ".jsonl";
  return path.string();
}

}  // namespace

std::string HexEncode(const std::string& data) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(data.size() * 2);
  for (unsigned char ch : data) {
    hex.push_back(kHex[ch >> 4]);
    hex.push_back(kHex[ch & 0x0f]);
  }
  return hex;
}

std::string SerializeQcEvidenceRecord(const QcEvidenceRecord& record) {
  std::ostringstream out;
  out << "{\"schema\":\"td_hotstuff_qc_evidence_v1\","
      << "\"node_id\":" << record.node_id << ","
      << "\"total_replicas\":" << record.total_replicas << ","
      << "\"qc_view\":" << record.qc_view << ","
      << "\"qc_hash_hex\":\"" << HexEncode(record.qc_hash) << "\","
      << "\"signer_bitmap_hex\":\"" << HexEncode(record.signer_bitmap)
      << "\"}";
  return out.str();
}

AsyncQcEvidenceRecorder::AsyncQcEvidenceRecorder(int node_id,
                                                 int total_replicas,
                                                 std::string output_dir,
                                                 size_t queue_capacity)
    : AsyncQcEvidenceRecorder(node_id, total_replicas, std::move(output_dir),
                              queue_capacity, true, nullptr) {}

AsyncQcEvidenceRecorder::AsyncQcEvidenceRecorder(int node_id,
                                                 int total_replicas,
                                                 std::string output_dir,
                                                 size_t queue_capacity,
                                                 bool write_json,
                                                 std::unique_ptr<AsyncVoteScoreReputationPlugin> reputation_plugin)
    : node_id_(node_id),
      total_replicas_(total_replicas),
      output_dir_(std::move(output_dir)),
      output_path_(OutputPath(output_dir_, node_id_)),
      queue_capacity_(queue_capacity == 0 ? kDefaultQueueCapacity
                                          : queue_capacity),
      write_json_(write_json),
      enabled_(write_json || reputation_plugin != nullptr),
      reputation_plugin_(std::move(reputation_plugin)) {}

AsyncQcEvidenceRecorder::~AsyncQcEvidenceRecorder() { Stop(); }

AsyncQcEvidenceRecorder AsyncQcEvidenceRecorder::Disabled(int node_id) {
  return AsyncQcEvidenceRecorder(node_id, 0, ".", kDefaultQueueCapacity, false,
                                 nullptr);
}

std::unique_ptr<AsyncQcEvidenceRecorder> AsyncQcEvidenceRecorder::CreateFromEnv(
    int node_id, int total_replicas,
    const std::vector<int64_t>& current_weights) {
  std::unique_ptr<AsyncVoteScoreReputationPlugin> reputation_plugin =
      AsyncVoteScoreReputationPlugin::CreateFromEnv(node_id, total_replicas,
                                                    current_weights);
  const bool write_json = EvidenceEnabledFromEnv();
  if (!write_json && reputation_plugin == nullptr) {
    return nullptr;
  }
  std::unique_ptr<AsyncQcEvidenceRecorder> recorder(new AsyncQcEvidenceRecorder(
      node_id, total_replicas, OutputDirFromEnv(), QueueCapacityFromEnv(),
      write_json, std::move(reputation_plugin)));
  recorder->Start();
  return recorder;
}

void AsyncQcEvidenceRecorder::Start() {
  if (!enabled_ || started_) {
    return;
  }
  stopping_.store(false);
  started_ = true;
  worker_ = std::thread(&AsyncQcEvidenceRecorder::WorkerLoop, this);
}

void AsyncQcEvidenceRecorder::Stop() {
  if (!started_) {
    return;
  }
  stopping_.store(true);
  cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
  started_ = false;
  if (reputation_plugin_ != nullptr) {
    reputation_plugin_->Stop();
  }
}

bool AsyncQcEvidenceRecorder::Enqueue(const QcEvidenceRecord& record) {
  if (!enabled_) {
    return false;
  }
  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(record.qc_view);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(record.qc_view);
    return false;
  }
  queue_.push_back(record);
  cv_.notify_one();
  return true;
}

bool AsyncQcEvidenceRecorder::RecordQc(int qc_view, const std::string& qc_hash,
                                       const std::string& signer_bitmap) {
  if (!enabled_ || qc_hash.empty()) {
    return false;
  }
  QcEvidenceRecord record;
  record.node_id = node_id_;
  record.total_replicas = total_replicas_;
  record.qc_view = qc_view;
  record.qc_hash = qc_hash;
  record.signer_bitmap = signer_bitmap;
  return Enqueue(record);
}

std::vector<VoteScoreCandidate>
AsyncQcEvidenceRecorder::TakeCompletedReputationCandidates() {
  if (reputation_plugin_ == nullptr) {
    return {};
  }
  return reputation_plugin_->TakeCompletedCandidates();
}

void AsyncQcEvidenceRecorder::UpdateReputationWeights(
    std::vector<int64_t> current_weights, std::string old_weight_root_hex,
    uint64_t old_weight_version) {
  if (reputation_plugin_ == nullptr) {
    return;
  }
  reputation_plugin_->UpdateCurrentWeights(std::move(current_weights),
                                           std::move(old_weight_root_hex),
                                           old_weight_version);
}

void AsyncQcEvidenceRecorder::DropRecord(int qc_view) {
  uint64_t dropped = dropped_count_.fetch_add(1) + 1;
  if (dropped == 1 || dropped % 1000 == 0) {
    LOG(WARNING) << "dropped TD-Hotstuff QC evidence record, node:" << node_id_
                 << " view:" << qc_view << " dropped_count:" << dropped;
  }
}

void AsyncQcEvidenceRecorder::WorkerLoop() {
  bool write_json = write_json_;
  std::ofstream output;
  if (write_json) {
    std::filesystem::create_directories(output_dir_);
    output.open(output_path_, std::ios::app);
    if (!output.is_open()) {
      LOG(ERROR) << "open TD-Hotstuff QC evidence output fail:" << output_path_;
      write_json = false;
      if (reputation_plugin_ == nullptr) {
        return;
      }
    }
  }

  size_t pending_flush = 0;
  while (true) {
    QcEvidenceRecord record;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
        return stopping_.load() || !queue_.empty();
      });
      if (queue_.empty()) {
        if (stopping_.load()) {
          break;
        }
        if (write_json) {
          output.flush();
          pending_flush = 0;
        }
        continue;
      }
      record = std::move(queue_.front());
      queue_.pop_front();
    }

    if (write_json) {
      output << SerializeQcEvidenceRecord(record) << '\n';
      ++pending_flush;
      if (pending_flush >= kFlushEveryRecords) {
        output.flush();
        pending_flush = 0;
      }
    }
    if (reputation_plugin_ != nullptr) {
      reputation_plugin_->RecordQc(record.qc_view, record.qc_hash,
                                   record.signer_bitmap);
    }
  }
  if (write_json) {
    output.flush();
  }
}

}  // namespace td_hotstuff
}  // namespace resdb
