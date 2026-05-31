#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace resdb {
namespace td_hotstuff {

class AsyncVoteScoreReputationPlugin;
struct VoteScoreCandidate;

struct QcEvidenceRecord {
  int node_id = 0;
  int total_replicas = 0;
  int qc_view = 0;
  int leader_id = 0;
  uint64_t weight_version = 0;
  std::string active_weight_root;
  std::string qc_hash;
  std::string signer_bitmap;
};

std::string HexEncode(const std::string& data);
std::string SerializeQcEvidenceRecord(const QcEvidenceRecord& record);

class AsyncQcEvidenceRecorder {
 public:
  AsyncQcEvidenceRecorder(int node_id, int total_replicas,
                          std::string output_dir, size_t queue_capacity);
  ~AsyncQcEvidenceRecorder();

  AsyncQcEvidenceRecorder(const AsyncQcEvidenceRecorder&) = delete;
  AsyncQcEvidenceRecorder& operator=(const AsyncQcEvidenceRecorder&) = delete;

  static AsyncQcEvidenceRecorder Disabled(int node_id);
  static std::unique_ptr<AsyncQcEvidenceRecorder> CreateFromEnv(
      int node_id, int total_replicas,
      const std::vector<int64_t>& current_weights);

  void Start();
  void Stop();
  bool Enqueue(const QcEvidenceRecord& record);
  bool RecordQc(int qc_view, const std::string& qc_hash,
                const std::string& signer_bitmap);
  bool RecordQc(int qc_view, const std::string& qc_hash,
                const std::string& signer_bitmap, int leader_id,
                uint64_t weight_version, std::string active_weight_root);
  std::vector<VoteScoreCandidate> TakeCompletedReputationCandidates();
  void UpdateReputationWeights(std::vector<int64_t> current_weights,
                               std::string old_weight_root_hex,
                               uint64_t old_weight_version);

  bool enabled() const { return enabled_; }
  uint64_t dropped_count() const { return dropped_count_.load(); }

 private:
  AsyncQcEvidenceRecorder(int node_id, int total_replicas,
                          std::string output_dir, size_t queue_capacity,
                          bool write_json,
                          std::unique_ptr<AsyncVoteScoreReputationPlugin> reputation_plugin);

  void WorkerLoop();
  void DropRecord(int qc_view);

  int node_id_;
  int total_replicas_;
  std::string output_dir_;
  std::string output_path_;
  size_t queue_capacity_;
  bool write_json_;
  bool enabled_;
  std::unique_ptr<AsyncVoteScoreReputationPlugin> reputation_plugin_;

  std::atomic<bool> stopping_{false};
  std::atomic<uint64_t> dropped_count_{0};
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<QcEvidenceRecord> queue_;
  std::thread worker_;
  bool started_ = false;
};

}  // namespace td_hotstuff
}  // namespace resdb
