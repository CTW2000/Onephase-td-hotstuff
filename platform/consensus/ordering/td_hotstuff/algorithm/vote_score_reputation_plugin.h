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

struct ReputationQcEvent {
  int qc_view = 0;
  std::string qc_hash;
  std::string signer_bitmap;
};

struct ValidatorVoteScore {
  int validator_id = 0;
  uint64_t opportunities = 0;
  uint64_t inclusions = 0;
  int vote_score = 0;
  int64_t current_weight = 1;
  int64_t next_weight = 1;
};

struct VoteScoreCandidate {
  int node_id = 0;
  int total_replicas = 0;
  uint64_t window_index = 0;
  int start_qc_view = 0;
  int end_qc_view = 0;
  uint64_t event_count = 0;
  std::vector<ValidatorVoteScore> validators;
  std::string metric_root_hex;
  std::string next_weight_root_hex;
  std::string candidate_digest_hex;
};

std::vector<int> DecodeSignerBitmap(const std::string& signer_bitmap,
                                    int total_replicas);

VoteScoreCandidate ComputeVoteScoreCandidate(
    int node_id, int total_replicas, uint64_t window_index,
    const std::vector<ReputationQcEvent>& events,
    const std::vector<int64_t>& current_weights, int max_delta);

std::string VoteScoreCandidateToJson(const VoteScoreCandidate& candidate);

class AsyncVoteScoreReputationPlugin {
 public:
  AsyncVoteScoreReputationPlugin(int node_id, int total_replicas,
                                 std::vector<int64_t> current_weights,
                                 std::string output_dir, size_t window_size,
                                 size_t queue_capacity, int max_delta);
  ~AsyncVoteScoreReputationPlugin();

  AsyncVoteScoreReputationPlugin(const AsyncVoteScoreReputationPlugin&) =
      delete;
  AsyncVoteScoreReputationPlugin& operator=(
      const AsyncVoteScoreReputationPlugin&) = delete;

  static std::unique_ptr<AsyncVoteScoreReputationPlugin> CreateFromEnv(
      int node_id, int total_replicas,
      const std::vector<int64_t>& current_weights);

  void Start();
  void Stop();
  bool RecordQc(int qc_view, const std::string& qc_hash,
                const std::string& signer_bitmap);

  bool enabled() const { return enabled_; }
  uint64_t dropped_count() const { return dropped_count_.load(); }

 private:
  void WorkerLoop();
  void ProcessEvent(const ReputationQcEvent& event, std::ofstream& output);
  void FlushWindow(std::ofstream& output);
  void DropRecord(int qc_view);

  int node_id_;
  int total_replicas_;
  std::vector<int64_t> current_weights_;
  std::string output_dir_;
  std::string output_path_;
  size_t window_size_;
  size_t queue_capacity_;
  int max_delta_;
  bool enabled_ = true;

  std::atomic<bool> stopping_{false};
  std::atomic<uint64_t> dropped_count_{0};
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<ReputationQcEvent> queue_;
  std::thread worker_;
  bool started_ = false;

  uint64_t window_index_ = 0;
  std::vector<ReputationQcEvent> current_window_;
};

}  // namespace td_hotstuff
}  // namespace resdb
