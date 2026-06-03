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

struct ReputationRecoveryConfig {
  int decay_per_epoch = 3;
  int max_recovery_per_epoch = 3;
  int bonus_per_epoch = 1;
  int64_t min_weight = 1;
  int64_t max_weight = 100;
  uint64_t min_decay_opportunities = 1;
  bool penalize_missing_leader = false;
};

struct ReputationQcEvent {
  int qc_view = 0;
  std::string qc_hash;
  std::string signer_bitmap;
  int leader_id = 0;
  uint64_t weight_version = 0;
  std::string active_weight_root;
};

struct ValidatorVoteScore {
  int validator_id = 0;
  uint64_t opportunities = 0;
  uint64_t inclusions = 0;
  int vote_score = 0;
  uint64_t leader_certified_count = 0;
  int leader_score = 100;
  int leader_diversity_score = 100;
  int reputation_score = 100;
  int decay_applied = 0;
  int recovery_credit = 0;
  int bonus_credit = 0;
  int64_t current_weight = 1;
  int64_t next_weight = 1;
};

struct VoteScoreCandidate {
  std::string algorithm = "bayes_v3";
  int local_node_id = 0;
  int total_replicas = 0;
  uint64_t window_index = 0;
  int start_qc_view = 0;
  int end_qc_view = 0;
  uint64_t event_count = 0;
  std::string old_weight_root_hex;
  uint64_t old_weight_version = 0;
  int activation_view = 0;
  std::vector<ValidatorVoteScore> validators;
  std::vector<int64_t> next_weights;
  std::vector<int64_t> leader_weights;
  std::string metric_root_hex;
  std::string next_weight_root_hex;
  std::string leader_weight_root_hex;
  uint64_t leader_params_version = 1;
  std::string leader_randomness_ref;
  std::string candidate_digest_hex;
};

std::vector<int> DecodeSignerBitmap(const std::string& signer_bitmap,
                                    int total_replicas);

VoteScoreCandidate ComputeBayesianReputationCandidate(
    int node_id, int total_replicas, uint64_t window_index,
    const std::vector<ReputationQcEvent>& events,
    const std::vector<int64_t>& current_weights, int max_delta,
    const std::string& old_weight_root_hex = "",
    uint64_t old_weight_version = 0, int activation_view = 0);

VoteScoreCandidate ComputeBayesianReputationCandidateWithConfig(
    int node_id, int total_replicas, uint64_t window_index,
    const std::vector<ReputationQcEvent>& events,
    const std::vector<int64_t>& current_weights,
    const ReputationRecoveryConfig& recovery_config,
    const std::string& old_weight_root_hex = "",
    uint64_t old_weight_version = 0, int activation_view = 0);

void RecomputeVoteScoreCandidateRoots(VoteScoreCandidate* candidate);

std::string VoteScoreCandidateDigest(
    int total_replicas, uint64_t window_index, int start_qc_view,
    int end_qc_view, uint64_t event_count,
    const std::string& old_weight_root_hex, uint64_t old_weight_version,
    int activation_view, const std::string& metric_root_hex,
    const std::string& next_weight_root_hex,
    const std::vector<int64_t>& next_weights,
    const std::string& leader_weight_root_hex = "",
    uint64_t leader_params_version = 0,
    const std::string& leader_randomness_ref = "",
    const std::vector<int64_t>& leader_weights = {});

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
  bool RecordQc(int qc_view, const std::string& qc_hash,
                const std::string& signer_bitmap, int leader_id,
                uint64_t weight_version, std::string active_weight_root);
  std::vector<VoteScoreCandidate> TakeCompletedCandidates();
  void UpdateCurrentWeights(std::vector<int64_t> current_weights,
                            std::string old_weight_root_hex,
                            uint64_t old_weight_version);

  bool enabled() const { return enabled_; }
  uint64_t dropped_count() const { return dropped_count_.load(); }

 private:
  void WorkerLoop();
  void ProcessEvent(const ReputationQcEvent& event, std::ofstream& output);
  void FlushWindow(std::ofstream& output,
                   std::vector<ReputationQcEvent> window,
                   uint64_t window_index,
                   std::vector<int64_t> current_weights,
                   std::string old_weight_root_hex,
                   uint64_t old_weight_version);
  void DropRecord(int qc_view);

  int node_id_;
  int total_replicas_;
  std::vector<int64_t> current_weights_;
  std::string old_weight_root_hex_;
  uint64_t old_weight_version_ = 0;
  size_t epoch_views_ = 4096;
  size_t activation_epoch_delay_ = 2;
  std::string output_dir_;
  std::string output_path_;
  size_t window_size_;
  size_t queue_capacity_;
  ReputationRecoveryConfig recovery_config_;
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
  std::deque<VoteScoreCandidate> completed_candidates_;
};

}  // namespace td_hotstuff
}  // namespace resdb
