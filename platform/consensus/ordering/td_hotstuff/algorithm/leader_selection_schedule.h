#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace resdb {
namespace td_hotstuff {

int RoundRobinLeaderForView(int view, int total_replicas);

class LeaderSelectionSchedule {
 public:
  LeaderSelectionSchedule(int total_replicas,
                          const std::vector<int64_t>& initial_weights,
                          bool enabled = false,
                          int64_t eligible_min_weight = 10);

  bool enabled() const { return enabled_; }
  int total_replicas() const { return total_replicas_; }

  int LeaderForView(int view) const;
  std::string ContextHashForView(int view) const;

  bool ScheduleUpdate(int activation_view, uint64_t leader_version,
                      const std::string& leader_weight_root,
                      const std::vector<int64_t>& leader_weights,
                      int64_t eligible_min_weight);
  bool ScheduleEpochUpdate(int activation_view, uint64_t leader_version,
                           const std::string& leader_weight_root,
                           const std::vector<int64_t>& leader_weights,
                           int64_t eligible_min_weight,
                           int epoch_start_view, int epoch_views,
                           const std::vector<int>& epoch_leaders,
                           const std::string& leader_schedule_root);
  bool ActivateUpTo(int current_view);

  uint64_t ActiveLeaderVersion() const;
  const std::string& ActiveLeaderWeightRoot() const;
  const std::vector<int64_t>& ActiveLeaderWeights() const;
  int64_t ActiveEligibleMinWeight() const;

 private:
  struct Record {
    int activation_view = 0;
    uint64_t version = 0;
    std::string root;
    std::vector<int64_t> weights;
    int64_t eligible_min_weight = 10;
    std::vector<int> sequence;
    std::string schedule_root;
    int epoch_start_view = 0;
    int epoch_views = 0;
    bool has_explicit_epoch = false;
    bool context_required = false;
  };

  const Record& RecordForViewLocked(int view) const;
  Record BuildRecord(int activation_view, uint64_t version,
                     const std::string& leader_weight_root,
                     const std::vector<int64_t>& leader_weights,
                     int64_t eligible_min_weight) const;
  Record BuildEpochRecord(int activation_view, uint64_t version,
                          const std::string& leader_weight_root,
                          const std::vector<int64_t>& leader_weights,
                          int64_t eligible_min_weight,
                          int epoch_start_view, int epoch_views,
                          const std::vector<int>& epoch_leaders,
                          const std::string& leader_schedule_root) const;
  std::vector<int> BuildSmoothWeightedRoundRobin(
      const std::vector<int64_t>& weights, int64_t eligible_min_weight) const;
  bool IsExactRoundRobinProfile(const std::vector<int64_t>& weights,
                                int64_t eligible_min_weight) const;

  const int total_replicas_;
  const bool enabled_;
  mutable std::mutex mutex_;
  std::vector<Record> records_;
  size_t active_index_ = 0;
};

}  // namespace td_hotstuff
}  // namespace resdb
