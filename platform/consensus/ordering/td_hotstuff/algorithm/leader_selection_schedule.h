#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff_digest.h"

namespace resdb {
namespace td_hotstuff {

struct LeaderSelectionConfig {
  bool enabled = false;
  bool dynamic_updates_enabled = true;
  int64_t eligible_min_weight = 10;
  int profile_activation_delay_views = 0;
};

LeaderSelectionConfig LeaderSelectionConfigFromEnv();
int DefaultLeaderForView(int view, int total_replicas);
class LeaderSelectionSchedule {
 public:
  LeaderSelectionSchedule(int total_replicas,
                          const std::vector<int64_t>& leader_weights,
                          LeaderSelectionConfig config);

  bool enabled() const { return config_.enabled; }
  bool dynamic_updates_enabled() const {
    return config_.dynamic_updates_enabled;
  }
  int profile_activation_delay_views() const {
    return config_.profile_activation_delay_views;
  }
  int64_t eligible_min_weight() const { return config_.eligible_min_weight; }
  uint64_t ActiveWeightVersion() const;
  int ActiveActivationView() const;
  const std::string& ActiveLeaderWeightRoot() const;
  const std::vector<int64_t>& ActiveLeaderWeights() const;

  int ActivationViewForView(int view) const;
  uint64_t WeightVersionForView(int view) const;
  const std::string& LeaderWeightRootForView(int view) const;
  const std::vector<int64_t>& LeaderWeightsForView(int view) const;
  std::string ContextHashForView(int view) const;
  int LeaderForView(int view) const;

  bool ValidateProfile(int activation_view, uint64_t weight_version,
                       const std::vector<int64_t>& leader_weights,
                       const std::string& leader_weight_root,
                       uint64_t leader_params_version,
                       const std::string& leader_randomness_ref) const;
  bool ScheduleUpdate(int activation_view, uint64_t weight_version,
                      const std::vector<int64_t>& leader_weights,
                      const std::string& leader_weight_root,
                      uint64_t leader_params_version,
                      const std::string& leader_randomness_ref);
  bool ActivateUpTo(int current_view);

 private:
  struct Record {
    int activation_view = 0;
    uint64_t weight_version = 0;
    uint64_t leader_params_version = 1;
    std::string leader_weight_root;
    std::string leader_randomness_ref;
    std::vector<int64_t> leader_weights;
    std::vector<int> leader_sequence;
  };

  const Record& RecordForView(int view) const;
  const Record& ActiveRecord() const;

  int total_replicas_;
  LeaderSelectionConfig config_;
  std::vector<Record> records_;
  size_t active_index_ = 0;
};

}  // namespace td_hotstuff
}  // namespace resdb
