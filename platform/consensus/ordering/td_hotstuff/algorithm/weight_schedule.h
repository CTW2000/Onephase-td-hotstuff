#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff_digest.h"

namespace resdb {
namespace td_hotstuff {

class WeightSchedule {
 public:
  WeightSchedule(int total_replicas, const std::vector<int64_t>& weights);

  int total_replicas() const { return total_replicas_; }
  int ActiveActivationView() const;
  uint64_t ActiveWeightVersion() const;
  const std::string& ActiveWeightRoot() const;
  const std::vector<int64_t>& ActiveWeights() const;

  uint64_t WeightVersionForView(int view) const;
  const std::string& WeightRootForView(int view) const;
  const std::vector<int64_t>& WeightsForView(int view) const;
  int64_t WeightForSigner(int signer, int view) const;
  int64_t QuorumWeightForView(int view) const;

  bool ScheduleUpdate(int activation_view,
                      const std::vector<int64_t>& next_weights,
                      const std::string& old_weight_root,
                      uint64_t old_weight_version);
  bool ActivateUpTo(int current_view);

 private:
  struct Record {
    int activation_view = 0;
    uint64_t version = 0;
    std::string root;
    std::vector<int64_t> weights;
    int64_t quorum_weight = 0;
  };

  const Record& RecordForView(int view) const;

  int total_replicas_;
  std::vector<Record> records_;
  size_t active_index_ = 0;
};

}  // namespace td_hotstuff
}  // namespace resdb
