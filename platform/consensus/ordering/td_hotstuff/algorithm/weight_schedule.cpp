#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"

#include <algorithm>
#include <utility>

namespace resdb {
namespace td_hotstuff {
WeightSchedule::WeightSchedule(int total_replicas,
                               const std::vector<int64_t>& weights)
    : total_replicas_(total_replicas) {
  Record record;
  record.activation_view = 0;
  record.version = 0;
  record.weights = NormalizeWeightPoints(weights, total_replicas_);
  record.root = WeightRootHex(record.weights);
  record.quorum_weight = CalculateWeightQuorum(record.weights);
  records_.push_back(std::move(record));
}

int WeightSchedule::ActiveActivationView() const {
  return records_[active_index_].activation_view;
}

uint64_t WeightSchedule::ActiveWeightVersion() const {
  return records_[active_index_].version;
}

const std::string& WeightSchedule::ActiveWeightRoot() const {
  return records_[active_index_].root;
}

const std::vector<int64_t>& WeightSchedule::ActiveWeights() const {
  return records_[active_index_].weights;
}

const WeightSchedule::Record& WeightSchedule::RecordForView(int view) const {
  const Record* selected = &records_.front();
  for (const Record& record : records_) {
    if (record.activation_view <= view) {
      selected = &record;
    } else {
      break;
    }
  }
  return *selected;
}

const WeightSchedule::Record* WeightSchedule::RecordForVersion(
    uint64_t version) const {
  for (const Record& record : records_) {
    if (record.version == version) {
      return &record;
    }
  }
  return nullptr;
}

uint64_t WeightSchedule::WeightVersionForView(int view) const {
  return RecordForView(view).version;
}

const std::string& WeightSchedule::WeightRootForView(int view) const {
  return RecordForView(view).root;
}

const std::vector<int64_t>& WeightSchedule::WeightsForView(int view) const {
  return RecordForView(view).weights;
}

int64_t WeightSchedule::WeightForSigner(int signer, int view) const {
  const std::vector<int64_t>& weights = WeightsForView(view);
  if (signer < 1 || signer > static_cast<int>(weights.size())) {
    return 0;
  }
  return weights[signer - 1];
}

int64_t WeightSchedule::QuorumWeightForView(int view) const {
  return RecordForView(view).quorum_weight;
}

int64_t WeightSchedule::WeightForSignerInVersion(int signer,
                                                 uint64_t version) const {
  const Record* record = RecordForVersion(version);
  if (record == nullptr || signer < 1 ||
      signer > static_cast<int>(record->weights.size())) {
    return 0;
  }
  return record->weights[signer - 1];
}

int64_t WeightSchedule::QuorumWeightForVersion(uint64_t version) const {
  const Record* record = RecordForVersion(version);
  return record == nullptr ? 0 : record->quorum_weight;
}

bool WeightSchedule::ScheduleUpdate(int activation_view,
                                    const std::vector<int64_t>& next_weights,
                                    const std::string& old_weight_root,
                                    uint64_t old_weight_version) {
  if (activation_view <= 0 ||
      next_weights.size() != static_cast<size_t>(total_replicas_)) {
    return false;
  }
  if (old_weight_root != ActiveWeightRoot() ||
      old_weight_version != ActiveWeightVersion()) {
    return false;
  }
  std::vector<int64_t> normalized =
      NormalizeWeightPoints(next_weights, total_replicas_);
  if (normalized != next_weights) {
    return false;
  }
  const std::string next_root = WeightRootHex(normalized);
  for (size_t i = active_index_ + 1; i < records_.size(); ++i) {
    const Record& pending = records_[i];
    if (pending.activation_view == activation_view &&
        pending.root == next_root && pending.weights == normalized) {
      return true;
    }
    return false;
  }

  Record record;
  record.activation_view = activation_view;
  record.version = records_[active_index_].version + 1;
  record.weights = std::move(normalized);
  record.root = next_root;
  record.quorum_weight = CalculateWeightQuorum(record.weights);
  records_.push_back(std::move(record));
  std::sort(records_.begin() + 1, records_.end(),
            [](const Record& a, const Record& b) {
              return a.activation_view < b.activation_view;
            });
  return true;
}

bool WeightSchedule::ActivateUpTo(int current_view) {
  size_t selected = active_index_;
  for (size_t i = active_index_; i < records_.size(); ++i) {
    if (records_[i].activation_view <= current_view) {
      selected = i;
    }
  }
  if (selected == active_index_) {
    return false;
  }
  active_index_ = selected;
  return true;
}

}  // namespace td_hotstuff
}  // namespace resdb
