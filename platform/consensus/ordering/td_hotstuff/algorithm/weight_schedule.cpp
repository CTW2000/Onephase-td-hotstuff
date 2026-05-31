#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <utility>

#include "common/crypto/hash.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr int64_t kMinWeight = 1;
constexpr int64_t kMaxWeight = 100;

std::string HexEncode(const std::string& data) {
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (unsigned char ch : data) {
    out << std::setw(2) << static_cast<int>(ch);
  }
  return out.str();
}

std::string WeightCanonical(const std::vector<int64_t>& weights) {
  std::ostringstream out;
  out << "td_hotstuff_weight_root_v1|" << weights.size();
  for (int64_t weight : weights) {
    out << '|' << weight;
  }
  return out.str();
}

}  // namespace

std::vector<int64_t> NormalizeWeightPoints(
    const std::vector<int64_t>& weights, int total_replicas) {
  std::vector<int64_t> normalized(std::max(total_replicas, 0), kMinWeight);
  for (int i = 0; i < total_replicas && i < static_cast<int>(weights.size());
       ++i) {
    normalized[i] = std::max(kMinWeight, std::min(kMaxWeight, weights[i]));
  }
  return normalized;
}

int64_t CalculateWeightQuorum(const std::vector<int64_t>& weights) {
  int64_t total_weight = 0;
  for (int64_t weight : weights) {
    total_weight += std::max<int64_t>(weight, kMinWeight);
  }
  return total_weight * 2 / 3 + 1;
}

std::string WeightRootHex(const std::vector<int64_t>& weights) {
  return HexEncode(resdb::utils::CalculateSHA256Hash(WeightCanonical(weights)));
}

std::string HashHexForTesting(const std::string& data) {
  return HexEncode(resdb::utils::CalculateSHA256Hash(data));
}

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

bool WeightSchedule::ScheduleUpdate(int activation_view,
                                    const std::vector<int64_t>& next_weights,
                                    const std::string& old_weight_root,
                                    uint64_t old_weight_version) {
  if (activation_view <= 0 || next_weights.size() != static_cast<size_t>(total_replicas_)) {
    return false;
  }
  if (old_weight_root != ActiveWeightRoot() ||
      old_weight_version != ActiveWeightVersion()) {
    return false;
  }
  std::vector<int64_t> normalized = NormalizeWeightPoints(next_weights, total_replicas_);
  if (normalized != next_weights) {
    return false;
  }
  Record record;
  record.activation_view = activation_view;
  record.version = records_.back().version + 1;
  record.weights = std::move(normalized);
  record.root = WeightRootHex(record.weights);
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
