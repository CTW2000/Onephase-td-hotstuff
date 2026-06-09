#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"

#include <algorithm>
#include <numeric>
#include <sstream>

#include <glog/logging.h>

#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff_digest.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr int64_t kMinLeaderWeight = 1;
constexpr int64_t kMaxLeaderWeight = 100;
constexpr int kLeaderParamsVersion = 1;
const std::string& EmptyContextHash() {
  static const std::string* empty = new std::string();
  return *empty;
}

std::vector<int64_t> NormalizeLeaderProfile(const std::vector<int64_t>& weights,
                                            int total_replicas) {
  std::vector<int64_t> normalized(std::max(total_replicas, 0), kMinLeaderWeight);
  for (int i = 0; i < total_replicas && i < static_cast<int>(weights.size());
       ++i) {
    normalized[i] = std::max(kMinLeaderWeight, std::min(kMaxLeaderWeight, weights[i]));
  }
  return normalized;
}

std::string BuildLeaderWeightRoot(const std::vector<int64_t>& weights,
                                  int64_t eligible_min_weight,
                                  int leader_selection_version) {
  std::ostringstream out;
  out << "protocol_neutral_leader_weight_root_v1|"
      << leader_selection_version << '|' << eligible_min_weight;
  for (size_t i = 0; i < weights.size(); ++i) {
    out << '|' << (i + 1) << ':' << weights[i];
  }
  return HashHexForTesting(out.str());
}

}  // namespace

int RoundRobinLeaderForView(int view, int total_replicas) {
  if (view <= 0 || total_replicas <= 0) {
    return 0;
  }
  return (view % total_replicas) + 1;
}

LeaderSelectionSchedule::LeaderSelectionSchedule(
    int total_replicas, const std::vector<int64_t>& initial_weights,
    bool enabled, int64_t eligible_min_weight)
    : total_replicas_(total_replicas), enabled_(enabled) {
  const std::vector<int64_t> normalized =
      NormalizeLeaderProfile(initial_weights, total_replicas_);
  records_.push_back(BuildRecord(
      /*activation_view=*/0, /*version=*/0,
      BuildLeaderWeightRoot(normalized, std::max<int64_t>(kMinLeaderWeight, eligible_min_weight),
                            kLeaderParamsVersion),
      normalized, eligible_min_weight));
}

int LeaderSelectionSchedule::LeaderForView(int view) const {
  if (!enabled_) {
    return RoundRobinLeaderForView(view, total_replicas_);
  }
  std::lock_guard<std::mutex> lk(mutex_);
  const Record& record = RecordForViewLocked(view);
  if (record.sequence.empty()) {
    return RoundRobinLeaderForView(view, total_replicas_);
  }
  const int idx = ((view % static_cast<int>(record.sequence.size())) +
                   static_cast<int>(record.sequence.size())) %
                  static_cast<int>(record.sequence.size());
  return record.sequence[idx];
}

std::string LeaderSelectionSchedule::ContextHashForView(int view) const {
  if (!enabled_) {
    return EmptyContextHash();
  }
  std::lock_guard<std::mutex> lk(mutex_);
  const Record& record = RecordForViewLocked(view);
  if (!record.context_required) {
    return EmptyContextHash();
  }
  std::ostringstream out;
  out << "td_hotstuff_leader_context_v1|" << view << '|'
      << record.version << '|' << record.root << '|'
      << record.eligible_min_weight << '|' << kLeaderParamsVersion;
  return HashHexForTesting(out.str());
}

bool LeaderSelectionSchedule::ScheduleUpdate(
    int activation_view, uint64_t leader_version,
    const std::string& leader_weight_root,
    const std::vector<int64_t>& leader_weights, int64_t eligible_min_weight) {
  if (!enabled_) {
    return true;
  }
  if (activation_view <= 0 || leader_weights.size() != static_cast<size_t>(total_replicas_) ||
      leader_version <= ActiveLeaderVersion()) {
    return false;
  }
  const std::vector<int64_t> normalized =
      NormalizeLeaderProfile(leader_weights, total_replicas_);
  if (normalized != leader_weights) {
    return false;
  }
  const int64_t normalized_threshold =
      std::max<int64_t>(kMinLeaderWeight, eligible_min_weight);
  if (leader_weight_root !=
      BuildLeaderWeightRoot(normalized, normalized_threshold, kLeaderParamsVersion)) {
    return false;
  }
  std::lock_guard<std::mutex> lk(mutex_);
  for (size_t i = active_index_ + 1; i < records_.size(); ++i) {
    const Record& pending = records_[i];
    if (pending.activation_view == activation_view &&
        pending.version == leader_version && pending.root == leader_weight_root &&
        pending.weights == normalized &&
        pending.eligible_min_weight == eligible_min_weight) {
      return true;
    }
    if (pending.activation_view == activation_view) {
      return false;
    }
  }
  records_.push_back(BuildRecord(activation_view, leader_version,
                                 leader_weight_root, normalized,
                                 eligible_min_weight));
  std::sort(records_.begin() + 1, records_.end(),
            [](const Record& a, const Record& b) {
              return a.activation_view < b.activation_view;
            });
  return true;
}

bool LeaderSelectionSchedule::ActivateUpTo(int current_view) {
  if (!enabled_) {
    return false;
  }
  std::lock_guard<std::mutex> lk(mutex_);
  size_t selected = active_index_;
  for (size_t i = active_index_; i < records_.size(); ++i) {
    if (records_[i].activation_view < current_view) {
      selected = i;
    }
  }
  if (selected == active_index_) {
    return false;
  }
  active_index_ = selected;
  return true;
}

uint64_t LeaderSelectionSchedule::ActiveLeaderVersion() const {
  std::lock_guard<std::mutex> lk(mutex_);
  return records_[active_index_].version;
}

const std::string& LeaderSelectionSchedule::ActiveLeaderWeightRoot() const {
  std::lock_guard<std::mutex> lk(mutex_);
  return records_[active_index_].root;
}

const std::vector<int64_t>& LeaderSelectionSchedule::ActiveLeaderWeights() const {
  std::lock_guard<std::mutex> lk(mutex_);
  return records_[active_index_].weights;
}

int64_t LeaderSelectionSchedule::ActiveEligibleMinWeight() const {
  std::lock_guard<std::mutex> lk(mutex_);
  return records_[active_index_].eligible_min_weight;
}

const LeaderSelectionSchedule::Record& LeaderSelectionSchedule::RecordForViewLocked(
    int view) const {
  const Record* selected = &records_.front();
  for (const Record& record : records_) {
    if (record.activation_view < view) {
      selected = &record;
    } else {
      break;
    }
  }
  return *selected;
}

LeaderSelectionSchedule::Record LeaderSelectionSchedule::BuildRecord(
    int activation_view, uint64_t version, const std::string& leader_weight_root,
    const std::vector<int64_t>& leader_weights, int64_t eligible_min_weight) const {
  Record record;
  record.activation_view = activation_view;
  record.version = version;
  record.weights = NormalizeLeaderProfile(leader_weights, total_replicas_);
  record.eligible_min_weight = std::max<int64_t>(kMinLeaderWeight, eligible_min_weight);
  record.root = leader_weight_root.empty()
                    ? BuildLeaderWeightRoot(record.weights,
                                            record.eligible_min_weight,
                                            kLeaderParamsVersion)
                    : leader_weight_root;
  record.sequence = BuildSmoothWeightedRoundRobin(record.weights,
                                                  record.eligible_min_weight);
  record.context_required = !IsExactRoundRobinProfile(record.weights,
                                                      record.eligible_min_weight);
  return record;
}

std::vector<int> LeaderSelectionSchedule::BuildSmoothWeightedRoundRobin(
    const std::vector<int64_t>& weights, int64_t eligible_min_weight) const {
  std::vector<int64_t> effective(weights.size(), 0);
  for (size_t i = 0; i < weights.size(); ++i) {
    if (weights[i] >= eligible_min_weight) {
      effective[i] = weights[i];
    }
  }
  int64_t total = std::accumulate(effective.begin(), effective.end(), int64_t{0});
  if (total <= 0) {
    for (size_t i = 0; i < weights.size(); ++i) {
      effective[i] = std::max<int64_t>(0, weights[i]);
    }
    total = std::accumulate(effective.begin(), effective.end(), int64_t{0});
    LOG(WARNING) << "TD-Hotstuff leader selection found no eligible validators; "
                 << "falling back to all positive leader weights";
  }
  if (total <= 0) {
    std::vector<int> fallback;
    fallback.reserve(total_replicas_);
    for (int id = 1; id <= total_replicas_; ++id) {
      fallback.push_back(id);
    }
    return fallback;
  }

  std::vector<int64_t> current(effective.size(), 0);
  std::vector<int> sequence;
  sequence.reserve(static_cast<size_t>(total));
  for (int64_t step = 0; step < total; ++step) {
    int best = -1;
    for (size_t i = 0; i < effective.size(); ++i) {
      if (effective[i] <= 0) {
        continue;
      }
      current[i] += effective[i];
      if (best < 0 || current[i] > current[best] ||
          (current[i] == current[best] && i < static_cast<size_t>(best))) {
        best = static_cast<int>(i);
      }
    }
    if (best < 0) {
      break;
    }
    sequence.push_back(best + 1);
    current[best] -= total;
  }
  return sequence;
}

bool LeaderSelectionSchedule::IsExactRoundRobinProfile(
    const std::vector<int64_t>& weights, int64_t eligible_min_weight) const {
  if (static_cast<int>(weights.size()) != total_replicas_ || total_replicas_ <= 0) {
    return true;
  }
  if (weights.empty() || weights.front() < eligible_min_weight) {
    return false;
  }
  for (int64_t weight : weights) {
    if (weight != weights.front() || weight < eligible_min_weight) {
      return false;
    }
  }
  return true;
}

}  // namespace td_hotstuff
}  // namespace resdb
