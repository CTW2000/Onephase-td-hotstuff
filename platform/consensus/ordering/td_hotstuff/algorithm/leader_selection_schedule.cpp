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
constexpr int kLeaderHistoryWindow = 32;
constexpr int kLeaderCooldownWindow = 4;
constexpr int kLeaderMaxConsecutiveSelections = 2;
constexpr int64_t kLeaderCooldownScale = 2;
constexpr int64_t kLeaderFairnessDebtLimitMultiplier = 4;
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

std::string BuildLeaderScheduleRoot(int leader_selection_version,
                                    int64_t eligible_min_weight,
                                    const std::string& leader_weight_root,
                                    int epoch_start_view, int epoch_views,
                                    const std::vector<int>& epoch_leaders) {
  std::ostringstream out;
  out << "protocol_neutral_leader_schedule_root_v1|"
      << leader_selection_version << '|' << eligible_min_weight << '|'
      << leader_weight_root << '|' << epoch_start_view << '|' << epoch_views;
  for (size_t i = 0; i < epoch_leaders.size(); ++i) {
    out << '|' << (epoch_start_view + static_cast<int>(i)) << ':'
        << epoch_leaders[i];
  }
  return HashHexForTesting(out.str());
}

int PositiveModulo(int value, int modulus) {
  if (modulus <= 0) {
    return 0;
  }
  int result = value % modulus;
  if (result < 0) {
    result += modulus;
  }
  return result;
}

int64_t ClampInt64(int64_t value, int64_t low, int64_t high) {
  return std::max(low, std::min(high, value));
}

int64_t Median3(int64_t a, int64_t b, int64_t c) {
  if ((a <= b && b <= c) || (c <= b && b <= a)) {
    return b;
  }
  if ((b <= a && a <= c) || (c <= a && a <= b)) {
    return a;
  }
  return c;
}

std::vector<int> RoundRobinSequence(int total_replicas) {
  std::vector<int> fallback;
  fallback.reserve(std::max(total_replicas, 0));
  for (int id = 1; id <= total_replicas; ++id) {
    fallback.push_back(id);
  }
  return fallback;
}

std::vector<int> BuildSmoothWeightedCycle(
    const std::vector<int64_t>& effective, int total_replicas) {
  const int64_t total =
      std::accumulate(effective.begin(), effective.end(), int64_t{0});
  if (total <= 0) {
    return RoundRobinSequence(total_replicas);
  }

  std::vector<int64_t> current(effective.size(), 0);
  std::vector<int> cycle;
  cycle.reserve(static_cast<size_t>(total));
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
    cycle.push_back(best + 1);
    current[best] -= total;
  }
  if (cycle.empty()) {
    return RoundRobinSequence(total_replicas);
  }
  return cycle;
}

std::vector<int64_t> BuildHistoryMedianCredits(
    const std::vector<int64_t>& effective, int total_replicas,
    int schedule_start_view) {
  const int64_t total =
      std::accumulate(effective.begin(), effective.end(), int64_t{0});
  if (total <= 0) {
    return effective;
  }
  const std::vector<int> base_cycle =
      BuildSmoothWeightedCycle(effective, total_replicas);
  std::vector<int64_t> history_counts(effective.size(), 0);
  if (!base_cycle.empty()) {
    for (int offset = 0; offset < kLeaderHistoryWindow; ++offset) {
      const int view = schedule_start_view - kLeaderHistoryWindow + offset;
      const int leader =
          base_cycle[static_cast<size_t>(
              PositiveModulo(view, static_cast<int>(base_cycle.size())))];
      if (leader >= 1 && leader <= static_cast<int>(history_counts.size())) {
        ++history_counts[static_cast<size_t>(leader - 1)];
      }
    }
  }

  int64_t eligible_count = 0;
  for (int64_t weight : effective) {
    if (weight > 0) {
      ++eligible_count;
    }
  }
  const int64_t equal_share =
      eligible_count > 0 ? std::max<int64_t>(1, total / eligible_count) : 0;
  std::vector<int64_t> credits(effective.size(), 0);
  for (size_t i = 0; i < effective.size(); ++i) {
    if (effective[i] <= 0) {
      continue;
    }
    const int64_t history_weight =
        (history_counts[i] * total + kLeaderHistoryWindow / 2) /
        kLeaderHistoryWindow;
    credits[i] =
        std::max<int64_t>(1, Median3(effective[i], history_weight, equal_share));
  }
  return credits;
}

std::vector<int> BuildHistoryCooldownFairnessSchedule(
    const std::vector<int64_t>& effective, int total_replicas,
    int schedule_start_view, int schedule_views) {
  if (schedule_views <= 0) {
    return {};
  }
  const std::vector<int64_t> credits =
      BuildHistoryMedianCredits(effective, total_replicas, schedule_start_view);
  const int64_t total_credit =
      std::accumulate(credits.begin(), credits.end(), int64_t{0});
  if (total_credit <= 0) {
    std::vector<int> fallback = RoundRobinSequence(total_replicas);
    if (fallback.empty()) {
      return fallback;
    }
    std::vector<int> sequence;
    sequence.reserve(static_cast<size_t>(schedule_views));
    for (int offset = 0; offset < schedule_views; ++offset) {
      sequence.push_back(fallback[static_cast<size_t>(
          PositiveModulo(schedule_start_view + offset,
                         static_cast<int>(fallback.size())))]);
    }
    return sequence;
  }

  const int64_t debt_limit =
      total_credit * kLeaderFairnessDebtLimitMultiplier;
  const std::vector<int> base_cycle =
      BuildSmoothWeightedCycle(effective, total_replicas);
  std::vector<int64_t> debt(credits.size(), 0);
  std::vector<int> recent_leaders;
  std::vector<int> recent_counts(credits.size(), 0);
  int last_leader = 0;
  int consecutive_count = 0;
  if (!base_cycle.empty()) {
    for (int offset = 0; offset < kLeaderCooldownWindow; ++offset) {
      const int view = schedule_start_view - kLeaderCooldownWindow + offset;
      const int leader =
          base_cycle[static_cast<size_t>(
              PositiveModulo(view, static_cast<int>(base_cycle.size())))];
      if (leader < 1 || leader > static_cast<int>(credits.size())) {
        continue;
      }
      recent_leaders.push_back(leader);
      ++recent_counts[static_cast<size_t>(leader - 1)];
      if (leader == last_leader) {
        ++consecutive_count;
      } else {
        last_leader = leader;
        consecutive_count = 1;
      }
    }
  }

  std::vector<int> sequence;
  sequence.reserve(static_cast<size_t>(schedule_views));
  for (int step = 0; step < schedule_views; ++step) {
    for (size_t i = 0; i < credits.size(); ++i) {
      if (credits[i] > 0) {
        debt[i] = ClampInt64(debt[i] + credits[i], -debt_limit, debt_limit);
      }
    }

    bool has_alternative = false;
    for (size_t i = 0; i < credits.size(); ++i) {
      if (credits[i] > 0 && static_cast<int>(i + 1) != last_leader) {
        has_alternative = true;
        break;
      }
    }

    int best = -1;
    int64_t best_score = 0;
    for (size_t i = 0; i < credits.size(); ++i) {
      if (credits[i] <= 0) {
        continue;
      }
      const int leader = static_cast<int>(i + 1);
      if (has_alternative && leader == last_leader &&
          consecutive_count >= kLeaderMaxConsecutiveSelections) {
        continue;
      }
      int64_t score = debt[i];
      if (recent_counts[i] > 0) {
        score = (score * kLeaderCooldownScale) /
                (kLeaderCooldownScale + recent_counts[i]);
      }
      if (best < 0 || score > best_score ||
          (score == best_score && i < static_cast<size_t>(best))) {
        best = static_cast<int>(i);
        best_score = score;
      }
    }
    if (best < 0) {
      break;
    }

    const int selected_leader = best + 1;
    sequence.push_back(selected_leader);
    debt[static_cast<size_t>(best)] =
        ClampInt64(debt[static_cast<size_t>(best)] - total_credit,
                   -debt_limit, debt_limit);
    if (selected_leader == last_leader) {
      ++consecutive_count;
    } else {
      last_leader = selected_leader;
      consecutive_count = 1;
    }
    recent_leaders.push_back(selected_leader);
    ++recent_counts[static_cast<size_t>(best)];
    if (static_cast<int>(recent_leaders.size()) > kLeaderCooldownWindow) {
      const int removed = recent_leaders.front();
      recent_leaders.erase(recent_leaders.begin());
      if (removed >= 1 && removed <= static_cast<int>(recent_counts.size())) {
        --recent_counts[static_cast<size_t>(removed - 1)];
      }
    }
  }
  if (sequence.empty()) {
    return RoundRobinSequence(total_replicas);
  }
  return sequence;
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
  if (record.has_explicit_epoch) {
    const int size = static_cast<int>(record.sequence.size());
    int idx = (view - record.epoch_start_view) % size;
    if (idx < 0) {
      idx += size;
    }
    return record.sequence[static_cast<size_t>(idx)];
  }
  if (!record.context_required) {
    return RoundRobinLeaderForView(view, total_replicas_);
  }
  const int idx = ((view % static_cast<int>(record.sequence.size())) +
                   static_cast<int>(record.sequence.size())) %
                  static_cast<int>(record.sequence.size());
  return record.sequence[idx];
}

std::string LeaderSelectionSchedule::ContextHashForView(int view) const {
  return EmptyContextHash();
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

bool LeaderSelectionSchedule::ScheduleEpochUpdate(
    int activation_view, uint64_t leader_version,
    const std::string& leader_weight_root,
    const std::vector<int64_t>& leader_weights, int64_t eligible_min_weight,
    int epoch_start_view, int epoch_views,
    const std::vector<int>& epoch_leaders,
    const std::string& leader_schedule_root) {
  if (!enabled_) {
    return true;
  }
  if (activation_view <= 0 || epoch_start_view != activation_view ||
      epoch_views <= 0 ||
      epoch_leaders.size() != static_cast<size_t>(epoch_views) ||
      leader_weights.size() != static_cast<size_t>(total_replicas_) ||
      leader_version <= ActiveLeaderVersion()) {
    return false;
  }
  const std::vector<int64_t> normalized =
      NormalizeLeaderProfile(leader_weights, total_replicas_);
  if (normalized != leader_weights) {
    return false;
  }
  for (int leader : epoch_leaders) {
    if (leader < 1 || leader > total_replicas_) {
      return false;
    }
  }
  const int64_t normalized_threshold =
      std::max<int64_t>(kMinLeaderWeight, eligible_min_weight);
  if (leader_weight_root !=
      BuildLeaderWeightRoot(normalized, normalized_threshold, kLeaderParamsVersion)) {
    return false;
  }
  if (leader_schedule_root !=
      BuildLeaderScheduleRoot(kLeaderParamsVersion, normalized_threshold,
                              leader_weight_root, epoch_start_view,
                              epoch_views, epoch_leaders)) {
    return false;
  }
  std::lock_guard<std::mutex> lk(mutex_);
  for (size_t i = active_index_ + 1; i < records_.size(); ++i) {
    const Record& pending = records_[i];
    if (pending.activation_view == activation_view &&
        pending.version == leader_version && pending.root == leader_weight_root &&
        pending.weights == normalized &&
        pending.eligible_min_weight == normalized_threshold &&
        pending.schedule_root == leader_schedule_root &&
        pending.sequence == epoch_leaders) {
      return true;
    }
    if (pending.activation_view == activation_view) {
      return false;
    }
  }
  records_.push_back(BuildEpochRecord(activation_view, leader_version,
                                      leader_weight_root, normalized,
                                      normalized_threshold, epoch_start_view,
                                      epoch_views, epoch_leaders,
                                      leader_schedule_root));
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
    if (record.activation_view <= view) {
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

LeaderSelectionSchedule::Record LeaderSelectionSchedule::BuildEpochRecord(
    int activation_view, uint64_t version, const std::string& leader_weight_root,
    const std::vector<int64_t>& leader_weights, int64_t eligible_min_weight,
    int epoch_start_view, int epoch_views,
    const std::vector<int>& epoch_leaders,
    const std::string& leader_schedule_root) const {
  Record record = BuildRecord(activation_view, version, leader_weight_root,
                              leader_weights, eligible_min_weight);
  record.sequence = epoch_leaders;
  record.schedule_root = leader_schedule_root;
  record.epoch_start_view = epoch_start_view;
  record.epoch_views = epoch_views;
  record.has_explicit_epoch = true;
  record.context_required = true;
  return record;
}

std::vector<int> LeaderSelectionSchedule::BuildSmoothWeightedRoundRobin(
    const std::vector<int64_t>& weights, int64_t eligible_min_weight) const {
  std::vector<int64_t> effective(weights.size(), 0);
  for (size_t i = 0; i < weights.size(); ++i) {
    if (weights[i] > eligible_min_weight) {
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
  if (total <= 0 || !IsExactRoundRobinProfile(weights, eligible_min_weight)) {
    return BuildHistoryCooldownFairnessSchedule(
        effective, total_replicas_, /*schedule_start_view=*/0,
        /*schedule_views=*/std::max<int64_t>(total, total_replicas_));
  }
  return BuildSmoothWeightedCycle(effective, total_replicas_);
}

bool LeaderSelectionSchedule::IsExactRoundRobinProfile(
    const std::vector<int64_t>& weights, int64_t eligible_min_weight) const {
  if (static_cast<int>(weights.size()) != total_replicas_ || total_replicas_ <= 0) {
    return true;
  }
  if (weights.empty() || weights.front() <= eligible_min_weight) {
    return false;
  }
  for (int64_t weight : weights) {
    if (weight != weights.front() || weight <= eligible_min_weight) {
      return false;
    }
  }
  return true;
}

}  // namespace td_hotstuff
}  // namespace resdb
