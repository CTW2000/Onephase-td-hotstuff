#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <utility>

#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr const char* kLeaderSelectionEnableEnv =
    "TD_HS_LEADER_SELECTION_ENABLE";
constexpr const char* kLeaderProfileUpdateEnableEnv =
    "TD_HS_LEADER_PROFILE_UPDATE_ENABLE";
constexpr const char* kLeaderEligibleMinWeightEnv =
    "TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT";
constexpr const char* kWeightUpdateEpochViewsEnv =
    "TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS";
constexpr uint64_t kLeaderParamsVersionV1 = 1;

bool EnvEnabled(const char* env_name) {
  const char* value = std::getenv(env_name);
  return value != nullptr && std::string(value) == "1";
}

uint64_t PositiveUint64FromEnv(const char* env_name, uint64_t default_value) {
  const char* value = std::getenv(env_name);
  if (value == nullptr || std::string(value).empty()) {
    return default_value;
  }
  try {
    const uint64_t parsed = std::stoull(value);
    return parsed == 0 ? default_value : parsed;
  } catch (...) {
    return default_value;
  }
}

bool AllLeaderWeightsEqual(const std::vector<int64_t>& weights) {
  return std::adjacent_find(weights.begin(), weights.end(),
                            std::not_equal_to<int64_t>()) == weights.end();
}

std::vector<int> SmoothWeightedRoundRobinSequence(
    const std::vector<int64_t>& weights, int total_replicas,
    int64_t eligible_min_weight) {
  std::vector<int64_t> active_weights(weights.size(), 0);
  int64_t total_weight = 0;
  int eligible_count = 0;
  for (size_t i = 0; i < weights.size(); ++i) {
    if (weights[i] >= eligible_min_weight && weights[i] > 0) {
      active_weights[i] = weights[i];
      total_weight += weights[i];
      ++eligible_count;
    }
  }
  if (eligible_count == 0) {
    for (size_t i = 0; i < weights.size(); ++i) {
      if (weights[i] > 0) {
        active_weights[i] = weights[i];
        total_weight += weights[i];
        ++eligible_count;
      }
    }
    std::cerr << "TD-Hotstuff leader selection: no validator meets "
              << "eligibility threshold, falling back to positive weights\n";
  }
  if (eligible_count == 0 || total_weight <= 0) {
    return {};
  }
  if (eligible_count == total_replicas && AllLeaderWeightsEqual(weights)) {
    return {};
  }

  std::vector<int64_t> current(weights.size(), 0);
  std::vector<int> sequence;
  sequence.reserve(static_cast<size_t>(total_weight));
  for (int64_t step = 0; step < total_weight; ++step) {
    int selected = -1;
    for (size_t i = 0; i < active_weights.size(); ++i) {
      if (active_weights[i] <= 0) {
        continue;
      }
      current[i] += active_weights[i];
      if (selected < 0 || current[i] > current[selected]) {
        selected = static_cast<int>(i);
      }
    }
    if (selected < 0) {
      break;
    }
    current[selected] -= total_weight;
    sequence.push_back(selected + 1);
  }
  return sequence;
}

std::string LeaderContextCanonical(int view, uint64_t weight_version,
                                   const std::string& leader_weight_root,
                                   uint64_t leader_params_version,
                                   const std::string& leader_randomness_ref) {
  std::ostringstream out;
  out << "td_hotstuff_leader_context_v1|" << view << '|' << weight_version
      << '|' << leader_weight_root << '|' << leader_params_version << '|'
      << leader_randomness_ref;
  return out.str();
}

}  // namespace

LeaderSelectionConfig LeaderSelectionConfigFromEnv() {
  LeaderSelectionConfig config;
  config.enabled = EnvEnabled(kLeaderSelectionEnableEnv);
  config.dynamic_updates_enabled = EnvEnabled(kLeaderProfileUpdateEnableEnv);
  config.eligible_min_weight = static_cast<int64_t>(
      PositiveUint64FromEnv(kLeaderEligibleMinWeightEnv, 10));
  // Candidate leader_weights already describe the previous active schedule.
  // Activating them at the candidate activation view gives the intended
  // one-epoch lag without delaying leader exclusion by another epoch.
  config.profile_activation_delay_views = 0;
  return config;
}

int DefaultLeaderForView(int view, int total_replicas) {
  if (view <= 0 || total_replicas <= 0) {
    return 0;
  }
  return (view % total_replicas) + 1;
}

LeaderSelectionSchedule::LeaderSelectionSchedule(
    int total_replicas, const std::vector<int64_t>& leader_weights,
    LeaderSelectionConfig config)
    : total_replicas_(total_replicas), config_(config) {
  Record record;
  record.activation_view = 0;
  record.weight_version = 0;
  record.leader_params_version = kLeaderParamsVersionV1;
  record.leader_weights =
      NormalizeLeaderWeights(leader_weights, total_replicas_);
  record.leader_weight_root = LeaderWeightRootHex(record.leader_weights);
  record.leader_randomness_ref = LeaderRandomnessRefHex(
      WeightRootHex(record.leader_weights), record.weight_version,
      record.activation_view, record.leader_weight_root);
  record.leader_sequence = SmoothWeightedRoundRobinSequence(
      record.leader_weights, total_replicas_, config_.eligible_min_weight);
  records_.push_back(std::move(record));
}

const LeaderSelectionSchedule::Record& LeaderSelectionSchedule::ActiveRecord()
    const {
  return records_[active_index_];
}

uint64_t LeaderSelectionSchedule::ActiveWeightVersion() const {
  return ActiveRecord().weight_version;
}

int LeaderSelectionSchedule::ActiveActivationView() const {
  return ActiveRecord().activation_view;
}

const std::string& LeaderSelectionSchedule::ActiveLeaderWeightRoot() const {
  return ActiveRecord().leader_weight_root;
}

const std::vector<int64_t>& LeaderSelectionSchedule::ActiveLeaderWeights()
    const {
  return ActiveRecord().leader_weights;
}

const LeaderSelectionSchedule::Record& LeaderSelectionSchedule::RecordForView(
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

int LeaderSelectionSchedule::ActivationViewForView(int view) const {
  return RecordForView(view).activation_view;
}

uint64_t LeaderSelectionSchedule::WeightVersionForView(int view) const {
  return RecordForView(view).weight_version;
}

const std::string& LeaderSelectionSchedule::LeaderWeightRootForView(
    int view) const {
  return RecordForView(view).leader_weight_root;
}

const std::vector<int64_t>& LeaderSelectionSchedule::LeaderWeightsForView(
    int view) const {
  return RecordForView(view).leader_weights;
}

std::string LeaderSelectionSchedule::ContextHashForView(int view) const {
  if (!config_.enabled) {
    return "";
  }
  const Record& record = RecordForView(view);
  if (record.leader_sequence.empty()) {
    return "";
  }
  return HashHexForTesting(LeaderContextCanonical(
      view, record.weight_version, record.leader_weight_root,
      record.leader_params_version, record.leader_randomness_ref));
}

int LeaderSelectionSchedule::LeaderForView(int view) const {
  if (!config_.enabled) {
    return DefaultLeaderForView(view, total_replicas_);
  }
  const Record& record = RecordForView(view);
  if (record.leader_sequence.empty()) {
    return DefaultLeaderForView(view, total_replicas_);
  }
  const int64_t offset =
      record.activation_view == 0
          ? static_cast<int64_t>(view) - 1
          : static_cast<int64_t>(view) - record.activation_view;
  if (offset < 0) {
    return DefaultLeaderForView(view, total_replicas_);
  }
  return record.leader_sequence[static_cast<size_t>(offset) %
                                record.leader_sequence.size()];
}

bool LeaderSelectionSchedule::ValidateProfile(
    int activation_view, uint64_t weight_version,
    const std::vector<int64_t>& leader_weights,
    const std::string& leader_weight_root, uint64_t leader_params_version,
    const std::string& leader_randomness_ref) const {
  if (activation_view <= 0 ||
      leader_weights.size() != static_cast<size_t>(total_replicas_) ||
      leader_params_version != kLeaderParamsVersionV1 ||
      leader_randomness_ref.empty() || weight_version == 0) {
    return false;
  }
  if (NormalizeLeaderWeights(leader_weights, total_replicas_) !=
      leader_weights) {
    return false;
  }
  return leader_weight_root == LeaderWeightRootHex(leader_weights);
}

bool LeaderSelectionSchedule::ScheduleUpdate(
    int activation_view, uint64_t weight_version,
    const std::vector<int64_t>& leader_weights,
    const std::string& leader_weight_root, uint64_t leader_params_version,
    const std::string& leader_randomness_ref) {
  if (!ValidateProfile(activation_view, weight_version, leader_weights,
                       leader_weight_root, leader_params_version,
                       leader_randomness_ref)) {
    return false;
  }
  for (size_t i = active_index_ + 1; i < records_.size(); ++i) {
    const Record& pending = records_[i];
    if (pending.activation_view == activation_view &&
        pending.weight_version == weight_version &&
        pending.leader_params_version == leader_params_version &&
        pending.leader_weights == leader_weights &&
        pending.leader_weight_root == leader_weight_root &&
        pending.leader_randomness_ref == leader_randomness_ref) {
      return true;
    }
    return false;
  }

  Record record;
  record.activation_view = activation_view;
  record.weight_version = weight_version;
  record.leader_params_version = leader_params_version;
  record.leader_weights = leader_weights;
  record.leader_weight_root = leader_weight_root;
  record.leader_randomness_ref = leader_randomness_ref;
  record.leader_sequence = SmoothWeightedRoundRobinSequence(
      record.leader_weights, total_replicas_, config_.eligible_min_weight);
  records_.push_back(std::move(record));
  std::sort(records_.begin() + 1, records_.end(),
            [](const Record& lhs, const Record& rhs) {
              return lhs.activation_view < rhs.activation_view;
            });
  return true;
}

bool LeaderSelectionSchedule::ActivateUpTo(int current_view) {
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

}  // namespace td_hotstuff
}  // namespace resdb
