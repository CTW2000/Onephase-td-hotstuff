#include "platform/consensus/ordering/td_hotstuff/algorithm/qc_signer_selector.h"

#include <algorithm>
#include <cstdlib>
#include <map>
#include <string>

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr const char* kDiversityEnableEnv = "TD_HS_QC_DIVERSITY_ENABLE";
constexpr const char* kCooldownRoundsEnv = "TD_HS_QC_SIGNER_COOLDOWN_ROUNDS";

bool EnvEnabled(const char* env_name) {
  const char* raw_value = std::getenv(env_name);
  return raw_value != nullptr && std::string(raw_value) == "1";
}

uint64_t Uint64FromEnv(const char* env_name, uint64_t default_value) {
  const char* raw_value = std::getenv(env_name);
  if (raw_value == nullptr || std::string(raw_value).empty()) {
    return default_value;
  }
  try {
    const uint64_t value = std::stoull(raw_value);
    return value;
  } catch (...) {
    return default_value;
  }
}

void AddUntilQuorum(const std::vector<QcSignerInfo>& signers,
                    int64_t quorum_weight, std::vector<int>* selected,
                    int64_t* selected_weight) {
  if (selected == nullptr || selected_weight == nullptr ||
      *selected_weight >= quorum_weight) {
    return;
  }
  for (const QcSignerInfo& signer : signers) {
    if (signer.signer <= 0 || signer.weight <= 0 ||
        *selected_weight >= quorum_weight) {
      continue;
    }
    selected->push_back(signer.signer);
    *selected_weight += signer.weight;
  }
}

uint64_t MapValueOrZero(const std::map<int, uint64_t>& values, int signer) {
  const auto it = values.find(signer);
  return it == values.end() ? 0 : it->second;
}

uint64_t RotatingRank(int signer, int max_signer, uint64_t fairness_seed) {
  if (signer <= 0 || max_signer <= 0) {
    return 0;
  }
  const uint64_t bounded_signer = static_cast<uint64_t>(signer - 1);
  const uint64_t total = static_cast<uint64_t>(max_signer);
  const uint64_t rotation = fairness_seed % total;
  return (bounded_signer + total - rotation) % total;
}

}  // namespace

QcSignerDiversityConfig QcSignerDiversityConfigFromEnv() {
  QcSignerDiversityConfig config;
  config.enabled = EnvEnabled(kDiversityEnableEnv);
  config.cooldown_rounds = Uint64FromEnv(kCooldownRoundsEnv, 1);
  return config;
}

QcSignerCooldownTracker::QcSignerCooldownTracker(
    QcSignerDiversityConfig config)
    : config_(config) {}

std::vector<int> QcSignerCooldownTracker::SelectSignersForQc(
    const std::vector<QcSignerInfo>& available_signers,
    int64_t quorum_weight, uint64_t fairness_seed) {
  if (quorum_weight <= 0) {
    return {};
  }

  std::vector<QcSignerInfo> usable_signers;
  usable_signers.reserve(available_signers.size());
  for (const QcSignerInfo& signer : available_signers) {
    if (signer.signer > 0 && signer.weight > 0) {
      usable_signers.push_back(signer);
    }
  }
  if (usable_signers.empty()) {
    return {};
  }
  const int max_signer = std::max_element(
                             usable_signers.begin(), usable_signers.end(),
                             [](const QcSignerInfo& lhs,
                                const QcSignerInfo& rhs) {
                               return lhs.signer < rhs.signer;
                             })
                             ->signer;

  std::vector<int> selected;
  selected.reserve(usable_signers.size());
  int64_t selected_weight = 0;
  if (!config_.enabled || config_.cooldown_rounds == 0) {
    AddUntilQuorum(usable_signers, quorum_weight, &selected, &selected_weight);
    return selected;
  }

  for (const QcSignerInfo& signer : usable_signers) {
    ++eligible_count_by_signer_[signer.signer];
  }

  std::vector<QcSignerInfo> preferred;
  std::vector<QcSignerInfo> cooling;
  preferred.reserve(usable_signers.size());
  cooling.reserve(usable_signers.size());
  for (const QcSignerInfo& signer : usable_signers) {
    if (IsCooling(signer.signer)) {
      cooling.push_back(signer);
    } else {
      preferred.push_back(signer);
    }
  }

  const auto by_debt = [this, max_signer, fairness_seed](
                           const QcSignerInfo& lhs,
                           const QcSignerInfo& rhs) {
    const uint64_t lhs_debt = SelectionDebt(lhs.signer);
    const uint64_t rhs_debt = SelectionDebt(rhs.signer);
    if (lhs_debt != rhs_debt) {
      return lhs_debt > rhs_debt;
    }
    const uint64_t lhs_last =
        MapValueOrZero(last_included_sequence_by_signer_, lhs.signer);
    const uint64_t rhs_last =
        MapValueOrZero(last_included_sequence_by_signer_, rhs.signer);
    if (lhs_last != rhs_last) {
      return lhs_last < rhs_last;
    }
    const uint64_t lhs_rank =
        RotatingRank(lhs.signer, max_signer, fairness_seed);
    const uint64_t rhs_rank =
        RotatingRank(rhs.signer, max_signer, fairness_seed);
    if (lhs_rank != rhs_rank) {
      return lhs_rank < rhs_rank;
    }
    return lhs.signer < rhs.signer;
  };
  std::sort(preferred.begin(), preferred.end(), by_debt);
  std::sort(cooling.begin(), cooling.end(), by_debt);

  AddUntilQuorum(preferred, quorum_weight, &selected, &selected_weight);
  AddUntilQuorum(cooling, quorum_weight, &selected, &selected_weight);
  return selected;
}

void QcSignerCooldownTracker::RecordQcSigners(
    const std::vector<int>& signers) {
  if (!config_.enabled || config_.cooldown_rounds == 0) {
    return;
  }
  for (int signer : signers) {
    if (signer > 0) {
      last_included_sequence_by_signer_[signer] = qc_sequence_;
      ++included_count_by_signer_[signer];
    }
  }
  ++qc_sequence_;
}

bool QcSignerCooldownTracker::IsCooling(int signer) const {
  const auto it = last_included_sequence_by_signer_.find(signer);
  if (it == last_included_sequence_by_signer_.end()) {
    return false;
  }
  return qc_sequence_ >= it->second &&
         qc_sequence_ - it->second <= config_.cooldown_rounds;
}

uint64_t QcSignerCooldownTracker::SelectionDebt(int signer) const {
  const uint64_t eligible = MapValueOrZero(eligible_count_by_signer_, signer);
  const uint64_t included = MapValueOrZero(included_count_by_signer_, signer);
  return eligible > included ? eligible - included : 0;
}

}  // namespace td_hotstuff
}  // namespace resdb
