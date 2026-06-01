#include "platform/consensus/ordering/td_hotstuff/algorithm/qc_signer_selector.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <numeric>
#include <vector>

#include <gtest/gtest.h>

namespace resdb {
namespace td_hotstuff {
namespace {

TEST(QcSignerSelectorTest, PrefersNonCoolingSigners) {
  QcSignerCooldownTracker tracker({/*enabled=*/true, /*cooldown_rounds=*/1});
  tracker.RecordQcSigners({1, 2, 3});

  const std::vector<QcSignerInfo> available = {
      {1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}, {6, 1}};

  EXPECT_EQ(tracker.SelectSignersForQc(available, /*quorum_weight=*/3),
            std::vector<int>({4, 5, 6}));
}

TEST(QcSignerSelectorTest, FallsBackToCoolingSignersForQuorum) {
  QcSignerCooldownTracker tracker({/*enabled=*/true, /*cooldown_rounds=*/1});
  tracker.RecordQcSigners({1, 2, 3});

  const std::vector<QcSignerInfo> available = {
      {1, 1}, {2, 1}, {3, 1}, {4, 1}};

  EXPECT_EQ(tracker.SelectSignersForQc(available, /*quorum_weight=*/3),
            std::vector<int>({4, 1, 2}));
}

TEST(QcSignerSelectorTest, UsesWeightsWhenSelectingQuorum) {
  QcSignerCooldownTracker tracker({/*enabled=*/true, /*cooldown_rounds=*/1});
  tracker.RecordQcSigners({1, 2});

  const std::vector<QcSignerInfo> available = {
      {1, 10}, {2, 10}, {3, 4}, {4, 4}, {5, 4}};

  EXPECT_EQ(tracker.SelectSignersForQc(available, /*quorum_weight=*/12),
            std::vector<int>({3, 4, 5}));
}

TEST(QcSignerSelectorTest, DisabledSelectorPreservesAvailableOrder) {
  QcSignerCooldownTracker tracker({/*enabled=*/false, /*cooldown_rounds=*/1});
  tracker.RecordQcSigners({1, 2, 3});

  const std::vector<QcSignerInfo> available = {
      {1, 1}, {2, 1}, {3, 1}, {4, 1}};

  EXPECT_EQ(tracker.SelectSignersForQc(available, /*quorum_weight=*/3),
            std::vector<int>({1, 2, 3}));
}

TEST(QcSignerSelectorTest, BalancesSelectionDebtAcrossAvailableSigners) {
  QcSignerCooldownTracker tracker({/*enabled=*/true, /*cooldown_rounds=*/1});
  std::vector<QcSignerInfo> available;
  for (int signer = 1; signer <= 20; ++signer) {
    available.push_back({signer, 30});
  }

  std::map<int, int> inclusion_count;
  for (int round = 0; round < 40; ++round) {
    const std::vector<int> selected =
        tracker.SelectSignersForQc(available, /*quorum_weight=*/401);
    const int64_t selected_weight =
        std::accumulate(selected.begin(), selected.end(), int64_t{0},
                        [](int64_t total, int) { return total + 30; });
    EXPECT_GE(selected_weight, 401);
    tracker.RecordQcSigners(selected);
    for (int signer : selected) {
      ++inclusion_count[signer];
    }
  }

  int min_inclusions = inclusion_count[1];
  int max_inclusions = inclusion_count[1];
  for (int signer = 1; signer <= 20; ++signer) {
    min_inclusions = std::min(min_inclusions, inclusion_count[signer]);
    max_inclusions = std::max(max_inclusions, inclusion_count[signer]);
  }
  EXPECT_LE(max_inclusions - min_inclusions, 1);
}

TEST(QcSignerSelectorTest, ViewSeedBalancesIndependentLeaderFirstQuorums) {
  std::vector<QcSignerInfo> available;
  for (int signer = 1; signer <= 20; ++signer) {
    available.push_back({signer, 30});
  }

  std::map<int, int> inclusion_count;
  for (int view = 1; view <= 20; ++view) {
    QcSignerCooldownTracker tracker(
        {/*enabled=*/true, /*cooldown_rounds=*/1});
    const std::vector<int> selected = tracker.SelectSignersForQc(
        available, /*quorum_weight=*/401, /*fairness_seed=*/view);
    for (int signer : selected) {
      ++inclusion_count[signer];
    }
  }

  int min_inclusions = inclusion_count[1];
  int max_inclusions = inclusion_count[1];
  for (int signer = 1; signer <= 20; ++signer) {
    min_inclusions = std::min(min_inclusions, inclusion_count[signer]);
    max_inclusions = std::max(max_inclusions, inclusion_count[signer]);
  }
  EXPECT_LE(max_inclusions - min_inclusions, 1);
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
