#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <map>
#include <string>
#include <vector>

namespace resdb {
namespace td_hotstuff {
namespace {

TEST(LeaderSelectionScheduleTest, DisabledModePreservesRoundRobinLeaders) {
  LeaderSelectionConfig config;
  config.enabled = false;
  LeaderSelectionSchedule schedule(/*total_replicas=*/4, {10, 20, 30, 40},
                                   config);

  EXPECT_EQ(schedule.LeaderForView(1), 2);
  EXPECT_EQ(schedule.LeaderForView(2), 3);
  EXPECT_EQ(schedule.LeaderForView(3), 4);
  EXPECT_EQ(schedule.LeaderForView(4), 1);
  EXPECT_TRUE(schedule.ContextHashForView(1).empty());
}

TEST(LeaderSelectionScheduleTest, EnvReadsLeaderSelectionKnobs) {
  setenv("TD_HS_LEADER_SELECTION_ENABLE", "1", /*overwrite=*/1);
  setenv("TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT", "20", /*overwrite=*/1);
  setenv("TD_HS_LEADER_SELECTION_COOLDOWN_VIEWS", "3", /*overwrite=*/1);
  setenv("TD_HS_LEADER_SELECTION_MAX_SHARE_PERCENT", "40", /*overwrite=*/1);
  setenv("TD_HS_LEADER_SELECTION_FAIRNESS_DEBT_ENABLE", "0",
         /*overwrite=*/1);

  LeaderSelectionConfig config = LeaderSelectionConfigFromEnv();
  EXPECT_TRUE(config.enabled);
  EXPECT_EQ(config.eligible_min_weight, 20);
  EXPECT_EQ(config.cooldown_views, 3);
  EXPECT_EQ(config.max_share_percent, 40);
  EXPECT_FALSE(config.fairness_debt_enabled);

  unsetenv("TD_HS_LEADER_SELECTION_ENABLE");
  unsetenv("TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT");
  unsetenv("TD_HS_LEADER_SELECTION_COOLDOWN_VIEWS");
  unsetenv("TD_HS_LEADER_SELECTION_MAX_SHARE_PERCENT");
  unsetenv("TD_HS_LEADER_SELECTION_FAIRNESS_DEBT_ENABLE");

  config = LeaderSelectionConfigFromEnv();
  EXPECT_FALSE(config.enabled);
  EXPECT_EQ(config.eligible_min_weight, 10);
  EXPECT_EQ(config.cooldown_views, 0);
  EXPECT_EQ(config.max_share_percent, 0);
  EXPECT_TRUE(config.fairness_debt_enabled);
}

TEST(LeaderSelectionScheduleTest, WeightedRoundRobinSelectionIsDeterministic) {
  LeaderSelectionConfig config;
  config.enabled = true;
  config.eligible_min_weight = 1;
  LeaderSelectionSchedule first(/*total_replicas=*/3, {1, 2, 3}, config);
  LeaderSelectionSchedule second(/*total_replicas=*/3, {1, 2, 3}, config);

  std::map<int, int> leader_counts;
  std::vector<int> first_cycle;
  for (int view = 1; view <= 60; ++view) {
    const int first_leader = first.LeaderForView(view);
    EXPECT_EQ(first_leader, second.LeaderForView(view));
    if (view <= 6) {
      first_cycle.push_back(first_leader);
    }
    ++leader_counts[first_leader];
  }

  EXPECT_EQ(first_cycle, std::vector<int>({3, 2, 1, 3, 2, 3}));
  EXPECT_EQ(leader_counts[1], 10);
  EXPECT_EQ(leader_counts[2], 20);
  EXPECT_EQ(leader_counts[3], 30);
  EXPECT_FALSE(first.ContextHashForView(10).empty());
  EXPECT_EQ(first.ContextHashForView(10), second.ContextHashForView(10));
}


TEST(LeaderSelectionScheduleTest, CooldownAvoidsImmediateRepeatsWhenAlternativesExist) {
  LeaderSelectionConfig config;
  config.enabled = true;
  config.eligible_min_weight = 1;
  config.cooldown_views = 2;
  config.fairness_debt_enabled = true;
  LeaderSelectionSchedule schedule(/*total_replicas=*/3, {90, 30, 30},
                                   config);

  int previous = 0;
  int immediate_repeats = 0;
  for (int view = 1; view <= 30; ++view) {
    const int leader = schedule.LeaderForView(view);
    if (leader == previous) {
      ++immediate_repeats;
    }
    previous = leader;
  }

  EXPECT_EQ(immediate_repeats, 0);
}

TEST(LeaderSelectionScheduleTest, MaxShareCapLimitsDominantLeaderSlots) {
  LeaderSelectionConfig config;
  config.enabled = true;
  config.eligible_min_weight = 1;
  config.cooldown_views = 0;
  config.max_share_percent = 50;
  config.fairness_debt_enabled = true;
  LeaderSelectionSchedule schedule(/*total_replicas=*/3, {100, 1, 1},
                                   config);

  std::map<int, int> leader_counts;
  for (int view = 1; view <= 102; ++view) {
    ++leader_counts[schedule.LeaderForView(view)];
  }

  EXPECT_LE(leader_counts[1], 51);
  EXPECT_GT(leader_counts[2], 0);
  EXPECT_GT(leader_counts[3], 0);
}

TEST(LeaderSelectionScheduleTest, FairnessDebtGivesMinorityValidatorsSlots) {
  LeaderSelectionConfig config;
  config.enabled = true;
  config.eligible_min_weight = 1;
  config.cooldown_views = 0;
  config.fairness_debt_enabled = true;
  LeaderSelectionSchedule schedule(/*total_replicas=*/4, {60, 20, 10, 10},
                                   config);

  std::map<int, int> leader_counts;
  for (int view = 1; view <= 100; ++view) {
    ++leader_counts[schedule.LeaderForView(view)];
  }

  EXPECT_GT(leader_counts[2], 0);
  EXPECT_GT(leader_counts[3], 0);
  EXPECT_GT(leader_counts[4], 0);
  EXPECT_GE(leader_counts[1], leader_counts[2]);
  EXPECT_GE(leader_counts[2], leader_counts[3]);
}

TEST(LeaderSelectionScheduleTest, EligibleThresholdExcludesLowWeightLeaders) {
  LeaderSelectionConfig config;
  config.enabled = true;
  config.eligible_min_weight = 10;
  LeaderSelectionSchedule schedule(/*total_replicas=*/4, {30, 30, 5, 5},
                                   config);

  std::map<int, int> leader_counts;
  for (int view = 1; view <= 40; ++view) {
    ++leader_counts[schedule.LeaderForView(view)];
  }

  EXPECT_GT(leader_counts[1], 0);
  EXPECT_GT(leader_counts[2], 0);
  EXPECT_EQ(leader_counts[3], 0);
  EXPECT_EQ(leader_counts[4], 0);
}

TEST(LeaderSelectionScheduleTest, AllBelowThresholdFallsBackToPositiveWeights) {
  LeaderSelectionConfig config;
  config.enabled = true;
  config.eligible_min_weight = 50;
  LeaderSelectionSchedule schedule(/*total_replicas=*/3, {10, 20, 30}, config);

  std::map<int, int> leader_counts;
  for (int view = 1; view <= 60; ++view) {
    ++leader_counts[schedule.LeaderForView(view)];
  }

  EXPECT_GT(leader_counts[1], 0);
  EXPECT_GT(leader_counts[2], 0);
  EXPECT_GT(leader_counts[3], 0);
}

TEST(LeaderSelectionScheduleTest, EqualWeightsUseRoundRobinCompatibility) {
  LeaderSelectionConfig config;
  config.enabled = true;
  LeaderSelectionSchedule schedule(/*total_replicas=*/4, {10, 10, 10, 10},
                                   config);

  for (int view = 1; view <= 20; ++view) {
    EXPECT_EQ(schedule.LeaderForView(view),
              DefaultLeaderForView(view, /*total_replicas=*/4));
    EXPECT_TRUE(schedule.ContextHashForView(view).empty());
  }
}

TEST(LeaderSelectionScheduleTest,
     InstalledEqualWeightSnapshotKeepsRoundRobinContextCompatibility) {
  LeaderSelectionConfig config;
  config.enabled = true;
  LeaderSelectionSchedule schedule(/*total_replicas=*/4, {10, 10, 10, 10},
                                   config);
  const std::vector<int64_t> equal_weights = {20, 20, 20, 20};

  ASSERT_TRUE(schedule.InstallWeightSnapshot(
      /*activation_view=*/10, /*weight_version=*/1, WeightRootHex(equal_weights),
      equal_weights));

  for (int view = 11; view <= 20; ++view) {
    EXPECT_EQ(schedule.LeaderForView(view),
              DefaultLeaderForView(view, /*total_replicas=*/4));
    EXPECT_TRUE(schedule.ContextHashForView(view).empty());
  }
}

TEST(LeaderSelectionScheduleTest, UsesOldSnapshotBeforeActivationBoundary) {
  LeaderSelectionConfig config;
  config.enabled = true;
  LeaderSelectionSchedule schedule(/*total_replicas=*/4, {10, 10, 10, 10},
                                   config);
  const std::string old_root = schedule.ActiveLeaderWeightRoot();
  const std::vector<int64_t> next_weights = {1, 1, 100, 1};
  const std::string next_root = WeightRootHex(next_weights);

  ASSERT_TRUE(schedule.InstallWeightSnapshot(/*activation_view=*/10,
                                             /*weight_version=*/1, next_root,
                                             next_weights));

  EXPECT_EQ(schedule.LeaderWeightRootForView(9), old_root);
  EXPECT_EQ(schedule.LeaderWeightRootForView(10), old_root);
  EXPECT_EQ(schedule.LeaderWeightRootForView(11), next_root);
  EXPECT_EQ(schedule.ActiveLeaderWeightRoot(), next_root);
  EXPECT_EQ(schedule.ActiveWeightVersion(), 1);
  EXPECT_EQ(schedule.WeightVersionForView(10), 0);
  EXPECT_EQ(schedule.WeightVersionForView(11), 1);
}

TEST(LeaderSelectionScheduleTest, RejectsInvalidSnapshotRoot) {
  LeaderSelectionConfig config;
  config.enabled = true;
  LeaderSelectionSchedule schedule(/*total_replicas=*/4, {10, 10, 10, 10},
                                   config);

  EXPECT_FALSE(schedule.InstallWeightSnapshot(
      /*activation_view=*/10, /*weight_version=*/1, /*weight_root=*/"wrong-root",
      /*weights=*/{1, 1, 100, 1}));
}

TEST(LeaderSelectionScheduleTest, RejectsConflictingSnapshotForSameVersion) {
  LeaderSelectionConfig config;
  config.enabled = true;
  LeaderSelectionSchedule schedule(/*total_replicas=*/4, {10, 10, 10, 10},
                                   config);
  const std::vector<int64_t> first = {1, 1, 100, 1};
  const std::vector<int64_t> conflict = {100, 1, 1, 1};

  ASSERT_TRUE(schedule.InstallWeightSnapshot(/*activation_view=*/10,
                                             /*weight_version=*/1,
                                             WeightRootHex(first), first));
  EXPECT_TRUE(schedule.InstallWeightSnapshot(/*activation_view=*/10,
                                             /*weight_version=*/1,
                                             WeightRootHex(first), first));
  EXPECT_FALSE(schedule.InstallWeightSnapshot(/*activation_view=*/12,
                                              /*weight_version=*/1,
                                              WeightRootHex(conflict),
                                              conflict));
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
