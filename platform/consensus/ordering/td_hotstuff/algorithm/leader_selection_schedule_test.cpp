#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"

#include <map>
#include <vector>

#include <gtest/gtest.h>

#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"
#include "platform/consensus/reputation/reputation_roots.h"

namespace resdb {
namespace td_hotstuff {
namespace {

std::string LeaderRoot(const std::vector<int64_t>& weights,
                       int64_t eligible_min_weight) {
  return resdb::consensus::reputation::LeaderWeightRootHex(
      weights, eligible_min_weight, /*leader_selection_version=*/1);
}

TEST(LeaderSelectionScheduleTest, DisabledMatchesRoundRobinAndEmptyContext) {
  LeaderSelectionSchedule schedule(/*total_replicas=*/4,
                                   std::vector<int64_t>{100, 1, 1, 1},
                                   /*enabled=*/false,
                                   /*eligible_min_weight=*/10);
  for (int view = 1; view <= 40; ++view) {
    EXPECT_EQ(DefaultLeaderForView(view, 4), schedule.LeaderForView(view));
    EXPECT_TRUE(schedule.ContextHashForView(view).empty());
  }
  for (int view : {16388, 24582, 32774, 57357}) {
    EXPECT_EQ(DefaultLeaderForView(view, 4), schedule.LeaderForView(view));
    EXPECT_TRUE(schedule.ContextHashForView(view).empty());
  }
}

TEST(LeaderSelectionScheduleTest, EqualEnabledWeightsMatchRoundRobin) {
  LeaderSelectionSchedule schedule(/*total_replicas=*/4,
                                   std::vector<int64_t>{10, 10, 10, 10},
                                   /*enabled=*/true,
                                   /*eligible_min_weight=*/10);
  for (int view = 1; view <= 40; ++view) {
    EXPECT_EQ(DefaultLeaderForView(view, 4), schedule.LeaderForView(view));
    EXPECT_TRUE(schedule.ContextHashForView(view).empty());
  }
}

TEST(LeaderSelectionScheduleTest, WeightedSelectionIsDeterministicAndEligible) {
  LeaderSelectionSchedule schedule(/*total_replicas=*/4,
                                   std::vector<int64_t>{100, 50, 1, 1},
                                   /*enabled=*/true,
                                   /*eligible_min_weight=*/10);
  std::map<int, int> counts;
  for (int view = 0; view < 150; ++view) {
    counts[schedule.LeaderForView(view)]++;
  }
  EXPECT_EQ(100, counts[1]);
  EXPECT_EQ(50, counts[2]);
  EXPECT_EQ(0, counts[3]);
  EXPECT_EQ(0, counts[4]);
  EXPECT_TRUE(schedule.ContextHashForView(1).empty());
}

TEST(LeaderSelectionScheduleTest, AllBelowThresholdFallsBackToPositiveWeights) {
  LeaderSelectionSchedule schedule(/*total_replicas=*/3,
                                   std::vector<int64_t>{1, 2, 3},
                                   /*enabled=*/true,
                                   /*eligible_min_weight=*/10);
  std::map<int, int> counts;
  for (int view = 0; view < 6; ++view) {
    counts[schedule.LeaderForView(view)]++;
  }
  EXPECT_EQ(1, counts[1]);
  EXPECT_EQ(2, counts[2]);
  EXPECT_EQ(3, counts[3]);
}

TEST(LeaderSelectionScheduleTest, CertifiedPendingScheduleAffectsLookupAtBoundary) {
  LeaderSelectionSchedule schedule(/*total_replicas=*/3,
                                   std::vector<int64_t>{10, 10, 10},
                                   /*enabled=*/true,
                                   /*eligible_min_weight=*/10);
  const std::vector<int64_t> next{100, 1, 1};
  ASSERT_TRUE(schedule.ScheduleUpdate(/*activation_view=*/5,
                                      /*leader_version=*/1,
                                      LeaderRoot(next, 10), next,
                                      /*eligible_min_weight=*/10));
  EXPECT_EQ(DefaultLeaderForView(4, 3), schedule.LeaderForView(4));
  EXPECT_EQ(1, schedule.LeaderForView(5));
  EXPECT_TRUE(schedule.ContextHashForView(5).empty());
  ASSERT_TRUE(schedule.ActivateUpTo(/*current_view=*/5));
  EXPECT_EQ(1, schedule.LeaderForView(5));
  EXPECT_TRUE(schedule.ContextHashForView(5).empty());
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
