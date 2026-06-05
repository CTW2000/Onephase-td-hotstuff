#include "platform/consensus/ordering/td_hotstuff/framework/performance_manager.h"

#include <gtest/gtest.h>

namespace resdb {
namespace td_hotstuff {
namespace {

TEST(HotStuffPerformanceManagerTest,
     DynamicRoutePrefersPredictedPrimary) {
  EXPECT_EQ(BenchmarkRouteForView(/*view=*/3, /*replica_num=*/5,
                                  /*predicted_primary=*/4,
                                  /*observed_primary=*/2),
            4);
}

TEST(HotStuffPerformanceManagerTest,
     DynamicRouteFallsBackToObservedPrimaryWithoutPrediction) {
  EXPECT_EQ(BenchmarkRouteForView(/*view=*/1, /*replica_num=*/5,
                                  /*predicted_primary=*/9,
                                  /*observed_primary=*/2),
            2);
}

TEST(HotStuffPerformanceManagerTest,
     DynamicRouteIgnoresInvalidPredictionAndObservedPrimary) {
  EXPECT_EQ(BenchmarkRouteForView(/*view=*/5, /*replica_num=*/5,
                                  /*predicted_primary=*/9,
                                  /*observed_primary=*/9),
            DefaultLeaderForView(/*view=*/5, /*total_replicas=*/5));
}


TEST(HotStuffPerformanceManagerTest,
     ObservedPrimarySetKeepsPredictionWhenStillActive) {
  EXPECT_EQ(BenchmarkRouteForObservedPrimaries(
                /*view=*/7, /*replica_num=*/5, /*predicted_primary=*/3,
                {1, 2, 3, 4}),
            3);
}

TEST(HotStuffPerformanceManagerTest,
     ObservedPrimarySetSkipsMissingPredictedLeader) {
  EXPECT_EQ(BenchmarkRouteForObservedPrimaries(
                /*view=*/4, /*replica_num=*/5, /*predicted_primary=*/1,
                {2, 3, 4, 5}),
            2);
}

TEST(HotStuffPerformanceManagerTest, BenchmarkRetryTimeoutDoesNotUseConsensusTimeoutByDefault) {
  EXPECT_EQ(BenchmarkRetryTimeoutUsForEnv(/*raw_request_timeout_ms=*/nullptr),
            500000);
  EXPECT_EQ(BenchmarkRetryTimeoutUsForEnv(/*raw_request_timeout_ms=*/"250"),
            250000);
}

TEST(HotStuffPerformanceManagerTest,
     DynamicRoutingDefaultsOnWhenLeaderSelectionIsEnabled) {
  LeaderSelectionConfig config;
  config.enabled = true;
  EXPECT_TRUE(BenchmarkDynamicRoutingEnabled(config, /*env_override=*/nullptr));

  config.enabled = false;
  EXPECT_FALSE(BenchmarkDynamicRoutingEnabled(config, /*env_override=*/nullptr));

  EXPECT_FALSE(BenchmarkDynamicRoutingEnabled(config, /*env_override=*/"0"));
  EXPECT_TRUE(BenchmarkDynamicRoutingEnabled(config, /*env_override=*/"1"));
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
