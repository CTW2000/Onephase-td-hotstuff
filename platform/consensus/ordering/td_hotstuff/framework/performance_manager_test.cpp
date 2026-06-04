#include "platform/consensus/ordering/td_hotstuff/framework/performance_manager.h"

#include <gtest/gtest.h>

namespace resdb {
namespace td_hotstuff {
namespace {

TEST(HotStuffPerformanceManagerTest,
     DynamicRoutePrefersPredictedLeader) {
  EXPECT_EQ(BenchmarkRouteForView(/*view=*/3, /*replica_num=*/5,
                                  /*predicted_primary=*/4,
                                  /*observed_primary=*/2),
            4);
}

TEST(HotStuffPerformanceManagerTest,
     DynamicRouteFallsBackToObservedPrimaryWithoutPrediction) {
  EXPECT_EQ(BenchmarkRouteForView(/*view=*/1, /*replica_num=*/5,
                                  /*predicted_primary=*/0,
                                  /*observed_primary=*/5),
            5);
}

TEST(HotStuffPerformanceManagerTest,
     DynamicRouteIgnoresInvalidPredictionAndObservedPrimary) {
  EXPECT_EQ(BenchmarkRouteForView(/*view=*/5, /*replica_num=*/5,
                                  /*predicted_primary=*/9,
                                  /*observed_primary=*/9),
            DefaultLeaderForView(/*view=*/5, /*total_replicas=*/5));
}


TEST(HotStuffPerformanceManagerTest, BenchmarkRetryTimeoutDoesNotUseConsensusTimeoutByDefault) {
  EXPECT_EQ(BenchmarkRetryTimeoutUsForEnv(/*raw_request_timeout_ms=*/nullptr),
            500000);
  EXPECT_EQ(BenchmarkRetryTimeoutUsForEnv(/*raw_request_timeout_ms=*/"250"),
            250000);
}

TEST(HotStuffPerformanceManagerTest,
     DynamicRoutingDefaultsOnOnlyForLiveLeaderProfileUpdates) {
  LeaderSelectionConfig config;
  config.enabled = true;
  config.dynamic_updates_enabled = true;
  EXPECT_TRUE(BenchmarkDynamicRoutingEnabled(config, /*env_override=*/nullptr));

  config.dynamic_updates_enabled = false;
  EXPECT_FALSE(BenchmarkDynamicRoutingEnabled(config, /*env_override=*/nullptr));

  EXPECT_FALSE(BenchmarkDynamicRoutingEnabled(config, /*env_override=*/"0"));
  EXPECT_TRUE(BenchmarkDynamicRoutingEnabled(config, /*env_override=*/"1"));
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
