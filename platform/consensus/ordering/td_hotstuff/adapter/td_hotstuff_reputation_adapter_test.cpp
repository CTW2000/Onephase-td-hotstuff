#include "platform/consensus/ordering/td_hotstuff/adapter/td_hotstuff_reputation_adapter.h"

#include <gtest/gtest.h>

#include <chrono>
#include <initializer_list>
#include <thread>
#include <vector>

namespace resdb {
namespace td_hotstuff {
namespace {

std::string Bitmap(std::initializer_list<int> signers, int total_replicas) {
  std::string bitmap((total_replicas + 7) / 8, '\0');
  for (int signer : signers) {
    if (signer < 1 || signer > total_replicas) {
      continue;
    }
    const int bit = signer - 1;
    bitmap[bit / 8] = static_cast<char>(bitmap[bit / 8] | (1 << (bit % 8)));
  }
  return bitmap;
}

TdHotstuffReputationAdapterOptions TestOptions(size_t window_size = 1,
                                               size_t queue_capacity = 8) {
  TdHotstuffReputationAdapterOptions options;
  options.enabled = true;
  options.window_size = window_size;
  options.queue_capacity = queue_capacity;
  options.reputation_config.decay_per_epoch = 3;
  options.reputation_config.max_recovery_per_epoch = 3;
  options.reputation_config.bonus_per_epoch = 0;
  options.reputation_config.min_weight = 1;
  options.reputation_config.max_weight = 100;
  options.reputation_config.min_decay_opportunities = 1;
  options.reputation_config.leader_recovery_enabled = false;
  return options;
}

TdHotstuffQcEvidenceSnapshot Snapshot(
    int view, int leader, std::initializer_list<int> selected_signers,
    std::initializer_list<int> available_signers = {}) {
  TdHotstuffQcEvidenceSnapshot snapshot;
  snapshot.local_node_id = 1;
  snapshot.total_replicas = 4;
  snapshot.view = view;
  snapshot.slot = 0;
  snapshot.leader_id = leader;
  snapshot.qc_hash = "qc-" + std::to_string(view);
  snapshot.signer_bitmap = Bitmap(selected_signers, 4);
  snapshot.available_signer_bitmap = Bitmap(
      available_signers.size() == 0 ? selected_signers : available_signers, 4);
  snapshot.active_weights = {1, 1, 1, 1};
  snapshot.active_weight_root = "old-root";
  snapshot.active_weight_version = 0;
  return snapshot;
}

std::vector<resdb::consensus::reputation::ReputationCandidate>
WaitForCandidates(TdHotstuffReputationAdapter* adapter, size_t count) {
  for (int i = 0; i < 200; ++i) {
    std::vector<resdb::consensus::reputation::ReputationCandidate> candidates =
        adapter->DrainCompletedCandidatesForTesting();
    if (candidates.size() >= count) {
      return candidates;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return adapter->DrainCompletedCandidatesForTesting();
}

TEST(TdHotstuffReputationAdapterTest, ConvertsQcSnapshotToCertifiedEvidence) {
  const TdHotstuffQcEvidenceSnapshot snapshot =
      Snapshot(/*view=*/7, /*leader=*/4, {1, 2, 3}, {1, 2, 3, 4});

  const auto evidence = ToCertifiedSignerEvidence(snapshot);

  EXPECT_EQ(evidence.view_or_round, 7);
  EXPECT_EQ(evidence.leader_id, 4);
  EXPECT_EQ(evidence.artifact_digest, "qc-7");
  EXPECT_EQ(evidence.signer_bitmap, Bitmap({1, 2, 3}, 4));
  EXPECT_EQ(evidence.available_signer_bitmap, Bitmap({1, 2, 3, 4}, 4));
}

TEST(TdHotstuffReputationAdapterTest,
     CandidateUsesAvailableSignerBitmapForOpportunities) {
  TdHotstuffReputationAdapter adapter(/*local_node_id=*/1, /*total_replicas=*/4,
                                      TestOptions(/*window_size=*/1));
  adapter.Start();

  ASSERT_TRUE(adapter.TryRecordCertifiedQc(
      Snapshot(/*view=*/7, /*leader=*/4, {1, 2, 3}, {1, 2, 3, 4})));
  ASSERT_TRUE(adapter.AdvanceWatermark(8));

  std::vector<resdb::consensus::reputation::ReputationCandidate> candidates =
      WaitForCandidates(&adapter, 1);
  adapter.Stop();

  ASSERT_EQ(candidates.size(), 1);
  ASSERT_EQ(candidates[0].validators.size(), 4);
  EXPECT_EQ(candidates[0].validators[0].opportunities, 1);
  EXPECT_EQ(candidates[0].validators[0].inclusions, 1);
  EXPECT_EQ(candidates[0].validators[3].opportunities, 1);
  EXPECT_EQ(candidates[0].validators[3].inclusions, 0);
}

TEST(TdHotstuffReputationAdapterTest,
     EquivalentEvidenceProducesIdenticalCandidateDigest) {
  TdHotstuffReputationAdapter first(/*local_node_id=*/1, /*total_replicas=*/4,
                                   TestOptions(/*window_size=*/2));
  TdHotstuffReputationAdapter second(/*local_node_id=*/2, /*total_replicas=*/4,
                                    TestOptions(/*window_size=*/2));
  first.Start();
  second.Start();

  ASSERT_TRUE(first.TryRecordCertifiedQc(Snapshot(7, 4, {1, 2, 3})));
  ASSERT_TRUE(first.TryRecordCertifiedQc(Snapshot(6, 1, {1, 2, 4})));
  ASSERT_TRUE(second.TryRecordCertifiedQc(Snapshot(6, 1, {1, 2, 4})));
  ASSERT_TRUE(second.TryRecordCertifiedQc(Snapshot(7, 4, {1, 2, 3})));
  ASSERT_TRUE(first.AdvanceWatermark(8));
  ASSERT_TRUE(second.AdvanceWatermark(8));

  std::vector<resdb::consensus::reputation::ReputationCandidate> first_result =
      WaitForCandidates(&first, 1);
  std::vector<resdb::consensus::reputation::ReputationCandidate> second_result =
      WaitForCandidates(&second, 1);
  first.Stop();
  second.Stop();

  ASSERT_EQ(first_result.size(), 1);
  ASSERT_EQ(second_result.size(), 1);
  EXPECT_EQ(first_result[0].candidate_digest_hex,
            second_result[0].candidate_digest_hex);
  EXPECT_EQ(first_result[0].metric_root_hex, second_result[0].metric_root_hex);
}

TEST(TdHotstuffReputationAdapterTest, QueueOverflowDropsWithoutBlocking) {
  TdHotstuffReputationAdapter adapter(/*local_node_id=*/1, /*total_replicas=*/4,
                                      TestOptions(/*window_size=*/8,
                                                  /*queue_capacity=*/1));

  EXPECT_TRUE(adapter.TryRecordCertifiedQc(Snapshot(7, 4, {1, 2, 3})));
  EXPECT_FALSE(adapter.TryRecordCertifiedQc(Snapshot(8, 1, {1, 2, 4})));
  EXPECT_EQ(adapter.queued_count(), 1);
  EXPECT_EQ(adapter.dropped_count(), 1);
}


TEST(TdHotstuffReputationAdapterTest, WindowFinalizationComputesCandidate) {
  TdHotstuffReputationAdapter adapter(/*local_node_id=*/1, /*total_replicas=*/4,
                                      TestOptions(/*window_size=*/2));
  adapter.Start();

  ASSERT_TRUE(adapter.TryRecordCertifiedQc(Snapshot(7, 4, {1, 2, 3})));
  ASSERT_TRUE(adapter.TryRecordCertifiedQc(Snapshot(6, 1, {1, 2, 4})));
  ASSERT_TRUE(adapter.AdvanceWatermark(8));

  std::vector<resdb::consensus::reputation::ReputationCandidate> candidates =
      WaitForCandidates(&adapter, 1);
  adapter.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 2);
  EXPECT_EQ(candidates[0].old_weight_root_hex, "old-root");
  EXPECT_EQ(candidates[0].old_weight_version, 0);
  EXPECT_EQ(candidates[0].next_weights, (std::vector<int64_t>{1, 1, 1, 1}));
  EXPECT_EQ(adapter.computed_window_count(), 1);
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
