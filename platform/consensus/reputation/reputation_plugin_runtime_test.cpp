#include "platform/consensus/reputation/reputation_plugin_runtime.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

namespace resdb {
namespace consensus {
namespace reputation {
namespace {

std::string Bitmap(std::initializer_list<int> signers, int total_replicas) {
  std::string bitmap((total_replicas + 7) / 8, '\0');
  for (int signer : signers) {
    const int bit = signer - 1;
    bitmap[bit / 8] = static_cast<char>(bitmap[bit / 8] | (1 << (bit % 8)));
  }
  return bitmap;
}

ReputationRuntimeOptions RuntimeOptions() {
  ReputationRuntimeOptions options;
  options.enabled = true;
  options.total_replicas = 4;
  options.window_size_views = 4;
  options.queue_capacity = 16;
  options.initial_weights = {1, 1, 1, 1};
  options.initial_weight_root = WeightRootHex(options.initial_weights);
  options.initial_weight_version = 0;
  options.activation_delay_windows = 1;
  options.config.decay_per_epoch = 3;
  options.config.max_recovery_per_epoch = 3;
  options.config.bonus_per_epoch = 0;
  options.config.min_decay_opportunities = 1;
  options.config.leader_recovery_enabled = false;
  return options;
}

CertifiedSignerEvidenceRecord Evidence(int view, const std::string& digest) {
  CertifiedSignerEvidenceRecord record;
  record.view_or_round = view;
  record.slot_or_height = 0;
  record.leader_id = (view % 4) + 1;
  record.artifact_digest = digest;
  record.signer_bitmap = Bitmap({1, 2, 3}, 4);
  record.available_signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
  record.weight_root_hex = WeightRootHex({1, 1, 1, 1});
  record.weight_version = 0;
  return record;
}

std::vector<ReputationCandidate> WaitForCandidates(
    ReputationPluginRuntime* runtime, size_t count) {
  for (int i = 0; i < 100; ++i) {
    auto candidates = runtime->TakeCompletedCandidates();
    if (candidates.size() >= count) {
      return candidates;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return runtime->TakeCompletedCandidates();
}

TEST(ReputationPluginRuntimeTest, FinalizesOnlyAfterWatermarkReachesWindowEnd) {
  ReputationPluginRuntime runtime(RuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(runtime.RecordEvidence(Evidence(1, "qc-1")));
  runtime.AdvanceWatermark(3);
  EXPECT_TRUE(WaitForCandidates(&runtime, 1).empty());

  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].window_index, 0);
  EXPECT_EQ(candidates[0].start_view, 0);
  EXPECT_EQ(candidates[0].end_view, 4);
  EXPECT_EQ(candidates[0].event_count, 2);
  EXPECT_EQ(candidates[0].activation_view, 8);
}

TEST(ReputationPluginRuntimeTest, DedupesAndSortsEquivalentEvidence) {
  ReputationPluginRuntime first(RuntimeOptions());
  ReputationPluginRuntime second(RuntimeOptions());
  first.Start();
  second.Start();

  EXPECT_TRUE(first.RecordEvidence(Evidence(1, "qc-1")));
  EXPECT_TRUE(first.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(first.RecordEvidence(Evidence(1, "qc-1")));
  EXPECT_TRUE(second.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(second.RecordEvidence(Evidence(1, "qc-1")));
  first.AdvanceWatermark(4);
  second.AdvanceWatermark(4);

  auto first_candidates = WaitForCandidates(&first, 1);
  auto second_candidates = WaitForCandidates(&second, 1);
  first.Stop();
  second.Stop();

  ASSERT_EQ(first_candidates.size(), 1);
  ASSERT_EQ(second_candidates.size(), 1);
  EXPECT_EQ(first_candidates[0].event_count, 2);
  EXPECT_EQ(first_candidates[0].candidate_digest_hex,
            second_candidates[0].candidate_digest_hex);
}

TEST(ReputationPluginRuntimeTest, FindLocalCandidateReturnsCompletedCandidate) {
  ReputationPluginRuntime runtime(RuntimeOptions());
  runtime.Start();
  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  ASSERT_EQ(candidates.size(), 1);

  const ReputationCandidateKey key = ReputationCandidateKey::FromCandidate(
      candidates[0]);
  auto found = runtime.FindLocalCandidate(key);
  runtime.Stop();

  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(found->candidate_digest_hex, candidates[0].candidate_digest_hex);
}

TEST(ReputationPluginRuntimeTest,
     ComputesSuccessiveWindowsForSameWeightVersion) {
  ReputationPluginRuntime runtime(RuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(runtime.RecordEvidence(Evidence(1, "qc-1")));
  runtime.AdvanceWatermark(4);
  auto first_candidates = WaitForCandidates(&runtime, 1);
  ASSERT_EQ(first_candidates.size(), 1);
  EXPECT_EQ(first_candidates[0].window_index, 0);
  EXPECT_EQ(first_candidates[0].old_weight_version, 0);

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(4, "qc-4")));
  EXPECT_TRUE(runtime.RecordEvidence(Evidence(5, "qc-5")));
  runtime.AdvanceWatermark(8);
  auto second_candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(second_candidates.size(), 1);
  EXPECT_EQ(second_candidates[0].window_index, 1);
  EXPECT_EQ(second_candidates[0].old_weight_version, 0);
  EXPECT_NE(second_candidates[0].candidate_digest_hex,
            first_candidates[0].candidate_digest_hex);
}


TEST(ReputationPluginRuntimeTest, QueueOverflowDropsWithoutBlocking) {
  ReputationRuntimeOptions options = RuntimeOptions();
  options.queue_capacity = 1;
  ReputationPluginRuntime runtime(options);

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_FALSE(runtime.RecordEvidence(Evidence(1, "qc-1")));
  EXPECT_EQ(runtime.queued_count(), 1);
  EXPECT_EQ(runtime.dropped_count(), 1);
}

}  // namespace
}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
