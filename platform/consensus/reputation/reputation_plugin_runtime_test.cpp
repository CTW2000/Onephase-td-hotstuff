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

LeaderOutcomeEvidenceRecord TimeoutOutcome(int view, int leader) {
  LeaderOutcomeEvidenceRecord record;
  record.view_or_round = view;
  record.leader_id = leader;
  record.outcome_class = OutcomeClass::kTimeoutOrViewChange;
  record.artifact_digest = "timeout-" + std::to_string(view);
  record.weight_root_hex = WeightRootHex({100, 100, 100, 100});
  record.weight_version = 0;
  record.active_weights = {100, 100, 100, 100};
  return record;
}

SignedProposalEvidence ProposalEvidence(int view, int leader,
                                        const std::string& hash) {
  SignedProposalEvidence evidence;
  evidence.protocol_id = "td_hotstuff";
  evidence.leader_id = leader;
  evidence.view_or_round = view;
  evidence.slot_or_height = 0;
  evidence.proposal_hash = hash;
  evidence.signature_verified = true;
  evidence.active_weight_root = WeightRootHex({100, 100, 100, 100});
  evidence.weight_version = 0;
  return evidence;
}

SignedVoteEvidence VoteEvidence(int view, int signer,
                                const std::string& hash) {
  SignedVoteEvidence evidence;
  evidence.protocol_id = "td_hotstuff";
  evidence.signer_id = signer;
  evidence.view_or_round = view;
  evidence.slot_or_height = 0;
  evidence.proposal_hash = hash;
  evidence.signature_verified = true;
  evidence.active_weight_root = WeightRootHex({100, 100, 100, 100});
  evidence.weight_version = 0;
  return evidence;
}

InvalidQcProposalEvidence InvalidQcEvidence(
    int view, int leader, const std::string& hash,
    bool proposal_signature_verified = true, bool qc_verified = false) {
  InvalidQcProposalEvidence evidence;
  evidence.protocol_id = "td_hotstuff";
  evidence.leader_id = leader;
  evidence.view_or_round = view;
  evidence.slot_or_height = 0;
  evidence.proposal_hash = hash;
  evidence.proposal_signature_verified = proposal_signature_verified;
  evidence.qc_verified = qc_verified;
  evidence.invalid_reason = "qc signer bitmap mismatch";
  evidence.active_weight_root = WeightRootHex({100, 100, 100, 100});
  evidence.weight_version = 0;
  return evidence;
}

SignedWeightUpdateVoteEvidence WeightUpdateVoteEvidence(
    int activation_view, int validator, const std::string& candidate_digest,
    bool signature_verified = true) {
  SignedWeightUpdateVoteEvidence evidence;
  evidence.protocol_id = "td_hotstuff";
  evidence.validator_id = validator;
  evidence.old_weight_root = WeightRootHex({100, 100, 100, 100});
  evidence.old_weight_version = 0;
  evidence.activation_view = activation_view;
  evidence.candidate_digest = candidate_digest;
  evidence.signature_verified = signature_verified;
  evidence.active_weight_root = evidence.old_weight_root;
  evidence.weight_version = evidence.old_weight_version;
  return evidence;
}

ReputationRuntimeOptions StrongFaultRuntimeOptions() {
  ReputationRuntimeOptions options = RuntimeOptions();
  options.initial_weights = {100, 100, 100, 100};
  options.initial_weight_root = WeightRootHex(options.initial_weights);
  options.config.strong_fault_enabled = true;
  options.config.double_proposal_detection_enabled = true;
  options.config.strong_fault_target_weight = 1;
  return options;
}

ReputationRuntimeOptions DoubleVoteRuntimeOptions() {
  ReputationRuntimeOptions options = StrongFaultRuntimeOptions();
  options.config.double_proposal_detection_enabled = false;
  options.config.double_vote_detection_enabled = true;
  return options;
}

ReputationRuntimeOptions InvalidQcRuntimeOptions() {
  ReputationRuntimeOptions options = StrongFaultRuntimeOptions();
  options.config.double_proposal_detection_enabled = false;
  options.config.invalid_qc_proposal_detection_enabled = true;
  return options;
}

ReputationRuntimeOptions WeightUpdateVoteRuntimeOptions() {
  ReputationRuntimeOptions options = StrongFaultRuntimeOptions();
  options.config.double_proposal_detection_enabled = false;
  options.config.weight_update_vote_equivocation_detection_enabled = true;
  return options;
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

TEST(ReputationPluginRuntimeTest, SkipsSoftOnlyPartialWindowBelowMinimumEvents) {
  ReputationRuntimeOptions options = RuntimeOptions();
  options.min_candidate_events = 2;
  ReputationPluginRuntime runtime(options);
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  EXPECT_TRUE(candidates.empty());
  EXPECT_EQ(runtime.computed_window_count(), 0);
}

TEST(ReputationPluginRuntimeTest, StrongFaultEvidenceBypassesMinimumEvents) {
  ReputationRuntimeOptions options = StrongFaultRuntimeOptions();
  options.min_candidate_events = 16;
  ReputationPluginRuntime runtime(options);
  runtime.Start();

  EXPECT_TRUE(runtime.RecordSignedProposalEvidence(
      ProposalEvidence(1, 2, "proposal-a")));
  EXPECT_TRUE(runtime.RecordSignedProposalEvidence(
      ProposalEvidence(1, 2, "proposal-b")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 2);
  EXPECT_EQ(candidates[0].validators[1].next_weight, 1);
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
     NormalSignedProposalsAreNotRetainedAsStrongFaultEvidence) {
  ReputationPluginRuntime runtime(StrongFaultRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(runtime.RecordSignedProposalEvidence(
      ProposalEvidence(0, /*leader=*/1, "proposal-0")));
  EXPECT_TRUE(runtime.RecordSignedProposalEvidence(
      ProposalEvidence(1, /*leader=*/2, "proposal-1")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  EXPECT_TRUE(candidates[0].strong_faults.empty());
  EXPECT_EQ(candidates[0].next_weights[0], 100);
}

TEST(ReputationPluginRuntimeTest, ConflictingSignedProposalsAreRetained) {
  ReputationPluginRuntime runtime(StrongFaultRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordSignedProposalEvidence(
      ProposalEvidence(1, /*leader=*/1, "proposal-a")));
  EXPECT_TRUE(runtime.RecordSignedProposalEvidence(
      ProposalEvidence(1, /*leader=*/1, "proposal-b")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 2);
  ASSERT_EQ(candidates[0].strong_faults.size(), 1);
  EXPECT_EQ(candidates[0].strong_faults[0].type,
            StrongFaultType::kDoubleProposal);
  EXPECT_EQ(candidates[0].next_weights[0], 1);
  EXPECT_EQ(candidates[0].next_weights[1], 100);
}

TEST(ReputationPluginRuntimeTest, DuplicateSameHashProposalIsIgnored) {
  ReputationPluginRuntime runtime(StrongFaultRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(runtime.RecordSignedProposalEvidence(
      ProposalEvidence(1, /*leader=*/1, "proposal-a")));
  EXPECT_TRUE(runtime.RecordSignedProposalEvidence(
      ProposalEvidence(1, /*leader=*/1, "proposal-a")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  EXPECT_TRUE(candidates[0].strong_faults.empty());
  EXPECT_EQ(candidates[0].next_weights[0], 100);
}

TEST(ReputationPluginRuntimeTest,
     NormalSignedVotesAreNotRetainedAsStrongFaultEvidence) {
  ReputationPluginRuntime runtime(DoubleVoteRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(runtime.RecordSignedVoteEvidence(
      VoteEvidence(1, /*signer=*/1, "proposal-a")));
  EXPECT_TRUE(runtime.RecordSignedVoteEvidence(
      VoteEvidence(2, /*signer=*/1, "proposal-b")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  EXPECT_TRUE(candidates[0].strong_faults.empty());
  EXPECT_EQ(candidates[0].next_weights[0], 100);
}

TEST(ReputationPluginRuntimeTest, ConflictingSignedVotesAreRetained) {
  ReputationPluginRuntime runtime(DoubleVoteRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordSignedVoteEvidence(
      VoteEvidence(1, /*signer=*/3, "proposal-a")));
  EXPECT_TRUE(runtime.RecordSignedVoteEvidence(
      VoteEvidence(1, /*signer=*/3, "proposal-b")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 2);
  ASSERT_EQ(candidates[0].strong_faults.size(), 1);
  EXPECT_EQ(candidates[0].strong_faults[0].type,
            StrongFaultType::kDoubleVote);
  EXPECT_EQ(candidates[0].strong_faults[0].validator_id, 3);
  EXPECT_EQ(candidates[0].next_weights[0], 100);
  EXPECT_EQ(candidates[0].next_weights[1], 100);
  EXPECT_EQ(candidates[0].next_weights[2], 1);
  EXPECT_EQ(candidates[0].next_weights[3], 100);
}

TEST(ReputationPluginRuntimeTest, InvalidQcEvidenceRejectsUnauthenticatedArtifacts) {
  ReputationPluginRuntime runtime(InvalidQcRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(runtime.RecordInvalidQcProposalEvidence(InvalidQcEvidence(
      1, /*leader=*/2, "proposal-bad-signature",
      /*proposal_signature_verified=*/false, /*qc_verified=*/false)));
  EXPECT_TRUE(runtime.RecordInvalidQcProposalEvidence(InvalidQcEvidence(
      2, /*leader=*/3, "proposal-valid-qc",
      /*proposal_signature_verified=*/true, /*qc_verified=*/true)));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  EXPECT_TRUE(candidates[0].strong_faults.empty());
  EXPECT_EQ(candidates[0].next_weights, (std::vector<int64_t>{100, 100, 100, 100}));
}

TEST(ReputationPluginRuntimeTest, InvalidQcEvidencePenalizesLeaderOnce) {
  ReputationPluginRuntime runtime(InvalidQcRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordInvalidQcProposalEvidence(
      InvalidQcEvidence(1, /*leader=*/4, "proposal-invalid-qc")));
  EXPECT_TRUE(runtime.RecordInvalidQcProposalEvidence(
      InvalidQcEvidence(1, /*leader=*/4, "proposal-invalid-qc")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  ASSERT_EQ(candidates[0].strong_faults.size(), 1);
  EXPECT_EQ(candidates[0].strong_faults[0].type,
            StrongFaultType::kInvalidQcProposal);
  EXPECT_EQ(candidates[0].strong_faults[0].validator_id, 4);
  EXPECT_EQ(candidates[0].next_weights[0], 100);
  EXPECT_EQ(candidates[0].next_weights[1], 100);
  EXPECT_EQ(candidates[0].next_weights[2], 100);
  EXPECT_EQ(candidates[0].next_weights[3], 1);
}

TEST(ReputationPluginRuntimeTest,
     NormalWeightUpdateVotesAreNotRetainedAsStrongFaultEvidence) {
  ReputationPluginRuntime runtime(WeightUpdateVoteRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(runtime.RecordSignedWeightUpdateVoteEvidence(
      WeightUpdateVoteEvidence(/*activation_view=*/1, /*validator=*/1,
                               "candidate-a")));
  EXPECT_TRUE(runtime.RecordSignedWeightUpdateVoteEvidence(
      WeightUpdateVoteEvidence(/*activation_view=*/2, /*validator=*/1,
                               "candidate-b")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  EXPECT_TRUE(candidates[0].strong_faults.empty());
  EXPECT_EQ(candidates[0].next_weights[0], 100);
}

TEST(ReputationPluginRuntimeTest,
     ConflictingWeightUpdateVotesAreRetainedAndPenalizeValidator) {
  ReputationPluginRuntime runtime(WeightUpdateVoteRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordSignedWeightUpdateVoteEvidence(
      WeightUpdateVoteEvidence(/*activation_view=*/1, /*validator=*/2,
                               "candidate-a")));
  EXPECT_TRUE(runtime.RecordSignedWeightUpdateVoteEvidence(
      WeightUpdateVoteEvidence(/*activation_view=*/1, /*validator=*/2,
                               "candidate-b")));
  EXPECT_TRUE(runtime.RecordSignedWeightUpdateVoteEvidence(
      WeightUpdateVoteEvidence(/*activation_view=*/1, /*validator=*/2,
                               "candidate-b")));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 2);
  ASSERT_EQ(candidates[0].strong_faults.size(), 1);
  EXPECT_EQ(candidates[0].strong_faults[0].type,
            StrongFaultType::kWeightUpdateVoteEquivocation);
  EXPECT_EQ(candidates[0].strong_faults[0].validator_id, 2);
  EXPECT_EQ(candidates[0].next_weights,
            (std::vector<int64_t>{100, 1, 100, 100}));
}

TEST(ReputationPluginRuntimeTest,
     WeightUpdateVoteEvidenceRejectsUnverifiedArtifacts) {
  ReputationPluginRuntime runtime(WeightUpdateVoteRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(runtime.RecordSignedWeightUpdateVoteEvidence(
      WeightUpdateVoteEvidence(/*activation_view=*/1, /*validator=*/2,
                               "candidate-a")));
  EXPECT_TRUE(runtime.RecordSignedWeightUpdateVoteEvidence(
      WeightUpdateVoteEvidence(/*activation_view=*/1, /*validator=*/2,
                               "candidate-b",
                               /*signature_verified=*/false)));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  EXPECT_TRUE(candidates[0].strong_faults.empty());
  EXPECT_EQ(candidates[0].next_weights,
            (std::vector<int64_t>{100, 100, 100, 100}));
}

TEST(ReputationPluginRuntimeTest, PersistentStrongFaultKeepsTargetWeight) {
  ReputationPluginRuntime runtime(StrongFaultRuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordSignedProposalEvidence(
      ProposalEvidence(1, /*leader=*/1, "proposal-a")));
  EXPECT_TRUE(runtime.RecordSignedProposalEvidence(
      ProposalEvidence(1, /*leader=*/1, "proposal-b")));
  runtime.AdvanceWatermark(4);
  auto first_candidates = WaitForCandidates(&runtime, 1);
  ASSERT_EQ(first_candidates.size(), 1);
  EXPECT_EQ(first_candidates[0].next_weights[0], 1);

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(4, "qc-4")));
  runtime.AdvanceWatermark(8);
  auto second_candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(second_candidates.size(), 1);
  EXPECT_TRUE(second_candidates[0].strong_faults.empty());
  EXPECT_EQ(second_candidates[0].next_weights[0], 1);
}

TEST(ReputationPluginRuntimeTest,
     FinalizedWindowAddsScheduledLeaderOpportunities) {
  ReputationRuntimeOptions options = RuntimeOptions();
  options.initial_weights = {100, 100, 100, 100};
  options.initial_weight_root = WeightRootHex(options.initial_weights);
  options.config.min_leader_opportunities = 1;
  options.config.leader_recovery_enabled = true;
  ReputationPluginRuntime runtime(options);
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(4, "qc-4")));
  runtime.AdvanceWatermark(8);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  ASSERT_EQ(candidates[0].validators.size(), 4);
  EXPECT_EQ(candidates[0].validators[1].leader_opportunity_count, 1);
  EXPECT_EQ(candidates[0].validators[1].leader_certified_count, 0);
  EXPECT_LT(candidates[0].validators[1].leader_score, 100);
}

TEST(ReputationPluginRuntimeTest,
     ExplicitTimeoutOutcomeDoesNotAddLeaderOpportunity) {
  ReputationRuntimeOptions options = RuntimeOptions();
  options.window_size_views = 1;
  options.initial_weights = {100, 100, 100, 100};
  options.initial_weight_root = WeightRootHex(options.initial_weights);
  options.config.min_leader_opportunities = 1;
  options.config.leader_recovery_enabled = true;
  ReputationPluginRuntime runtime(options);
  runtime.Start();

  EXPECT_TRUE(runtime.RecordLeaderOutcome(TimeoutOutcome(5, 3)));
  runtime.AdvanceWatermark(6);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  ASSERT_EQ(candidates[0].validators.size(), 4);
  EXPECT_EQ(candidates[0].validators[1].leader_opportunity_count, 1);
  EXPECT_EQ(candidates[0].validators[2].leader_opportunity_count, 0);
  EXPECT_EQ(candidates[0].validators[2].leader_certified_count, 0);
  EXPECT_EQ(candidates[0].validators[2].leader_score, 100);
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


TEST(ReputationPluginRuntimeTest, DrainsOnlyEarliestCandidatePerWeightVersion) {
  ReputationPluginRuntime runtime(RuntimeOptions());
  runtime.Start();

  EXPECT_TRUE(runtime.RecordEvidence(Evidence(0, "qc-0")));
  EXPECT_TRUE(runtime.RecordEvidence(Evidence(1, "qc-1")));
  EXPECT_TRUE(runtime.RecordEvidence(Evidence(4, "qc-4")));
  EXPECT_TRUE(runtime.RecordEvidence(Evidence(5, "qc-5")));
  runtime.AdvanceWatermark(8);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].window_index, 0);
  EXPECT_EQ(candidates[0].old_weight_version, 0);
  EXPECT_EQ(runtime.computed_window_count(), 2);
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
