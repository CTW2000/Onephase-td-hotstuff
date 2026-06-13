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

CertifiedSignerEvidenceRecord PeerTrustEvidence(
    int view, int leader, const std::string& digest,
    const std::string& signer_bitmap, const std::string& available_bitmap,
    const std::vector<int64_t>& weights, uint64_t weight_version) {
  CertifiedSignerEvidenceRecord record;
  record.view_or_round = view;
  record.slot_or_height = 0;
  record.leader_id = leader;
  record.artifact_digest = digest;
  record.signer_bitmap = signer_bitmap;
  record.available_signer_bitmap = available_bitmap;
  record.weight_root_hex = WeightRootHex(weights);
  record.weight_version = weight_version;
  record.active_weights = weights;
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

TEST(ReputationPluginRuntimeTest,
     PersistsVoteBetaCountersAcrossSameWeightSnapshot) {
  ReputationRuntimeOptions options = RuntimeOptions();
  options.total_replicas = 5;
  options.initial_weights = {100, 100, 100, 100, 100};
  options.initial_weight_root = WeightRootHex(options.initial_weights);
  options.config.decay_per_epoch = 10;
  options.config.max_recovery_per_epoch = 10;
  options.config.vote_beta_decay_per_mille = 1000;
  ReputationPluginRuntime runtime(options);
  runtime.Start();

  for (int view = 0; view < 4; ++view) {
    CertifiedSignerEvidenceRecord evidence = Evidence(
        view, "bad-qc-" + std::to_string(view));
    evidence.signer_bitmap = Bitmap({2, 3, 4, 5}, 5);
    evidence.available_signer_bitmap = Bitmap({2, 3, 4, 5}, 5);
    evidence.active_weights = options.initial_weights;
    evidence.weight_root_hex = options.initial_weight_root;
    EXPECT_TRUE(runtime.RecordEvidence(evidence));
  }
  runtime.AdvanceWatermark(4);
  auto first_candidates = WaitForCandidates(&runtime, 1);
  ASSERT_EQ(first_candidates.size(), 1);
  ASSERT_GT(first_candidates[0].validators[0].vote_beta_failure, 0);

  for (int view = 4; view < 8; ++view) {
    CertifiedSignerEvidenceRecord evidence = Evidence(
        view, "good-qc-" + std::to_string(view));
    evidence.signer_bitmap = Bitmap({1, 2, 3, 4, 5}, 5);
    evidence.available_signer_bitmap = Bitmap({1, 2, 3, 4, 5}, 5);
    evidence.active_weights = options.initial_weights;
    evidence.weight_root_hex = options.initial_weight_root;
    EXPECT_TRUE(runtime.RecordEvidence(evidence));
  }
  runtime.AdvanceWatermark(8);
  auto second_candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(second_candidates.size(), 1);
  const ReputationCandidate& second = second_candidates[0];
  EXPECT_GT(second.validators[0].vote_beta_failure, 0);
  EXPECT_LT(second.validators[0].vote_score, second.validators[1].vote_score);
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
     SameQcDigestWithDifferentSignerBitmapIsRetained) {
  ReputationPluginRuntime runtime(RuntimeOptions());
  runtime.Start();

  CertifiedSignerEvidenceRecord first = Evidence(0, "qc-0");
  CertifiedSignerEvidenceRecord second = Evidence(0, "qc-0");
  second.signer_bitmap = Bitmap({1, 2, 4}, 4);

  EXPECT_TRUE(runtime.RecordEvidence(first));
  EXPECT_TRUE(runtime.RecordEvidence(second));
  runtime.AdvanceWatermark(4);
  auto candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 2);
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
     PeerTrustDebtCarriesAcrossActivatedSnapshots) {
  ReputationRuntimeOptions options = RuntimeOptions();
  options.window_size_views = 16;
  options.queue_capacity = 64;
  options.initial_weights = {100, 100, 100, 100};
  options.initial_weight_root = WeightRootHex(options.initial_weights);
  options.initial_leader_weights = {100, 100, 100, 100};
  options.leader_selection_enabled = true;
  options.config.peertrust_enabled = true;
  options.config.decay_per_epoch = 5;
  options.config.max_recovery_per_epoch = 5;
  options.config.bonus_per_epoch = 0;
  ReputationPluginRuntime runtime(options);
  runtime.Start();

  const std::string narrow = Bitmap({1, 2, 3}, 4);
  const std::string broad = Bitmap({1, 2, 3, 4}, 4);
  for (int view = 0; view < 8; ++view) {
    EXPECT_TRUE(runtime.RecordEvidence(PeerTrustEvidence(
        view, /*leader=*/1, "narrow-0-" + std::to_string(view), narrow,
        broad, options.initial_weights, /*weight_version=*/0)));
  }
  for (int view = 8; view < 16; ++view) {
    EXPECT_TRUE(runtime.RecordEvidence(PeerTrustEvidence(
        view, /*leader=*/2, "broad-0-" + std::to_string(view), broad,
        broad, options.initial_weights, /*weight_version=*/0)));
  }
  runtime.AdvanceWatermark(16);
  auto first = WaitForCandidates(&runtime, 1);
  ASSERT_EQ(first.size(), 1);
  ASSERT_EQ(first[0].validators.size(), 4);
  EXPECT_EQ(first[0].validators[0].peertrust_leader_debt, 20);
  EXPECT_EQ(first[0].leader_weights[0], first[0].leader_weights[1]);

  ReputationWeightSnapshot snapshot;
  snapshot.weights = first[0].next_weights;
  snapshot.weight_root_hex = first[0].next_weight_root_hex;
  snapshot.weight_version = first[0].old_weight_version + 1;
  snapshot.leader_selection_enabled = true;
  snapshot.leader_weights = first[0].leader_weights;
  snapshot.leader_weight_root_hex = first[0].leader_weight_root_hex;
  snapshot.leader_weight_version = snapshot.weight_version;
  snapshot.leader_eligible_min_weight = first[0].leader_eligible_min_weight;
  runtime.UpdateActiveWeights(snapshot);

  for (int view = 16; view < 24; ++view) {
    EXPECT_TRUE(runtime.RecordEvidence(PeerTrustEvidence(
        view, /*leader=*/1, "narrow-1-" + std::to_string(view), narrow,
        broad, snapshot.weights, snapshot.weight_version)));
  }
  for (int view = 24; view < 32; ++view) {
    EXPECT_TRUE(runtime.RecordEvidence(PeerTrustEvidence(
        view, /*leader=*/2, "broad-1-" + std::to_string(view), broad,
        broad, snapshot.weights, snapshot.weight_version)));
  }
  runtime.AdvanceWatermark(32);
  auto second = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(second.size(), 1);
  ASSERT_EQ(second[0].validators.size(), 4);
  EXPECT_EQ(second[0].old_weight_version, snapshot.weight_version);
  EXPECT_EQ(second[0].validators[0].peertrust_leader_debt, 40);
  EXPECT_EQ(second[0].validators[0].peertrust_debt_delta, 20);
  EXPECT_EQ(first[0].validators[0].next_weight,
            options.config.peertrust_soft_min_weight);
  EXPECT_EQ(second[0].validators[0].next_weight,
            options.config.peertrust_soft_min_weight);
  EXPECT_EQ(second[0].leader_weights[0], first[0].leader_weights[0]);
}

TEST(ReputationPluginRuntimeTest,
     PeerTrustDebtCarriesAcrossSameVersionWindows) {
  ReputationRuntimeOptions options = RuntimeOptions();
  options.window_size_views = 16;
  options.queue_capacity = 64;
  options.initial_weights = {100, 100, 100, 100};
  options.initial_weight_root = WeightRootHex(options.initial_weights);
  options.config.peertrust_enabled = true;
  options.config.decay_per_epoch = 5;
  options.config.max_recovery_per_epoch = 5;
  options.config.bonus_per_epoch = 0;
  ReputationPluginRuntime runtime(options);
  runtime.Start();

  const std::string narrow = Bitmap({1, 2, 3}, 4);
  const std::string broad = Bitmap({1, 2, 3, 4}, 4);
  for (int view = 0; view < 8; ++view) {
    EXPECT_TRUE(runtime.RecordEvidence(PeerTrustEvidence(
        view, /*leader=*/1, "same-version-0-" + std::to_string(view),
        narrow, broad, options.initial_weights, /*weight_version=*/0)));
  }
  for (int view = 8; view < 16; ++view) {
    EXPECT_TRUE(runtime.RecordEvidence(PeerTrustEvidence(
        view, /*leader=*/2, "same-version-broad-0-" + std::to_string(view),
        broad, broad, options.initial_weights, /*weight_version=*/0)));
  }
  runtime.AdvanceWatermark(16);
  auto first = WaitForCandidates(&runtime, 1);
  ASSERT_EQ(first.size(), 1);
  EXPECT_EQ(first[0].old_weight_version, 0);
  EXPECT_EQ(first[0].validators[0].peertrust_leader_debt, 20);

  for (int view = 16; view < 24; ++view) {
    EXPECT_TRUE(runtime.RecordEvidence(PeerTrustEvidence(
        view, /*leader=*/1, "same-version-1-" + std::to_string(view),
        narrow, broad, options.initial_weights, /*weight_version=*/0)));
  }
  for (int view = 24; view < 32; ++view) {
    EXPECT_TRUE(runtime.RecordEvidence(PeerTrustEvidence(
        view, /*leader=*/2, "same-version-broad-1-" + std::to_string(view),
        broad, broad, options.initial_weights, /*weight_version=*/0)));
  }
  runtime.AdvanceWatermark(32);
  auto second = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(second.size(), 1);
  EXPECT_EQ(second[0].old_weight_version, 0);
  EXPECT_EQ(second[0].validators[0].peertrust_leader_debt, 40);
  EXPECT_EQ(second[0].validators[0].peertrust_debt_delta, 20);
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



TEST(ReputationPluginRuntimeTest,
     PersistsFormulaStakeAcrossCertifiedWeightActivation) {
  ReputationRuntimeOptions options = RuntimeOptions();
  options.window_size_views = 2;
  options.total_replicas = 5;
  options.initial_weights = {100, 100, 100, 100, 100};
  options.initial_weight_root = WeightRootHex(options.initial_weights);
  options.initial_weight_version = 0;
  ReputationPluginRuntime runtime(options);
  runtime.Start();

  for (int view = 0; view < 2; ++view) {
    CertifiedSignerEvidenceRecord record;
    record.view_or_round = view;
    record.slot_or_height = 0;
    record.leader_id = (view % 5) + 1;
    record.artifact_digest = "bad-window-" + std::to_string(view);
    record.signer_bitmap = Bitmap({2, 3, 4, 5}, 5);
    record.available_signer_bitmap = Bitmap({2, 3, 4, 5}, 5);
    record.weight_root_hex = options.initial_weight_root;
    record.weight_version = 0;
    record.active_weights = options.initial_weights;
    EXPECT_TRUE(runtime.RecordEvidence(record));
  }
  runtime.AdvanceWatermark(2);
  auto first_candidates = WaitForCandidates(&runtime, 1);
  ASSERT_EQ(first_candidates.size(), 1);
  ASSERT_EQ(first_candidates[0].validators[0].stake_factor_per_mille, 1000);
  ASSERT_LT(first_candidates[0].validators[0].next_weight, 100);

  for (int view = 2; view < 4; ++view) {
    CertifiedSignerEvidenceRecord record;
    record.view_or_round = view;
    record.slot_or_height = 0;
    record.leader_id = (view % 5) + 1;
    record.artifact_digest = "recovery-window-" + std::to_string(view);
    record.signer_bitmap = Bitmap({1, 2, 3, 4, 5}, 5);
    record.available_signer_bitmap = Bitmap({1, 2, 3, 4, 5}, 5);
    record.weight_root_hex = first_candidates[0].next_weight_root_hex;
    record.weight_version = first_candidates[0].old_weight_version + 1;
    record.active_weights = first_candidates[0].next_weights;
    EXPECT_TRUE(runtime.RecordEvidence(record));
  }
  runtime.AdvanceWatermark(4);
  auto second_candidates = WaitForCandidates(&runtime, 1);
  runtime.Stop();

  ASSERT_EQ(second_candidates.size(), 1);
  EXPECT_EQ(second_candidates[0].validators[0].stake_factor_per_mille, 1000);
  EXPECT_NE(second_candidates[0].validators[0].stake_factor_per_mille,
            static_cast<int>(second_candidates[0].validators[0].current_weight *
                             10));
  EXPECT_GE(second_candidates[0].validators[0].next_weight,
            first_candidates[0].validators[0].next_weight);
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
