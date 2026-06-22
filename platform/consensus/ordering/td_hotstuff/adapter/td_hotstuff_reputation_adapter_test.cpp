#include "platform/consensus/ordering/td_hotstuff/adapter/td_hotstuff_reputation_adapter.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
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

TdHotstuffLeaderOutcomeEvidenceSnapshot TimeoutSnapshot(int view, int leader) {
  TdHotstuffLeaderOutcomeEvidenceSnapshot snapshot;
  snapshot.local_node_id = 1;
  snapshot.total_replicas = 4;
  snapshot.view = view;
  snapshot.leader_id = leader;
  snapshot.outcome_class =
      resdb::consensus::reputation::OutcomeClass::kTimeoutOrViewChange;
  snapshot.artifact_digest = "tc-" + std::to_string(view);
  snapshot.active_weights = {100, 100, 100, 100};
  snapshot.active_weight_root =
      resdb::consensus::reputation::WeightRootHex(snapshot.active_weights);
  snapshot.active_weight_version = 0;
  return snapshot;
}

TdHotstuffSignedVoteEvidenceSnapshot VoteSnapshot(int view, int signer,
                                                  const std::string& hash) {
  TdHotstuffSignedVoteEvidenceSnapshot snapshot;
  snapshot.local_node_id = 1;
  snapshot.total_replicas = 4;
  snapshot.view = view;
  snapshot.slot = 0;
  snapshot.signer_id = signer;
  snapshot.proposal_hash = hash;
  snapshot.signature_verified = true;
  snapshot.active_weight_root = "old-root";
  snapshot.active_weight_version = 7;
  return snapshot;
}

TdHotstuffInvalidQcProposalEvidenceSnapshot InvalidQcSnapshot(
    int view, int leader, const std::string& hash) {
  TdHotstuffInvalidQcProposalEvidenceSnapshot snapshot;
  snapshot.local_node_id = 1;
  snapshot.total_replicas = 4;
  snapshot.view = view;
  snapshot.slot = 0;
  snapshot.leader_id = leader;
  snapshot.proposal_hash = hash;
  snapshot.proposal_signature_verified = true;
  snapshot.qc_verified = false;
  snapshot.invalid_reason = "qc signer bitmap mismatch";
  snapshot.active_weight_root = "old-root";
  snapshot.active_weight_version = 7;
  return snapshot;
}

TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot WeightUpdateVoteSnapshot(
    int activation_view, int validator, const std::string& candidate_digest,
    int observed_view = 1) {
  TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot snapshot;
  snapshot.local_node_id = 1;
  snapshot.total_replicas = 4;
  snapshot.view = observed_view;
  snapshot.validator_id = validator;
  snapshot.old_weight_root = "old-root";
  snapshot.old_weight_version = 7;
  snapshot.activation_view = activation_view;
  snapshot.candidate_digest = candidate_digest;
  snapshot.signature_verified = true;
  snapshot.active_weight_root = "old-root";
  snapshot.active_weight_version = 7;
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

TEST(TdHotstuffReputationAdapterTest, OptionsFromEnvReadsReputationTuning) {
  setenv("TD_HS_REPUTATION_ENABLE", "1", 1);
  setenv("TD_HS_REPUTATION_DECAY_PER_EPOCH", "30", 1);
  setenv("TD_HS_REPUTATION_MAX_RECOVERY_PER_EPOCH", "30", 1);
  setenv("TD_HS_REPUTATION_BONUS_PER_EPOCH", "2", 1);
  setenv("TD_HS_REPUTATION_VOTE_BETA_DECAY_PER_MILLE", "875", 1);
  setenv("TD_HS_REPUTATION_MIN_WEIGHT", "7", 1);
  setenv("TD_HS_REPUTATION_MAX_WEIGHT", "80", 1);
  setenv("TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES", "9", 1);
  setenv("TD_HS_REPUTATION_MIN_CANDIDATE_QCS", "16", 1);
  setenv("TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT", "10", 1);
  setenv("TD_HS_REPUTATION_LEADER_DIVERSITY_SOFT_MIN_WEIGHT", "82", 1);
  setenv("TD_HS_REPUTATION_PEERTRUST_ENABLE", "1", 1);
  setenv("TD_HS_REPUTATION_MULTIPLICATIVE_WEIGHT_ENABLE", "1", 1);
  setenv("TD_HS_REPUTATION_STAKE_TAU_PER_MILLE", "750", 1);
  setenv("TD_HS_REPUTATION_STAKE_MIN_PER_MILLE", "900", 1);
  setenv("TD_HS_REPUTATION_STAKE_MAX_PER_MILLE", "1100", 1);
  setenv("TD_HS_REPUTATION_IDENTITY_MIN_PER_MILLE", "950", 1);
  setenv("TD_HS_REPUTATION_IDENTITY_MAX_PER_MILLE", "1050", 1);
  setenv("TD_HS_REPUTATION_PEERTRUST_DEBT_INCREMENT", "12", 1);
  setenv("TD_HS_REPUTATION_PEERTRUST_DEBT_RECOVERY", "2", 1);
  setenv("TD_HS_REPUTATION_PEERTRUST_DEBT_MAX", "60", 1);
  setenv("TD_HS_REPUTATION_PEERTRUST_DEBT_TRIGGER_SCORE", "85", 1);
  setenv("TD_HS_REPUTATION_PEERTRUST_SOFT_MIN_WEIGHT", "80", 1);

  const TdHotstuffReputationAdapterOptions options =
      TdHotstuffReputationAdapter::OptionsFromEnv();

  EXPECT_TRUE(options.enabled);
  EXPECT_EQ(options.reputation_config.decay_per_epoch, 30);
  EXPECT_EQ(options.reputation_config.max_recovery_per_epoch, 30);
  EXPECT_EQ(options.reputation_config.bonus_per_epoch, 2);
  EXPECT_EQ(options.reputation_config.vote_beta_decay_per_mille, 875);
  EXPECT_EQ(options.reputation_config.min_weight, 7);
  EXPECT_EQ(options.reputation_config.max_weight, 80);
  EXPECT_EQ(options.reputation_config.min_decay_opportunities, 9);
  EXPECT_EQ(options.min_candidate_events, 16);
  EXPECT_EQ(options.reputation_config.leader_eligible_min_weight, 10);
  EXPECT_EQ(options.reputation_config.leader_diversity_soft_min_weight, 82);
  EXPECT_TRUE(options.reputation_config.peertrust_enabled);
  EXPECT_TRUE(options.reputation_config.multiplicative_weight_formula_enabled);
  EXPECT_EQ(options.reputation_config.stake_exponent_tau_per_mille, 750);
  EXPECT_EQ(options.reputation_config.stake_factor_min_per_mille, 900);
  EXPECT_EQ(options.reputation_config.stake_factor_max_per_mille, 1100);
  EXPECT_EQ(options.reputation_config.identity_factor_min_per_mille, 950);
  EXPECT_EQ(options.reputation_config.identity_factor_max_per_mille, 1050);
  EXPECT_EQ(options.reputation_config.peertrust_debt_increment, 12);
  EXPECT_EQ(options.reputation_config.peertrust_debt_recovery, 2);
  EXPECT_EQ(options.reputation_config.peertrust_debt_max, 60);
  EXPECT_EQ(options.reputation_config.peertrust_debt_trigger_score, 85);
  EXPECT_EQ(options.reputation_config.peertrust_soft_min_weight, 80);

  unsetenv("TD_HS_REPUTATION_ENABLE");
  unsetenv("TD_HS_REPUTATION_DECAY_PER_EPOCH");
  unsetenv("TD_HS_REPUTATION_MAX_RECOVERY_PER_EPOCH");
  unsetenv("TD_HS_REPUTATION_BONUS_PER_EPOCH");
  unsetenv("TD_HS_REPUTATION_VOTE_BETA_DECAY_PER_MILLE");
  unsetenv("TD_HS_REPUTATION_MIN_WEIGHT");
  unsetenv("TD_HS_REPUTATION_MAX_WEIGHT");
  unsetenv("TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES");
  unsetenv("TD_HS_REPUTATION_MIN_CANDIDATE_QCS");
  unsetenv("TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT");
  unsetenv("TD_HS_REPUTATION_LEADER_DIVERSITY_SOFT_MIN_WEIGHT");
  unsetenv("TD_HS_REPUTATION_PEERTRUST_ENABLE");
  unsetenv("TD_HS_REPUTATION_MULTIPLICATIVE_WEIGHT_ENABLE");
  unsetenv("TD_HS_REPUTATION_STAKE_TAU_PER_MILLE");
  unsetenv("TD_HS_REPUTATION_STAKE_MIN_PER_MILLE");
  unsetenv("TD_HS_REPUTATION_STAKE_MAX_PER_MILLE");
  unsetenv("TD_HS_REPUTATION_IDENTITY_MIN_PER_MILLE");
  unsetenv("TD_HS_REPUTATION_IDENTITY_MAX_PER_MILLE");
  unsetenv("TD_HS_REPUTATION_PEERTRUST_DEBT_INCREMENT");
  unsetenv("TD_HS_REPUTATION_PEERTRUST_DEBT_RECOVERY");
  unsetenv("TD_HS_REPUTATION_PEERTRUST_DEBT_MAX");
  unsetenv("TD_HS_REPUTATION_PEERTRUST_DEBT_TRIGGER_SCORE");
  unsetenv("TD_HS_REPUTATION_PEERTRUST_SOFT_MIN_WEIGHT");
}


TEST(TdHotstuffReputationAdapterTest,
     SignedProposalEvidenceIsGatedByDoubleProposalDetection) {
  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE");
  setenv("TD_HS_REPUTATION_ENABLE", "1", 1);
  TdHotstuffReputationAdapterOptions options =
      TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_FALSE(options.signed_proposal_evidence_enabled);

  setenv("TD_HS_STRONG_FAULT_ENABLE", "1", 1);
  options = TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_FALSE(options.signed_proposal_evidence_enabled);

  setenv("TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE", "1", 1);
  options = TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_TRUE(options.signed_proposal_evidence_enabled);

  unsetenv("TD_HS_REPUTATION_ENABLE");
  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE");
}

TEST(TdHotstuffReputationAdapterTest,
     SignedVoteEvidenceIsGatedByDoubleVoteDetection) {
  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_DOUBLE_VOTE_DETECT_ENABLE");
  setenv("TD_HS_REPUTATION_ENABLE", "1", 1);
  TdHotstuffReputationAdapterOptions options =
      TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_FALSE(options.signed_vote_evidence_enabled);

  setenv("TD_HS_STRONG_FAULT_ENABLE", "1", 1);
  options = TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_FALSE(options.signed_vote_evidence_enabled);

  setenv("TD_HS_DOUBLE_VOTE_DETECT_ENABLE", "1", 1);
  options = TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_TRUE(options.signed_vote_evidence_enabled);

  unsetenv("TD_HS_REPUTATION_ENABLE");
  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_DOUBLE_VOTE_DETECT_ENABLE");
}

TEST(TdHotstuffReputationAdapterTest,
     InvalidQcEvidenceIsGatedByInvalidQcDetection) {
  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE");
  setenv("TD_HS_REPUTATION_ENABLE", "1", 1);
  TdHotstuffReputationAdapterOptions options =
      TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_FALSE(options.invalid_qc_proposal_evidence_enabled);

  setenv("TD_HS_STRONG_FAULT_ENABLE", "1", 1);
  options = TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_FALSE(options.invalid_qc_proposal_evidence_enabled);

  setenv("TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE", "1", 1);
  options = TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_TRUE(options.invalid_qc_proposal_evidence_enabled);

  unsetenv("TD_HS_REPUTATION_ENABLE");
  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE");
}

TEST(TdHotstuffReputationAdapterTest,
     WeightUpdateVoteEvidenceIsGatedByWeightUpdateDetection) {
  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE");
  setenv("TD_HS_REPUTATION_ENABLE", "1", 1);
  TdHotstuffReputationAdapterOptions options =
      TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_FALSE(options.signed_weight_update_vote_evidence_enabled);

  setenv("TD_HS_STRONG_FAULT_ENABLE", "1", 1);
  options = TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_FALSE(options.signed_weight_update_vote_evidence_enabled);

  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE", "1", 1);
  options = TdHotstuffReputationAdapter::OptionsFromEnv();
  EXPECT_TRUE(options.signed_weight_update_vote_evidence_enabled);

  unsetenv("TD_HS_REPUTATION_ENABLE");
  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE");
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
     ConvertsTimeoutSnapshotToLeaderOutcomeEvidence) {
  const TdHotstuffLeaderOutcomeEvidenceSnapshot snapshot =
      TimeoutSnapshot(/*view=*/7, /*leader=*/4);

  const auto evidence = ToLeaderOutcomeEvidenceRecord(snapshot);

  EXPECT_EQ(evidence.view_or_round, 7);
  EXPECT_EQ(evidence.leader_id, 4);
  EXPECT_EQ(evidence.outcome_class,
            resdb::consensus::reputation::OutcomeClass::kTimeoutOrViewChange);
  EXPECT_EQ(evidence.artifact_digest, "tc-7");
}

TEST(TdHotstuffReputationAdapterTest, ConvertsVoteSnapshotToSignedVoteEvidence) {
  const TdHotstuffSignedVoteEvidenceSnapshot snapshot =
      VoteSnapshot(/*view=*/9, /*signer=*/3, "proposal-a");

  const auto evidence = ToSignedVoteEvidence(snapshot);

  EXPECT_EQ(evidence.protocol_id, "td_hotstuff");
  EXPECT_EQ(evidence.signer_id, 3);
  EXPECT_EQ(evidence.view_or_round, 9);
  EXPECT_EQ(evidence.slot_or_height, 0);
  EXPECT_EQ(evidence.proposal_hash, "proposal-a");
  EXPECT_TRUE(evidence.signature_verified);
  EXPECT_EQ(evidence.active_weight_root, "old-root");
  EXPECT_EQ(evidence.weight_version, 7);
}

TEST(TdHotstuffReputationAdapterTest,
     SignedVoteEvidenceQueuesOnlyConflictPairs) {
  TdHotstuffReputationAdapterOptions options = TestOptions(/*window_size=*/4);
  options.initial_weights = {100, 100, 100, 100};
  options.initial_weight_root = "old-root";
  options.initial_weight_version = 7;
  options.reputation_config.strong_fault_enabled = true;
  options.reputation_config.double_vote_detection_enabled = true;
  options.reputation_config.strong_fault_target_weight = 1;
  options.signed_vote_evidence_enabled = true;
  TdHotstuffReputationAdapter adapter(/*local_node_id=*/1, /*total_replicas=*/4,
                                      options);
  adapter.Start();

  EXPECT_FALSE(adapter.TryRecordSignedVote(
      VoteSnapshot(/*view=*/1, /*signer=*/2, "proposal-a")));
  EXPECT_EQ(adapter.queued_count(), 0);
  EXPECT_FALSE(adapter.TryRecordSignedVote(
      VoteSnapshot(/*view=*/1, /*signer=*/2, "proposal-a")));
  EXPECT_EQ(adapter.queued_count(), 0);

  ASSERT_TRUE(adapter.TryRecordSignedVote(
      VoteSnapshot(/*view=*/1, /*signer=*/2, "proposal-b")));
  ASSERT_TRUE(adapter.AdvanceWatermark(4));

  auto candidates = WaitForCandidates(&adapter, 1);
  adapter.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 2);
  ASSERT_EQ(candidates[0].strong_faults.size(), 1);
  EXPECT_EQ(candidates[0].strong_faults[0].type,
            resdb::consensus::reputation::StrongFaultType::kDoubleVote);
  EXPECT_EQ(candidates[0].next_weights,
            (std::vector<int64_t>{100, 1, 100, 100}));
}

TEST(TdHotstuffReputationAdapterTest,
     ConvertsInvalidQcSnapshotToInvalidQcEvidence) {
  const TdHotstuffInvalidQcProposalEvidenceSnapshot snapshot =
      InvalidQcSnapshot(/*view=*/9, /*leader=*/3, "proposal-a");

  const auto evidence = ToInvalidQcProposalEvidence(snapshot);

  EXPECT_EQ(evidence.protocol_id, "td_hotstuff");
  EXPECT_EQ(evidence.leader_id, 3);
  EXPECT_EQ(evidence.view_or_round, 9);
  EXPECT_EQ(evidence.slot_or_height, 0);
  EXPECT_EQ(evidence.proposal_hash, "proposal-a");
  EXPECT_TRUE(evidence.proposal_signature_verified);
  EXPECT_FALSE(evidence.qc_verified);
  EXPECT_EQ(evidence.invalid_reason, "qc signer bitmap mismatch");
  EXPECT_EQ(evidence.active_weight_root, "old-root");
  EXPECT_EQ(evidence.weight_version, 7);
}

TEST(TdHotstuffReputationAdapterTest,
     ConvertsWeightUpdateVoteSnapshotToSignedEvidence) {
  const TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot snapshot =
      WeightUpdateVoteSnapshot(/*activation_view=*/64, /*validator=*/2,
                               "candidate-a", /*observed_view=*/32);

  const auto evidence = ToSignedWeightUpdateVoteEvidence(snapshot);

  EXPECT_EQ(evidence.protocol_id, "td_hotstuff");
  EXPECT_EQ(evidence.validator_id, 2);
  EXPECT_EQ(evidence.view_or_round, 32);
  EXPECT_EQ(evidence.old_weight_root, "old-root");
  EXPECT_EQ(evidence.old_weight_version, 7);
  EXPECT_EQ(evidence.activation_view, 64);
  EXPECT_EQ(evidence.candidate_digest, "candidate-a");
  EXPECT_TRUE(evidence.signature_verified);
  EXPECT_EQ(evidence.active_weight_root, "old-root");
  EXPECT_EQ(evidence.weight_version, 7);
}

TEST(TdHotstuffReputationAdapterTest,
     InvalidQcEvidenceReachesRuntimeAndPenalizesLeader) {
  TdHotstuffReputationAdapterOptions options = TestOptions(/*window_size=*/4);
  options.initial_weights = {100, 100, 100, 100};
  options.initial_weight_root =
      resdb::consensus::reputation::WeightRootHex(options.initial_weights);
  options.reputation_config.strong_fault_enabled = true;
  options.reputation_config.invalid_qc_proposal_detection_enabled = true;
  options.reputation_config.strong_fault_target_weight = 1;
  options.invalid_qc_proposal_evidence_enabled = true;
  TdHotstuffReputationAdapter adapter(/*local_node_id=*/1, /*total_replicas=*/4,
                                      options);
  adapter.Start();

  ASSERT_TRUE(adapter.TryRecordInvalidQcProposal(
      InvalidQcSnapshot(/*view=*/1, /*leader=*/3, "proposal-invalid-qc")));
  ASSERT_TRUE(adapter.AdvanceWatermark(4));

  auto candidates = WaitForCandidates(&adapter, 1);
  adapter.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  ASSERT_EQ(candidates[0].strong_faults.size(), 1);
  EXPECT_EQ(candidates[0].strong_faults[0].type,
            resdb::consensus::reputation::StrongFaultType::kInvalidQcProposal);
  EXPECT_EQ(candidates[0].next_weights,
            (std::vector<int64_t>{100, 100, 1, 100}));
}

TEST(TdHotstuffReputationAdapterTest,
     WeightUpdateVoteEvidenceReachesRuntimeAndPenalizesValidator) {
  TdHotstuffReputationAdapterOptions options = TestOptions(/*window_size=*/4);
  options.initial_weights = {100, 100, 100, 100};
  options.initial_weight_root = "old-root";
  options.initial_weight_version = 7;
  options.reputation_config.strong_fault_enabled = true;
  options.reputation_config.weight_update_vote_equivocation_detection_enabled =
      true;
  options.reputation_config.strong_fault_target_weight = 1;
  options.signed_weight_update_vote_evidence_enabled = true;
  TdHotstuffReputationAdapter adapter(/*local_node_id=*/1, /*total_replicas=*/4,
                                      options);
  adapter.Start();

  ASSERT_TRUE(adapter.TryRecordSignedWeightUpdateVote(
      WeightUpdateVoteSnapshot(/*activation_view=*/1, /*validator=*/2,
                               "candidate-a")));
  ASSERT_TRUE(adapter.TryRecordSignedWeightUpdateVote(
      WeightUpdateVoteSnapshot(/*activation_view=*/1, /*validator=*/2,
                               "candidate-b")));
  ASSERT_TRUE(adapter.AdvanceWatermark(4));

  auto candidates = WaitForCandidates(&adapter, 1);
  adapter.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 2);
  ASSERT_EQ(candidates[0].strong_faults.size(), 1);
  EXPECT_EQ(candidates[0].strong_faults[0].type,
            resdb::consensus::reputation::StrongFaultType::
                kWeightUpdateVoteEquivocation);
  EXPECT_EQ(candidates[0].next_weights,
            (std::vector<int64_t>{100, 1, 100, 100}));
}

TEST(TdHotstuffReputationAdapterTest,
     CertifiedQcWindowAddsScheduledLeaderOpportunity) {
  TdHotstuffReputationAdapterOptions options = TestOptions(/*window_size=*/4);
  options.reputation_config.leader_recovery_enabled = true;
  options.reputation_config.min_leader_opportunities = 1;
  options.initial_weights = {100, 100, 100, 100};
  options.initial_weight_root =
      resdb::consensus::reputation::WeightRootHex(options.initial_weights);
  TdHotstuffReputationAdapter adapter(/*local_node_id=*/1, /*total_replicas=*/4,
                                      options);
  adapter.Start();

  ASSERT_TRUE(adapter.TryRecordCertifiedQc(
      Snapshot(/*view=*/4, /*leader=*/1, {1, 2, 3}, {1, 2, 3, 4})));
  ASSERT_TRUE(adapter.AdvanceWatermark(8));

  auto candidates = WaitForCandidates(&adapter, 1);
  adapter.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  EXPECT_EQ(candidates[0].validators[1].leader_opportunity_count, 1);
  EXPECT_EQ(candidates[0].validators[1].leader_certified_count, 0);
}

TEST(TdHotstuffReputationAdapterTest,
     ExplicitTimeoutOutcomeDoesNotAddLeaderOpportunity) {
  TdHotstuffReputationAdapterOptions options = TestOptions(/*window_size=*/1);
  options.reputation_config.leader_recovery_enabled = true;
  options.reputation_config.min_leader_opportunities = 1;
  options.initial_weights = {100, 100, 100, 100};
  options.initial_weight_root =
      resdb::consensus::reputation::WeightRootHex(options.initial_weights);
  TdHotstuffReputationAdapter adapter(/*local_node_id=*/1, /*total_replicas=*/4,
                                      options);
  adapter.Start();

  ASSERT_TRUE(adapter.TryRecordLeaderOutcome(TimeoutSnapshot(5, 3)));
  ASSERT_TRUE(adapter.AdvanceWatermark(6));

  auto candidates = WaitForCandidates(&adapter, 1);
  adapter.Stop();

  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].event_count, 1);
  EXPECT_EQ(candidates[0].validators[1].leader_opportunity_count, 1);
  EXPECT_EQ(candidates[0].validators[2].leader_opportunity_count, 0);
  EXPECT_EQ(candidates[0].validators[2].leader_certified_count, 0);
  EXPECT_EQ(candidates[0].validators[2].leader_score, 100);
}

TEST(TdHotstuffReputationAdapterTest,
     CandidateExcusesAvailableButUnselectedSigner) {
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
  EXPECT_EQ(candidates[0].validators[3].opportunities, 0);
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
