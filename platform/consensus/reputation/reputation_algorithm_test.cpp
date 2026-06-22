#include "platform/consensus/reputation/reputation_algorithm.h"
#include "platform/consensus/reputation/soft_reputation.h"

#include <algorithm>
#include <initializer_list>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace resdb {
namespace consensus {
namespace reputation {
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

std::string BitmapFromVector(const std::vector<int>& signers,
                             int total_replicas) {
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

std::vector<int> ConsecutiveModuloSigners(int start, int count,
                                          int total_replicas) {
  std::vector<int> signers;
  signers.reserve(std::max(count, 0));
  for (int offset = 0; offset < count; ++offset) {
    signers.push_back(((start - 1 + offset) % total_replicas) + 1);
  }
  return signers;
}

ReputationConfig TestConfig() {
  ReputationConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 1;
  config.leader_recovery_enabled = true;
  return config;
}

struct TestEvidence {
  CertifiedSignerEvidence certified;
  LeaderOutcomeEvidence leader_outcome;
  bool is_leader_outcome = false;
};

TestEvidence CertifiedQc(int view, int leader, std::string signer_bitmap,
                         std::string available_signer_bitmap = "") {
  TestEvidence event;
  event.certified.view_or_round = view;
  event.certified.leader_id = leader;
  event.certified.artifact_digest = "qc-" + std::to_string(view);
  event.certified.signer_bitmap = std::move(signer_bitmap);
  event.certified.available_signer_bitmap = std::move(available_signer_bitmap);
  return event;
}

TestEvidence TimeoutEvidence(int view, int leader) {
  TestEvidence event;
  event.is_leader_outcome = true;
  event.leader_outcome.view_or_round = view;
  event.leader_outcome.leader_id = leader;
  event.leader_outcome.outcome_class = OutcomeClass::kTimeoutOrViewChange;
  event.leader_outcome.artifact_digest = "timeout-" + std::to_string(view);
  return event;
}

TestEvidence CertifyOnlyEvidence(int view, int leader) {
  TestEvidence event;
  event.is_leader_outcome = true;
  event.leader_outcome.view_or_round = view;
  event.leader_outcome.leader_id = leader;
  event.leader_outcome.outcome_class = OutcomeClass::kCertifyOnly;
  event.leader_outcome.artifact_digest = "certify-only-" + std::to_string(view);
  return event;
}

TestEvidence CommittedLeaderEvidence(int view, int leader) {
  TestEvidence event;
  event.is_leader_outcome = true;
  event.leader_outcome.view_or_round = view;
  event.leader_outcome.leader_id = leader;
  event.leader_outcome.outcome_class = OutcomeClass::kCommitted;
  event.leader_outcome.artifact_digest = "commit-" + std::to_string(view);
  return event;
}

ReputationWindowInput BuildWindowInput(
    int node_id, int total_replicas, uint64_t window_index,
    const std::vector<TestEvidence>& evidence,
    const std::vector<int64_t>& current_weights,
    const std::string& old_weight_root_hex = "",
    uint64_t old_weight_version = 0, int activation_view = 0) {
  ReputationWindowInput input;
  input.local_node_id = node_id;
  input.total_replicas = total_replicas;
  input.window_index = window_index;
  input.current_weights = current_weights;
  input.old_weight_root_hex = old_weight_root_hex;
  input.old_weight_version = old_weight_version;
  input.activation_view = activation_view;
  for (const TestEvidence& event : evidence) {
    if (event.is_leader_outcome) {
      input.leader_outcome_evidence.push_back(event.leader_outcome);
    } else {
      input.certified_signer_evidence.push_back(event.certified);
    }
  }
  return input;
}

ReputationCandidate ComputeCandidate(
    int node_id, int total_replicas, uint64_t window_index,
    const std::vector<TestEvidence>& evidence,
    const std::vector<int64_t>& current_weights,
    const ReputationConfig& config,
    const std::string& old_weight_root_hex = "",
    uint64_t old_weight_version = 0, int activation_view = 0,
    const std::vector<SignedProposalEvidence>& signed_proposal_evidence = {},
    const std::vector<SignedVoteEvidence>& signed_vote_evidence = {},
    const std::vector<InvalidQcProposalEvidence>& invalid_qc_proposal_evidence =
        {},
    const std::vector<SignedWeightUpdateVoteEvidence>&
        signed_weight_update_vote_evidence = {},
    const std::vector<int>& removed_strong_fault_evidence_a = {},
    const std::vector<int>& removed_strong_fault_evidence_b = {},
    const std::vector<VerifiedQcArtifactEvidence>& verified_qc_artifact_evidence =
        {},
    const std::vector<int>& prior_peertrust_leader_debt = {},
    const std::vector<int>& prior_sybil_graph_debt = {},
    const std::vector<uint64_t>& scheduled_leader_counts = {},
    const std::vector<int64_t>& current_leader_weights = {}) {
  ReputationWindowInput input = BuildWindowInput(
      node_id, total_replicas, window_index, evidence, current_weights,
      old_weight_root_hex, old_weight_version, activation_view);
  input.current_leader_weights = current_leader_weights;
  input.signed_proposal_evidence = signed_proposal_evidence;
  input.signed_vote_evidence = signed_vote_evidence;
  input.invalid_qc_proposal_evidence = invalid_qc_proposal_evidence;
  input.signed_weight_update_vote_evidence = signed_weight_update_vote_evidence;
  (void)removed_strong_fault_evidence_a;
  (void)removed_strong_fault_evidence_b;
  input.verified_qc_artifact_evidence = verified_qc_artifact_evidence;
  input.prior_peertrust_leader_debt = prior_peertrust_leader_debt;
  input.prior_sybil_graph_debt = prior_sybil_graph_debt;
  input.scheduled_leader_counts = scheduled_leader_counts;
  return ComputeReputationCandidate(input, config);
}

SignedProposalEvidence ProposalArtifact(int leader, int view, int slot,
                                        std::string proposal_hash,
                                        bool signature_verified = true) {
  SignedProposalEvidence artifact;
  artifact.protocol_id = "td_hotstuff";
  artifact.leader_id = leader;
  artifact.view_or_round = view;
  artifact.slot_or_height = slot;
  artifact.proposal_hash = std::move(proposal_hash);
  artifact.signature_verified = signature_verified;
  artifact.weight_version = 7;
  artifact.active_weight_root = "old-root";
  return artifact;
}

SignedVoteEvidence VoteArtifact(int signer, int view, int slot,
                                std::string proposal_hash,
                                bool signature_verified = true) {
  SignedVoteEvidence artifact;
  artifact.protocol_id = "td_hotstuff";
  artifact.signer_id = signer;
  artifact.view_or_round = view;
  artifact.slot_or_height = slot;
  artifact.proposal_hash = std::move(proposal_hash);
  artifact.signature_verified = signature_verified;
  artifact.weight_version = 7;
  artifact.active_weight_root = "old-root";
  return artifact;
}

InvalidQcProposalEvidence InvalidQcArtifact(
    int leader, int view, int slot, std::string proposal_hash,
    bool proposal_signature_verified = true, bool qc_verified = false) {
  InvalidQcProposalEvidence artifact;
  artifact.protocol_id = "td_hotstuff";
  artifact.leader_id = leader;
  artifact.view_or_round = view;
  artifact.slot_or_height = slot;
  artifact.proposal_hash = std::move(proposal_hash);
  artifact.proposal_signature_verified = proposal_signature_verified;
  artifact.qc_verified = qc_verified;
  artifact.invalid_reason = "bad_qc";
  artifact.weight_version = 7;
  artifact.active_weight_root = "old-root";
  return artifact;
}

SignedWeightUpdateVoteEvidence WeightUpdateVoteArtifact(
    int validator, std::string candidate_digest,
    bool signature_verified = true) {
  SignedWeightUpdateVoteEvidence artifact;
  artifact.protocol_id = "td_hotstuff";
  artifact.validator_id = validator;
  artifact.old_weight_root = "old-root";
  artifact.old_weight_version = 7;
  artifact.activation_view = 64;
  artifact.candidate_digest = std::move(candidate_digest);
  artifact.signature_verified = signature_verified;
  artifact.weight_version = 7;
  artifact.active_weight_root = "old-root";
  return artifact;
}

VerifiedQcArtifactEvidence QcArtifact(int view, int slot, std::string qc_hash,
                                      std::string signer_bitmap,
                                      bool qc_verified = true) {
  VerifiedQcArtifactEvidence artifact;
  artifact.protocol_id = "td_hotstuff";
  artifact.view_or_round = view;
  artifact.slot_or_height = slot;
  artifact.qc_hash = std::move(qc_hash);
  artifact.signer_bitmap = std::move(signer_bitmap);
  artifact.qc_verified = qc_verified;
  artifact.weight_version = 7;
  artifact.active_weight_root = "old-root";
  return artifact;
}


TEST(ReputationAlgorithmTest, WindowInputComputesFromCertifiedSignerEvidence) {
  ReputationConfig config = TestConfig();
  ReputationWindowInput input;
  input.local_node_id = 1;
  input.total_replicas = 4;
  input.window_index = 9;
  input.current_weights = {25, 25, 25, 25};
  input.old_weight_root_hex = "old-root";
  input.old_weight_version = 7;
  input.activation_view = 64;

  for (int view = 1; view <= 4; ++view) {
    CertifiedSignerEvidence certified;
    certified.view_or_round = view;
    certified.leader_id = ((view - 1) % 4) + 1;
    certified.artifact_digest = "qc-" + std::to_string(view);
    certified.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    certified.available_signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    input.certified_signer_evidence.push_back(std::move(certified));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(input, config);

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({25, 25, 25, 25}));
  EXPECT_EQ(candidate.metric_root_hex.empty(), false);
  EXPECT_EQ(candidate.reputation_root_hex.empty(), false);
  EXPECT_EQ(candidate.next_weight_root_hex.empty(), false);
  EXPECT_EQ(candidate.candidate_digest_hex.empty(), false);
}

TEST(ReputationAlgorithmTest, DecodesSignerBitmap) {
  EXPECT_EQ(DecodeSignerBitmap(std::string(1, static_cast<char>(0x09)), 5),
            std::vector<int>({1, 4}));
}

TEST(ReputationAlgorithmTest, AllGoodWindowKeepsWeightsStable) {
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, TestConfig(), "old-root", 0, 64);

  EXPECT_EQ(candidate.algorithm, "bayes_v4");
  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({30, 30, 30, 30}));
  EXPECT_FALSE(candidate.metric_root_hex.empty());
  EXPECT_FALSE(candidate.reputation_root_hex.empty());
  EXPECT_FALSE(candidate.candidate_digest_hex.empty());
}

TEST(ReputationAlgorithmTest, BonusDoesNotDriftBalancedAllGoodWeights) {
  ReputationConfig config = TestConfig();
  config.bonus_per_epoch = 1;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 16; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({30, 30, 30, 30}));
  for (const ValidatorReputation& validator : candidate.validators) {
    EXPECT_EQ(validator.bonus_credit, 0);
  }
}

TEST(ReputationAlgorithmTest, BonusCanHelpBelowMeanHonestValidatorCatchUp) {
  ReputationConfig config = TestConfig();
  config.bonus_per_epoch = 1;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 16; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {20, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].bonus_credit, 1);
  EXPECT_EQ(candidate.validators[0].next_weight, 21);
  EXPECT_EQ(candidate.validators[1].bonus_credit, 0);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(ReputationAlgorithmTest, PartialHealthyBelowMeanValidatorCanCatchUp) {
  ReputationConfig config = TestConfig();
  config.bonus_per_epoch = 1;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 5; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }
  for (int view = 6; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({2, 3, 4}, 4),
                                   Bitmap({2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {20, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_GE(candidate.validators[0].vote_score, 60);
  EXPECT_EQ(candidate.validators[0].recovery_credit,
            candidate.validators[0].decay_applied);
  EXPECT_EQ(candidate.validators[0].bonus_credit, 1);
  EXPECT_EQ(candidate.validators[0].next_weight, 21);
}

TEST(ReputationAlgorithmTest, PriorVoteBetaCountersInfluenceVoteScore) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 10;
  config.max_recovery_per_epoch = 10;
  config.vote_beta_decay_per_mille = 1000;

  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  ReputationWindowInput input = BuildWindowInput(
      1, 4, 1, evidence, {100, 100, 100, 100}, "old-root", 0, 64);
  input.prior_vote_beta_counters.resize(4);
  input.prior_vote_beta_counters[0].failure = 64 * 1000;

  const ReputationCandidate candidate =
      ComputeReputationCandidate(input, config);

  EXPECT_EQ(candidate.validators[0].vote_beta_failure, 64 * 1000);
  EXPECT_LT(candidate.validators[0].vote_score,
            VoteScore(candidate.validators[0].inclusions,
                      candidate.validators[0].opportunities));
  EXPECT_LT(candidate.validators[0].recovery_credit,
            candidate.validators[0].decay_applied);
  EXPECT_EQ(candidate.validators[1].vote_score,
            VoteScore(candidate.validators[1].inclusions,
                      candidate.validators[1].opportunities));
}



TEST(ReputationAlgorithmTest,
     MultiplicativeFormulaIsDefaultAndPreservesHealthyCurrentWeight) {
  ReputationConfig config = TestConfig();

  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 50, 80, 100}, config, "old-root", 0, 64);

  EXPECT_TRUE(config.multiplicative_weight_formula_enabled);
  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({30, 50, 80, 100}));
  EXPECT_EQ(candidate.validators[0].stake_factor_per_mille, 1000);
  EXPECT_EQ(candidate.validators[1].stake_factor_per_mille, 1000);
  EXPECT_EQ(candidate.validators[2].stake_factor_per_mille, 1000);
  EXPECT_EQ(candidate.validators[3].stake_factor_per_mille, 1000);
  for (size_t i = 0; i < candidate.validators.size(); ++i) {
    const ValidatorReputation& validator = candidate.validators[i];
    EXPECT_EQ(validator.identity_factor_per_mille, 1000);
    EXPECT_EQ(validator.reputation_factor_per_mille,
              static_cast<int>(candidate.next_weights[i] * 10));
    EXPECT_EQ(validator.direct_penalty_factor_per_mille, 1000);
  }
}

TEST(ReputationAlgorithmTest,
     MultiplicativeFormulaSmoothsBadSoftReputationByDefault) {
  ReputationConfig config = TestConfig();

  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 5) + 1,
                                   Bitmap({2, 3, 4, 5}, 5),
                                   Bitmap({2, 3, 4, 5}, 5)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 5, 1, evidence, {100, 100, 100, 100, 100}, config, "old-root", 0, 64);

  EXPECT_TRUE(config.multiplicative_weight_formula_enabled);
  EXPECT_EQ(candidate.validators[0].vote_score, 20);
  EXPECT_EQ(candidate.validators[0].reputation_factor_per_mille, 980);
  EXPECT_EQ(candidate.validators[0].next_weight, 98);
  EXPECT_EQ(candidate.validators[1].reputation_factor_per_mille, 1000);
  EXPECT_EQ(candidate.validators[1].next_weight, 100);
}

TEST(ReputationAlgorithmTest,
     FirstWindowPartialVoteSampleDoesNotDecayMaxWeightValidator) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 99;
  config.max_recovery_per_epoch = 99;
  config.bonus_per_epoch = 0;
  config.multiplicative_weight_formula_enabled = false;

  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 64; ++view) {
    std::vector<int> signers;
    if (view <= 10) {
      for (int signer = 3; signer <= 15; ++signer) {
        signers.push_back(signer);
      }
      signers.push_back(20);
    } else {
      for (int signer = 3; signer <= 16; ++signer) {
        signers.push_back(signer);
      }
    }
    evidence.push_back(CertifiedQc(view, ((view - 1) % 20) + 1,
                                   BitmapFromVector(signers, 20)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 100), config,
      "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].inclusions, 0);
  EXPECT_EQ(candidate.validators[0].next_weight, 1);
  EXPECT_EQ(candidate.validators[19].inclusions, 10);
  EXPECT_LT(candidate.validators[19].vote_score, 30);
  EXPECT_EQ(candidate.validators[19].decay_applied, 0);
  EXPECT_EQ(candidate.validators[19].next_weight, 100);
}

TEST(ReputationAlgorithmTest,
     NarrowAvailableSignerSamplesDoNotCreateVoteMisses) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 99;
  config.max_recovery_per_epoch = 99;
  config.bonus_per_epoch = 0;
  config.multiplicative_weight_formula_enabled = true;

  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 64; ++view) {
    std::vector<int> signers;
    if (view <= 17) {
      for (int signer = 1; signer <= 13; ++signer) {
        signers.push_back(signer);
      }
      signers.push_back(20);
    } else {
      const int start = ((view - 18) % 19) + 1;
      for (int offset = 0; offset < 14; ++offset) {
        signers.push_back(((start - 1 + offset) % 19) + 1);
      }
    }
    evidence.push_back(CertifiedQc(view, ((view - 1) % 20) + 1,
                                   BitmapFromVector(signers, 20),
                                   BitmapFromVector(signers, 20)));
  }

  ReputationWindowInput input = BuildWindowInput(
      1, 20, 2, evidence, std::vector<int64_t>(20, 100), "old-root", 0,
      64);
  input.prior_vote_beta_counters.resize(20);
  input.prior_vote_beta_counters[19].success = 1000;

  const ReputationCandidate candidate =
      ComputeReputationCandidate(input, config);

  EXPECT_EQ(candidate.validators[19].inclusions, 17);
  EXPECT_EQ(candidate.validators[19].opportunities,
            candidate.validators[19].inclusions);
  EXPECT_GE(candidate.validators[19].vote_score, 90);
  EXPECT_EQ(candidate.validators[19].next_weight, 100);
}

TEST(ReputationAlgorithmTest,
     NearZeroQuorumAvailableParticipationStillLosesRecovery) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 99;
  config.max_recovery_per_epoch = 99;
  config.bonus_per_epoch = 0;
  config.multiplicative_weight_formula_enabled = true;

  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 64; ++view) {
    std::vector<int> signers;
    if (view == 1) {
      for (int signer = 1; signer <= 13; ++signer) {
        signers.push_back(signer);
      }
      signers.push_back(20);
    } else {
      const int start = ((view - 2) % 19) + 1;
      for (int offset = 0; offset < 14; ++offset) {
        signers.push_back(((start - 1 + offset) % 19) + 1);
      }
    }
    evidence.push_back(CertifiedQc(view, ((view - 1) % 20) + 1,
                                   BitmapFromVector(signers, 20),
                                   BitmapFromVector(signers, 20)));
  }

  ReputationWindowInput input = BuildWindowInput(
      1, 20, 2, evidence, std::vector<int64_t>(20, 100), "old-root", 0,
      64);
  input.prior_vote_beta_counters.resize(20);
  input.prior_vote_beta_counters[19].success = 1000;

  const ReputationCandidate candidate =
      ComputeReputationCandidate(input, config);

  EXPECT_EQ(candidate.validators[19].inclusions, 1);
  EXPECT_GT(candidate.validators[19].opportunities,
            candidate.validators[19].inclusions);
  EXPECT_LT(candidate.validators[19].vote_score, 30);
  EXPECT_LT(candidate.validators[19].next_weight, 100);
}

TEST(ReputationAlgorithmTest,
     MultiplicativeFormulaUsesStakeIdentityAndReputationFactors) {
  ReputationConfig config = TestConfig();
  config.multiplicative_weight_formula_enabled = true;
  config.stake_exponent_tau_per_mille = 1000;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;

  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 5) + 1,
                                   Bitmap({2, 3, 4, 5}, 5),
                                   Bitmap({2, 3, 4, 5}, 5)));
  }
  ReputationWindowInput input = BuildWindowInput(
      1, 5, 1, evidence, {100, 100, 100, 100, 100}, "old-root", 0, 64);
  input.stake_factors_per_mille = {1000, 800, 1000, 1000, 1000};
  input.identity_factors_per_mille = {800, 1000, 1000, 1000, 1000};

  const ReputationCandidate candidate =
      ComputeReputationCandidate(input, config);

  EXPECT_EQ(candidate.validators[0].stake_factor_per_mille, 1000);
  EXPECT_EQ(candidate.validators[0].stake_power_factor_per_mille, 1000);
  EXPECT_EQ(candidate.validators[0].identity_factor_per_mille, 800);
  EXPECT_EQ(candidate.validators[0].reputation_factor_per_mille, 980);
  EXPECT_EQ(candidate.validators[0].direct_penalty_factor_per_mille, 1000);
  EXPECT_EQ(candidate.validators[0].next_weight, 78);
  EXPECT_EQ(candidate.validators[1].stake_factor_per_mille, 800);
  EXPECT_EQ(candidate.validators[1].identity_factor_per_mille, 1000);
  EXPECT_EQ(candidate.validators[1].reputation_factor_per_mille, 1000);
  EXPECT_EQ(candidate.validators[1].next_weight, 80);
  EXPECT_EQ(candidate.validators[2].next_weight, 100);
  EXPECT_EQ(candidate.validators[3].next_weight, 100);
}

TEST(ReputationAlgorithmTest,
     MultiplicativeFormulaUsesDeterministicDefaultStakeAndIdentity) {
  ReputationConfig config = TestConfig();
  config.multiplicative_weight_formula_enabled = true;
  config.stake_factor_min_per_mille = 900;
  config.stake_factor_max_per_mille = 900;
  config.identity_factor_min_per_mille = 950;
  config.identity_factor_max_per_mille = 950;

  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {100, 100, 100, 100}, config, "old-root", 0, 64);

  for (const ValidatorReputation& validator : candidate.validators) {
    EXPECT_EQ(validator.stake_factor_per_mille, 900);
    EXPECT_EQ(validator.identity_factor_per_mille, 950);
    EXPECT_EQ(validator.reputation_factor_per_mille, 1000);
    EXPECT_EQ(validator.next_weight, 86);
  }
}

TEST(ReputationAlgorithmTest, LeaderWeightsStayRoundRobinWhenEveryoneEligible) {
  ReputationConfig config = TestConfig();
  config.leader_eligible_min_weight = 10;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {99, 100, 100, 100}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({99, 100, 100, 100}));
  EXPECT_EQ(candidate.leader_weights,
            std::vector<int64_t>({100, 100, 100, 100}));
}

TEST(ReputationAlgorithmTest, LeaderWeightsPreserveBelowThresholdForExclusion) {
  ReputationConfig config = TestConfig();
  config.leader_recovery_enabled = false;
  config.leader_eligible_min_weight = 10;

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, /*evidence=*/{}, {1, 100, 100, 100}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({1, 100, 100, 100}));
  EXPECT_EQ(candidate.leader_weights,
            std::vector<int64_t>({1, 100, 100, 100}));
}

TEST(ReputationAlgorithmTest, LeaderWeightsDropThresholdBoundaryFromLeaderSet) {
  ReputationConfig config = TestConfig();
  config.leader_recovery_enabled = false;
  config.leader_eligible_min_weight = 10;

  std::vector<TestEvidence> evidence;
  evidence.push_back(CertifiedQc(1, 2, Bitmap({1, 2, 3, 4}, 4),
                                 Bitmap({1, 2, 3, 4}, 4)));

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {10, 100, 100, 100}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({10, 100, 100, 100}));
  EXPECT_EQ(candidate.leader_weights,
            std::vector<int64_t>({1, 100, 100, 100}));
}

TEST(ReputationAlgorithmTest, SlowVoterLosesRecoveryWithoutDirectSlash) {
  ReputationConfig config = TestConfig();
  config.leader_recovery_enabled = false;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 5) + 1,
                                   Bitmap({1, 2, 3, 4}, 5),
                                   Bitmap({1, 2, 3, 4}, 5)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 5, 1, evidence, {30, 30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_GT(candidate.validators[4].opportunities, 0);
  EXPECT_EQ(candidate.validators[4].inclusions, 0);
  EXPECT_LT(candidate.validators[4].vote_score, 30);
  EXPECT_LT(candidate.validators[4].next_weight, 30);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
}

TEST(ReputationAlgorithmTest,
     SlowVoterDecayDoesNotReduceLeaderWeightWhenLeaderScoreHealthy) {
  ReputationConfig config = TestConfig();
  config.leader_eligible_min_weight = 10;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 10; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 5) + 1,
                                   Bitmap({1, 2, 3, 4}, 5),
                                   Bitmap({1, 2, 3, 4}, 5)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 5, 1, evidence, {11, 11, 11, 11, 11}, config, "old-root", 0, 64);

  EXPECT_LT(candidate.validators[4].next_weight,
            config.leader_eligible_min_weight);
  EXPECT_EQ(candidate.validators[4].leader_score, 100);
  EXPECT_EQ(candidate.leader_weights[4], 100);
}

TEST(ReputationAlgorithmTest,
     CurrentLeaderIneligibilityPersistsWithoutLeaderReentryEvidence) {
  ReputationConfig config = TestConfig();
  config.leader_eligible_min_weight = 10;
  config.min_leader_opportunities = 3;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, 2, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {1, 100, 100, 100}, config, "old-root", 1, 64,
      {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {1, 100, 100, 100});

  EXPECT_EQ(candidate.validators[0].leader_opportunity_count, 0);
  EXPECT_EQ(candidate.validators[0].next_weight, 1);
  EXPECT_EQ(candidate.leader_weights[0], 1);
}

TEST(ReputationAlgorithmTest, SingleTransientTimeoutDoesNotReduceLeaderRecovery) {
  ReputationConfig config = TestConfig();
  config.min_leader_opportunities = 8;
  std::vector<TestEvidence> evidence;
  evidence.push_back(CertifiedQc(1, 1, Bitmap({1, 2, 3, 4}, 4),
                                 Bitmap({1, 2, 3, 4}, 4)));
  evidence.push_back(TimeoutEvidence(2, 2));

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[1].leader_certified_count, 0);
  EXPECT_EQ(candidate.validators[1].leader_opportunity_count, 0);
  EXPECT_GE(candidate.validators[1].vote_score, 40);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(ReputationAlgorithmTest,
     ExplicitTimeoutOutcomeDoesNotAffectLeaderPosteriorByDefault) {
  ReputationConfig config = TestConfig();
  config.min_leader_opportunities = 1;

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, {TimeoutEvidence(2, 2)}, {30, 30, 30, 30}, config,
      "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[1].leader_dirichlet_timeout, 0);
  EXPECT_EQ(candidate.validators[1].leader_score, 100);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(ReputationAlgorithmTest,
     DirichletLeaderPosteriorRanksCommitCertifyOnlyAndTimeout) {
  ReputationConfig config = TestConfig();
  config.leader_timeout_outcome_enabled = true;
  config.leader_dirichlet_scoring_enabled = true;

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1,
      {CommittedLeaderEvidence(1, 1), CertifyOnlyEvidence(2, 2),
       TimeoutEvidence(3, 3)},
      {30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_GT(candidate.validators[0].leader_score,
            candidate.validators[1].leader_score);
  EXPECT_GT(candidate.validators[1].leader_score,
            candidate.validators[2].leader_score);
  EXPECT_GT(candidate.validators[1].leader_score, 0);
  EXPECT_LT(candidate.validators[1].leader_score, 100);
}

TEST(ReputationAlgorithmTest,
     ScheduledLeaderMissesBecomeDirichletTimeoutMass) {
  ReputationConfig config = TestConfig();
  config.leader_dirichlet_scoring_enabled = true;
  config.min_leader_opportunities = 3;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {100, 100, 100, 100}, config, "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, {}, {}, {4, 4, 0, 0});

  EXPECT_GT(candidate.validators[1].leader_dirichlet_timeout, 0);
  EXPECT_EQ(candidate.validators[1].leader_dirichlet_commit, 0);
  EXPECT_LT(candidate.validators[1].leader_score,
            candidate.validators[0].leader_score);
  EXPECT_GT(candidate.validators[1].leader_score, 0);
  EXPECT_LT(candidate.leader_weights[1], candidate.leader_weights[0]);
}

TEST(ReputationAlgorithmTest, SilentLeaderLosesLeaderRecovery) {
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, TestConfig(), "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, {}, {}, {4, 4, 0, 0});

  EXPECT_EQ(candidate.validators[0].leader_certified_count, 4);
  EXPECT_EQ(candidate.validators[0].leader_opportunity_count, 4);
  EXPECT_EQ(candidate.validators[1].leader_certified_count, 0);
  EXPECT_EQ(candidate.validators[1].leader_opportunity_count, 4);
  EXPECT_EQ(candidate.validators[1].next_weight,
            candidate.validators[0].next_weight);
  EXPECT_LT(candidate.leader_weights[1], candidate.leader_weights[0]);
}

TEST(ReputationAlgorithmTest,
     ScheduledLeaderMissWithHealthyVotesOnlyReducesLeaderWeight) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 99;
  config.max_recovery_per_epoch = 99;
  config.min_leader_opportunities = 3;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {100, 100, 100, 100}, config, "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, {}, {}, {4, 4, 0, 0});

  EXPECT_EQ(candidate.validators[1].leader_certified_count, 0);
  EXPECT_EQ(candidate.validators[1].leader_opportunity_count, 4);
  EXPECT_GE(candidate.validators[1].vote_score, 40);
  EXPECT_EQ(candidate.validators[1].next_weight, 100);
  EXPECT_EQ(candidate.leader_weights[1], 1);
}

TEST(ReputationAlgorithmTest,
     PeerTrustDoesNotClampScheduledSilentLeaderRecovery) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {67, 67, 67, 67}, config, "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, {}, {}, {4, 4, 0, 0});

  EXPECT_EQ(candidate.validators[1].leader_certified_count, 0);
  EXPECT_EQ(candidate.validators[1].leader_opportunity_count, 4);
  EXPECT_EQ(candidate.validators[1].peertrust_score, 100);
  EXPECT_EQ(candidate.validators[1].peertrust_leader_debt, 0);
  EXPECT_EQ(candidate.validators[1].next_weight,
            config.peertrust_soft_min_weight);
  EXPECT_EQ(candidate.leader_weights[1],
            config.max_weight - config.decay_per_epoch);
}

TEST(ReputationAlgorithmTest, OneScheduledLeaderMissDoesNotReduceRecovery) {
  ReputationConfig config = TestConfig();
  config.min_leader_opportunities = 8;
  config.leader_recovery_enabled = true;
  std::vector<TestEvidence> evidence;
  evidence.push_back(CertifiedQc(1, 1, Bitmap({1, 2, 3, 4}, 4),
                                 Bitmap({1, 2, 3, 4}, 4)));

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, {}, {}, {1, 1, 0, 0});

  EXPECT_EQ(candidate.validators[1].leader_certified_count, 0);
  EXPECT_EQ(candidate.validators[1].leader_opportunity_count, 1);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(ReputationAlgorithmTest, FewMostlySuccessfulLeaderOpportunitiesStayNeutral) {
  ReputationConfig config = TestConfig();
  config.min_leader_opportunities = 8;
  config.leader_recovery_enabled = true;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 3; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {100, 100, 100, 100}, config, "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, {}, {}, {4, 0, 0, 0});

  EXPECT_EQ(candidate.validators[0].leader_certified_count, 3);
  EXPECT_EQ(candidate.validators[0].leader_opportunity_count, 4);
  EXPECT_EQ(candidate.validators[0].leader_score, 100);
  EXPECT_EQ(candidate.leader_weights[0], 100);
}

TEST(ReputationAlgorithmTest,
     RepeatedScheduledLeaderMissesReduceRecoveryBeforeFullThreshold) {
  ReputationConfig config = TestConfig();
  config.min_leader_opportunities = 8;
  config.leader_recovery_enabled = true;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 3; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, {}, {}, {3, 3, 0, 0});

  EXPECT_EQ(candidate.validators[1].leader_certified_count, 0);
  EXPECT_EQ(candidate.validators[1].leader_opportunity_count, 3);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
  EXPECT_LT(candidate.leader_weights[1], candidate.leader_weights[0]);
}

TEST(ReputationAlgorithmTest,
     RepeatedLeaderTimeoutsDoNotReduceRecovery) {
  ReputationConfig config = TestConfig();
  config.min_leader_opportunities = 8;
  config.leader_recovery_enabled = true;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 3; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
    evidence.push_back(TimeoutEvidence(view + 10, 2));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[1].leader_certified_count, 0);
  EXPECT_EQ(candidate.validators[1].leader_opportunity_count, 0);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(ReputationAlgorithmTest,
     LeaderRecoveryPreventsCatchupBonusAtEligibilityBoundary) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 3;
  config.max_recovery_per_epoch = 3;
  config.bonus_per_epoch = 1;
  config.leader_eligible_min_weight = 10;
  std::vector<TestEvidence> evidence;
  evidence.push_back(CertifiedQc(1, 2, Bitmap({1, 2, 3, 4}, 4),
                                 Bitmap({1, 2, 3, 4}, 4)));

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {9, 100, 100, 100}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].current_weight, 9);
  EXPECT_EQ(candidate.validators[0].bonus_credit, 0);
  EXPECT_EQ(candidate.validators[0].next_weight, 9);
}

TEST(ReputationAlgorithmTest, NarrowSignerTargetOnlyReducesLeaderRecovery) {
  std::vector<TestEvidence> evidence;
  const std::string narrow_quorum =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 20);
  const std::string all_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, narrow_quorum, all_available));
  }
  for (int view = 9; view <= 16; ++view) {
    evidence.push_back(CertifiedQc(view, 2, all_available, all_available));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), TestConfig(),
      "old-root", 0, 64);

  EXPECT_LT(candidate.validators[0].leader_diversity_score,
            candidate.validators[1].leader_diversity_score);
  EXPECT_LT(candidate.validators[0].leader_score,
            candidate.validators[1].leader_score);
  EXPECT_EQ(candidate.validators[0].next_weight,
            candidate.validators[1].next_weight);
  EXPECT_LT(candidate.leader_weights[0], candidate.leader_weights[1]);
}

TEST(ReputationAlgorithmTest,
     DeterministicQuorumSelectionDoesNotReduceLeaderRecovery) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 40; ++view) {
    const int leader = ((view - 1) % 20) + 1;
    const std::string deterministic_quorum =
        BitmapFromVector(ConsecutiveModuloSigners(leader, 14, 20), 20);
    const std::string broad_available =
        BitmapFromVector(ConsecutiveModuloSigners(leader, 17, 20), 20);
    evidence.push_back(
        CertifiedQc(view, leader, deterministic_quorum, broad_available));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 100), config,
      "old-root", 0, 64);

  for (const ValidatorReputation& validator : candidate.validators) {
    EXPECT_EQ(validator.leader_diversity_score, 100)
        << "validator " << validator.validator_id;
    EXPECT_EQ(validator.next_weight, 100)
        << "validator " << validator.validator_id;
    EXPECT_EQ(validator.strong_fault_count, 0);
    EXPECT_EQ(validator.penalty_points, 0);
  }
}

TEST(ReputationAlgorithmTest,
     UncertifiedLeaderSoftFaultDecaysLeaderWeightGradually) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 2;
  config.max_recovery_per_epoch = 2;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, 2, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, std::vector<int64_t>(4, 100), config,
      "old-root", 0, 64, {}, {}, {}, {}, {}, {}, {}, {}, {}, {4, 0, 0, 0},
      std::vector<int64_t>(4, 100));

  EXPECT_EQ(candidate.validators[0].leader_certified_count, 0);
  EXPECT_EQ(candidate.validators[0].leader_opportunity_count, 4);
  EXPECT_EQ(candidate.validators[0].leader_score, 0);
  EXPECT_EQ(candidate.validators[0].next_weight, 100);
  EXPECT_EQ(candidate.leader_weights[0], 98);
  EXPECT_EQ(candidate.leader_weights[1], 100);
  EXPECT_EQ(candidate.validators[0].strong_fault_count, 0);
  EXPECT_EQ(candidate.validators[0].penalty_points, 0);
}

TEST(ReputationAlgorithmTest,
     SignerDiversityGateAffectsLeaderScoreNotVotingWeight) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  std::vector<TestEvidence> evidence;
  const std::string clique_quorum = BitmapFromVector(
      ConsecutiveModuloSigners(1, 14, 20), 20);
  const std::string all_available = BitmapFromVector(
      ConsecutiveModuloSigners(1, 20, 20), 20);
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, clique_quorum, all_available));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 100), config,
      "old-root", 0, 64);

  EXPECT_LT(candidate.validators[0].leader_diversity_score, 30);
  EXPECT_EQ(candidate.validators[0].leader_score,
            config.leader_diversity_soft_min_weight);
  EXPECT_GE(candidate.validators[0].vote_score, 90);
  EXPECT_EQ(candidate.validators[0].next_weight, 100);
  EXPECT_LT(candidate.leader_weights[0], 100);
  EXPECT_GE(candidate.leader_weights[0],
            config.leader_diversity_soft_min_weight);
  EXPECT_EQ(candidate.validators[0].strong_fault_count, 0);
  EXPECT_EQ(candidate.validators[0].penalty_points, 0);
}

TEST(ReputationAlgorithmTest,
     SignerDiversityGateDoesNotDropLeaderWeightBelowSoftFloor) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.leader_diversity_soft_min_weight = 80;
  std::vector<TestEvidence> evidence;
  const std::string clique_quorum = BitmapFromVector(
      ConsecutiveModuloSigners(1, 14, 20), 20);
  const std::string all_available = BitmapFromVector(
      ConsecutiveModuloSigners(1, 20, 20), 20);
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, clique_quorum, all_available));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 81), config,
      "old-root", 0, 64);

  EXPECT_LT(candidate.validators[0].leader_diversity_score, 30);
  EXPECT_EQ(candidate.validators[0].leader_score,
            config.leader_diversity_soft_min_weight);
  EXPECT_EQ(candidate.validators[0].next_weight, 81);
  EXPECT_EQ(candidate.leader_weights[0], 80);
  EXPECT_EQ(candidate.validators[0].strong_fault_count, 0);
  EXPECT_EQ(candidate.validators[0].penalty_points, 0);
}

TEST(ReputationAlgorithmTest,
     SignerDiversityGateCanCapLeaderScoreBeforeSampleThreshold) {
  ReputationConfig config = TestConfig();
  config.min_leader_opportunities = 8;
  config.leader_diversity_soft_min_weight = 80;
  std::vector<TestEvidence> evidence;
  const std::string clique_quorum = BitmapFromVector(
      ConsecutiveModuloSigners(1, 14, 20), 20);
  const std::string all_available = BitmapFromVector(
      ConsecutiveModuloSigners(1, 20, 20), 20);
  for (int view = 1; view <= 3; ++view) {
    evidence.push_back(CertifiedQc(view, 1, clique_quorum, all_available));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 100), config,
      "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].leader_opportunity_count, 3);
  EXPECT_LT(candidate.validators[0].leader_diversity_score, 30);
  EXPECT_EQ(candidate.validators[0].leader_score,
            config.leader_diversity_soft_min_weight);
  EXPECT_EQ(candidate.validators[0].next_weight, 100);
  EXPECT_EQ(candidate.leader_weights[0], 80);
}

TEST(ReputationAlgorithmTest,
     SignerDiversitySoftCapPersistsWithoutBroadRecoveryEvidence) {
  ReputationConfig config = TestConfig();
  config.leader_diversity_soft_min_weight = 80;
  std::vector<TestEvidence> evidence;
  const std::string clique_quorum = BitmapFromVector(
      ConsecutiveModuloSigners(1, 14, 20), 20);
  const std::string all_available = BitmapFromVector(
      ConsecutiveModuloSigners(1, 20, 20), 20);
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, 1, clique_quorum, all_available));
    evidence.push_back(CertifiedQc(view + 10, 2, clique_quorum, all_available));
    evidence.push_back(CertifiedQc(view + 30, 4, clique_quorum, all_available));
    evidence.push_back(CertifiedQc(view + 40, 5, clique_quorum, all_available));
  }

  std::vector<int64_t> current_leader_weights(20, 100);
  for (int id : {1, 2, 3, 4, 5}) {
    current_leader_weights[id - 1] = config.leader_diversity_soft_min_weight;
  }
  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 100), config,
      "old-root", 0, 64, {}, {}, {}, {}, {}, {}, {}, {}, {},
      /*scheduled_leader_counts=*/{}, current_leader_weights);

  EXPECT_EQ(candidate.leader_weights[2], 80);
  EXPECT_EQ(candidate.validators[2].leader_score, 100);
  EXPECT_EQ(candidate.validators[2].leader_diversity_score, 100);
  EXPECT_EQ(candidate.validators[2].next_weight, 100);
  EXPECT_EQ(candidate.validators[2].strong_fault_count, 0);
  EXPECT_EQ(candidate.validators[2].penalty_points, 0);
}

TEST(ReputationAlgorithmTest,
     SignerDiversityGateUsesWeightedJaccardForHighOverlapRotation) {
  ReputationConfig config = TestConfig();
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  std::vector<TestEvidence> evidence;
  const std::string all_available = BitmapFromVector(
      ConsecutiveModuloSigners(1, 20, 20), 20);
  for (int view = 1; view <= 20; ++view) {
    evidence.push_back(CertifiedQc(
        view, 1,
        BitmapFromVector(ConsecutiveModuloSigners(view, 14, 20), 20),
        all_available));
  }
  for (int view = 21; view <= 40; ++view) {
    const int start = 1 + ((view - 21) * 7) % 20;
    evidence.push_back(CertifiedQc(
        view, 2,
        BitmapFromVector(ConsecutiveModuloSigners(start, 14, 20), 20),
        all_available));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), config,
      "old-root", 0, 64);

  EXPECT_LT(candidate.validators[0].leader_diversity_score,
            candidate.validators[1].leader_diversity_score);
  EXPECT_LT(candidate.validators[0].leader_diversity_score, 30);
  EXPECT_GE(candidate.validators[1].leader_diversity_score, 60);
  EXPECT_LT(candidate.validators[0].leader_score,
            candidate.validators[1].leader_score);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
  EXPECT_LT(candidate.leader_weights[0], candidate.leader_weights[1]);
  EXPECT_EQ(candidate.validators[0].strong_fault_count, 0);
  EXPECT_EQ(candidate.validators[0].penalty_points, 0);
}

TEST(ReputationAlgorithmTest, PeerTrustDisabledKeepsAuditNeutral) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = false;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].peertrust_score, 100);
  EXPECT_EQ(candidate.validators[0].reviewer_credibility_score, 100);
  EXPECT_EQ(candidate.validators[0].transaction_context_score, 100);
  EXPECT_EQ(candidate.validators[0].community_context_score, 100);
  EXPECT_EQ(candidate.validators[0].feedback_count, 0);
}

TEST(ReputationAlgorithmTest, PeerTrustAllGoodRotatingFeedbackStaysHigh) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].feedback_count, 8);
  EXPECT_EQ(candidate.validators[0].reviewer_credibility_score, 100);
  EXPECT_EQ(candidate.validators[0].transaction_context_score, 100);
  EXPECT_EQ(candidate.validators[0].community_context_score, 100);
  EXPECT_EQ(candidate.validators[0].peertrust_score, 100);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
}

TEST(ReputationAlgorithmTest,
     PeerTrustQuorumSizedAvailabilityDoesNotCreateDebt) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  config.decay_per_epoch = 0;
  const std::vector<int> quorum_a = {1, 2, 3, 4, 5, 6, 7,
                                     8, 9, 10, 11, 12, 13, 14};
  const std::vector<int> quorum_b = {2, 3, 4, 5, 6, 7, 8,
                                     9, 10, 11, 12, 13, 14, 15};
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 5; ++view) {
    const std::vector<int>& quorum = view % 2 == 0 ? quorum_b : quorum_a;
    const std::string bitmap = BitmapFromVector(quorum, 20);
    evidence.push_back(CertifiedQc(view, view, bitmap, bitmap));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), config,
      "old-root", 0, 64);

  for (int leader = 1; leader <= 5; ++leader) {
    const ValidatorReputation& validator = candidate.validators[leader - 1];
    EXPECT_EQ(validator.peertrust_score, 100);
    EXPECT_EQ(validator.community_context_score, 100);
    EXPECT_EQ(validator.peertrust_leader_debt, 0);
    EXPECT_EQ(validator.next_weight, 30);
  }
}

TEST(ReputationAlgorithmTest, PeerTrustCliqueFeedbackLowersOnlyLeaderRecovery) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }
  for (int view = 9; view <= 16; ++view) {
    evidence.push_back(CertifiedQc(view, 2, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].feedback_count, 8);
  EXPECT_LT(candidate.validators[0].community_context_score,
            candidate.validators[1].community_context_score);
  EXPECT_LT(candidate.validators[0].peertrust_score,
            candidate.validators[1].peertrust_score);
  EXPECT_LT(candidate.validators[0].next_weight,
            candidate.validators[1].next_weight);
  EXPECT_EQ(candidate.validators[0].peertrust_leader_debt, 20);
  EXPECT_EQ(candidate.validators[1].peertrust_leader_debt, 0);
  EXPECT_LT(candidate.leader_weights[0], candidate.leader_weights[1]);
  EXPECT_GE(candidate.leader_weights[0],
            config.leader_diversity_soft_min_weight);
  EXPECT_EQ(candidate.validators[3].strong_fault_count, 0);
  EXPECT_EQ(candidate.validators[3].penalty_points, 0);
  EXPECT_EQ(candidate.validators[3].next_weight, 30);
}


TEST(ReputationAlgorithmTest,
     PeerTrustSharedReviewerCliqueDropsBelowSignerDiversityGate) {
  ReputationConfig signer_diversity_config = TestConfig();
  signer_diversity_config.peertrust_enabled = false;
  ReputationConfig peertrust_config = TestConfig();
  peertrust_config.peertrust_enabled = true;
  const std::string all_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);
  const std::string clique_reviewers =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 20);
  std::vector<TestEvidence> evidence = {
      CertifiedQc(1, 1, clique_reviewers, all_available),
      CertifiedQc(2, 2, clique_reviewers, all_available),
      CertifiedQc(3, 15, all_available, all_available),
      CertifiedQc(4, 16, all_available, all_available),
  };

  const ReputationCandidate signer_diversity_candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), signer_diversity_config,
      "old-root", 0, 64);
  const ReputationCandidate peertrust_candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), peertrust_config,
      "old-root", 0, 64);

  EXPECT_EQ(signer_diversity_candidate.validators[0].next_weight, 30);
  EXPECT_EQ(signer_diversity_candidate.validators[1].next_weight, 30);
  EXPECT_LT(peertrust_candidate.validators[0].community_context_score,
            peertrust_candidate.validators[14].community_context_score);
  EXPECT_LT(peertrust_candidate.validators[1].community_context_score,
            peertrust_candidate.validators[15].community_context_score);
  EXPECT_LT(peertrust_candidate.validators[0].next_weight,
            signer_diversity_candidate.validators[0].next_weight);
  EXPECT_LT(peertrust_candidate.validators[1].next_weight,
            signer_diversity_candidate.validators[1].next_weight);
  EXPECT_EQ(peertrust_candidate.validators[0].peertrust_leader_debt, 20);
  EXPECT_EQ(peertrust_candidate.validators[1].peertrust_leader_debt, 20);
  EXPECT_EQ(peertrust_candidate.leader_weights[0],
            signer_diversity_candidate.leader_weights[0]);
  EXPECT_EQ(peertrust_candidate.leader_weights[1],
            signer_diversity_candidate.leader_weights[1]);
  EXPECT_EQ(peertrust_candidate.validators[0].strong_fault_count, 0);
  EXPECT_EQ(peertrust_candidate.validators[0].penalty_points, 0);
  EXPECT_EQ(peertrust_candidate.validators[14].next_weight, 30);
  EXPECT_EQ(peertrust_candidate.validators[15].next_weight, 30);
}

TEST(ReputationAlgorithmTest,
     PeerTrustSoftReputationDoesNotCollapseHealthyVotingWeight) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  const std::string all_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);
  const std::string clique_reviewers =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 20);
  std::vector<TestEvidence> evidence = {
      CertifiedQc(1, 1, clique_reviewers, all_available),
      CertifiedQc(2, 2, clique_reviewers, all_available),
      CertifiedQc(3, 15, all_available, all_available),
      CertifiedQc(4, 16, all_available, all_available),
  };
  std::vector<int64_t> weights(20, 68);

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, weights, config, "old-root", 0, 64);

  EXPECT_LT(candidate.validators[0].peertrust_score,
            candidate.validators[14].peertrust_score);
  EXPECT_EQ(candidate.validators[0].peertrust_leader_debt, 20);
  EXPECT_GE(candidate.validators[0].next_weight, 67);
  EXPECT_GE(candidate.validators[1].next_weight, 67);
  EXPECT_EQ(candidate.validators[0].strong_fault_count, 0);
  EXPECT_EQ(candidate.validators[0].penalty_points, 0);
}

TEST(ReputationAlgorithmTest,
     PeerTrustLowScoreMovesHighWeightDirectlyToSoftFloor) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  const std::string all_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);
  const std::string clique_reviewers =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 20);
  std::vector<TestEvidence> evidence = {
      CertifiedQc(1, 1, clique_reviewers, all_available),
      CertifiedQc(2, 2, clique_reviewers, all_available),
      CertifiedQc(3, 15, all_available, all_available),
      CertifiedQc(4, 16, all_available, all_available),
  };

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 100), config,
      "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].peertrust_score, 0);
  EXPECT_EQ(candidate.validators[1].peertrust_score, 0);
  EXPECT_EQ(candidate.validators[0].next_weight,
            config.peertrust_soft_min_weight);
  EXPECT_EQ(candidate.validators[1].next_weight,
            config.peertrust_soft_min_weight);
  EXPECT_EQ(candidate.validators[14].next_weight, 100);
}

TEST(ReputationAlgorithmTest,
     PeerTrustSoftFloorDoesNotMaskNonPeerTrustCarryoverDecay) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  std::vector<int64_t> weights(20, 100);
  weights[0] = 67;

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, {}, weights, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].vote_score, 50);
  EXPECT_EQ(candidate.validators[0].peertrust_leader_debt, 0);
  EXPECT_EQ(candidate.validators[0].decay_applied, config.decay_per_epoch);
  EXPECT_LT(candidate.validators[0].next_weight,
            config.peertrust_soft_min_weight);
  EXPECT_EQ(candidate.validators[0].strong_fault_count, 0);
  EXPECT_EQ(candidate.validators[0].penalty_points, 0);
}

TEST(ReputationAlgorithmTest,
     PeerTrustSoftFloorStopsDebtCarryoverDecayNearFloor) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  std::vector<int64_t> weights(20, 100);
  weights[0] = 67;
  std::vector<int> prior_peertrust_debt(20, 0);
  prior_peertrust_debt[0] = 20;

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, {}, weights, config, "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, prior_peertrust_debt);

  EXPECT_EQ(candidate.validators[0].vote_score, 50);
  EXPECT_EQ(candidate.validators[0].peertrust_leader_debt, 20);
  EXPECT_EQ(candidate.validators[0].decay_applied, config.decay_per_epoch);
  EXPECT_GE(candidate.validators[0].next_weight, config.peertrust_soft_min_weight);
  EXPECT_EQ(candidate.validators[0].strong_fault_count, 0);
  EXPECT_EQ(candidate.validators[0].penalty_points, 0);
}


TEST(ReputationAlgorithmTest,
     AvailableSignerBitmapLimitsVoterRecoveryOpportunities) {
  ReputationConfig config = TestConfig();
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3}, 5),
                                   Bitmap({1, 2, 3}, 5)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 5, 1, evidence, {30, 30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_GT(candidate.validators[0].opportunities, 0);
  EXPECT_EQ(candidate.validators[3].opportunities, 0);
  EXPECT_EQ(candidate.validators[4].opportunities, 0);
  EXPECT_EQ(candidate.validators[3].decay_applied, 0);
  EXPECT_EQ(candidate.validators[4].decay_applied, 0);
  EXPECT_EQ(candidate.validators[3].next_weight, 30);
  EXPECT_EQ(candidate.validators[4].next_weight, 30);
}

TEST(ReputationAlgorithmTest,
     BroadAvailableSignerBitmapExcusesUnselectedTimelyVoter) {
  ReputationConfig config = TestConfig();
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 5),
                                   Bitmap({1, 2, 3, 4, 5}, 5)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 5, 1, evidence, {30, 30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[4].opportunities, 0);
  EXPECT_EQ(candidate.validators[4].inclusions, 0);
  EXPECT_EQ(candidate.validators[4].decay_applied, 0);
  EXPECT_EQ(candidate.validators[4].next_weight, 30);
}

TEST(ReputationAlgorithmTest,
     QuorumAvailableBitmapDetectsRepeatedUnavailableSlowVoter) {
  ReputationConfig config = TestConfig();
  config.leader_recovery_enabled = false;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 5),
                                   Bitmap({1, 2, 3, 4}, 5)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 5, 1, evidence, {30, 30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_GT(candidate.validators[4].opportunities, 0);
  EXPECT_EQ(candidate.validators[4].inclusions, 0);
  EXPECT_LT(candidate.validators[4].next_weight, 30);
}

TEST(ReputationAlgorithmTest,
     QuorumAvailableBitmapWorksWithLeaderRecoveryEnabled) {
  ReputationConfig config = TestConfig();
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    std::vector<int> timely_quorum;
    for (int offset = 0; static_cast<int>(timely_quorum.size()) < 14;
         ++offset) {
      const int validator = 2 + ((view + offset - 1) % 19);
      timely_quorum.push_back(validator);
    }
    const std::string bitmap = BitmapFromVector(timely_quorum, 20);
    evidence.push_back(CertifiedQc(view, 4, bitmap, bitmap));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), config, "old-root", 0,
      64);

  EXPECT_GT(candidate.validators[0].opportunities, 0);
  EXPECT_EQ(candidate.validators[0].inclusions, 0);
  EXPECT_LT(candidate.validators[0].next_weight, 30);
  EXPECT_GT(candidate.validators[1].next_weight,
            candidate.validators[0].next_weight);
}

TEST(ReputationAlgorithmTest,
     SparseTimelyAvailabilityGetsOnlyPartialRecovery) {
  ReputationConfig config = TestConfig();
  config.leader_recovery_enabled = false;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 7; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 5),
                                   Bitmap({1, 2, 3, 4}, 5)));
  }
  evidence.push_back(CertifiedQc(8, 1, Bitmap({1, 2, 3, 4, 5}, 5),
                                 Bitmap({1, 2, 3, 4, 5}, 5)));

  const ReputationCandidate candidate = ComputeCandidate(
      1, 5, 1, evidence, {30, 30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_GT(candidate.validators[4].opportunities, 1);
  EXPECT_EQ(candidate.validators[4].inclusions, 1);
  EXPECT_LT(candidate.validators[4].recovery_credit,
            candidate.validators[4].decay_applied);
  EXPECT_LT(candidate.validators[4].next_weight, 30);
}

TEST(ReputationAlgorithmTest,
     LowParticipationCarryoverContinuesDecayWithoutFreshOpportunities) {
  ReputationConfig config = TestConfig();
  config.leader_recovery_enabled = false;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 4, Bitmap({4, 5, 6, 7}, 20),
                                   Bitmap({4, 5, 6, 7}, 20)));
  }
  std::vector<int64_t> weights(20, 100);
  weights[0] = 75;

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, weights, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].opportunities, 0);
  EXPECT_LT(candidate.validators[0].next_weight, weights[0]);
  EXPECT_EQ(candidate.validators[19].next_weight, 100);
}


TEST(ReputationAlgorithmTest,
     PeerTrustCleanCliqueSeparatesLeadersFromReviewers) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  const std::string clean_reviewers =
      Bitmap({6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, 20);
  const std::string all_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 5; ++view) {
    evidence.push_back(
        CertifiedQc(view, view, clean_reviewers, all_available));
  }
  for (int view = 6; view <= 20; ++view) {
    evidence.push_back(CertifiedQc(view, 20, all_available, all_available));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), config,
      "old-root", 0, 64);

  for (int leader = 1; leader <= 5; ++leader) {
    EXPECT_LT(candidate.validators[leader - 1].community_context_score,
              candidate.validators[19].community_context_score);
    EXPECT_LT(candidate.validators[leader - 1].peertrust_score,
              candidate.validators[19].peertrust_score);
    EXPECT_EQ(candidate.validators[leader - 1].next_weight, 30);
  }
  EXPECT_EQ(candidate.validators[19].next_weight, 30);
}

TEST(ReputationAlgorithmTest, PeerTrustLowReviewerCredibilityReducesFeedback) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  const std::string low_weight_reviewers = Bitmap({1, 2, 3}, 5);
  const std::string all_available = Bitmap({1, 2, 3, 4, 5}, 5);
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, 1, low_weight_reviewers,
                                   all_available));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 5, 1, evidence, {5, 5, 5, 30, 30}, config, "old-root", 0, 64);

  EXPECT_LT(candidate.validators[0].reviewer_credibility_score, 100);
  EXPECT_LT(candidate.validators[0].peertrust_score, 100);
}



TEST(ReputationAlgorithmTest,
     PeerTrustBroadAvailableOverlapDoesNotCreateDebt) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  const std::string all_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);
  const std::string overlapping_quorum =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 20);
  std::vector<TestEvidence> evidence;
  for (int leader = 6; leader <= 10; ++leader) {
    for (int round = 0; round < 3; ++round) {
      evidence.push_back(CertifiedQc(leader * 10 + round, leader,
                                     overlapping_quorum, all_available));
    }
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), config,
      "old-root", 0, 64);

  for (int leader = 6; leader <= 10; ++leader) {
    const ValidatorReputation& validator = candidate.validators[leader - 1];
    EXPECT_EQ(validator.community_context_score, 100);
    EXPECT_EQ(validator.peertrust_leader_debt, 0);
    EXPECT_EQ(validator.next_weight, 30);
  }
}

TEST(ReputationAlgorithmTest, PeerTrustDebtPersistsWithoutLeaderFeedback) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  config.bonus_per_epoch = 1;
  const std::vector<TestEvidence> evidence = {
      CertifiedQc(1, 2, Bitmap({1, 2, 3, 4}, 4), Bitmap({1, 2, 3, 4}, 4)),
  };

  const ReputationCandidate no_debt = ComputeCandidate(
      1, 4, 1, evidence, {20, 30, 30, 30}, config, "old-root", 0, 64);
  const ReputationCandidate with_debt = ComputeCandidate(
      1, 4, 1, evidence, {20, 30, 30, 30}, config, "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, {40, 0, 0, 0});

  EXPECT_EQ(with_debt.validators[0].feedback_count, 0);
  EXPECT_EQ(with_debt.validators[0].peertrust_leader_debt, 40);
  EXPECT_LT(with_debt.validators[0].next_weight,
            no_debt.validators[0].next_weight);
  EXPECT_EQ(with_debt.leader_weights[0], no_debt.leader_weights[0]);
}

TEST(ReputationAlgorithmTest, PeerTrustBroadGoodLeadershipRepaysDebt) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, {40, 0, 0, 0});

  EXPECT_EQ(candidate.validators[0].feedback_count, 8);
  EXPECT_EQ(candidate.validators[0].peertrust_score, 100);
  EXPECT_EQ(candidate.validators[0].peertrust_debt_delta,
            -config.peertrust_debt_recovery);
  EXPECT_EQ(candidate.validators[0].peertrust_leader_debt,
            40 - config.peertrust_debt_recovery);
}

TEST(ReputationAlgorithmTest, CandidateDigestIsDeterministic) {
  std::vector<TestEvidence> evidence = {
      CertifiedQc(1, 1, Bitmap({1, 2, 3}, 4), Bitmap({1, 2, 3, 4}, 4)),
      TimeoutEvidence(2, 2),
      CertifiedQc(3, 3, Bitmap({1, 3, 4}, 4), Bitmap({1, 2, 3, 4}, 4)),
  };

  const ReputationCandidate first = ComputeCandidate(
      1, 4, 9, evidence, {20, 30, 30, 30}, TestConfig(), "old-root", 7, 128);
  const ReputationCandidate second = ComputeCandidate(
      4, 4, 9, evidence, {20, 30, 30, 30}, TestConfig(), "old-root", 7, 128);

  EXPECT_EQ(first.metric_root_hex, second.metric_root_hex);
  EXPECT_EQ(first.reputation_root_hex, second.reputation_root_hex);
  EXPECT_EQ(first.next_weight_root_hex, second.next_weight_root_hex);
  EXPECT_EQ(first.candidate_digest_hex, second.candidate_digest_hex);
}

TEST(ReputationAlgorithmTest, CandidateDigestIgnoresAuditRoots) {
  const std::string digest = ReputationCandidateDigest(
      /*total_replicas=*/4, /*window_index=*/1, /*start_view=*/10,
      /*end_view=*/12, /*event_count=*/3, /*old_weight_root_hex=*/"old",
      /*old_weight_version=*/7, /*activation_view=*/64,
      /*metric_root_hex=*/"metric", /*reputation_root_hex=*/"rep-a",
      /*next_weight_root_hex=*/"weights", /*next_weights=*/{10, 20, 30, 40});
  const std::string changed = ReputationCandidateDigest(
      /*total_replicas=*/4, /*window_index=*/1, /*start_view=*/10,
      /*end_view=*/12, /*event_count=*/3, /*old_weight_root_hex=*/"old",
      /*old_weight_version=*/7, /*activation_view=*/64,
      /*metric_root_hex=*/"metric", /*reputation_root_hex=*/"rep-b",
      /*next_weight_root_hex=*/"weights", /*next_weights=*/{10, 20, 30, 40});

  EXPECT_EQ(digest, changed);
}

TEST(ReputationAlgorithmTest, SybilGraphDisabledKeepsAuditNeutral) {
  ReputationConfig config = TestConfig();
  config.sybil_graph_enabled = false;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, view, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config);

  for (const ValidatorReputation& validator : candidate.validators) {
    EXPECT_EQ(validator.sybil_rank_score, 100);
    EXPECT_EQ(validator.sybil_cut_score, 100);
    EXPECT_EQ(validator.sybil_graph_score, 100);
    EXPECT_EQ(validator.sybil_graph_debt, 0);
    EXPECT_EQ(validator.sybil_graph_debt_delta, 0);
  }
}

TEST(ReputationAlgorithmTest, SybilGraphAllGoodRotatingEvidenceStaysHigh) {
  ReputationConfig config = TestConfig();
  config.sybil_graph_enabled = true;
  config.sybil_graph_min_edges = 1;
  const int total_replicas = 20;
  const std::vector<int64_t> weights(total_replicas, 30);
  std::vector<int> all_signers;
  for (int id = 1; id <= total_replicas; ++id) {
    all_signers.push_back(id);
  }
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 20; ++view) {
    evidence.push_back(CertifiedQc(
        view, ((view - 1) % total_replicas) + 1,
        BitmapFromVector(all_signers, total_replicas),
        BitmapFromVector(all_signers, total_replicas)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, total_replicas, 1, evidence, weights, config);

  for (const ValidatorReputation& validator : candidate.validators) {
    EXPECT_GE(validator.sybil_rank_score, 90);
    EXPECT_GE(validator.sybil_cut_score, 90);
    EXPECT_GE(validator.sybil_graph_score, 90);
    EXPECT_EQ(validator.sybil_graph_debt, 0);
    EXPECT_EQ(validator.strong_fault_count, 0);
  }
}

TEST(ReputationAlgorithmTest,
     SybilGraphDenseClusterWithFewAttackEdgesDropsCluster) {
  ReputationConfig config = TestConfig();
  config.sybil_graph_enabled = true;
  config.sybil_graph_min_edges = 1;
  config.sybil_graph_debt_increment = 40;
  config.sybil_graph_debt_trigger_score = 75;
  const int total_replicas = 20;
  const std::vector<int64_t> weights(total_replicas, 30);
  const std::vector<int> sybil_reviewers = {1, 2, 3, 4, 5, 6, 7, 8,
                                            9, 10, 11, 12, 13, 14};
  std::vector<int> all_signers;
  for (int id = 1; id <= total_replicas; ++id) {
    all_signers.push_back(id);
  }
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 16; ++view) {
    const int leader = ((view - 1) % 8) + 1;
    evidence.push_back(CertifiedQc(
        view, leader, BitmapFromVector(sybil_reviewers, total_replicas),
        BitmapFromVector(sybil_reviewers, total_replicas)));
  }
  for (int view = 17; view <= 32; ++view) {
    const int leader = 15 + ((view - 17) % 6);
    evidence.push_back(CertifiedQc(
        view, leader, BitmapFromVector(all_signers, total_replicas),
        BitmapFromVector(all_signers, total_replicas)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, total_replicas, 1, evidence, weights, config);

  int sybil_score_sum = 0;
  int honest_score_sum = 0;
  int sybil_weight_sum = 0;
  int honest_weight_sum = 0;
  for (int id = 1; id <= total_replicas; ++id) {
    const ValidatorReputation& validator = candidate.validators[id - 1];
    if (id <= 8) {
      sybil_score_sum += validator.sybil_graph_score;
      sybil_weight_sum += validator.next_weight;
      EXPECT_GT(validator.sybil_graph_debt, 0);
      EXPECT_LT(validator.next_weight, validator.current_weight)
          << "id=" << id << " debt=" << validator.sybil_graph_debt
          << " score=" << validator.sybil_graph_score
          << " current=" << validator.current_weight
          << " next=" << validator.next_weight;
    } else if (id >= 15) {
      honest_score_sum += validator.sybil_graph_score;
      honest_weight_sum += validator.next_weight;
      EXPECT_EQ(validator.strong_fault_count, 0);
      EXPECT_EQ(validator.penalty_points, 0);
    }
  }
  EXPECT_LT(sybil_score_sum / 8, honest_score_sum / 6);
  EXPECT_LT(sybil_weight_sum / 8, honest_weight_sum / 6);
}

TEST(ReputationAlgorithmTest,
     SybilGraphBroadHonestEndorsementsWeakenDiscount) {
  ReputationConfig config = TestConfig();
  config.sybil_graph_enabled = true;
  config.sybil_graph_min_edges = 1;
  const int total_replicas = 20;
  const std::vector<int64_t> weights(total_replicas, 30);
  std::vector<int> all_signers;
  for (int id = 1; id <= total_replicas; ++id) {
    all_signers.push_back(id);
  }
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 16; ++view) {
    const int leader = ((view - 1) % 8) + 1;
    evidence.push_back(CertifiedQc(
        view, leader, BitmapFromVector(all_signers, total_replicas),
        BitmapFromVector(all_signers, total_replicas)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, total_replicas, 1, evidence, weights, config);

  for (int id = 1; id <= 8; ++id) {
    const ValidatorReputation& validator = candidate.validators[id - 1];
    EXPECT_GE(validator.sybil_graph_score, 90);
    EXPECT_EQ(validator.sybil_graph_debt, 0);
  }
}

TEST(ReputationAlgorithmTest, SybilGraphDebtPersistsAcrossQuietWindow) {
  ReputationConfig config = TestConfig();
  config.sybil_graph_enabled = true;
  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, /*evidence=*/{}, {30, 30, 30, 30}, config, "", 0, 0,
      {}, {}, {}, {}, {}, {}, {}, /*prior_peertrust_leader_debt=*/{},
      /*prior_sybil_graph_debt=*/{30, 0, 0, 0});

  EXPECT_EQ(candidate.validators[0].sybil_graph_debt, 30);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(ReputationAlgorithmTest, SybilGraphDebtGatesLaterRecovery) {
  ReputationConfig config = TestConfig();
  config.sybil_graph_enabled = true;
  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, {CertifiedQc(1, 1, Bitmap({1, 2, 3, 4}, 4),
                            Bitmap({1, 2, 3, 4}, 4))},
      {30, 30, 30, 30}, config, "", 0, 0, {}, {}, {}, {}, {}, {}, {},
      /*prior_peertrust_leader_debt=*/{}, /*prior_sybil_graph_debt=*/{80, 0, 0, 0});

  EXPECT_EQ(candidate.validators[0].sybil_graph_debt, 75);
  EXPECT_EQ(candidate.validators[0].next_weight, 25);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(ReputationAlgorithmTest, SybilGraphDebtDoesNotRecoverFromVoterOnlyEvidence) {
  ReputationConfig config = TestConfig();
  config.sybil_graph_enabled = true;
  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, {CertifiedQc(1, 2, Bitmap({1, 2, 3, 4}, 4),
                            Bitmap({1, 2, 3, 4}, 4))},
      {30, 30, 30, 30}, config, "", 0, 0, {}, {}, {}, {}, {}, {}, {},
      /*prior_peertrust_leader_debt=*/{}, /*prior_sybil_graph_debt=*/{80, 0, 0, 0});

  EXPECT_EQ(candidate.validators[0].sybil_graph_debt, 80);
  EXPECT_EQ(candidate.validators[0].next_weight, 25);
  EXPECT_EQ(candidate.validators[1].sybil_graph_debt, 0);
}

TEST(ReputationAlgorithmTest, DetectsVerifiedDoubleProposal) {
  const std::vector<StrongFaultRecord> faults = DetectDoubleProposalFaults({
      ProposalArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-a"),
      ProposalArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-b"),
  });

  ASSERT_EQ(faults.size(), 1);
  EXPECT_EQ(faults[0].type, StrongFaultType::kDoubleProposal);
  EXPECT_EQ(faults[0].validator_id, 2);
  EXPECT_EQ(faults[0].view_or_round, 9);
  EXPECT_EQ(faults[0].slot_or_height, 0);
  EXPECT_EQ(faults[0].first_artifact_digest, "hash-a");
  EXPECT_EQ(faults[0].second_artifact_digest, "hash-b");
}

TEST(ReputationAlgorithmTest, IgnoresDuplicateOrUnverifiedProposalArtifacts) {
  EXPECT_TRUE(DetectDoubleProposalFaults({
      ProposalArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-a"),
      ProposalArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-a"),
  }).empty());

  EXPECT_TRUE(DetectDoubleProposalFaults({
      ProposalArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-a"),
      ProposalArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-b",
                       /*signature_verified=*/false),
  }).empty());

  EXPECT_TRUE(DetectDoubleProposalFaults({
      ProposalArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-a"),
      ProposalArtifact(/*leader=*/2, /*view=*/10, /*slot=*/0, "hash-b"),
  }).empty());
}

TEST(ReputationAlgorithmTest, DoubleProposalPenaltyOverridesSoftReputation) {
  ReputationConfig config = TestConfig();
  config.strong_fault_enabled = true;
  config.double_proposal_detection_enabled = true;
  config.strong_fault_target_weight = 1;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 7, 64,
      {ProposalArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-a"),
       ProposalArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-b")});

  EXPECT_EQ(candidate.validators[1].strong_fault_count, 1);
  EXPECT_GT(candidate.validators[1].penalty_points, 0);
  EXPECT_EQ(candidate.validators[1].recovery_credit, 0);
  EXPECT_EQ(candidate.validators[1].bonus_credit, 0);
  EXPECT_EQ(candidate.validators[1].reputation_score, 0);
  EXPECT_EQ(candidate.validators[1].next_weight, 1);
  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({30, 1, 30, 30}));
  EXPECT_FALSE(candidate.strong_fault_root_hex.empty());
  EXPECT_FALSE(candidate.penalty_root_hex.empty());
}

TEST(ReputationAlgorithmTest, DetectsVerifiedDoubleVote) {
  const std::vector<StrongFaultRecord> faults = DetectDoubleVoteFaults({
      VoteArtifact(/*signer=*/3, /*view=*/9, /*slot=*/0, "hash-a"),
      VoteArtifact(/*signer=*/3, /*view=*/9, /*slot=*/0, "hash-b"),
  });

  ASSERT_EQ(faults.size(), 1);
  EXPECT_EQ(faults[0].type, StrongFaultType::kDoubleVote);
  EXPECT_EQ(faults[0].validator_id, 3);
  EXPECT_EQ(faults[0].view_or_round, 9);
  EXPECT_EQ(faults[0].slot_or_height, 0);
  EXPECT_EQ(faults[0].first_artifact_digest, "hash-a");
  EXPECT_EQ(faults[0].second_artifact_digest, "hash-b");
}

TEST(ReputationAlgorithmTest, IgnoresDuplicateOrUnverifiedVoteArtifacts) {
  EXPECT_TRUE(DetectDoubleVoteFaults({
      VoteArtifact(/*signer=*/3, /*view=*/9, /*slot=*/0, "hash-a"),
      VoteArtifact(/*signer=*/3, /*view=*/9, /*slot=*/0, "hash-a"),
  }).empty());

  EXPECT_TRUE(DetectDoubleVoteFaults({
      VoteArtifact(/*signer=*/3, /*view=*/9, /*slot=*/0, "hash-a"),
      VoteArtifact(/*signer=*/3, /*view=*/9, /*slot=*/0, "hash-b",
                   /*signature_verified=*/false),
  }).empty());

  EXPECT_TRUE(DetectDoubleVoteFaults({
      VoteArtifact(/*signer=*/3, /*view=*/9, /*slot=*/0, "hash-a"),
      VoteArtifact(/*signer=*/4, /*view=*/9, /*slot=*/0, "hash-b"),
  }).empty());
}

TEST(ReputationAlgorithmTest, DetectsInvalidQcProposal) {
  const std::vector<StrongFaultRecord> faults =
      DetectInvalidQcProposalFaults({
          InvalidQcArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-a"),
      });

  ASSERT_EQ(faults.size(), 1);
  EXPECT_EQ(faults[0].type, StrongFaultType::kInvalidQcProposal);
  EXPECT_EQ(faults[0].validator_id, 2);
  EXPECT_EQ(faults[0].view_or_round, 9);
  EXPECT_EQ(faults[0].slot_or_height, 0);
  EXPECT_EQ(faults[0].first_artifact_digest, "hash-a");
}

TEST(ReputationAlgorithmTest, IgnoresUnverifiedOrValidQcProposalArtifacts) {
  EXPECT_TRUE(DetectInvalidQcProposalFaults({
      InvalidQcArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-a",
                        /*proposal_signature_verified=*/false),
  }).empty());

  EXPECT_TRUE(DetectInvalidQcProposalFaults({
      InvalidQcArtifact(/*leader=*/2, /*view=*/9, /*slot=*/0, "hash-a",
                        /*proposal_signature_verified=*/true,
                        /*qc_verified=*/true),
  }).empty());
}

TEST(ReputationAlgorithmTest, DoubleVotePenaltyOverridesSoftReputation) {
  ReputationConfig config = TestConfig();
  config.strong_fault_enabled = true;
  config.double_vote_detection_enabled = true;
  config.strong_fault_target_weight = 1;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 7, 64,
      /*signed_proposal_evidence=*/{},
      {VoteArtifact(/*signer=*/3, /*view=*/9, /*slot=*/0, "hash-a"),
       VoteArtifact(/*signer=*/3, /*view=*/9, /*slot=*/0, "hash-b")});

  EXPECT_EQ(candidate.validators[2].strong_fault_count, 1);
  EXPECT_GT(candidate.validators[2].penalty_points, 0);
  EXPECT_EQ(candidate.validators[2].next_weight, 1);
  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({30, 30, 1, 30}));
}

TEST(ReputationAlgorithmTest, InvalidQcPenaltyOverridesSoftReputation) {
  ReputationConfig config = TestConfig();
  config.strong_fault_enabled = true;
  config.invalid_qc_proposal_detection_enabled = true;
  config.strong_fault_target_weight = 1;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 7, 64,
      /*signed_proposal_evidence=*/{}, /*signed_vote_evidence=*/{},
      {InvalidQcArtifact(/*leader=*/4, /*view=*/9, /*slot=*/0, "hash-a")});

  EXPECT_EQ(candidate.validators[3].strong_fault_count, 1);
  EXPECT_GT(candidate.validators[3].penalty_points, 0);
  EXPECT_EQ(candidate.validators[3].next_weight, 1);
  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({30, 30, 30, 1}));
}

TEST(ReputationAlgorithmTest, DetectsWeightUpdateVoteEquivocation) {
  const std::vector<StrongFaultRecord> faults =
      DetectWeightUpdateVoteEquivocationFaults({
          WeightUpdateVoteArtifact(/*validator=*/2, "candidate-a"),
          WeightUpdateVoteArtifact(/*validator=*/2, "candidate-b"),
      });

  ASSERT_EQ(faults.size(), 1);
  EXPECT_EQ(faults[0].type,
            StrongFaultType::kWeightUpdateVoteEquivocation);
  EXPECT_EQ(faults[0].validator_id, 2);
  EXPECT_EQ(faults[0].view_or_round, 64);
  EXPECT_EQ(faults[0].first_artifact_digest, "candidate-a");
  EXPECT_EQ(faults[0].second_artifact_digest, "candidate-b");
}

TEST(ReputationAlgorithmTest,
     IgnoresDuplicateOrUnverifiedWeightUpdateVoteArtifacts) {
  EXPECT_TRUE(DetectWeightUpdateVoteEquivocationFaults({
      WeightUpdateVoteArtifact(/*validator=*/2, "candidate-a"),
      WeightUpdateVoteArtifact(/*validator=*/2, "candidate-a"),
  }).empty());

  EXPECT_TRUE(DetectWeightUpdateVoteEquivocationFaults({
      WeightUpdateVoteArtifact(/*validator=*/2, "candidate-a"),
      WeightUpdateVoteArtifact(/*validator=*/2, "candidate-b",
                               /*signature_verified=*/false),
  }).empty());

  EXPECT_TRUE(DetectWeightUpdateVoteEquivocationFaults({
      WeightUpdateVoteArtifact(/*validator=*/2, "candidate-a"),
      WeightUpdateVoteArtifact(/*validator=*/3, "candidate-b"),
  }).empty());
}

TEST(ReputationAlgorithmTest, ConflictingQcsPenalizeOnlySignerIntersection) {
  const std::vector<StrongFaultRecord> faults = DetectConflictingQcFaults(
      {
          QcArtifact(/*view=*/21, /*slot=*/0, "qc-a", Bitmap({1, 2, 3}, 4)),
          QcArtifact(/*view=*/21, /*slot=*/0, "qc-b", Bitmap({2, 3, 4}, 4)),
      },
      /*total_replicas=*/4);

  ASSERT_EQ(faults.size(), 2);
  EXPECT_EQ(faults[0].type, StrongFaultType::kConflictingQc);
  EXPECT_EQ(faults[0].validator_id, 2);
  EXPECT_EQ(faults[1].type, StrongFaultType::kConflictingQc);
  EXPECT_EQ(faults[1].validator_id, 3);
}

TEST(ReputationAlgorithmTest, ConflictingQcIgnoresDuplicatesAndUnverifiedQcs) {
  EXPECT_TRUE(DetectConflictingQcFaults(
      {
          QcArtifact(/*view=*/21, /*slot=*/0, "qc-a", Bitmap({1, 2, 3}, 4)),
          QcArtifact(/*view=*/21, /*slot=*/0, "qc-a", Bitmap({2, 3, 4}, 4)),
      },
      /*total_replicas=*/4).empty());

  EXPECT_TRUE(DetectConflictingQcFaults(
      {
          QcArtifact(/*view=*/21, /*slot=*/0, "qc-a", Bitmap({1, 2, 3}, 4)),
          QcArtifact(/*view=*/21, /*slot=*/0, "qc-b", Bitmap({2, 3, 4}, 4),
                     /*qc_verified=*/false),
      },
      /*total_replicas=*/4).empty());
}

TEST(ReputationAlgorithmTest, V2StrongFaultCandidatePenaltiesAreDeterministic) {
  ReputationConfig config = TestConfig();
  config.strong_fault_enabled = true;
  config.weight_update_vote_equivocation_detection_enabled = true;
  config.conflicting_qc_detection_enabled = true;
  config.strong_fault_target_weight = 1;
  std::vector<TestEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 7, 64,
      /*signed_proposal_evidence=*/{}, /*signed_vote_evidence=*/{},
      /*invalid_qc_proposal_evidence=*/{},
      {
          WeightUpdateVoteArtifact(/*validator=*/1, "candidate-a"),
          WeightUpdateVoteArtifact(/*validator=*/1, "candidate-b"),
      },
      {}, {},
      {
          QcArtifact(/*view=*/21, /*slot=*/0, "qc-a", Bitmap({2, 3, 4}, 4)),
          QcArtifact(/*view=*/21, /*slot=*/0, "qc-b", Bitmap({2, 3, 4}, 4)),
      });

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({1, 1, 1, 1}));
  EXPECT_EQ(candidate.validators[0].strong_fault_count, 1);
  EXPECT_EQ(candidate.validators[1].strong_fault_count, 1);
  EXPECT_EQ(candidate.validators[2].strong_fault_count, 1);
  EXPECT_EQ(candidate.validators[3].strong_fault_count, 1);
  EXPECT_FALSE(candidate.strong_fault_root_hex.empty());
  EXPECT_FALSE(candidate.penalty_root_hex.empty());
  EXPECT_FALSE(candidate.candidate_digest_hex.empty());
}


TEST(ReputationAlgorithmTest, CandidateDigestChangesWhenPenaltyRootChanges) {
  const std::string no_penalty = ReputationCandidateDigest(
      /*total_replicas=*/4, /*window_index=*/1, /*start_view=*/10,
      /*end_view=*/12, /*event_count=*/3, /*old_weight_root_hex=*/"old",
      /*old_weight_version=*/7, /*activation_view=*/64,
      /*metric_root_hex=*/"metric", /*reputation_root_hex=*/"rep",
      /*next_weight_root_hex=*/"weights", /*next_weights=*/{10, 20, 30, 40},
      /*strong_fault_root_hex=*/"", /*penalty_root_hex=*/"");
  const std::string with_penalty = ReputationCandidateDigest(
      /*total_replicas=*/4, /*window_index=*/1, /*start_view=*/10,
      /*end_view=*/12, /*event_count=*/3, /*old_weight_root_hex=*/"old",
      /*old_weight_version=*/7, /*activation_view=*/64,
      /*metric_root_hex=*/"metric", /*reputation_root_hex=*/"rep",
      /*next_weight_root_hex=*/"weights", /*next_weights=*/{10, 20, 30, 40},
      /*strong_fault_root_hex=*/"fault-root",
      /*penalty_root_hex=*/"penalty-root");

  EXPECT_NE(no_penalty, with_penalty);
}

}  // namespace
}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
