#include "platform/consensus/reputation/reputation_algorithm.h"

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

MetricEvidence CertifiedQc(int view, int leader, std::string signer_bitmap,
                           std::string available_signer_bitmap = "") {
  MetricEvidence event;
  event.artifact_family = ArtifactFamily::kQc;
  event.view_or_round = view;
  event.leader_id = leader;
  event.collector_id = leader;
  event.artifact_digest = "qc-" + std::to_string(view);
  event.signer_bitmap = std::move(signer_bitmap);
  event.available_signer_bitmap = std::move(available_signer_bitmap);
  event.outcome_class = OutcomeClass::kCertified;
  return event;
}

MetricEvidence TimeoutEvidence(int view, int leader) {
  MetricEvidence event;
  event.artifact_family = ArtifactFamily::kTimeout;
  event.view_or_round = view;
  event.leader_id = leader;
  event.outcome_class = OutcomeClass::kTimeoutOrViewChange;
  return event;
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

SignedTimeoutVoteEvidence TimeoutVoteArtifact(
    int signer, int view, std::string high_qc_digest,
    bool signature_verified = true) {
  SignedTimeoutVoteEvidence artifact;
  artifact.protocol_id = "td_hotstuff";
  artifact.signer_id = signer;
  artifact.view_or_round = view;
  artifact.high_qc_digest = std::move(high_qc_digest);
  artifact.signature_verified = signature_verified;
  artifact.weight_version = 7;
  artifact.active_weight_root = "old-root";
  return artifact;
}

InvalidTcProposalEvidence InvalidTcArtifact(
    int leader, int view, int slot, std::string proposal_hash,
    bool proposal_signature_verified = true,
    bool timeout_cert_verified = false) {
  InvalidTcProposalEvidence artifact;
  artifact.protocol_id = "td_hotstuff";
  artifact.leader_id = leader;
  artifact.view_or_round = view;
  artifact.slot_or_height = slot;
  artifact.proposal_hash = std::move(proposal_hash);
  artifact.proposal_signature_verified = proposal_signature_verified;
  artifact.timeout_cert_verified = timeout_cert_verified;
  artifact.invalid_reason = "bad_tc";
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

TEST(ReputationAlgorithmTest, DecodesSignerBitmap) {
  EXPECT_EQ(DecodeSignerBitmap(std::string(1, static_cast<char>(0x09)), 5),
            std::vector<int>({1, 4}));
}

TEST(ReputationAlgorithmTest, AllGoodWindowKeepsWeightsStable) {
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 16; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({30, 30, 30, 30}));
  for (const ValidatorReputation& validator : candidate.validators) {
    EXPECT_EQ(validator.bonus_credit, 0);
  }
}

TEST(ReputationAlgorithmTest, BonusCanHelpBelowMeanHonestValidatorCatchUp) {
  ReputationConfig config = TestConfig();
  config.bonus_per_epoch = 1;
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 16; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 4, 1, evidence, {20, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].bonus_credit, 1);
  EXPECT_EQ(candidate.validators[0].next_weight, 21);
  EXPECT_EQ(candidate.validators[1].bonus_credit, 0);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(ReputationAlgorithmTest, SlowVoterLosesRecoveryWithoutDirectSlash) {
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, TestConfig(), "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[3].inclusions, 0);
  EXPECT_LT(candidate.validators[3].vote_score, 30);
  EXPECT_LT(candidate.validators[3].next_weight, 30);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
}

TEST(ReputationAlgorithmTest, SilentLeaderLosesLeaderRecovery) {
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }
  for (int view = 5; view <= 8; ++view) {
    evidence.push_back(TimeoutEvidence(view, 2));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, TestConfig(), "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].leader_certified_count, 4);
  EXPECT_EQ(candidate.validators[0].leader_opportunity_count, 4);
  EXPECT_EQ(candidate.validators[1].leader_certified_count, 0);
  EXPECT_EQ(candidate.validators[1].leader_opportunity_count, 4);
  EXPECT_LT(candidate.validators[1].next_weight, candidate.validators[0].next_weight);
}

TEST(ReputationAlgorithmTest, NarrowSignerTargetOnlyReducesLeaderRecovery) {
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3}, 4),
                                   Bitmap({1, 2, 3}, 4)));
  }
  for (int view = 9; view <= 16; ++view) {
    const std::string varied_signers =
        view % 2 == 0 ? Bitmap({1, 2, 4}, 4) : Bitmap({2, 3, 4}, 4);
    evidence.push_back(CertifiedQc(view, 2, varied_signers,
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, TestConfig(), "old-root", 0, 64);

  EXPECT_LT(candidate.validators[0].leader_diversity_score,
            candidate.validators[1].leader_diversity_score);
  EXPECT_LT(candidate.validators[0].next_weight,
            candidate.validators[1].next_weight);
  EXPECT_EQ(candidate.validators[3].next_weight, 30);
}

TEST(ReputationAlgorithmTest, PeerTrustDisabledKeepsAuditNeutral) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = false;
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].feedback_count, 8);
  EXPECT_EQ(candidate.validators[0].reviewer_credibility_score, 100);
  EXPECT_EQ(candidate.validators[0].transaction_context_score, 100);
  EXPECT_EQ(candidate.validators[0].community_context_score, 100);
  EXPECT_EQ(candidate.validators[0].peertrust_score, 100);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
}

TEST(ReputationAlgorithmTest, PeerTrustCliqueFeedbackLowersOnlyLeaderRecovery) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }
  for (int view = 9; view <= 16; ++view) {
    evidence.push_back(CertifiedQc(view, 2, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_EQ(candidate.validators[0].feedback_count, 8);
  EXPECT_LT(candidate.validators[0].community_context_score,
            candidate.validators[1].community_context_score);
  EXPECT_LT(candidate.validators[0].peertrust_score,
            candidate.validators[1].peertrust_score);
  EXPECT_LT(candidate.validators[0].next_weight,
            candidate.validators[1].next_weight);
  EXPECT_EQ(candidate.validators[3].strong_fault_count, 0);
  EXPECT_EQ(candidate.validators[3].penalty_points, 0);
  EXPECT_EQ(candidate.validators[3].next_weight, 30);
}


TEST(ReputationAlgorithmTest,
     PeerTrustSharedReviewerCliqueDropsBelowLegacyDiversityGate) {
  ReputationConfig legacy_config = TestConfig();
  legacy_config.peertrust_enabled = false;
  ReputationConfig peertrust_config = TestConfig();
  peertrust_config.peertrust_enabled = true;
  const std::string all_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);
  const std::string clique_reviewers =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 20);
  std::vector<MetricEvidence> evidence = {
      CertifiedQc(1, 1, clique_reviewers, all_available),
      CertifiedQc(2, 2, clique_reviewers, all_available),
      CertifiedQc(3, 15, all_available, all_available),
      CertifiedQc(4, 16, all_available, all_available),
  };

  const ReputationCandidate legacy_candidate = ComputeReputationCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), legacy_config,
      "old-root", 0, 64);
  const ReputationCandidate peertrust_candidate = ComputeReputationCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), peertrust_config,
      "old-root", 0, 64);

  EXPECT_EQ(legacy_candidate.validators[0].next_weight, 30);
  EXPECT_EQ(legacy_candidate.validators[1].next_weight, 30);
  EXPECT_LT(peertrust_candidate.validators[0].community_context_score,
            peertrust_candidate.validators[14].community_context_score);
  EXPECT_LT(peertrust_candidate.validators[1].community_context_score,
            peertrust_candidate.validators[15].community_context_score);
  EXPECT_LT(peertrust_candidate.validators[0].next_weight,
            legacy_candidate.validators[0].next_weight);
  EXPECT_LT(peertrust_candidate.validators[1].next_weight,
            legacy_candidate.validators[1].next_weight);
  EXPECT_EQ(peertrust_candidate.validators[0].strong_fault_count, 0);
  EXPECT_EQ(peertrust_candidate.validators[0].penalty_points, 0);
  EXPECT_EQ(peertrust_candidate.validators[14].next_weight, 30);
  EXPECT_EQ(peertrust_candidate.validators[15].next_weight, 30);
}


TEST(ReputationAlgorithmTest,
     AvailableSignerBitmapLimitsVoterRecoveryOpportunities) {
  ReputationConfig config = TestConfig();
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3}, 5),
                                   Bitmap({1, 2, 3}, 5)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
     BroadAvailableSignerBitmapStillDetectsRepeatedSlowVoter) {
  ReputationConfig config = TestConfig();
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 5),
                                   Bitmap({1, 2, 3, 4, 5}, 5)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 5, 1, evidence, {30, 30, 30, 30, 30}, config, "old-root", 0, 64);

  EXPECT_GT(candidate.validators[4].opportunities, 0);
  EXPECT_EQ(candidate.validators[4].inclusions, 0);
  EXPECT_LT(candidate.validators[4].next_weight, 30);
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
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 5; ++view) {
    evidence.push_back(
        CertifiedQc(view, view, clean_reviewers, clean_reviewers));
  }
  evidence.push_back(CertifiedQc(6, 20, all_available, all_available));

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 20, 1, evidence, std::vector<int64_t>(20, 30), config,
      "old-root", 0, 64);

  for (int leader = 1; leader <= 5; ++leader) {
    EXPECT_LT(candidate.validators[leader - 1].community_context_score,
              candidate.validators[19].community_context_score);
    EXPECT_LT(candidate.validators[leader - 1].next_weight,
              candidate.validators[19].next_weight);
  }
  EXPECT_EQ(candidate.validators[19].next_weight, 30);
}

TEST(ReputationAlgorithmTest, PeerTrustLowReviewerCredibilityReducesFeedback) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  const std::string low_weight_reviewers = Bitmap({1, 2, 3, 4}, 4);
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, 1, low_weight_reviewers,
                                   low_weight_reviewers));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 4, 1, evidence, {5, 5, 30, 30}, config, "old-root", 0, 64);

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
  std::vector<MetricEvidence> evidence;
  for (int leader = 6; leader <= 10; ++leader) {
    for (int round = 0; round < 3; ++round) {
      evidence.push_back(CertifiedQc(leader * 10 + round, leader,
                                     overlapping_quorum, all_available));
    }
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  const std::vector<MetricEvidence> evidence = {
      CertifiedQc(1, 2, Bitmap({1, 2, 3, 4}, 4), Bitmap({1, 2, 3, 4}, 4)),
  };

  const ReputationCandidate no_debt = ComputeReputationCandidate(
      1, 4, 1, evidence, {20, 30, 30, 30}, config, "old-root", 0, 64);
  const ReputationCandidate with_debt = ComputeReputationCandidate(
      1, 4, 1, evidence, {20, 30, 30, 30}, config, "old-root", 0, 64,
      {}, {}, {}, {}, {}, {}, {}, {40, 0, 0, 0});

  EXPECT_EQ(with_debt.validators[0].feedback_count, 0);
  EXPECT_EQ(with_debt.validators[0].peertrust_leader_debt, 40);
  EXPECT_LT(with_debt.validators[0].next_weight,
            no_debt.validators[0].next_weight);
}

TEST(ReputationAlgorithmTest, PeerTrustBroadGoodLeadershipRepaysDebt) {
  ReputationConfig config = TestConfig();
  config.peertrust_enabled = true;
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, 1, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  std::vector<MetricEvidence> evidence = {
      CertifiedQc(1, 1, Bitmap({1, 2, 3}, 4), Bitmap({1, 2, 3, 4}, 4)),
      TimeoutEvidence(2, 2),
      CertifiedQc(3, 3, Bitmap({1, 3, 4}, 4), Bitmap({1, 2, 3, 4}, 4)),
  };

  const ReputationCandidate first = ComputeReputationCandidate(
      1, 4, 9, evidence, {20, 30, 30, 30}, TestConfig(), "old-root", 7, 128);
  const ReputationCandidate second = ComputeReputationCandidate(
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
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 4; ++view) {
    evidence.push_back(CertifiedQc(view, view, Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 20; ++view) {
    evidence.push_back(CertifiedQc(
        view, ((view - 1) % total_replicas) + 1,
        BitmapFromVector(all_signers, total_replicas),
        BitmapFromVector(all_signers, total_replicas)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  std::vector<MetricEvidence> evidence;
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

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 16; ++view) {
    const int leader = ((view - 1) % 8) + 1;
    evidence.push_back(CertifiedQc(
        view, leader, BitmapFromVector(all_signers, total_replicas),
        BitmapFromVector(all_signers, total_replicas)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
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

TEST(ReputationAlgorithmTest, DetectsTimeoutVoteEquivocation) {
  const std::vector<StrongFaultRecord> faults =
      DetectTimeoutVoteEquivocationFaults({
          TimeoutVoteArtifact(/*signer=*/3, /*view=*/12, "high-qc-a"),
          TimeoutVoteArtifact(/*signer=*/3, /*view=*/12, "high-qc-b"),
      });

  ASSERT_EQ(faults.size(), 1);
  EXPECT_EQ(faults[0].type, StrongFaultType::kTimeoutVoteEquivocation);
  EXPECT_EQ(faults[0].validator_id, 3);
  EXPECT_EQ(faults[0].view_or_round, 12);
  EXPECT_EQ(faults[0].first_artifact_digest, "high-qc-a");
  EXPECT_EQ(faults[0].second_artifact_digest, "high-qc-b");
}

TEST(ReputationAlgorithmTest, IgnoresDuplicateOrUnverifiedTimeoutVotes) {
  EXPECT_TRUE(DetectTimeoutVoteEquivocationFaults({
      TimeoutVoteArtifact(/*signer=*/3, /*view=*/12, "high-qc-a"),
      TimeoutVoteArtifact(/*signer=*/3, /*view=*/12, "high-qc-a"),
  }).empty());

  EXPECT_TRUE(DetectTimeoutVoteEquivocationFaults({
      TimeoutVoteArtifact(/*signer=*/3, /*view=*/12, "high-qc-a"),
      TimeoutVoteArtifact(/*signer=*/3, /*view=*/12, "high-qc-b",
                          /*signature_verified=*/false),
  }).empty());

  EXPECT_TRUE(DetectTimeoutVoteEquivocationFaults({
      TimeoutVoteArtifact(/*signer=*/3, /*view=*/12, "high-qc-a"),
      TimeoutVoteArtifact(/*signer=*/3, /*view=*/13, "high-qc-b"),
  }).empty());
}

TEST(ReputationAlgorithmTest, DetectsInvalidTcProposal) {
  const std::vector<StrongFaultRecord> faults =
      DetectInvalidTcProposalFaults({
          InvalidTcArtifact(/*leader=*/4, /*view=*/15, /*slot=*/0, "hash-a"),
      });

  ASSERT_EQ(faults.size(), 1);
  EXPECT_EQ(faults[0].type, StrongFaultType::kInvalidTcProposal);
  EXPECT_EQ(faults[0].validator_id, 4);
  EXPECT_EQ(faults[0].view_or_round, 15);
  EXPECT_EQ(faults[0].first_artifact_digest, "hash-a");
}

TEST(ReputationAlgorithmTest, IgnoresUnverifiedOrValidTcProposalArtifacts) {
  EXPECT_TRUE(DetectInvalidTcProposalFaults({
      InvalidTcArtifact(/*leader=*/4, /*view=*/15, /*slot=*/0, "hash-a",
                        /*proposal_signature_verified=*/false),
  }).empty());

  EXPECT_TRUE(DetectInvalidTcProposalFaults({
      InvalidTcArtifact(/*leader=*/4, /*view=*/15, /*slot=*/0, "hash-a",
                        /*proposal_signature_verified=*/true,
                        /*timeout_cert_verified=*/true),
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
  config.timeout_vote_equivocation_detection_enabled = true;
  config.invalid_tc_proposal_detection_enabled = true;
  config.conflicting_qc_detection_enabled = true;
  config.strong_fault_target_weight = 1;
  std::vector<MetricEvidence> evidence;
  for (int view = 1; view <= 8; ++view) {
    evidence.push_back(CertifiedQc(view, ((view - 1) % 4) + 1,
                                   Bitmap({1, 2, 3, 4}, 4),
                                   Bitmap({1, 2, 3, 4}, 4)));
  }

  const ReputationCandidate candidate = ComputeReputationCandidate(
      1, 4, 1, evidence, {30, 30, 30, 30}, config, "old-root", 7, 64,
      /*signed_proposal_evidence=*/{}, /*signed_vote_evidence=*/{},
      /*invalid_qc_proposal_evidence=*/{},
      {
          WeightUpdateVoteArtifact(/*validator=*/1, "candidate-a"),
          WeightUpdateVoteArtifact(/*validator=*/1, "candidate-b"),
      },
      {
          TimeoutVoteArtifact(/*signer=*/2, /*view=*/12, "high-qc-a"),
          TimeoutVoteArtifact(/*signer=*/2, /*view=*/12, "high-qc-b"),
      },
      {
          InvalidTcArtifact(/*leader=*/3, /*view=*/15, /*slot=*/0, "hash-a"),
      },
      {
          QcArtifact(/*view=*/21, /*slot=*/0, "qc-a", Bitmap({3, 4}, 4)),
          QcArtifact(/*view=*/21, /*slot=*/0, "qc-b", Bitmap({4}, 4)),
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
