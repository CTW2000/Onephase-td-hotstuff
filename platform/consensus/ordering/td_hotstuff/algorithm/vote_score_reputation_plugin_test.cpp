#include "platform/consensus/ordering/td_hotstuff/algorithm/vote_score_reputation_plugin.h"

#include <cstdlib>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"
#include "platform/consensus/reputation/reputation_algorithm.h"

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

std::string RotatingBitmap(int start_signer, int signer_count,
                           int total_replicas) {
  std::string bitmap((total_replicas + 7) / 8, '\0');
  for (int offset = 0; offset < signer_count; ++offset) {
    const int signer = ((start_signer - 1 + offset) % total_replicas) + 1;
    const int bit = signer - 1;
    bitmap[bit / 8] = static_cast<char>(bitmap[bit / 8] | (1 << (bit % 8)));
  }
  return bitmap;
}

std::string TempDir(const std::string& name) {
  return "/tmp/" + name + "_" + std::to_string(getpid());
}

std::string ReadFile(const std::string& path) {
  std::ifstream input(path);
  std::ostringstream data;
  data << input.rdbuf();
  return data.str();
}

int CountLines(const std::string& data) {
  int lines = 0;
  for (char ch : data) {
    if (ch == '\n') {
      ++lines;
    }
  }
  return lines;
}

std::vector<VoteScoreCandidate> WaitForCandidates(
    AsyncVoteScoreReputationPlugin* plugin, int expected_count) {
  std::vector<VoteScoreCandidate> candidates;
  for (int attempt = 0; attempt < 100 &&
                        static_cast<int>(candidates.size()) < expected_count;
       ++attempt) {
    std::vector<VoteScoreCandidate> next = plugin->TakeCompletedCandidates();
    candidates.insert(candidates.end(), std::make_move_iterator(next.begin()),
                      std::make_move_iterator(next.end()));
    if (static_cast<int>(candidates.size()) >= expected_count) {
      break;
    }
    usleep(10000);
  }
  return candidates;
}

TEST(VoteScoreReputationPluginTest, DecodesTdHotstuffSignerBitmap) {
  const std::string bitmap(1, static_cast<char>(0x09));

  EXPECT_EQ(DecodeSignerBitmap(bitmap, 5), std::vector<int>({1, 4}));
}

TEST(VoteScoreReputationPluginTest, MapsQcEvidenceToCanonicalMetricEvidence) {
  ReputationQcEvent event;
  event.qc_view = 17;
  event.qc_hash = "qc-hash";
  event.signer_bitmap = Bitmap({1, 3, 5}, 5);
  event.available_signer_bitmap = Bitmap({1, 2, 3, 4, 5}, 5);
  event.leader_id = 4;
  event.qc_collector_id = 2;
  event.weight_version = 9;
  event.active_weight_root = "weight-root";
  event.leader_eligible_min_weight = 10;

  const auto evidence = ToMetricEvidence(event);

  EXPECT_EQ(evidence.artifact_family,
            consensus::reputation::ArtifactFamily::kQc);
  EXPECT_EQ(evidence.outcome_class,
            consensus::reputation::OutcomeClass::kCertified);
  EXPECT_EQ(evidence.view_or_round, 17);
  EXPECT_EQ(evidence.leader_id, 4);
  EXPECT_EQ(evidence.collector_id, 2);
  EXPECT_EQ(evidence.artifact_digest, "qc-hash");
  EXPECT_EQ(evidence.signer_bitmap, event.signer_bitmap);
  EXPECT_EQ(evidence.available_signer_bitmap, event.available_signer_bitmap);
  EXPECT_EQ(evidence.weight_version, 9);
  EXPECT_EQ(evidence.active_weight_root, "weight-root");
  EXPECT_EQ(evidence.leader_eligible_min_weight, 10);
}

TEST(VoteScoreReputationPluginTest,
     MapsLeaderOpportunityToCanonicalTimeoutOutcome) {
  ReputationQcEvent event;
  event.qc_view = 18;
  event.leader_id = 3;
  event.leader_opportunity = true;
  event.weight_version = 2;
  event.active_weight_root = "root-2";

  const auto evidence = ToMetricEvidence(event);

  EXPECT_EQ(evidence.artifact_family,
            consensus::reputation::ArtifactFamily::kTimeout);
  EXPECT_EQ(evidence.outcome_class,
            consensus::reputation::OutcomeClass::kTimeoutOrViewChange);
  EXPECT_EQ(evidence.view_or_round, 18);
  EXPECT_EQ(evidence.leader_id, 3);
  EXPECT_TRUE(evidence.signer_bitmap.empty());
  EXPECT_EQ(evidence.weight_version, 2);
  EXPECT_EQ(evidence.active_weight_root, "root-2");
}

TEST(VoteScoreReputationPluginTest, ComputesVoteScoresWithRecoveryDecay) {
  std::vector<ReputationQcEvent> events;
  events.push_back({10, "hash-a", std::string(1, static_cast<char>(0x07))});
  events.push_back({11, "hash-b", std::string(1, static_cast<char>(0x03))});
  events.push_back({12, "hash-c", std::string(1, static_cast<char>(0x0e))});

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      /*node_id=*/2, /*total_replicas=*/4, /*window_index=*/3, events,
      /*current_weights=*/{10, 10, 10, 10}, /*max_delta=*/2);

  ASSERT_EQ(candidate.validators.size(), 4);
  EXPECT_EQ(candidate.start_qc_view, 10);
  EXPECT_EQ(candidate.end_qc_view, 12);
  EXPECT_EQ(candidate.event_count, 3);

  EXPECT_EQ(candidate.validators[0].opportunities, 2);
  EXPECT_EQ(candidate.validators[0].inclusions, 2);
  EXPECT_EQ(candidate.validators[0].vote_score, 75);
  EXPECT_EQ(candidate.validators[0].next_weight, 10);

  EXPECT_EQ(candidate.validators[1].inclusions, 3);
  EXPECT_EQ(candidate.validators[1].vote_score, 100);
  EXPECT_EQ(candidate.validators[1].next_weight, 10);

  EXPECT_EQ(candidate.validators[2].inclusions, 2);
  EXPECT_EQ(candidate.validators[2].vote_score, 75);
  EXPECT_EQ(candidate.validators[2].next_weight, 10);

  EXPECT_EQ(candidate.validators[3].inclusions, 1);
  EXPECT_EQ(candidate.validators[3].vote_score, 50);
  EXPECT_EQ(candidate.validators[3].next_weight, 10);
}

TEST(VoteScoreReputationPluginTest, CandidateRootsAreStable) {
  std::vector<ReputationQcEvent> events;
  events.push_back({4, "hash-a", std::string(1, static_cast<char>(0x03))});
  events.push_back({5, "hash-b", std::string(1, static_cast<char>(0x05))});

  const VoteScoreCandidate first = ComputeBayesianReputationCandidate(
      1, 3, 0, events, {10, 20, 30}, 2);
  const VoteScoreCandidate second = ComputeBayesianReputationCandidate(
      1, 3, 0, events, {10, 20, 30}, 2);

  EXPECT_EQ(first.metric_root_hex, second.metric_root_hex);
  EXPECT_EQ(first.next_weight_root_hex, second.next_weight_root_hex);
  EXPECT_EQ(first.candidate_digest_hex, second.candidate_digest_hex);
  EXPECT_FALSE(first.metric_root_hex.empty());
  EXPECT_FALSE(first.next_weight_root_hex.empty());
  EXPECT_FALSE(first.candidate_digest_hex.empty());

  const std::string json = VoteScoreCandidateToJson(first);
  EXPECT_NE(json.find("\"schema\":\"td_hotstuff_reputation_bayes_v4\""),
            std::string::npos);
  EXPECT_NE(json.find("\"vote_score\":100"), std::string::npos);
  EXPECT_NE(json.find("\"reputation_root\":\""), std::string::npos);
  EXPECT_NE(json.find("\"next_weight_root\":\""), std::string::npos);
}

TEST(VoteScoreReputationPluginTest, CandidateIsWeightOnlyForV1Pipeline) {
  std::vector<ReputationQcEvent> events;
  events.push_back({4, "hash-a", std::string(1, static_cast<char>(0x03))});
  events.push_back({5, "hash-b", std::string(1, static_cast<char>(0x05))});

  VoteScoreCandidate first = ComputeBayesianReputationCandidate(
      1, 3, 1, events, {10, 20, 30}, 2, "old-root",
      /*old_weight_version=*/7, /*activation_view=*/8192);
  VoteScoreCandidate second = first;
  second.validators[2].next_weight = 29;
  RecomputeVoteScoreCandidateRoots(&second);

  EXPECT_TRUE(first.leader_weights.empty());
  EXPECT_TRUE(first.leader_weight_root_hex.empty());
  EXPECT_EQ(first.leader_params_version, 0);
  EXPECT_TRUE(first.leader_randomness_ref.empty());
  EXPECT_EQ(first.leader_weight_root_hex, second.leader_weight_root_hex);
  EXPECT_NE(first.candidate_digest_hex, second.candidate_digest_hex);

  const std::string json = VoteScoreCandidateToJson(first);
  EXPECT_EQ(json.find("leader_weight_root"), std::string::npos);
  EXPECT_EQ(json.find("leader_params_version"), std::string::npos);
  EXPECT_EQ(json.find("leader_randomness_ref"), std::string::npos);
  EXPECT_EQ(json.find("leader_weights"), std::string::npos);
}

TEST(VoteScoreReputationPluginTest,
     MapsV2StrongFaultArtifactsToNeutralCandidatePenalties) {
  ReputationRecoveryConfig config;
  config.decay_per_epoch = 1;
  config.max_recovery_per_epoch = 1;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.strong_fault_enabled = true;
  config.weight_update_vote_equivocation_detection_enabled = true;
  config.timeout_vote_equivocation_detection_enabled = true;
  config.invalid_tc_proposal_detection_enabled = true;
  config.conflicting_qc_detection_enabled = true;
  config.strong_fault_target_weight = 1;

  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 2; ++view) {
    ReputationQcEvent qc;
    qc.qc_view = view;
    qc.qc_hash = "qc-" + std::to_string(view);
    qc.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    qc.leader_id = view;
    qc.weight_version = 7;
    qc.active_weight_root = "old-root";
    events.push_back(qc);
  }

  ReputationQcEvent weight_vote_a;
  weight_vote_a.weight_update_vote_artifact = true;
  weight_vote_a.protocol_id = "td_hotstuff";
  weight_vote_a.vote_signer_id = 1;
  weight_vote_a.old_weight_root = "old-root";
  weight_vote_a.old_weight_version = 7;
  weight_vote_a.activation_view = 64;
  weight_vote_a.candidate_digest = "candidate-a";
  weight_vote_a.vote_signature_verified = true;
  weight_vote_a.weight_version = 7;
  weight_vote_a.active_weight_root = "old-root";
  ReputationQcEvent weight_vote_b = weight_vote_a;
  weight_vote_b.candidate_digest = "candidate-b";
  events.push_back(weight_vote_a);
  events.push_back(weight_vote_b);

  ReputationQcEvent timeout_a;
  timeout_a.timeout_vote_artifact = true;
  timeout_a.protocol_id = "td_hotstuff";
  timeout_a.vote_signer_id = 2;
  timeout_a.qc_view = 12;
  timeout_a.high_qc_digest = "high-qc-a";
  timeout_a.vote_signature_verified = true;
  timeout_a.weight_version = 7;
  timeout_a.active_weight_root = "old-root";
  ReputationQcEvent timeout_b = timeout_a;
  timeout_b.high_qc_digest = "high-qc-b";
  events.push_back(timeout_a);
  events.push_back(timeout_b);

  ReputationQcEvent invalid_tc;
  invalid_tc.invalid_tc_proposal_artifact = true;
  invalid_tc.protocol_id = "td_hotstuff";
  invalid_tc.leader_id = 3;
  invalid_tc.qc_view = 15;
  invalid_tc.proposal_hash = "proposal-with-bad-tc";
  invalid_tc.proposal_signature_verified = true;
  invalid_tc.timeout_cert_verified = false;
  invalid_tc.invalid_reason = "bad_tc";
  invalid_tc.weight_version = 7;
  invalid_tc.active_weight_root = "old-root";
  events.push_back(invalid_tc);

  ReputationQcEvent qc_a;
  qc_a.verified_qc_artifact = true;
  qc_a.protocol_id = "td_hotstuff";
  qc_a.qc_view = 21;
  qc_a.qc_hash = "conflict-qc-a";
  qc_a.signer_bitmap = Bitmap({3, 4}, 4);
  qc_a.qc_verified = true;
  qc_a.weight_version = 7;
  qc_a.active_weight_root = "old-root";
  ReputationQcEvent qc_b = qc_a;
  qc_b.qc_hash = "conflict-qc-b";
  qc_b.signer_bitmap = Bitmap({4}, 4);
  events.push_back(qc_a);
  events.push_back(qc_b);

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
          /*current_weights=*/{30, 30, 30, 30}, config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/7,
          /*activation_view=*/64);

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({1, 1, 1, 1}));
  EXPECT_EQ(candidate.validators[0].strong_fault_count, 1);
  EXPECT_EQ(candidate.validators[1].strong_fault_count, 1);
  EXPECT_EQ(candidate.validators[2].strong_fault_count, 1);
  EXPECT_EQ(candidate.validators[3].strong_fault_count, 1);
  EXPECT_FALSE(candidate.strong_fault_root_hex.empty());
  EXPECT_FALSE(candidate.penalty_root_hex.empty());
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3CertifiedQcCountsAsDeduplicatedLeaderOpportunity) {
  std::vector<ReputationQcEvent> events;
  ReputationQcEvent certified;
  certified.qc_view = 1;
  certified.qc_hash = "certified-1";
  certified.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
  certified.leader_id = 1;
  events.push_back(certified);

  ReputationQcEvent duplicate_opportunity;
  duplicate_opportunity.qc_view = 1;
  duplicate_opportunity.leader_id = 1;
  duplicate_opportunity.leader_opportunity = true;
  events.push_back(duplicate_opportunity);
  events.push_back(duplicate_opportunity);

  ReputationQcEvent timeout_opportunity;
  timeout_opportunity.qc_view = 2;
  timeout_opportunity.leader_id = 2;
  timeout_opportunity.leader_opportunity = true;
  events.push_back(timeout_opportunity);

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 1;
  config.leader_recovery_enabled = true;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
          /*current_weights=*/{30, 30, 30, 30}, config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  ASSERT_EQ(candidate.validators.size(), 4);
  EXPECT_EQ(candidate.validators[0].leader_opportunity_count, 1);
  EXPECT_EQ(candidate.validators[0].leader_certified_count, 1);
  EXPECT_EQ(candidate.validators[0].leader_score, 67);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
  EXPECT_EQ(candidate.validators[1].leader_opportunity_count, 1);
  EXPECT_EQ(candidate.validators[1].leader_certified_count, 0);
  EXPECT_LT(candidate.validators[1].leader_score, 50);
  EXPECT_LT(candidate.validators[1].next_weight, 30);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3RepeatedMissingParticipationLosesUnrecoveredDecay) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 4; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 5);
    event.leader_id = view;
    events.push_back(event);
  }

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      /*node_id=*/1, /*total_replicas=*/5, /*window_index=*/1, events,
      /*current_weights=*/{10, 10, 10, 10, 10}, /*max_delta=*/1,
      /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/2,
      /*activation_view=*/8);

  EXPECT_EQ(candidate.algorithm, "bayes_v4");
  EXPECT_EQ(candidate.next_weights,
            std::vector<int64_t>({10, 10, 10, 10, 9}));
  EXPECT_EQ(candidate.validators[4].inclusions, 0);
  EXPECT_EQ(candidate.validators[4].next_weight, 9);
}

TEST(VoteScoreReputationPluginTest, BayesianV3KeepsSmallAllGoodWindowStable) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 4; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    event.leader_id = DefaultLeaderForView(view, 4);
    events.push_back(event);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 1;
  config.max_recovery_per_epoch = 1;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          2, 4, 1, events, {15, 15, 10, 10}, config, "old-root", 3, 8);

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({15, 15, 10, 10}));
  for (const ValidatorVoteScore& validator : candidate.validators) {
    EXPECT_EQ(validator.next_weight, validator.current_weight);
  }
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3FairRotatingQuorumKeepsHonestWeightsStable) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 20; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = RotatingBitmap(view, /*signer_count=*/10,
                                         /*total_replicas=*/20);
    event.leader_id = DefaultLeaderForView(view, /*total_replicas=*/20);
    events.push_back(event);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 3;
  config.max_recovery_per_epoch = 3;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/20, /*window_index=*/1, events,
          /*current_weights=*/std::vector<int64_t>(20, 30), config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  ASSERT_EQ(candidate.validators.size(), 20);
  for (const ValidatorVoteScore& validator : candidate.validators) {
    EXPECT_EQ(validator.opportunities, 10);
    EXPECT_EQ(validator.inclusions, 10);
    EXPECT_EQ(validator.next_weight, validator.current_weight);
  }
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3SlowVotersDecayBelowFairRotatingHonestValidators) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 20; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    std::string bitmap((20 + 7) / 8, '\0');
    for (int offset = 0; offset < 10; ++offset) {
      const int signer = 7 + ((view - 1 + offset) % 14);
      const int bit = signer - 1;
      bitmap[bit / 8] =
          static_cast<char>(bitmap[bit / 8] | (1 << (bit % 8)));
    }
    event.signer_bitmap = bitmap;
    event.leader_id = DefaultLeaderForView(view, /*total_replicas=*/20);
    events.push_back(event);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 3;
  config.max_recovery_per_epoch = 3;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/20, /*window_index=*/1, events,
          /*current_weights=*/std::vector<int64_t>(20, 30), config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  for (int validator = 1; validator <= 6; ++validator) {
    EXPECT_EQ(candidate.validators[validator - 1].inclusions, 0);
    EXPECT_LT(candidate.validators[validator - 1].next_weight, 30);
  }
  for (int validator = 7; validator <= 20; ++validator) {
    EXPECT_GE(candidate.validators[validator - 1].inclusions, 7);
    EXPECT_EQ(candidate.validators[validator - 1].next_weight, 30);
  }
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3SparseNonzeroInclusionRecoversShortWindowDecay) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 16; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap =
        view == 1 ? Bitmap({1}, 4) : Bitmap({2, 3, 4}, 4);
    event.leader_id = DefaultLeaderForView(view, /*total_replicas=*/4);
    events.push_back(event);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 3;
  config.max_recovery_per_epoch = 3;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
          /*current_weights=*/{30, 30, 30, 30}, config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  EXPECT_EQ(candidate.validators[0].inclusions, 1);
  EXPECT_LT(candidate.validators[0].vote_score, 67);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3NearFairInclusionReceivesSmallRecoveryBonus) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 8; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    std::vector<int> signers;
    for (int signer = 1; signer <= 13; ++signer) {
      signers.push_back(signer);
    }
    if (view <= 4) {
      signers.back() = 20;
    }
    std::string bitmap((20 + 7) / 8, '\0');
    for (int signer : signers) {
      const int bit = signer - 1;
      bitmap[bit / 8] =
          static_cast<char>(bitmap[bit / 8] | (1 << (bit % 8)));
    }
    event.signer_bitmap = bitmap;
    event.leader_id = DefaultLeaderForView(view, /*total_replicas=*/20);
    events.push_back(event);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 1;
  config.min_weight = 1;
  config.max_weight = 100;
  std::vector<int64_t> current_weights(20, 30);
  current_weights[19] = 25;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/20, /*window_index=*/1, events,
          current_weights, config, /*old_weight_root_hex=*/"old-root",
          /*old_weight_version=*/0, /*activation_view=*/128);

  EXPECT_EQ(candidate.validators[19].opportunities, 5);
  EXPECT_EQ(candidate.validators[19].inclusions, 4);
  EXPECT_EQ(candidate.validators[19].next_weight, 26);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3InsufficientFairOpportunityDoesNotChangeWeights) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 8; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = RotatingBitmap(view, /*signer_count=*/13,
                                         /*total_replicas=*/20);
    event.leader_id = DefaultLeaderForView(view, /*total_replicas=*/20);
    events.push_back(event);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 1;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 8;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/20, /*window_index=*/1, events,
          /*current_weights=*/std::vector<int64_t>(20, 30), config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  ASSERT_EQ(candidate.validators.size(), 20);
  for (const ValidatorVoteScore& validator : candidate.validators) {
    EXPECT_EQ(validator.opportunities, 5);
    EXPECT_EQ(validator.decay_applied, 0);
    EXPECT_EQ(validator.bonus_credit, 0);
    EXPECT_EQ(validator.next_weight, 30);
  }
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3CertifiedLeaderDoesNotFullyRecoverMissingVotes) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 4; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 4}, 4);
    event.leader_id = 3;
    events.push_back(event);
  }

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      1, 4, 1, events, {10, 10, 10, 10}, 1, "old-root", 0, 8);

  EXPECT_EQ(candidate.validators[2].inclusions, 0);
  EXPECT_EQ(candidate.validators[2].leader_certified_count, 4);
  EXPECT_EQ(candidate.validators[2].next_weight, 9);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3SignerDiversityDropsForRepeatedNarrowSignerGroup) {
  std::vector<ReputationQcEvent> narrow_events;
  std::vector<ReputationQcEvent> varied_events;
  for (int i = 0; i < 4; ++i) {
    ReputationQcEvent narrow;
    narrow.qc_view = i + 1;
    narrow.qc_hash = "narrow-" + std::to_string(i);
    narrow.signer_bitmap = Bitmap({1, 2}, 4);
    narrow.available_signer_bitmap = Bitmap({1, 2}, 4);
    narrow.leader_id = 1;
    narrow_events.push_back(narrow);

    ReputationQcEvent varied;
    varied.qc_view = i + 1;
    varied.qc_hash = "varied-" + std::to_string(i);
    varied.signer_bitmap = Bitmap({1 + (i % 4), 1 + ((i + 1) % 4),
                                   1 + ((i + 2) % 4)}, 4);
    varied.leader_id = 1;
    varied_events.push_back(varied);
  }

  const VoteScoreCandidate narrow = ComputeBayesianReputationCandidate(
      1, 4, 1, narrow_events, {10, 10, 10, 10}, 1, "old-root", 0, 8);
  const VoteScoreCandidate varied = ComputeBayesianReputationCandidate(
      1, 4, 1, varied_events, {10, 10, 10, 10}, 1, "old-root", 0, 8);

  EXPECT_LT(narrow.validators[0].leader_diversity_score,
            varied.validators[0].leader_diversity_score);

  for (int view = 1; view <= 4; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 1;
    opportunity.leader_opportunity = true;
    narrow_events.push_back(opportunity);
  }
  for (int view = 5; view <= 8; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 2;
    opportunity.leader_opportunity = true;
    narrow_events.push_back(opportunity);

    ReputationQcEvent fair;
    fair.qc_view = view;
    fair.qc_hash = "fair-" + std::to_string(view);
    fair.signer_bitmap = view % 2 == 0 ? Bitmap({1, 2, 3}, 4)
                                       : Bitmap({2, 3, 4}, 4);
    fair.available_signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    fair.leader_id = 2;
    narrow_events.push_back(fair);
  }
  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 1;
  config.leader_recovery_enabled = true;
  const VoteScoreCandidate punished =
      ComputeBayesianReputationCandidateWithConfig(
          1, 4, 1, narrow_events, {10, 10, 10, 10}, config, "old-root",
          0, 8);
  EXPECT_LT(punished.validators[0].next_weight,
            punished.validators[1].next_weight);
}


TEST(VoteScoreReputationPluginTest,
     BayesianV3RepeatedQuorumGroupIsNotHiddenByOccasionalCoverage) {
  std::vector<ReputationQcEvent> events;
  const std::vector<int64_t> weights(20, 30);
  const std::string narrow_quorum =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 20);
  const std::string fallback_15 =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15}, 20);
  const std::string fallback_16_17 =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17}, 20);
  const std::string fallback_18_20 =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 18, 19, 20}, 20);
  const std::string fair_alternate =
      Bitmap({7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 20);

  for (int view = 1; view <= 32; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 1;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    ReputationQcEvent qc;
    qc.qc_view = view;
    qc.qc_hash = "qc-" + std::to_string(view);
    qc.leader_id = 1;
    if (view % 8 == 0) {
      const int fallback_index = view / 8;
      if (fallback_index == 1) {
        qc.signer_bitmap = fallback_15;
      } else if (fallback_index == 2) {
        qc.signer_bitmap = fallback_16_17;
      } else {
        qc.signer_bitmap = fallback_18_20;
      }
    } else {
      qc.signer_bitmap = narrow_quorum;
    }
    qc.available_signer_bitmap = qc.signer_bitmap;
    events.push_back(qc);
  }
  for (int view = 33; view <= 64; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 2;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    ReputationQcEvent qc;
    qc.qc_view = view;
    qc.qc_hash = "fair-" + std::to_string(view);
    qc.leader_id = 2;
    qc.signer_bitmap = view % 2 == 0 ? narrow_quorum : fair_alternate;
    events.push_back(qc);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 1;
  config.leader_recovery_enabled = true;
  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidateWithConfig(
      1, 20, 1, events, weights, config, "old-root", 0, 64);

  EXPECT_LT(candidate.validators[0].leader_diversity_score, 67);
  EXPECT_EQ(candidate.validators[1].leader_diversity_score, 100);
  EXPECT_LT(candidate.validators[0].next_weight, 30);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}



TEST(VoteScoreReputationPluginTest,
     BayesianV3FrequencySkewWithBroadTargetIsMetricOnly) {
  std::vector<ReputationQcEvent> events;
  const std::vector<int64_t> weights(20, 30);
  const std::string broad_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);

  for (int view = 1; view <= 32; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 1;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    const int rare_signer = 14 + (view % 7);
    ReputationQcEvent qc;
    qc.qc_view = view;
    qc.qc_hash = "skewed-" + std::to_string(view);
    qc.leader_id = 1;
    qc.signer_bitmap = Bitmap({1, 2, 3, 4, 5, 6, 7,
                               8, 9, 10, 11, 12, 13, rare_signer},
                              20);
    qc.available_signer_bitmap = broad_available;
    events.push_back(qc);
  }
  for (int view = 33; view <= 64; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 2;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    ReputationQcEvent qc;
    qc.qc_view = view;
    qc.qc_hash = "fair-" + std::to_string(view);
    qc.leader_id = 2;
    qc.signer_bitmap = broad_available;
    qc.available_signer_bitmap = broad_available;
    events.push_back(qc);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 1;
  config.leader_recovery_enabled = true;
  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidateWithConfig(
      1, 20, 1, events, weights, config, "old-root", 0, 128);

  EXPECT_LT(candidate.validators[0].leader_diversity_score,
            candidate.validators[1].leader_diversity_score);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3TargetCoverageDistinguishesNarrowAndBroadCollectors) {
  std::vector<ReputationQcEvent> events;
  const std::vector<int64_t> weights(20, 30);
  const std::string narrow_quorum =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 20);
  const std::string broad_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);

  for (int view = 1; view <= 32; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 1;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    ReputationQcEvent qc;
    qc.qc_view = view;
    qc.qc_hash = "narrow-selected-" + std::to_string(view);
    qc.leader_id = 1;
    qc.signer_bitmap = narrow_quorum;
    qc.available_signer_bitmap = broad_available;
    events.push_back(qc);
  }
  for (int view = 33; view <= 64; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 2;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    ReputationQcEvent qc;
    qc.qc_view = view;
    qc.qc_hash = "narrow-target-" + std::to_string(view);
    qc.leader_id = 2;
    qc.signer_bitmap = narrow_quorum;
    qc.available_signer_bitmap = narrow_quorum;
    events.push_back(qc);
  }
  for (int view = 65; view <= 96; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 3;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    ReputationQcEvent qc;
    qc.qc_view = view;
    qc.qc_hash = "broad-rotating-" + std::to_string(view);
    qc.leader_id = 3;
    qc.signer_bitmap = RotatingBitmap(view, /*signer_count=*/14,
                                      /*total_replicas=*/20);
    qc.available_signer_bitmap = broad_available;
    events.push_back(qc);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 1;
  config.leader_recovery_enabled = true;
  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidateWithConfig(
      1, 20, 1, events, weights, config, "old-root", 0, 128);

  EXPECT_LT(candidate.validators[0].leader_diversity_score,
            candidate.validators[2].leader_diversity_score);
  EXPECT_LT(candidate.validators[1].leader_diversity_score,
            candidate.validators[2].leader_diversity_score);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
  EXPECT_LT(candidate.validators[1].next_weight,
            candidate.validators[2].next_weight);
  EXPECT_EQ(candidate.validators[2].next_weight, 30);
}


TEST(VoteScoreReputationPluginTest,
     BayesianV3SilentLeaderRecoveryDoesNotUseBroadTargetDiversity) {
  std::vector<ReputationQcEvent> events;
  const std::vector<int64_t> weights(20, 30);
  const std::string broad_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);
  const std::string narrow_signers =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 20);

  for (int view = 1; view <= 32; ++view) {
    ReputationQcEvent silent_opportunity;
    silent_opportunity.qc_view = view;
    silent_opportunity.leader_id = 1;
    silent_opportunity.leader_opportunity = true;
    events.push_back(silent_opportunity);

    ReputationQcEvent honest_opportunity;
    honest_opportunity.qc_view = 100 + view;
    honest_opportunity.leader_id = 2;
    honest_opportunity.leader_opportunity = true;
    events.push_back(honest_opportunity);

    ReputationQcEvent qc;
    qc.qc_view = 100 + view;
    qc.qc_hash = "honest-low-selected-diversity-" + std::to_string(view);
    qc.leader_id = 2;
    qc.signer_bitmap = narrow_signers;
    qc.available_signer_bitmap = broad_available;
    events.push_back(qc);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 1;
  config.leader_recovery_enabled = true;
  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidateWithConfig(
      1, 20, 1, events, weights, config, "old-root", 0, 128);

  EXPECT_LT(candidate.validators[0].leader_score, 50);
  EXPECT_LT(candidate.validators[0].next_weight, 30);
  EXPECT_LT(candidate.validators[1].leader_diversity_score, 100);
  EXPECT_GE(candidate.validators[1].leader_score, 95);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3CollectorTargetCoverageReducesCollectorRecovery) {
  std::vector<ReputationQcEvent> events;
  const std::vector<int64_t> weights(20, 30);
  const std::string narrow_quorum =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, 20);
  const std::string broad_available =
      Bitmap({1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
             20);

  for (int view = 1; view <= 32; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 1;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    ReputationQcEvent qc;
    qc.qc_view = view;
    qc.qc_hash = "collector-narrow-" + std::to_string(view);
    qc.leader_id = 1;
    qc.qc_collector_id = 1;
    qc.signer_bitmap = narrow_quorum;
    qc.available_signer_bitmap = narrow_quorum;
    events.push_back(qc);
  }
  for (int view = 33; view <= 64; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 2;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    ReputationQcEvent qc;
    qc.qc_view = view;
    qc.qc_hash = "collector-broad-" + std::to_string(view);
    qc.leader_id = 2;
    qc.qc_collector_id = 2;
    qc.signer_bitmap = RotatingBitmap(view, /*signer_count=*/14,
                                      /*total_replicas=*/20);
    qc.available_signer_bitmap = broad_available;
    events.push_back(qc);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 1;
  config.leader_recovery_enabled = true;
  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidateWithConfig(
      1, 20, 1, events, weights, config, "old-root", 0, 128);

  EXPECT_LT(candidate.validators[0].leader_diversity_score,
            candidate.validators[1].leader_diversity_score);
  EXPECT_LT(candidate.validators[0].next_weight,
            candidate.validators[1].next_weight);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3LeaderCertifiedCountContributesToLeaderScore) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 5; ++view) {
    const int leader = view == 1 ? 1 : 2;
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = leader;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    event.leader_id = leader;
    events.push_back(event);
  }

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      1, 4, 1, events, {10, 10, 10, 10}, 1, "old-root", 0, 8);

  EXPECT_EQ(candidate.validators[0].leader_certified_count, 1);
  EXPECT_EQ(candidate.validators[1].leader_certified_count, 4);
  EXPECT_EQ(candidate.validators[0].leader_score, 67);
  EXPECT_EQ(candidate.validators[1].leader_score, 83);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3UsesRecordedLeaderIdInsteadOfDefaultLeaderSchedule) {
  ReputationQcEvent event;
  event.qc_view = 1;
  event.qc_hash = "hash-1";
  event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
  event.leader_id = 4;

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      1, 4, 1, {event}, {30, 30, 30, 30}, 1, "old-root", 0, 8);

  EXPECT_EQ(DefaultLeaderForView(1, 4), 2);
  EXPECT_EQ(candidate.validators[1].leader_certified_count, 0);
  EXPECT_EQ(candidate.validators[3].leader_certified_count, 1);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3LeaderEligibilityComesFromEvidenceMetadata) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 12; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 1;
    opportunity.leader_opportunity = true;
    opportunity.leader_eligible_min_weight = 10;
    events.push_back(opportunity);

    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    event.leader_id = 1;
    event.leader_eligible_min_weight = 10;
    events.push_back(event);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 10;
  config.max_recovery_per_epoch = 10;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 2;
  config.leader_eligible_min_weight = 1;
  config.leader_recovery_enabled = true;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
          /*current_weights=*/{30, 5, 5, 5}, config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  EXPECT_GT(candidate.validators[0].leader_opportunity_count, 0);
  EXPECT_EQ(candidate.validators[1].leader_opportunity_count, 0);
  EXPECT_EQ(candidate.validators[2].leader_opportunity_count, 0);
  EXPECT_EQ(candidate.validators[3].leader_opportunity_count, 0);
  EXPECT_EQ(candidate.validators[1].next_weight, 5);
  EXPECT_EQ(candidate.validators[2].next_weight, 5);
  EXPECT_EQ(candidate.validators[3].next_weight, 5);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3SilentEligibleLeaderLosesRecoveryFromLowProposalSuccess) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 12; ++view) {
    if (view <= 4) {
      ReputationQcEvent opportunity;
      opportunity.qc_view = view;
      opportunity.leader_id = 1;
      opportunity.leader_opportunity = true;
      events.push_back(opportunity);
    }

    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    event.leader_id = 2 + ((view - 1) % 3);
    events.push_back(event);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 10;
  config.max_recovery_per_epoch = 10;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 2;
  config.leader_eligible_min_weight = 1;
  config.leader_recovery_enabled = true;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
          /*current_weights=*/{30, 30, 30, 30}, config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  EXPECT_EQ(candidate.validators[0].leader_certified_count, 0);
  EXPECT_GT(candidate.validators[0].leader_opportunity_count, 0);
  EXPECT_LT(candidate.validators[0].leader_score, 67);
  EXPECT_LT(candidate.validators[0].next_weight, 30);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
  EXPECT_TRUE(candidate.leader_weights.empty());
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3LeaderScoreDoesNotAffectWeightsByDefault) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 12; ++view) {
    if (view <= 4) {
      ReputationQcEvent opportunity;
      opportunity.qc_view = view;
      opportunity.leader_id = 1;
      opportunity.leader_opportunity = true;
      events.push_back(opportunity);
    }

    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    event.leader_id = 2 + ((view - 1) % 3);
    events.push_back(event);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 10;
  config.max_recovery_per_epoch = 10;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 2;
  config.leader_eligible_min_weight = 1;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
          /*current_weights=*/{30, 30, 30, 30}, config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  EXPECT_EQ(candidate.validators[0].leader_certified_count, 0);
  EXPECT_GT(candidate.validators[0].leader_opportunity_count, 0);
  EXPECT_LT(candidate.validators[0].leader_score, 67);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3UsesExplicitLeaderOpportunitiesForSilentLeader) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 12; ++view) {
    const int leader = DefaultLeaderForView(view, 4);
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = leader;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);

    if (leader == 1) {
      continue;
    }
    ReputationQcEvent certified;
    certified.qc_view = view;
    certified.qc_hash = "hash-" + std::to_string(view);
    certified.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    certified.leader_id = leader;
    events.push_back(certified);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 10;
  config.max_recovery_per_epoch = 10;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 2;
  config.leader_eligible_min_weight = 1;
  config.leader_recovery_enabled = true;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
          /*current_weights=*/{30, 30, 30, 30}, config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  EXPECT_EQ(candidate.validators[0].leader_certified_count, 0);
  EXPECT_GT(candidate.validators[0].leader_opportunity_count, 0);
  EXPECT_LT(candidate.validators[0].leader_score, 67);
  EXPECT_LT(candidate.validators[0].next_weight, 30);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
  EXPECT_TRUE(candidate.leader_weights.empty());
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3VeryLowLeaderScoreCanReduceRecoveryBeforeLargeSampleGate) {
  std::vector<ReputationQcEvent> events;
  ReputationQcEvent opportunity;
  opportunity.qc_view = 1;
  opportunity.leader_id = 1;
  opportunity.leader_opportunity = true;
  events.push_back(opportunity);
  for (int view = 1; view <= 8; ++view) {
    ReputationQcEvent certified;
    certified.qc_view = view;
    certified.qc_hash = "hash-" + std::to_string(view);
    certified.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    certified.leader_id = 2 + ((view - 1) % 3);
    events.push_back(certified);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 8;
  config.leader_recovery_enabled = true;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
          /*current_weights=*/{30, 30, 30, 30}, config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  EXPECT_GE(candidate.validators[0].vote_score, 67);
  EXPECT_EQ(candidate.validators[0].leader_opportunity_count, 1);
  EXPECT_EQ(candidate.validators[0].leader_certified_count, 0);
  EXPECT_LT(candidate.validators[0].leader_score, 50);
  EXPECT_LT(candidate.validators[0].next_weight, 30);
  EXPECT_EQ(candidate.validators[1].next_weight, 30);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3PartialLeaderSuccessDoesNotTriggerEarlyLeaderReduction) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 3; ++view) {
    ReputationQcEvent opportunity;
    opportunity.qc_view = view;
    opportunity.leader_id = 1;
    opportunity.leader_opportunity = true;
    events.push_back(opportunity);
  }
  ReputationQcEvent certified_once;
  certified_once.qc_view = 1;
  certified_once.qc_hash = "hash-1";
  certified_once.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
  certified_once.leader_id = 1;
  events.push_back(certified_once);
  for (int view = 4; view <= 12; ++view) {
    ReputationQcEvent certified;
    certified.qc_view = view;
    certified.qc_hash = "hash-" + std::to_string(view);
    certified.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    certified.leader_id = 2 + ((view - 1) % 3);
    events.push_back(certified);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 5;
  config.max_recovery_per_epoch = 5;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.min_decay_opportunities = 1;
  config.min_leader_opportunities = 8;
  config.leader_recovery_enabled = true;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
          /*current_weights=*/{30, 30, 30, 30}, config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  EXPECT_GT(candidate.validators[0].leader_certified_count, 0);
  EXPECT_LT(candidate.validators[0].leader_score, 50);
  EXPECT_EQ(candidate.validators[0].next_weight, 30);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3MissingViewsDoNotLowerLeaderScore) {
  ReputationQcEvent first;
  first.qc_view = 1;
  first.qc_hash = "hash-1";
  first.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
  first.leader_id = 2;

  ReputationQcEvent second;
  second.qc_view = 4;
  second.qc_hash = "hash-4";
  second.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
  second.leader_id = 1;

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      1, 4, 1, {first, second}, {10, 10, 10, 10}, 1, "old-root", 0, 8);

  EXPECT_EQ(candidate.validators[2].leader_score, 100);
  EXPECT_EQ(candidate.validators[3].leader_score, 100);
  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({10, 10, 10, 10}));
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3BonusDoesNotDriftBalancedAllGoodWindow) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 64; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    event.leader_id = DefaultLeaderForView(view, 4);
    events.push_back(event);
  }

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      1, 4, 2, events, {30, 30, 30, 30}, 3, "old-root", 0, 128);

  EXPECT_EQ(candidate.algorithm, "bayes_v4");
  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({30, 30, 30, 30}));
  for (const ValidatorVoteScore& validator : candidate.validators) {
    EXPECT_EQ(validator.bonus_credit, 0);
    EXPECT_EQ(validator.next_weight, validator.current_weight);
  }
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3LowParticipationDecaysWithoutDirectSlash) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 4; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 5);
    event.leader_id = view;
    events.push_back(event);
  }

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      1, 5, 2, events, {10, 10, 10, 10, 10}, 3, "old-root", 0, 128);

  EXPECT_EQ(candidate.validators[0].next_weight, 10);
  EXPECT_EQ(candidate.validators[1].next_weight, 10);
  EXPECT_EQ(candidate.validators[2].next_weight, 10);
  EXPECT_EQ(candidate.validators[3].next_weight, 10);
  EXPECT_EQ(candidate.validators[4].inclusions, 0);
  EXPECT_EQ(candidate.validators[4].next_weight, 7);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3RepeatedNonParticipationReachesMinimumWeight) {
  std::vector<int64_t> weights = {10, 10, 10, 10, 10};
  for (int epoch = 0; epoch < 4; ++epoch) {
    std::vector<ReputationQcEvent> events;
    for (int offset = 1; offset <= 4; ++offset) {
      const int view = epoch * 4 + offset;
      ReputationQcEvent event;
      event.qc_view = view;
      event.qc_hash = "hash-" + std::to_string(view);
      event.signer_bitmap = Bitmap({1, 2, 3, 4}, 5);
      event.leader_id = offset;
      events.push_back(event);
    }
    const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
        1, 5, epoch + 2, events, weights, 3, "old-root", epoch, 128);
    weights = candidate.next_weights;
  }

  EXPECT_EQ(weights, std::vector<int64_t>({10, 10, 10, 10, 1}));
}

TEST(VoteScoreReputationPluginTest,
     VoteScoreCandidateJsonDoesNotWriteRemovedLeaderGapField) {
  ReputationQcEvent event;
  event.qc_view = 1;
  event.qc_hash = "hash-1";
  event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
  event.leader_id = 1;

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      1, 4, 1, {event}, {10, 10, 10, 10}, 1, "old-root", 0, 8);

  const std::string json = VoteScoreCandidateToJson(candidate);
  EXPECT_EQ(json.find(std::string("leader_") + "gap_count"), std::string::npos);
}

TEST(VoteScoreReputationPluginTest,
     CandidateDigestIgnoresLocalNodeButAuditJsonKeepsIt) {
  std::vector<ReputationQcEvent> events;
  events.push_back({4, "hash-a", std::string(1, static_cast<char>(0x03))});
  events.push_back({5, "hash-b", std::string(1, static_cast<char>(0x05))});

  const std::string old_weight_root = "old-root";
  const VoteScoreCandidate first = ComputeBayesianReputationCandidate(
      1, 3, 0, events, {10, 20, 30}, 2, old_weight_root,
      /*old_weight_version=*/7, /*activation_view=*/8192);
  const VoteScoreCandidate second = ComputeBayesianReputationCandidate(
      2, 3, 0, events, {10, 20, 30}, 2, old_weight_root,
      /*old_weight_version=*/7, /*activation_view=*/8192);

  EXPECT_EQ(first.metric_root_hex, second.metric_root_hex);
  EXPECT_EQ(first.next_weight_root_hex, second.next_weight_root_hex);
  EXPECT_EQ(first.candidate_digest_hex, second.candidate_digest_hex);
  EXPECT_EQ(first.old_weight_root_hex, old_weight_root);
  EXPECT_EQ(first.old_weight_version, 7);
  EXPECT_EQ(first.activation_view, 8192);
  EXPECT_EQ(first.next_weights, std::vector<int64_t>({11, 20, 30}));

  const std::string first_json = VoteScoreCandidateToJson(first);
  const std::string second_json = VoteScoreCandidateToJson(second);
  EXPECT_NE(first_json.find("\"local_node_id\":1"), std::string::npos);
  EXPECT_NE(second_json.find("\"local_node_id\":2"), std::string::npos);
  EXPECT_NE(first_json.find("\"old_weight_root\":\"old-root\""),
            std::string::npos);
  EXPECT_NE(first_json.find("\"activation_view\":8192"), std::string::npos);
}

TEST(VoteScoreReputationPluginTest, AsyncPluginWritesOnlyCompleteWindows) {
  const std::string output_dir = TempDir("td_hotstuff_reputation_writer_test");
  const std::string output_file = output_dir + "/td_hotstuff_reputation_node_3.jsonl";
  std::remove(output_file.c_str());

  AsyncVoteScoreReputationPlugin plugin(
      /*node_id=*/3, /*total_replicas=*/4, /*current_weights=*/{10, 10, 10, 10},
      output_dir, /*window_size=*/2, /*queue_capacity=*/16, /*max_delta=*/2);
  plugin.Start();
  EXPECT_TRUE(plugin.RecordQc(1, "hash-a", std::string(1, static_cast<char>(0x07))));
  EXPECT_TRUE(plugin.RecordQc(2, "hash-b", std::string(1, static_cast<char>(0x03))));
  EXPECT_TRUE(plugin.RecordQc(3, "hash-c", std::string(1, static_cast<char>(0x0f))));
  plugin.Stop();

  const std::string data = ReadFile(output_file);
  EXPECT_EQ(CountLines(data), 1);
  EXPECT_NE(data.find("\"window_index\":0"), std::string::npos);
  EXPECT_EQ(data.find("\"window_index\":1"), std::string::npos);
}

TEST(VoteScoreReputationPluginTest,
     AsyncPluginIgnoresLatePartialWindowsForCertification) {
  setenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS", "4", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY", "1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES", "1", /*overwrite=*/1);
  const std::string output_dir =
      TempDir("td_hotstuff_reputation_late_partial_test");
  const std::string output_file =
      output_dir + "/td_hotstuff_reputation_node_1.jsonl";
  std::remove(output_file.c_str());

  const std::vector<int64_t> weights = {30, 30, 30, 30};
  const std::string weight_root = WeightRootHex(weights);
  AsyncVoteScoreReputationPlugin plugin(
      /*node_id=*/1, /*total_replicas=*/4, weights, output_dir,
      /*window_size=*/4, /*queue_capacity=*/64, /*max_delta=*/5);
  plugin.Start();
  for (int view = 1; view <= 4; ++view) {
    EXPECT_TRUE(plugin.RecordQc(view, "hash-" + std::to_string(view),
                                Bitmap({1, 2, 3, 4}, 4),
                                DefaultLeaderForView(view, 4),
                                /*weight_version=*/0, weight_root));
  }
  EXPECT_TRUE(plugin.RecordLeaderOpportunity(
      /*view=*/5, DefaultLeaderForView(5, 4), /*weight_version=*/0,
      weight_root));
  EXPECT_TRUE(plugin.RecordQc(/*qc_view=*/1, "late-hash",
                              Bitmap({1}, 4),
                              DefaultLeaderForView(1, 4),
                              /*weight_version=*/0, weight_root));
  plugin.Stop();

  const std::string data = ReadFile(output_file);
  EXPECT_EQ(CountLines(data), 1);
  EXPECT_NE(data.find("\"start_qc_view\":1"), std::string::npos);
  EXPECT_NE(data.find("\"end_qc_view\":4"), std::string::npos);
  EXPECT_EQ(data.find("late-hash"), std::string::npos);

  unsetenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS");
  unsetenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY");
  unsetenv("TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES");
}

TEST(VoteScoreReputationPluginTest,
     AsyncPluginRealignsWindowAfterWeightActivation) {
  setenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS", "4", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY", "1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES", "1", /*overwrite=*/1);
  const std::string output_dir =
      TempDir("td_hotstuff_reputation_realign_test");
  const std::vector<int64_t> initial_weights = {30, 30, 30, 30};
  const std::vector<int64_t> activated_weights = {25, 30, 30, 30};
  const std::string initial_root = WeightRootHex(initial_weights);
  const std::string activated_root = WeightRootHex(activated_weights);

  AsyncVoteScoreReputationPlugin plugin(
      /*node_id=*/1, /*total_replicas=*/4, initial_weights, output_dir,
      /*window_size=*/4, /*queue_capacity=*/64, /*max_delta=*/5);
  plugin.Start();
  for (int view = 1; view <= 4; ++view) {
    EXPECT_TRUE(plugin.RecordQc(view, "hash-" + std::to_string(view),
                                Bitmap({2, 3, 4}, 4),
                                DefaultLeaderForView(view, 4),
                                /*weight_version=*/0, initial_root));
  }
  std::vector<VoteScoreCandidate> first = WaitForCandidates(&plugin, 1);
  ASSERT_EQ(first.size(), 1);
  EXPECT_EQ(first[0].start_qc_view, 1);
  EXPECT_EQ(first[0].end_qc_view, 4);

  plugin.UpdateCurrentWeights(activated_weights, activated_root,
                              /*old_weight_version=*/1);
  for (int view = 7; view <= 12; ++view) {
    EXPECT_TRUE(plugin.RecordQc(view, "hash-" + std::to_string(view),
                                Bitmap({2, 3, 4}, 4),
                                DefaultLeaderForView(view, 4),
                                /*weight_version=*/1, activated_root));
  }
  std::vector<VoteScoreCandidate> second = WaitForCandidates(&plugin, 1);
  plugin.Stop();

  ASSERT_EQ(second.size(), 1);
  EXPECT_EQ(second[0].old_weight_version, 1);
  EXPECT_EQ(second[0].old_weight_root_hex, activated_root);
  EXPECT_EQ(second[0].start_qc_view, 9);
  EXPECT_EQ(second[0].end_qc_view, 12);

  unsetenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS");
  unsetenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY");
  unsetenv("TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES");
}


TEST(VoteScoreReputationPluginTest,
     StrongFaultStickinessAppliesOnlyToProvenFaults) {
  setenv("TD_HS_STRONG_FAULT_ENABLE", "1", /*overwrite=*/1);
  setenv("TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE", "1", /*overwrite=*/1);
  setenv("TD_HS_STRONG_FAULT_TARGET_WEIGHT", "1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_BONUS_PER_EPOCH", "3", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES", "1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_MIN_CANDIDATE_QCS", "1", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS", "2", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY", "1",
         /*overwrite=*/1);

  const std::string output_dir =
      TempDir("td_hotstuff_reputation_strong_fault_sticky_test");
  const std::vector<int64_t> low_non_fault_weights = {1, 30, 30, 30};
  const std::string low_non_fault_root = WeightRootHex(low_non_fault_weights);
  AsyncVoteScoreReputationPlugin non_fault_plugin(
      /*node_id=*/1, /*total_replicas=*/4, low_non_fault_weights,
      output_dir + "_non_fault", /*window_size=*/2, /*queue_capacity=*/64,
      /*max_delta=*/3);
  non_fault_plugin.Start();
  for (int view = 1; view <= 2; ++view) {
    EXPECT_TRUE(non_fault_plugin.RecordQc(
        view, "non-fault-qc-" + std::to_string(view),
        Bitmap({1, 2, 3, 4}, 4), DefaultLeaderForView(view, 4),
        /*weight_version=*/0, low_non_fault_root));
  }
  std::vector<VoteScoreCandidate> non_fault =
      WaitForCandidates(&non_fault_plugin, 1);
  non_fault_plugin.Stop();
  ASSERT_EQ(non_fault.size(), 1);
  EXPECT_EQ(non_fault[0].validators[0].strong_fault_count, 0);
  EXPECT_GT(non_fault[0].next_weights[0], 1);

  const std::vector<int64_t> initial_weights = {30, 30, 30, 30};
  const std::string initial_root = WeightRootHex(initial_weights);
  AsyncVoteScoreReputationPlugin fault_plugin(
      /*node_id=*/1, /*total_replicas=*/4, initial_weights,
      output_dir + "_fault", /*window_size=*/2, /*queue_capacity=*/64,
      /*max_delta=*/3);
  fault_plugin.Start();
  consensus::reputation::SignedProposalEvidence first;
  first.protocol_id = "td_hotstuff";
  first.leader_id = 2;
  first.view_or_round = 1;
  first.slot_or_height = 0;
  first.proposal_hash = "proposal-a";
  first.signature_verified = true;
  first.active_weight_root = initial_root;
  first.weight_version = 0;
  consensus::reputation::SignedProposalEvidence second = first;
  second.proposal_hash = "proposal-b";
  EXPECT_TRUE(fault_plugin.RecordSignedProposalArtifact(first));
  EXPECT_TRUE(fault_plugin.RecordSignedProposalArtifact(second));
  std::vector<VoteScoreCandidate> first_fault_window =
      WaitForCandidates(&fault_plugin, 1);
  ASSERT_EQ(first_fault_window.size(), 1);
  EXPECT_EQ(first_fault_window[0].validators[1].strong_fault_count, 1);
  EXPECT_EQ(first_fault_window[0].next_weights[1], 1);

  const std::vector<int64_t> activated_weights = {30, 2, 30, 30};
  const std::string activated_root = WeightRootHex(activated_weights);
  fault_plugin.UpdateCurrentWeights(activated_weights, activated_root,
                                    /*old_weight_version=*/1);
  for (int view = 3; view <= 4; ++view) {
    EXPECT_TRUE(fault_plugin.RecordQc(
        view, "post-fault-qc-" + std::to_string(view),
        Bitmap({1, 2, 3, 4}, 4), DefaultLeaderForView(view, 4),
        /*weight_version=*/1, activated_root));
  }
  std::vector<VoteScoreCandidate> second_fault_window =
      WaitForCandidates(&fault_plugin, 1);
  fault_plugin.Stop();
  ASSERT_EQ(second_fault_window.size(), 1);
  EXPECT_EQ(second_fault_window[0].validators[1].strong_fault_count, 1);
  EXPECT_EQ(second_fault_window[0].next_weights[1], 1);
  EXPECT_GT(second_fault_window[0].next_weights[0], 1);

  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE");
  unsetenv("TD_HS_STRONG_FAULT_TARGET_WEIGHT");
  unsetenv("TD_HS_REPUTATION_BONUS_PER_EPOCH");
  unsetenv("TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES");
  unsetenv("TD_HS_REPUTATION_MIN_CANDIDATE_QCS");
  unsetenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS");
  unsetenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY");
}

TEST(VoteScoreReputationPluginTest,
     DirectInvalidTcFaultFlushesSparseCandidateWithoutSoftWeightDrift) {
  setenv("TD_HS_STRONG_FAULT_ENABLE", "1", /*overwrite=*/1);
  setenv("TD_HS_INVALID_TC_PROPOSAL_DETECT_ENABLE", "1", /*overwrite=*/1);
  setenv("TD_HS_STRONG_FAULT_TARGET_WEIGHT", "1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_MIN_CANDIDATE_QCS", "16", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS", "64", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY", "1",
         /*overwrite=*/1);

  const std::vector<int64_t> initial_weights = {30, 30, 30, 30};
  const std::string initial_root = WeightRootHex(initial_weights);
  AsyncVoteScoreReputationPlugin plugin(
      /*node_id=*/1, /*total_replicas=*/4, initial_weights,
      TempDir("td_hotstuff_reputation_sparse_invalid_tc_test"),
      /*window_size=*/64, /*queue_capacity=*/64, /*max_delta=*/3);
  plugin.Start();

  consensus::reputation::InvalidTcProposalEvidence invalid_tc;
  invalid_tc.protocol_id = "td_hotstuff";
  invalid_tc.leader_id = 2;
  invalid_tc.view_or_round = 5;
  invalid_tc.slot_or_height = 0;
  invalid_tc.proposal_hash = "signed-invalid-tc-proposal";
  invalid_tc.proposal_signature_verified = true;
  invalid_tc.timeout_cert_verified = false;
  invalid_tc.invalid_reason = "invalid_tc_in_proposal";
  invalid_tc.active_weight_root = initial_root;
  invalid_tc.weight_version = 0;
  EXPECT_TRUE(plugin.RecordInvalidTcProposalArtifact(invalid_tc));

  std::vector<VoteScoreCandidate> candidates = WaitForCandidates(&plugin, 1);
  plugin.Stop();
  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].validators[1].strong_fault_count, 1);
  EXPECT_EQ(candidates[0].next_weights[1], 1);
  EXPECT_EQ(candidates[0].next_weights[0], 30);
  EXPECT_EQ(candidates[0].next_weights[2], 30);
  EXPECT_EQ(candidates[0].next_weights[3], 30);
  EXPECT_EQ(candidates[0].event_count, 0);
  EXPECT_EQ(candidates[0].activation_view, 129);

  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_INVALID_TC_PROPOSAL_DETECT_ENABLE");
  unsetenv("TD_HS_STRONG_FAULT_TARGET_WEIGHT");
  unsetenv("TD_HS_REPUTATION_MIN_CANDIDATE_QCS");
  unsetenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS");
  unsetenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY");
}

TEST(VoteScoreReputationPluginTest,
     WeightUpdateVoteEquivocationFlushesSparsePenaltyCandidate) {
  setenv("TD_HS_STRONG_FAULT_ENABLE", "1", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE", "1",
         /*overwrite=*/1);
  setenv("TD_HS_STRONG_FAULT_TARGET_WEIGHT", "1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_MIN_CANDIDATE_QCS", "16", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS", "64", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY", "1",
         /*overwrite=*/1);

  const std::vector<int64_t> initial_weights = {30, 30, 30, 30};
  const std::string initial_root = WeightRootHex(initial_weights);
  AsyncVoteScoreReputationPlugin plugin(
      /*node_id=*/1, /*total_replicas=*/4, initial_weights,
      TempDir("td_hotstuff_reputation_sparse_wuv_test"),
      /*window_size=*/64, /*queue_capacity=*/64, /*max_delta=*/3);
  plugin.Start();

  consensus::reputation::SignedWeightUpdateVoteEvidence first;
  first.protocol_id = "td_hotstuff";
  first.validator_id = 3;
  first.old_weight_root = initial_root;
  first.old_weight_version = 0;
  first.activation_view = 2;
  first.candidate_digest = "candidate-a";
  first.signature_verified = true;
  first.active_weight_root = initial_root;
  first.weight_version = 0;
  consensus::reputation::SignedWeightUpdateVoteEvidence second = first;
  second.candidate_digest = "candidate-b";
  EXPECT_TRUE(plugin.RecordSignedWeightUpdateVoteArtifact(first));
  EXPECT_TRUE(plugin.RecordSignedWeightUpdateVoteArtifact(second));

  std::vector<VoteScoreCandidate> candidates = WaitForCandidates(&plugin, 1);
  plugin.Stop();
  ASSERT_EQ(candidates.size(), 1);
  EXPECT_EQ(candidates[0].validators[2].strong_fault_count, 1);
  EXPECT_EQ(candidates[0].next_weights[2], 1);
  EXPECT_EQ(candidates[0].next_weights[0], 30);
  EXPECT_EQ(candidates[0].next_weights[1], 30);
  EXPECT_EQ(candidates[0].next_weights[3], 30);
  EXPECT_EQ(candidates[0].event_count, 0);
  EXPECT_EQ(candidates[0].activation_view, 129);

  unsetenv("TD_HS_STRONG_FAULT_ENABLE");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE");
  unsetenv("TD_HS_STRONG_FAULT_TARGET_WEIGHT");
  unsetenv("TD_HS_REPUTATION_MIN_CANDIDATE_QCS");
  unsetenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS");
  unsetenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY");
}

TEST(VoteScoreReputationPluginTest, CreateFromEnvReturnsNullWhenDisabled) {
  unsetenv("TD_HS_REPUTATION_ENABLE");

  EXPECT_EQ(AsyncVoteScoreReputationPlugin::CreateFromEnv(
                /*node_id=*/1, /*total_replicas=*/4, /*current_weights=*/{1, 1, 1, 1}),
            nullptr);
}

TEST(VoteScoreReputationPluginTest, DropsWhenQueueIsFull) {
  AsyncVoteScoreReputationPlugin plugin(
      /*node_id=*/1, /*total_replicas=*/4, /*current_weights=*/{1, 1, 1, 1},
      TempDir("td_hotstuff_reputation_drop_test"), /*window_size=*/8,
      /*queue_capacity=*/1, /*max_delta=*/2);

  EXPECT_TRUE(plugin.RecordQc(1, "hash-a", std::string(1, static_cast<char>(0x0f))));
  EXPECT_FALSE(plugin.RecordQc(2, "hash-b", std::string(1, static_cast<char>(0x0f))));
  EXPECT_EQ(plugin.dropped_count(), 1);
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
