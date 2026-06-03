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
  EXPECT_EQ(candidate.validators[1].next_weight, 11);

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
  EXPECT_NE(json.find("\"schema\":\"td_hotstuff_reputation_bayes_v3\""),
            std::string::npos);
  EXPECT_NE(json.find("\"vote_score\":100"), std::string::npos);
  EXPECT_NE(json.find("\"next_weight_root\":\""), std::string::npos);
}

TEST(VoteScoreReputationPluginTest, CandidateCarriesCertifiedLeaderProfile) {
  std::vector<ReputationQcEvent> events;
  events.push_back({4, "hash-a", std::string(1, static_cast<char>(0x03))});
  events.push_back({5, "hash-b", std::string(1, static_cast<char>(0x05))});

  VoteScoreCandidate first = ComputeBayesianReputationCandidate(
      1, 3, 1, events, {10, 20, 30}, 2, "old-root",
      /*old_weight_version=*/7, /*activation_view=*/8192);
  VoteScoreCandidate second = first;
  second.validators[2].next_weight = 29;
  RecomputeVoteScoreCandidateRoots(&second);

  EXPECT_EQ(first.leader_weights, std::vector<int64_t>({10, 20, 30}));
  EXPECT_NE(first.leader_weights, first.next_weights);
  EXPECT_EQ(first.leader_weight_root_hex,
            LeaderWeightRootHex(first.leader_weights));
  EXPECT_EQ(first.leader_params_version, 1);
  EXPECT_FALSE(first.leader_randomness_ref.empty());
  EXPECT_EQ(first.leader_weight_root_hex, second.leader_weight_root_hex);
  EXPECT_NE(first.candidate_digest_hex, second.candidate_digest_hex);

  const std::string json = VoteScoreCandidateToJson(first);
  EXPECT_NE(json.find("\"leader_weight_root\":\""), std::string::npos);
  EXPECT_NE(json.find("\"leader_params_version\":1"), std::string::npos);
  EXPECT_NE(json.find("\"leader_weights\":[10,20,30]"), std::string::npos);
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

  EXPECT_EQ(candidate.algorithm, "bayes_v3");
  EXPECT_EQ(candidate.next_weights,
            std::vector<int64_t>({11, 11, 11, 11, 9}));
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
    event.leader_id = view <= 2 ? 1 : 2;
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
}

TEST(VoteScoreReputationPluginTest,
     BayesianV3LeaderCertifiedCountContributesToLeaderScore) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 5; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    event.leader_id = view == 1 ? 1 : 2;
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
     BayesianV3CertifiedLeaderIsNotPenalizedByStaleDefaultSchedule) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 20; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    event.leader_id = view == 1 ? 4 : 2;
    events.push_back(event);
  }

  ReputationRecoveryConfig config;
  config.decay_per_epoch = 10;
  config.max_recovery_per_epoch = 10;
  config.bonus_per_epoch = 0;
  config.min_weight = 1;
  config.max_weight = 100;
  config.penalize_missing_leader = true;

  const VoteScoreCandidate candidate =
      ComputeBayesianReputationCandidateWithConfig(
          /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
          /*current_weights=*/{30, 30, 30, 30}, config,
          /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/0,
          /*activation_view=*/128);

  EXPECT_EQ(candidate.validators[3].leader_certified_count, 1);
  EXPECT_EQ(candidate.validators[3].leader_score, 67);
  EXPECT_EQ(candidate.validators[3].next_weight, 30);
  EXPECT_LT(candidate.validators[0].next_weight, 30);
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
     BayesianV3RewardsAllGoodWindowWithSmallBonus) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 64; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    event.leader_id = view <= 2 ? 1 : 2;
    events.push_back(event);
  }

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      1, 4, 2, events, {30, 30, 30, 30}, 3, "old-root", 0, 128);

  EXPECT_EQ(candidate.algorithm, "bayes_v3");
  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({31, 31, 31, 31}));
  for (const ValidatorVoteScore& validator : candidate.validators) {
    EXPECT_EQ(validator.reputation_score, 100);
    EXPECT_EQ(validator.next_weight, validator.current_weight + 1);
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

  EXPECT_EQ(candidate.validators[0].next_weight, 11);
  EXPECT_EQ(candidate.validators[1].next_weight, 11);
  EXPECT_EQ(candidate.validators[2].next_weight, 11);
  EXPECT_EQ(candidate.validators[3].next_weight, 11);
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

  EXPECT_EQ(weights, std::vector<int64_t>({14, 14, 14, 14, 1}));
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

TEST(VoteScoreReputationPluginTest, AsyncPluginWritesFullAndPartialWindows) {
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
  EXPECT_EQ(CountLines(data), 2);
  EXPECT_NE(data.find("\"window_index\":0"), std::string::npos);
  EXPECT_NE(data.find("\"window_index\":1"), std::string::npos);
}

TEST(VoteScoreReputationPluginTest,
     AsyncPluginRealignsWindowAfterWeightActivation) {
  setenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS", "4", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY", "1", /*overwrite=*/1);
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
                                Bitmap({1, 2, 3, 4}, 4),
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
                                Bitmap({1, 2, 3, 4}, 4),
                                DefaultLeaderForView(view, 4),
                                /*weight_version=*/1, activated_root));
  }
  std::vector<VoteScoreCandidate> second = WaitForCandidates(&plugin, 2);
  plugin.Stop();

  ASSERT_EQ(second.size(), 2);
  EXPECT_EQ(second[0].old_weight_version, 1);
  EXPECT_EQ(second[0].old_weight_root_hex, activated_root);
  EXPECT_EQ(second[0].start_qc_view, 5);
  EXPECT_EQ(second[0].end_qc_view, 8);
  EXPECT_EQ(second[1].old_weight_version, 1);
  EXPECT_EQ(second[1].old_weight_root_hex, activated_root);
  EXPECT_EQ(second[1].start_qc_view, 9);
  EXPECT_EQ(second[1].end_qc_view, 12);

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
