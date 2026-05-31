#include "platform/consensus/ordering/td_hotstuff/algorithm/vote_score_reputation_plugin.h"

#include <cstdlib>
#include <fstream>
#include <initializer_list>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

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

TEST(VoteScoreReputationPluginTest, DecodesTdHotstuffSignerBitmap) {
  const std::string bitmap(1, static_cast<char>(0x09));

  EXPECT_EQ(DecodeSignerBitmap(bitmap, 5), std::vector<int>({1, 4}));
}

TEST(VoteScoreReputationPluginTest, ComputesVoteScoresWithoutAbsencePenalty) {
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

  EXPECT_EQ(candidate.validators[0].opportunities, 3);
  EXPECT_EQ(candidate.validators[0].inclusions, 2);
  EXPECT_EQ(candidate.validators[0].vote_score, 60);
  EXPECT_EQ(candidate.validators[0].next_weight, 10);

  EXPECT_EQ(candidate.validators[1].inclusions, 3);
  EXPECT_EQ(candidate.validators[1].vote_score, 80);
  EXPECT_EQ(candidate.validators[1].next_weight, 10);

  EXPECT_EQ(candidate.validators[2].inclusions, 2);
  EXPECT_EQ(candidate.validators[2].vote_score, 60);
  EXPECT_EQ(candidate.validators[2].next_weight, 10);

  EXPECT_EQ(candidate.validators[3].inclusions, 1);
  EXPECT_EQ(candidate.validators[3].vote_score, 40);
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
  EXPECT_NE(json.find("\"schema\":\"td_hotstuff_reputation_bayes_v2\""),
            std::string::npos);
  EXPECT_NE(json.find("\"vote_score\":75"), std::string::npos);
  EXPECT_NE(json.find("\"next_weight_root\":\""), std::string::npos);
}

TEST(VoteScoreReputationPluginTest,
     BayesianV2DoesNotLowerValidatorsMissingFromQc) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 4; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2}, 4);
    event.leader_id = view % 4 + 1;
    events.push_back(event);
  }

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/1, events,
      /*current_weights=*/{10, 10, 10, 10}, /*max_delta=*/1,
      /*old_weight_root_hex=*/"old-root", /*old_weight_version=*/2,
      /*activation_view=*/8);

  EXPECT_EQ(candidate.algorithm, "bayes_v2");
  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({10, 10, 10, 10}));
  EXPECT_EQ(candidate.validators[2].inclusions, 0);
  EXPECT_EQ(candidate.validators[2].next_weight, 10);
  EXPECT_EQ(candidate.validators[3].inclusions, 0);
  EXPECT_EQ(candidate.validators[3].next_weight, 10);
}

TEST(VoteScoreReputationPluginTest, BayesianV2KeepsAllGoodWindowStable) {
  std::vector<ReputationQcEvent> events;
  for (int view = 1; view <= 4; ++view) {
    ReputationQcEvent event;
    event.qc_view = view;
    event.qc_hash = "hash-" + std::to_string(view);
    event.signer_bitmap = Bitmap({1, 2, 3, 4}, 4);
    event.leader_id = view % 4 + 1;
    events.push_back(event);
  }

  const VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      2, 4, 1, events, {15, 15, 10, 10}, 1, "old-root", 3, 8);

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({15, 15, 10, 10}));
  for (const ValidatorVoteScore& validator : candidate.validators) {
    EXPECT_EQ(validator.leader_gap_count, 0);
    EXPECT_EQ(validator.next_weight, validator.current_weight);
  }
}

TEST(VoteScoreReputationPluginTest,
     BayesianV2SignerDiversityDropsForRepeatedNarrowSignerGroup) {
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
  EXPECT_EQ(narrow.next_weights, std::vector<int64_t>({10, 10, 10, 10}));
}

TEST(VoteScoreReputationPluginTest,
     BayesianV2ViewGapsLowerOnlyResponsibleLeaders) {
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

  EXPECT_EQ(candidate.next_weights, std::vector<int64_t>({10, 10, 9, 9}));
  EXPECT_EQ(candidate.validators[0].leader_gap_count, 0);
  EXPECT_EQ(candidate.validators[1].leader_gap_count, 0);
  EXPECT_EQ(candidate.validators[2].leader_gap_count, 1);
  EXPECT_EQ(candidate.validators[3].leader_gap_count, 1);
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
  EXPECT_EQ(first.next_weights, std::vector<int64_t>({10, 20, 30}));

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

TEST(VoteScoreReputationPluginTest, CreateFromEnvReturnsNullWhenDisabled) {
  unsetenv("TD_HS_REPUTATION_ENABLE");

  EXPECT_EQ(AsyncVoteScoreReputationPlugin::CreateFromEnv(
                /*node_id=*/1, /*total_replicas=*/4, /*current_weights=*/{1, 1, 1, 1}),
            nullptr);
}

TEST(VoteScoreReputationPluginTest, CreateFromEnvIgnoresDeprecatedAlgorithmEnv) {
  const std::string output_dir = TempDir("td_hotstuff_reputation_bayes_env_test");
  const std::string output_file =
      output_dir + "/td_hotstuff_reputation_node_2.jsonl";
  std::remove(output_file.c_str());

  setenv("TD_HS_REPUTATION_ENABLE", "1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_ALGORITHM", "vote_score_v1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_WINDOW_SIZE", "1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_OUTPUT_DIR", output_dir.c_str(), /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_QUEUE_CAPACITY", "8", /*overwrite=*/1);

  std::unique_ptr<AsyncVoteScoreReputationPlugin> plugin =
      AsyncVoteScoreReputationPlugin::CreateFromEnv(
          /*node_id=*/2, /*total_replicas=*/4,
          /*current_weights=*/{10, 10, 10, 10});
  ASSERT_NE(plugin, nullptr);

  EXPECT_TRUE(plugin->RecordQc(1, "hash-a", Bitmap({1, 2}, 4),
                               /*leader_id=*/2, /*weight_version=*/0,
                               /*active_weight_root=*/"root"));
  plugin->Stop();

  const std::string data = ReadFile(output_file);
  EXPECT_NE(data.find("\"schema\":\"td_hotstuff_reputation_bayes_v2\""),
            std::string::npos);
  EXPECT_NE(data.find("\"algorithm\":\"bayes_v2\""),
            std::string::npos);
  EXPECT_NE(data.find("\"next_weights\":[10,10,10,10]"),
            std::string::npos);

  unsetenv("TD_HS_REPUTATION_ENABLE");
  unsetenv("TD_HS_REPUTATION_ALGORITHM");
  unsetenv("TD_HS_REPUTATION_WINDOW_SIZE");
  unsetenv("TD_HS_REPUTATION_OUTPUT_DIR");
  unsetenv("TD_HS_REPUTATION_QUEUE_CAPACITY");
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
