#include "platform/consensus/ordering/td_hotstuff/algorithm/vote_score_reputation_plugin.h"

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

namespace resdb {
namespace td_hotstuff {
namespace {

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

TEST(VoteScoreReputationPluginTest, ComputesVoteScoresAndSmallWeightDeltas) {
  std::vector<ReputationQcEvent> events;
  events.push_back({10, "hash-a", std::string(1, static_cast<char>(0x07))});
  events.push_back({11, "hash-b", std::string(1, static_cast<char>(0x03))});
  events.push_back({12, "hash-c", std::string(1, static_cast<char>(0x0e))});

  const VoteScoreCandidate candidate = ComputeVoteScoreCandidate(
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
  EXPECT_EQ(candidate.validators[1].next_weight, 11);

  EXPECT_EQ(candidate.validators[2].inclusions, 2);
  EXPECT_EQ(candidate.validators[2].vote_score, 60);
  EXPECT_EQ(candidate.validators[2].next_weight, 10);

  EXPECT_EQ(candidate.validators[3].inclusions, 1);
  EXPECT_EQ(candidate.validators[3].vote_score, 40);
  EXPECT_EQ(candidate.validators[3].next_weight, 9);
}

TEST(VoteScoreReputationPluginTest, CandidateRootsAreStable) {
  std::vector<ReputationQcEvent> events;
  events.push_back({4, "hash-a", std::string(1, static_cast<char>(0x03))});
  events.push_back({5, "hash-b", std::string(1, static_cast<char>(0x05))});

  const VoteScoreCandidate first = ComputeVoteScoreCandidate(
      1, 3, 0, events, {10, 20, 30}, 2);
  const VoteScoreCandidate second = ComputeVoteScoreCandidate(
      1, 3, 0, events, {10, 20, 30}, 2);

  EXPECT_EQ(first.metric_root_hex, second.metric_root_hex);
  EXPECT_EQ(first.next_weight_root_hex, second.next_weight_root_hex);
  EXPECT_EQ(first.candidate_digest_hex, second.candidate_digest_hex);
  EXPECT_FALSE(first.metric_root_hex.empty());
  EXPECT_FALSE(first.next_weight_root_hex.empty());
  EXPECT_FALSE(first.candidate_digest_hex.empty());

  const std::string json = VoteScoreCandidateToJson(first);
  EXPECT_NE(json.find("\"schema\":\"td_hotstuff_reputation_vote_score_v1\""),
            std::string::npos);
  EXPECT_NE(json.find("\"vote_score\":75"), std::string::npos);
  EXPECT_NE(json.find("\"next_weight_root\":\""), std::string::npos);
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
