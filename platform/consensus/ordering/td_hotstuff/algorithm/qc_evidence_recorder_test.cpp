#include "platform/consensus/ordering/td_hotstuff/algorithm/qc_evidence_recorder.h"

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <iterator>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>

namespace resdb {
namespace td_hotstuff {
namespace {

std::string ReadFile(const std::string& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

std::string TempDir(const std::string& name) {
  return "/tmp/" + name + "_" + std::to_string(getpid());
}

TEST(QcEvidenceRecorderTest, FormatsCompactJsonWithoutSignatures) {
  QcEvidenceRecord record;
  record.node_id = 3;
  record.total_replicas = 4;
  record.qc_view = 7;
  record.leader_id = 4;
  record.weight_version = 2;
  record.active_weight_root = "root-2";
  record.leader_eligible_min_weight = 11;
  record.qc_hash = std::string("\x01\xAB", 2);
  record.signer_bitmap = std::string("\x0D", 1);

  EXPECT_EQ(SerializeQcEvidenceRecord(record),
            "{\"schema\":\"td_hotstuff_qc_evidence_v1\","
            "\"node_id\":3,\"total_replicas\":4,\"qc_view\":7,"
            "\"leader_id\":4,\"weight_version\":2,"
            "\"active_weight_root\":\"root-2\","
            "\"leader_eligible_min_weight\":11,"
            "\"qc_hash_hex\":\"01ab\",\"signer_bitmap_hex\":\"0d\"}");
}

TEST(QcEvidenceRecorderTest, FormatsLeaderOpportunityJson) {
  QcEvidenceRecord record;
  record.node_id = 3;
  record.total_replicas = 4;
  record.qc_view = 9;
  record.leader_id = 2;
  record.leader_opportunity = true;
  record.weight_version = 4;
  record.active_weight_root = "root-4";
  record.leader_eligible_min_weight = 11;

  EXPECT_EQ(SerializeQcEvidenceRecord(record),
            "{\"schema\":\"td_hotstuff_leader_opportunity_evidence_v1\","
            "\"node_id\":3,\"total_replicas\":4,\"view\":9,"
            "\"leader_id\":2,\"weight_version\":4,"
            "\"active_weight_root\":\"root-4\","
            "\"leader_eligible_min_weight\":11}");
}

TEST(QcEvidenceRecorderTest, DisabledRecorderDoesNotWriteFile) {
  const std::string output_dir = "/tmp/td_hotstuff_qc_evidence_disabled_test";
  std::remove((output_dir + "/td_hotstuff_qc_evidence_node_1.jsonl").c_str());
  AsyncQcEvidenceRecorder recorder = AsyncQcEvidenceRecorder::Disabled(1);

  QcEvidenceRecord record;
  record.node_id = 1;
  record.total_replicas = 4;
  record.qc_view = 2;
  record.qc_hash = "hash";
  record.signer_bitmap = "bitmap";

  EXPECT_FALSE(recorder.Enqueue(record));
  recorder.Stop();

  EXPECT_FALSE(
      std::ifstream(output_dir + "/td_hotstuff_qc_evidence_node_1.jsonl")
          .good());
}


TEST(QcEvidenceRecorderTest, CreateFromEnvReturnsNullWhenDisabled) {
  unsetenv("TD_HS_EVIDENCE_ENABLE");
  unsetenv("TD_HS_REPUTATION_ENABLE");
  EXPECT_EQ(AsyncQcEvidenceRecorder::CreateFromEnv(/*node_id=*/1,
                                                   /*total_replicas=*/4,
                                                   /*current_weights=*/{1, 1, 1, 1}),
            nullptr);
}

TEST(QcEvidenceRecorderTest, ReputationOnlyModeDispatchesWithoutEvidenceJson) {
  const std::string output_dir = TempDir("td_hotstuff_reputation_dispatch_test");
  const std::string reputation_file =
      output_dir + "/td_hotstuff_reputation_node_5.jsonl";
  std::remove(reputation_file.c_str());

  unsetenv("TD_HS_EVIDENCE_ENABLE");
  setenv("TD_HS_REPUTATION_ENABLE", "1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_WINDOW_SIZE", "1", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_OUTPUT_DIR", output_dir.c_str(), /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_QUEUE_CAPACITY", "8", /*overwrite=*/1);
  setenv("TD_HS_REPUTATION_MAX_DELTA", "2", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS", "1", /*overwrite=*/1);

  std::unique_ptr<AsyncQcEvidenceRecorder> recorder =
      AsyncQcEvidenceRecorder::CreateFromEnv(
          /*node_id=*/5, /*total_replicas=*/4,
          /*current_weights=*/{10, 10, 10, 10});
  ASSERT_NE(recorder, nullptr);

  EXPECT_TRUE(recorder->RecordQc(8, "hash", std::string(1, static_cast<char>(0x0f))));
  recorder->Stop();

  const std::string data = ReadFile(reputation_file);
  EXPECT_NE(data.find("\"schema\":\"td_hotstuff_reputation_bayes_v3\""),
            std::string::npos);
  EXPECT_NE(data.find("\"event_count\":1"), std::string::npos);

  unsetenv("TD_HS_REPUTATION_ENABLE");
  unsetenv("TD_HS_REPUTATION_WINDOW_SIZE");
  unsetenv("TD_HS_REPUTATION_OUTPUT_DIR");
  unsetenv("TD_HS_REPUTATION_QUEUE_CAPACITY");
  unsetenv("TD_HS_REPUTATION_MAX_DELTA");
  unsetenv("TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS");
}


TEST(QcEvidenceRecorderTest, DropsWhenQueueIsFull) {
  AsyncQcEvidenceRecorder recorder(/*node_id=*/1, /*total_replicas=*/4,
                                   "/tmp/td_hotstuff_qc_evidence_drop_test",
                                   /*queue_capacity=*/1);
  QcEvidenceRecord record;
  record.node_id = 1;
  record.total_replicas = 4;
  record.qc_view = 5;
  record.qc_hash = "hash";
  record.signer_bitmap = "bitmap";

  EXPECT_TRUE(recorder.Enqueue(record));
  EXPECT_FALSE(recorder.Enqueue(record));
  EXPECT_EQ(recorder.dropped_count(), 1);
}

TEST(QcEvidenceRecorderTest, AsyncWriterDrainsQueuedRecords) {
  const std::string output_dir = "/tmp/td_hotstuff_qc_evidence_writer_test";
  std::remove((output_dir + "/td_hotstuff_qc_evidence_node_2.jsonl").c_str());

  AsyncQcEvidenceRecorder recorder(/*node_id=*/2, /*total_replicas=*/4,
                                   output_dir, /*queue_capacity=*/8);
  recorder.Start();

  QcEvidenceRecord first;
  first.node_id = 2;
  first.total_replicas = 4;
  first.qc_view = 3;
  first.qc_hash = std::string("\xAA", 1);
  first.signer_bitmap = std::string("\x07", 1);
  EXPECT_TRUE(recorder.Enqueue(first));

  QcEvidenceRecord second = first;
  second.qc_view = 4;
  second.qc_hash = std::string("\xBB", 1);
  second.signer_bitmap = std::string("\x0F", 1);
  EXPECT_TRUE(recorder.Enqueue(second));

  recorder.Stop();

  const std::string data =
      ReadFile(output_dir + "/td_hotstuff_qc_evidence_node_2.jsonl");
  EXPECT_NE(data.find("\"qc_view\":3"), std::string::npos);
  EXPECT_NE(data.find("\"qc_hash_hex\":\"aa\""), std::string::npos);
  EXPECT_NE(data.find("\"signer_bitmap_hex\":\"07\""), std::string::npos);
  EXPECT_NE(data.find("\"qc_view\":4"), std::string::npos);
  EXPECT_NE(data.find("\"qc_hash_hex\":\"bb\""), std::string::npos);
  EXPECT_NE(data.find("\"signer_bitmap_hex\":\"0f\""), std::string::npos);
  EXPECT_EQ(data.find("signature"), std::string::npos);
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
