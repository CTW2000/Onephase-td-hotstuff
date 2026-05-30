#include "platform/consensus/ordering/td_hotstuff/algorithm/qc_evidence_recorder.h"

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <iterator>
#include <string>

#include <gtest/gtest.h>

namespace resdb {
namespace td_hotstuff {
namespace {

std::string ReadFile(const std::string& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

TEST(QcEvidenceRecorderTest, FormatsCompactJsonWithoutSignatures) {
  QcEvidenceRecord record;
  record.node_id = 3;
  record.total_replicas = 4;
  record.qc_view = 7;
  record.qc_hash = std::string("\x01\xAB", 2);
  record.signer_bitmap = std::string("\x0D", 1);

  EXPECT_EQ(SerializeQcEvidenceRecord(record),
            "{\"schema\":\"td_hotstuff_qc_evidence_v1\","
            "\"node_id\":3,\"total_replicas\":4,\"qc_view\":7,"
            "\"qc_hash_hex\":\"01ab\",\"signer_bitmap_hex\":\"0d\"}");
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
  EXPECT_EQ(AsyncQcEvidenceRecorder::CreateFromEnv(/*node_id=*/1,
                                                   /*total_replicas=*/4),
            nullptr);
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
