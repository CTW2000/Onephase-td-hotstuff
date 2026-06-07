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
  record.qc_collector_id = 5;
  record.weight_version = 2;
  record.active_weight_root = "root-2";
  record.leader_eligible_min_weight = 11;
  record.qc_hash = std::string("\x01\xAB", 2);
  record.signer_bitmap = std::string("\x0D", 1);

  EXPECT_EQ(SerializeQcEvidenceRecord(record),
            "{\"schema\":\"td_hotstuff_qc_evidence_v1\","
            "\"node_id\":3,\"total_replicas\":4,\"qc_view\":7,"
            "\"leader_id\":4,\"qc_collector_id\":5,\"weight_version\":2,"
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

TEST(QcEvidenceRecorderTest, FormatsSignedProposalArtifactJsonCompactly) {
  QcEvidenceRecord record;
  record.type = ReputationEvidenceType::kSignedProposalArtifact;
  record.node_id = 3;
  record.total_replicas = 4;
  record.protocol_id = "td_hotstuff";
  record.qc_view = 9;
  record.proposal_slot = 2;
  record.leader_id = 2;
  record.weight_version = 4;
  record.active_weight_root = "root-4";
  record.proposal_hash = std::string("\xCA\xFE", 2);
  record.proposal_signature_verified = true;

  const std::string json = SerializeQcEvidenceRecord(record);
  EXPECT_NE(json.find("\"schema\":\"td_hotstuff_signed_proposal_artifact_v1\""),
            std::string::npos);
  EXPECT_NE(json.find("\"protocol_id\":\"td_hotstuff\""), std::string::npos);
  EXPECT_NE(json.find("\"view\":9"), std::string::npos);
  EXPECT_NE(json.find("\"slot\":2"), std::string::npos);
  EXPECT_NE(json.find("\"proposal_hash_hex\":\"cafe\""), std::string::npos);
  EXPECT_NE(json.find("\"signature_verified\":true"), std::string::npos);
  EXPECT_EQ(json.find("transaction"), std::string::npos);
}

TEST(QcEvidenceRecorderTest, FormatsV2StrongFaultArtifactJsonCompactly) {
  QcEvidenceRecord weight_vote;
  weight_vote.type = ReputationEvidenceType::kSignedWeightUpdateVoteArtifact;
  weight_vote.node_id = 3;
  weight_vote.total_replicas = 4;
  weight_vote.protocol_id = "td_hotstuff";
  weight_vote.vote_signer_id = 2;
  weight_vote.old_weight_root = "old-root";
  weight_vote.old_weight_version = 7;
  weight_vote.activation_view = 64;
  weight_vote.candidate_digest = std::string("\x01\x02", 2);
  weight_vote.vote_signature_verified = true;
  EXPECT_NE(SerializeQcEvidenceRecord(weight_vote)
                .find("\"schema\":\"td_hotstuff_weight_update_vote_artifact_v1\""),
            std::string::npos);
  EXPECT_NE(SerializeQcEvidenceRecord(weight_vote)
                .find("\"candidate_digest_hex\":\"0102\""),
            std::string::npos);

  QcEvidenceRecord timeout_vote;
  timeout_vote.type = ReputationEvidenceType::kSignedTimeoutVoteArtifact;
  timeout_vote.node_id = 3;
  timeout_vote.total_replicas = 4;
  timeout_vote.protocol_id = "td_hotstuff";
  timeout_vote.qc_view = 9;
  timeout_vote.vote_signer_id = 2;
  timeout_vote.high_qc_digest = std::string("\x03\x04", 2);
  timeout_vote.vote_signature_verified = true;
  EXPECT_NE(SerializeQcEvidenceRecord(timeout_vote)
                .find("\"schema\":\"td_hotstuff_timeout_vote_artifact_v1\""),
            std::string::npos);
  EXPECT_NE(SerializeQcEvidenceRecord(timeout_vote)
                .find("\"high_qc_digest_hex\":\"0304\""),
            std::string::npos);

  QcEvidenceRecord invalid_tc;
  invalid_tc.type = ReputationEvidenceType::kInvalidTcProposalArtifact;
  invalid_tc.node_id = 3;
  invalid_tc.total_replicas = 4;
  invalid_tc.protocol_id = "td_hotstuff";
  invalid_tc.qc_view = 10;
  invalid_tc.leader_id = 4;
  invalid_tc.proposal_hash = std::string("\xCA\xFE", 2);
  invalid_tc.proposal_signature_verified = true;
  invalid_tc.timeout_cert_verified = false;
  invalid_tc.invalid_reason = "bad_tc";
  EXPECT_NE(SerializeQcEvidenceRecord(invalid_tc)
                .find("\"schema\":\"td_hotstuff_invalid_tc_proposal_artifact_v1\""),
            std::string::npos);
  EXPECT_NE(SerializeQcEvidenceRecord(invalid_tc)
                .find("\"timeout_cert_verified\":false"),
            std::string::npos);

  QcEvidenceRecord verified_qc;
  verified_qc.type = ReputationEvidenceType::kVerifiedQcArtifact;
  verified_qc.node_id = 3;
  verified_qc.total_replicas = 4;
  verified_qc.protocol_id = "td_hotstuff";
  verified_qc.qc_view = 11;
  verified_qc.qc_hash = std::string("\xBA\xAD", 2);
  verified_qc.signer_bitmap = std::string("\x0f", 1);
  verified_qc.qc_verified = true;
  EXPECT_NE(SerializeQcEvidenceRecord(verified_qc)
                .find("\"schema\":\"td_hotstuff_verified_qc_artifact_v1\""),
            std::string::npos);
  EXPECT_NE(SerializeQcEvidenceRecord(verified_qc)
                .find("\"signer_bitmap_hex\":\"0f\""),
            std::string::npos);
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
  EXPECT_NE(data.find("\"schema\":\"td_hotstuff_reputation_bayes_v4\""),
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
