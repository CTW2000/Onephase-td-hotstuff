#include "platform/consensus/ordering/td_hotstuff/algorithm/timeout_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/crypto/mock_signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"

namespace resdb {
namespace td_hotstuff {
namespace {

using ::testing::_;
using ::testing::Return;

SignatureInfo SignatureFrom(int signer) {
  SignatureInfo signature;
  signature.set_node_id(signer);
  signature.set_signature("signature-" + std::to_string(signer));
  return signature;
}

QC HighQc(int view, const std::string& hash = "qc-hash") {
  QC qc;
  qc.set_view(view);
  qc.set_hash(hash);
  qc.set_signer_bitmap(BuildSignerBitmap({1, 2, 3}, 4));
  *qc.add_signatures() = SignatureFrom(1);
  *qc.add_signatures() = SignatureFrom(2);
  *qc.add_signatures() = SignatureFrom(3);
  return qc;
}

TimeoutVote VoteFor(int view, int signer, const QC& high_qc) {
  TimeoutVote vote;
  vote.set_view(view);
  vote.set_signer(signer);
  *vote.mutable_high_qc() = high_qc;
  *vote.mutable_signature() = SignatureFrom(signer);
  return vote;
}

TEST(TimeoutManagerTest, TimeoutVotePayloadIsDeterministic) {
  TimeoutVote vote = VoteFor(/*view=*/9, /*signer=*/2, HighQc(7));

  EXPECT_EQ(TimeoutVotePayload(vote), TimeoutVotePayload(vote));

  TimeoutVote changed = vote;
  changed.mutable_high_qc()->set_hash("different-qc");
  EXPECT_NE(TimeoutVotePayload(vote), TimeoutVotePayload(changed));
}

TEST(TimeoutManagerTest, FormsCertAtWeightedQuorumAndIgnoresDuplicate) {
  auto schedule = std::make_shared<WeightSchedule>(
      /*total_replicas=*/4, std::vector<int64_t>{40, 40, 10, 10});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  TimeoutManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                         schedule);
  const QC high_qc = HighQc(5);

  EXPECT_EQ(manager.AddVote(VoteFor(9, 1, high_qc)), nullptr);
  EXPECT_EQ(manager.AddVote(VoteFor(9, 1, high_qc)), nullptr);

  std::unique_ptr<TimeoutCert> cert = manager.AddVote(VoteFor(9, 2, high_qc));

  ASSERT_NE(cert, nullptr);
  EXPECT_EQ(cert->view(), 9);
  EXPECT_EQ(cert->votes_size(), 2);
  EXPECT_EQ(cert->signer_bitmap(), BuildSignerBitmap({1, 2}, 4));
  EXPECT_EQ(cert->high_qc().view(), 5);
  EXPECT_TRUE(manager.VerifyTimeoutCert(*cert));
}

TEST(TimeoutManagerTest, RejectsTimeoutCertWithWrongBitmap) {
  auto schedule = std::make_shared<WeightSchedule>(
      /*total_replicas=*/4, std::vector<int64_t>{40, 40, 10, 10});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  TimeoutManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                         schedule);
  const QC high_qc = HighQc(5);

  std::unique_ptr<TimeoutCert> cert = manager.AddVote(VoteFor(9, 1, high_qc));
  ASSERT_EQ(cert, nullptr);
  cert = manager.AddVote(VoteFor(9, 2, high_qc));

  ASSERT_NE(cert, nullptr);
  cert->set_signer_bitmap(BuildSignerBitmap({1, 3}, 4));

  std::string error;
  EXPECT_FALSE(manager.VerifyTimeoutCert(*cert, &error));
  EXPECT_NE(error.find("bitmap"), std::string::npos);
}

TEST(TimeoutManagerTest, TimeoutRequiresNoValidProposalForCurrentView) {
  EXPECT_FALSE(ShouldTimeoutView(0, 0));
  EXPECT_TRUE(ShouldTimeoutView(7, 6));
  EXPECT_FALSE(ShouldTimeoutView(7, 7));
  EXPECT_FALSE(ShouldTimeoutView(7, 8));
}

TEST(TimeoutManagerTest, CreatesSignedLocalTimeoutVote) {
  auto schedule = std::make_shared<WeightSchedule>(
      /*total_replicas=*/4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  TimeoutVote expected;
  expected.set_view(11);
  expected.set_signer(3);
  *expected.mutable_high_qc() = HighQc(8);
  EXPECT_CALL(verifier, SignMessage(TimeoutVotePayload(expected)))
      .WillOnce(Return(SignatureFrom(3)));

  TimeoutManager manager(/*node_id=*/3, /*total_replicas=*/4, &verifier,
                         schedule);
  std::unique_ptr<TimeoutVote> vote = manager.CreateTimeoutVote(11, HighQc(8));

  ASSERT_NE(vote, nullptr);
  EXPECT_EQ(vote->view(), 11);
  EXPECT_EQ(vote->signer(), 3);
  EXPECT_EQ(vote->signature().node_id(), 3);
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
