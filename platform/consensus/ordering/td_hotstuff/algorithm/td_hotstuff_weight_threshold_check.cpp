#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/crypto/mock_signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"

namespace resdb {
namespace td_hotstuff {
namespace {

using ::testing::_;
using ::testing::Return;

class TestProposalManager : public ProposalManager {
 public:
  TestProposalManager(SignatureVerifier* verifier,
                      const std::vector<int64_t>& replica_weights = {})
      : ProposalManager(1, 3, verifier, 4, 0, 0, replica_weights) {}

  TestProposalManager(int node_id, SignatureVerifier* verifier,
                      std::shared_ptr<LeaderSelectionSchedule> leader_schedule)
      : ProposalManager(node_id, 3, verifier, 4, 0, 0,
                        /*replica_weights=*/{}, /*quorum_weight=*/0,
                        /*weight_schedule=*/nullptr, leader_schedule) {}

  using ProposalManager::GetHash;
  using ProposalManager::VerifyQC;
};

QC MakeQC(const std::vector<int>& signers, int total_num) {
  QC qc;
  qc.set_hash("block-hash");
  qc.set_view(2);
  qc.set_signer_bitmap(BuildSignerBitmap(signers, total_num));
  for (int signer : signers) {
    SignatureInfo* signature = qc.add_signatures();
    signature->set_node_id(signer);
    signature->set_signature("signature");
  }
  return qc;
}

TimeoutVote MakeTimeoutVote(int view, int signer, const QC& high_qc) {
  TimeoutVote vote;
  vote.set_view(view);
  vote.set_signer(signer);
  *vote.mutable_high_qc() = high_qc;
  vote.mutable_signature()->set_node_id(signer);
  vote.mutable_signature()->set_signature("timeout-signature");
  return vote;
}

TimeoutCert MakeTimeoutCert(int view, const std::vector<int>& signers,
                            const QC& high_qc) {
  TimeoutCert cert;
  cert.set_view(view);
  *cert.mutable_high_qc() = high_qc;
  cert.set_signer_bitmap(BuildSignerBitmap(signers, 4));
  cert.set_quorum_rule_id(kTimeoutQuorumRuleId);
  for (int signer : signers) {
    *cert.add_votes() = MakeTimeoutVote(view, signer, high_qc);
  }
  return cert;
}


TEST(TdHotstuffWeightedQCTest, DefaultWeightsPreserveClassicQuorum) {
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage("block-hash", _))
      .Times(5)
      .WillRepeatedly(Return(true));

  TestProposalManager manager(&verifier);

  EXPECT_TRUE(manager.VerifyQC(MakeQC({1, 2, 3}, 4)));
  EXPECT_FALSE(manager.VerifyQC(MakeQC({1, 2}, 4)));
}

TEST(TdHotstuffWeightedQCTest, AcceptsWeightedQuorumBelowClassicCount) {
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage("block-hash", _))
      .Times(2)
      .WillRepeatedly(Return(true));

  TestProposalManager manager(&verifier, {3, 3, 1, 1});

  EXPECT_TRUE(manager.VerifyQC(MakeQC({1, 2}, 4)));
}

TEST(TdHotstuffWeightedQCTest, RejectsClassicCountWithoutEnoughWeight) {
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage("block-hash", _))
      .Times(3)
      .WillRepeatedly(Return(true));

  TestProposalManager manager(&verifier, {1, 1, 1, 4});

  EXPECT_FALSE(manager.VerifyQC(MakeQC({1, 2, 3}, 4)));
}


TEST(TdHotstuffWeightedQCTest, RejectsCertificateWithMismatchedSigner) {
  MockSignatureVerifier verifier;
  TestProposalManager manager(&verifier, {3, 3, 1, 1});

  Certificate cert;
  cert.set_hash("block-hash");
  cert.set_signer(2);
  cert.mutable_sign()->set_node_id(1);
  cert.mutable_sign()->set_signature("signature");

  EXPECT_FALSE(manager.VerifyCert(cert));
}

TEST(TdHotstuffWeightedQCTest, RejectsBitmapThatDoesNotMatchSignatures) {
  MockSignatureVerifier verifier;

  TestProposalManager manager(&verifier, {3, 3, 1, 1});
  QC qc = MakeQC({1, 2}, 4);
  qc.set_signer_bitmap(BuildSignerBitmap({1, 3}, 4));

  EXPECT_FALSE(manager.VerifyQC(qc));
}

TEST(TdHotstuffTimeoutTest,
     AdvanceToViewByTimeoutMovesToNextViewAndGeneratedProposalCarriesTc) {
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  TestProposalManager manager(/*node_id=*/3, &verifier, nullptr);
  TimeoutCert cert = MakeTimeoutCert(/*view=*/1, {1, 2, 3}, QC());

  EXPECT_TRUE(manager.AdvanceToViewByTimeout(cert));
  EXPECT_EQ(manager.CurrentView(), 2);

  std::vector<std::unique_ptr<Transaction>> txns;
  std::unique_ptr<Proposal> proposal = manager.GenerateProposal(txns);

  ASSERT_TRUE(proposal->header().has_timeout_cert());
  EXPECT_EQ(proposal->header().timeout_cert().view(), 1);
  EXPECT_TRUE(manager.Verify(*proposal));
}

TEST(TdHotstuffTimeoutTest, RejectsProposalWithInvalidTimeoutCert) {
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  TestProposalManager manager(/*node_id=*/3, &verifier, nullptr);
  TimeoutCert cert = MakeTimeoutCert(/*view=*/1, {1, 2, 3}, QC());
  ASSERT_TRUE(manager.AdvanceToViewByTimeout(cert));

  std::vector<std::unique_ptr<Transaction>> txns;
  std::unique_ptr<Proposal> proposal = manager.GenerateProposal(txns);
  proposal->mutable_header()->mutable_timeout_cert()->set_signer_bitmap(
      BuildSignerBitmap({1, 3, 4}, 4));
  proposal->set_hash(manager.GetHash(*proposal));

  EXPECT_FALSE(manager.Verify(*proposal));
}

TEST(TdHotstuffLeaderSelectionTest,
     RejectsWrongLeaderSenderAndContextHashWhenEnabled) {
  LeaderSelectionConfig config;
  config.enabled = true;
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      /*total_replicas=*/4, std::vector<int64_t>{1, 1, 100, 1}, config);
  const int expected_leader = leader_schedule->LeaderForView(1);

  MockSignatureVerifier verifier;
  TestProposalManager manager(expected_leader, &verifier, leader_schedule);
  std::vector<std::unique_ptr<Transaction>> txns;
  std::unique_ptr<Proposal> proposal = manager.GenerateProposal(txns);

  ASSERT_EQ(proposal->header().view(), 1);
  ASSERT_EQ(proposal->sender(), expected_leader);
  ASSERT_EQ(proposal->header().leader_context_hash(),
            leader_schedule->ContextHashForView(1));
  EXPECT_TRUE(manager.Verify(*proposal));

  Proposal wrong_sender = *proposal;
  wrong_sender.set_sender(expected_leader == 1 ? 2 : 1);
  EXPECT_FALSE(manager.Verify(wrong_sender));

  Proposal wrong_context = *proposal;
  wrong_context.mutable_header()->set_leader_context_hash("wrong-context");
  wrong_context.set_hash(manager.GetHash(wrong_context));
  EXPECT_FALSE(manager.Verify(wrong_context));
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
