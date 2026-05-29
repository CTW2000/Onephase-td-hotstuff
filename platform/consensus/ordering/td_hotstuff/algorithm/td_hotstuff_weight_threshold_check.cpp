#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/crypto/mock_signature_verifier.h"

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

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
