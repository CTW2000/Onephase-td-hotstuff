#include "platform/consensus/ordering/td_hotstuff/algorithm/certificate_verifier.h"

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
using ::testing::StrEq;

SignatureInfo SignatureFrom(int signer) {
  SignatureInfo signature;
  signature.set_node_id(signer);
  signature.set_signature("signature-" + std::to_string(signer));
  return signature;
}

Certificate VoteFor(int view, int signer, const std::string& hash,
                    int slot = 0) {
  Certificate vote;
  vote.set_view(view);
  vote.set_signer(signer);
  vote.set_hash(hash);
  vote.set_slot(slot);
  *vote.mutable_sign() = SignatureFrom(signer);
  return vote;
}

QC QcFor(const std::vector<int>& signers, int view = 7,
         const std::string& hash = "proposal-hash", int total = 4) {
  QC qc;
  qc.set_view(view);
  qc.set_hash(hash);
  qc.set_slot(0);
  qc.set_signer_bitmap(BuildSignerBitmap(signers, total));
  for (int signer : signers) {
    *qc.add_signatures() = SignatureFrom(signer);
  }
  return qc;
}

TimeoutVote TimeoutVoteFor(int view, int signer, const QC& high_qc) {
  TimeoutVote vote;
  vote.set_view(view);
  vote.set_signer(signer);
  *vote.mutable_high_qc() = high_qc;
  *vote.mutable_signature() = SignatureFrom(signer);
  return vote;
}

class TestProposalManager : public ProposalManager {
 public:
  TestProposalManager(int32_t id, SignatureVerifier* verifier)
      : ProposalManager(id, /*limit_count=*/0, verifier, /*total_num=*/4,
                        /*non_responsive_num=*/0, /*fork_tail_num=*/0) {}

  std::string HashForTesting(const Proposal& proposal) {
    return GetHash(proposal);
  }
};

Proposal ProposalWithQc(const QC& qc, TestProposalManager* manager) {
  Proposal proposal;
  proposal.set_sender(4);
  proposal.mutable_header()->set_view(7);
  proposal.mutable_header()->set_slot(0);
  *proposal.mutable_header()->mutable_qc() = qc;
  proposal.set_hash(manager->HashForTesting(proposal));
  *proposal.mutable_signature() = SignatureFrom(4);
  return proposal;
}

TEST(CertificateVerifierTest, VotePayloadBindsSignerViewSlotAndHash) {
  const Certificate base = VoteFor(/*view=*/7, /*signer=*/2, "hash-a", 0);
  Certificate changed_signer = base;
  changed_signer.set_signer(3);
  Certificate changed_view = base;
  changed_view.set_view(8);
  Certificate changed_slot = base;
  changed_slot.set_slot(1);
  Certificate changed_hash = base;
  changed_hash.set_hash("hash-b");

  EXPECT_NE(VoteSignaturePayload(base), VoteSignaturePayload(changed_signer));
  EXPECT_NE(VoteSignaturePayload(base), VoteSignaturePayload(changed_view));
  EXPECT_NE(VoteSignaturePayload(base), VoteSignaturePayload(changed_slot));
  EXPECT_NE(VoteSignaturePayload(base), VoteSignaturePayload(changed_hash));
}

TEST(CertificateVerifierTest, ProposalSignaturePayloadBindsSenderAndHash) {
  Proposal proposal;
  proposal.set_sender(2);
  proposal.set_hash("proposal-hash");
  proposal.mutable_header()->set_view(1);

  Proposal changed_sender = proposal;
  changed_sender.set_sender(3);
  Proposal changed_hash = proposal;
  changed_hash.set_hash("other-hash");
  Proposal changed_signature = proposal;
  *changed_signature.mutable_signature() = SignatureFrom(2);

  EXPECT_NE(ProposalSignaturePayload(proposal),
            ProposalSignaturePayload(changed_sender));
  EXPECT_NE(ProposalSignaturePayload(proposal),
            ProposalSignaturePayload(changed_hash));
  EXPECT_EQ(ProposalSignaturePayload(proposal),
            ProposalSignaturePayload(changed_signature));
}

TEST(CertificateVerifierTest, VerifiesQcWithCanonicalVotePayload) {
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(StrEq(VoteSignaturePayload(VoteFor(7, 1, "proposal-hash"))), _))
      .WillOnce(Return(true));
  EXPECT_CALL(verifier, VerifyMessage(StrEq(VoteSignaturePayload(VoteFor(7, 2, "proposal-hash"))), _))
      .WillOnce(Return(true));
  EXPECT_CALL(verifier, VerifyMessage(StrEq(VoteSignaturePayload(VoteFor(7, 3, "proposal-hash"))), _))
      .WillOnce(Return(true));

  CertificateVerifier certificate_verifier(/*total_replicas=*/4, &verifier);
  EXPECT_TRUE(certificate_verifier.VerifyQC(QcFor({1, 2, 3})));
}

TEST(CertificateVerifierTest, RejectsQcWithDuplicateSigner) {
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  QC qc = QcFor({1, 2, 3});
  *qc.add_signatures() = SignatureFrom(1);

  CertificateVerifier certificate_verifier(/*total_replicas=*/4, &verifier);
  EXPECT_FALSE(certificate_verifier.VerifyQC(qc));
}

TEST(CertificateVerifierTest,
     ProposalValidationReportsForgedQcSignerBitmap) {
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  TestProposalManager manager(/*id=*/1, &verifier);

  QC qc = QcFor({1, 2, 3}, /*view=*/6, "proposal-hash");
  qc.set_signer_bitmap(BuildSignerBitmap({1, 2, 4}, 4));
  Proposal proposal = ProposalWithQc(qc, &manager);

  const ProposalValidationResult result = manager.ValidateProposal(proposal);

  EXPECT_FALSE(result.valid);
  EXPECT_TRUE(result.proposal_hash_verified);
  EXPECT_TRUE(result.proposal_signature_verified);
  EXPECT_TRUE(result.leader_verified);
  EXPECT_TRUE(result.leader_context_verified);
  EXPECT_TRUE(result.qc_present);
  EXPECT_FALSE(result.qc_verified);
  EXPECT_EQ(result.error_code, ProposalValidationErrorCode::kInvalidQc);
  EXPECT_EQ(result.error_message, "qc signer bitmap mismatch");
}

TEST(CertificateVerifierTest, TimeoutCertVerifiesHighQcThroughSameQcVerifier) {
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  TimeoutCert cert;
  cert.set_view(9);
  cert.set_quorum_rule_id(kTimeoutQuorumRuleId);
  *cert.mutable_high_qc() = QcFor({1, 2, 3}, /*view=*/7,
                                  "high-qc-hash");
  cert.set_signer_bitmap(BuildSignerBitmap({1, 2, 3}, 4));
  *cert.add_votes() = TimeoutVoteFor(9, 1, cert.high_qc());
  *cert.add_votes() = TimeoutVoteFor(9, 2, cert.high_qc());
  *cert.add_votes() = TimeoutVoteFor(9, 3, cert.high_qc());

  CertificateVerifier certificate_verifier(/*total_replicas=*/4, &verifier);
  EXPECT_TRUE(certificate_verifier.VerifyTimeoutCert(cert));
}

TEST(CertificateVerifierTest, MissingProposalSignatureIsInvalid) {
  MockSignatureVerifier verifier;
  CertificateVerifier certificate_verifier(/*total_replicas=*/4, &verifier);

  Proposal proposal;
  proposal.set_sender(2);
  proposal.set_hash("proposal-hash");
  proposal.mutable_header()->set_view(1);

  EXPECT_FALSE(certificate_verifier.VerifyProposalSignature(proposal));
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
