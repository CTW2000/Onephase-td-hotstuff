#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff_fault_injector.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/crypto/mock_signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/timeout_manager.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_manager.h"

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

class TestProposalManager : public ProposalManager {
 public:
  TestProposalManager(int node_id, SignatureVerifier* verifier)
      : ProposalManager(node_id, /*limit_count=*/3, verifier,
                        /*total_num=*/4, /*non_responsive_num=*/0,
                        /*fork_tail_num=*/0) {}
};

QC HighQc(int view, const std::string& hash = "qc-hash") {
  QC qc;
  qc.set_view(view);
  qc.set_hash(hash);
  qc.set_slot(0);
  qc.set_signer_bitmap(BuildSignerBitmap({1, 2, 3}, 4));
  *qc.add_signatures() = SignatureFrom(1);
  *qc.add_signatures() = SignatureFrom(2);
  *qc.add_signatures() = SignatureFrom(3);
  return qc;
}

TEST(TdHotstuffFaultInjectorTest,
     BuildsConflictingProposalOutsideProposalManager) {
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_))
      .WillRepeatedly(Return(SignatureFrom(2)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  TestProposalManager manager(/*node_id=*/2, &verifier);
  std::vector<std::unique_ptr<Transaction>> txns;
  std::unique_ptr<Proposal> normal = manager.GenerateProposal(txns);
  ASSERT_NE(normal, nullptr);

  std::unique_ptr<Proposal> conflict =
      BuildConflictingProposalForExperiment(*normal, &verifier);

  ASSERT_NE(conflict, nullptr);
  EXPECT_EQ(conflict->sender(), normal->sender());
  EXPECT_EQ(conflict->header().view(), normal->header().view());
  EXPECT_EQ(conflict->header().slot(), normal->header().slot());
  EXPECT_EQ(conflict->header().prehash(), normal->header().prehash());
  EXPECT_NE(conflict->header().proposal_id(), normal->header().proposal_id());
  EXPECT_NE(conflict->hash(), normal->hash());
  EXPECT_TRUE(manager.Verify(*normal));
  EXPECT_TRUE(manager.Verify(*conflict));
}

TEST(TdHotstuffFaultInjectorTest,
     BuildsConflictingWeightUpdateVoteOutsideManager) {
  MockSignatureVerifier verifier;
  WeightUpdateVote base;
  base.set_candidate_digest("candidate-a");
  base.set_validator_id(3);
  base.set_old_weight_root("old-root");
  base.set_old_weight_version(7);
  base.set_activation_view(128);
  *base.mutable_signature() = SignatureFrom(3);

  WeightUpdateVote expected = base;
  expected.set_candidate_digest("candidate-b");
  EXPECT_CALL(verifier, SignMessage(WeightUpdateVotePayload(expected)))
      .WillOnce(Return(SignatureFrom(3)));

  std::unique_ptr<WeightUpdateVote> conflict =
      BuildConflictingWeightUpdateVoteForExperiment(base, "candidate-b",
                                                    &verifier);

  ASSERT_NE(conflict, nullptr);
  EXPECT_EQ(conflict->validator_id(), base.validator_id());
  EXPECT_EQ(conflict->old_weight_root(), base.old_weight_root());
  EXPECT_EQ(conflict->old_weight_version(), base.old_weight_version());
  EXPECT_EQ(conflict->activation_view(), base.activation_view());
  EXPECT_NE(conflict->candidate_digest(), base.candidate_digest());
}

TEST(TdHotstuffFaultInjectorTest,
     BuildsConflictingTimeoutVoteOutsideTimeoutManager) {
  MockSignatureVerifier verifier;
  TimeoutVote base;
  base.set_view(11);
  base.set_signer(3);
  *base.mutable_high_qc() = HighQc(8);
  *base.mutable_signature() = SignatureFrom(3);

  TimeoutVote expected;
  expected.set_view(11);
  expected.set_signer(3);
  *expected.mutable_signature() = SignatureFrom(3);
  EXPECT_CALL(verifier, SignMessage(TimeoutVotePayload(expected)))
      .WillOnce(Return(SignatureFrom(3)));

  std::unique_ptr<TimeoutVote> conflict =
      BuildConflictingTimeoutVoteForExperiment(base, &verifier);

  ASSERT_NE(conflict, nullptr);
  EXPECT_EQ(conflict->view(), base.view());
  EXPECT_EQ(conflict->signer(), base.signer());
  EXPECT_TRUE(conflict->high_qc().hash().empty());
  EXPECT_NE(TimeoutVotePayload(*conflict), TimeoutVotePayload(base));
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
