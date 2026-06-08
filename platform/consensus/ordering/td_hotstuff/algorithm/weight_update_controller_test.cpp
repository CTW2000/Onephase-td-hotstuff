#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_controller.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <memory>
#include <vector>

#include "common/crypto/mock_signature_verifier.h"
#include "platform/consensus/reputation/reputation_roots.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff_digest.h"

namespace resdb {
namespace td_hotstuff {
namespace {

using ::testing::_;
using ::testing::Return;

SignatureInfo SignatureFor(int signer) {
  SignatureInfo signature;
  signature.set_node_id(signer);
  signature.set_signature("sig-" + std::to_string(signer));
  return signature;
}

resdb::consensus::reputation::ReputationCandidate Candidate() {
  resdb::consensus::reputation::ReputationCandidate candidate;
  candidate.total_replicas = 4;
  candidate.window_index = 0;
  candidate.start_view = 0;
  candidate.end_view = 4;
  candidate.event_count = 4;
  candidate.old_weight_version = 0;
  candidate.old_weight_root_hex =
      resdb::td_hotstuff::WeightRootHex({1, 1, 1, 1});
  candidate.activation_view = 8;
  for (int i = 1; i <= 4; ++i) {
    resdb::consensus::reputation::ValidatorReputation validator;
    validator.validator_id = i;
    validator.current_weight = 1;
    validator.next_weight = i == 4 ? 1 : 2;
    candidate.validators.push_back(validator);
  }
  resdb::consensus::reputation::RecomputeReputationCandidateRoots(&candidate);
  return candidate;
}


resdb::consensus::reputation::ReputationCandidate NoOpCandidate() {
  auto candidate = Candidate();
  for (auto& validator : candidate.validators) {
    validator.next_weight = 1;
  }
  resdb::consensus::reputation::RecomputeReputationCandidateRoots(&candidate);
  return candidate;
}

CandidateWeightUpdate CandidateMessage(
    const resdb::consensus::reputation::ReputationCandidate& candidate) {
  return ToCandidateWeightUpdate(candidate);
}


TEST(WeightUpdateControllerTest, IgnoresNoOpLocalCandidate) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = NoOpCandidate();

  EXPECT_FALSE(controller.AddLocalCandidate(candidate));
  EXPECT_EQ(controller.HandleCandidate(CandidateMessage(candidate)), nullptr);
}

TEST(WeightUpdateControllerTest, VotesOnlyForMatchingLocalCandidate) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(1)));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = Candidate();
  controller.AddLocalCandidate(candidate);

  std::unique_ptr<WeightUpdateVote> vote = controller.HandleCandidate(
      CandidateMessage(candidate));

  ASSERT_NE(vote, nullptr);
  EXPECT_EQ(vote->signer(), 1);
  EXPECT_EQ(vote->candidate_digest(), candidate.candidate_digest_hex);

  CandidateWeightUpdate wrong = CandidateMessage(candidate);
  wrong.set_candidate_digest("wrong");
  EXPECT_EQ(controller.HandleCandidate(wrong), nullptr);
}

TEST(WeightUpdateControllerTest, FormsCertAfterOldWeightQuorumVotes) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = Candidate();
  CandidateWeightUpdate message = CandidateMessage(candidate);
  controller.AddLocalCandidate(candidate);
  ASSERT_NE(controller.HandleCandidate(message), nullptr);

  std::unique_ptr<WeightUpdateCert> cert;
  for (int signer : {1, 2, 3}) {
    WeightUpdateVote vote;
    vote.set_signer(signer);
    vote.set_old_weight_root(candidate.old_weight_root_hex);
    vote.set_old_weight_version(candidate.old_weight_version);
    vote.set_activation_view(candidate.activation_view);
    vote.set_candidate_digest(candidate.candidate_digest_hex);
    *vote.mutable_signature() = SignatureFor(signer);
    cert = controller.HandleVote(vote);
  }

  ASSERT_NE(cert, nullptr);
  EXPECT_EQ(cert->candidate().candidate_digest(), candidate.candidate_digest_hex);
  EXPECT_FALSE(cert->signer_bitmap().empty());
}

TEST(WeightUpdateControllerTest, ActivatesCertifiedWeightsAtBoundary) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = Candidate();
  WeightUpdateCert cert;
  *cert.mutable_candidate() = CandidateMessage(candidate);
  cert.set_signer_bitmap(std::string(1, static_cast<char>(0x07)));
  cert.set_quorum_rule_id("old_weight_quorum_v1");
  for (int signer : {1, 2, 3}) {
    WeightUpdateVote* vote = cert.add_votes();
    vote->set_signer(signer);
    vote->set_old_weight_root(candidate.old_weight_root_hex);
    vote->set_old_weight_version(candidate.old_weight_version);
    vote->set_activation_view(candidate.activation_view);
    vote->set_candidate_digest(candidate.candidate_digest_hex);
    *vote->mutable_signature() = SignatureFor(signer);
  }

  EXPECT_TRUE(controller.HandleCert(cert));
  EXPECT_FALSE(controller.ActivateReady(/*current_view=*/8));
  EXPECT_TRUE(controller.ActivateReady(/*current_view=*/9));
  EXPECT_EQ(schedule->ActiveWeightVersion(), 1);
  EXPECT_EQ(schedule->ActiveWeights(), candidate.next_weights);
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
