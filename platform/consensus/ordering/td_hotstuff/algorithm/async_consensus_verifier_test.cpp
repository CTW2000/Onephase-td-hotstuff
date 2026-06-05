#include "platform/consensus/ordering/td_hotstuff/algorithm/async_consensus_verifier.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
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

TimeoutCert TimeoutCertFor(int view, const QC& high_qc) {
  TimeoutCert cert;
  cert.set_view(view);
  cert.set_quorum_rule_id(kTimeoutQuorumRuleId);
  *cert.mutable_high_qc() = high_qc;
  cert.set_signer_bitmap(BuildSignerBitmap({1, 2, 3}, 4));
  *cert.add_votes() = TimeoutVoteFor(view, 1, high_qc);
  *cert.add_votes() = TimeoutVoteFor(view, 2, high_qc);
  *cert.add_votes() = TimeoutVoteFor(view, 3, high_qc);
  return cert;
}

std::vector<VerifiedConsensusEvent> WaitForEvents(AsyncConsensusVerifier* verifier,
                                                  size_t count) {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(2);
  std::vector<VerifiedConsensusEvent> collected;
  while (std::chrono::steady_clock::now() < deadline) {
    std::vector<VerifiedConsensusEvent> events = verifier->DrainVerified();
    for (auto& event : events) {
      collected.push_back(std::move(event));
    }
    if (collected.size() >= count) {
      return collected;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return collected;
}

TEST(AsyncConsensusVerifierTest, VerifiedVotePreservesViewHashAndSigner) {
  MockSignatureVerifier signature_verifier;
  EXPECT_CALL(signature_verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  AsyncConsensusVerifier verifier(/*total_replicas=*/4, &signature_verifier,
                                  /*worker_count=*/1,
                                  /*queue_capacity=*/8);
  verifier.Start();
  ASSERT_TRUE(verifier.SubmitVote(std::make_unique<Certificate>(
      VoteFor(/*view=*/9, /*signer=*/2, "hash-a", /*slot=*/3))));

  std::vector<VerifiedConsensusEvent> events = WaitForEvents(&verifier, 1);
  ASSERT_EQ(events.size(), 1);
  VerifiedConsensusEvent event = std::move(events.front());
  verifier.Stop();

  ASSERT_EQ(event.type, VerifiedConsensusEvent::Type::kVote);
  ASSERT_NE(event.vote, nullptr);
  EXPECT_EQ(event.view, 9);
  EXPECT_EQ(event.hash, "hash-a");
  EXPECT_EQ(event.signer, 2);
  EXPECT_EQ(event.vote->slot(), 3);
  EXPECT_TRUE(event.error.empty());
}

TEST(AsyncConsensusVerifierTest, WaitForVerifiedReturnsWhenVoteIsReady) {
  MockSignatureVerifier signature_verifier;
  EXPECT_CALL(signature_verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  AsyncConsensusVerifier verifier(/*total_replicas=*/4, &signature_verifier,
                                  /*worker_count=*/1,
                                  /*queue_capacity=*/8);
  verifier.Start();
  ASSERT_TRUE(verifier.SubmitVote(std::make_unique<Certificate>(
      VoteFor(/*view=*/10, /*signer=*/3, "hash-wait"))));

  std::vector<VerifiedConsensusEvent> events =
      verifier.WaitForVerified(std::chrono::seconds(2));
  verifier.Stop();
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events.front().hash, "hash-wait");
}

TEST(AsyncConsensusVerifierTest, InvalidVoteDoesNotReachVerifiedQueue) {
  MockSignatureVerifier signature_verifier;
  EXPECT_CALL(signature_verifier, VerifyMessage(_, _)).WillRepeatedly(Return(false));

  AsyncConsensusVerifier verifier(/*total_replicas=*/4, &signature_verifier,
                                  /*worker_count=*/1,
                                  /*queue_capacity=*/8);
  verifier.Start();
  ASSERT_TRUE(verifier.SubmitVote(std::make_unique<Certificate>(
      VoteFor(/*view=*/9, /*signer=*/2, "hash-a"))));

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  std::vector<VerifiedConsensusEvent> events = verifier.DrainVerified();
  verifier.Stop();

  EXPECT_TRUE(events.empty());
  EXPECT_EQ(verifier.invalid_count(), 1);
}

TEST(AsyncConsensusVerifierTest, VerifiedTimeoutArtifactsKeepOriginalView) {
  MockSignatureVerifier signature_verifier;
  EXPECT_CALL(signature_verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  AsyncConsensusVerifier verifier(/*total_replicas=*/4, &signature_verifier,
                                  /*worker_count=*/1,
                                  /*queue_capacity=*/8);
  verifier.Start();
  QC high_qc = QcFor({1, 2, 3}, /*view=*/7, "high-qc");
  ASSERT_TRUE(verifier.SubmitTimeoutVote(
      std::make_unique<TimeoutVote>(TimeoutVoteFor(/*view=*/11, 3, high_qc))));
  ASSERT_TRUE(verifier.SubmitTimeoutCert(
      std::make_unique<TimeoutCert>(TimeoutCertFor(/*view=*/11, high_qc))));

  std::vector<VerifiedConsensusEvent> events = WaitForEvents(&verifier, 2);
  ASSERT_EQ(events.size(), 2);
  VerifiedConsensusEvent first = std::move(events[0]);
  VerifiedConsensusEvent second = std::move(events[1]);
  verifier.Stop();

  EXPECT_EQ(first.view, 11);
  EXPECT_EQ(second.view, 11);
  EXPECT_NE(first.type, second.type);
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
