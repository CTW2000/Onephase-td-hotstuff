#include "platform/consensus/ordering/td_hotstuff/algorithm/safety_rules.h"

#include <gtest/gtest.h>

#include <string>

#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"

namespace resdb {
namespace td_hotstuff {
namespace {

Proposal ProposalFor(int view, const std::string& hash, int qc_view = 0) {
  Proposal proposal;
  proposal.set_sender(DefaultLeaderForView(view, 4));
  proposal.set_hash(hash);
  proposal.mutable_header()->set_view(view);
  proposal.mutable_header()->mutable_qc()->set_view(qc_view);
  proposal.mutable_header()->mutable_qc()->set_hash("qc-" + std::to_string(qc_view));
  return proposal;
}

void AddTimeoutCertForProposalQc(Proposal* proposal, int timeout_view) {
  ASSERT_NE(proposal, nullptr);
  TimeoutCert* cert = proposal->mutable_header()->mutable_timeout_cert();
  cert->set_view(timeout_view);
  cert->set_quorum_rule_id(kTimeoutQuorumRuleId);
  *cert->mutable_high_qc() = proposal->header().qc();
}

TEST(SafetyRulesTest, RejectsSecondVoteForSameView) {
  SafetyRules safety;
  std::string error;

  EXPECT_TRUE(safety.RecordVote(ProposalFor(3, "proposal-a", 2), &error));
  EXPECT_FALSE(safety.RecordVote(ProposalFor(3, "proposal-b", 2), &error));
  EXPECT_NE(error.find("already voted"), std::string::npos);
}

TEST(SafetyRulesTest, RejectsVoteBelowLock) {
  SafetyRules safety;
  std::string error;
  QC lock_qc;
  lock_qc.set_view(5);
  lock_qc.set_hash("locked");
  safety.ObserveLock(lock_qc);

  EXPECT_FALSE(safety.RecordVote(ProposalFor(6, "proposal", 4), &error));
  EXPECT_NE(error.find("lock"), std::string::npos);
}

TEST(SafetyRulesTest, AllowsHigherQcToMovePastLock) {
  SafetyRules safety;
  std::string error;
  QC lock_qc;
  lock_qc.set_view(5);
  lock_qc.set_hash("locked");
  safety.ObserveLock(lock_qc);

  EXPECT_TRUE(safety.RecordVote(ProposalFor(7, "proposal", 6), &error));
  EXPECT_EQ(safety.last_voted_view(), 7);
}

TEST(SafetyRulesTest, AllowsTimeoutBackedProposalAtLockedQc) {
  SafetyRules safety;
  std::string error;
  QC lock_qc;
  lock_qc.set_view(5);
  lock_qc.set_hash("qc-5");
  safety.ObserveLock(lock_qc);

  Proposal proposal = ProposalFor(8, "proposal", 5);
  AddTimeoutCertForProposalQc(&proposal, /*timeout_view=*/7);

  EXPECT_TRUE(safety.RecordVote(proposal, &error)) << error;
  EXPECT_EQ(safety.last_voted_view(), 8);
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
