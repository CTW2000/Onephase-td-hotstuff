#include "platform/consensus/ordering/td_hotstuff/algorithm/safety_rules.h"

namespace resdb {
namespace td_hotstuff {
namespace {

void SetError(std::string* error, const std::string& message) {
  if (error != nullptr) {
    *error = message;
  }
}

bool TimeoutJustifiesLockedProposal(const Proposal& proposal,
                                    const QC& lock_qc) {
  if (!proposal.header().has_timeout_cert()) {
    return false;
  }
  const QC& proposal_qc = proposal.header().qc();
  const QC& timeout_high_qc = proposal.header().timeout_cert().high_qc();
  if (proposal_qc.view() != timeout_high_qc.view() ||
      proposal_qc.hash() != timeout_high_qc.hash()) {
    return false;
  }
  if (timeout_high_qc.view() > lock_qc.view()) {
    return true;
  }
  return timeout_high_qc.view() == lock_qc.view() &&
         timeout_high_qc.hash() == lock_qc.hash();
}

}  // namespace

bool SafetyRules::RecordVote(const Proposal& proposal, std::string* error) {
  const int view = proposal.header().view();
  if (view <= 0) {
    SetError(error, "invalid proposal view");
    return false;
  }
  if (view <= last_voted_view_) {
    SetError(error, "already voted in this or a later view");
    return false;
  }
  if (lock_qc_.view() > 0 &&
      proposal.header().qc().view() <= lock_qc_.view() &&
      !TimeoutJustifiesLockedProposal(proposal, lock_qc_)) {
    SetError(error, "proposal does not extend local lock");
    return false;
  }
  last_voted_view_ = view;
  last_voted_hash_ = proposal.hash();
  return true;
}

void SafetyRules::ObserveLock(const QC& qc) {
  if (qc.view() > lock_qc_.view()) {
    lock_qc_ = qc;
  }
}

}  // namespace td_hotstuff
}  // namespace resdb
