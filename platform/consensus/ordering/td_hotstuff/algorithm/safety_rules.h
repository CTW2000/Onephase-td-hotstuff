#pragma once

#include <string>

#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"

namespace resdb {
namespace td_hotstuff {

class SafetyRules {
 public:
  bool RecordVote(const Proposal& proposal, std::string* error = nullptr);
  void ObserveLock(const QC& qc);

  int last_voted_view() const { return last_voted_view_; }
  const std::string& last_voted_hash() const { return last_voted_hash_; }
  const QC& lock_qc() const { return lock_qc_; }

 private:
  int last_voted_view_ = 0;
  std::string last_voted_hash_;
  QC lock_qc_;
};

}  // namespace td_hotstuff
}  // namespace resdb
