#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "common/crypto/signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"

namespace resdb {
namespace td_hotstuff {

constexpr const char* kTimeoutQuorumRuleId =
    "td_hotstuff_timeout_quorum_v1";

struct TimeoutConfig {
  bool enabled = false;
  int timeout_ms = 500;
};

TimeoutConfig TimeoutConfigFromEnv();
std::string TimeoutVotePayload(const TimeoutVote& vote);
bool ShouldTimeoutView(int current_view, int last_valid_proposal_view);

class TimeoutManager {
 public:
  TimeoutManager(int node_id, int total_replicas, SignatureVerifier* verifier,
                 std::shared_ptr<WeightSchedule> weight_schedule);

  std::unique_ptr<TimeoutVote> CreateTimeoutVote(int view, const QC& high_qc);
  std::unique_ptr<TimeoutCert> AddVote(const TimeoutVote& vote);

  bool VerifyTimeoutVote(const TimeoutVote& vote,
                         std::string* error = nullptr) const;
  bool VerifyTimeoutCert(const TimeoutCert& cert,
                         std::string* error = nullptr) const;
  void ResetBelowView(int view);

 private:
  bool VerifyHighQc(const QC& qc, int timeout_view,
                    std::string* error) const;
  int64_t WeightForSigner(int signer, int view) const;
  int64_t QuorumWeightForView(int view) const;
  TimeoutCert BuildCertForView(int view) const;

  int node_id_;
  int total_replicas_;
  SignatureVerifier* verifier_;
  std::shared_ptr<WeightSchedule> weight_schedule_;
  std::map<int, std::map<int, TimeoutVote>> votes_by_view_;
  std::set<int> formed_cert_views_;
};

}  // namespace td_hotstuff
}  // namespace resdb
