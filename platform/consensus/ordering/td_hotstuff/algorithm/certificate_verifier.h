#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/crypto/signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"

namespace resdb {
namespace td_hotstuff {

constexpr const char* kTimeoutQuorumRuleId =
    "td_hotstuff_timeout_quorum_v1";

std::string ProposalSignaturePayload(const Proposal& proposal);
std::string VoteSignaturePayload(const Certificate& vote);
std::string TimeoutVotePayload(const TimeoutVote& vote);

class CertificateVerifier {
 public:
  CertificateVerifier(int total_replicas, SignatureVerifier* verifier,
                      std::shared_ptr<WeightSchedule> weight_schedule = nullptr);

  bool VerifyProposalSignature(const Proposal& proposal,
                               std::string* error = nullptr) const;
  bool VerifyVote(const Certificate& vote,
                  std::string* error = nullptr) const;
  bool VerifyQC(const QC& qc, std::string* error = nullptr) const;
  bool VerifyTimeoutVote(const TimeoutVote& vote,
                         std::string* error = nullptr) const;
  bool VerifyTimeoutCert(const TimeoutCert& cert,
                         std::string* error = nullptr) const;

  int64_t WeightForSigner(int signer, int view) const;
  int64_t QuorumWeightForView(int view) const;

 private:
  int total_replicas_;
  SignatureVerifier* verifier_;
  std::shared_ptr<WeightSchedule> weight_schedule_;
};

}  // namespace td_hotstuff
}  // namespace resdb
