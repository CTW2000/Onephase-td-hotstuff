#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/certificate_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/safety_rules.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/timeout_manager.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"
#include "common/crypto/signature_verifier.h"
#include "platform/statistic/stats.h"

namespace resdb {
namespace td_hotstuff {

std::vector<int64_t> NormalizeReplicaWeights(
    const std::vector<int64_t>& replica_weights, int total_num);
int64_t CalculateQuorumWeight(const std::vector<int64_t>& replica_weights);
std::string BuildSignerBitmap(const std::vector<int>& signers, int total_num);
bool SignerBitmapMatchesSignatures(const QC& qc, int total_num);

class ProposalManager {
 public:
  ProposalManager(int32_t id, int limit_count, SignatureVerifier* verifier, int total_num, int non_responsive_num, int fork_tail_num, const std::vector<int64_t>& replica_weights = {}, int64_t quorum_weight = 0, std::shared_ptr<WeightSchedule> weight_schedule = nullptr, std::shared_ptr<LeaderSelectionSchedule> leader_schedule = nullptr);

  std::unique_ptr<Proposal> GenerateProposal(const std::vector<std::unique_ptr<Transaction>>& txns);
  bool Verify(const Proposal& proposal);
  bool VerifyCert(const Certificate& cert);
  bool RecordVote(const Proposal& proposal, std::string* error = nullptr);

  int CurrentView();
  const QC& HighQC() const;
  const TimeoutCert& HighestTimeoutCert() const;
  bool AdvanceToViewByTimeout(const TimeoutCert& cert);
  bool VerifyTimeoutCert(const TimeoutCert& cert);

  void AddQC(std::unique_ptr<QC> qc);
  std::vector<std::unique_ptr<Proposal>> AddProposal(std::unique_ptr<Proposal> proposal);
  const Proposal * GetProposal(const std::string& hash);

  int GetLeader(int view);

  protected:
    std::string GetHash(const Proposal& proposal);
    const Proposal * GetHighQC();
    bool SafeNode(const Proposal& proposal);
    bool VerifyQC(const QC& qc);
    bool VerifyHash(const Proposal& proposal);
    bool VerifyLeader(const Proposal& proposal);
    bool VerifyProposalSignature(const Proposal& proposal);
    bool VerifyTimeoutJustification(const Proposal& proposal);
    int64_t WeightForSigner(int signer, int view) const;
    int64_t QuorumWeightForView(int view) const;
  std::unique_ptr<Proposal> FetchProposal(const std::string& hash);

 private:
  int32_t id_;
  int round_;
  int limit_count_;
  int total_num_;
  int non_responsive_num_;
  int fork_tail_num_;
  std::vector<int64_t> replica_weights_;
  int64_t quorum_weight_;
  std::shared_ptr<WeightSchedule> weight_schedule_;
  std::shared_ptr<LeaderSelectionSchedule> leader_schedule_;

  std::mutex txn_mutex_;
  std::map<std::string, std::unique_ptr<Proposal> > local_block_;

  QC generic_qc_, lock_qc_;
  TimeoutCert highest_timeout_cert_;
  SignatureVerifier* verifier_;
  CertificateVerifier certificate_verifier_;
  SafetyRules safety_rules_;
  Stats* global_stats_ = nullptr;
};

}  // namespace td_hotstuff
}  // namespace resdb
