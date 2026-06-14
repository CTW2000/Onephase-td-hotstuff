#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/crypto/signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/certificate_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/safety_rules.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/timeout_manager.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"
#include "platform/statistic/stats.h"

namespace resdb {
namespace td_hotstuff {

std::vector<int64_t> NormalizeReplicaWeights(
    const std::vector<int64_t>& replica_weights, int total_num);
int64_t CalculateQuorumWeight(const std::vector<int64_t>& replica_weights);
std::string BuildSignerBitmap(const std::vector<int>& signers, int total_num);
bool SignerBitmapMatchesSignatures(const QC& qc, int total_num);
int DefaultLeaderForView(int view, int total_replicas);

enum class ProposalValidationErrorCode {
  kNone,
  kHashMismatch,
  kBadProposalSignature,
  kLeaderMismatch,
  kLeaderContextMismatch,
  kInvalidTimeoutCert,
  kInvalidQc,
  kInvalidTimeoutJustification,
};

struct ProposalValidationResult {
  bool valid = false;
  bool proposal_hash_verified = false;
  bool proposal_signature_verified = false;
  bool leader_verified = false;
  bool leader_context_verified = false;
  bool qc_present = false;
  bool qc_verified = false;
  ProposalValidationErrorCode error_code = ProposalValidationErrorCode::kNone;
  std::string error_message;
};

class ProposalManager {
 public:
  ProposalManager(int32_t id, int limit_count, SignatureVerifier* verifier,
                  int total_num, int non_responsive_num, int fork_tail_num,
                  const std::vector<int64_t>& replica_weights = {},
                  int64_t quorum_weight = 0,
                  std::shared_ptr<WeightSchedule> weight_schedule = nullptr,
                  std::shared_ptr<LeaderSelectionSchedule> leader_schedule = nullptr);

  std::unique_ptr<Proposal> GenerateProposal(
      const std::vector<std::unique_ptr<Transaction>>& txns,
      const WeightUpdateCert* weight_update_cert = nullptr);
  ProposalValidationResult ValidateProposal(const Proposal& proposal);
  bool Verify(const Proposal& proposal);
  bool VerifyEnvelopeForEvidence(const Proposal& proposal);
  bool VerifyQcForEvidence(const QC& qc);
  bool VerifyQcForEvidence(const QC& qc, std::string* error);
  bool VerifyCert(const Certificate& cert);
  bool RecordVote(const Proposal& proposal, std::string* error = nullptr);

  int CurrentView();
  const QC& HighQC() const;
  const TimeoutCert& HighestTimeoutCert() const;
  bool AdvanceToViewByTimeout(const TimeoutCert& cert);
  bool VerifyTimeoutCert(const TimeoutCert& cert);

  void AddQC(std::unique_ptr<QC> qc);
  std::vector<std::unique_ptr<Proposal>> AddProposal(
      std::unique_ptr<Proposal> proposal);
  const Proposal* GetProposal(const std::string& hash);

  int GetLeader(int view);

 protected:
  std::string GetHash(const Proposal& proposal);
  const Proposal* GetHighQC();
  bool SafeNode(const Proposal& proposal);
  bool VerifyQC(const QC& qc);
  bool VerifyHash(const Proposal& proposal);
  bool VerifyLeader(const Proposal& proposal);
  bool VerifyLeaderContext(const Proposal& proposal);
  bool VerifyProposalSignature(const Proposal& proposal);
  bool VerifyTimeoutJustification(const Proposal& proposal);
  int64_t WeightForSigner(int signer, int view) const;
  int64_t QuorumWeightForView(int view) const;
  std::unique_ptr<Proposal> FetchProposal(const std::string& hash);

 private:
  int32_t id_ = 0;
  int round_ = 1;
  int limit_count_ = 0;
  int total_num_ = 0;
  int non_responsive_num_ = 0;
  int fork_tail_num_ = 0;
  std::vector<int64_t> replica_weights_;
  int64_t quorum_weight_ = 0;
  std::shared_ptr<WeightSchedule> weight_schedule_;
  std::shared_ptr<LeaderSelectionSchedule> leader_schedule_;

  std::mutex txn_mutex_;
  std::map<std::string, std::unique_ptr<Proposal>> local_block_;

  QC generic_qc_, lock_qc_;
  TimeoutCert highest_timeout_cert_;
  int signed_proposal_view_ = 0;
  std::unique_ptr<Proposal> signed_proposal_for_view_;
  SignatureVerifier* verifier_ = nullptr;
  CertificateVerifier certificate_verifier_;
  SafetyRules safety_rules_;
  Stats* global_stats_ = nullptr;
};

}  // namespace td_hotstuff
}  // namespace resdb
