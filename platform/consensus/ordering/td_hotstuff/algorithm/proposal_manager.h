#pragma once

#include <vector>

#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"
#include "common/crypto/signature_verifier.h"
#include "platform/statistic/stats.h"

namespace resdb {
namespace td_hotstuff {

class ProposalManager {
 public:
  ProposalManager(int32_t id, int weight_threshold, int slot_num, SignatureVerifier * verifier, int total_num, int fork_tail_num, int rollback_num, const std::vector<int>& weights);

  std::unique_ptr<Proposal> GenerateFakeProposal(const std::vector<std::unique_ptr<Transaction>>& txns, int slot, bool is_final);
  std::unique_ptr<Proposal> GenerateProposal(const std::vector<std::unique_ptr<Transaction>>& txns, int slot, bool is_final);
  bool Verify(const Proposal& proposal);
  bool VerifyCert(const Certificate& cert);

  int CurrentView();

  void AddQC(std::unique_ptr<QC> qc);
  std::unique_ptr<Proposal> AddProposal(std::unique_ptr<Proposal> proposal);
  const Proposal * GetProposal(const std::string& hash);

  int GetLeader(int view);

  protected:
    std::string GetHash(const Proposal& proposal);
    const Proposal * GetHighQC();
    bool SafeNode(const Proposal& proposal);
    bool VerifyQC(const QC& qc);
    bool VerifyHash(const Proposal& proposal);
  std::unique_ptr<Proposal> FetchProposal(const std::string& hash);

 private:
  int32_t id_;
  int round_;
  int weight_threshold_;  // weighted QC threshold = floor(2*W/3) + 1
  int slot_num_;

  std::mutex txn_mutex_;
  std::map<std::string, std::unique_ptr<Proposal> > local_block_;

  QC generic_qc_, lock_qc_, fake_generic_qc_;;
  SignatureVerifier* verifier_;

  int total_num_;
  int fork_tail_num_, rollback_num_;

  Stats* global_stats_ = nullptr;

  // Weighted QC: per-replica weights (indexed by node_id - 1)
  std::vector<int> weights_;
};

}  // namespace td_hotstuff
}  // namespace resdb