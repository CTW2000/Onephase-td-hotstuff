#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"
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
  ProposalManager(int32_t id, int limit_count, SignatureVerifier* verifier, int total_num, int non_responsive_num, int fork_tail_num, const std::vector<int64_t>& replica_weights = {}, int64_t quorum_weight = 0);

  std::unique_ptr<Proposal> GenerateProposal(const std::vector<std::unique_ptr<Transaction>>& txns);
  bool Verify(const Proposal& proposal);
  bool VerifyCert(const Certificate& cert);

  int CurrentView();

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
    int64_t WeightForSigner(int signer) const;
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

  std::mutex txn_mutex_;
  std::map<std::string, std::unique_ptr<Proposal> > local_block_;

  QC generic_qc_, lock_qc_;
  SignatureVerifier* verifier_;
  Stats* global_stats_ = nullptr;
};

}  // namespace td_hotstuff
}  // namespace resdb
