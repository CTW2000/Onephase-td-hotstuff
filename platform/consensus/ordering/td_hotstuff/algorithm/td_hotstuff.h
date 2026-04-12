#pragma once

#include <thread>
#include <vector>
#include <set>

#include "platform/common/queue/lock_free_queue.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"
#include "platform/consensus/ordering/common/algorithm/protocol_base.h"
#include "platform/statistic/stats.h"

namespace resdb {
namespace td_hotstuff {

class TdHotstuff: public common::ProtocolBase {
 public:
  TdHotstuff(int id, int f, int total_num, SignatureVerifier* verifier, int non_responsive_num, int fork_tail_num, int rollback_num, uint64_t timer_length, const std::vector<int>& weights);
  ~TdHotstuff();

  //  recv txn -> send block with links -> rec block ack -> send block with certs
  bool ReceiveTransaction(std::unique_ptr<Transaction> txn);
  bool ReceiveProposal(std::unique_ptr<Proposal> proposal);
  bool ReceiveCertificate(std::unique_ptr<Certificate> cert);


  private:
    bool Ready();
    void StartNewRound();
    void AsyncSend();
    void AsyncCommit();

    std::unique_ptr<Certificate> GenerateCertificate(const Proposal& proposal);

    // VRF-based weighted leader election
    int VRFLeader(int view);
    int NextLeader(int view);
    bool IsLeader(int view);

    // Trust-aware dynamic timeout (Pacemaker)
    // Δ(r) = Δ_base · (1 + κ · T̃(leader_r))
    // where T̃(v) = T(v) / max{T(v')} ∈ (0, 1]
    uint64_t GetDynamicTimeout(int view);

    void CommitProposal(std::unique_ptr<Proposal> p);

 private:
  LockFreeQueue<Transaction> txns_;
  LockFreeQueue<Proposal> commit_q_;

  std::mutex mutex_, rec_mutex_, rec_mutex2_, n_mutex_, pmutex_[1024];
  //std::mutex mutex_, n_mutex_;
  std::condition_variable vote_cv_;
  std::unique_ptr<ProposalManager> proposal_manager_;
  bool has_sent_;
  SignatureVerifier * verifier_;

  std::thread send_thread_, commit_thread_;

  int batch_size_;
  //[view][hash][signer][cert]
  //std::map<std::string, std::map<int, std::unique_ptr<Certificate>> >  receive_[1024];
  std::map<int,  std::map<std::string, std::map<int, std::unique_ptr<Certificate>> > > receive_;
  // [view][hash] -> accumulated weight of received certs
  std::map<int, std::map<std::string, int>> received_weight_;
  Stats* global_stats_ = nullptr;

  bool ready_;
  int slot_num_;
  uint64_t timer_length_;

  int non_responsive_num_, fork_tail_num_, rollback_num_;

  uint64_t last_time_ = 0;
  uint64_t min_view_ = 0;
  uint64_t min_slot_ = 0;
  uint64_t cert_count_ = 0;

  bool qc_formed_, proposal_received_;

  // Weighted QC members
  std::vector<int> weights_;       // weight for each replica, indexed by (node_id - 1)
  int total_weight_;               // W = sum of all weights
  int weight_threshold_;           // threshold = floor(2*W/3) + 1, so accumulated > 2W/3
  int accumulated_weight_ = 0;     // running weight sum for early-exit check

  // VRF-based weighted leader election
  // Prefix sums of weights for interval mapping: prefix_weights_[i] = sum(weights_[0..i-1])
  std::vector<int> prefix_weights_;
  uint64_t epoch_ = 1;            // current epoch (fixed seed for VRF)
  // Cache: view -> leader_id, to avoid recomputation
  std::map<int, int> leader_cache_;

  // Trust-aware dynamic timeout (Pacemaker)
  int max_weight_;                 // max(weights_), used to normalize T̃
  double kappa_;                   // κ coefficient (default 0.5)

  // Fast-path: set of (view*10000+slot) keys for which QC is already done
  std::set<int64_t> qc_done_;
};

}  // namespace td_hotstuff
}  // namespace resdb