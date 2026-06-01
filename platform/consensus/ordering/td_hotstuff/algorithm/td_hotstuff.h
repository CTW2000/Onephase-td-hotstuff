#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "platform/common/queue/lock_free_queue.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/qc_evidence_recorder.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/qc_signer_selector.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_manager.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"
#include "platform/consensus/ordering/common/algorithm/protocol_base.h"
#include "platform/statistic/stats.h"

namespace resdb {
namespace td_hotstuff {

class HotStuff: public common::ProtocolBase {
 public:
  HotStuff(int id, int f, int total_num, SignatureVerifier* verifier, int non_responsive_num, int fork_tail_num, uint64_t timer_length, const std::vector<int64_t>& replica_weights = {}, int64_t quorum_weight = 0);
  ~HotStuff();

  //  recv txn -> send block with links -> rec block ack -> send block with certs
  bool ReceiveTransaction(std::unique_ptr<Transaction> txn);
  bool ReceiveProposal(std::unique_ptr<Proposal> proposal);
  bool ReceiveCertificate(std::unique_ptr<Certificate> cert);
  bool ReceiveWeightUpdateCandidate(std::unique_ptr<CandidateWeightUpdate> candidate);
  bool ReceiveWeightUpdateVote(std::unique_ptr<WeightUpdateVote> vote);
  bool ReceiveWeightUpdateCert(std::unique_ptr<WeightUpdateCert> cert);
  int CurrentView();
  int LeaderForView(int view);
  int CurrentLeader();


  private:
    bool Ready();
    void StartNewRound();
    void AsyncSend();
    void AsyncCommit();

    std::unique_ptr<Certificate> GenerateCertificate(const Proposal& proposal);

    int NextLeader(int view);
    bool IsLeader(int view);

    void CommitProposal(std::unique_ptr<Proposal> p);
    int64_t WeightForSigner(int signer, int view) const;
    int64_t CertificateWeight(
        const std::map<int, std::unique_ptr<Certificate>>& certs,
        int view) const;
    std::vector<int> CertificateSigners(
        const std::map<int, std::unique_ptr<Certificate>>& certs) const;
    std::vector<QcSignerInfo> CertificateSignerInfos(
        const std::map<int, std::unique_ptr<Certificate>>& certs,
        int view) const;
    WeightSnapshot CurrentWeightSnapshot(int current_view) const;
    WeightPluginOutboundMessages DrainWeightPlugin(int current_view);
    bool InstallWeightUpdate(const InstallableWeightUpdate& update,
                             int current_view);
    void BroadcastWeightPluginMessages(
        const WeightPluginOutboundMessages& messages);
    void AsyncBroadcastWeightPluginMessages();
    void BroadcastWeightPluginMessagesNow(
        const WeightPluginOutboundMessages& messages);
    void SyncReputationWeightsToActiveSchedule();

 private:
  LockFreeQueue<Transaction> txns_;
  LockFreeQueue<Proposal> commit_q_;

  std::mutex mutex_, n_mutex_, pmutex_[1024];
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
  Stats* global_stats_ = nullptr;

  int non_responsive_num_;
  int fork_tail_num_;

  bool qc_formed_, proposal_received_;
  std::unique_ptr<QC> formed_qc_;

  int crash_num_ = 0;

  uint64_t timer_length_;
  std::vector<int64_t> replica_weights_;
  int64_t quorum_weight_;
  std::shared_ptr<WeightSchedule> weight_schedule_;
  std::shared_ptr<LeaderSelectionSchedule> leader_selection_schedule_;
  std::unique_ptr<WeightUpdateManager> weight_update_manager_;
  std::unique_ptr<AsyncQcEvidenceRecorder> qc_evidence_recorder_;
  QcSignerCooldownTracker qc_signer_cooldown_;
  std::mutex weight_plugin_broadcast_mutex_;
  std::condition_variable weight_plugin_broadcast_cv_;
  std::deque<WeightPluginOutboundMessages> weight_plugin_broadcast_queue_;
  std::thread weight_plugin_broadcast_thread_;
  bool stop_weight_plugin_broadcast_ = false;
};

}  // namespace td_hotstuff
}  // namespace resdb
