#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "platform/common/queue/lock_free_queue.h"
#include "platform/consensus/ordering/common/algorithm/protocol_base.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/async_consensus_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/certified_weight_update_pipeline.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"
#include "platform/consensus/ordering/td_hotstuff/adapter/td_hotstuff_reputation_adapter.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/timeout_manager.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"
#include "platform/statistic/stats.h"

namespace resdb {
namespace td_hotstuff {

class HotStuff : public common::ProtocolBase {
 public:
  HotStuff(int id, int f, int total_num, SignatureVerifier* verifier,
           int non_responsive_num, int fork_tail_num, uint64_t timer_length,
           const std::vector<int64_t>& replica_weights = {},
           int64_t quorum_weight = 0);
  ~HotStuff();

  bool ReceiveTransaction(std::unique_ptr<Transaction> txn);
  bool ReceiveTransactionForView(std::unique_ptr<Transaction> txn,
                                 int target_view);
  bool ReceiveProposal(std::unique_ptr<Proposal> proposal);
  bool ReceiveCertificate(std::unique_ptr<Certificate> cert);
  bool ReceiveTimeoutVote(std::unique_ptr<TimeoutVote> vote);
  bool ReceiveTimeoutCert(std::unique_ptr<TimeoutCert> cert);
  bool ReceiveCandidateWeightUpdate(std::unique_ptr<CandidateWeightUpdate> candidate);
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
  void AsyncTimeout();
  void AsyncVerifiedEvents();
  void AsyncWeightUpdates();
  bool ProcessVerifiedCertificate(std::unique_ptr<Certificate> cert);
  void ProcessVerifiedConsensusEvent(VerifiedConsensusEvent event);

  std::unique_ptr<Certificate> GenerateCertificate(const Proposal& proposal);

  int NextLeader(int view);
  bool IsLeader(int view);

  void CommitProposal(std::unique_ptr<Proposal> p);
  int64_t WeightForSigner(int signer, int view) const;
  int64_t CertificateWeight(
      const std::map<int, std::unique_ptr<Certificate>>& certs, int view) const;
  std::vector<int> CertificateSigners(
      const std::map<int, std::unique_ptr<Certificate>>& certs,
      int view) const;
  std::vector<int> SelectPeerTrustCliqueSigners(
      const std::map<int, std::unique_ptr<Certificate>>& certs, int view,
      int64_t quorum_weight) const;
  bool MaybeMakeSignedProposalEvidenceSnapshotLocked(
      const Proposal& proposal,
      TdHotstuffSignedProposalEvidenceSnapshot* snapshot);
  bool MaybeMakeSignedVoteEvidenceSnapshotLocked(
      const Certificate& cert, TdHotstuffSignedVoteEvidenceSnapshot* snapshot);
  bool MaybeMakeInvalidQcProposalEvidenceSnapshotLocked(
      const Proposal& proposal, const ProposalValidationResult& validation,
      TdHotstuffInvalidQcProposalEvidenceSnapshot* snapshot);
  bool MaybeMakeQcEvidenceSnapshotLocked(const QC& qc,
                                          TdHotstuffQcEvidenceSnapshot* snapshot);
  bool MaybeFormQcLocked(int view, const std::string& hash);
  bool MaybeFormQcLocked(int view, const std::string& hash,
                         TdHotstuffQcEvidenceSnapshot* reputation_snapshot);
  void MarkTimeoutProgressLocked();
  void BroadcastTimeoutVote(const TimeoutVote& vote);
  void BroadcastTimeoutCert(const TimeoutCert& cert);
  bool ApplyTimeoutCertLocked(const TimeoutCert& cert);
  bool IsSilentLeaderForExperiment(int view) const;
  bool IsDoubleProposalForExperiment(int view) const;
  bool IsDoubleVoteForExperiment(int view) const;
  bool IsSlowVoteForExperiment(int view) const;
  void MaybeDelayVoteForExperiment(int view) const;
  bool ShouldUsePeerTrustCliqueForView(int view) const;
  bool IsInvalidQcForExperiment(int view) const;
  std::unique_ptr<Proposal> MakeConflictingProposalForExperiment(
      const Proposal& proposal);
  std::unique_ptr<Proposal> MakeInvalidQcProposalForExperiment(
      const Proposal& proposal);
  std::unique_ptr<Certificate> MakeConflictingCertificateForExperiment(
      const Certificate& cert);
  std::vector<std::unique_ptr<Transaction>> TakeTransactionsForView(
      int view, int max_count);
  void MarkTransactionCommitted(const Transaction& txn);

 private:
  LockFreeQueue<Transaction> txns_;
  LockFreeQueue<Proposal> commit_q_;
  std::mutex txn_buffer_mutex_;
  std::map<int, std::deque<std::unique_ptr<Transaction>>> pending_txns_by_view_;
  std::set<std::string> queued_txn_keys_;
  std::set<std::string> committed_txn_keys_;

  std::mutex mutex_, n_mutex_, pmutex_[1024];
  std::condition_variable vote_cv_;
  std::unique_ptr<ProposalManager> proposal_manager_;
  bool has_sent_ = false;
  SignatureVerifier* verifier_ = nullptr;

  std::thread send_thread_, commit_thread_;

  int batch_size_ = 1;
  std::map<int,
           std::map<std::string, std::map<int, std::unique_ptr<Certificate>>>>
      receive_;
  Stats* global_stats_ = nullptr;

  int non_responsive_num_ = 0;
  int fork_tail_num_ = 0;

  bool qc_formed_ = false;
  bool proposal_received_ = false;
  std::unique_ptr<QC> formed_qc_;

  int crash_num_ = 0;

  uint64_t timer_length_ = 0;
  TimeoutConfig timeout_config_;
  std::vector<int64_t> replica_weights_;
  int64_t quorum_weight_ = 0;
  std::shared_ptr<WeightSchedule> weight_schedule_;
  std::shared_ptr<LeaderSelectionSchedule> leader_schedule_;
  // Silent-leader attack-model state machine (experiment fault injection only;
  // mutated inside IsSilentLeaderForExperiment, which runs under mutex_).
  mutable bool silent_attacking_ = false;        // adaptive/band hysteresis phase
  mutable bool silent_persist_stopped_ = false;  // persist: latched-honest once below floor
  mutable int silent_relapse_phase_ = 0;         // relapse: 0/2 = attack, 1 = recover
  mutable uint64_t silent_leader_slots_ = 0;     // leader-slot counter (burst/degrade)
  std::unique_ptr<AsyncConsensusVerifier> async_verifier_;
  std::unique_ptr<TdHotstuffReputationAdapter> reputation_adapter_;
  std::unique_ptr<CertifiedWeightUpdatePipeline> weight_update_pipeline_;
  std::unique_ptr<TimeoutManager> timeout_manager_;
  std::set<int> timeout_echoed_views_;
  std::thread timeout_thread_;
  std::thread weight_update_thread_;
  std::thread verified_event_thread_;
  std::atomic<bool> stop_timeout_{false};
  std::atomic<bool> stop_weight_updates_{false};
  int timeout_empty_proposal_until_view_ = 0;
  int timeout_empty_proposal_budget_views_ = 0;
  uint64_t timeout_progress_epoch_ = 0;
  std::atomic<uint64_t> timeout_progress_epoch_atomic_{0};
  std::atomic<bool> stop_verified_events_{false};
  int last_valid_proposal_view_ = 0;
};

}  // namespace td_hotstuff
}  // namespace resdb
