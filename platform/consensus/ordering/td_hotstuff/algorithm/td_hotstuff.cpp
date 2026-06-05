#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <thread>

#include "common/utils/utils.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/certificate_verifier.h"

namespace resdb {
namespace td_hotstuff {
namespace {

bool SilentLeaderForExperimentFromEnv() {
  const char* raw = std::getenv("TD_HS_SILENT_LEADER");
  if (raw == nullptr) {
    return false;
  }
  const std::string value(raw);
  return value == "1" || value == "true" || value == "TRUE" || value == "yes" ||
         value == "YES" || value == "on" || value == "ON";
}

int PositiveIntFromEnv(const char* name, int default_value) {
  const char* raw = std::getenv(name);
  if (raw == nullptr || std::string(raw).empty()) {
    return default_value;
  }
  try {
    const int value = std::stoi(raw);
    return value > 0 ? value : default_value;
  } catch (...) {
    return default_value;
  }
}


std::string TransactionDedupKey(const Transaction& txn) {
  if (!txn.hash().empty()) {
    return std::string("h:") + txn.hash();
  }
  if (txn.proxy_id() != 0 || txn.user_seq() != 0) {
    return std::string("u:") + std::to_string(txn.proxy_id()) + ":" +
           std::to_string(txn.user_seq());
  }
  return std::string("d:") + SignatureVerifier::CalculateHash(txn.data());
}

std::string WeightsForLog(const std::vector<int64_t>& weights) {
  std::ostringstream out;
  out << '[';
  for (size_t i = 0; i < weights.size(); ++i) {
    if (i > 0) {
      out << ',';
    }
    out << weights[i];
  }
  out << ']';
  return out.str();
}

}  // namespace

HotStuff::HotStuff(int id, int f, int total_num, SignatureVerifier* verifier,
                   int non_responsive_num, int fork_tail_num,
                   uint64_t timer_length,
                   const std::vector<int64_t>& replica_weights,
                   int64_t quorum_weight)
    : ProtocolBase(id, f, total_num),
      verifier_(verifier),
      non_responsive_num_(non_responsive_num),
      fork_tail_num_(fork_tail_num),
      timer_length_(timer_length),
      timeout_config_(TimeoutConfigFromEnv()),
      replica_weights_(NormalizeReplicaWeights(replica_weights, total_num)),
      quorum_weight_(quorum_weight > 0
                         ? quorum_weight
                         : CalculateQuorumWeight(replica_weights_)),
      weight_schedule_(
          std::make_shared<WeightSchedule>(total_num, replica_weights_)),
      qc_signer_cooldown_(QcSignerDiversityConfigFromEnv()) {
  LOG(ERROR) << "id:" << id << " f:" << f << " total:" << total_num_;

  global_stats_ = Stats::GetGlobalStats();
  leader_selection_schedule_ = std::make_shared<LeaderSelectionSchedule>(
      total_num_, replica_weights_, LeaderSelectionConfigFromEnv());
  proposal_manager_ = std::make_unique<ProposalManager>(
      id, 2 * f_ + 1, verifier, total_num, non_responsive_num, fork_tail_num,
      replica_weights_, quorum_weight_, weight_schedule_,
      leader_selection_schedule_);
  silent_leader_for_experiment_ = SilentLeaderForExperimentFromEnv();
  if (timeout_config_.enabled) {
    timeout_manager_ = std::make_unique<TimeoutManager>(
        id_, total_num_, verifier_, weight_schedule_);
  }
  weight_update_manager_ = std::make_unique<WeightUpdateManager>(
      id_, total_num_, verifier_, WeightUpdateConfigFromEnv());
  qc_evidence_recorder_ = AsyncQcEvidenceRecorder::CreateFromEnv(
      id_, total_num_, weight_schedule_->ActiveWeights());
  async_verifier_ = std::make_unique<AsyncConsensusVerifier>(
      total_num_, verifier_, AsyncVerifierWorkerCountFromEnv(),
      AsyncVerifierQueueCapacityFromEnv(), weight_schedule_);
  async_verifier_->Start();
  weight_plugin_drain_interval_views_ =
      PositiveIntFromEnv("TD_HS_WEIGHT_PLUGIN_DRAIN_INTERVAL_VIEWS", 256);
  timeout_empty_proposal_budget_views_ =
      timeout_config_.enabled
          ? PositiveIntFromEnv("TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS",
                               total_num_)
          : 0;
  SyncReputationWeightsToActiveSchedule();
  has_sent_ = false;
  send_thread_ = std::thread(&HotStuff::AsyncSend, this);
  commit_thread_ = std::thread(&HotStuff::AsyncCommit, this);
  weight_plugin_broadcast_thread_ =
      std::thread(&HotStuff::AsyncBroadcastWeightPluginMessages, this);
  verified_event_thread_ = std::thread(&HotStuff::AsyncVerifiedEvents, this);
  if (timeout_manager_ != nullptr) {
    timeout_thread_ = std::thread(&HotStuff::AsyncTimeout, this);
  }
  batch_size_ = 1;
  qc_formed_ = proposal_received_ = false;
}

HotStuff::~HotStuff() {
  stop_verified_events_.store(true);
  if (async_verifier_ != nullptr) {
    async_verifier_->Stop();
  }
  if (verified_event_thread_.joinable()) {
    verified_event_thread_.join();
  }

  stop_timeout_.store(true);
  if (timeout_thread_.joinable()) {
    timeout_thread_.join();
  }
  {
    std::unique_lock<std::mutex> lk(weight_plugin_broadcast_mutex_);
    stop_weight_plugin_broadcast_ = true;
  }
  weight_plugin_broadcast_cv_.notify_all();
  if (weight_plugin_broadcast_thread_.joinable()) {
    weight_plugin_broadcast_thread_.join();
  }
  if (qc_evidence_recorder_ != nullptr) {
    qc_evidence_recorder_->Stop();
  }
}

int HotStuff::LeaderForView(int view) {
  return proposal_manager_ != nullptr ? proposal_manager_->GetLeader(view)
                                      : DefaultLeaderForView(view, total_num_);
}

int HotStuff::NextLeader(int view) { return LeaderForView(view + 1); }

bool HotStuff::IsLeader(int view) { return LeaderForView(view) == id_; }

bool HotStuff::IsSilentLeaderForExperiment() const {
  return silent_leader_for_experiment_;
}

void HotStuff::MarkTimeoutProgressLocked() {
  ++timeout_progress_epoch_;
  timeout_progress_epoch_atomic_.store(timeout_progress_epoch_,
                                       std::memory_order_release);
}

bool HotStuff::Ready() {
  int view = proposal_manager_->CurrentView();
  return IsLeader(view) && !has_sent_;
}

void HotStuff::StartNewRound() {
  std::unique_lock<std::mutex> lk(n_mutex_);
  has_sent_ = false;
  vote_cv_.notify_one();
  // LOG(ERROR)<<" start new round";
}

void HotStuff::AsyncTimeout() {
  int watched_view = 0;
  auto view_started_at = std::chrono::steady_clock::now();
  auto last_vote_at = std::chrono::steady_clock::time_point::min();
  const auto timeout_interval =
      std::chrono::milliseconds(timeout_config_.timeout_ms);
  const auto poll_interval = std::chrono::milliseconds(
      std::max(1, std::min(timeout_config_.timeout_ms, 50)));
  const auto startup_grace_interval =
      std::chrono::milliseconds(std::max(timeout_config_.timeout_ms * 2, 1500));
  auto public_keys_ready_at = std::chrono::steady_clock::time_point::min();
  uint64_t observed_progress_epoch = 0;
  auto last_weight_plugin_drain_at =
      std::chrono::steady_clock::time_point::min();
  const auto weight_plugin_drain_interval = std::chrono::milliseconds(50);

  while (!IsStop() && !stop_timeout_.load()) {
    std::this_thread::sleep_for(poll_interval);
    if (IsStop() || stop_timeout_.load() || timeout_manager_ == nullptr) {
      continue;
    }
    const auto now = std::chrono::steady_clock::now();
    if (verifier_ != nullptr &&
        verifier_->GetPublicKeysSize() < static_cast<size_t>(total_num_)) {
      watched_view = 0;
      view_started_at = now;
      last_vote_at = std::chrono::steady_clock::time_point::min();
      public_keys_ready_at = std::chrono::steady_clock::time_point::min();
      continue;
    }
    if (public_keys_ready_at == std::chrono::steady_clock::time_point::min()) {
      public_keys_ready_at = now;
      watched_view = 0;
      view_started_at = now;
      last_vote_at = std::chrono::steady_clock::time_point::min();
      continue;
    }
    if (now - public_keys_ready_at < startup_grace_interval) {
      watched_view = 0;
      view_started_at = now;
      last_vote_at = std::chrono::steady_clock::time_point::min();
      continue;
    }

    const uint64_t progress_epoch =
        timeout_progress_epoch_atomic_.load(std::memory_order_acquire);
    if (progress_epoch != observed_progress_epoch) {
      observed_progress_epoch = progress_epoch;
      watched_view = 0;
      view_started_at = now;
      last_vote_at = std::chrono::steady_clock::time_point::min();
      continue;
    }
    if (now - view_started_at < timeout_interval) {
      continue;
    }

    WeightPluginOutboundMessages periodic_weight_messages;
    if (last_weight_plugin_drain_at ==
            std::chrono::steady_clock::time_point::min() ||
        now - last_weight_plugin_drain_at >= weight_plugin_drain_interval) {
      {
        std::unique_lock<std::mutex> lk(mutex_);
        periodic_weight_messages =
            DrainWeightPlugin(proposal_manager_->CurrentView());
      }
      BroadcastWeightPluginMessages(periodic_weight_messages);
      last_weight_plugin_drain_at = now;
    }

    TimeoutVote vote;
    TimeoutCert cert;
    bool has_vote = false;
    bool has_cert = false;
    bool advanced = false;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      if (timeout_progress_epoch_ != observed_progress_epoch) {
        observed_progress_epoch = timeout_progress_epoch_;
        timeout_progress_epoch_atomic_.store(timeout_progress_epoch_,
                                             std::memory_order_release);
        watched_view = 0;
        view_started_at = now;
        last_vote_at = std::chrono::steady_clock::time_point::min();
        continue;
      }
      const int local_view = proposal_manager_->CurrentView();
      const int view = std::max(local_view, last_valid_proposal_view_ + 1);
      const QC& high_qc = proposal_manager_->HighQC();
      (void)LeaderForView(view);
      if (!high_qc.hash().empty() && high_qc.view() >= view) {
        watched_view = 0;
        view_started_at = now;
        last_vote_at = std::chrono::steady_clock::time_point::min();
        continue;
      }
      if (!ShouldTimeoutView(view, last_valid_proposal_view_)) {
        watched_view = 0;
        view_started_at = now;
        last_vote_at = std::chrono::steady_clock::time_point::min();
        continue;
      }
      RecordLeaderOpportunityLocked(view);
      if (view != watched_view) {
        watched_view = view;
        last_vote_at = std::chrono::steady_clock::time_point::min();
        // The view has already been idle for timeout_interval by this point.
        // Do not reset view_started_at, otherwise the first timeout vote waits
        // almost two full timeout intervals.
      }
      if (last_vote_at != std::chrono::steady_clock::time_point::min() &&
          now - last_vote_at < timeout_interval) {
        continue;
      }
      std::unique_ptr<TimeoutVote> local_vote =
          timeout_manager_->CreateTimeoutVote(view, high_qc);
      if (local_vote == nullptr) {
        continue;
      }
      last_vote_at = now;
      vote = *local_vote;
      has_vote = true;
      std::unique_ptr<TimeoutCert> formed_cert =
          timeout_manager_->AddVote(*local_vote);
      if (formed_cert != nullptr) {
        cert = *formed_cert;
        has_cert = true;
        advanced = ApplyTimeoutCertLocked(cert);
        if (advanced) {
          watched_view = proposal_manager_->CurrentView();
          view_started_at = now;
          last_vote_at = std::chrono::steady_clock::time_point::min();
        }
      }
    }
    if (has_vote) {
      BroadcastTimeoutVote(vote);
    }
    if (has_cert) {
      BroadcastTimeoutCert(cert);
    }
    if (advanced) {
      StartNewRound();
    }
  }
}

void HotStuff::AsyncSend() {
  while (!IsStop()) {
    {
      std::unique_lock<std::mutex> lk(n_mutex_);
      vote_cv_.wait_for(lk, std::chrono::microseconds(1000),
                        [&] { return Ready(); });
    }
    if (IsStop()) {
      return;
    }
    if (!Ready()) {
      continue;
    }

    std::unique_ptr<Proposal> proposal = nullptr;
    WeightPluginOutboundMessages weight_messages;
    bool silent_leader = false;
    bool no_transactions_ready = false;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      const int view = proposal_manager_->CurrentView();
      weight_messages = DrainWeightPlugin(view);
      if (!IsLeader(view) || has_sent_) {
        // Leadership may have changed while we were waking up.
      } else if (IsSilentLeaderForExperiment()) {
        silent_leader = true;
        has_sent_ = true;
      } else {
        std::vector<std::unique_ptr<Transaction>> txns =
            TakeTransactionsForView(view, batch_size_);
        if (txns.empty() &&
            (timeout_manager_ == nullptr ||
             view > timeout_empty_proposal_until_view_)) {
          no_transactions_ready = true;
        } else {
          proposal = proposal_manager_->GenerateProposal(txns);
          if (proposal != nullptr) {
            last_valid_proposal_view_ =
                std::max(last_valid_proposal_view_, proposal->header().view());
          }
          has_sent_ = true;
          MarkTimeoutProgressLocked();
        }
      }
    }

    BroadcastWeightPluginMessages(weight_messages);
    if (proposal != nullptr) {
      broadcast_call_(MessageType::NewProposal, *proposal);
      continue;
    }
    if (silent_leader || no_transactions_ready) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
}

void HotStuff::AsyncCommit() {
  int seq = 1;
  while (!IsStop()) {
    auto p = commit_q_.Pop();
    if (p == nullptr) {
      continue;
    }
    global_stats_->AddConsensusLatency(GetCurrentTime() - p->createtime());
    // LOG(ERROR) << "create time: " << p->createtime();
    for (Transaction& txn : *p->mutable_transactions()) {
      MarkTransactionCommitted(txn);
      txn.set_id(seq++);
      Commit(txn);
    }
  }
}

void HotStuff::AsyncVerifiedEvents() {
  while (true) {
    std::vector<VerifiedConsensusEvent> events;
    if (async_verifier_ != nullptr) {
      events =
          async_verifier_->WaitForVerified(std::chrono::milliseconds(100));
    }
    for (VerifiedConsensusEvent& event : events) {
      ProcessVerifiedConsensusEvent(std::move(event));
    }
    if ((IsStop() || stop_verified_events_.load()) && events.empty()) {
      break;
    }
    if (async_verifier_ == nullptr) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
}

void HotStuff::ProcessVerifiedConsensusEvent(VerifiedConsensusEvent event) {
  switch (event.type) {
    case VerifiedConsensusEvent::Type::kVote:
      ProcessVerifiedCertificate(std::move(event.vote));
      return;
    case VerifiedConsensusEvent::Type::kTimeoutVote:
    case VerifiedConsensusEvent::Type::kTimeoutCert:
    case VerifiedConsensusEvent::Type::kNone:
      return;
  }
}

bool HotStuff::ReceiveTransaction(std::unique_ptr<Transaction> txn) {
  return ReceiveTransactionForView(std::move(txn), CurrentView());
}

bool HotStuff::ReceiveTransactionForView(std::unique_ptr<Transaction> txn,
                                         int target_view) {
  if (txn == nullptr) {
    return false;
  }
  if (target_view <= 0) {
    target_view = CurrentView();
  }
  txn->set_reception_time(GetCurrentTime());
  txn->set_proposer(id_);
  const std::string key = TransactionDedupKey(*txn);
  {
    std::unique_lock<std::mutex> lk(txn_buffer_mutex_);
    if (committed_txn_keys_.find(key) != committed_txn_keys_.end() ||
        queued_txn_keys_.find(key) != queued_txn_keys_.end()) {
      return true;
    }
    queued_txn_keys_.insert(key);
    pending_txns_by_view_[target_view].push_back(std::move(txn));
  }
  vote_cv_.notify_one();
  return true;
}

std::vector<std::unique_ptr<Transaction>> HotStuff::TakeTransactionsForView(
    int view, int max_count) {
  std::vector<std::unique_ptr<Transaction>> txns;
  if (max_count <= 0) {
    return txns;
  }
  std::unique_lock<std::mutex> lk(txn_buffer_mutex_);
  for (auto it = pending_txns_by_view_.begin();
       it != pending_txns_by_view_.end() && it->first <= view &&
       static_cast<int>(txns.size()) < max_count;) {
    auto& pending = it->second;
    while (!pending.empty() && static_cast<int>(txns.size()) < max_count) {
      std::unique_ptr<Transaction> txn = std::move(pending.front());
      pending.pop_front();
      if (txn == nullptr) {
        continue;
      }
      const std::string key = TransactionDedupKey(*txn);
      if (committed_txn_keys_.find(key) != committed_txn_keys_.end()) {
        queued_txn_keys_.erase(key);
        continue;
      }
      txns.push_back(std::move(txn));
    }
    if (pending.empty()) {
      it = pending_txns_by_view_.erase(it);
    } else {
      ++it;
    }
  }
  return txns;
}

void HotStuff::MarkTransactionCommitted(const Transaction& txn) {
  const std::string key = TransactionDedupKey(txn);
  std::unique_lock<std::mutex> lk(txn_buffer_mutex_);
  committed_txn_keys_.insert(key);
  queued_txn_keys_.erase(key);
}

int HotStuff::CurrentView() {
  std::unique_lock<std::mutex> lk(mutex_);
  return proposal_manager_ != nullptr ? proposal_manager_->CurrentView() : 0;
}

int HotStuff::CurrentLeader() {
  std::unique_lock<std::mutex> lk(mutex_);
  if (proposal_manager_ == nullptr) {
    return 0;
  }
  return LeaderForView(proposal_manager_->CurrentView());
}

int64_t HotStuff::WeightForSigner(int signer, int view) const {
  if (weight_schedule_ != nullptr) {
    return weight_schedule_->WeightForSigner(signer, view);
  }
  if (signer < 1 || signer > static_cast<int>(replica_weights_.size())) {
    return 0;
  }
  return replica_weights_[signer - 1];
}

int64_t HotStuff::CertificateWeight(
    const std::map<int, std::unique_ptr<Certificate>>& certs, int view) const {
  int64_t total_weight = 0;
  for (const auto& entry : certs) {
    total_weight += WeightForSigner(entry.first, view);
  }
  return total_weight;
}

std::vector<int> HotStuff::CertificateSigners(
    const std::map<int, std::unique_ptr<Certificate>>& certs) const {
  std::vector<int> signers;
  signers.reserve(certs.size());
  for (const auto& entry : certs) {
    signers.push_back(entry.first);
  }
  return signers;
}

std::vector<QcSignerInfo> HotStuff::CertificateSignerInfos(
    const std::map<int, std::unique_ptr<Certificate>>& certs, int view) const {
  std::vector<QcSignerInfo> signers;
  signers.reserve(certs.size());
  for (const auto& entry : certs) {
    const int64_t weight = WeightForSigner(entry.first, view);
    if (weight > 0) {
      signers.push_back({entry.first, weight});
    }
  }
  return signers;
}

void HotStuff::SyncReputationWeightsToActiveSchedule() {
  if (qc_evidence_recorder_ == nullptr || weight_schedule_ == nullptr) {
    return;
  }
  qc_evidence_recorder_->UpdateReputationWeights(
      weight_schedule_->ActiveWeights(), weight_schedule_->ActiveWeightRoot(),
      weight_schedule_->ActiveWeightVersion());
}

void HotStuff::RecordLeaderOpportunityLocked(int view) {
  if (qc_evidence_recorder_ == nullptr || proposal_manager_ == nullptr) {
    return;
  }
  if (view <= 0 || view == last_recorded_leader_opportunity_view_) {
    return;
  }
  const int leader_id = proposal_manager_->GetLeader(view);
  if (leader_id <= 0) {
    return;
  }
  last_recorded_leader_opportunity_view_ = view;
  const uint64_t weight_version =
      weight_schedule_ != nullptr ? weight_schedule_->WeightVersionForView(view)
                                  : 0;
  const std::string active_weight_root =
      weight_schedule_ != nullptr ? weight_schedule_->WeightRootForView(view)
                                  : std::string();
  const int64_t leader_eligible_min_weight =
      leader_selection_schedule_ != nullptr
          ? leader_selection_schedule_->eligible_min_weight()
          : 10;
  qc_evidence_recorder_->RecordLeaderOpportunity(
      view, leader_id, weight_version, active_weight_root,
      leader_eligible_min_weight);
}

WeightSnapshot HotStuff::CurrentWeightSnapshot(int current_view) const {
  WeightSnapshot snapshot = MakeWeightSnapshot(*weight_schedule_, current_view);
  return snapshot;
}

WeightPluginOutboundMessages HotStuff::DrainWeightPlugin(int current_view,
                                                         bool force) {
  WeightPluginOutboundMessages messages;
  if (weight_update_manager_ == nullptr || !weight_update_manager_->enabled()) {
    return messages;
  }
  const bool weight_activated =
      weight_schedule_ != nullptr && weight_schedule_->ActivateUpTo(current_view);
  if (weight_activated) {
    if (leader_selection_schedule_ != nullptr &&
        leader_selection_schedule_->enabled()) {
      leader_selection_schedule_->InstallWeightSnapshot(
          weight_schedule_->ActiveActivationView() + 1,
          weight_schedule_->ActiveWeightVersion(),
          weight_schedule_->ActiveWeightRoot(),
          weight_schedule_->ActiveWeights());
    }
    SyncReputationWeightsToActiveSchedule();
    weight_update_manager_->OnWeightsActivated(
        CurrentWeightSnapshot(current_view));
  }
  if (weight_activated) {
    LOG(ERROR) << "activated TD-Hotstuff weight schedule version:"
               << weight_schedule_->ActiveWeightVersion()
               << " view:" << current_view
               << " root:" << weight_schedule_->ActiveWeightRoot()
               << " active_weights:"
               << WeightsForLog(weight_schedule_->ActiveWeights());
  }
  if (!force && weight_plugin_drain_interval_views_ > 0 &&
      current_view < next_weight_plugin_drain_view_) {
    return messages;
  }
  if (!force && weight_plugin_drain_interval_views_ > 0) {
    next_weight_plugin_drain_view_ = std::max(
        current_view + weight_plugin_drain_interval_views_,
        next_weight_plugin_drain_view_ + weight_plugin_drain_interval_views_);
  }

  WeightSnapshot snapshot = CurrentWeightSnapshot(current_view);
  if (qc_evidence_recorder_ != nullptr) {
    weight_update_manager_->AddLocalCandidates(
        qc_evidence_recorder_->TakeCompletedReputationCandidates(), snapshot);
  }

  messages =
      weight_update_manager_->DrainOutboundMessages(current_view, snapshot);
  std::vector<InstallableWeightUpdate> installable_updates =
      weight_update_manager_->TakeInstallableUpdates(current_view, snapshot);
  for (const InstallableWeightUpdate& update : installable_updates) {
    InstallWeightUpdate(update, current_view);
  }
  return messages;
}

bool HotStuff::InstallWeightUpdate(const InstallableWeightUpdate& update,
                                   int current_view) {
  if (weight_schedule_ == nullptr ||
      update.old_weight_root != weight_schedule_->ActiveWeightRoot() ||
      update.old_weight_version != weight_schedule_->ActiveWeightVersion()) {
    return false;
  }
  if (update.activation_view <= current_view) {
    LOG(WARNING) << "drop stale TD-Hotstuff weight update activation_view:"
                 << update.activation_view << " current_view:" << current_view;
    return false;
  }
  if (!weight_schedule_->ScheduleUpdate(
          update.activation_view, update.next_weights, update.old_weight_root,
          update.old_weight_version)) {
    return false;
  }
  const bool weight_activated = weight_schedule_->ActivateUpTo(current_view);
  if (weight_activated) {
    if (leader_selection_schedule_ != nullptr &&
        leader_selection_schedule_->enabled()) {
      leader_selection_schedule_->InstallWeightSnapshot(
          weight_schedule_->ActiveActivationView() + 1,
          weight_schedule_->ActiveWeightVersion(),
          weight_schedule_->ActiveWeightRoot(),
          weight_schedule_->ActiveWeights());
    }
    SyncReputationWeightsToActiveSchedule();
    weight_update_manager_->OnWeightsActivated(
        CurrentWeightSnapshot(current_view));
  }
  if (weight_activated) {
    LOG(ERROR) << "activated TD-Hotstuff weight update version:"
               << weight_schedule_->ActiveWeightVersion()
               << " view:" << current_view
               << " root:" << weight_schedule_->ActiveWeightRoot()
               << " active_weights:"
               << WeightsForLog(weight_schedule_->ActiveWeights());
  } else {
    LOG(ERROR) << "scheduled TD-Hotstuff weight update activation_view:"
               << update.activation_view << " current_view:" << current_view
               << " next_weights:" << WeightsForLog(update.next_weights);
  }
  return true;
}

void HotStuff::BroadcastWeightPluginMessages(
    const WeightPluginOutboundMessages& messages) {
  if (messages.candidates.empty() && messages.votes.empty() &&
      messages.certs.empty()) {
    return;
  }
  {
    std::unique_lock<std::mutex> lk(weight_plugin_broadcast_mutex_);
    weight_plugin_broadcast_queue_.push_back(messages);
  }
  weight_plugin_broadcast_cv_.notify_one();
}

void HotStuff::AsyncBroadcastWeightPluginMessages() {
  while (true) {
    WeightPluginOutboundMessages messages;
    {
      std::unique_lock<std::mutex> lk(weight_plugin_broadcast_mutex_);
      weight_plugin_broadcast_cv_.wait_for(
          lk, std::chrono::milliseconds(100), [this] {
            return stop_weight_plugin_broadcast_ ||
                   !weight_plugin_broadcast_queue_.empty();
          });
      if (weight_plugin_broadcast_queue_.empty()) {
        if (stop_weight_plugin_broadcast_ || IsStop()) {
          break;
        }
        continue;
      }
      messages = std::move(weight_plugin_broadcast_queue_.front());
      weight_plugin_broadcast_queue_.pop_front();
    }
    BroadcastWeightPluginMessagesNow(messages);
  }
}

void HotStuff::BroadcastWeightPluginMessagesNow(
    const WeightPluginOutboundMessages& messages) {
  if (broadcast_call_ == nullptr) {
    return;
  }
  for (const CandidateWeightUpdate& candidate : messages.candidates) {
    broadcast_call_(MessageType::WeightUpdateCandidateMsg, candidate);
  }
  for (const WeightUpdateVote& vote : messages.votes) {
    broadcast_call_(MessageType::WeightUpdateVoteMsg, vote);
  }
  for (const WeightUpdateCert& cert : messages.certs) {
    broadcast_call_(MessageType::WeightUpdateCertMsg, cert);
  }
}

bool HotStuff::ReceiveProposal(std::unique_ptr<Proposal> proposal) {
  if (IsSlowReplica(id_)) {
    usleep(GetRandomDelay());
  }
  int view = proposal->header().view();
  std::unique_ptr<Certificate> cert = nullptr;
  WeightPluginOutboundMessages weight_messages;
  bool proposal_valid = true;
  bool stale_proposal = false;
  AsyncQcEvidenceRecorder* evidence_recorder = nullptr;
  bool has_qc_evidence = false;
  int qc_evidence_view = 0;
  int qc_evidence_leader_id = 0;
  uint64_t qc_evidence_weight_version = 0;
  int64_t qc_evidence_leader_eligible_min_weight = 10;
  std::string qc_evidence_hash;
  std::string qc_evidence_signer_bitmap;
  std::string qc_evidence_active_weight_root;
  {
    // LOG(ERROR)<<"RECEIVE proposer view:"<<proposal->header().view() << "
    // from: " << proposal->sender();
    std::unique_lock<std::mutex> lk(mutex_);

    if (id_ == NextLeader(view)) {
      proposal_received_ = true;
    }

    if (proposal->header().has_timeout_cert()) {
      ApplyTimeoutCertLocked(proposal->header().timeout_cert());
    }
    const int local_current_view = proposal_manager_->CurrentView();
    // Mutate active weight/leader state only at the local pacemaker boundary.
    // Proposal/QC verification below still uses view-indexed schedules.
    weight_messages = DrainWeightPlugin(local_current_view);
    RecordLeaderOpportunityLocked(view);
    if (view < local_current_view) {
      stale_proposal = true;
      proposal_valid = false;
    } else if (!proposal_manager_->Verify(*proposal)) {
      LOG(ERROR) << " proposal invalid";
      proposal_valid = false;
    } else {
      std::string safety_error;
      if (!proposal_manager_->RecordVote(*proposal, &safety_error)) {
        LOG(ERROR) << "proposal vote safety rejected: " << safety_error;
        proposal_valid = false;
      }
    }

    if (proposal_valid) {
      last_valid_proposal_view_ = std::max(last_valid_proposal_view_, view);
      MarkTimeoutProgressLocked();
      if (qc_evidence_recorder_ != nullptr) {
        evidence_recorder = qc_evidence_recorder_.get();
        const QC& qc = proposal->header().qc();
        qc_evidence_view = qc.view();
        qc_evidence_leader_id =
            qc_evidence_view > 0 ? proposal_manager_->GetLeader(qc_evidence_view) : 0;
        qc_evidence_weight_version =
            weight_schedule_ != nullptr
                ? weight_schedule_->WeightVersionForView(qc_evidence_view)
                : 0;
        qc_evidence_active_weight_root =
            weight_schedule_ != nullptr
                ? weight_schedule_->WeightRootForView(qc_evidence_view)
                : std::string();
        qc_evidence_leader_eligible_min_weight =
            leader_selection_schedule_ != nullptr
                ? leader_selection_schedule_->eligible_min_weight()
                : 10;
        qc_evidence_hash = qc.hash();
        qc_evidence_signer_bitmap = qc.signer_bitmap();
        has_qc_evidence = true;
      }
      WeightPluginOutboundMessages post_verify_messages =
          DrainWeightPlugin(proposal_manager_->CurrentView());
      weight_messages.candidates.insert(weight_messages.candidates.end(),
                                        post_verify_messages.candidates.begin(),
                                        post_verify_messages.candidates.end());
      weight_messages.votes.insert(weight_messages.votes.end(),
                                   post_verify_messages.votes.begin(),
                                   post_verify_messages.votes.end());
      weight_messages.certs.insert(weight_messages.certs.end(),
                                   post_verify_messages.certs.begin(),
                                   post_verify_messages.certs.end());

      cert = GenerateCertificate(*proposal);
      assert(cert != nullptr);

      std::vector<std::unique_ptr<Proposal>> committed_p_list =
          proposal_manager_->AddProposal(std::move(proposal));
      for (int i = committed_p_list.size() - 1; i >= 0; i--) {
        // LOG(ERROR) << "commit view: " <<
        // committed_p_list[i]->header().view();
        CommitProposal(std::move(committed_p_list[i]));
      }

      // AddProposal/AddQC may advance the local view. Drain once more before
      // choosing the vote destination so boundary votes follow the active
      // leader schedule.
      WeightPluginOutboundMessages post_add_messages =
          DrainWeightPlugin(proposal_manager_->CurrentView());
      weight_messages.candidates.insert(weight_messages.candidates.end(),
                                        post_add_messages.candidates.begin(),
                                        post_add_messages.candidates.end());
      weight_messages.votes.insert(weight_messages.votes.end(),
                                   post_add_messages.votes.begin(),
                                   post_add_messages.votes.end());
      weight_messages.certs.insert(weight_messages.certs.end(),
                                   post_add_messages.certs.begin(),
                                   post_add_messages.certs.end());

      // Re-check after the schedule drain: the next leader for this proposal
      // may change at a certified activation boundary.
      if (id_ == NextLeader(view)) {
        proposal_received_ = true;
      }
      // LOG(ERROR)<<"send cert view:"<<view<<" to:"<<NextLeader(view);
      if (qc_formed_) {
        // LOG(ERROR) << "after proposal recieeved";
        proposal_manager_->AddQC(std::move(formed_qc_));
        StartNewRound();
        qc_formed_ = proposal_received_ = false;
      }
    }
  }

  BroadcastWeightPluginMessages(weight_messages);
  if (stale_proposal) {
    return true;
  }
  if (!proposal_valid) {
    return false;
  }
  auto next_leader = proposal_manager_->GetLeader(view + 1);

  SendMessage(MessageType::Vote, *cert, next_leader);

  if (has_qc_evidence && evidence_recorder != nullptr) {
    evidence_recorder->RecordQc(qc_evidence_view, qc_evidence_hash,
                                qc_evidence_signer_bitmap, qc_evidence_leader_id,
                                qc_evidence_weight_version,
                                std::move(qc_evidence_active_weight_root),
                                qc_evidence_leader_eligible_min_weight);
  }

  return true;
}

bool HotStuff::ReceiveCertificate(std::unique_ptr<Certificate> cert) {
  if (cert == nullptr) {
    return false;
  }
  if (async_verifier_ != nullptr &&
      async_verifier_->TrySubmitVote(&cert)) {
    return true;
  }
  {
    std::unique_lock<std::mutex> lk(mutex_);
    if (cert == nullptr || !proposal_manager_->VerifyCert(*cert)) {
      return false;
    }
  }
  return ProcessVerifiedCertificate(std::move(cert));
}

bool HotStuff::ProcessVerifiedCertificate(std::unique_ptr<Certificate> cert) {
  if (cert == nullptr) {
    return false;
  }
  WeightPluginOutboundMessages weight_messages;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    // LOG(ERROR)<<"RECEIVE proposer cert :"<<cert->view()<<"
    // from:"<<cert->signer();
    int view = cert->view();
    const int local_current_view = proposal_manager_->CurrentView();
    if (view < local_current_view) {
      return true;
    }
    // Mutate active weight/leader state only at the local pacemaker boundary.
    // Vote aggregation below still uses the view-indexed weight schedule.
    weight_messages = DrainWeightPlugin(local_current_view);

    std::string hash = cert->hash();
    auto& certs = receive_[view][hash];
    int64_t previous_weight = CertificateWeight(certs, view);
    certs.insert(std::make_pair(cert->signer(), std::move(cert)));
    int64_t current_weight = CertificateWeight(certs, view);

    // LOG(ERROR)<<"RECEIVE proposer cert :"<<view<<" weight:"<<current_weight;
    const int64_t quorum_weight = weight_schedule_->QuorumWeightForView(view);
    if (previous_weight < quorum_weight && current_weight >= quorum_weight) {
      MarkTimeoutProgressLocked();
      const std::vector<QcSignerInfo> signer_infos =
          CertificateSignerInfos(certs, view);
      std::vector<int> selected_signers =
          qc_signer_cooldown_.SelectSignersForQc(signer_infos, quorum_weight,
                                                 static_cast<uint64_t>(view));
      int64_t selected_weight = 0;
      for (int signer : selected_signers) {
        selected_weight += WeightForSigner(signer, view);
      }
      if (selected_signers.empty() || selected_weight < quorum_weight) {
        selected_signers = CertificateSigners(certs);
      }
      std::unique_ptr<QC> qc = std::make_unique<QC>();
      qc->set_hash(hash);
      qc->set_view(view);
      qc->set_signer_bitmap(BuildSignerBitmap(selected_signers, total_num_));

      for (int signer : selected_signers) {
        auto it = certs.find(signer);
        if (it != certs.end()) {
          *qc->add_signatures() = it->second->sign();
        }
      }
      qc_signer_cooldown_.RecordQcSigners(selected_signers);

      qc_formed_ = true;
      if (proposal_received_) {
        // LOG(ERROR) << "after qc formed";
        proposal_manager_->AddQC(std::move(qc));
        StartNewRound();
        qc_formed_ = proposal_received_ = false;
      } else {
        formed_qc_ = std::move(qc);
      }
    }
    WeightPluginOutboundMessages post_cert_messages =
        DrainWeightPlugin(proposal_manager_->CurrentView());
    weight_messages.candidates.insert(weight_messages.candidates.end(),
                                      post_cert_messages.candidates.begin(),
                                      post_cert_messages.candidates.end());
    weight_messages.votes.insert(weight_messages.votes.end(),
                                 post_cert_messages.votes.begin(),
                                 post_cert_messages.votes.end());
    weight_messages.certs.insert(weight_messages.certs.end(),
                                 post_cert_messages.certs.begin(),
                                 post_cert_messages.certs.end());
  }
  BroadcastWeightPluginMessages(weight_messages);
  return true;
}

bool HotStuff::ReceiveTimeoutVote(std::unique_ptr<TimeoutVote> vote) {
  if (vote == nullptr || timeout_manager_ == nullptr ||
      !timeout_config_.enabled) {
    return false;
  }
  TimeoutVote local_vote;
  TimeoutCert cert;
  bool has_local_vote = false;
  bool formed_cert = false;
  bool advanced = false;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    const int current_view = proposal_manager_->CurrentView();
    if (vote->view() < current_view) {
      return true;
    }
    std::unique_ptr<TimeoutCert> maybe_cert = timeout_manager_->AddVote(*vote);
    if (maybe_cert != nullptr) {
      cert = *maybe_cert;
      formed_cert = true;
      advanced = ApplyTimeoutCertLocked(cert);
    }

    if (vote->signer() != id_ && vote->view() >= current_view &&
        timeout_echoed_views_.insert(vote->view()).second) {
      std::unique_ptr<TimeoutVote> maybe_local_vote =
          timeout_manager_->CreateTimeoutVote(vote->view(),
                                              proposal_manager_->HighQC());
      if (maybe_local_vote != nullptr) {
        local_vote = *maybe_local_vote;
        has_local_vote = true;
        std::unique_ptr<TimeoutCert> local_cert =
            timeout_manager_->AddVote(*maybe_local_vote);
        if (local_cert != nullptr && !formed_cert) {
          cert = *local_cert;
          formed_cert = true;
          advanced = ApplyTimeoutCertLocked(cert);
        }
      }
    }
  }
  if (has_local_vote) {
    BroadcastTimeoutVote(local_vote);
  }
  if (formed_cert) {
    BroadcastTimeoutCert(cert);
  }
  if (advanced) {
    StartNewRound();
  }
  return true;
}

bool HotStuff::ReceiveTimeoutCert(std::unique_ptr<TimeoutCert> cert) {
  if (cert == nullptr || timeout_manager_ == nullptr ||
      !timeout_config_.enabled) {
    return false;
  }
  bool advanced = false;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    advanced = ApplyTimeoutCertLocked(*cert);
  }
  if (advanced) {
    StartNewRound();
  }
  return advanced;
}

void HotStuff::BroadcastTimeoutVote(const TimeoutVote& vote) {
  if (broadcast_call_ != nullptr) {
    broadcast_call_(MessageType::TimeoutVoteMsg, vote);
  }
}

void HotStuff::BroadcastTimeoutCert(const TimeoutCert& cert) {
  if (broadcast_call_ != nullptr) {
    broadcast_call_(MessageType::TimeoutCertMsg, cert);
  }
}

bool HotStuff::ApplyTimeoutCertLocked(const TimeoutCert& cert) {
  if (timeout_manager_ == nullptr ||
      !timeout_manager_->VerifyTimeoutCert(cert)) {
    return false;
  }
  if (!proposal_manager_->AdvanceToViewByTimeout(cert)) {
    return false;
  }
  // A valid TC is deterministic evidence that the scheduled leader for the
  // timed-out view had an opportunity but did not produce a certified proposal.
  RecordLeaderOpportunityLocked(cert.view());
  qc_formed_ = false;
  proposal_received_ = false;
  formed_qc_.reset();
  has_sent_ = false;
  MarkTimeoutProgressLocked();
  timeout_empty_proposal_until_view_ =
      proposal_manager_->CurrentView() + timeout_empty_proposal_budget_views_;
  timeout_manager_->ResetBelowView(proposal_manager_->CurrentView());
  for (auto it = timeout_echoed_views_.begin();
       it != timeout_echoed_views_.end();) {
    if (*it < proposal_manager_->CurrentView()) {
      it = timeout_echoed_views_.erase(it);
    } else {
      ++it;
    }
  }
  LOG_EVERY_N(INFO, 1000) << "TD-Hotstuff timeout advanced to view:"
                          << proposal_manager_->CurrentView()
                          << " by tc view:" << cert.view();
  return true;
}

bool HotStuff::ReceiveWeightUpdateCandidate(
    std::unique_ptr<CandidateWeightUpdate> candidate) {
  if (candidate == nullptr || weight_update_manager_ == nullptr ||
      !weight_update_manager_->enabled()) {
    return false;
  }
  WeightPluginOutboundMessages weight_messages;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    const int current_view = proposal_manager_->CurrentView();
    weight_update_manager_->HandleCandidate(
        *candidate, CurrentWeightSnapshot(current_view));
    weight_messages = DrainWeightPlugin(current_view, /*force=*/true);
  }
  BroadcastWeightPluginMessages(weight_messages);
  return true;
}

bool HotStuff::ReceiveWeightUpdateVote(std::unique_ptr<WeightUpdateVote> vote) {
  if (vote == nullptr || weight_update_manager_ == nullptr ||
      !weight_update_manager_->enabled()) {
    return false;
  }
  WeightPluginOutboundMessages weight_messages;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    const int current_view = proposal_manager_->CurrentView();
    weight_update_manager_->HandleVote(*vote,
                                       CurrentWeightSnapshot(current_view));
    weight_messages = DrainWeightPlugin(current_view, /*force=*/true);
  }
  BroadcastWeightPluginMessages(weight_messages);
  return true;
}

bool HotStuff::ReceiveWeightUpdateCert(std::unique_ptr<WeightUpdateCert> cert) {
  if (cert == nullptr || weight_update_manager_ == nullptr ||
      !weight_update_manager_->enabled()) {
    return false;
  }
  WeightPluginOutboundMessages weight_messages;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    const int current_view = proposal_manager_->CurrentView();
    weight_update_manager_->HandleCert(*cert,
                                       CurrentWeightSnapshot(current_view));
    weight_messages = DrainWeightPlugin(current_view, /*force=*/true);
  }
  BroadcastWeightPluginMessages(weight_messages);
  return true;
}

void HotStuff::CommitProposal(std::unique_ptr<Proposal> p) {
  commit_q_.Push(std::move(p));
}

std::unique_ptr<Certificate> HotStuff::GenerateCertificate(
    const Proposal& proposal) {
  std::unique_ptr<Certificate> cert = std::make_unique<Certificate>();
  cert->set_hash(proposal.hash());
  cert->set_view(proposal.header().view());
  cert->set_signer(id_);

  auto hash_signature_or = verifier_->SignMessage(VoteSignaturePayload(*cert));
  if (!hash_signature_or.ok()) {
    LOG(ERROR) << "Sign message fail";
    return nullptr;
  }
  *cert->mutable_sign() = *hash_signature_or;
  return cert;
}

}  // namespace td_hotstuff
}  // namespace resdb
