#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

#include "common/utils/utils.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/certificate_verifier.h"

namespace resdb {
namespace td_hotstuff {
namespace {

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

bool EnvFlagEnabled(const char* name) {
  const char* raw = std::getenv(name);
  if (raw == nullptr) {
    return false;
  }
  const std::string value(raw);
  return value == "1" || value == "true" || value == "TRUE" ||
         value == "yes" || value == "YES" || value == "on" ||
         value == "ON";
}

bool EnvListContainsId(const char* name, int id) {
  const char* raw = std::getenv(name);
  if (raw == nullptr || std::string(raw).empty()) {
    return false;
  }
  const std::string input(raw);
  size_t start = 0;
  while (start <= input.size()) {
    const size_t comma = input.find(',', start);
    const std::string item = input.substr(
        start, comma == std::string::npos ? std::string::npos : comma - start);
    if (!item.empty()) {
      try {
        if (std::stoi(item) == id) {
          return true;
        }
      } catch (...) {
      }
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  return false;
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
          std::make_shared<WeightSchedule>(total_num, replica_weights_)) {
  LOG(ERROR) << "id:" << id << " f:" << f << " total:" << total_num_;

  global_stats_ = Stats::GetGlobalStats();
  proposal_manager_ = std::make_unique<ProposalManager>(
      id, 2 * f_ + 1, verifier, total_num, non_responsive_num, fork_tail_num,
      replica_weights_, quorum_weight_, weight_schedule_);
  if (timeout_config_.enabled) {
    timeout_manager_ = std::make_unique<TimeoutManager>(
        id_, total_num_, verifier_, weight_schedule_);
  }
  async_verifier_ = std::make_unique<AsyncConsensusVerifier>(
      total_num_, verifier_, AsyncVerifierWorkerCountFromEnv(),
      AsyncVerifierQueueCapacityFromEnv(), weight_schedule_);
  async_verifier_->Start();
  timeout_empty_proposal_budget_views_ =
      timeout_config_.enabled
          ? PositiveIntFromEnv("TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS", total_num_)
          : 0;
  send_thread_ = std::thread(&HotStuff::AsyncSend, this);
  commit_thread_ = std::thread(&HotStuff::AsyncCommit, this);
  verified_event_thread_ = std::thread(&HotStuff::AsyncVerifiedEvents, this);
  if (timeout_manager_ != nullptr) {
    timeout_thread_ = std::thread(&HotStuff::AsyncTimeout, this);
  }
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
}

int HotStuff::LeaderForView(int view) {
  return proposal_manager_ != nullptr ? proposal_manager_->GetLeader(view)
                                      : DefaultLeaderForView(view, total_num_);
}

int HotStuff::NextLeader(int view) { return LeaderForView(view + 1); }

bool HotStuff::IsLeader(int view) { return LeaderForView(view) == id_; }

bool HotStuff::IsSilentLeaderForExperiment() const {
  return EnvFlagEnabled("TD_HS_SILENT_LEADER") ||
         EnvListContainsId("TD_HS_SILENT_LEADER_IDS", id_);
}

void HotStuff::MarkTimeoutProgressLocked() {
  ++timeout_progress_epoch_;
  timeout_progress_epoch_atomic_.store(timeout_progress_epoch_,
                                       std::memory_order_release);
}

bool HotStuff::Ready() {
  return proposal_manager_ != nullptr && IsLeader(proposal_manager_->CurrentView()) &&
         !has_sent_;
}

void HotStuff::StartNewRound() {
  std::unique_lock<std::mutex> lk(n_mutex_);
  has_sent_ = false;
  vote_cv_.notify_one();
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
      if (view != watched_view) {
        watched_view = view;
        last_vote_at = std::chrono::steady_clock::time_point::min();
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

    std::unique_ptr<Proposal> proposal;
    bool silent_leader = false;
    bool no_transactions_ready = false;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      const int view = proposal_manager_->CurrentView();
      if (!IsLeader(view) || has_sent_) {
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
      events = async_verifier_->WaitForVerified(std::chrono::milliseconds(100));
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
      ReceiveTimeoutVote(std::move(event.timeout_vote));
      return;
    case VerifiedConsensusEvent::Type::kTimeoutCert:
      ReceiveTimeoutCert(std::move(event.timeout_cert));
      return;
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
    const std::map<int, std::unique_ptr<Certificate>>& certs, int view) const {
  std::vector<int> signers;
  signers.reserve(certs.size());
  int64_t selected_weight = 0;
  const int64_t quorum_weight =
      weight_schedule_ != nullptr ? weight_schedule_->QuorumWeightForView(view)
                                  : quorum_weight_;
  for (const auto& entry : certs) {
    const int64_t weight = WeightForSigner(entry.first, view);
    if (weight <= 0) {
      continue;
    }
    signers.push_back(entry.first);
    selected_weight += weight;
    if (selected_weight >= quorum_weight) {
      break;
    }
  }
  return signers;
}

bool HotStuff::MaybeFormQcLocked(int view, const std::string& hash) {
  auto view_it = receive_.find(view);
  if (view_it == receive_.end()) {
    return false;
  }
  auto hash_it = view_it->second.find(hash);
  if (hash_it == view_it->second.end()) {
    return false;
  }
  auto& certs = hash_it->second;
  const int64_t quorum_weight =
      weight_schedule_ != nullptr ? weight_schedule_->QuorumWeightForView(view)
                                  : quorum_weight_;
  if (CertificateWeight(certs, view) < quorum_weight) {
    return false;
  }

  const std::vector<int> selected_signers = CertificateSigners(certs, view);
  if (selected_signers.empty()) {
    return false;
  }

  std::unique_ptr<QC> qc = std::make_unique<QC>();
  qc->set_hash(hash);
  qc->set_view(view);
  qc->set_signer_bitmap(BuildSignerBitmap(selected_signers, total_num_));
  qc->set_available_signer_bitmap(qc->signer_bitmap());
  qc->set_collector_id(id_);
  for (int signer : selected_signers) {
    auto it = certs.find(signer);
    if (it != certs.end()) {
      if (qc->slot() == 0) {
        qc->set_slot(it->second->slot());
      }
      *qc->add_signatures() = it->second->sign();
    }
  }

  qc_formed_ = true;
  if (proposal_received_) {
    proposal_manager_->AddQC(std::move(qc));
    StartNewRound();
    qc_formed_ = proposal_received_ = false;
  } else {
    formed_qc_ = std::move(qc);
  }
  return true;
}

bool HotStuff::ReceiveCertificate(std::unique_ptr<Certificate> cert) {
  if (cert == nullptr) {
    return false;
  }
  if (async_verifier_ != nullptr && async_verifier_->TrySubmitVote(&cert)) {
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
  {
    std::unique_lock<std::mutex> lk(mutex_);
    const int view = cert->view();
    const int local_current_view = proposal_manager_->CurrentView();
    if (view < local_current_view) {
      return true;
    }
    const std::string hash = cert->hash();
    auto& certs = receive_[view][hash];
    const int64_t previous_weight = CertificateWeight(certs, view);
    certs.insert(std::make_pair(cert->signer(), std::move(cert)));
    const int64_t current_weight = CertificateWeight(certs, view);
    const int64_t quorum_weight =
        weight_schedule_ != nullptr ? weight_schedule_->QuorumWeightForView(view)
                                    : quorum_weight_;
    if (previous_weight < quorum_weight && current_weight >= quorum_weight) {
      MarkTimeoutProgressLocked();
      MaybeFormQcLocked(view, hash);
    }
  }
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

bool HotStuff::ReceiveProposal(std::unique_ptr<Proposal> proposal) {
  if (proposal == nullptr) {
    return false;
  }
  const int view = proposal->header().view();
  std::unique_ptr<Certificate> cert;
  bool proposal_valid = true;
  bool stale_proposal = false;
  int next_leader = 0;
  {
    std::unique_lock<std::mutex> lk(mutex_);

    if (id_ == NextLeader(view)) {
      proposal_received_ = true;
    }

    if (proposal->header().has_timeout_cert()) {
      ApplyTimeoutCertLocked(proposal->header().timeout_cert());
    }
    const int local_current_view = proposal_manager_->CurrentView();
    if (view < local_current_view) {
      stale_proposal = true;
      proposal_valid = false;
    } else if (!proposal_manager_->Verify(*proposal)) {
      LOG(ERROR) << "proposal invalid";
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
      cert = GenerateCertificate(*proposal);
      if (cert == nullptr) {
        proposal_valid = false;
      } else {
        std::vector<std::unique_ptr<Proposal>> committed_p_list =
            proposal_manager_->AddProposal(std::move(proposal));
        for (int i = static_cast<int>(committed_p_list.size()) - 1; i >= 0;
             --i) {
          CommitProposal(std::move(committed_p_list[i]));
        }

        if (id_ == NextLeader(view)) {
          proposal_received_ = true;
        }
        if (qc_formed_) {
          proposal_manager_->AddQC(std::move(formed_qc_));
          StartNewRound();
          qc_formed_ = proposal_received_ = false;
        }
        next_leader = proposal_manager_->GetLeader(view + 1);
      }
    }
  }

  if (stale_proposal) {
    return true;
  }
  if (!proposal_valid || cert == nullptr) {
    return false;
  }
  SendMessage(MessageType::Vote, *cert, next_leader);
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
  cert->set_slot(proposal.header().slot());

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
