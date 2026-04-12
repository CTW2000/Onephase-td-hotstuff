#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff.h"

#include <glog/logging.h>
#include <sstream>
#include "common/utils/utils.h"
#include "common/crypto/hash.h"


namespace resdb {
namespace td_hotstuff {

TdHotstuff::TdHotstuff(int id, int f, int total_num, SignatureVerifier * verifier, int non_responsive_num, int fork_tail_num, int rollback_num, uint64_t timer_length, const std::vector<int>& weights)
  : ProtocolBase(id, f, total_num), verifier_(verifier), non_responsive_num_(non_responsive_num), fork_tail_num_(fork_tail_num), rollback_num_(rollback_num), timer_length_(timer_length), weights_(weights){

    // Scale timer_length_ with replica count so every view has enough
    // non-final slots for the commit chain to advance.
    // With more replicas the per-slot consensus round-trip is longer;
    // a timer that is too short makes slot 0 immediately final, leaving
    // only one proposal per view and starving the commit pipeline.
    // Formula: ensure at least 3 full consensus round-trips fit in the
    // timer window.  Each round-trip ≈ total_num * 500µs on localhost.
    uint64_t min_timer = (uint64_t)total_num_ * 1500;  // ~1.5ms per replica
    if (timer_length_ < min_timer) {
      LOG(ERROR) << "TD-HotStuff: scaling timer_length from " << timer_length_
                 << " to " << min_timer << " for " << total_num_ << " replicas";
      timer_length_ = min_timer;
    }

    // Compute total weight W and threshold
    total_weight_ = 0;
    for (int w : weights_) {
      total_weight_ += w;
    }
    // Threshold: strictly greater than 2/3 of total weight
    // weight_threshold_ = floor(2*W/3) + 1, so accumulated >= threshold means accumulated > 2W/3
    weight_threshold_ = (2 * total_weight_) / 3 + 1;

    LOG(ERROR) << "TD-HotStuff init: id=" << id << " f=" << f << " total=" << total_num_
               << " total_weight=" << total_weight_ << " weight_threshold=" << weight_threshold_
               << " non_responsive=" << non_responsive_num_;

    // Log per-replica weights
    for (int i = 0; i < (int)weights_.size(); i++) {
      LOG(ERROR) << "  replica " << (i+1) << " weight=" << weights_[i];
    }

    // Build prefix weight sums for VRF weighted leader election
    // prefix_weights_[0] = 0, prefix_weights_[i] = sum(weights_[0..i-1])
    prefix_weights_.resize(total_num_ + 1, 0);
    for (int i = 0; i < total_num_; i++) {
      prefix_weights_[i + 1] = prefix_weights_[i] + weights_[i];
    }
    LOG(ERROR) << "TD-HotStuff VRF leader election: epoch=" << epoch_
               << " total_weight=" << total_weight_;
    for (int i = 0; i < total_num_; i++) {
      LOG(ERROR) << "  replica " << (i+1) << " weight_interval=["
                 << prefix_weights_[i] << "/" << total_weight_ << ", "
                 << prefix_weights_[i+1] << "/" << total_weight_ << ")";
    }

    // Pre-compute first few leaders to log
    for (int v = 1; v <= std::min(5, total_num_); v++) {
      int leader = VRFLeader(v);
      LOG(ERROR) << "  VRF preview: view=" << v << " -> leader=" << leader;
    }

    // ── Trust-aware dynamic timeout (Pacemaker) ──
    // Δ(r) = Δ_base · (1 + κ · T̃(leader_r))
    // T̃(v) = T(v) / max{T(v')} ∈ (0, 1]
    // High-trust leader → longer timeout → fewer false view changes
    // Low-trust leader  → shorter timeout → faster fault detection
    kappa_ = 0.2;
    max_weight_ = 1;
    for (int w : weights_) {
      if (w > max_weight_) max_weight_ = w;
    }
    LOG(ERROR) << "TD-HS Pacemaker: kappa=" << kappa_
               << " max_weight=" << max_weight_
               << " base_timer=" << timer_length_ << "us";
    // Log dynamic timeouts for the first few views
    for (int v = 1; v <= std::min(5, total_num_); v++) {
      uint64_t dt = GetDynamicTimeout(v);
      int leader = VRFLeader(v);
      int lw = (leader >= 1 && leader <= (int)weights_.size()) ? weights_[leader-1] : 1;
      double t_norm = (double)lw / max_weight_;
      LOG(ERROR) << "  Pacemaker preview: view=" << v
                 << " leader=" << leader << "(w=" << lw << ")"
                 << " T_norm=" << t_norm
                 << " timeout=" << dt << "us"
                 << " (" << dt/1000.0 << "ms)";
    }

  global_stats_ = Stats::GetGlobalStats();
  proposal_manager_ = std::make_unique<ProposalManager>(id, weight_threshold_, slot_num_, verifier, total_num_, fork_tail_num_, rollback_num, weights_);
  has_sent_ = false;
  batch_size_ = 1;
  qc_formed_ = proposal_received_ = false;

  ready_ = IsLeader(1);
    send_thread_ = std::thread(&TdHotstuff::AsyncSend, this);
    commit_thread_ = std::thread(&TdHotstuff::AsyncCommit, this);

}

TdHotstuff::~TdHotstuff() {
}

// VRF-based weighted leader election:
// Computes SHA256(epoch || view) to get a deterministic pseudo-random value,
// then maps it onto the [0, total_weight) interval partitioned by replica weights.
// All nodes compute the same result for a given (epoch, view).
int TdHotstuff::VRFLeader(int view) {
  // Check cache first
  auto it = leader_cache_.find(view);
  if (it != leader_cache_.end()) {
    return it->second;
  }

  // Build VRF input: "epoch:view"
  std::string vrf_input = std::to_string(epoch_) + ":" + std::to_string(view);

  // Compute deterministic SHA256 hash (returns raw 32 bytes)
  std::string hash_raw = utils::CalculateSHA256Hash(vrf_input);

  // Read first 4 bytes as uint32 (big-endian) for uniform distribution
  uint32_t hash_val = 0;
  for (int i = 0; i < 4 && i < (int)hash_raw.size(); i++) {
    hash_val = (hash_val << 8) | (uint8_t)hash_raw[i];
  }

  // Map hash_val to [0, total_weight) range
  // position = hash_val % total_weight
  int position = (int)(hash_val % (uint32_t)total_weight_);

  // Find which replica's interval [prefix_weights_[i], prefix_weights_[i+1]) contains position
  int leader = 1;  // default
  for (int i = 0; i < total_num_; i++) {
    if (position >= prefix_weights_[i] && position < prefix_weights_[i + 1]) {
      leader = i + 1;  // node IDs are 1-based
      break;
    }
  }

  // Cache and return
  leader_cache_[view] = leader;
  return leader;
}

int TdHotstuff::NextLeader(int view){
  return VRFLeader(view + 1);
}

bool TdHotstuff::IsLeader(int view){
  return VRFLeader(view) == id_;
}

// Trust-aware dynamic timeout (Pacemaker)
//
// Δ(r) = Δ_base · (1 + κ · T̃(leader_r))
//
// T̃(v) = T(v) / max{T(v')} ∈ (0, 1]
//
// High-trust leader (T̃ close to 1) → Δ ≈ Δ_base · (1 + κ)
//   → longer wait before declaring view-change → fewer false timeouts
// Low-trust leader  (T̃ close to 0) → Δ ≈ Δ_base
//   → shorter wait → fast detection of faulty/slow leader
//
uint64_t TdHotstuff::GetDynamicTimeout(int view) {
  int leader = VRFLeader(view);
  int leader_weight = 1;
  if (leader >= 1 && leader <= (int)weights_.size()) {
    leader_weight = weights_[leader - 1];
  }
  // Normalized trust: T̃ = T(leader) / max_weight ∈ (0, 1]
  double t_norm = (double)leader_weight / max_weight_;
  // Dynamic timeout: Δ = Δ_base * (1 + κ * T̃)
  uint64_t dynamic_timeout = (uint64_t)(timer_length_ * (1.0 + kappa_ * t_norm));
  return dynamic_timeout;
}

bool TdHotstuff::Ready() {
  int view = proposal_manager_->CurrentView();
  // return IsLeader(view) && !has_sent_ && ready_;
  return IsLeader(view) && ready_;
}

void TdHotstuff::StartNewRound() {
  std::unique_lock<std::mutex> lk(n_mutex_);
  has_sent_ = false;
  vote_cv_.notify_one();
  //LOG(ERROR)<<" start new round";
}

void TdHotstuff::AsyncSend() {
  int slot = 0;
  bool popped = true;
  uint64_t start_time = GetCurrentTime();
  while (!IsStop()) {
    // First wait until we are the leader, THEN pop transactions.
    // This prevents consuming transactions while not being the leader,
    // which would block the client's backpressure mechanism.
    while(!IsStop()){
      std::unique_lock<std::mutex> lk(n_mutex_);
      vote_cv_.wait_for(lk, std::chrono::microseconds(1000),
          [&] { return Ready(); });

      if(Ready()){
        if (slot == 0) {
          start_time = GetCurrentTime();
          if (id_ % 3 == 1 && id_ < non_responsive_num_ * 3) {
            usleep(timer_length_);
          }
        }
        break;
      }
    }
    if(IsStop()){
      return;
    }

    // Now we know we're the leader — pop a transaction
    auto txn = txns_.Pop();
    if(txn == nullptr){
      // No transaction available yet — reset start_time so the timer
      // doesn't expire while we were waiting for a transaction.
      if (slot == 0) {
        start_time = GetCurrentTime();
      }
      continue;
    }

    // For views past the warmup phase, try to get the freshest transaction
    if (slot == 0 && proposal_manager_->CurrentView() > total_num_) {
      auto fresh = txns_.Pop();
      if (fresh != nullptr) {
        txn = std::move(fresh);
      }
    }
    

    std::vector<std::unique_ptr<Transaction> > txns;
    txns.push_back(std::move(txn));

    // Reset start_time when the first transaction of a new view arrives.
    // The Pop() call above may have blocked waiting for a transaction,
    // so the timer must start from when work actually begins, not from
    // when Ready() was detected.
    if (slot == 0) {
      start_time = GetCurrentTime();
    }

    for(int i = 1; i < batch_size_; ++i){
      auto txn = txns_.Pop();
      if(txn == nullptr){
        continue;
        //break;
      }
      txns.push_back(std::move(txn));
    }

    std::unique_ptr<Proposal> proposal =  nullptr;
    std::unique_ptr<Proposal> proposal2 = nullptr;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      // Trust-aware dynamic timeout: use GetDynamicTimeout(currentView)
      // instead of fixed timer_length_.
      int current_view = proposal_manager_->CurrentView();
      uint64_t view_timeout = GetDynamicTimeout(current_view);
      bool is_final = GetCurrentTime() - start_time >= view_timeout ? true : false;

      // Log the first FINAL decision for debugging
      if (is_final && slot > 0 && current_view <= 5) {
        int leader = VRFLeader(current_view);
        int lw = (leader >= 1 && leader <= (int)weights_.size()) ? weights_[leader-1] : 1;
        LOG(ERROR) << "TD-HS Pacemaker: view=" << current_view
                   << " FINAL at slot=" << slot
                   << " leader=" << leader << "(w=" << lw << ")"
                   << " timeout=" << view_timeout << "us"
                   << " elapsed=" << (GetCurrentTime() - start_time) << "us";
      }

      proposal = proposal_manager_ -> GenerateProposal(txns, slot, is_final);
      if(id_ < 3 * rollback_num_ && id_ % 3 ==1 && slot == 0) {
        proposal2 = proposal_manager_ -> GenerateFakeProposal(txns, slot, is_final);
      }
      slot = is_final ? 0 : slot + 1;
    }
    
    ready_ = false;
    if (proposal2 != nullptr) {
      LOG(ERROR) << "Rollbakc Attack Done by " << id_;
      for(int i = 1; i <= total_num_; i++) {
        if (i % 3 == 0 && i <= 3 * f_) {
          SendMessage(MessageType::NewProposal, *proposal2, i);
        } else {
          SendMessage(MessageType::NewProposal, *proposal, i);
        }
      }
    } else {
      broadcast_call_(MessageType::NewProposal, *proposal);
    }
  }
}

void TdHotstuff::AsyncCommit() {
  int seq = 1;
  while (!IsStop()) {
    auto p = commit_q_.Pop();
    if(p == nullptr){
      continue;
    }
    // LOG(ERROR)<<"[X]commit proposal view: "<<p->header().view()<<" slot: "<<p->header().slot();
    // if (p->header().slot() > 0) {
      global_stats_->AddConsensusLatency(GetCurrentTime()-p->createtime());
    // }
    for(Transaction& txn : *p->mutable_transactions()){
      txn.set_id(seq++);
      Commit(txn);
    }
  }
}


bool TdHotstuff::ReceiveTransaction(std::unique_ptr<Transaction> txn) {
  txn->set_reception_time(GetCurrentTime());
  txn->set_proposer(id_);
  txns_.Push(std::move(txn));
  return true;
}

bool TdHotstuff::ReceiveProposal(std::unique_ptr<Proposal> proposal) {
  if (IsSlowReplica(id_)) {
    usleep(GetRandomDelay());
  }
    int view = proposal->header().view();
    int slot = proposal->header().slot();
    bool is_final = proposal->header().is_final();
    int sender = proposal->sender();
    std::unique_ptr<Certificate> cert = nullptr;
  {
    // LOG(ERROR)<<"[X]process proposer view:"<<view<<" slot:"<<slot << " is_final: " << is_final << " at " << (GetCurrentTime() - last_time_) << " " << GetCurrentTime();
    last_time_ = GetCurrentTime();

    // LOG(ERROR)<<"RECEIVE proposer view:"<<proposal->header().view() << " slot: " << proposal->header().slot();
    std::unique_lock<std::mutex> lk(mutex_);
    if(!proposal_manager_->Verify(*proposal)){
      LOG(ERROR)<<" proposal invalid";
      return false;
    }

    cert = GenerateCertificate(*proposal);
    assert(cert != nullptr);
    
    std::unique_ptr<Proposal> committed_p = proposal_manager_->AddProposal(std::move(proposal));
    if(committed_p != nullptr){
      if (is_final) {
        // Use VRF to determine the next leader for the committed transactions
        int next_leader = NextLeader(view);
        for(Transaction& txn : *committed_p->mutable_transactions()){
          txn.set_next_primary(next_leader);
        }
      }
      CommitProposal(std::move(committed_p));
    }

    //LOG(ERROR)<<"send cert view:"<<view<<" to:"<<NextLeader(view);
  }

  if (is_final) {
    SendMessage(MessageType::Vote, *cert, NextLeader(view));
  } else {
    SendMessage(MessageType::Vote, *cert, sender);
  }

  return true;
}

bool TdHotstuff::ReceiveCertificate(std::unique_ptr<Certificate> cert) {
  // LOG(ERROR)<<"RECEIVE proposer cert :"<<cert->view()<<" from:"<<cert->signer();
  int view = cert->view();
  std::string hash = cert->hash();
  int slot = cert->slot();
  bool is_final = cert->is_final();
  int signer = cert->signer();

  // Look up the signer's weight (node IDs are 1-based)
  int signer_weight = 0;
  if (signer >= 1 && signer <= (int)weights_.size()) {
    signer_weight = weights_[signer - 1];
  } else {
    LOG(ERROR) << "TD-HotStuff: unknown signer id=" << signer << ", ignoring cert";
    return false;
  }

  {
    std::unique_lock<std::mutex> lk(rec_mutex2_);
    if(view < min_view_ || (view == min_view_ && slot <= min_slot_)) {
      return false;
    }
    // Fast-path: skip if QC already formed for this (view, slot).
    // Uses rec_mutex2_ (lightweight) instead of rec_mutex_ (heavy).
    int64_t key = (int64_t)view * 100000 + slot;
    if (qc_done_.count(key)) {
      return false;
    }
  }

  bool valid = proposal_manager_->VerifyCert(*cert);
  if(!valid){
    LOG(ERROR) << "Verify message fail";
    assert(1==0);
    return false;
  }

  uint64_t x = GetCurrentTime();
  std::unique_lock<std::mutex> lk(rec_mutex_);

  // Re-check after VerifyCert (another thread may have formed the QC)
  if (received_weight_.count(view) && received_weight_[view].count(hash)
      && received_weight_[view][hash] >= weight_threshold_) {
    return false;
  }

  // Deduplicate: only count each signer once
  if (receive_[view][hash].count(signer)) {
    return false;
  }

  receive_[view][hash].insert(std::make_pair(cert->signer(), std::move(cert)));

  // Accumulate weight for this (view, hash) pair
  received_weight_[view][hash] += signer_weight;
  int current_weight = received_weight_[view][hash];

  // Per-cert logging only during the first view for initial verification.
  if (view == 1 && slot == 0) {
    LOG(ERROR) << "TD-HS cert: view=" << view << " slot=" << slot
               << " signer=" << signer << " w=" << signer_weight
               << " acc=" << current_weight << "/" << weight_threshold_
               << " (n_sigs=" << receive_[view][hash].size() << ")"
               << (is_final ? " [FINAL]" : "");
  }

  // Check if weighted quorum is reached: accumulated weight > 2/3 * W
  if(current_weight >= weight_threshold_){
    {
      std::unique_lock<std::mutex> lk(rec_mutex2_);
      min_view_ = view;
      min_slot_ = slot;
      accumulated_weight_ = 0;
      // Mark this (view, slot) as QC-done for the fast-path
      int64_t key = (int64_t)view * 100000 + slot;
      qc_done_.insert(key);
      // GC: remove entries older than view-2
      if (view > 2) {
        int64_t gc_bound = (int64_t)(view - 2) * 100000;
        auto it = qc_done_.begin();
        while (it != qc_done_.end() && *it < gc_bound) {
          it = qc_done_.erase(it);
        }
      }
    }

    // Log QC formation: only FINAL QCs and view 1 for initial verification
    if (view == 1 || is_final) {
      std::string signer_str;
      for (auto & it : receive_[view][hash]) {
        if (!signer_str.empty()) signer_str += ",";
        int sid = it.first;
        int sw = (sid >= 1 && sid <= (int)weights_.size()) ? weights_[sid-1] : 0;
        signer_str += std::to_string(sid) + "(w" + std::to_string(sw) + ")";
      }

      LOG(ERROR) << "TD-HS QC formed! view=" << view << " slot=" << slot
                 << " weight=" << current_weight << "/" << total_weight_
                 << " threshold=" << weight_threshold_
                 << " signers=[" << signer_str << "]"
                 << (is_final ? " [FINAL->nextL=" + std::to_string(NextLeader(view)) + "]" : "");
    }

    std::unique_ptr<QC> qc = std::make_unique<QC>();
    qc->set_hash(hash);
    qc->set_view(view);
    qc->set_slot(slot);
    qc->set_is_final(is_final);

    for(auto & it: receive_[view][hash]){
      *qc->add_signatures() = it.second->sign();
    }

    proposal_manager_->AddQC(std::move(qc));
    ready_ = true;

    // Garbage-collect old view entries to prevent map bloat
    if (view > 2) {
      int gc_view = view - 2;
      receive_.erase(gc_view);
      received_weight_.erase(gc_view);
    }

    StartNewRound();
  }
  return true;
}

void TdHotstuff::CommitProposal(std::unique_ptr<Proposal> p){
  commit_q_.Push(std::move(p));
}

std::unique_ptr<Certificate> TdHotstuff::GenerateCertificate(const Proposal& proposal) {
  std::unique_ptr<Certificate> cert = std::make_unique<Certificate>();
  cert->set_hash(proposal.hash());
  cert->set_view(proposal.header().view());
  cert->set_slot(proposal.header().slot());
  cert->set_is_final(proposal.header().is_final());
  cert->set_signer(id_);

  // Attach this replica's trust weight to the certificate
  int my_weight = (id_ >= 1 && id_ <= (int)weights_.size()) ? weights_[id_ - 1] : 1;
  cert->set_weight(my_weight);

  std::string data_str = proposal.hash();
  auto hash_signature_or = verifier_->SignMessage(data_str);
  if (!hash_signature_or.ok()) {
    LOG(ERROR) << "Sign message fail";
    return nullptr;
  }
  *cert->mutable_sign()=*hash_signature_or;
  return cert;
}

}  // namespace td_hotstuff
}  // namespace resdb