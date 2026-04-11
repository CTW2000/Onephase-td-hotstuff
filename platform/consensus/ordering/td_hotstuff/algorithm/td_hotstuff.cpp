#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff.h"

#include <glog/logging.h>
#include "common/utils/utils.h"


namespace resdb {
namespace td_hotstuff {

TdHotstuff::TdHotstuff(int id, int f, int total_num, SignatureVerifier * verifier, int non_responsive_num, int fork_tail_num, int rollback_num, uint64_t timer_length, const std::vector<int>& weights)
  : ProtocolBase(id, f, total_num), verifier_(verifier), non_responsive_num_(non_responsive_num), fork_tail_num_(fork_tail_num), rollback_num_(rollback_num), timer_length_(timer_length), weights_(weights){

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

int TdHotstuff::NextLeader(int view){
  //LOG(ERROR)<<" view:"<<view<<" next leader:"<<(view+1)%total_num_ + 1;
  return (view+1)%total_num_ + 1;
}

bool TdHotstuff::IsLeader(int view){
  return (view % total_num_)+1 == id_;
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
    auto txn = txns_.Pop();
    if(txn == nullptr){
      continue;
    }

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

    // if (slot == 0 && proposal_manager_->CurrentView() > total_num_) {
    //   popped = false;
    // }
    // if (popped == false && !txns_.Empty()) {
    //   txn = txns_.Pop();
    //   popped = true;
    // }

    if (slot == 0 && proposal_manager_->CurrentView() > total_num_) {
      while (true) {
        txn = txns_.Pop();
        if (txn != nullptr) {
          break;
        }
        // count ++;
      }
    }
    

    std::vector<std::unique_ptr<Transaction> > txns;
    txns.push_back(std::move(txn));

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
      // LOG(ERROR) << "LOCK1" << GetCurrentTime();
      bool is_final = GetCurrentTime() - start_time >= timer_length_ ? true : false;
      // bool is_final = false;
      proposal = proposal_manager_ -> GenerateProposal(txns, slot, is_final);
      if(id_ < 3 * rollback_num_ && id_ % 3 ==1 && slot == 0) {
        proposal2 = proposal_manager_ -> GenerateFakeProposal(txns, slot, is_final);
      } 
      // LOG(ERROR)<<"[X]propose view:"<<proposal->header().view() << " slot: " << slot << " " <<GetCurrentTime();
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
  //  std::unique_lock<std::mutex> lk(txn_mutex_);
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
        for(Transaction& txn : *committed_p->mutable_transactions()){
          txn.set_next_primary(sender % total_num_ + 1);
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
    // Early exit: if we've already accumulated enough weight, or this is a stale view/slot
    if(accumulated_weight_ >= weight_threshold_ || view < min_view_ || (view == min_view_ && slot <= min_slot_)) {
      return false;
    }
    accumulated_weight_ += signer_weight;
  }
  bool valid = proposal_manager_->VerifyCert(*cert);
  if(!valid){
    LOG(ERROR) << "Verify message fail";
    assert(1==0);
    return false;
  }

  uint64_t x = GetCurrentTime();
  std::unique_lock<std::mutex> lk(rec_mutex_);

  // Deduplicate: only count each signer once
  if (receive_[view][hash].count(signer)) {
    return false;
  }

  receive_[view][hash].insert(std::make_pair(cert->signer(), std::move(cert)));

  // Accumulate weight for this (view, hash) pair
  received_weight_[view][hash] += signer_weight;
  int current_weight = received_weight_[view][hash];

  // LOG(ERROR) << "TD-HotStuff cert: view=" << view << " signer=" << signer
  //            << " weight=" << signer_weight << " accumulated=" << current_weight
  //            << "/" << weight_threshold_ << " (count=" << receive_[view][hash].size() << ")";

  // Check if weighted quorum is reached: accumulated weight > 2/3 * W
  if(current_weight >= weight_threshold_){
    {
      std::unique_lock<std::mutex> lk(rec_mutex2_);
      min_view_ = view;
      min_slot_ = slot;
      accumulated_weight_ = 0;
    }

    LOG(ERROR) << "TD-HotStuff: weighted QC formed! view=" << view << " slot=" << slot
               << " weight=" << current_weight << "/" << total_weight_
               << " threshold=" << weight_threshold_
               << " signers=" << receive_[view][hash].size();

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