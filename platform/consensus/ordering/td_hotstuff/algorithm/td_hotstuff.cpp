#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff.h"

#include <glog/logging.h>
#include "common/utils/utils.h"


namespace resdb {
namespace td_hotstuff {

HotStuff::HotStuff(int id, int f, int total_num, SignatureVerifier * verifier, int non_responsive_num, int fork_tail_num, uint64_t timer_length, const std::vector<int64_t>& replica_weights, int64_t quorum_weight)
  : ProtocolBase(id, f, total_num), verifier_(verifier), non_responsive_num_(non_responsive_num), fork_tail_num_(fork_tail_num), timer_length_(timer_length), replica_weights_(NormalizeReplicaWeights(replica_weights, total_num)), quorum_weight_(quorum_weight > 0 ? quorum_weight : CalculateQuorumWeight(replica_weights_)) {

    LOG(ERROR)<<"id:"<<id<<" f:"<<f<<" total:"<<total_num_;

  global_stats_ = Stats::GetGlobalStats();
  proposal_manager_ = std::make_unique<ProposalManager>(id, 2*f_+1, verifier, total_num, non_responsive_num, fork_tail_num, replica_weights_, quorum_weight_);
  qc_evidence_recorder_ = AsyncQcEvidenceRecorder::CreateFromEnv(id_, total_num_, replica_weights_);
  has_sent_ = false;
    send_thread_ = std::thread(&HotStuff::AsyncSend, this);
    commit_thread_ = std::thread(&HotStuff::AsyncCommit, this);
    batch_size_ = 1;
  qc_formed_ = proposal_received_ = false;
}

HotStuff::~HotStuff() {
  if (qc_evidence_recorder_ != nullptr) {
    qc_evidence_recorder_->Stop();
  }
}

int HotStuff::NextLeader(int view){
  //LOG(ERROR)<<" view:"<<view<<" next leader:"<<(view+1)%total_num_ + 1;
  return (view+1)%total_num_ + 1;
}

bool HotStuff::IsLeader(int view){
  return (view % total_num_)+1 == id_;
}

bool HotStuff::Ready() {
  int view = proposal_manager_->CurrentView();
  return IsLeader(view) && !has_sent_;
}

void HotStuff::StartNewRound() {
  std::unique_lock<std::mutex> lk(n_mutex_);
  has_sent_ = false;
  vote_cv_.notify_one();
  //LOG(ERROR)<<" start new round";
}

void HotStuff::AsyncSend() {
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
        break;
      }
    }
    if(IsStop()){
      return;
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

    if (id_ < 3 * non_responsive_num_ && id_ % 3 == 1) {
      usleep(timer_length_);
    }

    std::unique_ptr<Proposal> proposal =  nullptr;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      proposal = proposal_manager_ -> GenerateProposal(txns);
      //LOG(ERROR)<<"propose view:"<<proposal->header().view();
    }
    has_sent_ = true;
    broadcast_call_(MessageType::NewProposal, *proposal);
  }
}

void HotStuff::AsyncCommit() {
  int seq = 1;
  while (!IsStop()) {
    auto p = commit_q_.Pop();
    if(p == nullptr){
      continue;
    }
    global_stats_->AddConsensusLatency(GetCurrentTime()-p->createtime());
    // LOG(ERROR) << "create time: " << p->createtime();
    for(Transaction& txn : *p->mutable_transactions()){
      txn.set_id(seq++);
      Commit(txn);
    }
  }
}


bool HotStuff::ReceiveTransaction(std::unique_ptr<Transaction> txn) {
  //  std::unique_lock<std::mutex> lk(txn_mutex_);
  txn->set_reception_time(GetCurrentTime());
  txn->set_proposer(id_);
  txns_.Push(std::move(txn));
  return true;
}

int64_t HotStuff::WeightForSigner(int signer) const {
  if (signer < 1 || signer > static_cast<int>(replica_weights_.size())) {
    return 0;
  }
  return replica_weights_[signer - 1];
}

int64_t HotStuff::CertificateWeight(
    const std::map<int, std::unique_ptr<Certificate>>& certs) const {
  int64_t total_weight = 0;
  for (const auto& entry : certs) {
    total_weight += WeightForSigner(entry.first);
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

bool HotStuff::ReceiveProposal(std::unique_ptr<Proposal> proposal) {
  if (IsSlowReplica(id_)) {
    usleep(GetRandomDelay());
  }
    int view = proposal->header().view();
    std::unique_ptr<Certificate> cert = nullptr;
  {
    // LOG(ERROR)<<"RECEIVE proposer view:"<<proposal->header().view() << " from: " << proposal->sender();
    std::unique_lock<std::mutex> lk(mutex_);

    if (id_ == NextLeader(view)) {
      proposal_received_ = true;
    }

    if(!proposal_manager_->Verify(*proposal)){
      LOG(ERROR)<<" proposal invalid";
      return false;
    }

    if (qc_evidence_recorder_ != nullptr) {
      const QC& qc = proposal->header().qc();
      qc_evidence_recorder_->RecordQc(qc.view(), qc.hash(),
                                      qc.signer_bitmap());
    }

    cert = GenerateCertificate(*proposal);
    assert(cert != nullptr);

    std::vector<std::unique_ptr<Proposal>> committed_p_list = proposal_manager_->AddProposal(std::move(proposal));
    for (int i=committed_p_list.size()-1; i>=0; i--) {
      // LOG(ERROR) << "commit view: " << committed_p_list[i]->header().view();
      CommitProposal(std::move(committed_p_list[i]));
    }

    //LOG(ERROR)<<"send cert view:"<<view<<" to:"<<NextLeader(view);
    if (qc_formed_) {
      // LOG(ERROR) << "after proposal recieeved";
      proposal_manager_->AddQC(std::move(formed_qc_));
      StartNewRound();
      qc_formed_ = proposal_received_ = false;
    }
  }

  auto next_leader = proposal_manager_->GetLeader(view+1);

  SendMessage(MessageType::Vote, *cert, next_leader);

  if (next_leader % 3 == 1 && next_leader <= 3 * crash_num_) {
    int next_next_leader = proposal_manager_->GetLeader(view+2);
    if (id_ == next_next_leader) {
      proposal_received_ = true;
    }
    cert->set_view(view+1);
    usleep(timer_length_);
    SendMessage(MessageType::Vote, *cert, next_next_leader);
  }

  return true;
}

bool HotStuff::ReceiveCertificate(std::unique_ptr<Certificate> cert) {

  if (id_ % 3 == 1 && id_ <= 3*crash_num_) {
    return true;
  }

  std::unique_lock<std::mutex> lk(mutex_);
  //LOG(ERROR)<<"RECEIVE proposer cert :"<<cert->view()<<" from:"<<cert->signer();
  bool valid = proposal_manager_->VerifyCert(*cert);
  if(!valid){
    LOG(ERROR) << "Verify message fail";
    assert(1==0);
    return false;
  }

  int view = cert->view();
  std::string hash = cert->hash();
  auto& certs = receive_[view][hash];
  int64_t previous_weight = CertificateWeight(certs);
  certs.insert(std::make_pair(cert->signer(), std::move(cert)));
  int64_t current_weight = CertificateWeight(certs);

  //LOG(ERROR)<<"RECEIVE proposer cert :"<<view<<" weight:"<<current_weight;
  if(previous_weight < quorum_weight_ && current_weight >= quorum_weight_){
    std::unique_ptr<QC> qc = std::make_unique<QC>();
    qc->set_hash(hash);
    qc->set_view(view);
    qc->set_signer_bitmap(BuildSignerBitmap(CertificateSigners(certs), total_num_));

    for(auto & it: certs){
      *qc->add_signatures() = it.second->sign();
    }

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
  return true;
}

void HotStuff::CommitProposal(std::unique_ptr<Proposal> p){
  commit_q_.Push(std::move(p));
}

std::unique_ptr<Certificate> HotStuff::GenerateCertificate(const Proposal& proposal) {
  std::unique_ptr<Certificate> cert = std::make_unique<Certificate>();
  cert->set_hash(proposal.hash());
  cert->set_view(proposal.header().view());
  cert->set_signer(id_);

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
