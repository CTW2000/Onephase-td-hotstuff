#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"

#include <algorithm>
#include <set>

#include <glog/logging.h>
#include "common/utils/utils.h"
#include "common/crypto/signature_verifier.h"

namespace resdb {
namespace td_hotstuff {

std::vector<int64_t> NormalizeReplicaWeights(
    const std::vector<int64_t>& replica_weights, int total_num) {
  std::vector<int64_t> weights(std::max(total_num, 0), 1);
  for (int i = 0; i < total_num && i < static_cast<int>(replica_weights.size());
       ++i) {
    weights[i] = std::max<int64_t>(replica_weights[i], 1);
  }
  return weights;
}

int64_t CalculateQuorumWeight(const std::vector<int64_t>& replica_weights) {
  int64_t total_weight = 0;
  for (int64_t weight : replica_weights) {
    total_weight += std::max<int64_t>(weight, 1);
  }
  return total_weight * 2 / 3 + 1;
}

std::string BuildSignerBitmap(const std::vector<int>& signers, int total_num) {
  if (total_num <= 0) {
    return "";
  }
  std::string bitmap((total_num + 7) / 8, '\0');
  for (int signer : signers) {
    if (signer < 1 || signer > total_num) {
      continue;
    }
    int bit = signer - 1;
    bitmap[bit / 8] = static_cast<char>(bitmap[bit / 8] | (1 << (bit % 8)));
  }
  return bitmap;
}

bool SignerBitmapMatchesSignatures(const QC& qc, int total_num) {
  const size_t expected_size = total_num <= 0 ? 0 : (total_num + 7) / 8;
  if (qc.signer_bitmap().size() != expected_size) {
    LOG(ERROR) << "qc signer bitmap size:" << qc.signer_bitmap().size()
               << " expected:" << expected_size;
    return false;
  }

  std::vector<int> signers;
  signers.reserve(qc.signatures_size());
  for (const auto& sign : qc.signatures()) {
    signers.push_back(sign.node_id());
  }
  return qc.signer_bitmap() == BuildSignerBitmap(signers, total_num);
}

ProposalManager::ProposalManager(int32_t id, int limit_count, SignatureVerifier* verifier, int total_num, int non_responsive_num, int fork_tail_num, const std::vector<int64_t>& replica_weights, int64_t quorum_weight)
    : id_(id), limit_count_(limit_count), total_num_(total_num), non_responsive_num_(non_responsive_num), fork_tail_num_(fork_tail_num), replica_weights_(NormalizeReplicaWeights(replica_weights, total_num)), quorum_weight_(quorum_weight > 0 ? quorum_weight : CalculateQuorumWeight(replica_weights_)), verifier_(verifier) {
    round_ = 1;
    global_stats_ = Stats::GetGlobalStats();
    assert(verifier_ != nullptr);
}


std::string ProposalManager::GetHash(const Proposal& proposal){
  std::string data;
  for(const auto& txn: proposal.transactions()){
    std::string tmp;
    txn.SerializeToString(&tmp);
    data += tmp;
  }

  std::string header_data;
  proposal.header().SerializeToString(&header_data);
  data +=  header_data;

  //LOG(ERROR)<<"get hash";
  return SignatureVerifier::CalculateHash(data);
}

bool ProposalManager::VerifyHash(const Proposal& proposal) {
  return  GetHash(proposal) == proposal.hash();
}

int64_t ProposalManager::WeightForSigner(int signer) const {
  if (signer < 1 || signer > static_cast<int>(replica_weights_.size())) {
    return 0;
  }
  return replica_weights_[signer - 1];
}

bool ProposalManager::VerifyCert(const Certificate& cert) {
  if (cert.signer() != cert.sign().node_id()) {
    LOG(ERROR) << "cert signer mismatch, signer:" << cert.signer()
               << " sign node:" << cert.sign().node_id();
    return false;
  }
  if (WeightForSigner(cert.signer()) <= 0) {
    LOG(ERROR) << "cert signer out of range:" << cert.signer();
    return false;
  }
  return verifier_->VerifyMessage(cert.hash(), cert.sign());
}


bool ProposalManager::VerifyQC(const QC& qc) {
  if (!SignerBitmapMatchesSignatures(qc, total_num_)) {
    LOG(ERROR) << "qc signer bitmap does not match signatures";
    return false;
  }

  int64_t total_weight = 0;
  std::set<int> seen_signers;
  for(const auto& sign : qc.signatures()){
    int signer = sign.node_id();
    if (WeightForSigner(signer) <= 0) {
      LOG(ERROR) << "qc has unknown signer:" << signer;
      return false;
    }
    if (!seen_signers.insert(signer).second) {
      LOG(ERROR) << "qc has duplicate signer:" << signer;
      return false;
    }
    bool valid = verifier_->VerifyMessage(qc.hash(), sign);
    if(!valid){
      LOG(ERROR) << "Verify message fail";
      return false;
    }
    total_weight += WeightForSigner(signer);
  }

  if (total_weight < quorum_weight_) {
    LOG(ERROR) << "qc weight:" << total_weight << " not enough, quorum:"
               << quorum_weight_ << " signatures:" << qc.signatures_size();
    return false;
  }
  return true;
}

bool ProposalManager::SafeNode(const Proposal& proposal){
  if(proposal.header().qc().view() > lock_qc_.view()){
    return true;
  }
  //LOG(ERROR)<<"qc view:"<<proposal.header().qc().view()<<" lock view:"<<lock_qc_.view()<<" safe fail";
  return false;
}

bool ProposalManager::Verify(const Proposal& proposal) {
  if( !VerifyHash(proposal)){
    LOG(ERROR)<<"hash not match:";
    return false;
  }

  if(proposal.header().view() == 1){
    return true;
  }

  if(!SafeNode(proposal)){
    return false;
  }
  return VerifyQC(proposal.header().qc());
}

std::unique_ptr<Proposal> ProposalManager::GenerateProposal(
    const std::vector<std::unique_ptr<Transaction>>& txns) {
  std::unique_ptr<Proposal> proposal = std::make_unique<Proposal>();
  {
    std::unique_lock<std::mutex> lk(txn_mutex_);
    for(const auto& txn: txns){
      global_stats_->AddProposeLatency(GetCurrentTime() - txn->reception_time());
      *proposal->add_transactions() = *txn;
    }
    if(!generic_qc_.hash().empty()){
     //LOG(ERROR)<<" add qc:"<<generic_qc_.view();
      proposal->mutable_header()->set_prehash(generic_qc_.hash());
      *proposal->mutable_header()->mutable_qc() = generic_qc_;
    }

    proposal->mutable_header()->set_view(round_);
    proposal->set_sender(id_);
  }
  proposal->set_createtime(GetCurrentTime());
  proposal->set_hash(GetHash(*proposal));
  return proposal;
}

int ProposalManager::CurrentView(){
  return round_;
}

void ProposalManager::AddQC(std::unique_ptr<QC> qc){
  std::unique_lock<std::mutex> lk(txn_mutex_);
  if(generic_qc_.view() == 0 || generic_qc_.view() < qc->view()){
    int leader = GetLeader(qc->view());
    if(!(leader < 3 * fork_tail_num_ && leader % 3 ==1)) {
      generic_qc_ = *qc;
    }
    round_ = qc->view()+1;
  //  LOG(ERROR)<<"get new round:"<<round_;
  }
}

std::vector<std::unique_ptr<Proposal>> ProposalManager::AddProposal(std::unique_ptr<Proposal> proposal){
  //LOG(ERROR)<<"ADD PROPOSAL";
  std::unique_lock<std::mutex> lk(txn_mutex_);
  if(generic_qc_.view() < proposal->header().qc().view()){
    generic_qc_ = proposal->header().qc();
  }

  std::vector<std::unique_ptr<Proposal>> commit_ready_proposals_;
  const Proposal * father = nullptr, * fafather = nullptr, * fafafather = nullptr;
  father = GetProposal(proposal->header().prehash());
  if(father != nullptr){
    //LOG(ERROR)<<"get father view:"<<father->header().view();
    fafather = GetProposal(father->header().prehash());
    lock_qc_ = father->header().qc();
    // if(fafather != nullptr && fafather->header().view() == father->header().view() - 1){
    if(fafather != nullptr && (fafather->header().view() == father->header().view() - 1 || fork_tail_num_ > 0)){
     // LOG(ERROR)<<"get fafather view:"<<fafather->header().view();
      fafafather = GetProposal(fafather->header().prehash());
      // if(fafafather != nullptr && fafafather->header().view() == fafather->header().view() - 1){
      if(fafafather != nullptr && fafafather->header().view() == fafather->header().view() - 1){
        auto fetched_proposal = FetchProposal(fafather->header().prehash());
          while (fetched_proposal) {
            auto got_proposal = fetched_proposal.get();
            commit_ready_proposals_.push_back(std::move(fetched_proposal));
            fetched_proposal = FetchProposal(got_proposal->header().prehash());
        }
      }
    }
  }
  local_block_[proposal->hash()] = std::move(proposal);
  return commit_ready_proposals_;
}

const Proposal * ProposalManager::GetProposal(const std::string& hash){
  auto it = local_block_.find(hash);
  if(it == local_block_.end()){
    return nullptr;
  }
  return it->second.get();
}

std::unique_ptr<Proposal> ProposalManager::FetchProposal(const std::string& hash){
  auto it = local_block_.find(hash);
  // assert(it != local_block_.end());
  if (it == local_block_.end()) {
    return nullptr;
  }
  std::unique_ptr<Proposal> ret = std::move(it->second);
  local_block_.erase(it);
  return ret;
}

int ProposalManager::GetLeader(int view){
  //LOG(ERROR)<<" view:"<<view<<" next leader:"<<(view+1)%total_num_ + 1;
  return view % total_num_ + 1;
}

}  // namespace td_hotstuff
}  // namespace resdb
