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

int DefaultLeaderForView(int view, int total_replicas) {
  return RoundRobinLeaderForView(view, total_replicas);
}

ProposalManager::ProposalManager(int32_t id, int limit_count,
                                 SignatureVerifier* verifier, int total_num,
                                 int non_responsive_num, int fork_tail_num,
                                 const std::vector<int64_t>& replica_weights,
                                 int64_t quorum_weight,
                                 std::shared_ptr<WeightSchedule> weight_schedule,
                                 std::shared_ptr<LeaderSelectionSchedule> leader_schedule)
    : id_(id),
      limit_count_(limit_count),
      total_num_(total_num),
      non_responsive_num_(non_responsive_num),
      fork_tail_num_(fork_tail_num),
      replica_weights_(NormalizeReplicaWeights(replica_weights, total_num)),
      quorum_weight_(quorum_weight > 0 ? quorum_weight
                                       : CalculateQuorumWeight(replica_weights_)),
      weight_schedule_(weight_schedule != nullptr
                           ? weight_schedule
                           : std::make_shared<WeightSchedule>(total_num,
                                                              replica_weights_)),
      leader_schedule_(leader_schedule != nullptr
                           ? leader_schedule
                           : std::make_shared<LeaderSelectionSchedule>(
                                 total_num, replica_weights_, /*enabled=*/false)),
      verifier_(verifier),
      certificate_verifier_(total_num, verifier, weight_schedule_) {
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

bool ProposalManager::VerifyProposalSignature(const Proposal& proposal) {
  std::string error;
  if (!certificate_verifier_.VerifyProposalSignature(proposal, &error)) {
    LOG(ERROR) << "proposal signature invalid: " << error;
    return false;
  }
  return true;
}

bool ProposalManager::VerifyLeader(const Proposal& proposal) {
  const int view = proposal.header().view();
  const int expected_leader = GetLeader(view);
  if (expected_leader <= 0 || proposal.sender() != expected_leader) {
    LOG(ERROR) << "proposal leader mismatch, view:" << view
               << " sender:" << proposal.sender()
               << " expected:" << expected_leader;
    return false;
  }
  return true;
}

bool ProposalManager::VerifyLeaderContext(const Proposal& proposal) {
  const std::string expected =
      leader_schedule_ == nullptr
          ? std::string()
          : leader_schedule_->ContextHashForView(proposal.header().view());
  if (proposal.header().leader_context_hash() != expected) {
    LOG(ERROR) << "proposal leader context mismatch, view:"
               << proposal.header().view();
    return false;
  }
  return true;
}

int64_t ProposalManager::WeightForSigner(int signer, int view) const {
  if (weight_schedule_ != nullptr) {
    return weight_schedule_->WeightForSigner(signer, view);
  }
  if (signer < 1 || signer > static_cast<int>(replica_weights_.size())) {
    return 0;
  }
  return replica_weights_[signer - 1];
}

int64_t ProposalManager::QuorumWeightForView(int view) const {
  if (weight_schedule_ != nullptr) {
    return weight_schedule_->QuorumWeightForView(view);
  }
  return quorum_weight_;
}

bool ProposalManager::VerifyCert(const Certificate& cert) {
  std::string error;
  if (!certificate_verifier_.VerifyVote(cert, &error)) {
    LOG(ERROR) << "cert invalid: " << error;
    return false;
  }
  return true;
}


bool ProposalManager::VerifyQC(const QC& qc) {
  std::string error;
  if (!certificate_verifier_.VerifyQC(qc, &error)) {
    LOG(ERROR) << "qc invalid: " << error;
    return false;
  }
  return true;
}

bool ProposalManager::VerifyQcForEvidence(const QC& qc) {
  return VerifyQC(qc);
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

  if (!VerifyProposalSignature(proposal)) {
    return false;
  }

  if (!VerifyLeader(proposal)) {
    return false;
  }

  if (!VerifyLeaderContext(proposal)) {
    return false;
  }

  if (proposal.header().has_timeout_cert() &&
      !VerifyTimeoutCert(proposal.header().timeout_cert())) {
    return false;
  }

  if(proposal.header().view() == 1){
    return true;
  }

  if (SafeNode(proposal)) {
    return VerifyQC(proposal.header().qc());
  }
  return VerifyTimeoutJustification(proposal);
}

bool ProposalManager::VerifyEnvelopeForEvidence(const Proposal& proposal) {
  return VerifyHash(proposal) && VerifyProposalSignature(proposal) &&
         VerifyLeader(proposal) && VerifyLeaderContext(proposal);
}

std::unique_ptr<Proposal> ProposalManager::GenerateProposal(
    const std::vector<std::unique_ptr<Transaction>>& txns) {
  std::unique_ptr<Proposal> proposal = std::make_unique<Proposal>();
  int proposal_view = 0;
  {
    std::unique_lock<std::mutex> lk(txn_mutex_);
    if (signed_proposal_for_view_ != nullptr &&
        signed_proposal_view_ == round_) {
      return std::make_unique<Proposal>(*signed_proposal_for_view_);
    }
    for(const auto& txn: txns){
      global_stats_->AddProposeLatency(GetCurrentTime() - txn->reception_time());
      *proposal->add_transactions() = *txn;
    }
    if(!generic_qc_.hash().empty()){
     //LOG(ERROR)<<" add qc:"<<generic_qc_.view();
      proposal->mutable_header()->set_prehash(generic_qc_.hash());
      *proposal->mutable_header()->mutable_qc() = generic_qc_;
    }
    if (highest_timeout_cert_.view() > 0 &&
        highest_timeout_cert_.view() + 1 == round_) {
      *proposal->mutable_header()->mutable_timeout_cert() =
          highest_timeout_cert_;
    }

    proposal->mutable_header()->set_view(round_);
    const std::string leader_context =
        leader_schedule_ == nullptr ? std::string()
                                    : leader_schedule_->ContextHashForView(round_);
    proposal->mutable_header()->set_leader_context_hash(leader_context);
    proposal_view = round_;
    proposal->set_sender(id_);
  }
  proposal->set_createtime(GetCurrentTime());
  proposal->set_hash(GetHash(*proposal));
  auto signature_or = verifier_->SignMessage(ProposalSignaturePayload(*proposal));
  if (!signature_or.ok()) {
    LOG(ERROR) << "failed to sign TD-Hotstuff proposal view:" << proposal_view;
    return nullptr;
  }
  *proposal->mutable_signature() = *signature_or;
  {
    std::unique_lock<std::mutex> lk(txn_mutex_);
    if (signed_proposal_for_view_ != nullptr &&
        signed_proposal_view_ == proposal_view) {
      return std::make_unique<Proposal>(*signed_proposal_for_view_);
    }
    signed_proposal_view_ = proposal_view;
    signed_proposal_for_view_ = std::make_unique<Proposal>(*proposal);
  }
  return proposal;
}

int ProposalManager::CurrentView(){
  return round_;
}

const QC& ProposalManager::HighQC() const {
  return generic_qc_;
}

const TimeoutCert& ProposalManager::HighestTimeoutCert() const {
  return highest_timeout_cert_;
}

bool ProposalManager::VerifyTimeoutCert(const TimeoutCert& cert) {
  std::string error;
  if (!certificate_verifier_.VerifyTimeoutCert(cert, &error)) {
    LOG(ERROR) << "invalid TD-Hotstuff timeout cert: " << error;
    return false;
  }
  return true;
}

bool ProposalManager::VerifyTimeoutJustification(const Proposal& proposal) {
  if (!proposal.header().has_timeout_cert()) {
    LOG(ERROR) << "proposal is not safe and has no timeout cert";
    return false;
  }
  const TimeoutCert& cert = proposal.header().timeout_cert();
  if (cert.view() + 1 != proposal.header().view()) {
    LOG(ERROR) << "proposal timeout cert view mismatch, proposal view:"
               << proposal.header().view() << " tc view:" << cert.view();
    return false;
  }
  if (!VerifyTimeoutCert(cert)) {
    return false;
  }
  if (cert.high_qc().view() < lock_qc_.view()) {
    LOG(ERROR) << "timeout cert high qc is behind local lock qc";
    return false;
  }
  if (cert.high_qc().hash().empty()) {
    return lock_qc_.view() == 0;
  }
  if (proposal.header().qc().view() != cert.high_qc().view() ||
      proposal.header().qc().hash() != cert.high_qc().hash() ||
      proposal.header().qc().signer_bitmap() != cert.high_qc().signer_bitmap()) {
    LOG(ERROR) << "proposal qc does not match timeout cert high qc";
    return false;
  }
  return VerifyQC(proposal.header().qc());
}

bool ProposalManager::RecordVote(const Proposal& proposal, std::string* error) {
  return safety_rules_.RecordVote(proposal, error);
}

bool ProposalManager::AdvanceToViewByTimeout(const TimeoutCert& cert) {
  if (!VerifyTimeoutCert(cert)) {
    return false;
  }
  const int next_view = cert.view() + 1;
  if (next_view <= round_) {
    return false;
  }
  if (!cert.high_qc().hash().empty() &&
      generic_qc_.view() < cert.high_qc().view()) {
    generic_qc_ = cert.high_qc();
  }
  highest_timeout_cert_ = cert;
  round_ = next_view;
  return true;
}

void ProposalManager::AddQC(std::unique_ptr<QC> qc){
  std::unique_lock<std::mutex> lk(txn_mutex_);
  if(generic_qc_.view() == 0 || generic_qc_.view() < qc->view()){
    generic_qc_ = *qc;
    round_ = qc->view()+1;
  //  LOG(ERROR)<<"get new round:"<<round_;
  }
}

std::vector<std::unique_ptr<Proposal>> ProposalManager::AddProposal(std::unique_ptr<Proposal> proposal){
  //LOG(ERROR)<<"ADD PROPOSAL";
  std::unique_lock<std::mutex> lk(txn_mutex_);
  if(generic_qc_.view() < proposal->header().qc().view() &&
     VerifyQC(proposal->header().qc())){
    generic_qc_ = proposal->header().qc();
    round_ = std::max(round_, generic_qc_.view() + 1);
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

int ProposalManager::GetLeader(int view) {
  return leader_schedule_ != nullptr ? leader_schedule_->LeaderForView(view)
                                     : DefaultLeaderForView(view, total_num_);
}

}  // namespace td_hotstuff
}  // namespace resdb
