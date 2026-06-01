#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff.h"

#include <glog/logging.h>
#include <sstream>
#include "common/utils/utils.h"


namespace resdb {
namespace td_hotstuff {
namespace {

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

HotStuff::HotStuff(int id, int f, int total_num, SignatureVerifier * verifier, int non_responsive_num, int fork_tail_num, uint64_t timer_length, const std::vector<int64_t>& replica_weights, int64_t quorum_weight)
  : ProtocolBase(id, f, total_num), verifier_(verifier), non_responsive_num_(non_responsive_num), fork_tail_num_(fork_tail_num), timer_length_(timer_length), replica_weights_(NormalizeReplicaWeights(replica_weights, total_num)), quorum_weight_(quorum_weight > 0 ? quorum_weight : CalculateQuorumWeight(replica_weights_)), weight_schedule_(std::make_shared<WeightSchedule>(total_num, replica_weights_)), qc_signer_cooldown_(QcSignerDiversityConfigFromEnv()) {

    LOG(ERROR)<<"id:"<<id<<" f:"<<f<<" total:"<<total_num_;

  global_stats_ = Stats::GetGlobalStats();
  leader_selection_schedule_ = std::make_shared<LeaderSelectionSchedule>(
      total_num_, replica_weights_, LeaderSelectionConfigFromEnv());
  proposal_manager_ = std::make_unique<ProposalManager>(id, 2*f_+1, verifier, total_num, non_responsive_num, fork_tail_num, replica_weights_, quorum_weight_, weight_schedule_, leader_selection_schedule_);
  weight_update_manager_ = std::make_unique<WeightUpdateManager>(id_, total_num_, verifier_, WeightUpdateConfigFromEnv());
  qc_evidence_recorder_ = AsyncQcEvidenceRecorder::CreateFromEnv(id_, total_num_, weight_schedule_->ActiveWeights());
  SyncReputationWeightsToActiveSchedule();
  has_sent_ = false;
    send_thread_ = std::thread(&HotStuff::AsyncSend, this);
    commit_thread_ = std::thread(&HotStuff::AsyncCommit, this);
    weight_plugin_broadcast_thread_ =
        std::thread(&HotStuff::AsyncBroadcastWeightPluginMessages, this);
    batch_size_ = 1;
  qc_formed_ = proposal_received_ = false;
}

HotStuff::~HotStuff() {
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

int HotStuff::NextLeader(int view){
  return LeaderForView(view + 1);
}

bool HotStuff::IsLeader(int view){
  return LeaderForView(view) == id_;
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

    std::unique_ptr<Proposal> proposal = nullptr;
    WeightPluginOutboundMessages weight_messages;
    bool still_leader = true;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      weight_messages = DrainWeightPlugin(proposal_manager_->CurrentView());
      still_leader = IsLeader(proposal_manager_->CurrentView());
      if (still_leader) {
        proposal = proposal_manager_ -> GenerateProposal(txns);
      }
      //LOG(ERROR)<<"propose view:"<<proposal->header().view();
    }
    if (!still_leader) {
      for (auto& pending_txn : txns) {
        if (pending_txn != nullptr) {
          txns_.Push(std::move(pending_txn));
        }
      }
      BroadcastWeightPluginMessages(weight_messages);
      continue;
    }
    has_sent_ = true;
    BroadcastWeightPluginMessages(weight_messages);
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
    const std::map<int, std::unique_ptr<Certificate>>& certs,
    int view) const {
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

WeightSnapshot HotStuff::CurrentWeightSnapshot(int current_view) const {
  return MakeWeightSnapshot(*weight_schedule_, current_view);
}

WeightPluginOutboundMessages HotStuff::DrainWeightPlugin(int current_view) {
  WeightPluginOutboundMessages messages;
  if (weight_update_manager_ == nullptr || !weight_update_manager_->enabled()) {
    return messages;
  }

  WeightSnapshot snapshot = CurrentWeightSnapshot(current_view);
  if (qc_evidence_recorder_ != nullptr) {
    weight_update_manager_->AddLocalCandidates(
        qc_evidence_recorder_->TakeCompletedReputationCandidates(), snapshot);
  }

  messages = weight_update_manager_->DrainOutboundMessages(current_view,
                                                           snapshot);
  std::vector<InstallableWeightUpdate> installable_updates =
      weight_update_manager_->TakeInstallableUpdates(current_view, snapshot);
  for (const InstallableWeightUpdate& update : installable_updates) {
    if (InstallWeightUpdate(update, current_view)) {
      weight_update_manager_->OnWeightsActivated(
          CurrentWeightSnapshot(current_view));
    }
  }
  return messages;
}

bool HotStuff::InstallWeightUpdate(const InstallableWeightUpdate& update,
                                   int current_view) {
  if (weight_schedule_ == nullptr || update.activation_view > current_view ||
      update.old_weight_root != weight_schedule_->ActiveWeightRoot() ||
      update.old_weight_version != weight_schedule_->ActiveWeightVersion()) {
    return false;
  }
  const uint64_t next_weight_version = update.old_weight_version + 1;
  const int leader_activation_view =
      leader_selection_schedule_ != nullptr
          ? update.activation_view +
                leader_selection_schedule_->profile_activation_delay_views()
          : update.activation_view;
  if (leader_selection_schedule_ != nullptr &&
      leader_selection_schedule_->enabled() &&
      leader_selection_schedule_->dynamic_updates_enabled() &&
      !leader_selection_schedule_->ValidateProfile(
          leader_activation_view, next_weight_version, update.leader_weights,
          update.leader_weight_root, update.leader_params_version,
          update.leader_randomness_ref)) {
    LOG(ERROR) << "reject TD-Hotstuff weight update with invalid leader profile";
    return false;
  }
  if (!weight_schedule_->ScheduleUpdate(update.activation_view,
                                        update.next_weights,
                                        update.old_weight_root,
                                        update.old_weight_version)) {
    return false;
  }
  if (leader_selection_schedule_ != nullptr &&
      leader_selection_schedule_->enabled() &&
      leader_selection_schedule_->dynamic_updates_enabled() &&
      !leader_selection_schedule_->ScheduleUpdate(
          leader_activation_view, next_weight_version, update.leader_weights,
          update.leader_weight_root, update.leader_params_version,
          update.leader_randomness_ref)) {
    LOG(ERROR) << "failed to schedule TD-Hotstuff leader profile";
    return false;
  }
  weight_schedule_->ActivateUpTo(current_view);
  if (leader_selection_schedule_ != nullptr) {
    leader_selection_schedule_->ActivateUpTo(current_view);
  }
  SyncReputationWeightsToActiveSchedule();
  LOG(ERROR) << "activated TD-Hotstuff weight update version:"
             << weight_schedule_->ActiveWeightVersion()
             << " view:" << current_view
             << " root:" << weight_schedule_->ActiveWeightRoot()
             << " active_weights:"
             << WeightsForLog(weight_schedule_->ActiveWeights())
             << " leader_profile_activation_view:" << leader_activation_view;
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
  {
    // LOG(ERROR)<<"RECEIVE proposer view:"<<proposal->header().view() << " from: " << proposal->sender();
    std::unique_lock<std::mutex> lk(mutex_);

    if (id_ == NextLeader(view)) {
      proposal_received_ = true;
    }

    // Install any pending profile/weight update before checking proposal
    // leader context for this view.
    weight_messages = DrainWeightPlugin(view);
    if(!proposal_manager_->Verify(*proposal)){
      LOG(ERROR)<<" proposal invalid";
      proposal_valid = false;
    }

    if (proposal_valid) {
      if (qc_evidence_recorder_ != nullptr) {
        const QC& qc = proposal->header().qc();
        const int qc_view = qc.view();
        const int leader_id = qc_view > 0 ? proposal_manager_->GetLeader(qc_view) : 0;
        const uint64_t weight_version = weight_schedule_ != nullptr
                                            ? weight_schedule_->WeightVersionForView(qc_view)
                                            : 0;
        const std::string active_weight_root = weight_schedule_ != nullptr
                                                   ? weight_schedule_->WeightRootForView(qc_view)
                                                   : std::string();
        qc_evidence_recorder_->RecordQc(qc_view, qc.hash(), qc.signer_bitmap(),
                                        leader_id, weight_version,
                                        active_weight_root);
      }
      WeightPluginOutboundMessages post_verify_messages = DrainWeightPlugin(view);
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
  }

  BroadcastWeightPluginMessages(weight_messages);
  if (!proposal_valid) {
    return false;
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

  WeightPluginOutboundMessages weight_messages;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    //LOG(ERROR)<<"RECEIVE proposer cert :"<<cert->view()<<" from:"<<cert->signer();
    int view = cert->view();
    // Install any pending weight update before checking a vote for a view that
    // may already belong to the new weight schedule.
    weight_messages = DrainWeightPlugin(view);
    bool valid = proposal_manager_->VerifyCert(*cert);
    if(!valid){
      LOG(ERROR) << "Verify message fail";
      assert(1==0);
      return false;
    }

    std::string hash = cert->hash();
    auto& certs = receive_[view][hash];
    int64_t previous_weight = CertificateWeight(certs, view);
    certs.insert(std::make_pair(cert->signer(), std::move(cert)));
    int64_t current_weight = CertificateWeight(certs, view);

    //LOG(ERROR)<<"RECEIVE proposer cert :"<<view<<" weight:"<<current_weight;
    const int64_t quorum_weight = weight_schedule_->QuorumWeightForView(view);
    if(previous_weight < quorum_weight && current_weight >= quorum_weight){
      const std::vector<QcSignerInfo> signer_infos =
          CertificateSignerInfos(certs, view);
      std::vector<int> selected_signers =
          qc_signer_cooldown_.SelectSignersForQc(
              signer_infos, quorum_weight, static_cast<uint64_t>(view));
      if (selected_signers.empty()) {
        selected_signers = CertificateSigners(certs);
      }
      std::unique_ptr<QC> qc = std::make_unique<QC>();
      qc->set_hash(hash);
      qc->set_view(view);
      qc->set_signer_bitmap(BuildSignerBitmap(selected_signers, total_num_));

      for(int signer : selected_signers){
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
    WeightPluginOutboundMessages post_cert_messages = DrainWeightPlugin(view);
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
    weight_messages = DrainWeightPlugin(current_view);
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
    weight_update_manager_->HandleVote(*vote, CurrentWeightSnapshot(current_view));
    weight_messages = DrainWeightPlugin(current_view);
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
    weight_update_manager_->HandleCert(*cert, CurrentWeightSnapshot(current_view));
    weight_messages = DrainWeightPlugin(current_view);
  }
  BroadcastWeightPluginMessages(weight_messages);
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
