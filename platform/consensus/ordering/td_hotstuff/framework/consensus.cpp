/*

 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 */

#include "platform/consensus/ordering/td_hotstuff/framework/consensus.h"

#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/utils/utils.h"

namespace resdb {
namespace td_hotstuff {
namespace {

std::vector<int64_t> ParseReplicaWeightsFromEnv(int total_replicas) {
  std::vector<int64_t> weights(total_replicas, 1);
  const char* raw_weights = std::getenv("TD_HS_WEIGHTS");
  if (raw_weights == nullptr || std::string(raw_weights).empty()) {
    return weights;
  }

  std::stringstream input(raw_weights);
  std::string item;
  std::vector<int64_t> parsed_weights;
  while (std::getline(input, item, ',')) {
    if (item.empty()) {
      LOG(FATAL) << "TD_HS_WEIGHTS has an empty weight entry";
    }
    int64_t weight = std::stoll(item);
    if (weight <= 0) {
      LOG(FATAL) << "TD_HS_WEIGHTS must contain positive weights: "
                 << raw_weights;
    }
    parsed_weights.push_back(weight);
  }

  if (parsed_weights.size() != static_cast<size_t>(total_replicas)) {
    LOG(FATAL) << "TD_HS_WEIGHTS size:" << parsed_weights.size()
               << " does not match replica num:" << total_replicas;
  }
  return parsed_weights;
}


int TxForwardLookahead() {
  const char* raw = std::getenv("TD_HS_TX_FORWARD_LOOKAHEAD");
  if (raw != nullptr && std::string(raw).size() > 0) {
    return std::max(0, std::stoi(raw));
  }
  return 4;
}

int RequestViewLookahead(const ResDBConfig& config) {
  const char* raw = std::getenv("TD_HS_REQUEST_VIEW_LOOKAHEAD");
  if (raw != nullptr && std::string(raw).size() > 0) {
    return std::max(0, std::stoi(raw));
  }
  return std::max(static_cast<int>(config.GetReplicaNum()),
                  static_cast<int>(config.GetMaxProcessTxn()));
}

}  // namespace

std::unique_ptr<HotStuffPerformanceManager> Consensus::GetPerformanceManager() {
        return config_.IsPerformanceRunning()
        ? std::make_unique<HotStuffPerformanceManager>(
          config_, GetBroadCastClient(), GetSignatureVerifier())
        : nullptr;
}

bool Consensus::ShouldForwardClientTransaction(const Request& request) const {
  const int sender = request.sender_id();
  return sender <= 0 || sender > static_cast<int>(config_.GetReplicaNum());
}

std::vector<std::pair<int, int>> Consensus::TransactionForwardTargets(
    int request_view) {
  std::vector<std::pair<int, int>> targets;
  if (hs_ == nullptr) {
    return targets;
  }
  std::set<int> seen_leaders;
  const int lookahead = TxForwardLookahead();
  for (int offset = 0; offset <= lookahead; ++offset) {
    const int view = request_view + offset;
    const int leader = hs_->LeaderForView(view);
    if (leader <= 0 || !seen_leaders.insert(leader).second) {
      continue;
    }
    targets.push_back({leader, view});
  }
  return targets;
}

Consensus::Consensus(const ResDBConfig& config,
                     std::unique_ptr<TransactionManager> executor)
    : common::Consensus(config, std::move(executor)) {

  SetPerformanceManager(GetPerformanceManager());

  Init();

  int total_replicas = config_.GetReplicaNum();
  int f = (total_replicas - 1) / 3;

  if (config_.GetPublicKeyCertificateInfo()
          .public_key()
          .public_key_info()
          .type() != CertificateKeyInfo::CLIENT) {
    std::vector<int64_t> replica_weights = ParseReplicaWeightsFromEnv(total_replicas);
    int64_t quorum_weight = CalculateQuorumWeight(replica_weights);
    hs_= std::make_unique<HotStuff>(config_.GetSelfInfo().id(), f, total_replicas, GetSignatureVerifier(), config_.GetNonResponsiveNum(), config_.GetForkTailNum(), config_.GetTimerLength() * 1000, replica_weights, quorum_weight);
    InitProtocol(hs_.get());
  }
}

int Consensus::ProcessCustomConsensus(std::unique_ptr<Request> request) {
  //LOG(ERROR)<<"recv request:"<<MessageType_Name(request->user_type());
  //int64_t current_time = GetCurrentTime();

  if(request->user_type() == MessageType::NewProposal) {
    std::unique_ptr<Proposal> p = std::make_unique<Proposal>();
    if (!p->ParseFromString(request->data())) {
      LOG(ERROR) << "parse proposal fail";
      assert(1 == 0);
      return -1;
    }
    hs_->ReceiveProposal(std::move(p));
  }
  else if(request->user_type() == MessageType::Vote) {
    std::unique_ptr<Certificate> cert = std::make_unique<Certificate>();
    if (!cert->ParseFromString(request->data())) {
      LOG(ERROR) << "parse proposal fail";
      assert(1 == 0);
      return -1;
    }
    hs_->ReceiveCertificate(std::move(cert));
  }
  else if(request->user_type() == MessageType::WeightUpdateCandidateMsg) {
    std::unique_ptr<CandidateWeightUpdate> candidate =
        std::make_unique<CandidateWeightUpdate>();
    if (!candidate->ParseFromString(request->data())) {
      LOG(ERROR) << "parse weight update candidate fail";
      assert(1 == 0);
      return -1;
    }
    hs_->ReceiveWeightUpdateCandidate(std::move(candidate));
  }
  else if(request->user_type() == MessageType::WeightUpdateVoteMsg) {
    std::unique_ptr<WeightUpdateVote> vote =
        std::make_unique<WeightUpdateVote>();
    if (!vote->ParseFromString(request->data())) {
      LOG(ERROR) << "parse weight update vote fail";
      assert(1 == 0);
      return -1;
    }
    hs_->ReceiveWeightUpdateVote(std::move(vote));
  }
  else if(request->user_type() == MessageType::WeightUpdateCertMsg) {
    std::unique_ptr<WeightUpdateCert> cert =
        std::make_unique<WeightUpdateCert>();
    if (!cert->ParseFromString(request->data())) {
      LOG(ERROR) << "parse weight update cert fail";
      assert(1 == 0);
      return -1;
    }
    hs_->ReceiveWeightUpdateCert(std::move(cert));
  }
  else if(request->user_type() == MessageType::TimeoutVoteMsg) {
    std::unique_ptr<TimeoutVote> vote = std::make_unique<TimeoutVote>();
    if (!vote->ParseFromString(request->data())) {
      LOG(ERROR) << "parse timeout vote fail";
      assert(1 == 0);
      return -1;
    }
    hs_->ReceiveTimeoutVote(std::move(vote));
  }
  else if(request->user_type() == MessageType::TimeoutCertMsg) {
    std::unique_ptr<TimeoutCert> cert = std::make_unique<TimeoutCert>();
    if (!cert->ParseFromString(request->data())) {
      LOG(ERROR) << "parse timeout cert fail";
      assert(1 == 0);
      return -1;
    }
    hs_->ReceiveTimeoutCert(std::move(cert));
  }
  return 0;
}

int Consensus::ProcessNewTransaction(std::unique_ptr<Request> request) {
  if (request == nullptr) {
    return -1;
  }
  if (hs_ == nullptr) {
    return -1;
  }

  int request_view = request->current_view() > 0 ? request->current_view()
                                               : hs_->CurrentView();
  const int current_view = hs_->CurrentView();
  const int lookahead = RequestViewLookahead(config_);
  if (request_view < current_view ||
      request_view > current_view + lookahead) {
    request_view = current_view;
  }

  std::unique_ptr<Transaction> txn = std::make_unique<Transaction>();
  txn->set_data(request->data());
  txn->set_hash(request->hash());
  txn->set_proxy_id(request->proxy_id());
  txn->set_user_seq(request->user_seq());
  hs_->ReceiveTransactionForView(std::move(txn), request_view);

  if (replica_communicator_ != nullptr &&
      ShouldForwardClientTransaction(*request)) {
    const int self = config_.GetSelfInfo().id();
    for (const auto& target : TransactionForwardTargets(request_view)) {
      const int leader = target.first;
      const int view = target.second;
      if (leader <= 0 || leader == self) {
        continue;
      }
      Request forwarded(*request);
      forwarded.set_current_view(view);
      forwarded.set_next_primary(leader);
      forwarded.set_sender_id(self);
      replica_communicator_->SendMessage(forwarded, leader);
    }
  }
  return 0;
}

int Consensus::CommitMsg(const google::protobuf::Message& msg) {
  return CommitMsgInternal(dynamic_cast<const Transaction&>(msg));
}

int Consensus::CommitMsgInternal(const Transaction& txn) {
  //LOG(ERROR)<<"commit txn:"<<txn.id()<<" proxy id:"<<txn.proxy_id();
  std::unique_ptr<Request> request = std::make_unique<Request>();
  request->set_data(txn.data());
  request->set_seq(txn.id());
  request->set_proxy_id(txn.proxy_id());
  request->set_primary_id(txn.proposer());
  request->set_commit_time(GetCurrentTime());
  transaction_executor_->AddExecuteMessage(std::move(request));
  return 0;
}


}  // namespace fairdag
}  // namespace resdb
