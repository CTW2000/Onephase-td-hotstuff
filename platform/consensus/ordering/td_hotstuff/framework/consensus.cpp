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

#include <cstdlib>
#include <sstream>
#include <string>
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

}  // namespace

std::unique_ptr<HotStuffPerformanceManager> Consensus::GetPerformanceManager() {
        return config_.IsPerformanceRunning()
        ? std::make_unique<HotStuffPerformanceManager>(
          config_, GetBroadCastClient(), GetSignatureVerifier())
        : nullptr;
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
  return 0;
}

int Consensus::ProcessNewTransaction(std::unique_ptr<Request> request) {
  std::unique_ptr<Transaction> txn = std::make_unique<Transaction>();
  txn->set_data(request->data());
  txn->set_hash(request->hash());
  txn->set_proxy_id(request->proxy_id());
  txn->set_user_seq(request->user_seq());
  return hs_->ReceiveTransaction(std::move(txn));
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
  transaction_executor_->AddExecuteMessage(std::move(request));
  return 0;
}


}  // namespace fairdag
}  // namespace resdb
