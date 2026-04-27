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
#include <vector>

#include "common/utils/utils.h"

namespace resdb {
namespace td_hotstuff {

std::unique_ptr<TdHotstuffPerformanceManager> Consensus::GetPerformanceManager() {
        return config_.IsPerformanceRunning()
        ? std::make_unique<TdHotstuffPerformanceManager>(
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

  // Build weight vector from TD_HS_WEIGHTS env var (comma-separated ints),
  // or fall back to uniform weight=1 for all replicas.
  std::vector<int> weights(total_replicas, 1);
  const char* env_weights = std::getenv("TD_HS_WEIGHTS");
  if (env_weights != nullptr && std::string(env_weights).size() > 0) {
    std::istringstream ss(env_weights);
    std::string token;
    int idx = 0;
    while (std::getline(ss, token, ',') && idx < total_replicas) {
      weights[idx++] = std::stoi(token);
    }
  }
  {
    std::ostringstream wlog;
    wlog << "TD-HotStuff weights: [";
    for (int i = 0; i < total_replicas; i++) {
      if (i > 0) wlog << ",";
      wlog << weights[i];
    }
    int W = 0; for (int w : weights) W += w;
    wlog << "] W=" << W << " threshold=" << ComputeWeightThreshold(f, weights);
    LOG(ERROR) << wlog.str();
  }

  if (config_.GetPublicKeyCertificateInfo()
          .public_key()
          .public_key_info()
          .type() != CertificateKeyInfo::CLIENT) {
    td_hotstuff_= std::make_unique<TdHotstuff>(config_.GetSelfInfo().id(), f, total_replicas, GetSignatureVerifier(), config_.GetNonResponsiveNum(), config_.GetForkTailNum(), config_.GetRollBackNum(), config_.GetTimerLength() * 1000, weights);
    InitProtocol(td_hotstuff_.get());
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
    td_hotstuff_->ReceiveProposal(std::move(p));
  }
  else if(request->user_type() == MessageType::Vote) {
    std::unique_ptr<Certificate> cert = std::make_unique<Certificate>();
    if (!cert->ParseFromString(request->data())) {
      LOG(ERROR) << "parse proposal fail";
      assert(1 == 0);
      return -1;
    }
    td_hotstuff_->ReceiveCertificate(std::move(cert));
  }
  return 0;
}

int Consensus::ProcessNewTransaction(std::unique_ptr<Request> request) {
  std::unique_ptr<Transaction> txn = std::make_unique<Transaction>();
  txn->set_data(request->data());
  txn->set_hash(request->hash());
  txn->set_proxy_id(request->proxy_id());
  txn->set_user_seq(request->user_seq());
  return td_hotstuff_->ReceiveTransaction(std::move(txn));
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
  request->set_next_primary(txn.next_primary());
  // LOG(ERROR) << "primary: " << txn.proposer();
  transaction_executor_->AddExecuteMessage(std::move(request));
  return 0;
}


}  // namespace td_hotstuff
}  // namespace resdb