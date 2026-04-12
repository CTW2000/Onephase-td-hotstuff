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
#include <vector>

#include "common/utils/utils.h"

namespace resdb {
namespace td_hotstuff {

std::vector<int> Consensus::BuildWeights(int total_replicas) {
  // Trust-diverse weight assignment.
  // Higher weight = higher trust.  The VRF election probability and
  // QC threshold are both proportional to weight.
  //
  // BFT safety constraint:
  //   For any subset of f replicas removed, the remaining weight
  //   must still reach threshold = floor(2W/3) + 1.
  //   => sum(top f weights) <= W - threshold = ceil(W/3) - 1
  //
  // Strategy: give every replica weight=2 (uniform-high), then bump
  // the first replica to 3.  This gives meaningful differentiation
  // while staying BFT-safe.
  //
  // n=5 f=1: [3,2,2,2,2] W=11 threshold=8  top1=3 remain=8>=8 OK
  // n=10 f=3: [2,2,...,2] W=20 threshold=14 top3=6 remain=14>=14 OK
  //   (At n=10 with f=3, even w=3 for one replica breaks BFT, so
  //    we use uniform w=2 and rely on the Pacemaker timeout to
  //    differentiate trust at the protocol level.)

  int f = (total_replicas - 1) / 3;
  std::vector<int> weights(total_replicas, 2);  // base weight = 2

  if (total_replicas >= 4 && f == 1) {
    // n=4..6 (f=1): safe to give one replica w=3
    weights[0] = 3;
  }
  // For f>=2 (n>=7): keep uniform w=2 to guarantee BFT safety

  // Verify BFT safety at startup
  int total_weight = 0;
  for (int w : weights) total_weight += w;
  int threshold = (2 * total_weight) / 3 + 1;

  // Compute worst-case: sum of f largest weights
  std::vector<int> sorted_w = weights;
  std::sort(sorted_w.rbegin(), sorted_w.rend());
  int top_f_sum = 0;
  for (int i = 0; i < f; i++) top_f_sum += sorted_w[i];
  int min_honest_weight = total_weight - top_f_sum;
  bool bft_safe = min_honest_weight >= threshold;

  LOG(ERROR) << "TD-HS weights: n=" << total_replicas
             << " f=" << f
             << " W=" << total_weight
             << " threshold=" << threshold
             << " top" << f << "_sum=" << top_f_sum
             << " min_honest=" << min_honest_weight
             << (bft_safe ? " [BFT-SAFE]" : " [BFT-UNSAFE!]");
  for (int i = 0; i < total_replicas; i++) {
    LOG(ERROR) << "  replica " << (i+1) << " weight=" << weights[i];
  }
  return weights;
}

std::unique_ptr<TdHotstuffPerformanceManager> Consensus::GetPerformanceManager(
    const std::vector<int>& weights) {
        return config_.IsPerformanceRunning()
        ? std::make_unique<TdHotstuffPerformanceManager>(
          config_, GetBroadCastClient(), GetSignatureVerifier(), weights)
        : nullptr;
}

Consensus::Consensus(const ResDBConfig& config,
                     std::unique_ptr<TransactionManager> executor)
    : common::Consensus(config, std::move(executor)) {

  int total_replicas = config_.GetReplicaNum();
  int f = (total_replicas - 1) / 3;

  // Build trust weights FIRST — both PerformanceManager and TdHotstuff need them
  std::vector<int> weights = BuildWeights(total_replicas);

  SetPerformanceManager(GetPerformanceManager(weights));

  Init();

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