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

#include "platform/consensus/ordering/td_hotstuff/framework/performance_manager.h"

#include <glog/logging.h>

#include "common/utils/utils.h"
#include "common/crypto/hash.h"

namespace resdb {
namespace td_hotstuff {

using comm::CollectorResultCode;

TdHotstuffPerformanceManager::TdHotstuffPerformanceManager(
    const ResDBConfig& config, ReplicaCommunicator* replica_communicator,
    SignatureVerifier* verifier, const std::vector<int>& weights)
    : PerformanceManager(config, replica_communicator, verifier){
  client_num_ = 1;
  slot_num_ = config_.GetSlotNum();
  last_response_ = 2;
  inflight_limit_ = 20;

  // Compute VRF leader for view=1 using the actual trust weights
  int total_replicas = config_.GetReplicaNum();
  int total_weight = 0;
  for (int w : weights) total_weight += w;

  // Build prefix sums for weighted interval mapping
  std::vector<int> prefix(total_replicas + 1, 0);
  for (int i = 0; i < total_replicas; i++) {
    prefix[i + 1] = prefix[i] + weights[i];
  }

  uint64_t epoch = 1;
  std::string vrf_input = std::to_string(epoch) + ":1";
  std::string hash_raw = utils::CalculateSHA256Hash(vrf_input);

  uint32_t hash_val = 0;
  for (int i = 0; i < 4 && i < (int)hash_raw.size(); i++) {
    hash_val = (hash_val << 8) | (uint8_t)hash_raw[i];
  }
  int position = (int)(hash_val % (uint32_t)total_weight);

  // Find which replica's interval contains position
  primary_ = 1;
  for (int i = 0; i < total_replicas; i++) {
    if (position >= prefix[i] && position < prefix[i + 1]) {
      primary_ = i + 1;
      break;
    }
  }

  LOG(ERROR) << "TD-HS PM: initial VRF leader for view=1 is node "
             << primary_ << " (n=" << total_replicas
             << " W=" << total_weight
             << " hash_pos=" << position << ")";
}


CollectorResultCode TdHotstuffPerformanceManager::AddResponseMsg(
    std::unique_ptr<Request> request,
    std::function<void(std::unique_ptr<BatchUserResponse>)> response_call_back) {
  if (request == nullptr) {
    return CollectorResultCode::INVALID;
  }

  //uint64_t seq = request->seq();

  std::unique_ptr<BatchUserResponse> batch_response = std::make_unique<BatchUserResponse>();
  if (!batch_response->ParseFromString(request->data())) {
    LOG(ERROR) << "parse response fail:"<<request->data().size()
    <<" seq:"<<request->seq(); return CollectorResultCode::INVALID;
  }

  uint64_t seq = batch_response->local_id();
  //LOG(ERROR)<<"receive seq:"<<seq;

  bool first = false;
  bool done = false;
  {
    int idx = seq % response_set_size_;
    std::unique_lock<std::mutex> lk(response_lock_[idx]);
    if (response_[idx].find(seq) == response_[idx].end()) {
      //LOG(ERROR)<<"has done local seq:"<<seq<<" global seq:"<<request->seq();
      return CollectorResultCode::OK;
    }
    response_[idx][seq]++;
    //LOG(ERROR)<<"get seq :"<<request->seq()<<" local id:"<<seq<<" num:"<<response_[idx][seq]<<" send:"<<send_num_;

    if (response_[idx][seq] == 1) {
      first = true;
    }

    if (response_[idx][seq] >= config_.GetMinDataReceiveNum()) {
      //LOG(ERROR)<<"get seq :"<<request->seq()<<" local id:"<<seq<<" num:"<<response_[idx][seq]<<" done:"<<send_num_;
      response_[idx].erase(response_[idx].find(seq));
      done = true;
    }
  }

  if (first) {
    response_call_back(std::move(batch_response));
    return CollectorResultCode::FIRST_RESPONSE;
  }

  if (done) {
    response_call_back(std::move(batch_response));
    return CollectorResultCode::STATE_CHANGED;
  }
  return CollectorResultCode::OK;
}


int TdHotstuffPerformanceManager::ProcessResponseMsg(std::unique_ptr<Context> context,
                                           std::unique_ptr<Request> request) {
  std::unique_ptr<Request> response;
  if (request->ret() == -2) {
    send_num_--;
    return 0;
  }

  std::unique_ptr<BatchUserResponse> batch_response = nullptr;
  CollectorResultCode ret =
      AddResponseMsg(std::move(request), [&](std::unique_ptr<BatchUserResponse> request) {
        batch_response = std::move(request);
        return;
      });

  if (ret == CollectorResultCode::STATE_CHANGED) {
    assert(batch_response);
    int next_primary = batch_response->next_primary();
    int primary_id = batch_response->primary_id();

    bool already_released = false;
    {
      std::unique_lock<std::mutex> lk(vrf_mutex_);
      if (next_primary) {
        if (next_primary != primary_) {
          // VRF leader changed. Release in-flight slots for the old primary.
          int old_inflight = inflight_[primary_];
          if (old_inflight > 0) {
            send_num_ -= old_inflight;
            inflight_[primary_] = 0;
          }
        }
        primary_ = next_primary;
      }

      if (inflight_[primary_id] > 0) {
        inflight_[primary_id]--;
      } else {
        already_released = true;
      }
    }

    if (!already_released) {
      SendResponseToClient(*batch_response);
    }
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}


int TdHotstuffPerformanceManager::GetPrimary() {
  // Called only from SendMessage below, where mutex is already held.
  return primary_;
}

void TdHotstuffPerformanceManager::SendMessage(const Request& request) {
  int primary;
  {
    std::unique_lock<std::mutex> lk(vrf_mutex_);
    primary = primary_;
    inflight_[primary]++;
    // Increment send_num_ atomically with inflight_ to prevent
    // race with leader-change pre-release in ProcessResponseMsg.
    send_num_++;
  }
  replica_communicator_->SendMessage(request, primary);
}

void TdHotstuffPerformanceManager::SendResponseToClient(const BatchUserResponse& batch_response) {
  uint64_t create_time = batch_response.createtime();
  uint64_t primary_id = batch_response.primary_id();
  if (create_time > 0 && last_primary_id_ == primary_id) {
  // if (create_time > 0) {
    uint64_t run_time = GetCurrentTime() - create_time;
    // LOG(ERROR)<<"receive current:"<<GetCurrentTime()<<" create time:"<<create_time<<" run time:"<<run_time<<" local id:"<<batch_response.local_id();
    global_stats_->AddLatency(run_time);
  } else {
  }
  last_primary_id_ = primary_id;
  //send_num_-=10;
  send_num_--;
}

}  // namespace td_hotstuff
}  // namespace resdb