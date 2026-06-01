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

#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "common/utils/utils.h"

namespace resdb {
namespace td_hotstuff {
namespace {

std::vector<int64_t> ParseLeaderWeightsFromEnv(int total_replicas) {
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
    const int64_t weight = std::stoll(item);
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

bool EnvFlagEnabled(const char* raw_value) {
  if (raw_value == nullptr) {
    return false;
  }
  const std::string value(raw_value);
  return value == "1" || value == "true" || value == "TRUE" ||
         value == "yes" || value == "YES" || value == "on" ||
         value == "ON";
}

}  // namespace

using comm::CollectorResultCode;

bool BenchmarkDynamicRoutingEnabled(const LeaderSelectionConfig& config,
                                    const char* env_override) {
  if (env_override != nullptr) {
    return EnvFlagEnabled(env_override);
  }
  return config.enabled && config.dynamic_updates_enabled;
}

int BenchmarkRouteForView(int view, int replica_num, int observed_primary) {
  if (observed_primary > 0 && observed_primary <= replica_num) {
    return observed_primary;
  }
  return DefaultLeaderForView(view, replica_num);
}

HotStuffPerformanceManager::HotStuffPerformanceManager(
    const ResDBConfig& config, ReplicaCommunicator* replica_communicator,
    SignatureVerifier* verifier)
    : PerformanceManager(config, replica_communicator, verifier){
  client_num_ = 1;
  primary_ = id_ % replica_num_;
  const LeaderSelectionConfig leader_config = LeaderSelectionConfigFromEnv();
  leader_selection_schedule_ = std::make_unique<LeaderSelectionSchedule>(
      replica_num_, ParseLeaderWeightsFromEnv(replica_num_), leader_config);
  dynamic_benchmark_routing_enabled_ = BenchmarkDynamicRoutingEnabled(
      leader_config, std::getenv("TD_HS_BENCHMARK_DYNAMIC_ROUTING_ENABLE"));
}

int HotStuffPerformanceManager::GetPrimary(){
  int view, value;
  int crash_num_ = 0;
  while (true) {
    view = primary_;
    primary_ += client_num_;
    last_primary_view_ = view;
    value = leader_selection_schedule_ != nullptr
                ? leader_selection_schedule_->LeaderForView(view)
                : DefaultLeaderForView(view, replica_num_);
    const int predicted_leader = value;
    if (dynamic_benchmark_routing_enabled_) {
      value = BenchmarkRouteForView(
          view, replica_num_, static_cast<int>(last_response_.load()));
    }
    int next_leader = (view+1) % replica_num_ + 1;
    if (crash_num_ > 0) {
      if (next_leader % 3 == 1 && next_leader < 3 * crash_num_) {
        send_num_-=2;
      }
      if (value % 3 == 1 && value < 3 * crash_num_) {
        count_++;
        send_num_--;
      }
      if ((replica_num_ == 3*crash_num_ + 1 && value == 2) || (value == 3*crash_num_ + 2)) {
        send_num_ += 2*count_;
        count_ = 0;
      }
    }

    if (value % 3 == 1 &&
        value < 3 * static_cast<int>(config_.GetForkTailNum())) {
      send_num_--;
    }
    LOG(ERROR) << "Send to "<< value << " with send_num_: " << send_num_
               << " count: " << count_
               << " predicted_leader:" << predicted_leader
               << " observed_primary:" << last_response_.load()
               << " dynamic_benchmark_routing:"
               << dynamic_benchmark_routing_enabled_;
    return value;
  }
}

void HotStuffPerformanceManager::SendMessage(const Request& request) {
  const int primary = GetPrimary();
  Request tagged_request(request);
  tagged_request.set_current_view(last_primary_view_);
  last_send_time_ = GetCurrentTime();
  replica_communicator_->SendMessage(tagged_request, primary);
}


CollectorResultCode HotStuffPerformanceManager::AddResponseMsg(
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
      // LOG(ERROR)<<"has done local seq:"<<seq<<" global seq:"<<request->seq();
      return CollectorResultCode::OK;
    }
    response_[idx][seq]++;
    // LOG(ERROR)<<"get seq :"<<request->seq()<<" local id:"<<seq<<" num:"<<response_[idx][seq]<<" send:"<<send_num_;

    if (response_[idx][seq] == 1) {
      first = true;
    }

    if (response_[idx][seq] >= config_.GetMinClientReceiveNum()) {
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



}  // namespace td_hotstuff
}  // namespace resdb
