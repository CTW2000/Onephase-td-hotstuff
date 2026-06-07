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

#include <algorithm>
#include <deque>
#include <cstdlib>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "common/utils/utils.h"

namespace resdb {
namespace td_hotstuff {
namespace {

bool EnvFlagEnabled(const char* raw_value) {
  if (raw_value == nullptr) {
    return false;
  }
  const std::string value(raw_value);
  return value == "1" || value == "true" || value == "TRUE" ||
         value == "yes" || value == "YES" || value == "on" ||
         value == "ON";
}

int64_t EnvIntOrDefault(const char* raw_value, int64_t default_value,
                        int64_t min_value) {
  if (raw_value == nullptr || std::string(raw_value).empty()) {
    return default_value;
  }
  const int64_t parsed = std::stoll(raw_value);
  if (parsed < min_value) {
    LOG(FATAL) << "invalid TD-Hotstuff benchmark env value:" << raw_value;
  }
  return parsed;
}

bool BenchmarkRetryEnabled() {
  return EnvFlagEnabled(std::getenv("TD_HS_BENCHMARK_RETRY_ENABLE"));
}

uint64_t BenchmarkRetryTimeoutUs() {
  return BenchmarkRetryTimeoutUsForEnv(
      std::getenv("TD_HS_BENCHMARK_REQUEST_TIMEOUT_MS"));
}

}  // namespace

using comm::CollectorResultCode;

uint64_t BenchmarkRetryTimeoutUsForEnv(const char* raw_request_timeout_ms) {
  const int64_t timeout_ms = EnvIntOrDefault(raw_request_timeout_ms, 500, 1);
  return static_cast<uint64_t>(timeout_ms) * 1000;
}

bool BenchmarkDynamicRoutingEnabled(bool leader_selection_enabled,
                                    const char* env_override) {
  if (env_override != nullptr) {
    return EnvFlagEnabled(env_override);
  }
  return leader_selection_enabled;
}

int BenchmarkRouteForView(int view, int replica_num, int predicted_primary,
                          int observed_primary) {
  if (replica_num <= 0) {
    return DefaultLeaderForView(view, replica_num);
  }
  if (predicted_primary > 0 && predicted_primary <= replica_num) {
    return predicted_primary;
  }
  if (observed_primary > 0 && observed_primary <= replica_num) {
    return observed_primary;
  }
  return DefaultLeaderForView(view, replica_num);
}

int BenchmarkRouteForObservedPrimaries(
    int view, int replica_num, int predicted_primary,
    const std::vector<int>& observed_primaries) {
  if (replica_num <= 0 || observed_primaries.empty()) {
    return BenchmarkRouteForView(view, replica_num, predicted_primary, 0);
  }
  std::vector<int> leaders;
  leaders.reserve(observed_primaries.size());
  for (int primary : observed_primaries) {
    if (primary > 0 && primary <= replica_num &&
        std::find(leaders.begin(), leaders.end(), primary) == leaders.end()) {
      leaders.push_back(primary);
    }
  }
  if (leaders.empty()) {
    return BenchmarkRouteForView(view, replica_num, predicted_primary, 0);
  }
  std::sort(leaders.begin(), leaders.end());
  if (predicted_primary > 0 && predicted_primary <= replica_num &&
      std::find(leaders.begin(), leaders.end(), predicted_primary) !=
          leaders.end()) {
    return predicted_primary;
  }
  const int idx = ((view % static_cast<int>(leaders.size())) +
                   static_cast<int>(leaders.size())) %
                  static_cast<int>(leaders.size());
  return leaders[idx];
}

HotStuffPerformanceManager::HotStuffPerformanceManager(
    const ResDBConfig& config, ReplicaCommunicator* replica_communicator,
    SignatureVerifier* verifier)
    : PerformanceManager(config, replica_communicator, verifier){
  client_num_ = 1;
  primary_ = id_ % replica_num_;
  dynamic_benchmark_routing_enabled_ = BenchmarkDynamicRoutingEnabled(
      /*leader_selection_enabled=*/false,
      std::getenv("TD_HS_BENCHMARK_DYNAMIC_ROUTING_ENABLE"));
}

int HotStuffPerformanceManager::GetPrimary(){
  int view, value;
  while (true) {
    view = primary_;
    primary_ += client_num_;
    last_primary_view_ = view;
    value = DefaultLeaderForView(view, replica_num_);
    const int predicted_leader = value;
    if (dynamic_benchmark_routing_enabled_) {
      const int observed_route = ObservedRoutingPrimary(view, predicted_leader);
      value = observed_route > 0
                  ? observed_route
                  : BenchmarkRouteForView(
                        view, replica_num_, predicted_leader,
                        static_cast<int>(last_response_.load()));
    }

    VLOG(2) << "Send to " << value << " with send_num_: " << send_num_
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
  const uint64_t now = GetCurrentTime();
  {
    std::unique_lock<std::mutex> lk(pending_send_times_mutex_);
    pending_send_times_[request.user_seq()] = now;
  }
  Request tagged_request(request);
  tagged_request.set_current_view(last_primary_view_);
  last_send_time_ = now;
  replica_communicator_->SendMessage(tagged_request, primary);
}

void HotStuffPerformanceManager::UntrackPendingSendTime(uint64_t local_id) {
  std::unique_lock<std::mutex> lk(pending_send_times_mutex_);
  pending_send_times_.erase(local_id);
}

void HotStuffPerformanceManager::RecordObservedPrimary(int primary_id) {
  if (primary_id <= 0 || primary_id > replica_num_) {
    return;
  }
  const size_t window_limit =
      static_cast<size_t>(std::max(32, replica_num_ * 10));
  std::unique_lock<std::mutex> lk(observed_primary_mutex_);
  observed_primary_window_.push_back(primary_id);
  observed_primary_counts_[primary_id]++;
  while (observed_primary_window_.size() > window_limit) {
    const int old = observed_primary_window_.front();
    observed_primary_window_.pop_front();
    auto it = observed_primary_counts_.find(old);
    if (it != observed_primary_counts_.end()) {
      it->second--;
      if (it->second <= 0) {
        observed_primary_counts_.erase(it);
      }
    }
  }
}

std::vector<int> HotStuffPerformanceManager::ObservedPrimarySet() const {
  std::vector<int> primaries;
  std::unique_lock<std::mutex> lk(observed_primary_mutex_);
  primaries.reserve(observed_primary_counts_.size());
  for (const auto& entry : observed_primary_counts_) {
    if (entry.second > 0) {
      primaries.push_back(entry.first);
    }
  }
  std::sort(primaries.begin(), primaries.end());
  return primaries;
}

int HotStuffPerformanceManager::ObservedRoutingPrimary(
    int view, int predicted_primary) const {
  std::vector<int> observed = ObservedPrimarySet();
  const size_t min_observed =
      static_cast<size_t>(std::max(3, replica_num_ / 2));
  if (observed.size() < min_observed) {
    return 0;
  }
  return BenchmarkRouteForObservedPrimaries(
      view, replica_num_, predicted_primary, observed);
}

int HotStuffPerformanceManager::ExpireTimedOutPendingResponses(
    uint64_t now, uint64_t timeout_us) {
  std::vector<uint64_t> expired_ids;
  {
    std::unique_lock<std::mutex> lk(pending_send_times_mutex_);
    for (auto it = pending_send_times_.begin();
         it != pending_send_times_.end();) {
      if (now >= it->second && now - it->second >= timeout_us) {
        expired_ids.push_back(it->first);
        it = pending_send_times_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (uint64_t id : expired_ids) {
    const int idx = id % response_set_size_;
    std::unique_lock<std::mutex> lk(response_lock_[idx]);
    response_[idx].erase(static_cast<int64_t>(id));
  }
  return static_cast<int>(expired_ids.size());
}

void HotStuffPerformanceManager::MaybeReleaseStalledInflight() {
  if (!BenchmarkRetryEnabled() || send_num_ <= 0 || last_send_time_ == 0) {
    return;
  }
  const int max_process_txn = config_.GetMaxProcessTxn();
  if (max_process_txn <= 0 || send_num_ < max_process_txn) {
    return;
  }
  const uint64_t now = GetCurrentTime();
  const uint64_t retry_timeout_us = BenchmarkRetryTimeoutUs();
  const int expired_responses =
      ExpireTimedOutPendingResponses(now, retry_timeout_us);
  if (expired_responses <= 0) {
    return;
  }
  int old_send_num = send_num_.load();
  while (old_send_num > 0 &&
         !send_num_.compare_exchange_weak(
             old_send_num, std::max(0, old_send_num - expired_responses))) {
  }
  const uint64_t observed_primary = last_response_.load();
  last_send_time_ = now;
  LOG(WARNING) << "release stalled TD-Hotstuff benchmark inflight: old_send_num="
               << old_send_num << " expired_responses=" << expired_responses
               << " retry_timeout_us=" << retry_timeout_us
               << " observed_primary=" << observed_primary;
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
    RecordObservedPrimary(batch_response->primary_id());
    response_call_back(std::move(batch_response));
    return CollectorResultCode::FIRST_RESPONSE;
  }

  if (done) {
    RecordObservedPrimary(batch_response->primary_id());
    UntrackPendingResponse(seq);
    UntrackPendingSendTime(seq);
    response_call_back(std::move(batch_response));
    return CollectorResultCode::STATE_CHANGED;
  }
  return CollectorResultCode::OK;
}



}  // namespace td_hotstuff
}  // namespace resdb
