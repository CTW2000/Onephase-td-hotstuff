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

#pragma once

#include <deque>
#include <future>
#include <mutex>
#include <memory>
#include <unordered_map>
#include <vector>

#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"
#include "platform/consensus/ordering/common/framework/performance_manager.h"

namespace resdb {
namespace td_hotstuff {

bool BenchmarkDynamicRoutingEnabled(const LeaderSelectionConfig& config,
                                    const char* env_override);
uint64_t BenchmarkRetryTimeoutUsForEnv(const char* raw_request_timeout_ms);
int BenchmarkRouteForView(int view, int replica_num, int predicted_primary,
                          int observed_primary);
int BenchmarkRouteForObservedPrimaries(
    int view, int replica_num, int predicted_primary,
    const std::vector<int>& observed_primaries);

class HotStuffPerformanceManager : public common::PerformanceManager {
 public:
  HotStuffPerformanceManager(const ResDBConfig& config,
                     ReplicaCommunicator* replica_communicator,
                     SignatureVerifier* verifier);

protected:
  comm::CollectorResultCode AddResponseMsg(
      std::unique_ptr<Request> request,
      std::function<void(std::unique_ptr<BatchUserResponse>)> call_back) override;
  void SendMessage(const Request& request) override;
  void MaybeReleaseStalledInflight() override;
  int GetPrimary() override;

  void UntrackPendingSendTime(uint64_t local_id);
  int ExpireTimedOutPendingResponses(uint64_t now, uint64_t timeout_us);
  void RecordObservedPrimary(int primary_id);
  std::vector<int> ObservedPrimarySet() const;
  int ObservedRoutingPrimary(int view, int predicted_primary) const;

  int count_ = 0;
  int last_primary_view_ = 0;
  bool dynamic_benchmark_routing_enabled_ = false;
  std::unique_ptr<LeaderSelectionSchedule> leader_selection_schedule_;
  std::mutex pending_send_times_mutex_;
  std::unordered_map<uint64_t, uint64_t> pending_send_times_;
  mutable std::mutex observed_primary_mutex_;
  std::deque<int> observed_primary_window_;
  std::unordered_map<int, int> observed_primary_counts_;
};

}  // namespace td_hotstuff
}  // namespace resdb
