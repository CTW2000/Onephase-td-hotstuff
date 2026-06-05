#pragma once

#include <atomic>
#include <condition_variable>
#include <chrono>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/crypto/signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/certificate_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/proto/proposal.pb.h"

namespace resdb {
namespace td_hotstuff {

struct VerifiedConsensusEvent {
  enum class Type {
    kNone = 0,
    kVote,
    kTimeoutVote,
    kTimeoutCert,
  };

  Type type = Type::kNone;
  int view = 0;
  int signer = 0;
  std::string hash;
  std::string error;
  std::unique_ptr<Certificate> vote;
  std::unique_ptr<TimeoutVote> timeout_vote;
  std::unique_ptr<TimeoutCert> timeout_cert;
};

class AsyncConsensusVerifier {
 public:
  AsyncConsensusVerifier(
      int total_replicas, SignatureVerifier* verifier, size_t worker_count,
      size_t queue_capacity,
      std::shared_ptr<WeightSchedule> weight_schedule = nullptr);
  ~AsyncConsensusVerifier();

  AsyncConsensusVerifier(const AsyncConsensusVerifier&) = delete;
  AsyncConsensusVerifier& operator=(const AsyncConsensusVerifier&) = delete;

  void Start();
  void Stop();

  bool SubmitVote(std::unique_ptr<Certificate> vote);
  bool TrySubmitVote(std::unique_ptr<Certificate>* vote);
  bool SubmitTimeoutVote(std::unique_ptr<TimeoutVote> vote);
  bool SubmitTimeoutCert(std::unique_ptr<TimeoutCert> cert);

  std::vector<VerifiedConsensusEvent> DrainVerified();
  std::vector<VerifiedConsensusEvent> WaitForVerified(std::chrono::milliseconds timeout);

  uint64_t dropped_count() const { return dropped_count_.load(); }
  uint64_t invalid_count() const { return invalid_count_.load(); }

 private:
  struct WorkItem {
    VerifiedConsensusEvent::Type type = VerifiedConsensusEvent::Type::kNone;
    std::unique_ptr<Certificate> vote;
    std::unique_ptr<TimeoutVote> timeout_vote;
    std::unique_ptr<TimeoutCert> timeout_cert;
  };

  bool Submit(WorkItem item);
  void WorkerLoop();
  void PushVerified(VerifiedConsensusEvent event);
  VerifiedConsensusEvent Verify(WorkItem item);

  const int total_replicas_;
  SignatureVerifier* const verifier_;
  const size_t worker_count_;
  const size_t queue_capacity_;
  CertificateVerifier certificate_verifier_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::condition_variable verified_cv_;
  bool started_ = false;
  bool stop_ = false;
  std::deque<WorkItem> pending_;
  std::deque<VerifiedConsensusEvent> verified_;
  std::vector<std::thread> workers_;
  std::atomic<uint64_t> dropped_count_{0};
  std::atomic<uint64_t> invalid_count_{0};
};

size_t AsyncVerifierWorkerCountFromEnv();
size_t AsyncVerifierQueueCapacityFromEnv();

}  // namespace td_hotstuff
}  // namespace resdb
