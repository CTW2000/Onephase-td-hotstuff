#include "platform/consensus/ordering/td_hotstuff/algorithm/async_consensus_verifier.h"

#include <algorithm>
#include <cstdlib>
#include <utility>

#include "common/crypto/hash.h"
#include "glog/logging.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr size_t kDefaultWorkerCount = 5;
constexpr size_t kDefaultQueueCapacity = 65536;

size_t PositiveSizeFromEnv(const char* name, size_t fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return fallback;
  }
  char* end = nullptr;
  const long parsed = std::strtol(value, &end, 10);
  if (end == value || parsed <= 0) {
    return fallback;
  }
  return static_cast<size_t>(parsed);
}

std::string TimeoutCertHash(const TimeoutCert& cert) {
  std::string bytes;
  cert.SerializeToString(&bytes);
  return SignatureVerifier::CalculateHash(bytes);
}

}  // namespace

size_t AsyncVerifierWorkerCountFromEnv() {
  return PositiveSizeFromEnv("TD_HS_ASYNC_VERIFY_WORKERS",
                             kDefaultWorkerCount);
}

size_t AsyncVerifierQueueCapacityFromEnv() {
  return PositiveSizeFromEnv("TD_HS_ASYNC_VERIFY_QUEUE_CAPACITY",
                             kDefaultQueueCapacity);
}

AsyncConsensusVerifier::AsyncConsensusVerifier(
    int total_replicas, SignatureVerifier* verifier, size_t worker_count,
    size_t queue_capacity, std::shared_ptr<WeightSchedule> weight_schedule)
    : total_replicas_(total_replicas),
      verifier_(verifier),
      worker_count_(std::max<size_t>(1, worker_count)),
      queue_capacity_(std::max<size_t>(1, queue_capacity)),
      certificate_verifier_(total_replicas, verifier, std::move(weight_schedule)) {}

AsyncConsensusVerifier::~AsyncConsensusVerifier() { Stop(); }

void AsyncConsensusVerifier::Start() {
  std::lock_guard<std::mutex> lk(mutex_);
  if (started_) {
    return;
  }
  stop_ = false;
  started_ = true;
  workers_.reserve(worker_count_);
  for (size_t i = 0; i < worker_count_; ++i) {
    workers_.emplace_back(&AsyncConsensusVerifier::WorkerLoop, this);
  }
}

void AsyncConsensusVerifier::Stop() {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!started_) {
      return;
    }
    stop_ = true;
  }
  cv_.notify_all();
  verified_cv_.notify_all();
  for (std::thread& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  workers_.clear();
  std::lock_guard<std::mutex> lk(mutex_);
  started_ = false;
  pending_.clear();
}

bool AsyncConsensusVerifier::SubmitVote(std::unique_ptr<Certificate> vote) {
  return TrySubmitVote(&vote);
}

bool AsyncConsensusVerifier::TrySubmitVote(std::unique_ptr<Certificate>* vote) {
  if (vote == nullptr || *vote == nullptr) {
    return false;
  }
  WorkItem item;
  item.type = VerifiedConsensusEvent::Type::kVote;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!started_ || stop_) {
      dropped_count_.fetch_add(1);
      return false;
    }
    if (pending_.size() >= queue_capacity_) {
      dropped_count_.fetch_add(1);
      LOG_EVERY_N(WARNING, 1000)
          << "TD-Hotstuff async verifier queue full, dropped:"
          << dropped_count_.load();
      return false;
    }
    item.vote = std::move(*vote);
    pending_.push_back(std::move(item));
  }
  cv_.notify_one();
  return true;
}

bool AsyncConsensusVerifier::SubmitTimeoutVote(
    std::unique_ptr<TimeoutVote> vote) {
  if (vote == nullptr) {
    return false;
  }
  WorkItem item;
  item.type = VerifiedConsensusEvent::Type::kTimeoutVote;
  item.timeout_vote = std::move(vote);
  return Submit(std::move(item));
}

bool AsyncConsensusVerifier::SubmitTimeoutCert(
    std::unique_ptr<TimeoutCert> cert) {
  if (cert == nullptr) {
    return false;
  }
  WorkItem item;
  item.type = VerifiedConsensusEvent::Type::kTimeoutCert;
  item.timeout_cert = std::move(cert);
  return Submit(std::move(item));
}

bool AsyncConsensusVerifier::Submit(WorkItem item) {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!started_ || stop_) {
      dropped_count_.fetch_add(1);
      return false;
    }
    if (pending_.size() >= queue_capacity_) {
      dropped_count_.fetch_add(1);
      LOG_EVERY_N(WARNING, 1000)
          << "TD-Hotstuff async verifier queue full, dropped:"
          << dropped_count_.load();
      return false;
    }
    pending_.push_back(std::move(item));
  }
  cv_.notify_one();
  return true;
}

std::vector<VerifiedConsensusEvent> AsyncConsensusVerifier::DrainVerified() {
  std::deque<VerifiedConsensusEvent> drained;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    drained.swap(verified_);
  }
  std::vector<VerifiedConsensusEvent> events;
  events.reserve(drained.size());
  while (!drained.empty()) {
    events.push_back(std::move(drained.front()));
    drained.pop_front();
  }
  return events;
}

std::vector<VerifiedConsensusEvent> AsyncConsensusVerifier::WaitForVerified(
    std::chrono::milliseconds timeout) {
  std::deque<VerifiedConsensusEvent> drained;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    if (timeout.count() <= 0) {
      verified_cv_.wait(lk, [this]() { return stop_ || !verified_.empty(); });
    } else {
      verified_cv_.wait_for(lk, timeout,
                            [this]() { return stop_ || !verified_.empty(); });
    }
    drained.swap(verified_);
  }
  std::vector<VerifiedConsensusEvent> events;
  events.reserve(drained.size());
  while (!drained.empty()) {
    events.push_back(std::move(drained.front()));
    drained.pop_front();
  }
  return events;
}

void AsyncConsensusVerifier::WorkerLoop() {
  while (true) {
    WorkItem item;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      cv_.wait(lk, [this]() { return stop_ || !pending_.empty(); });
      if (stop_ && pending_.empty()) {
        return;
      }
      item = std::move(pending_.front());
      pending_.pop_front();
    }

    VerifiedConsensusEvent event = Verify(std::move(item));
    if (event.type == VerifiedConsensusEvent::Type::kNone) {
      invalid_count_.fetch_add(1);
      LOG_EVERY_N(WARNING, 1000)
          << "TD-Hotstuff async verifier rejected artifact: " << event.error;
      continue;
    }
    PushVerified(std::move(event));
  }
}

void AsyncConsensusVerifier::PushVerified(VerifiedConsensusEvent event) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (stop_) {
    return;
  }
  verified_.push_back(std::move(event));
  verified_cv_.notify_one();
}

VerifiedConsensusEvent AsyncConsensusVerifier::Verify(WorkItem item) {
  VerifiedConsensusEvent event;
  event.type = item.type;
  switch (item.type) {
    case VerifiedConsensusEvent::Type::kVote: {
      if (item.vote == nullptr) {
        event.type = VerifiedConsensusEvent::Type::kNone;
        event.error = "missing vote";
        return event;
      }
      event.view = item.vote->view();
      event.signer = item.vote->signer();
      event.hash = item.vote->hash();
      std::string error;
      if (!certificate_verifier_.VerifyVote(*item.vote, &error)) {
        event.type = VerifiedConsensusEvent::Type::kNone;
        event.error = error;
        return event;
      }
      event.vote = std::move(item.vote);
      return event;
    }
    case VerifiedConsensusEvent::Type::kTimeoutVote: {
      if (item.timeout_vote == nullptr) {
        event.type = VerifiedConsensusEvent::Type::kNone;
        event.error = "missing timeout vote";
        return event;
      }
      event.view = item.timeout_vote->view();
      event.signer = item.timeout_vote->signer();
      event.hash = item.timeout_vote->high_qc().hash();
      std::string error;
      if (!certificate_verifier_.VerifyTimeoutVote(*item.timeout_vote, &error)) {
        event.type = VerifiedConsensusEvent::Type::kNone;
        event.error = error;
        return event;
      }
      event.timeout_vote = std::move(item.timeout_vote);
      return event;
    }
    case VerifiedConsensusEvent::Type::kTimeoutCert: {
      if (item.timeout_cert == nullptr) {
        event.type = VerifiedConsensusEvent::Type::kNone;
        event.error = "missing timeout cert";
        return event;
      }
      event.view = item.timeout_cert->view();
      event.hash = TimeoutCertHash(*item.timeout_cert);
      std::string error;
      if (!certificate_verifier_.VerifyTimeoutCert(*item.timeout_cert, &error)) {
        event.type = VerifiedConsensusEvent::Type::kNone;
        event.error = error;
        return event;
      }
      event.timeout_cert = std::move(item.timeout_cert);
      return event;
    }
    case VerifiedConsensusEvent::Type::kNone:
      event.error = "empty work item";
      return event;
  }
  event.type = VerifiedConsensusEvent::Type::kNone;
  event.error = "unknown work item";
  return event;
}

}  // namespace td_hotstuff
}  // namespace resdb
