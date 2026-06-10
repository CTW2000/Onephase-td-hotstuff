#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <fstream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "platform/consensus/reputation/reputation_algorithm.h"
#include "platform/consensus/reputation/reputation_types.h"

namespace resdb {
namespace consensus {
namespace reputation {

struct CertifiedSignerEvidenceRecord {
  int view_or_round = 0;
  int slot_or_height = 0;
  int leader_id = 0;
  std::string artifact_digest;
  std::string signer_bitmap;
  std::string available_signer_bitmap;
  std::string weight_root_hex;
  uint64_t weight_version = 0;
  std::vector<int64_t> active_weights;
};

struct LeaderOutcomeEvidenceRecord {
  int view_or_round = 0;
  int leader_id = 0;
  OutcomeClass outcome_class = OutcomeClass::kNone;
  std::string artifact_digest;
  std::string weight_root_hex;
  uint64_t weight_version = 0;
  std::vector<int64_t> active_weights;
};

struct ReputationWeightSnapshot {
  std::vector<int64_t> weights;
  std::string weight_root_hex;
  uint64_t weight_version = 0;
  bool leader_selection_enabled = false;
  std::vector<int64_t> leader_weights;
  std::string leader_weight_root_hex;
  uint64_t leader_weight_version = 0;
  int leader_selection_version = 1;
  int64_t leader_eligible_min_weight = 10;
};

struct ReputationRuntimeOptions {
  bool enabled = false;
  int total_replicas = 0;
  size_t window_size_views = 4096;
  size_t queue_capacity = 65536;
  int activation_delay_windows = 1;
  std::vector<int64_t> initial_weights;
  std::string initial_weight_root;
  uint64_t initial_weight_version = 0;
  bool leader_selection_enabled = false;
  std::vector<int64_t> initial_leader_weights;
  std::string initial_leader_weight_root;
  uint64_t initial_leader_weight_version = 0;
  bool audit_jsonl_enabled = false;
  std::string audit_jsonl_path;
  ReputationConfig config;
};

struct ReputationCandidateKey {
  std::string old_weight_root_hex;
  uint64_t old_weight_version = 0;
  int start_view = 0;
  int end_view = 0;
  int activation_view = 0;
  std::string candidate_digest_hex;

  static ReputationCandidateKey FromCandidate(const ReputationCandidate& candidate);

  bool operator<(const ReputationCandidateKey& other) const {
    return std::tie(old_weight_root_hex, old_weight_version, start_view,
                    end_view, activation_view, candidate_digest_hex) <
           std::tie(other.old_weight_root_hex, other.old_weight_version,
                    other.start_view, other.end_view, other.activation_view,
                    other.candidate_digest_hex);
  }
};

class ReputationPluginRuntime {
 public:
  explicit ReputationPluginRuntime(ReputationRuntimeOptions options);
  ~ReputationPluginRuntime();

  ReputationPluginRuntime(const ReputationPluginRuntime&) = delete;
  ReputationPluginRuntime& operator=(const ReputationPluginRuntime&) = delete;

  void Start();
  void Stop();

  bool RecordEvidence(CertifiedSignerEvidenceRecord evidence);
  bool RecordLeaderOutcome(LeaderOutcomeEvidenceRecord evidence);
  bool RecordSignedProposalEvidence(SignedProposalEvidence evidence);
  bool RecordSignedVoteEvidence(SignedVoteEvidence evidence);
  bool AdvanceWatermark(int view_or_round);
  void UpdateActiveWeights(ReputationWeightSnapshot snapshot);

  std::vector<ReputationCandidate> TakeCompletedCandidates();
  std::optional<ReputationCandidate> FindLocalCandidate(
      const ReputationCandidateKey& key) const;

  bool enabled() const { return options_.enabled; }
  uint64_t queued_count() const { return queued_count_.load(); }
  uint64_t dropped_count() const { return dropped_count_.load(); }
  uint64_t computed_window_count() const { return computed_window_count_.load(); }

 private:
  struct RuntimeEvent;
  struct WindowKey;
  struct WindowBuffer;

  bool Enqueue(RuntimeEvent event);
  void WorkerLoop();
  void ProcessEvidence(CertifiedSignerEvidenceRecord evidence);
  void ProcessLeaderOutcome(LeaderOutcomeEvidenceRecord evidence);
  void ProcessSignedProposalEvidence(SignedProposalEvidence evidence);
  void ProcessSignedVoteEvidence(SignedVoteEvidence evidence);
  void ProcessWatermark(int view_or_round);
  void FinalizeWindow(const WindowKey& key, WindowBuffer* buffer);
  void ApplyPersistentStrongFaults(ReputationCandidate* candidate);
  void PushCompleted(ReputationCandidate candidate);
  void WriteAudit(const ReputationCandidate& candidate);
  ReputationWeightSnapshot SnapshotFromFields(
      const std::string& weight_root_hex, uint64_t weight_version,
      const std::vector<int64_t>& weights) const;
  ReputationWeightSnapshot SnapshotForEvidence(
      const CertifiedSignerEvidenceRecord& evidence) const;
  ReputationWeightSnapshot SnapshotForLeaderOutcome(
      const LeaderOutcomeEvidenceRecord& evidence) const;
  ReputationWeightSnapshot SnapshotForSignedProposal(
      const SignedProposalEvidence& evidence) const;
  ReputationWeightSnapshot SnapshotForSignedVote(
      const SignedVoteEvidence& evidence) const;
  std::vector<uint64_t> ScheduledLeaderCountsForWindow(
      int start_view, int end_view,
      const ReputationWeightSnapshot& snapshot) const;

  ReputationRuntimeOptions options_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool started_ = false;
  bool stop_ = false;
  std::deque<RuntimeEvent> pending_;
  std::thread worker_;

  ReputationWeightSnapshot active_snapshot_;
  std::map<std::pair<std::string, uint64_t>, std::vector<int64_t>> known_weights_;
  std::map<std::pair<std::string, uint64_t>, ReputationWeightSnapshot>
      known_snapshots_;
  std::map<WindowKey, WindowBuffer> windows_;
  std::map<uint64_t, ReputationCandidate> completed_by_version_;
  std::map<ReputationCandidateKey, ReputationCandidate> completed_index_;
  std::set<int> persistent_strong_fault_validators_;
  std::ofstream audit_file_;

  std::atomic<uint64_t> queued_count_{0};
  std::atomic<uint64_t> dropped_count_{0};
  std::atomic<uint64_t> computed_window_count_{0};
};

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
