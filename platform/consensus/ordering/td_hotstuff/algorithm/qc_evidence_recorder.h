#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "platform/consensus/reputation/reputation_algorithm.h"

namespace resdb {
namespace td_hotstuff {

class AsyncVoteScoreReputationPlugin;
struct VoteScoreCandidate;

enum class ReputationEvidenceType {
  kCertifiedQc,
  kLeaderOpportunity,
  kSignedProposalArtifact,
  kSignedVoteArtifact,
  kInvalidQcProposalArtifact,
  kSignedWeightUpdateVoteArtifact,
  kSignedTimeoutVoteArtifact,
  kInvalidTcProposalArtifact,
  kVerifiedQcArtifact,
};

struct ReputationEvidence {
  ReputationEvidenceType type = ReputationEvidenceType::kCertifiedQc;
  int node_id = 0;
  int total_replicas = 0;
  int qc_view = 0;
  int leader_id = 0;
  int qc_collector_id = 0;
  bool leader_opportunity = false;
  uint64_t weight_version = 0;
  std::string active_weight_root;
  int64_t leader_eligible_min_weight = 0;
  std::string qc_hash;
  std::string signer_bitmap;
  std::string available_signer_bitmap;
  std::string protocol_id;
  int proposal_slot = 0;
  std::string proposal_hash;
  bool proposal_signature_verified = false;
  int vote_signer_id = 0;
  std::string vote_proposal_hash;
  bool vote_signature_verified = false;
  bool qc_verified = false;
  std::string invalid_reason;
  std::string old_weight_root;
  uint64_t old_weight_version = 0;
  int activation_view = 0;
  std::string candidate_digest;
  std::string high_qc_digest;
  bool timeout_cert_verified = false;
};

using QcEvidenceRecord = ReputationEvidence;

std::string HexEncode(const std::string& data);
std::string SerializeQcEvidenceRecord(const QcEvidenceRecord& record);

class AsyncQcEvidenceRecorder {
 public:
  AsyncQcEvidenceRecorder(int node_id, int total_replicas,
                          std::string output_dir, size_t queue_capacity);
  ~AsyncQcEvidenceRecorder();

  AsyncQcEvidenceRecorder(const AsyncQcEvidenceRecorder&) = delete;
  AsyncQcEvidenceRecorder& operator=(const AsyncQcEvidenceRecorder&) = delete;

  static AsyncQcEvidenceRecorder Disabled(int node_id);
  static std::unique_ptr<AsyncQcEvidenceRecorder> CreateFromEnv(
      int node_id, int total_replicas,
      const std::vector<int64_t>& current_weights);

  void Start();
  void Stop();
  bool Enqueue(const QcEvidenceRecord& record);
  bool RecordQc(int qc_view, const std::string& qc_hash,
                const std::string& signer_bitmap);
  bool RecordQc(int qc_view, const std::string& qc_hash,
                const std::string& signer_bitmap, int leader_id,
                uint64_t weight_version, std::string active_weight_root,
                int64_t leader_eligible_min_weight = 0,
                std::string available_signer_bitmap = "",
                int qc_collector_id = 0);
  bool RecordLeaderOpportunity(int view, int leader_id,
                               uint64_t weight_version,
                               std::string active_weight_root,
                               int64_t leader_eligible_min_weight = 0);
  bool RecordSignedProposalArtifact(
      const ::resdb::consensus::reputation::SignedProposalEvidence& artifact);
  bool RecordSignedVoteArtifact(
      const ::resdb::consensus::reputation::SignedVoteEvidence& artifact);
  bool RecordInvalidQcProposalArtifact(
      const ::resdb::consensus::reputation::InvalidQcProposalEvidence& artifact);
  bool RecordSignedWeightUpdateVoteArtifact(
      const ::resdb::consensus::reputation::SignedWeightUpdateVoteEvidence&
          artifact);
  bool RecordSignedTimeoutVoteArtifact(
      const ::resdb::consensus::reputation::SignedTimeoutVoteEvidence& artifact);
  bool RecordInvalidTcProposalArtifact(
      const ::resdb::consensus::reputation::InvalidTcProposalEvidence& artifact);
  bool RecordVerifiedQcArtifact(
      const ::resdb::consensus::reputation::VerifiedQcArtifactEvidence& artifact);
  std::vector<VoteScoreCandidate> TakeCompletedReputationCandidates();
  void UpdateReputationWeights(std::vector<int64_t> current_weights,
                               std::string old_weight_root_hex,
                               uint64_t old_weight_version);

  bool enabled() const { return enabled_; }
  uint64_t dropped_count() const { return dropped_count_.load(); }

 private:
  AsyncQcEvidenceRecorder(int node_id, int total_replicas,
                          std::string output_dir, size_t queue_capacity,
                          bool write_json,
                          std::unique_ptr<AsyncVoteScoreReputationPlugin> reputation_plugin);

  void WorkerLoop();
  void DropRecord(int qc_view);

  int node_id_;
  int total_replicas_;
  std::string output_dir_;
  std::string output_path_;
  size_t queue_capacity_;
  bool write_json_;
  bool enabled_;
  std::unique_ptr<AsyncVoteScoreReputationPlugin> reputation_plugin_;

  std::atomic<bool> stopping_{false};
  std::atomic<uint64_t> dropped_count_{0};
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<QcEvidenceRecord> queue_;
  std::thread worker_;
  bool started_ = false;
};

}  // namespace td_hotstuff
}  // namespace resdb
