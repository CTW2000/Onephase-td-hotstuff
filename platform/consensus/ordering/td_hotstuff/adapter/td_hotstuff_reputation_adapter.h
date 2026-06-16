#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "platform/consensus/reputation/reputation_plugin_runtime.h"
#include "platform/consensus/reputation/reputation_types.h"

namespace resdb {
namespace td_hotstuff {

struct TdHotstuffQcEvidenceSnapshot {
  int local_node_id = 0;
  int total_replicas = 0;
  int view = 0;
  int slot = 0;
  int leader_id = 0;
  std::string qc_hash;
  std::string signer_bitmap;
  std::string available_signer_bitmap;
  std::vector<int64_t> active_weights;
  std::string active_weight_root;
  uint64_t active_weight_version = 0;
};

struct TdHotstuffLeaderOutcomeEvidenceSnapshot {
  int local_node_id = 0;
  int total_replicas = 0;
  int view = 0;
  int leader_id = 0;
  resdb::consensus::reputation::OutcomeClass outcome_class =
      resdb::consensus::reputation::OutcomeClass::kNone;
  std::string artifact_digest;
  std::vector<int64_t> active_weights;
  std::string active_weight_root;
  uint64_t active_weight_version = 0;
};

struct TdHotstuffSignedProposalEvidenceSnapshot {
  int local_node_id = 0;
  int total_replicas = 0;
  int view = 0;
  int slot = 0;
  int leader_id = 0;
  std::string proposal_hash;
  bool signature_verified = false;
  std::string active_weight_root;
  uint64_t active_weight_version = 0;
};

struct TdHotstuffSignedVoteEvidenceSnapshot {
  int local_node_id = 0;
  int total_replicas = 0;
  int view = 0;
  int slot = 0;
  int signer_id = 0;
  std::string proposal_hash;
  bool signature_verified = false;
  std::string active_weight_root;
  uint64_t active_weight_version = 0;
};

struct TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot {
  int local_node_id = 0;
  int total_replicas = 0;
  int validator_id = 0;
  std::string old_weight_root;
  uint64_t old_weight_version = 0;
  int activation_view = 0;
  std::string candidate_digest;
  bool signature_verified = false;
  std::string active_weight_root;
  uint64_t active_weight_version = 0;
};

struct TdHotstuffInvalidQcProposalEvidenceSnapshot {
  int local_node_id = 0;
  int total_replicas = 0;
  int view = 0;
  int slot = 0;
  int leader_id = 0;
  std::string proposal_hash;
  bool proposal_signature_verified = false;
  bool qc_verified = false;
  std::string invalid_reason;
  std::string active_weight_root;
  uint64_t active_weight_version = 0;
};

struct TdHotstuffReputationAdapterOptions {
  bool enabled = false;
  size_t window_size = 4096;
  size_t queue_capacity = 65536;
  size_t min_candidate_events = 1;
  int activation_delay_windows = 4;
  bool audit_jsonl_enabled = false;
  std::string audit_jsonl_path;
  std::vector<int64_t> initial_weights;
  std::string initial_weight_root;
  uint64_t initial_weight_version = 0;
  bool leader_selection_enabled = false;
  std::vector<int64_t> initial_leader_weights;
  std::string initial_leader_weight_root;
  uint64_t initial_leader_weight_version = 0;
  resdb::consensus::reputation::ReputationConfig reputation_config;
  bool signed_proposal_evidence_enabled = false;
  bool signed_vote_evidence_enabled = false;
  bool signed_weight_update_vote_evidence_enabled = false;
  bool invalid_qc_proposal_evidence_enabled = false;
};

resdb::consensus::reputation::CertifiedSignerEvidence
ToCertifiedSignerEvidence(const TdHotstuffQcEvidenceSnapshot& snapshot);

resdb::consensus::reputation::CertifiedSignerEvidenceRecord
ToCertifiedSignerEvidenceRecord(const TdHotstuffQcEvidenceSnapshot& snapshot);

resdb::consensus::reputation::LeaderOutcomeEvidenceRecord
ToLeaderOutcomeEvidenceRecord(
    const TdHotstuffLeaderOutcomeEvidenceSnapshot& snapshot);

resdb::consensus::reputation::SignedProposalEvidence ToSignedProposalEvidence(
    const TdHotstuffSignedProposalEvidenceSnapshot& snapshot);

resdb::consensus::reputation::SignedVoteEvidence ToSignedVoteEvidence(
    const TdHotstuffSignedVoteEvidenceSnapshot& snapshot);

resdb::consensus::reputation::SignedWeightUpdateVoteEvidence
ToSignedWeightUpdateVoteEvidence(
    const TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot& snapshot);

resdb::consensus::reputation::InvalidQcProposalEvidence
ToInvalidQcProposalEvidence(
    const TdHotstuffInvalidQcProposalEvidenceSnapshot& snapshot);

class TdHotstuffReputationAdapter {
 public:
  static TdHotstuffReputationAdapterOptions OptionsFromEnv();

  TdHotstuffReputationAdapter(
      int local_node_id, int total_replicas,
      TdHotstuffReputationAdapterOptions options = OptionsFromEnv());
  TdHotstuffReputationAdapter(
      int local_node_id, int total_replicas,
      std::shared_ptr<resdb::consensus::reputation::ReputationPluginRuntime>
          runtime,
      bool enabled = true);
  ~TdHotstuffReputationAdapter();

  TdHotstuffReputationAdapter(const TdHotstuffReputationAdapter&) = delete;
  TdHotstuffReputationAdapter& operator=(
      const TdHotstuffReputationAdapter&) = delete;

  void Start();
  void Stop();

  bool TryRecordCertifiedQc(TdHotstuffQcEvidenceSnapshot snapshot);
  bool TryRecordLeaderOutcome(TdHotstuffLeaderOutcomeEvidenceSnapshot snapshot);
  bool TryRecordSignedProposal(
      TdHotstuffSignedProposalEvidenceSnapshot snapshot);
  bool TryRecordSignedVote(TdHotstuffSignedVoteEvidenceSnapshot snapshot);
  bool TryRecordSignedWeightUpdateVote(
      TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot snapshot);
  bool TryRecordInvalidQcProposal(
      TdHotstuffInvalidQcProposalEvidenceSnapshot snapshot);
  bool AdvanceWatermark(int view);
  void UpdateActiveWeights(
      resdb::consensus::reputation::ReputationWeightSnapshot snapshot);

  std::vector<resdb::consensus::reputation::ReputationCandidate>
  TakeCompletedCandidates();
  std::vector<resdb::consensus::reputation::ReputationCandidate>
  DrainCompletedCandidatesForTesting();
  std::optional<resdb::consensus::reputation::ReputationCandidate>
  FindLocalCandidate(
      const resdb::consensus::reputation::ReputationCandidateKey& key) const;

  bool enabled() const { return enabled_; }
  bool WantsSignedProposalEvidence() const;
  bool WantsSignedVoteEvidence() const;
  bool WantsSignedWeightUpdateVoteEvidence() const;
  bool WantsInvalidQcProposalEvidence() const;
  bool IsPersistentStrongFaultValidator(int validator_id) const;
  uint64_t queued_count() const;
  uint64_t dropped_count() const;
  uint64_t computed_window_count() const;

 private:
  static std::shared_ptr<resdb::consensus::reputation::ReputationPluginRuntime>
  RuntimeFromOptions(int local_node_id, int total_replicas,
                     const TdHotstuffReputationAdapterOptions& options);

  const int local_node_id_;
  const int total_replicas_;
  bool enabled_ = false;
  bool signed_proposal_evidence_enabled_ = false;
  bool signed_vote_evidence_enabled_ = false;
  bool signed_weight_update_vote_evidence_enabled_ = false;
  bool invalid_qc_proposal_evidence_enabled_ = false;
  std::shared_ptr<resdb::consensus::reputation::ReputationPluginRuntime>
      runtime_;
};

}  // namespace td_hotstuff
}  // namespace resdb
