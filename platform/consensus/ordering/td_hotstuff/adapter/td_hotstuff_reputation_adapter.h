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

struct TdHotstuffReputationAdapterOptions {
  bool enabled = false;
  size_t window_size = 4096;
  size_t queue_capacity = 65536;
  int activation_delay_windows = 1;
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
};

resdb::consensus::reputation::CertifiedSignerEvidence
ToCertifiedSignerEvidence(const TdHotstuffQcEvidenceSnapshot& snapshot);

resdb::consensus::reputation::CertifiedSignerEvidenceRecord
ToCertifiedSignerEvidenceRecord(const TdHotstuffQcEvidenceSnapshot& snapshot);

resdb::consensus::reputation::LeaderOutcomeEvidenceRecord
ToLeaderOutcomeEvidenceRecord(
    const TdHotstuffLeaderOutcomeEvidenceSnapshot& snapshot);

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
  std::shared_ptr<resdb::consensus::reputation::ReputationPluginRuntime>
      runtime_;
};

}  // namespace td_hotstuff
}  // namespace resdb
