#include "platform/consensus/ordering/td_hotstuff/adapter/td_hotstuff_reputation_adapter.h"

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "platform/consensus/reputation/reputation_roots.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr size_t kDefaultWindowSize = 4096;
constexpr size_t kDefaultQueueCapacity = 65536;

bool EnvFlagEnabled(const char* name) {
  const char* raw = std::getenv(name);
  if (raw == nullptr) {
    return false;
  }
  const std::string value(raw);
  return value == "1" || value == "true" || value == "TRUE" ||
         value == "yes" || value == "YES" || value == "on" ||
         value == "ON";
}

size_t PositiveSizeFromEnv(const char* name, size_t fallback) {
  const char* raw = std::getenv(name);
  if (raw == nullptr || std::string(raw).empty()) {
    return fallback;
  }
  char* end = nullptr;
  const unsigned long parsed = std::strtoul(raw, &end, 10);
  if (end == raw || parsed == 0 ||
      parsed > static_cast<unsigned long>(std::numeric_limits<size_t>::max())) {
    return fallback;
  }
  return static_cast<size_t>(parsed);
}

int PositiveIntFromEnv(const char* name, int fallback) {
  return static_cast<int>(PositiveSizeFromEnv(name, fallback));
}

std::string AuditPathForNode(int local_node_id) {
  return "td_hotstuff_reputation_node_" + std::to_string(local_node_id) +
         ".jsonl";
}

}  // namespace

resdb::consensus::reputation::CertifiedSignerEvidence
ToCertifiedSignerEvidence(const TdHotstuffQcEvidenceSnapshot& snapshot) {
  resdb::consensus::reputation::CertifiedSignerEvidence evidence;
  evidence.view_or_round = snapshot.view;
  evidence.leader_id = snapshot.leader_id;
  evidence.artifact_digest = snapshot.qc_hash;
  evidence.signer_bitmap = snapshot.signer_bitmap;
  evidence.available_signer_bitmap = snapshot.available_signer_bitmap;
  return evidence;
}

resdb::consensus::reputation::CertifiedSignerEvidenceRecord
ToCertifiedSignerEvidenceRecord(const TdHotstuffQcEvidenceSnapshot& snapshot) {
  resdb::consensus::reputation::CertifiedSignerEvidenceRecord record;
  record.view_or_round = snapshot.view;
  record.slot_or_height = snapshot.slot;
  record.leader_id = snapshot.leader_id;
  record.artifact_digest = snapshot.qc_hash;
  record.signer_bitmap = snapshot.signer_bitmap;
  record.available_signer_bitmap = snapshot.available_signer_bitmap;
  record.weight_root_hex = snapshot.active_weight_root;
  record.weight_version = snapshot.active_weight_version;
  record.active_weights = snapshot.active_weights;
  return record;
}

resdb::consensus::reputation::LeaderOutcomeEvidenceRecord
ToLeaderOutcomeEvidenceRecord(
    const TdHotstuffLeaderOutcomeEvidenceSnapshot& snapshot) {
  resdb::consensus::reputation::LeaderOutcomeEvidenceRecord record;
  record.view_or_round = snapshot.view;
  record.leader_id = snapshot.leader_id;
  record.outcome_class = snapshot.outcome_class;
  record.artifact_digest = snapshot.artifact_digest;
  record.weight_root_hex = snapshot.active_weight_root;
  record.weight_version = snapshot.active_weight_version;
  record.active_weights = snapshot.active_weights;
  return record;
}

resdb::consensus::reputation::SignedProposalEvidence ToSignedProposalEvidence(
    const TdHotstuffSignedProposalEvidenceSnapshot& snapshot) {
  resdb::consensus::reputation::SignedProposalEvidence evidence;
  evidence.protocol_id = "td_hotstuff";
  evidence.leader_id = snapshot.leader_id;
  evidence.view_or_round = snapshot.view;
  evidence.slot_or_height = snapshot.slot;
  evidence.proposal_hash = snapshot.proposal_hash;
  evidence.signature_verified = snapshot.signature_verified;
  evidence.active_weight_root = snapshot.active_weight_root;
  evidence.weight_version = snapshot.active_weight_version;
  return evidence;
}

resdb::consensus::reputation::SignedVoteEvidence ToSignedVoteEvidence(
    const TdHotstuffSignedVoteEvidenceSnapshot& snapshot) {
  resdb::consensus::reputation::SignedVoteEvidence evidence;
  evidence.protocol_id = "td_hotstuff";
  evidence.signer_id = snapshot.signer_id;
  evidence.view_or_round = snapshot.view;
  evidence.slot_or_height = snapshot.slot;
  evidence.proposal_hash = snapshot.proposal_hash;
  evidence.signature_verified = snapshot.signature_verified;
  evidence.active_weight_root = snapshot.active_weight_root;
  evidence.weight_version = snapshot.active_weight_version;
  return evidence;
}

resdb::consensus::reputation::SignedWeightUpdateVoteEvidence
ToSignedWeightUpdateVoteEvidence(
    const TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot& snapshot) {
  resdb::consensus::reputation::SignedWeightUpdateVoteEvidence evidence;
  evidence.protocol_id = "td_hotstuff";
  evidence.validator_id = snapshot.validator_id;
  evidence.old_weight_root = snapshot.old_weight_root;
  evidence.old_weight_version = snapshot.old_weight_version;
  evidence.activation_view = snapshot.activation_view;
  evidence.candidate_digest = snapshot.candidate_digest;
  evidence.signature_verified = snapshot.signature_verified;
  evidence.active_weight_root = snapshot.active_weight_root;
  evidence.weight_version = snapshot.active_weight_version;
  return evidence;
}

resdb::consensus::reputation::InvalidQcProposalEvidence
ToInvalidQcProposalEvidence(
    const TdHotstuffInvalidQcProposalEvidenceSnapshot& snapshot) {
  resdb::consensus::reputation::InvalidQcProposalEvidence evidence;
  evidence.protocol_id = "td_hotstuff";
  evidence.leader_id = snapshot.leader_id;
  evidence.view_or_round = snapshot.view;
  evidence.slot_or_height = snapshot.slot;
  evidence.proposal_hash = snapshot.proposal_hash;
  evidence.proposal_signature_verified = snapshot.proposal_signature_verified;
  evidence.qc_verified = snapshot.qc_verified;
  evidence.invalid_reason = snapshot.invalid_reason;
  evidence.active_weight_root = snapshot.active_weight_root;
  evidence.weight_version = snapshot.active_weight_version;
  return evidence;
}

TdHotstuffReputationAdapterOptions
TdHotstuffReputationAdapter::OptionsFromEnv() {
  TdHotstuffReputationAdapterOptions options;
  options.enabled = EnvFlagEnabled("TD_HS_REPUTATION_ENABLE");
  options.window_size = PositiveSizeFromEnv(
      "TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS",
      PositiveSizeFromEnv("TD_HS_REPUTATION_WINDOW_SIZE", kDefaultWindowSize));
  options.queue_capacity = PositiveSizeFromEnv(
      "TD_HS_REPUTATION_ADAPTER_QUEUE_CAPACITY",
      PositiveSizeFromEnv("TD_HS_REPUTATION_QUEUE_CAPACITY",
                          kDefaultQueueCapacity));
  options.min_candidate_events = PositiveSizeFromEnv(
      "TD_HS_REPUTATION_MIN_CANDIDATE_QCS", options.min_candidate_events);
  options.activation_delay_windows = PositiveIntFromEnv(
      "TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY", 1);
  options.reputation_config.decay_per_epoch = PositiveIntFromEnv(
      "TD_HS_REPUTATION_DECAY_PER_EPOCH",
      options.reputation_config.decay_per_epoch);
  options.reputation_config.max_recovery_per_epoch = PositiveIntFromEnv(
      "TD_HS_REPUTATION_MAX_RECOVERY_PER_EPOCH",
      options.reputation_config.max_recovery_per_epoch);
  options.reputation_config.bonus_per_epoch = PositiveIntFromEnv(
      "TD_HS_REPUTATION_BONUS_PER_EPOCH",
      options.reputation_config.bonus_per_epoch);
  options.reputation_config.min_weight = PositiveIntFromEnv(
      "TD_HS_REPUTATION_MIN_WEIGHT",
      options.reputation_config.min_weight);
  options.reputation_config.max_weight = PositiveIntFromEnv(
      "TD_HS_REPUTATION_MAX_WEIGHT",
      options.reputation_config.max_weight);
  options.reputation_config.min_decay_opportunities = PositiveSizeFromEnv(
      "TD_HS_REPUTATION_MIN_DECAY_OPPORTUNITIES",
      options.reputation_config.min_decay_opportunities);
  options.reputation_config.leader_eligible_min_weight = PositiveIntFromEnv(
      "TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT",
      options.reputation_config.leader_eligible_min_weight);
  options.reputation_config.min_leader_opportunities = PositiveIntFromEnv(
      "TD_HS_REPUTATION_MIN_LEADER_OPPORTUNITIES",
      options.reputation_config.min_leader_opportunities);
  options.leader_selection_enabled = EnvFlagEnabled("TD_HS_LEADER_SELECTION_ENABLE");
  options.reputation_config.leader_recovery_enabled =
      EnvFlagEnabled("TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE");
  options.reputation_config.strong_fault_enabled =
      EnvFlagEnabled("TD_HS_STRONG_FAULT_ENABLE");
  options.reputation_config.double_proposal_detection_enabled =
      EnvFlagEnabled("TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE");
  options.reputation_config.double_vote_detection_enabled =
      EnvFlagEnabled("TD_HS_DOUBLE_VOTE_DETECT_ENABLE");
  options.reputation_config.invalid_qc_proposal_detection_enabled =
      EnvFlagEnabled("TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE");
  options.reputation_config.weight_update_vote_equivocation_detection_enabled =
      EnvFlagEnabled("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE");
  options.reputation_config.timeout_vote_equivocation_detection_enabled =
      EnvFlagEnabled("TD_HS_TIMEOUT_VOTE_EQUIVOCATION_DETECT_ENABLE");
  options.reputation_config.invalid_tc_proposal_detection_enabled =
      EnvFlagEnabled("TD_HS_INVALID_TC_PROPOSAL_DETECT_ENABLE");
  options.reputation_config.conflicting_qc_detection_enabled =
      EnvFlagEnabled("TD_HS_CONFLICTING_QC_DETECT_ENABLE");
  options.reputation_config.strong_fault_target_weight = PositiveIntFromEnv(
      "TD_HS_STRONG_FAULT_TARGET_WEIGHT",
      options.reputation_config.strong_fault_target_weight);
  options.signed_proposal_evidence_enabled =
      options.reputation_config.strong_fault_enabled &&
      options.reputation_config.double_proposal_detection_enabled;
  options.signed_vote_evidence_enabled =
      options.reputation_config.strong_fault_enabled &&
      options.reputation_config.double_vote_detection_enabled;
  options.signed_weight_update_vote_evidence_enabled =
      options.reputation_config.strong_fault_enabled &&
      options.reputation_config
          .weight_update_vote_equivocation_detection_enabled;
  options.invalid_qc_proposal_evidence_enabled =
      options.reputation_config.strong_fault_enabled &&
      options.reputation_config.invalid_qc_proposal_detection_enabled;
  options.audit_jsonl_enabled =
      EnvFlagEnabled("TD_HS_REPUTATION_AUDIT_JSONL_ENABLE");
  const char* audit_path = std::getenv("TD_HS_REPUTATION_AUDIT_JSONL_PATH");
  if (audit_path != nullptr) {
    options.audit_jsonl_path = audit_path;
  }
  return options;
}

TdHotstuffReputationAdapter::TdHotstuffReputationAdapter(
    int local_node_id, int total_replicas,
    TdHotstuffReputationAdapterOptions options)
    : TdHotstuffReputationAdapter(
          local_node_id, total_replicas,
          RuntimeFromOptions(local_node_id, total_replicas, options),
          options.enabled) {
  signed_proposal_evidence_enabled_ = options.signed_proposal_evidence_enabled;
  signed_vote_evidence_enabled_ = options.signed_vote_evidence_enabled;
  signed_weight_update_vote_evidence_enabled_ =
      options.signed_weight_update_vote_evidence_enabled;
  invalid_qc_proposal_evidence_enabled_ =
      options.invalid_qc_proposal_evidence_enabled;
}

TdHotstuffReputationAdapter::TdHotstuffReputationAdapter(
    int local_node_id, int total_replicas,
    std::shared_ptr<resdb::consensus::reputation::ReputationPluginRuntime>
        runtime,
    bool enabled)
    : local_node_id_(local_node_id),
      total_replicas_(total_replicas),
      enabled_(enabled),
      signed_proposal_evidence_enabled_(false),
      signed_vote_evidence_enabled_(false),
      signed_weight_update_vote_evidence_enabled_(false),
      invalid_qc_proposal_evidence_enabled_(false),
      runtime_(std::move(runtime)) {}

TdHotstuffReputationAdapter::~TdHotstuffReputationAdapter() { Stop(); }

std::shared_ptr<resdb::consensus::reputation::ReputationPluginRuntime>
TdHotstuffReputationAdapter::RuntimeFromOptions(
    int local_node_id, int total_replicas,
    const TdHotstuffReputationAdapterOptions& options) {
  resdb::consensus::reputation::ReputationRuntimeOptions runtime_options;
  runtime_options.enabled = options.enabled;
  runtime_options.total_replicas = total_replicas;
  runtime_options.window_size_views = options.window_size;
  runtime_options.queue_capacity = options.queue_capacity;
  runtime_options.min_candidate_events = options.min_candidate_events;
  runtime_options.activation_delay_windows = options.activation_delay_windows;
  runtime_options.initial_weights = options.initial_weights;
  runtime_options.initial_weight_root = options.initial_weight_root;
  runtime_options.initial_weight_version = options.initial_weight_version;
  runtime_options.leader_selection_enabled = options.leader_selection_enabled;
  runtime_options.initial_leader_weights = options.initial_leader_weights;
  runtime_options.initial_leader_weight_root = options.initial_leader_weight_root;
  runtime_options.initial_leader_weight_version =
      options.initial_leader_weight_version;
  runtime_options.audit_jsonl_enabled = options.audit_jsonl_enabled;
  runtime_options.audit_jsonl_path = options.audit_jsonl_path.empty()
                                        ? AuditPathForNode(local_node_id)
                                        : options.audit_jsonl_path;
  runtime_options.config = options.reputation_config;
  return std::make_shared<
      resdb::consensus::reputation::ReputationPluginRuntime>(
      std::move(runtime_options));
}

void TdHotstuffReputationAdapter::Start() {
  if (enabled_ && runtime_ != nullptr) {
    runtime_->Start();
  }
}

void TdHotstuffReputationAdapter::Stop() {
  if (runtime_ != nullptr) {
    runtime_->Stop();
  }
}

bool TdHotstuffReputationAdapter::TryRecordCertifiedQc(
    TdHotstuffQcEvidenceSnapshot snapshot) {
  if (!enabled_ || runtime_ == nullptr) {
    return false;
  }
  if (snapshot.total_replicas <= 0) {
    snapshot.total_replicas = total_replicas_;
  }
  if (snapshot.local_node_id <= 0) {
    snapshot.local_node_id = local_node_id_;
  }
  return runtime_->RecordEvidence(ToCertifiedSignerEvidenceRecord(snapshot));
}

bool TdHotstuffReputationAdapter::TryRecordLeaderOutcome(
    TdHotstuffLeaderOutcomeEvidenceSnapshot snapshot) {
  if (!enabled_ || runtime_ == nullptr) {
    return false;
  }
  if (snapshot.total_replicas <= 0) {
    snapshot.total_replicas = total_replicas_;
  }
  if (snapshot.local_node_id <= 0) {
    snapshot.local_node_id = local_node_id_;
  }
  return runtime_->RecordLeaderOutcome(
      ToLeaderOutcomeEvidenceRecord(snapshot));
}

bool TdHotstuffReputationAdapter::TryRecordSignedProposal(
    TdHotstuffSignedProposalEvidenceSnapshot snapshot) {
  if (!WantsSignedProposalEvidence() || runtime_ == nullptr) {
    return false;
  }
  if (snapshot.total_replicas <= 0) {
    snapshot.total_replicas = total_replicas_;
  }
  if (snapshot.local_node_id <= 0) {
    snapshot.local_node_id = local_node_id_;
  }
  return runtime_->RecordSignedProposalEvidence(
      ToSignedProposalEvidence(snapshot));
}

bool TdHotstuffReputationAdapter::TryRecordSignedVote(
    TdHotstuffSignedVoteEvidenceSnapshot snapshot) {
  if (!WantsSignedVoteEvidence() || runtime_ == nullptr) {
    return false;
  }
  if (snapshot.total_replicas <= 0) {
    snapshot.total_replicas = total_replicas_;
  }
  if (snapshot.local_node_id <= 0) {
    snapshot.local_node_id = local_node_id_;
  }
  return runtime_->RecordSignedVoteEvidence(ToSignedVoteEvidence(snapshot));
}

bool TdHotstuffReputationAdapter::TryRecordSignedWeightUpdateVote(
    TdHotstuffSignedWeightUpdateVoteEvidenceSnapshot snapshot) {
  if (!WantsSignedWeightUpdateVoteEvidence() || runtime_ == nullptr) {
    return false;
  }
  if (snapshot.total_replicas <= 0) {
    snapshot.total_replicas = total_replicas_;
  }
  if (snapshot.local_node_id <= 0) {
    snapshot.local_node_id = local_node_id_;
  }
  return runtime_->RecordSignedWeightUpdateVoteEvidence(
      ToSignedWeightUpdateVoteEvidence(snapshot));
}

bool TdHotstuffReputationAdapter::TryRecordInvalidQcProposal(
    TdHotstuffInvalidQcProposalEvidenceSnapshot snapshot) {
  if (!WantsInvalidQcProposalEvidence() || runtime_ == nullptr) {
    return false;
  }
  if (snapshot.total_replicas <= 0) {
    snapshot.total_replicas = total_replicas_;
  }
  if (snapshot.local_node_id <= 0) {
    snapshot.local_node_id = local_node_id_;
  }
  return runtime_->RecordInvalidQcProposalEvidence(
      ToInvalidQcProposalEvidence(snapshot));
}

bool TdHotstuffReputationAdapter::AdvanceWatermark(int view) {
  return enabled_ && runtime_ != nullptr && runtime_->AdvanceWatermark(view);
}

void TdHotstuffReputationAdapter::UpdateActiveWeights(
    resdb::consensus::reputation::ReputationWeightSnapshot snapshot) {
  if (runtime_ != nullptr) {
    runtime_->UpdateActiveWeights(std::move(snapshot));
  }
}

std::vector<resdb::consensus::reputation::ReputationCandidate>
TdHotstuffReputationAdapter::TakeCompletedCandidates() {
  if (runtime_ == nullptr) {
    return {};
  }
  return runtime_->TakeCompletedCandidates();
}

std::vector<resdb::consensus::reputation::ReputationCandidate>
TdHotstuffReputationAdapter::DrainCompletedCandidatesForTesting() {
  return TakeCompletedCandidates();
}

std::optional<resdb::consensus::reputation::ReputationCandidate>
TdHotstuffReputationAdapter::FindLocalCandidate(
    const resdb::consensus::reputation::ReputationCandidateKey& key) const {
  if (runtime_ == nullptr) {
    return std::nullopt;
  }
  return runtime_->FindLocalCandidate(key);
}

bool TdHotstuffReputationAdapter::WantsSignedProposalEvidence() const {
  return enabled_ && signed_proposal_evidence_enabled_;
}

bool TdHotstuffReputationAdapter::WantsSignedVoteEvidence() const {
  return enabled_ && signed_vote_evidence_enabled_;
}

bool TdHotstuffReputationAdapter::WantsSignedWeightUpdateVoteEvidence() const {
  return enabled_ && signed_weight_update_vote_evidence_enabled_;
}

bool TdHotstuffReputationAdapter::WantsInvalidQcProposalEvidence() const {
  return enabled_ && invalid_qc_proposal_evidence_enabled_;
}

uint64_t TdHotstuffReputationAdapter::queued_count() const {
  return runtime_ == nullptr ? 0 : runtime_->queued_count();
}

uint64_t TdHotstuffReputationAdapter::dropped_count() const {
  return runtime_ == nullptr ? 0 : runtime_->dropped_count();
}

uint64_t TdHotstuffReputationAdapter::computed_window_count() const {
  return runtime_ == nullptr ? 0 : runtime_->computed_window_count();
}

}  // namespace td_hotstuff
}  // namespace resdb
