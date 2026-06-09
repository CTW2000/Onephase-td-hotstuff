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
  options.activation_delay_windows = PositiveIntFromEnv(
      "TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY", 1);
  options.min_activation_lead_views = PositiveIntFromEnv(
      "TD_HS_WEIGHT_UPDATE_MIN_ACTIVATION_LEAD_VIEWS",
      options.min_activation_lead_views);
  options.reputation_config.leader_eligible_min_weight = PositiveIntFromEnv(
      "TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT",
      options.reputation_config.leader_eligible_min_weight);
  options.reputation_config.leader_weight_deadband = PositiveIntFromEnv(
      "TD_HS_REPUTATION_LEADER_WEIGHT_DEADBAND",
      options.reputation_config.leader_weight_deadband);
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
          options.enabled) {}

TdHotstuffReputationAdapter::TdHotstuffReputationAdapter(
    int local_node_id, int total_replicas,
    std::shared_ptr<resdb::consensus::reputation::ReputationPluginRuntime>
        runtime,
    bool enabled)
    : local_node_id_(local_node_id),
      total_replicas_(total_replicas),
      enabled_(enabled),
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
  runtime_options.activation_delay_windows = options.activation_delay_windows;
  runtime_options.min_activation_lead_views = options.min_activation_lead_views;
  runtime_options.initial_weights = options.initial_weights;
  runtime_options.initial_weight_root = options.initial_weight_root;
  runtime_options.initial_weight_version = options.initial_weight_version;
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
