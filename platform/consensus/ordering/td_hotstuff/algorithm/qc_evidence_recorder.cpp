#include "platform/consensus/ordering/td_hotstuff/algorithm/qc_evidence_recorder.h"

#include <memory>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "platform/consensus/ordering/td_hotstuff/algorithm/vote_score_reputation_plugin.h"

namespace resdb {
namespace td_hotstuff {
namespace {

constexpr const char* kEnableEnv = "TD_HS_EVIDENCE_ENABLE";
constexpr const char* kOutputDirEnv = "TD_HS_EVIDENCE_OUTPUT_DIR";
constexpr const char* kQueueCapacityEnv = "TD_HS_EVIDENCE_QUEUE_CAPACITY";
constexpr size_t kDefaultQueueCapacity = 65536;
constexpr size_t kFlushEveryRecords = 1024;

bool EvidenceEnabledFromEnv() {
  const char* enabled = std::getenv(kEnableEnv);
  return enabled != nullptr && std::string(enabled) == "1";
}

size_t QueueCapacityFromEnv() {
  const char* raw_capacity = std::getenv(kQueueCapacityEnv);
  if (raw_capacity == nullptr || std::string(raw_capacity).empty()) {
    return kDefaultQueueCapacity;
  }
  try {
    const size_t capacity = std::stoull(raw_capacity);
    return capacity == 0 ? kDefaultQueueCapacity : capacity;
  } catch (const std::exception&) {
    LOG(WARNING) << "invalid " << kQueueCapacityEnv << ":" << raw_capacity
                 << ", use default:" << kDefaultQueueCapacity;
    return kDefaultQueueCapacity;
  }
}

std::string OutputDirFromEnv() {
  const char* output_dir = std::getenv(kOutputDirEnv);
  if (output_dir == nullptr || std::string(output_dir).empty()) {
    return ".";
  }
  return output_dir;
}

std::string OutputPath(const std::string& output_dir, int node_id) {
  std::filesystem::path path(output_dir);
  path /= "td_hotstuff_qc_evidence_node_" + std::to_string(node_id) + ".jsonl";
  return path.string();
}

}  // namespace

std::string HexEncode(const std::string& data) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(data.size() * 2);
  for (unsigned char ch : data) {
    hex.push_back(kHex[ch >> 4]);
    hex.push_back(kHex[ch & 0x0f]);
  }
  return hex;
}

std::string SerializeQcEvidenceRecord(const QcEvidenceRecord& record) {
  std::ostringstream out;
  if (record.type == ReputationEvidenceType::kSignedProposalArtifact) {
    out << "{\"schema\":\"td_hotstuff_signed_proposal_artifact_v1\","
        << "\"node_id\":" << record.node_id << ","
        << "\"total_replicas\":" << record.total_replicas << ","
        << "\"protocol_id\":\"" << record.protocol_id << "\","
        << "\"view\":" << record.qc_view << ","
        << "\"slot\":" << record.proposal_slot << ","
        << "\"leader_id\":" << record.leader_id << ","
        << "\"weight_version\":" << record.weight_version << ","
        << "\"active_weight_root\":\"" << record.active_weight_root << "\","
        << "\"proposal_hash_hex\":\"" << HexEncode(record.proposal_hash)
        << "\",\"signature_verified\":"
        << (record.proposal_signature_verified ? "true" : "false") << "}";
    return out.str();
  }
  if (record.type == ReputationEvidenceType::kSignedVoteArtifact) {
    out << "{\"schema\":\"td_hotstuff_signed_vote_artifact_v1\","
        << "\"node_id\":" << record.node_id << ","
        << "\"total_replicas\":" << record.total_replicas << ","
        << "\"protocol_id\":\"" << record.protocol_id << "\","
        << "\"view\":" << record.qc_view << ","
        << "\"slot\":" << record.proposal_slot << ","
        << "\"signer_id\":" << record.vote_signer_id << ","
        << "\"weight_version\":" << record.weight_version << ","
        << "\"active_weight_root\":\"" << record.active_weight_root << "\","
        << "\"proposal_hash_hex\":\"" << HexEncode(record.vote_proposal_hash)
        << "\",\"signature_verified\":"
        << (record.vote_signature_verified ? "true" : "false") << "}";
    return out.str();
  }
  if (record.type == ReputationEvidenceType::kInvalidQcProposalArtifact) {
    out << "{\"schema\":\"td_hotstuff_invalid_qc_proposal_artifact_v1\","
        << "\"node_id\":" << record.node_id << ","
        << "\"total_replicas\":" << record.total_replicas << ","
        << "\"protocol_id\":\"" << record.protocol_id << "\","
        << "\"view\":" << record.qc_view << ","
        << "\"slot\":" << record.proposal_slot << ","
        << "\"leader_id\":" << record.leader_id << ","
        << "\"weight_version\":" << record.weight_version << ","
        << "\"active_weight_root\":\"" << record.active_weight_root << "\","
        << "\"proposal_hash_hex\":\"" << HexEncode(record.proposal_hash)
        << "\",\"proposal_signature_verified\":"
        << (record.proposal_signature_verified ? "true" : "false")
        << ",\"qc_verified\":" << (record.qc_verified ? "true" : "false")
        << ",\"invalid_reason\":\"" << record.invalid_reason << "\"}";
    return out.str();
  }
  if (record.type ==
      ReputationEvidenceType::kSignedWeightUpdateVoteArtifact) {
    out << "{\"schema\":\"td_hotstuff_weight_update_vote_artifact_v1\","
        << "\"node_id\":" << record.node_id << ","
        << "\"total_replicas\":" << record.total_replicas << ","
        << "\"protocol_id\":\"" << record.protocol_id << "\","
        << "\"validator_id\":" << record.vote_signer_id << ","
        << "\"old_weight_root\":\"" << record.old_weight_root << "\","
        << "\"old_weight_version\":" << record.old_weight_version << ","
        << "\"activation_view\":" << record.activation_view << ","
        << "\"candidate_digest_hex\":\"" << HexEncode(record.candidate_digest)
        << "\",\"signature_verified\":"
        << (record.vote_signature_verified ? "true" : "false") << "}";
    return out.str();
  }
  if (record.type == ReputationEvidenceType::kSignedTimeoutVoteArtifact) {
    out << "{\"schema\":\"td_hotstuff_timeout_vote_artifact_v1\","
        << "\"node_id\":" << record.node_id << ","
        << "\"total_replicas\":" << record.total_replicas << ","
        << "\"protocol_id\":\"" << record.protocol_id << "\","
        << "\"view\":" << record.qc_view << ","
        << "\"signer_id\":" << record.vote_signer_id << ","
        << "\"weight_version\":" << record.weight_version << ","
        << "\"active_weight_root\":\"" << record.active_weight_root << "\","
        << "\"high_qc_digest_hex\":\"" << HexEncode(record.high_qc_digest)
        << "\",\"signature_verified\":"
        << (record.vote_signature_verified ? "true" : "false") << "}";
    return out.str();
  }
  if (record.type == ReputationEvidenceType::kInvalidTcProposalArtifact) {
    out << "{\"schema\":\"td_hotstuff_invalid_tc_proposal_artifact_v1\","
        << "\"node_id\":" << record.node_id << ","
        << "\"total_replicas\":" << record.total_replicas << ","
        << "\"protocol_id\":\"" << record.protocol_id << "\","
        << "\"view\":" << record.qc_view << ","
        << "\"slot\":" << record.proposal_slot << ","
        << "\"leader_id\":" << record.leader_id << ","
        << "\"weight_version\":" << record.weight_version << ","
        << "\"active_weight_root\":\"" << record.active_weight_root << "\","
        << "\"proposal_hash_hex\":\"" << HexEncode(record.proposal_hash)
        << "\",\"proposal_signature_verified\":"
        << (record.proposal_signature_verified ? "true" : "false")
        << ",\"timeout_cert_verified\":"
        << (record.timeout_cert_verified ? "true" : "false")
        << ",\"invalid_reason\":\"" << record.invalid_reason << "\"}";
    return out.str();
  }
  if (record.type == ReputationEvidenceType::kVerifiedQcArtifact) {
    out << "{\"schema\":\"td_hotstuff_verified_qc_artifact_v1\","
        << "\"node_id\":" << record.node_id << ","
        << "\"total_replicas\":" << record.total_replicas << ","
        << "\"protocol_id\":\"" << record.protocol_id << "\","
        << "\"view\":" << record.qc_view << ","
        << "\"slot\":" << record.proposal_slot << ","
        << "\"weight_version\":" << record.weight_version << ","
        << "\"active_weight_root\":\"" << record.active_weight_root << "\","
        << "\"qc_hash_hex\":\"" << HexEncode(record.qc_hash) << "\","
        << "\"signer_bitmap_hex\":\"" << HexEncode(record.signer_bitmap)
        << "\",\"qc_verified\":" << (record.qc_verified ? "true" : "false")
        << "}";
    return out.str();
  }
  if (record.leader_opportunity && record.qc_hash.empty()) {
    out << "{\"schema\":\"td_hotstuff_leader_opportunity_evidence_v1\","
        << "\"node_id\":" << record.node_id << ","
        << "\"total_replicas\":" << record.total_replicas << ","
        << "\"view\":" << record.qc_view << ","
        << "\"leader_id\":" << record.leader_id << ","
        << "\"weight_version\":" << record.weight_version << ","
        << "\"active_weight_root\":\"" << record.active_weight_root << "\","
        << "\"leader_eligible_min_weight\":"
        << record.leader_eligible_min_weight << "}";
    return out.str();
  }
  out << "{\"schema\":\"td_hotstuff_qc_evidence_v1\","
      << "\"node_id\":" << record.node_id << ","
      << "\"total_replicas\":" << record.total_replicas << ","
      << "\"qc_view\":" << record.qc_view << ","
      << "\"leader_id\":" << record.leader_id << ","
      << "\"qc_collector_id\":" << record.qc_collector_id << ","
      << "\"weight_version\":" << record.weight_version << ","
      << "\"active_weight_root\":\"" << record.active_weight_root << "\","
      << "\"leader_eligible_min_weight\":"
      << record.leader_eligible_min_weight << ","
      << "\"qc_hash_hex\":\"" << HexEncode(record.qc_hash) << "\","
      << "\"signer_bitmap_hex\":\"" << HexEncode(record.signer_bitmap)
      << "\"";
  if (!record.available_signer_bitmap.empty()) {
    out << ",\"available_signer_bitmap_hex\":\""
        << HexEncode(record.available_signer_bitmap) << "\"";
  }
  out << "}";
  return out.str();
}

AsyncQcEvidenceRecorder::AsyncQcEvidenceRecorder(int node_id,
                                                 int total_replicas,
                                                 std::string output_dir,
                                                 size_t queue_capacity)
    : AsyncQcEvidenceRecorder(node_id, total_replicas, std::move(output_dir),
                              queue_capacity, true, nullptr) {}

AsyncQcEvidenceRecorder::AsyncQcEvidenceRecorder(int node_id,
                                                 int total_replicas,
                                                 std::string output_dir,
                                                 size_t queue_capacity,
                                                 bool write_json,
                                                 std::unique_ptr<AsyncVoteScoreReputationPlugin> reputation_plugin)
    : node_id_(node_id),
      total_replicas_(total_replicas),
      output_dir_(std::move(output_dir)),
      output_path_(OutputPath(output_dir_, node_id_)),
      queue_capacity_(queue_capacity == 0 ? kDefaultQueueCapacity
                                          : queue_capacity),
      write_json_(write_json),
      enabled_(write_json || reputation_plugin != nullptr),
      reputation_plugin_(std::move(reputation_plugin)) {}

AsyncQcEvidenceRecorder::~AsyncQcEvidenceRecorder() { Stop(); }

AsyncQcEvidenceRecorder AsyncQcEvidenceRecorder::Disabled(int node_id) {
  return AsyncQcEvidenceRecorder(node_id, 0, ".", kDefaultQueueCapacity, false,
                                 nullptr);
}

std::unique_ptr<AsyncQcEvidenceRecorder> AsyncQcEvidenceRecorder::CreateFromEnv(
    int node_id, int total_replicas,
    const std::vector<int64_t>& current_weights) {
  std::unique_ptr<AsyncVoteScoreReputationPlugin> reputation_plugin =
      AsyncVoteScoreReputationPlugin::CreateFromEnv(node_id, total_replicas,
                                                    current_weights);
  const bool write_json = EvidenceEnabledFromEnv();
  if (!write_json && reputation_plugin == nullptr) {
    return nullptr;
  }
  std::unique_ptr<AsyncQcEvidenceRecorder> recorder(new AsyncQcEvidenceRecorder(
      node_id, total_replicas, OutputDirFromEnv(), QueueCapacityFromEnv(),
      write_json, std::move(reputation_plugin)));
  recorder->Start();
  return recorder;
}

void AsyncQcEvidenceRecorder::Start() {
  if (!enabled_ || started_ || !write_json_) {
    return;
  }
  stopping_.store(false);
  started_ = true;
  worker_ = std::thread(&AsyncQcEvidenceRecorder::WorkerLoop, this);
}

void AsyncQcEvidenceRecorder::Stop() {
  if (started_) {
    stopping_.store(true);
    cv_.notify_all();
    if (worker_.joinable()) {
      worker_.join();
    }
    started_ = false;
  }
  if (reputation_plugin_ != nullptr) {
    reputation_plugin_->Stop();
  }
}

bool AsyncQcEvidenceRecorder::Enqueue(const QcEvidenceRecord& record) {
  if (!enabled_) {
    return false;
  }
  std::unique_lock<std::mutex> lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    DropRecord(record.qc_view);
    return false;
  }
  if (queue_.size() >= queue_capacity_) {
    DropRecord(record.qc_view);
    return false;
  }
  queue_.push_back(record);
  // Polling lets the worker batch QCs without waking it on every consensus event.
  return true;
}

bool AsyncQcEvidenceRecorder::RecordQc(int qc_view, const std::string& qc_hash,
                                       const std::string& signer_bitmap) {
  return RecordQc(qc_view, qc_hash, signer_bitmap, /*leader_id=*/0,
                  /*weight_version=*/0, /*active_weight_root=*/"");
}

bool AsyncQcEvidenceRecorder::RecordQc(int qc_view, const std::string& qc_hash,
                                       const std::string& signer_bitmap,
                                       int leader_id, uint64_t weight_version,
                                       std::string active_weight_root,
                                       int64_t leader_eligible_min_weight,
                                       std::string available_signer_bitmap,
                                       int qc_collector_id) {
  if (!enabled_ || qc_hash.empty()) {
    return false;
  }
  if (!write_json_ && reputation_plugin_ != nullptr) {
    return reputation_plugin_->RecordQc(
        qc_view, qc_hash, signer_bitmap, leader_id, weight_version,
        std::move(active_weight_root), leader_eligible_min_weight,
        std::move(available_signer_bitmap), qc_collector_id);
  }
  QcEvidenceRecord record;
  record.type = ReputationEvidenceType::kCertifiedQc;
  record.node_id = node_id_;
  record.total_replicas = total_replicas_;
  record.qc_view = qc_view;
  record.leader_id = leader_id;
  record.qc_collector_id = qc_collector_id;
  record.weight_version = weight_version;
  record.active_weight_root = std::move(active_weight_root);
  record.leader_eligible_min_weight = leader_eligible_min_weight;
  record.qc_hash = qc_hash;
  record.signer_bitmap = signer_bitmap;
  record.available_signer_bitmap = std::move(available_signer_bitmap);
  return Enqueue(record);
}

bool AsyncQcEvidenceRecorder::RecordLeaderOpportunity(
    int view, int leader_id, uint64_t weight_version,
    std::string active_weight_root, int64_t leader_eligible_min_weight) {
  if (!enabled_ || view <= 0 || leader_id <= 0) {
    return false;
  }
  if (!write_json_ && reputation_plugin_ != nullptr) {
    return reputation_plugin_->RecordLeaderOpportunity(
        view, leader_id, weight_version, std::move(active_weight_root),
        leader_eligible_min_weight);
  }
  QcEvidenceRecord record;
  record.type = ReputationEvidenceType::kLeaderOpportunity;
  record.node_id = node_id_;
  record.total_replicas = total_replicas_;
  record.qc_view = view;
  record.leader_id = leader_id;
  record.leader_opportunity = true;
  record.weight_version = weight_version;
  record.active_weight_root = std::move(active_weight_root);
  record.leader_eligible_min_weight = leader_eligible_min_weight;
  return Enqueue(record);
}

bool AsyncQcEvidenceRecorder::RecordSignedProposalArtifact(
    const ::resdb::consensus::reputation::SignedProposalEvidence& artifact) {
  if (!enabled_ || artifact.protocol_id.empty() || artifact.leader_id <= 0 ||
      artifact.view_or_round <= 0 || artifact.proposal_hash.empty()) {
    return false;
  }
  if (!write_json_ && reputation_plugin_ != nullptr) {
    return reputation_plugin_->RecordSignedProposalArtifact(artifact);
  }
  QcEvidenceRecord record;
  record.type = ReputationEvidenceType::kSignedProposalArtifact;
  record.node_id = node_id_;
  record.total_replicas = total_replicas_;
  record.protocol_id = artifact.protocol_id;
  record.qc_view = artifact.view_or_round;
  record.leader_id = artifact.leader_id;
  record.proposal_slot = artifact.slot_or_height;
  record.proposal_hash = artifact.proposal_hash;
  record.proposal_signature_verified = artifact.signature_verified;
  record.weight_version = artifact.weight_version;
  record.active_weight_root = artifact.active_weight_root;
  return Enqueue(record);
}

bool AsyncQcEvidenceRecorder::RecordSignedVoteArtifact(
    const ::resdb::consensus::reputation::SignedVoteEvidence& artifact) {
  if (!enabled_ || artifact.protocol_id.empty() || artifact.signer_id <= 0 ||
      artifact.view_or_round <= 0 || artifact.proposal_hash.empty()) {
    return false;
  }
  if (!write_json_ && reputation_plugin_ != nullptr) {
    return reputation_plugin_->RecordSignedVoteArtifact(artifact);
  }
  QcEvidenceRecord record;
  record.type = ReputationEvidenceType::kSignedVoteArtifact;
  record.node_id = node_id_;
  record.total_replicas = total_replicas_;
  record.protocol_id = artifact.protocol_id;
  record.qc_view = artifact.view_or_round;
  record.proposal_slot = artifact.slot_or_height;
  record.vote_signer_id = artifact.signer_id;
  record.vote_proposal_hash = artifact.proposal_hash;
  record.vote_signature_verified = artifact.signature_verified;
  record.weight_version = artifact.weight_version;
  record.active_weight_root = artifact.active_weight_root;
  return Enqueue(record);
}

bool AsyncQcEvidenceRecorder::RecordInvalidQcProposalArtifact(
    const ::resdb::consensus::reputation::InvalidQcProposalEvidence& artifact) {
  if (!enabled_ || artifact.protocol_id.empty() || artifact.leader_id <= 0 ||
      artifact.view_or_round <= 0 || artifact.proposal_hash.empty()) {
    return false;
  }
  if (!write_json_ && reputation_plugin_ != nullptr) {
    return reputation_plugin_->RecordInvalidQcProposalArtifact(artifact);
  }
  QcEvidenceRecord record;
  record.type = ReputationEvidenceType::kInvalidQcProposalArtifact;
  record.node_id = node_id_;
  record.total_replicas = total_replicas_;
  record.protocol_id = artifact.protocol_id;
  record.qc_view = artifact.view_or_round;
  record.leader_id = artifact.leader_id;
  record.proposal_slot = artifact.slot_or_height;
  record.proposal_hash = artifact.proposal_hash;
  record.proposal_signature_verified = artifact.proposal_signature_verified;
  record.qc_verified = artifact.qc_verified;
  record.invalid_reason = artifact.invalid_reason;
  record.weight_version = artifact.weight_version;
  record.active_weight_root = artifact.active_weight_root;
  return Enqueue(record);
}

bool AsyncQcEvidenceRecorder::RecordSignedWeightUpdateVoteArtifact(
    const ::resdb::consensus::reputation::SignedWeightUpdateVoteEvidence&
        artifact) {
  if (!enabled_ || artifact.protocol_id.empty() || artifact.validator_id <= 0 ||
      artifact.old_weight_root.empty() || artifact.activation_view <= 0 ||
      artifact.candidate_digest.empty()) {
    return false;
  }
  if (!write_json_ && reputation_plugin_ != nullptr) {
    return reputation_plugin_->RecordSignedWeightUpdateVoteArtifact(artifact);
  }
  QcEvidenceRecord record;
  record.type = ReputationEvidenceType::kSignedWeightUpdateVoteArtifact;
  record.node_id = node_id_;
  record.total_replicas = total_replicas_;
  record.protocol_id = artifact.protocol_id;
  record.vote_signer_id = artifact.validator_id;
  record.old_weight_root = artifact.old_weight_root;
  record.old_weight_version = artifact.old_weight_version;
  record.activation_view = artifact.activation_view;
  record.candidate_digest = artifact.candidate_digest;
  record.vote_signature_verified = artifact.signature_verified;
  record.weight_version = artifact.weight_version;
  record.active_weight_root = artifact.active_weight_root;
  return Enqueue(record);
}

bool AsyncQcEvidenceRecorder::RecordSignedTimeoutVoteArtifact(
    const ::resdb::consensus::reputation::SignedTimeoutVoteEvidence& artifact) {
  if (!enabled_ || artifact.protocol_id.empty() || artifact.signer_id <= 0 ||
      artifact.view_or_round <= 0 || artifact.high_qc_digest.empty()) {
    return false;
  }
  if (!write_json_ && reputation_plugin_ != nullptr) {
    return reputation_plugin_->RecordSignedTimeoutVoteArtifact(artifact);
  }
  QcEvidenceRecord record;
  record.type = ReputationEvidenceType::kSignedTimeoutVoteArtifact;
  record.node_id = node_id_;
  record.total_replicas = total_replicas_;
  record.protocol_id = artifact.protocol_id;
  record.qc_view = artifact.view_or_round;
  record.vote_signer_id = artifact.signer_id;
  record.high_qc_digest = artifact.high_qc_digest;
  record.vote_signature_verified = artifact.signature_verified;
  record.weight_version = artifact.weight_version;
  record.active_weight_root = artifact.active_weight_root;
  return Enqueue(record);
}

bool AsyncQcEvidenceRecorder::RecordInvalidTcProposalArtifact(
    const ::resdb::consensus::reputation::InvalidTcProposalEvidence& artifact) {
  if (!enabled_ || artifact.protocol_id.empty() || artifact.leader_id <= 0 ||
      artifact.view_or_round <= 0 || artifact.proposal_hash.empty()) {
    return false;
  }
  if (!write_json_ && reputation_plugin_ != nullptr) {
    return reputation_plugin_->RecordInvalidTcProposalArtifact(artifact);
  }
  QcEvidenceRecord record;
  record.type = ReputationEvidenceType::kInvalidTcProposalArtifact;
  record.node_id = node_id_;
  record.total_replicas = total_replicas_;
  record.protocol_id = artifact.protocol_id;
  record.qc_view = artifact.view_or_round;
  record.leader_id = artifact.leader_id;
  record.proposal_slot = artifact.slot_or_height;
  record.proposal_hash = artifact.proposal_hash;
  record.proposal_signature_verified = artifact.proposal_signature_verified;
  record.timeout_cert_verified = artifact.timeout_cert_verified;
  record.invalid_reason = artifact.invalid_reason;
  record.weight_version = artifact.weight_version;
  record.active_weight_root = artifact.active_weight_root;
  return Enqueue(record);
}

bool AsyncQcEvidenceRecorder::RecordVerifiedQcArtifact(
    const ::resdb::consensus::reputation::VerifiedQcArtifactEvidence& artifact) {
  if (!enabled_ || artifact.protocol_id.empty() || artifact.view_or_round <= 0 ||
      artifact.qc_hash.empty() || artifact.signer_bitmap.empty()) {
    return false;
  }
  if (!write_json_ && reputation_plugin_ != nullptr) {
    return reputation_plugin_->RecordVerifiedQcArtifact(artifact);
  }
  QcEvidenceRecord record;
  record.type = ReputationEvidenceType::kVerifiedQcArtifact;
  record.node_id = node_id_;
  record.total_replicas = total_replicas_;
  record.protocol_id = artifact.protocol_id;
  record.qc_view = artifact.view_or_round;
  record.proposal_slot = artifact.slot_or_height;
  record.qc_hash = artifact.qc_hash;
  record.signer_bitmap = artifact.signer_bitmap;
  record.qc_verified = artifact.qc_verified;
  record.weight_version = artifact.weight_version;
  record.active_weight_root = artifact.active_weight_root;
  return Enqueue(record);
}

std::vector<VoteScoreCandidate>
AsyncQcEvidenceRecorder::TakeCompletedReputationCandidates() {
  if (reputation_plugin_ == nullptr) {
    return {};
  }
  return reputation_plugin_->TakeCompletedCandidates();
}

void AsyncQcEvidenceRecorder::UpdateReputationWeights(
    std::vector<int64_t> current_weights, std::string old_weight_root_hex,
    uint64_t old_weight_version) {
  if (reputation_plugin_ == nullptr) {
    return;
  }
  reputation_plugin_->UpdateCurrentWeights(
      std::move(current_weights), std::move(old_weight_root_hex),
      old_weight_version);
}

void AsyncQcEvidenceRecorder::DropRecord(int qc_view) {
  uint64_t dropped = dropped_count_.fetch_add(1) + 1;
  if (dropped == 1 || dropped % 1000 == 0) {
    LOG(WARNING) << "dropped TD-Hotstuff QC evidence record, node:" << node_id_
                 << " view:" << qc_view << " dropped_count:" << dropped;
  }
}

void AsyncQcEvidenceRecorder::WorkerLoop() {
  bool write_json = write_json_;
  std::ofstream output;
  if (write_json) {
    std::filesystem::create_directories(output_dir_);
    output.open(output_path_, std::ios::app);
    if (!output.is_open()) {
      LOG(ERROR) << "open TD-Hotstuff QC evidence output fail:" << output_path_;
      write_json = false;
      if (reputation_plugin_ == nullptr) {
        return;
      }
    }
  }

  size_t pending_flush = 0;
  while (true) {
    QcEvidenceRecord record;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
        return stopping_.load() || !queue_.empty();
      });
      if (queue_.empty()) {
        if (stopping_.load()) {
          break;
        }
        if (write_json) {
          output.flush();
          pending_flush = 0;
        }
        continue;
      }
      record = std::move(queue_.front());
      queue_.pop_front();
    }

    if (write_json) {
      output << SerializeQcEvidenceRecord(record) << '\n';
      ++pending_flush;
      if (pending_flush >= kFlushEveryRecords) {
        output.flush();
        pending_flush = 0;
      }
    }
    if (reputation_plugin_ != nullptr) {
      if (record.type == ReputationEvidenceType::kSignedProposalArtifact) {
        ::resdb::consensus::reputation::SignedProposalEvidence artifact;
        artifact.protocol_id = record.protocol_id;
        artifact.leader_id = record.leader_id;
        artifact.view_or_round = record.qc_view;
        artifact.slot_or_height = record.proposal_slot;
        artifact.proposal_hash = record.proposal_hash;
        artifact.signature_verified = record.proposal_signature_verified;
        artifact.active_weight_root = record.active_weight_root;
        artifact.weight_version = record.weight_version;
        reputation_plugin_->RecordSignedProposalArtifact(artifact);
      } else if (record.type == ReputationEvidenceType::kSignedVoteArtifact) {
        ::resdb::consensus::reputation::SignedVoteEvidence artifact;
        artifact.protocol_id = record.protocol_id;
        artifact.signer_id = record.vote_signer_id;
        artifact.view_or_round = record.qc_view;
        artifact.slot_or_height = record.proposal_slot;
        artifact.proposal_hash = record.vote_proposal_hash;
        artifact.signature_verified = record.vote_signature_verified;
        artifact.active_weight_root = record.active_weight_root;
        artifact.weight_version = record.weight_version;
        reputation_plugin_->RecordSignedVoteArtifact(artifact);
      } else if (record.type ==
                 ReputationEvidenceType::kInvalidQcProposalArtifact) {
        ::resdb::consensus::reputation::InvalidQcProposalEvidence artifact;
        artifact.protocol_id = record.protocol_id;
        artifact.leader_id = record.leader_id;
        artifact.view_or_round = record.qc_view;
        artifact.slot_or_height = record.proposal_slot;
        artifact.proposal_hash = record.proposal_hash;
        artifact.proposal_signature_verified =
            record.proposal_signature_verified;
        artifact.qc_verified = record.qc_verified;
        artifact.invalid_reason = record.invalid_reason;
        artifact.active_weight_root = record.active_weight_root;
        artifact.weight_version = record.weight_version;
        reputation_plugin_->RecordInvalidQcProposalArtifact(artifact);
      } else if (record.type ==
                 ReputationEvidenceType::kSignedWeightUpdateVoteArtifact) {
        ::resdb::consensus::reputation::SignedWeightUpdateVoteEvidence artifact;
        artifact.protocol_id = record.protocol_id;
        artifact.validator_id = record.vote_signer_id;
        artifact.old_weight_root = record.old_weight_root;
        artifact.old_weight_version = record.old_weight_version;
        artifact.activation_view = record.activation_view;
        artifact.candidate_digest = record.candidate_digest;
        artifact.signature_verified = record.vote_signature_verified;
        artifact.active_weight_root = record.active_weight_root;
        artifact.weight_version = record.weight_version;
        reputation_plugin_->RecordSignedWeightUpdateVoteArtifact(artifact);
      } else if (record.type ==
                 ReputationEvidenceType::kSignedTimeoutVoteArtifact) {
        ::resdb::consensus::reputation::SignedTimeoutVoteEvidence artifact;
        artifact.protocol_id = record.protocol_id;
        artifact.signer_id = record.vote_signer_id;
        artifact.view_or_round = record.qc_view;
        artifact.high_qc_digest = record.high_qc_digest;
        artifact.signature_verified = record.vote_signature_verified;
        artifact.active_weight_root = record.active_weight_root;
        artifact.weight_version = record.weight_version;
        reputation_plugin_->RecordSignedTimeoutVoteArtifact(artifact);
      } else if (record.type ==
                 ReputationEvidenceType::kInvalidTcProposalArtifact) {
        ::resdb::consensus::reputation::InvalidTcProposalEvidence artifact;
        artifact.protocol_id = record.protocol_id;
        artifact.leader_id = record.leader_id;
        artifact.view_or_round = record.qc_view;
        artifact.slot_or_height = record.proposal_slot;
        artifact.proposal_hash = record.proposal_hash;
        artifact.proposal_signature_verified =
            record.proposal_signature_verified;
        artifact.timeout_cert_verified = record.timeout_cert_verified;
        artifact.invalid_reason = record.invalid_reason;
        artifact.active_weight_root = record.active_weight_root;
        artifact.weight_version = record.weight_version;
        reputation_plugin_->RecordInvalidTcProposalArtifact(artifact);
      } else if (record.type == ReputationEvidenceType::kVerifiedQcArtifact) {
        ::resdb::consensus::reputation::VerifiedQcArtifactEvidence artifact;
        artifact.protocol_id = record.protocol_id;
        artifact.view_or_round = record.qc_view;
        artifact.slot_or_height = record.proposal_slot;
        artifact.qc_hash = record.qc_hash;
        artifact.signer_bitmap = record.signer_bitmap;
        artifact.qc_verified = record.qc_verified;
        artifact.active_weight_root = record.active_weight_root;
        artifact.weight_version = record.weight_version;
        reputation_plugin_->RecordVerifiedQcArtifact(artifact);
      } else if (record.leader_opportunity && record.qc_hash.empty()) {
        reputation_plugin_->RecordLeaderOpportunity(
            record.qc_view, record.leader_id, record.weight_version,
            record.active_weight_root, record.leader_eligible_min_weight);
      } else {
        reputation_plugin_->RecordQc(record.qc_view, record.qc_hash,
                                     record.signer_bitmap, record.leader_id,
                                     record.weight_version,
                                     record.active_weight_root,
                                     record.leader_eligible_min_weight,
                                     record.available_signer_bitmap,
                                     record.qc_collector_id);
      }
    }
  }
  if (write_json) {
    output.flush();
  }
}

}  // namespace td_hotstuff
}  // namespace resdb
