#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "common/utils/utils.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/certificate_verifier.h"

namespace resdb {
namespace td_hotstuff {
namespace {

int PositiveIntFromEnv(const char* name, int default_value) {
  const char* raw = std::getenv(name);
  if (raw == nullptr || std::string(raw).empty()) {
    return default_value;
  }
  try {
    const int value = std::stoi(raw);
    return value > 0 ? value : default_value;
  } catch (...) {
    return default_value;
  }
}

int NonNegativeIntFromEnv(const char* name, int default_value) {
  const char* raw = std::getenv(name);
  if (raw == nullptr || std::string(raw).empty()) {
    return default_value;
  }
  try {
    const int value = std::stoi(raw);
    return value >= 0 ? value : default_value;
  } catch (...) {
    return default_value;
  }
}

bool ExperimentStartedAtView(const char* env_name, int view) {
  return view >= NonNegativeIntFromEnv(env_name, 0);
}

bool StrongFaultExperimentStartedAtView(const char* env_name, int view) {
  const char* raw = std::getenv(env_name);
  if (raw != nullptr && !std::string(raw).empty()) {
    return view >= NonNegativeIntFromEnv(env_name, 0);
  }
  return ExperimentStartedAtView("TD_HS_STRONG_FAULT_ATTACK_START_VIEW", view);
}

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

bool WeightUpdateEnabled() {
  return EnvFlagEnabled("TD_HS_WEIGHT_UPDATE_ENABLE");
}

bool ReputationEnabled() {
  return EnvFlagEnabled("TD_HS_REPUTATION_ENABLE");
}

bool LeaderSelectionEnabled() {
  return EnvFlagEnabled("TD_HS_LEADER_SELECTION_ENABLE");
}

int ReputationBootstrapWeightFromEnv() {
  return PositiveIntFromEnv(
      "TD_HS_REPUTATION_BOOTSTRAP_WEIGHT",
      20);
}

std::vector<int64_t> InitialReplicaWeightsForMode(
    const std::vector<int64_t>& replica_weights, int total_replicas) {
  if (!replica_weights.empty() ||
      (!ReputationEnabled() && !WeightUpdateEnabled() &&
       !LeaderSelectionEnabled())) {
    return replica_weights;
  }
  return std::vector<int64_t>(std::max(total_replicas, 0),
                              ReputationBootstrapWeightFromEnv());
}

std::string TimeoutCertDigest(const TimeoutCert& cert) {
  std::string bytes;
  cert.SerializeToString(&bytes);
  return SignatureVerifier::CalculateHash(bytes);
}

int64_t LeaderEligibleMinWeightFromEnv() {
  return PositiveIntFromEnv("TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT", 10);
}

bool EnvListContainsId(const char* name, int id) {
  const char* raw = std::getenv(name);
  if (raw == nullptr || std::string(raw).empty()) {
    return false;
  }
  const std::string input(raw);
  size_t start = 0;
  while (start <= input.size()) {
    const size_t comma = input.find(',', start);
    const std::string item = input.substr(
        start, comma == std::string::npos ? std::string::npos : comma - start);
    if (!item.empty()) {
      try {
        if (std::stoi(item) == id) {
          return true;
        }
      } catch (...) {
      }
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  return false;
}

std::vector<int> IntListFromEnv(const char* name) {
  const char* raw = std::getenv(name);
  if (raw == nullptr || std::string(raw).empty()) {
    return {};
  }
  std::vector<int> values;
  const std::string input(raw);
  size_t start = 0;
  while (start <= input.size()) {
    const size_t comma = input.find(',', start);
    const std::string item = input.substr(
        start, comma == std::string::npos ? std::string::npos : comma - start);
    if (!item.empty()) {
      try {
        const int value = std::stoi(item);
        if (value > 0) {
          values.push_back(value);
        }
      } catch (...) {
      }
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  return values;
}

std::string TransactionDedupKey(const Transaction& txn) {
  if (!txn.hash().empty()) {
    return std::string("h:") + txn.hash();
  }
  if (txn.proxy_id() != 0 || txn.user_seq() != 0) {
    return std::string("u:") + std::to_string(txn.proxy_id()) + ":" +
           std::to_string(txn.user_seq());
  }
  return std::string("d:") + SignatureVerifier::CalculateHash(txn.data());
}

std::string ProposalHashForExperiment(const Proposal& proposal) {
  std::string data;
  for (const auto& txn : proposal.transactions()) {
    std::string serialized;
    txn.SerializeToString(&serialized);
    data += serialized;
  }
  std::string header_data;
  proposal.header().SerializeToString(&header_data);
  data += header_data;
  return SignatureVerifier::CalculateHash(data);
}

}  // namespace

HotStuff::HotStuff(int id, int f, int total_num, SignatureVerifier* verifier,
                   int non_responsive_num, int fork_tail_num,
                   uint64_t timer_length,
                   const std::vector<int64_t>& replica_weights,
                   int64_t quorum_weight)
    : ProtocolBase(id, f, total_num),
      verifier_(verifier),
      non_responsive_num_(non_responsive_num),
      fork_tail_num_(fork_tail_num),
      timer_length_(timer_length),
      timeout_config_(TimeoutConfigFromEnv()),
      replica_weights_(NormalizeReplicaWeights(
          InitialReplicaWeightsForMode(replica_weights, total_num),
          total_num)),
      quorum_weight_(quorum_weight > 0
                         ? quorum_weight
                         : CalculateQuorumWeight(replica_weights_)),
      weight_schedule_(
          std::make_shared<WeightSchedule>(total_num, replica_weights_)),
      leader_schedule_(std::make_shared<LeaderSelectionSchedule>(
          total_num, replica_weights_, LeaderSelectionEnabled(),
          LeaderEligibleMinWeightFromEnv())) {
  LOG(ERROR) << "id:" << id << " f:" << f << " total:" << total_num_;

  global_stats_ = Stats::GetGlobalStats();
  proposal_manager_ = std::make_unique<ProposalManager>(
      id, 2 * f_ + 1, verifier, total_num, non_responsive_num, fork_tail_num,
      replica_weights_, quorum_weight_, weight_schedule_, leader_schedule_);
  if (timeout_config_.enabled) {
    timeout_manager_ = std::make_unique<TimeoutManager>(
        id_, total_num_, verifier_, weight_schedule_);
  }
  TdHotstuffReputationAdapterOptions reputation_options =
      TdHotstuffReputationAdapter::OptionsFromEnv();
  if (reputation_options.enabled) {
    reputation_options.initial_weights = weight_schedule_->ActiveWeights();
    reputation_options.initial_weight_root = weight_schedule_->ActiveWeightRoot();
    reputation_options.initial_weight_version = weight_schedule_->ActiveWeightVersion();
    reputation_adapter_ = std::make_unique<TdHotstuffReputationAdapter>(
        id_, total_num_, std::move(reputation_options));
    reputation_adapter_->Start();
    if (WeightUpdateEnabled()) {
      CertifiedWeightUpdatePipeline::Callbacks callbacks;
      callbacks.current_view = [this] { return CurrentView(); };
      callbacks.broadcast_candidate =
          [this](const CandidateWeightUpdate& candidate) {
            if (broadcast_call_ != nullptr) {
              broadcast_call_(MessageType::CandidateWeightUpdateMsg, candidate);
            }
          };
      callbacks.broadcast_vote = [this](const WeightUpdateVote& vote) {
        if (broadcast_call_ != nullptr) {
          broadcast_call_(MessageType::WeightUpdateVoteMsg, vote);
        }
      };
      callbacks.broadcast_cert = [this](const WeightUpdateCert& cert) {
        if (broadcast_call_ != nullptr) {
          broadcast_call_(MessageType::WeightUpdateCertMsg, cert);
        }
      };
      weight_update_pipeline_ = std::make_unique<CertifiedWeightUpdatePipeline>(
          id_, total_num_, weight_schedule_, leader_schedule_, verifier_,
          reputation_adapter_.get(), std::move(callbacks));
    }
  }
  async_verifier_ = std::make_unique<AsyncConsensusVerifier>(
      total_num_, verifier_, AsyncVerifierWorkerCountFromEnv(),
      AsyncVerifierQueueCapacityFromEnv(), weight_schedule_);
  async_verifier_->Start();
  timeout_empty_proposal_budget_views_ =
      timeout_config_.enabled
          ? PositiveIntFromEnv("TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS", total_num_)
          : 0;
  send_thread_ = std::thread(&HotStuff::AsyncSend, this);
  commit_thread_ = std::thread(&HotStuff::AsyncCommit, this);
  verified_event_thread_ = std::thread(&HotStuff::AsyncVerifiedEvents, this);
  if (reputation_adapter_ != nullptr) {
    weight_update_thread_ = std::thread(&HotStuff::AsyncWeightUpdates, this);
  }
  if (timeout_manager_ != nullptr) {
    timeout_thread_ = std::thread(&HotStuff::AsyncTimeout, this);
  }
}

HotStuff::~HotStuff() {
  stop_verified_events_.store(true);
  if (async_verifier_ != nullptr) {
    async_verifier_->Stop();
  }
  if (verified_event_thread_.joinable()) {
    verified_event_thread_.join();
  }
  stop_weight_updates_.store(true);
  if (weight_update_thread_.joinable()) {
    weight_update_thread_.join();
  }
  if (reputation_adapter_ != nullptr) {
    reputation_adapter_->Stop();
  }

  stop_timeout_.store(true);
  if (timeout_thread_.joinable()) {
    timeout_thread_.join();
  }
}

int HotStuff::LeaderForView(int view) {
  return proposal_manager_ != nullptr ? proposal_manager_->GetLeader(view)
                                      : DefaultLeaderForView(view, total_num_);
}

int HotStuff::NextLeader(int view) { return LeaderForView(view + 1); }

bool HotStuff::IsLeader(int view) { return LeaderForView(view) == id_; }

namespace {
std::string SilentLeaderModeFromEnv() {
  const char* raw = std::getenv("TD_HS_SILENT_LEADER_MODE");
  return (raw != nullptr && *raw != '\0') ? std::string(raw)
                                          : std::string("static");
}
std::string SlowVoteModeFromEnv() {
  const char* raw = std::getenv("TD_HS_SLOW_VOTE_MODE");
  return (raw != nullptr && *raw != '\0') ? std::string(raw)
                                          : std::string("static");
}
// Deterministic, reproducible per-(view,id) value in [0,1000) for the
// probability-driven modes — every replica and every rerun agrees.
int SilentDeterministicPermille(int view, int id) {
  uint64_t x = static_cast<uint64_t>(static_cast<uint32_t>(view)) *
                   0x9E3779B97F4A7C15ULL +
               static_cast<uint64_t>(static_cast<uint32_t>(id)) +
               0x165667B19E3779F9ULL;
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  return static_cast<int>(x % 1000);
}
}  // namespace

// Switchable silent-leader attack model. The faulty leader decides whether to
// stay silent (skip its proposal) this slot from: TD_HS_SILENT_LEADER_MODE, its
// own activated leader weight, a per-node leader-slot counter, and a
// deterministic per-(view,id) hash. Soft fault only: it never alters the
// committed log, only whether this leader proposes.
bool HotStuff::IsSilentLeaderForExperiment(int view) const {
  if (!ExperimentStartedAtView("TD_HS_SILENT_LEADER_START_VIEW", view)) {
    return false;
  }
  if (!(EnvFlagEnabled("TD_HS_SILENT_LEADER") ||
        EnvListContainsId("TD_HS_SILENT_LEADER_IDS", id_))) {
    return false;
  }
  const std::string mode = SilentLeaderModeFromEnv();
  if (mode == "static") {
    return true;
  }
  // Weight-aware modes read this node's own activated leader weight.
  int64_t w = -1;
  if (leader_schedule_ != nullptr) {
    const std::vector<int64_t>& lw = leader_schedule_->ActiveLeaderWeights();
    if (id_ >= 1 && id_ <= static_cast<int>(lw.size())) {
      w = lw[id_ - 1];
    }
  }
  ++silent_leader_slots_;
  if (mode == "probabilistic") {
    return SilentDeterministicPermille(view, id_) <
           PositiveIntFromEnv("TD_HS_SILENT_PROB_PERMILLE", 500);
  }
  if (mode == "burst") {
    const uint64_t on =
        static_cast<uint64_t>(PositiveIntFromEnv("TD_HS_SILENT_BURST_ON", 5));
    const uint64_t off =
        static_cast<uint64_t>(PositiveIntFromEnv("TD_HS_SILENT_BURST_OFF", 5));
    const uint64_t cycle = std::max<uint64_t>(1, on + off);
    return ((silent_leader_slots_ - 1) % cycle) < on;
  }
  if (mode == "degrade") {
    const int step =
        std::max(1, PositiveIntFromEnv("TD_HS_SILENT_DEGRADE_STEP", 8));
    const int p = std::min<int>(
        1000, static_cast<int>(silent_leader_slots_ /
                               static_cast<uint64_t>(step)) *
                  100);
    return SilentDeterministicPermille(view, id_) < p;
  }
  if (mode == "persist") {
    const int64_t stop = PositiveIntFromEnv("TD_HS_SILENT_PERSIST_STOP", 10);
    if (!silent_persist_stopped_ && w >= 0 && w < stop) {
      silent_persist_stopped_ = true;
    }
    return !silent_persist_stopped_;
  }
  if (mode == "relapse") {
    const int64_t high = PositiveIntFromEnv("TD_HS_SILENT_ATTACK_HIGH", 90);
    const int64_t low = PositiveIntFromEnv("TD_HS_SILENT_RETREAT_LOW", 25);
    if (silent_relapse_phase_ == 0 && w >= 0 && w <= low) {
      silent_relapse_phase_ = 1;
    } else if (silent_relapse_phase_ == 1 && w >= 0 && w >= high) {
      silent_relapse_phase_ = 2;
    }
    return silent_relapse_phase_ != 1;  // attack in phases 0,2; honest while recovering
  }
  // adaptive (default) and band: hysteresis between high/low thresholds.
  int64_t high;
  int64_t low;
  if (mode == "band") {
    high = PositiveIntFromEnv("TD_HS_SILENT_BAND_HI", 15);
    low = PositiveIntFromEnv("TD_HS_SILENT_BAND_LO", 11);
  } else {
    high = PositiveIntFromEnv("TD_HS_SILENT_ATTACK_HIGH", 90);
    low = PositiveIntFromEnv("TD_HS_SILENT_RETREAT_LOW", 25);
  }
  if (w < 0) {
    return true;
  }
  if (silent_attacking_ && w <= low) {
    silent_attacking_ = false;
  } else if (!silent_attacking_ && w >= high) {
    silent_attacking_ = true;
  }
  return silent_attacking_;
}

bool HotStuff::IsDoubleProposalForExperiment(int view) const {
  return StrongFaultExperimentStartedAtView("TD_HS_DOUBLE_PROPOSAL_START_VIEW",
                                            view) &&
         (EnvFlagEnabled("TD_HS_DOUBLE_PROPOSAL") ||
          EnvListContainsId("TD_HS_DOUBLE_PROPOSAL_IDS", id_));
}

bool HotStuff::IsDoubleVoteForExperiment(int view) const {
  return StrongFaultExperimentStartedAtView("TD_HS_DOUBLE_VOTE_START_VIEW",
                                            view) &&
         EnvFlagEnabled("TD_HS_DOUBLE_VOTE");
}

bool HotStuff::IsSlowVoteForExperiment(int view) const {
  return ExperimentStartedAtView("TD_HS_SLOW_VOTE_START_VIEW", view) &&
         EnvFlagEnabled("TD_HS_SLOW_VOTE");
}

void HotStuff::MaybeDelayVoteForExperiment(int view) const {
  if (!IsSlowVoteForExperiment(view)) {
    return;
  }
  const int delay_us =
      PositiveIntFromEnv("TD_HS_SLOW_VOTE_DELAY_US", 10000);
  std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
}

// Switchable slow-vote attack model (soft fault: only late/absent votes,
// never an invalid vote). Mirrors IsSilentLeaderForExperiment but the faulty
// voter is weight-aware on its own activated VOTE (overall) weight.
bool HotStuff::SlowVoteAttackForExperiment(int view) const {
  if (!ExperimentStartedAtView("TD_HS_SLOW_VOTE_START_VIEW", view)) {
    return false;
  }
  if (!(EnvFlagEnabled("TD_HS_SLOW_VOTE") ||
        EnvListContainsId("TD_HS_SLOW_VOTE_IDS", id_))) {
    return false;
  }
  const std::string mode = SlowVoteModeFromEnv();
  if (mode == "static") {
    return true;
  }
  int64_t w = -1;
  if (weight_schedule_ != nullptr) {
    const std::vector<int64_t>& vw = weight_schedule_->ActiveWeights();
    if (id_ >= 1 && id_ <= static_cast<int>(vw.size())) {
      w = vw[id_ - 1];
    }
  }
  ++slow_vote_slots_;
  if (mode == "probabilistic") {
    return SilentDeterministicPermille(view, id_) <
           PositiveIntFromEnv("TD_HS_SLOW_VOTE_PROB_PERMILLE", 500);
  }
  if (mode == "burst") {
    const uint64_t on = static_cast<uint64_t>(
        PositiveIntFromEnv("TD_HS_SLOW_VOTE_BURST_ON", 5));
    const uint64_t off = static_cast<uint64_t>(
        PositiveIntFromEnv("TD_HS_SLOW_VOTE_BURST_OFF", 5));
    const uint64_t cycle = std::max<uint64_t>(1, on + off);
    return ((slow_vote_slots_ - 1) % cycle) < on;
  }
  if (mode == "degrade") {
    const int step =
        std::max(1, PositiveIntFromEnv("TD_HS_SLOW_VOTE_DEGRADE_STEP", 8));
    const int p = std::min<int>(
        1000, static_cast<int>(slow_vote_slots_ /
                               static_cast<uint64_t>(step)) *
                  100);
    return SilentDeterministicPermille(view, id_) < p;
  }
  if (mode == "persist") {
    const int64_t stop = PositiveIntFromEnv("TD_HS_SLOW_VOTE_PERSIST_STOP", 40);
    if (!slow_vote_persist_stopped_ && w >= 0 && w < stop) {
      slow_vote_persist_stopped_ = true;
    }
    return !slow_vote_persist_stopped_;
  }
  if (mode == "relapse") {
    const int64_t high = PositiveIntFromEnv("TD_HS_SLOW_VOTE_ATTACK_HIGH", 60);
    const int64_t low = PositiveIntFromEnv("TD_HS_SLOW_VOTE_RETREAT_LOW", 30);
    if (slow_vote_relapse_phase_ == 0 && w >= 0 && w <= low) {
      slow_vote_relapse_phase_ = 1;
    } else if (slow_vote_relapse_phase_ == 1 && w >= 0 && w >= high) {
      slow_vote_relapse_phase_ = 2;
    }
    return slow_vote_relapse_phase_ != 1;
  }
  int64_t high;
  int64_t low;
  if (mode == "band") {
    high = PositiveIntFromEnv("TD_HS_SLOW_VOTE_BAND_HI", 55);
    low = PositiveIntFromEnv("TD_HS_SLOW_VOTE_BAND_LO", 45);
  } else {
    high = PositiveIntFromEnv("TD_HS_SLOW_VOTE_ATTACK_HIGH", 60);
    low = PositiveIntFromEnv("TD_HS_SLOW_VOTE_RETREAT_LOW", 30);
  }
  if (w < 0) {
    return true;
  }
  if (slow_vote_attacking_ && w <= low) {
    slow_vote_attacking_ = false;
  } else if (!slow_vote_attacking_ && w >= high) {
    slow_vote_attacking_ = true;
  }
  return slow_vote_attacking_;
}

bool HotStuff::ShouldUsePeerTrustCliqueForView(int view) const {
  if (!EnvFlagEnabled("TD_HS_PEERTRUST_CLIQUE") ||
      !ExperimentStartedAtView("TD_HS_PEERTRUST_CLIQUE_START_VIEW", view)) {
    return false;
  }
  const int leader = leader_schedule_ != nullptr
                         ? leader_schedule_->LeaderForView(view)
                         : DefaultLeaderForView(view, total_num_);
  return leader > 0 &&
         EnvListContainsId("TD_HS_PEERTRUST_CLIQUE_TARGET_IDS", leader);
}

bool HotStuff::IsInvalidQcForExperiment(int view) const {
  return StrongFaultExperimentStartedAtView("TD_HS_INVALID_QC_START_VIEW",
                                            view) &&
         EnvFlagEnabled("TD_HS_INVALID_QC");
}

std::unique_ptr<Proposal> HotStuff::MakeConflictingProposalForExperiment(
    const Proposal& proposal) {
  if (verifier_ == nullptr) {
    return nullptr;
  }
  std::unique_ptr<Proposal> conflicting = std::make_unique<Proposal>(proposal);
  conflicting->mutable_header()->set_proposal_id(
      proposal.header().proposal_id() + 1000000 + id_);
  conflicting->clear_signature();
  conflicting->set_hash(ProposalHashForExperiment(*conflicting));
  auto signature_or =
      verifier_->SignMessage(ProposalSignaturePayload(*conflicting));
  if (!signature_or.ok()) {
    LOG(ERROR) << "failed to sign TD-Hotstuff conflicting proposal view:"
               << conflicting->header().view();
    return nullptr;
  }
  *conflicting->mutable_signature() = *signature_or;
  return conflicting;
}

std::unique_ptr<Proposal> HotStuff::MakeInvalidQcProposalForExperiment(
    const Proposal& proposal) {
  if (verifier_ == nullptr || proposal.header().qc().hash().empty() ||
      proposal.header().qc().signer_bitmap().empty()) {
    return nullptr;
  }
  std::unique_ptr<Proposal> invalid = std::make_unique<Proposal>(proposal);
  std::string forged_bitmap = invalid->header().qc().signer_bitmap();
  forged_bitmap[0] = static_cast<char>(forged_bitmap[0] ^ 0x01);
  invalid->mutable_header()->mutable_qc()->set_signer_bitmap(forged_bitmap);
  invalid->clear_signature();
  invalid->set_hash(ProposalHashForExperiment(*invalid));
  auto signature_or = verifier_->SignMessage(ProposalSignaturePayload(*invalid));
  if (!signature_or.ok()) {
    LOG(ERROR) << "failed to sign TD-Hotstuff invalid-QC proposal view:"
               << invalid->header().view();
    return nullptr;
  }
  *invalid->mutable_signature() = *signature_or;
  return invalid;
}

std::unique_ptr<Certificate> HotStuff::MakeConflictingCertificateForExperiment(
    const Certificate& cert) {
  if (verifier_ == nullptr || cert.hash().empty()) {
    return nullptr;
  }
  std::unique_ptr<Certificate> conflicting =
      std::make_unique<Certificate>(cert);
  const std::string conflict_material =
      cert.hash() + "|td_hotstuff_double_vote|" + std::to_string(id_) + "|" +
      std::to_string(cert.view()) + "|" + std::to_string(cert.slot());
  conflicting->set_hash(SignatureVerifier::CalculateHash(conflict_material));
  conflicting->clear_sign();
  auto signature_or =
      verifier_->SignMessage(VoteSignaturePayload(*conflicting));
  if (!signature_or.ok()) {
    LOG(ERROR) << "failed to sign TD-Hotstuff conflicting vote view:"
               << conflicting->view();
    return nullptr;
  }
  *conflicting->mutable_sign() = *signature_or;
  return conflicting;
}

void HotStuff::MarkTimeoutProgressLocked() {
  ++timeout_progress_epoch_;
  timeout_progress_epoch_atomic_.store(timeout_progress_epoch_,
                                       std::memory_order_release);
}

bool HotStuff::Ready() {
  return proposal_manager_ != nullptr && IsLeader(proposal_manager_->CurrentView()) &&
         !has_sent_;
}

void HotStuff::StartNewRound() {
  std::unique_lock<std::mutex> lk(n_mutex_);
  has_sent_ = false;
  vote_cv_.notify_one();
}

void HotStuff::AsyncTimeout() {
  int watched_view = 0;
  auto view_started_at = std::chrono::steady_clock::now();
  auto last_vote_at = std::chrono::steady_clock::time_point::min();
  const auto timeout_interval =
      std::chrono::milliseconds(timeout_config_.timeout_ms);
  const auto poll_interval = std::chrono::milliseconds(
      std::max(1, std::min(timeout_config_.timeout_ms, 50)));
  const auto startup_grace_interval =
      std::chrono::milliseconds(std::max(timeout_config_.timeout_ms * 2, 1500));
  auto public_keys_ready_at = std::chrono::steady_clock::time_point::min();
  uint64_t observed_progress_epoch = 0;

  while (!IsStop() && !stop_timeout_.load()) {
    std::this_thread::sleep_for(poll_interval);
    if (IsStop() || stop_timeout_.load() || timeout_manager_ == nullptr) {
      continue;
    }
    const auto now = std::chrono::steady_clock::now();
    if (verifier_ != nullptr &&
        verifier_->GetPublicKeysSize() < static_cast<size_t>(total_num_)) {
      watched_view = 0;
      view_started_at = now;
      last_vote_at = std::chrono::steady_clock::time_point::min();
      public_keys_ready_at = std::chrono::steady_clock::time_point::min();
      continue;
    }
    if (public_keys_ready_at == std::chrono::steady_clock::time_point::min()) {
      public_keys_ready_at = now;
      watched_view = 0;
      view_started_at = now;
      last_vote_at = std::chrono::steady_clock::time_point::min();
      continue;
    }
    if (now - public_keys_ready_at < startup_grace_interval) {
      watched_view = 0;
      view_started_at = now;
      last_vote_at = std::chrono::steady_clock::time_point::min();
      continue;
    }

    const uint64_t progress_epoch =
        timeout_progress_epoch_atomic_.load(std::memory_order_acquire);
    if (progress_epoch != observed_progress_epoch) {
      observed_progress_epoch = progress_epoch;
      watched_view = 0;
      view_started_at = now;
      last_vote_at = std::chrono::steady_clock::time_point::min();
      continue;
    }
    if (now - view_started_at < timeout_interval) {
      continue;
    }

    TimeoutVote vote;
    TimeoutCert cert;
    bool has_vote = false;
    bool has_cert = false;
    bool advanced = false;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      if (timeout_progress_epoch_ != observed_progress_epoch) {
        observed_progress_epoch = timeout_progress_epoch_;
        timeout_progress_epoch_atomic_.store(timeout_progress_epoch_,
                                             std::memory_order_release);
        watched_view = 0;
        view_started_at = now;
        last_vote_at = std::chrono::steady_clock::time_point::min();
        continue;
      }
      const int local_view = proposal_manager_->CurrentView();
      const int view = std::max(local_view, last_valid_proposal_view_ + 1);
      const QC& high_qc = proposal_manager_->HighQC();
      if (!high_qc.hash().empty() && high_qc.view() >= view) {
        watched_view = 0;
        view_started_at = now;
        last_vote_at = std::chrono::steady_clock::time_point::min();
        continue;
      }
      if (!ShouldTimeoutView(view, last_valid_proposal_view_)) {
        watched_view = 0;
        view_started_at = now;
        last_vote_at = std::chrono::steady_clock::time_point::min();
        continue;
      }
      if (view != watched_view) {
        watched_view = view;
        last_vote_at = std::chrono::steady_clock::time_point::min();
      }
      if (last_vote_at != std::chrono::steady_clock::time_point::min() &&
          now - last_vote_at < timeout_interval) {
        continue;
      }
      std::unique_ptr<TimeoutVote> local_vote =
          timeout_manager_->CreateTimeoutVote(view, high_qc);
      if (local_vote == nullptr) {
        continue;
      }
      last_vote_at = now;
      vote = *local_vote;
      has_vote = true;
      std::unique_ptr<TimeoutCert> formed_cert =
          timeout_manager_->AddVote(*local_vote);
      if (formed_cert != nullptr) {
        cert = *formed_cert;
        has_cert = true;
        advanced = ApplyTimeoutCertLocked(cert);
        if (advanced) {
          watched_view = proposal_manager_->CurrentView();
          view_started_at = now;
          last_vote_at = std::chrono::steady_clock::time_point::min();
        }
      }
    }
    if (has_vote) {
      BroadcastTimeoutVote(vote);
    }
    if (has_cert) {
      BroadcastTimeoutCert(cert);
    }
    if (advanced) {
      StartNewRound();
    }
  }
}

void HotStuff::AsyncSend() {
  while (!IsStop()) {
    if (weight_update_pipeline_ != nullptr) {
      weight_update_pipeline_->MaybeActivateReadyAfterViewAdvance();
    }
    {
      std::unique_lock<std::mutex> lk(n_mutex_);
      vote_cv_.wait_for(lk, std::chrono::microseconds(1000),
                        [&] { return Ready(); });
    }
    if (IsStop()) {
      return;
    }
    if (weight_update_pipeline_ != nullptr) {
      weight_update_pipeline_->MaybeActivateReadyAfterViewAdvance();
    }
    if (!Ready()) {
      continue;
    }

    std::unique_ptr<Proposal> proposal;
    std::unique_ptr<Proposal> conflicting_proposal;
    bool silent_leader = false;
    bool no_transactions_ready = false;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      const int view = proposal_manager_->CurrentView();
      if (weight_update_pipeline_ != nullptr) {
        weight_update_pipeline_->ActivateReady(view);
      }
      const bool is_leader = IsLeader(view);
      if (is_leader && !has_sent_ && IsSilentLeaderForExperiment(view)) {
        silent_leader = true;
        has_sent_ = true;
      } else if (is_leader && !has_sent_) {
        std::vector<std::unique_ptr<Transaction>> txns =
            TakeTransactionsForView(view, batch_size_);
        if (txns.empty() &&
            (timeout_manager_ == nullptr ||
             view > timeout_empty_proposal_until_view_)) {
          no_transactions_ready = true;
        } else {
          std::unique_ptr<WeightUpdateCert> weight_update_cert;
          if (weight_update_pipeline_ != nullptr) {
            weight_update_cert =
                weight_update_pipeline_->LatestCertForProposal(view);
          }
          proposal =
              proposal_manager_->GenerateProposal(txns, weight_update_cert.get());
          if (proposal != nullptr) {
            last_valid_proposal_view_ =
                std::max(last_valid_proposal_view_, proposal->header().view());
          }
          has_sent_ = true;
          MarkTimeoutProgressLocked();
          if (proposal != nullptr && IsInvalidQcForExperiment(view)) {
            std::unique_ptr<Proposal> invalid_qc_proposal =
                MakeInvalidQcProposalForExperiment(*proposal);
            if (invalid_qc_proposal != nullptr) {
              proposal = std::move(invalid_qc_proposal);
            }
          }
          if (proposal != nullptr && IsDoubleProposalForExperiment(view)) {
            conflicting_proposal =
                MakeConflictingProposalForExperiment(*proposal);
          }
        }
      }
    }

    if (proposal != nullptr) {
      if (weight_update_pipeline_ != nullptr) {
        weight_update_pipeline_->MaybeActivateReadyAfterViewAdvance();
      }
      const int proposal_view = proposal->header().view();
      const int active_leader = LeaderForView(proposal_view);
      if (proposal->sender() != active_leader) {
        LOG_EVERY_N(WARNING, 1000)
            << "drop locally generated proposal after leader profile activation, view:"
            << proposal_view << " sender:" << proposal->sender()
            << " active_leader:" << active_leader;
        continue;
      }
      broadcast_call_(MessageType::NewProposal, *proposal);
      if (conflicting_proposal != nullptr) {
        broadcast_call_(MessageType::NewProposal, *conflicting_proposal);
      }
      continue;
    }
    if (silent_leader || no_transactions_ready) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
}

void HotStuff::AsyncCommit() {
  int seq = 1;
  while (!IsStop()) {
    auto p = commit_q_.Pop();
    if (p == nullptr) {
      continue;
    }
    global_stats_->AddConsensusLatency(GetCurrentTime() - p->createtime());
    for (Transaction& txn : *p->mutable_transactions()) {
      MarkTransactionCommitted(txn);
      txn.set_id(seq++);
      Commit(txn);
    }
  }
}

void HotStuff::AsyncVerifiedEvents() {
  while (true) {
    std::vector<VerifiedConsensusEvent> events;
    if (async_verifier_ != nullptr) {
      events = async_verifier_->WaitForVerified(std::chrono::milliseconds(100));
    }
    for (VerifiedConsensusEvent& event : events) {
      ProcessVerifiedConsensusEvent(std::move(event));
    }
    if ((IsStop() || stop_verified_events_.load()) && events.empty()) {
      break;
    }
    if (async_verifier_ == nullptr) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
}

void HotStuff::AsyncWeightUpdates() {
  while (!IsStop() && !stop_weight_updates_.load()) {
    if (reputation_adapter_ != nullptr) {
      reputation_adapter_->AdvanceWatermark(CurrentView());
    }
    if (weight_update_pipeline_ != nullptr) {
      weight_update_pipeline_->DrainCompletedCandidates();
      weight_update_pipeline_->ActivateReady();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (weight_update_pipeline_ != nullptr) {
    weight_update_pipeline_->DrainCompletedCandidates();
    weight_update_pipeline_->ActivateReady();
  }
}

void HotStuff::ProcessVerifiedConsensusEvent(VerifiedConsensusEvent event) {
  switch (event.type) {
    case VerifiedConsensusEvent::Type::kVote:
      ProcessVerifiedCertificate(std::move(event.vote));
      return;
    case VerifiedConsensusEvent::Type::kTimeoutVote:
      ReceiveTimeoutVote(std::move(event.timeout_vote));
      return;
    case VerifiedConsensusEvent::Type::kTimeoutCert:
      ReceiveTimeoutCert(std::move(event.timeout_cert));
      return;
    case VerifiedConsensusEvent::Type::kNone:
      return;
  }
}

bool HotStuff::ReceiveTransaction(std::unique_ptr<Transaction> txn) {
  return ReceiveTransactionForView(std::move(txn), CurrentView());
}

bool HotStuff::ReceiveTransactionForView(std::unique_ptr<Transaction> txn,
                                         int target_view) {
  if (txn == nullptr) {
    return false;
  }
  const int current_view = CurrentView();
  if (target_view <= 0) {
    target_view = current_view;
  }
  txn->set_reception_time(GetCurrentTime());
  txn->set_proposer(id_);
  const std::string key = TransactionDedupKey(*txn);
  {
    std::unique_lock<std::mutex> lk(txn_buffer_mutex_);
    if (committed_txn_keys_.find(key) != committed_txn_keys_.end() ||
        queued_txn_keys_.find(key) != queued_txn_keys_.end()) {
      return true;
    }
    queued_txn_keys_.insert(key);
    pending_txns_by_view_[target_view].push_back(std::move(txn));
  }
  vote_cv_.notify_one();
  return true;
}

std::vector<std::unique_ptr<Transaction>> HotStuff::TakeTransactionsForView(
    int view, int max_count) {
  std::vector<std::unique_ptr<Transaction>> txns;
  if (max_count <= 0) {
    return txns;
  }
  std::unique_lock<std::mutex> lk(txn_buffer_mutex_);
  for (auto it = pending_txns_by_view_.begin();
       it != pending_txns_by_view_.end() && it->first <= view &&
       static_cast<int>(txns.size()) < max_count;) {
    auto& pending = it->second;
    while (!pending.empty() && static_cast<int>(txns.size()) < max_count) {
      std::unique_ptr<Transaction> txn = std::move(pending.front());
      pending.pop_front();
      if (txn == nullptr) {
        continue;
      }
      const std::string key = TransactionDedupKey(*txn);
      if (committed_txn_keys_.find(key) != committed_txn_keys_.end()) {
        queued_txn_keys_.erase(key);
        continue;
      }
      txns.push_back(std::move(txn));
    }
    if (pending.empty()) {
      it = pending_txns_by_view_.erase(it);
    } else {
      ++it;
    }
  }
  return txns;
}

void HotStuff::MarkTransactionCommitted(const Transaction& txn) {
  const std::string key = TransactionDedupKey(txn);
  std::unique_lock<std::mutex> lk(txn_buffer_mutex_);
  committed_txn_keys_.insert(key);
  queued_txn_keys_.erase(key);
}

int HotStuff::CurrentView() {
  std::unique_lock<std::mutex> lk(mutex_);
  return proposal_manager_ != nullptr ? proposal_manager_->CurrentView() : 0;
}

int HotStuff::CurrentLeader() {
  std::unique_lock<std::mutex> lk(mutex_);
  if (proposal_manager_ == nullptr) {
    return 0;
  }
  return LeaderForView(proposal_manager_->CurrentView());
}

int64_t HotStuff::WeightForSigner(int signer, int view) const {
  if (weight_schedule_ != nullptr) {
    return weight_schedule_->WeightForSigner(signer, view);
  }
  if (signer < 1 || signer > static_cast<int>(replica_weights_.size())) {
    return 0;
  }
  return replica_weights_[signer - 1];
}

int64_t HotStuff::CertificateWeight(
    const std::map<int, std::unique_ptr<Certificate>>& certs, int view) const {
  int64_t total_weight = 0;
  for (const auto& entry : certs) {
    total_weight += WeightForSigner(entry.first, view);
  }
  return total_weight;
}

std::vector<int> HotStuff::CertificateSigners(
    const std::map<int, std::unique_ptr<Certificate>>& certs, int view) const {
  std::vector<int> available_signers;
  available_signers.reserve(certs.size());
  for (const auto& entry : certs) {
    if (WeightForSigner(entry.first, view) > 0) {
      available_signers.push_back(entry.first);
    }
  }
  if (available_signers.empty()) {
    return {};
  }

  const int64_t quorum_weight =
      weight_schedule_ != nullptr ? weight_schedule_->QuorumWeightForView(view)
                                  : quorum_weight_;

  // std::map iteration always starts from the smallest validator id. Rotate the
  // timely signer set by view so all honest voters get comparable QC inclusion
  // opportunities without waiting for late votes.
  const int view_index = std::max(view - 1, 0);
  const size_t start =
      static_cast<size_t>(view_index) % available_signers.size();

  std::vector<int> signers;
  signers.reserve(available_signers.size());
  int64_t selected_weight = 0;
  for (size_t offset = 0; offset < available_signers.size(); ++offset) {
    const int signer = available_signers[(start + offset) % available_signers.size()];
    const int64_t weight = WeightForSigner(signer, view);
    if (weight <= 0) {
      continue;
    }
    signers.push_back(signer);
    selected_weight += weight;
    if (selected_weight >= quorum_weight) {
      break;
    }
  }
  return signers;
}

std::vector<int> HotStuff::SelectPeerTrustCliqueSigners(
    const std::map<int, std::unique_ptr<Certificate>>& certs, int view,
    int64_t quorum_weight) const {
  std::vector<int> reviewers =
      IntListFromEnv("TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS");
  std::set<int> seen;
  std::vector<int> selected_signers;
  int64_t selected_weight = 0;
  for (int reviewer : reviewers) {
    if (reviewer < 1 || reviewer > total_num_ || !seen.insert(reviewer).second) {
      continue;
    }
    auto it = certs.find(reviewer);
    if (it == certs.end()) {
      continue;
    }
    const int64_t weight = WeightForSigner(reviewer, view);
    if (weight <= 0) {
      continue;
    }
    selected_signers.push_back(reviewer);
    selected_weight += weight;
    if (selected_weight >= quorum_weight) {
      return selected_signers;
    }
  }
  return {};
}

bool HotStuff::MaybeMakeSignedProposalEvidenceSnapshotLocked(
    const Proposal& proposal,
    TdHotstuffSignedProposalEvidenceSnapshot* snapshot) {
  if (snapshot == nullptr || reputation_adapter_ == nullptr ||
      !reputation_adapter_->WantsSignedProposalEvidence() ||
      proposal.hash().empty() || weight_schedule_ == nullptr) {
    return false;
  }
  snapshot->local_node_id = id_;
  snapshot->total_replicas = total_num_;
  snapshot->view = proposal.header().view();
  snapshot->slot = proposal.header().slot();
  snapshot->leader_id = proposal.sender();
  snapshot->proposal_hash = proposal.hash();
  snapshot->signature_verified = true;
  snapshot->active_weight_root =
      weight_schedule_->WeightRootForView(snapshot->view);
  snapshot->active_weight_version =
      weight_schedule_->WeightVersionForView(snapshot->view);
  return true;
}

bool HotStuff::MaybeMakeSignedVoteEvidenceSnapshotLocked(
    const Certificate& cert, TdHotstuffSignedVoteEvidenceSnapshot* snapshot) {
  if (snapshot == nullptr || reputation_adapter_ == nullptr ||
      !reputation_adapter_->WantsSignedVoteEvidence() || cert.hash().empty() ||
      weight_schedule_ == nullptr) {
    return false;
  }
  snapshot->local_node_id = id_;
  snapshot->total_replicas = total_num_;
  snapshot->view = cert.view();
  snapshot->slot = cert.slot();
  snapshot->signer_id = cert.signer();
  snapshot->proposal_hash = cert.hash();
  snapshot->signature_verified = true;
  snapshot->active_weight_root =
      weight_schedule_->WeightRootForView(snapshot->view);
  snapshot->active_weight_version =
      weight_schedule_->WeightVersionForView(snapshot->view);
  return true;
}

bool HotStuff::MaybeMakeInvalidQcProposalEvidenceSnapshotLocked(
    const Proposal& proposal, const ProposalValidationResult& validation,
    TdHotstuffInvalidQcProposalEvidenceSnapshot* snapshot) {
  if (snapshot == nullptr || reputation_adapter_ == nullptr ||
      !reputation_adapter_->WantsInvalidQcProposalEvidence() ||
      weight_schedule_ == nullptr || proposal.hash().empty() ||
      validation.valid ||
      validation.error_code != ProposalValidationErrorCode::kInvalidQc ||
      !validation.proposal_hash_verified ||
      !validation.proposal_signature_verified || !validation.leader_verified ||
      !validation.leader_context_verified || !validation.qc_present ||
      validation.qc_verified) {
    return false;
  }
  snapshot->local_node_id = id_;
  snapshot->total_replicas = total_num_;
  snapshot->view = proposal.header().view();
  snapshot->slot = proposal.header().slot();
  snapshot->leader_id = proposal.sender();
  snapshot->proposal_hash = proposal.hash();
  snapshot->proposal_signature_verified = true;
  snapshot->qc_verified = false;
  snapshot->invalid_reason = validation.error_message.empty()
                                 ? "invalid_qc"
                                 : validation.error_message;
  snapshot->active_weight_root =
      weight_schedule_->WeightRootForView(snapshot->view);
  snapshot->active_weight_version =
      weight_schedule_->WeightVersionForView(snapshot->view);
  return true;
}

bool HotStuff::MaybeMakeQcEvidenceSnapshotLocked(
    const QC& qc, TdHotstuffQcEvidenceSnapshot* snapshot) {
  if (snapshot == nullptr || reputation_adapter_ == nullptr ||
      qc.hash().empty() || weight_schedule_ == nullptr) {
    return false;
  }
  snapshot->local_node_id = id_;
  snapshot->total_replicas = total_num_;
  snapshot->view = qc.view();
  snapshot->slot = qc.slot();
  snapshot->leader_id = LeaderForView(qc.view());
  snapshot->qc_hash = qc.hash();
  snapshot->signer_bitmap = qc.signer_bitmap();
  snapshot->available_signer_bitmap =
      qc.available_signer_bitmap().empty() ? qc.signer_bitmap()
                                           : qc.available_signer_bitmap();
  snapshot->active_weight_root = weight_schedule_->WeightRootForView(qc.view());
  snapshot->active_weight_version =
      weight_schedule_->WeightVersionForView(qc.view());
  return true;
}

bool HotStuff::MaybeFormQcLocked(int view, const std::string& hash) {
  return MaybeFormQcLocked(view, hash, nullptr);
}

bool HotStuff::MaybeFormQcLocked(
    int view, const std::string& hash,
    TdHotstuffQcEvidenceSnapshot* reputation_snapshot) {
  auto view_it = receive_.find(view);
  if (view_it == receive_.end()) {
    return false;
  }
  auto hash_it = view_it->second.find(hash);
  if (hash_it == view_it->second.end()) {
    return false;
  }
  auto& certs = hash_it->second;
  const int64_t quorum_weight =
      weight_schedule_ != nullptr ? weight_schedule_->QuorumWeightForView(view)
                                  : quorum_weight_;
  const int64_t cert_weight = CertificateWeight(certs, view);
  if (cert_weight < quorum_weight) {
    return false;
  }

  std::vector<int> selected_signers = CertificateSigners(certs, view);
  if (selected_signers.empty()) {
    return false;
  }

  std::vector<int> available_signers;
  available_signers.reserve(certs.size());
  for (const auto& entry : certs) {
    if (WeightForSigner(entry.first, view) > 0) {
      available_signers.push_back(entry.first);
    }
  }
  if (available_signers.empty()) {
    available_signers = selected_signers;
  }
  if (ShouldUsePeerTrustCliqueForView(view)) {
    const std::vector<int> peertrust_signers =
        SelectPeerTrustCliqueSigners(certs, view, quorum_weight);
    if (!peertrust_signers.empty()) {
      selected_signers = peertrust_signers;
    }
    available_signers.clear();
    for (int signer = 1; signer <= total_num_; ++signer) {
      if (WeightForSigner(signer, view) > 0) {
        available_signers.push_back(signer);
      }
    }
    if (EnvFlagEnabled("TD_HS_PEERTRUST_QC_TRACE")) {
      LOG(ERROR) << "[PeerTrustCliqueEvidence] view=" << view
                 << " selected_count=" << selected_signers.size()
                 << " available_count=" << available_signers.size()
                 << " clique_selected=" << (!peertrust_signers.empty())
                 << " shared_qc_metadata=public_available_set";
    }
  }

  std::unique_ptr<QC> qc = std::make_unique<QC>();
  qc->set_hash(hash);
  qc->set_view(view);
  qc->set_signer_bitmap(BuildSignerBitmap(selected_signers, total_num_));
  qc->set_available_signer_bitmap(BuildSignerBitmap(available_signers,
                                                    total_num_));
  qc->set_collector_id(id_);
  for (int signer : selected_signers) {
    auto it = certs.find(signer);
    if (it != certs.end()) {
      if (qc->slot() == 0) {
        qc->set_slot(it->second->slot());
      }
      *qc->add_signatures() = it->second->sign();
    }
  }

  if (reputation_snapshot != nullptr) {
    reputation_snapshot->local_node_id = id_;
    reputation_snapshot->total_replicas = total_num_;
    reputation_snapshot->view = view;
    reputation_snapshot->slot = qc->slot();
    reputation_snapshot->leader_id = LeaderForView(view);
    reputation_snapshot->qc_hash = hash;
    reputation_snapshot->signer_bitmap = qc->signer_bitmap();
    reputation_snapshot->available_signer_bitmap =
        qc->available_signer_bitmap();
    reputation_snapshot->active_weight_root =
        weight_schedule_->WeightRootForView(view);
    reputation_snapshot->active_weight_version =
        weight_schedule_->WeightVersionForView(view);
  }

  qc_formed_ = true;
  if (proposal_received_) {
    proposal_manager_->AddQC(std::move(qc));
    StartNewRound();
    qc_formed_ = proposal_received_ = false;
    return true;
  } else {
    formed_qc_ = std::move(qc);
    return false;
  }
}

bool HotStuff::ReceiveCertificate(std::unique_ptr<Certificate> cert) {
  if (cert == nullptr) {
    return false;
  }
  const bool submitted_async =
      async_verifier_ != nullptr && async_verifier_->TrySubmitVote(&cert);
  if (submitted_async) {
    return true;
  }
  {
    std::unique_lock<std::mutex> lk(mutex_);
    if (cert == nullptr || !proposal_manager_->VerifyCert(*cert)) {
      return false;
    }
  }
  return ProcessVerifiedCertificate(std::move(cert));
}

bool HotStuff::ProcessVerifiedCertificate(std::unique_ptr<Certificate> cert) {
  if (cert == nullptr) {
    return false;
  }
  TdHotstuffSignedVoteEvidenceSnapshot signed_vote_snapshot;
  bool has_signed_vote_snapshot = false;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    has_signed_vote_snapshot = MaybeMakeSignedVoteEvidenceSnapshotLocked(
        *cert, &signed_vote_snapshot);
    const int view = cert->view();
    const int local_current_view = proposal_manager_->CurrentView();
    const std::string hash = cert->hash();
    if (view >= local_current_view) {
      auto& certs = receive_[view][hash];
      const int64_t previous_weight = CertificateWeight(certs, view);
      certs.insert(std::make_pair(cert->signer(), std::move(cert)));
      const int64_t current_weight = CertificateWeight(certs, view);
      const int64_t quorum_weight =
          weight_schedule_ != nullptr ? weight_schedule_->QuorumWeightForView(view)
                                      : quorum_weight_;
      const bool crossed_quorum =
          previous_weight < quorum_weight && current_weight >= quorum_weight;
      if (view >= local_current_view && crossed_quorum) {
        if (MaybeFormQcLocked(view, hash)) {
          MarkTimeoutProgressLocked();
        }
      }
    }
  }
  if (has_signed_vote_snapshot && reputation_adapter_ != nullptr) {
    reputation_adapter_->TryRecordSignedVote(std::move(signed_vote_snapshot));
  }
  return true;
}

bool HotStuff::ReceiveTimeoutVote(std::unique_ptr<TimeoutVote> vote) {
  if (vote == nullptr || timeout_manager_ == nullptr ||
      !timeout_config_.enabled) {
    return false;
  }
  TimeoutVote local_vote;
  TimeoutCert cert;
  bool has_local_vote = false;
  bool formed_cert = false;
  bool advanced = false;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    const int current_view = proposal_manager_->CurrentView();
    if (vote->view() < current_view) {
      return true;
    }
    std::unique_ptr<TimeoutCert> maybe_cert = timeout_manager_->AddVote(*vote);
    if (maybe_cert != nullptr) {
      cert = *maybe_cert;
      formed_cert = true;
      advanced = ApplyTimeoutCertLocked(cert);
    }

    if (vote->signer() != id_ && vote->view() >= current_view &&
        timeout_echoed_views_.insert(vote->view()).second) {
      std::unique_ptr<TimeoutVote> maybe_local_vote =
          timeout_manager_->CreateTimeoutVote(vote->view(),
                                              proposal_manager_->HighQC());
      if (maybe_local_vote != nullptr) {
        local_vote = *maybe_local_vote;
        has_local_vote = true;
        std::unique_ptr<TimeoutCert> local_cert =
            timeout_manager_->AddVote(*maybe_local_vote);
        if (local_cert != nullptr && !formed_cert) {
          cert = *local_cert;
          formed_cert = true;
          advanced = ApplyTimeoutCertLocked(cert);
        }
      }
    }
  }
  if (has_local_vote) {
    BroadcastTimeoutVote(local_vote);
  }
  if (formed_cert) {
    BroadcastTimeoutCert(cert);
  }
  if (advanced) {
    StartNewRound();
  }
  return true;
}

bool HotStuff::ReceiveTimeoutCert(std::unique_ptr<TimeoutCert> cert) {
  if (cert == nullptr || timeout_manager_ == nullptr ||
      !timeout_config_.enabled) {
    return false;
  }
  bool advanced = false;
  {
    std::unique_lock<std::mutex> lk(mutex_);
    advanced = ApplyTimeoutCertLocked(*cert);
  }
  if (advanced) {
    StartNewRound();
  }
  return advanced;
}

void HotStuff::BroadcastTimeoutVote(const TimeoutVote& vote) {
  if (broadcast_call_ != nullptr) {
    broadcast_call_(MessageType::TimeoutVoteMsg, vote);
  }
}

void HotStuff::BroadcastTimeoutCert(const TimeoutCert& cert) {
  if (broadcast_call_ != nullptr) {
    broadcast_call_(MessageType::TimeoutCertMsg, cert);
  }
}

bool HotStuff::ReceiveCandidateWeightUpdate(
    std::unique_ptr<CandidateWeightUpdate> candidate) {
  return weight_update_pipeline_ != nullptr &&
         weight_update_pipeline_->ReceiveCandidate(std::move(candidate));
}

bool HotStuff::ReceiveWeightUpdateVote(std::unique_ptr<WeightUpdateVote> vote) {
  return weight_update_pipeline_ != nullptr &&
         weight_update_pipeline_->ReceiveVote(std::move(vote));
}

bool HotStuff::ReceiveWeightUpdateCert(std::unique_ptr<WeightUpdateCert> cert) {
  return weight_update_pipeline_ != nullptr &&
         weight_update_pipeline_->ReceiveCert(std::move(cert));
}

bool HotStuff::ApplyTimeoutCertLocked(const TimeoutCert& cert) {
  if (timeout_manager_ == nullptr ||
      !timeout_manager_->VerifyTimeoutCert(cert)) {
    return false;
  }
  if (!proposal_manager_->AdvanceToViewByTimeout(cert)) {
    return false;
  }
  qc_formed_ = false;
  proposal_received_ = false;
  formed_qc_.reset();
  has_sent_ = false;
  MarkTimeoutProgressLocked();
  timeout_empty_proposal_until_view_ =
      proposal_manager_->CurrentView() + timeout_empty_proposal_budget_views_;
  timeout_manager_->ResetBelowView(proposal_manager_->CurrentView());
  for (auto it = timeout_echoed_views_.begin();
       it != timeout_echoed_views_.end();) {
    if (*it < proposal_manager_->CurrentView()) {
      it = timeout_echoed_views_.erase(it);
    } else {
      ++it;
    }
  }
  LOG_EVERY_N(INFO, 1000) << "TD-Hotstuff timeout advanced to view:"
                          << proposal_manager_->CurrentView()
                          << " by tc view:" << cert.view();
  return true;
}

bool HotStuff::ReceiveProposal(std::unique_ptr<Proposal> proposal) {
  if (proposal == nullptr) {
    return false;
  }
  const int view = proposal->header().view();
  std::unique_ptr<Certificate> cert;
  std::unique_ptr<Certificate> conflicting_cert;
  TdHotstuffQcEvidenceSnapshot proposal_qc_snapshot;
  TdHotstuffSignedProposalEvidenceSnapshot signed_proposal_snapshot;
  TdHotstuffInvalidQcProposalEvidenceSnapshot invalid_qc_snapshot;
  bool has_proposal_qc_snapshot = false;
  bool has_signed_proposal_snapshot = false;
  bool has_invalid_qc_snapshot = false;
  bool proposal_valid = true;
  bool stale_proposal = false;
  int next_leader = 0;
  {
    std::unique_lock<std::mutex> lk(mutex_);

    if (id_ == NextLeader(view)) {
      proposal_received_ = true;
    }
    if (proposal->header().has_timeout_cert()) {
      ApplyTimeoutCertLocked(proposal->header().timeout_cert());
    }
    if (proposal->header().has_weight_update_cert() &&
        weight_update_pipeline_ != nullptr) {
      weight_update_pipeline_->ProcessCertFromProposal(
          proposal->header().weight_update_cert(), view);
    }
    const int local_current_view = proposal_manager_->CurrentView();
    if (view < local_current_view) {
      stale_proposal = true;
      proposal_valid = false;
    } else {
      const ProposalValidationResult validation =
          proposal_manager_->ValidateProposal(*proposal);
      if (!validation.valid) {
        LOG(ERROR) << "proposal invalid";
        has_invalid_qc_snapshot = MaybeMakeInvalidQcProposalEvidenceSnapshotLocked(
            *proposal, validation, &invalid_qc_snapshot);
        proposal_valid = false;
      }
    }

    if (proposal_valid) {
      if (reputation_adapter_ != nullptr &&
          reputation_adapter_->WantsSignedProposalEvidence()) {
        has_signed_proposal_snapshot =
            MaybeMakeSignedProposalEvidenceSnapshotLocked(
                *proposal, &signed_proposal_snapshot);
      }
      if (reputation_adapter_ != nullptr &&
          !proposal->header().qc().hash().empty()) {
        has_proposal_qc_snapshot = MaybeMakeQcEvidenceSnapshotLocked(
            proposal->header().qc(), &proposal_qc_snapshot);
      }
      std::string safety_error;
      const bool recorded_vote =
          proposal_manager_->RecordVote(*proposal, &safety_error);
      if (!recorded_vote) {
        LOG_EVERY_N(WARNING, 1000) << "proposal vote safety rejected: " << safety_error;
        proposal_valid = false;
      }
    }

    if (proposal_valid) {
      last_valid_proposal_view_ = std::max(last_valid_proposal_view_, view);
      MarkTimeoutProgressLocked();
      cert = GenerateCertificate(*proposal);
      if (cert == nullptr) {
        proposal_valid = false;
      } else {
        if (IsDoubleVoteForExperiment(view)) {
          conflicting_cert = MakeConflictingCertificateForExperiment(*cert);
        }
        std::vector<std::unique_ptr<Proposal>> committed_p_list =
            proposal_manager_->AddProposal(std::move(proposal));
        for (int i = static_cast<int>(committed_p_list.size()) - 1; i >= 0;
             --i) {
          CommitProposal(std::move(committed_p_list[i]));
        }

        if (id_ == NextLeader(view)) {
          proposal_received_ = true;
        }
        if (qc_formed_) {
          proposal_manager_->AddQC(std::move(formed_qc_));
          StartNewRound();
          qc_formed_ = proposal_received_ = false;
        }
        next_leader = proposal_manager_->GetLeader(view + 1);
      }
    }
  }

  if (stale_proposal) {
    if (has_proposal_qc_snapshot && reputation_adapter_ != nullptr) {
      reputation_adapter_->TryRecordCertifiedQc(std::move(proposal_qc_snapshot));
    }
    return true;
  }
  if (!proposal_valid || cert == nullptr) {
    if (has_invalid_qc_snapshot && reputation_adapter_ != nullptr) {
      reputation_adapter_->TryRecordInvalidQcProposal(
          std::move(invalid_qc_snapshot));
    }
    if (has_signed_proposal_snapshot && reputation_adapter_ != nullptr) {
      reputation_adapter_->TryRecordSignedProposal(
          std::move(signed_proposal_snapshot));
    }
    return false;
  }
  int send_result = 0;
  if (SlowVoteAttackForExperiment(view)) {
    if (!EnvFlagEnabled("TD_HS_SLOW_VOTE_WITHHOLD")) {
      const int delay_us =
          PositiveIntFromEnv("TD_HS_SLOW_VOTE_DELAY_US", 10000);
      Certificate delayed_vote = *cert;
      std::thread([this, delayed_vote = std::move(delayed_vote), next_leader,
                   delay_us]() mutable {
        std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
        if (!IsStop()) {
          SendMessage(MessageType::Vote, delayed_vote, next_leader);
        }
      }).detach();
    }
    // else: withhold -- drop this vote entirely (never sent).
  } else {
    send_result = SendMessage(MessageType::Vote, *cert, next_leader);
  }
  if (conflicting_cert != nullptr) {
    SendMessage(MessageType::Vote, *conflicting_cert, next_leader);
  }
  if (has_signed_proposal_snapshot && reputation_adapter_ != nullptr) {
    reputation_adapter_->TryRecordSignedProposal(
        std::move(signed_proposal_snapshot));
  }
  if (has_proposal_qc_snapshot && reputation_adapter_ != nullptr) {
    reputation_adapter_->TryRecordCertifiedQc(std::move(proposal_qc_snapshot));
  }
  return send_result >= 0;
}

void HotStuff::CommitProposal(std::unique_ptr<Proposal> p) {
  commit_q_.Push(std::move(p));
}

std::unique_ptr<Certificate> HotStuff::GenerateCertificate(
    const Proposal& proposal) {
  std::unique_ptr<Certificate> cert = std::make_unique<Certificate>();
  cert->set_hash(proposal.hash());
  cert->set_view(proposal.header().view());
  cert->set_signer(id_);
  cert->set_slot(proposal.header().slot());

  auto hash_signature_or = verifier_->SignMessage(VoteSignaturePayload(*cert));
  if (!hash_signature_or.ok()) {
    LOG(ERROR) << "Sign message fail";
    return nullptr;
  }
  *cert->mutable_sign() = *hash_signature_or;
  return cert;
}

}  // namespace td_hotstuff
}  // namespace resdb
