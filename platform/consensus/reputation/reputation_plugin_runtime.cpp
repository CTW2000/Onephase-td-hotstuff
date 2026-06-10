#include "platform/consensus/reputation/reputation_plugin_runtime.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <numeric>
#include <sstream>
#include <utility>

#include "platform/consensus/reputation/reputation_roots.h"

namespace resdb {
namespace consensus {
namespace reputation {
namespace {

std::vector<int64_t> DefaultWeights(int total_replicas) {
  return std::vector<int64_t>(std::max(total_replicas, 0), 1);
}

std::string WeightRootOrDefault(const std::vector<int64_t>& weights,
                                const std::string& root) {
  return root.empty() ? WeightRootHex(weights) : root;
}

std::string AuditWeightsJson(const std::vector<int64_t>& values) {
  std::ostringstream out;
  out << '[';
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      out << ',';
    }
    out << values[i];
  }
  out << ']';
  return out.str();
}

constexpr int64_t kMinLeaderWeight = 1;
constexpr int64_t kMaxLeaderWeight = 100;
constexpr int kLeaderSelectionVersion = 1;

int RoundRobinLeaderForView(int view, int total_replicas) {
  if (view <= 0 || total_replicas <= 0) {
    return 0;
  }
  return (view % total_replicas) + 1;
}

std::vector<int64_t> NormalizeLeaderWeights(const std::vector<int64_t>& weights,
                                            int total_replicas) {
  std::vector<int64_t> normalized(std::max(total_replicas, 0), kMinLeaderWeight);
  for (int i = 0; i < total_replicas && i < static_cast<int>(weights.size());
       ++i) {
    normalized[i] = std::max(kMinLeaderWeight,
                             std::min(kMaxLeaderWeight, weights[i]));
  }
  return normalized;
}

bool IsRoundRobinLeaderProfile(const std::vector<int64_t>& weights,
                               int total_replicas,
                               int64_t eligible_min_weight) {
  if (total_replicas <= 0 || weights.size() != static_cast<size_t>(total_replicas)) {
    return true;
  }
  if (weights.empty() || weights.front() < eligible_min_weight) {
    return false;
  }
  for (int64_t weight : weights) {
    if (weight != weights.front() || weight < eligible_min_weight) {
      return false;
    }
  }
  return true;
}

std::vector<int> BuildSmoothWeightedRoundRobin(
    const std::vector<int64_t>& weights, int64_t eligible_min_weight,
    int total_replicas) {
  std::vector<int64_t> effective(weights.size(), 0);
  for (size_t i = 0; i < weights.size(); ++i) {
    if (weights[i] >= eligible_min_weight) {
      effective[i] = weights[i];
    }
  }
  int64_t total = std::accumulate(effective.begin(), effective.end(), int64_t{0});
  if (total <= 0) {
    for (size_t i = 0; i < weights.size(); ++i) {
      effective[i] = std::max<int64_t>(0, weights[i]);
    }
    total = std::accumulate(effective.begin(), effective.end(), int64_t{0});
  }
  if (total <= 0) {
    std::vector<int> fallback;
    fallback.reserve(std::max(total_replicas, 0));
    for (int id = 1; id <= total_replicas; ++id) {
      fallback.push_back(id);
    }
    return fallback;
  }

  std::vector<int64_t> current(effective.size(), 0);
  std::vector<int> sequence;
  sequence.reserve(static_cast<size_t>(total));
  for (int64_t step = 0; step < total; ++step) {
    int best = -1;
    for (size_t i = 0; i < effective.size(); ++i) {
      if (effective[i] <= 0) {
        continue;
      }
      current[i] += effective[i];
      if (best < 0 || current[i] > current[best] ||
          (current[i] == current[best] && i < static_cast<size_t>(best))) {
        best = static_cast<int>(i);
      }
    }
    if (best < 0) {
      break;
    }
    sequence.push_back(best + 1);
    current[best] -= total;
  }
  return sequence;
}

void FillSnapshotDefaults(ReputationWeightSnapshot* snapshot,
                          const ReputationRuntimeOptions& options) {
  if (snapshot == nullptr) {
    return;
  }
  if (snapshot->weights.empty()) {
    snapshot->weights = DefaultWeights(options.total_replicas);
  }
  snapshot->weight_root_hex =
      WeightRootOrDefault(snapshot->weights, snapshot->weight_root_hex);
  snapshot->leader_selection_enabled = options.leader_selection_enabled;
  if (snapshot->leader_weights.empty()) {
    snapshot->leader_weights = snapshot->weights;
  }
  snapshot->leader_weights = NormalizeLeaderWeights(snapshot->leader_weights,
                                                    options.total_replicas);
  if (snapshot->leader_weight_version == 0) {
    snapshot->leader_weight_version = snapshot->weight_version;
  }
  if (snapshot->leader_selection_version <= 0) {
    snapshot->leader_selection_version = kLeaderSelectionVersion;
  }
  if (snapshot->leader_eligible_min_weight <= 0) {
    snapshot->leader_eligible_min_weight =
        options.config.leader_eligible_min_weight;
  }
  snapshot->leader_eligible_min_weight = std::max<int64_t>(
      kMinLeaderWeight, snapshot->leader_eligible_min_weight);
  if (snapshot->leader_weight_root_hex.empty()) {
    snapshot->leader_weight_root_hex = LeaderWeightRootHex(
        snapshot->leader_weights, snapshot->leader_eligible_min_weight,
        snapshot->leader_selection_version);
  }
}

std::string AuditValidatorsJson(const std::vector<ValidatorReputation>& validators) {
  std::ostringstream out;
  out << '[';
  for (size_t i = 0; i < validators.size(); ++i) {
    const ValidatorReputation& validator = validators[i];
    if (i > 0) {
      out << ',';
    }
    out << "{\"id\":" << validator.validator_id
        << ",\"opportunities\":" << validator.opportunities
        << ",\"inclusions\":" << validator.inclusions
        << ",\"vote_score\":" << validator.vote_score
        << ",\"leader_certified_count\":" << validator.leader_certified_count
        << ",\"leader_opportunity_count\":" << validator.leader_opportunity_count
        << ",\"leader_score\":" << validator.leader_score
        << ",\"reputation_score\":" << validator.reputation_score
        << ",\"decay_applied\":" << validator.decay_applied
        << ",\"recovery_credit\":" << validator.recovery_credit
        << ",\"bonus_credit\":" << validator.bonus_credit
        << ",\"current_weight\":" << validator.current_weight
        << ",\"next_weight\":" << validator.next_weight << '}';
  }
  out << ']';
  return out.str();
}

}  // namespace

struct ReputationPluginRuntime::RuntimeEvent {
  enum class Type {
    kCertifiedSignerEvidence,
    kLeaderOutcomeEvidence,
    kSignedProposalEvidence,
    kSignedVoteEvidence,
    kWatermark
  };
  Type type = Type::kCertifiedSignerEvidence;
  CertifiedSignerEvidenceRecord evidence;
  LeaderOutcomeEvidenceRecord leader_outcome;
  SignedProposalEvidence signed_proposal;
  SignedVoteEvidence signed_vote;
  int watermark = 0;
};

struct ReputationPluginRuntime::WindowKey {
  uint64_t window_index = 0;
  std::string weight_root_hex;
  uint64_t weight_version = 0;

  bool operator<(const WindowKey& other) const {
    return std::tie(window_index, weight_root_hex, weight_version) <
           std::tie(other.window_index, other.weight_root_hex,
                    other.weight_version);
  }
};

struct ReputationPluginRuntime::WindowBuffer {
  int start_view = 0;
  int end_view = 0;
  ReputationWeightSnapshot snapshot;
  std::vector<CertifiedSignerEvidenceRecord> evidence;
  std::vector<LeaderOutcomeEvidenceRecord> leader_outcomes;
  std::vector<SignedProposalEvidence> signed_proposals;
  std::vector<SignedVoteEvidence> signed_votes;
  std::map<std::tuple<int, int, int>, SignedProposalEvidence>
      first_signed_proposal_by_key;
  std::map<std::tuple<int, int, int>, SignedVoteEvidence>
      first_signed_vote_by_key;
  std::set<std::tuple<int, int, std::string>> seen;
  std::set<std::tuple<int, int, std::string>> seen_leader_outcomes;
  std::set<std::tuple<int, int, int, std::string>> seen_signed_proposals;
  std::set<std::tuple<int, int, int, std::string>> seen_signed_votes;
  std::set<std::tuple<int, int, int>> emitted_signed_proposal_conflicts;
  std::set<std::tuple<int, int, int>> emitted_signed_vote_conflicts;
};

bool CandidateComesBefore(const ReputationCandidate& lhs,
                          const ReputationCandidate& rhs) {
  return std::tie(lhs.activation_view, lhs.start_view, lhs.end_view,
                  lhs.candidate_digest_hex) <
         std::tie(rhs.activation_view, rhs.start_view, rhs.end_view,
                  rhs.candidate_digest_hex);
}

ReputationCandidateKey ReputationCandidateKey::FromCandidate(
    const ReputationCandidate& candidate) {
  ReputationCandidateKey key;
  key.old_weight_root_hex = candidate.old_weight_root_hex;
  key.old_weight_version = candidate.old_weight_version;
  key.start_view = candidate.start_view;
  key.end_view = candidate.end_view;
  key.activation_view = candidate.activation_view;
  key.candidate_digest_hex = candidate.candidate_digest_hex;
  return key;
}

ReputationPluginRuntime::ReputationPluginRuntime(
    ReputationRuntimeOptions options)
    : options_(std::move(options)) {
  options_.window_size_views = std::max<size_t>(1, options_.window_size_views);
  options_.queue_capacity = std::max<size_t>(1, options_.queue_capacity);
  options_.activation_delay_windows = std::max(1, options_.activation_delay_windows);
  if (options_.total_replicas <= 0 && !options_.initial_weights.empty()) {
    options_.total_replicas = static_cast<int>(options_.initial_weights.size());
  }
  if (options_.initial_weights.empty()) {
    options_.initial_weights = DefaultWeights(options_.total_replicas);
  }
  active_snapshot_.weights = options_.initial_weights;
  active_snapshot_.weight_root_hex = options_.initial_weight_root;
  active_snapshot_.weight_version = options_.initial_weight_version;
  active_snapshot_.leader_selection_enabled = options_.leader_selection_enabled;
  active_snapshot_.leader_weights = options_.initial_leader_weights;
  active_snapshot_.leader_weight_root_hex = options_.initial_leader_weight_root;
  active_snapshot_.leader_weight_version = options_.initial_leader_weight_version;
  active_snapshot_.leader_eligible_min_weight =
      options_.config.leader_eligible_min_weight;
  FillSnapshotDefaults(&active_snapshot_, options_);
  known_weights_[{active_snapshot_.weight_root_hex,
                  active_snapshot_.weight_version}] = active_snapshot_.weights;
  known_snapshots_[{active_snapshot_.weight_root_hex,
                    active_snapshot_.weight_version}] = active_snapshot_;
  if (options_.audit_jsonl_enabled && !options_.audit_jsonl_path.empty()) {
    audit_file_.open(options_.audit_jsonl_path, std::ios::out | std::ios::app);
    if (!audit_file_.is_open()) {
      LOG(WARNING) << "failed to open reputation runtime audit path:"
                   << options_.audit_jsonl_path;
    }
  }
}

ReputationPluginRuntime::~ReputationPluginRuntime() { Stop(); }

void ReputationPluginRuntime::Start() {
  if (!options_.enabled) {
    return;
  }
  std::lock_guard<std::mutex> lk(mutex_);
  if (started_) {
    return;
  }
  stop_ = false;
  started_ = true;
  worker_ = std::thread(&ReputationPluginRuntime::WorkerLoop, this);
}

void ReputationPluginRuntime::Stop() {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!started_) {
      pending_.clear();
      return;
    }
    stop_ = true;
  }
  cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
  std::lock_guard<std::mutex> lk(mutex_);
  started_ = false;
  stop_ = false;
  pending_.clear();
}

bool ReputationPluginRuntime::RecordEvidence(
    CertifiedSignerEvidenceRecord evidence) {
  RuntimeEvent event;
  event.type = RuntimeEvent::Type::kCertifiedSignerEvidence;
  event.evidence = std::move(evidence);
  return Enqueue(std::move(event));
}

bool ReputationPluginRuntime::RecordLeaderOutcome(
    LeaderOutcomeEvidenceRecord evidence) {
  RuntimeEvent event;
  event.type = RuntimeEvent::Type::kLeaderOutcomeEvidence;
  event.leader_outcome = std::move(evidence);
  return Enqueue(std::move(event));
}

bool ReputationPluginRuntime::RecordSignedProposalEvidence(
    SignedProposalEvidence evidence) {
  RuntimeEvent event;
  event.type = RuntimeEvent::Type::kSignedProposalEvidence;
  event.signed_proposal = std::move(evidence);
  return Enqueue(std::move(event));
}

bool ReputationPluginRuntime::RecordSignedVoteEvidence(
    SignedVoteEvidence evidence) {
  RuntimeEvent event;
  event.type = RuntimeEvent::Type::kSignedVoteEvidence;
  event.signed_vote = std::move(evidence);
  return Enqueue(std::move(event));
}

bool ReputationPluginRuntime::AdvanceWatermark(int view_or_round) {
  RuntimeEvent event;
  event.type = RuntimeEvent::Type::kWatermark;
  event.watermark = view_or_round;
  return Enqueue(std::move(event));
}

bool ReputationPluginRuntime::Enqueue(RuntimeEvent event) {
  if (!options_.enabled) {
    return false;
  }
  bool should_notify = false;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (stop_ || pending_.size() >= options_.queue_capacity) {
      dropped_count_.fetch_add(1);
      LOG_EVERY_N(WARNING, 1000)
          << "reputation plugin runtime queue full, dropped:"
          << dropped_count_.load();
      return false;
    }
    should_notify = pending_.empty();
    pending_.push_back(std::move(event));
    queued_count_.fetch_add(1);
  }
  if (should_notify) {
    cv_.notify_one();
  }
  return true;
}

void ReputationPluginRuntime::UpdateActiveWeights(
    ReputationWeightSnapshot snapshot) {
  FillSnapshotDefaults(&snapshot, options_);
  std::lock_guard<std::mutex> lk(mutex_);
  active_snapshot_ = std::move(snapshot);
  known_weights_[{active_snapshot_.weight_root_hex,
                  active_snapshot_.weight_version}] = active_snapshot_.weights;
  known_snapshots_[{active_snapshot_.weight_root_hex,
                    active_snapshot_.weight_version}] = active_snapshot_;
}

std::vector<ReputationCandidate>
ReputationPluginRuntime::TakeCompletedCandidates() {
  std::map<uint64_t, ReputationCandidate> drained;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    drained.swap(completed_by_version_);
  }
  std::vector<ReputationCandidate> output;
  output.reserve(drained.size());
  for (auto& entry : drained) {
    output.push_back(std::move(entry.second));
  }
  return output;
}

std::optional<ReputationCandidate> ReputationPluginRuntime::FindLocalCandidate(
    const ReputationCandidateKey& key) const {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = completed_index_.find(key);
  if (it == completed_index_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void ReputationPluginRuntime::WorkerLoop() {
  while (true) {
    std::deque<RuntimeEvent> batch;
    {
      std::unique_lock<std::mutex> lk(mutex_);
      cv_.wait(lk, [this]() { return stop_ || !pending_.empty(); });
      if (pending_.empty() && stop_) {
        break;
      }
      batch.swap(pending_);
    }
    while (!batch.empty()) {
      RuntimeEvent event = std::move(batch.front());
      batch.pop_front();
      if (event.type == RuntimeEvent::Type::kCertifiedSignerEvidence) {
        ProcessEvidence(std::move(event.evidence));
      } else if (event.type == RuntimeEvent::Type::kLeaderOutcomeEvidence) {
        ProcessLeaderOutcome(std::move(event.leader_outcome));
      } else if (event.type == RuntimeEvent::Type::kSignedProposalEvidence) {
        ProcessSignedProposalEvidence(std::move(event.signed_proposal));
      } else if (event.type == RuntimeEvent::Type::kSignedVoteEvidence) {
        ProcessSignedVoteEvidence(std::move(event.signed_vote));
      } else {
        ProcessWatermark(event.watermark);
      }
    }
  }
}

ReputationWeightSnapshot ReputationPluginRuntime::SnapshotFromFields(
    const std::string& weight_root_hex, uint64_t weight_version,
    const std::vector<int64_t>& weights) const {
  ReputationWeightSnapshot snapshot;
  snapshot.weight_root_hex = weight_root_hex.empty()
                                 ? active_snapshot_.weight_root_hex
                                 : weight_root_hex;
  snapshot.weight_version = weight_version;
  auto snapshot_it = known_snapshots_.find({snapshot.weight_root_hex,
                                            snapshot.weight_version});
  if (snapshot_it != known_snapshots_.end()) {
    snapshot = snapshot_it->second;
  }
  snapshot.weight_root_hex = weight_root_hex.empty()
                                 ? snapshot.weight_root_hex
                                 : weight_root_hex;
  snapshot.weight_version = weight_version;
  if (!weights.empty()) {
    snapshot.weights = weights;
  } else if (snapshot.weights.empty()) {
    auto it = known_weights_.find({snapshot.weight_root_hex,
                                   snapshot.weight_version});
    if (it != known_weights_.end()) {
      snapshot.weights = it->second;
    } else {
      snapshot.weights = active_snapshot_.weights;
    }
  }
  FillSnapshotDefaults(&snapshot, options_);
  return snapshot;
}

ReputationWeightSnapshot ReputationPluginRuntime::SnapshotForEvidence(
    const CertifiedSignerEvidenceRecord& evidence) const {
  return SnapshotFromFields(evidence.weight_root_hex, evidence.weight_version,
                            evidence.active_weights);
}

ReputationWeightSnapshot ReputationPluginRuntime::SnapshotForLeaderOutcome(
    const LeaderOutcomeEvidenceRecord& evidence) const {
  return SnapshotFromFields(evidence.weight_root_hex, evidence.weight_version,
                            evidence.active_weights);
}

ReputationWeightSnapshot ReputationPluginRuntime::SnapshotForSignedProposal(
    const SignedProposalEvidence& evidence) const {
  return SnapshotFromFields(evidence.active_weight_root, evidence.weight_version,
                            /*weights=*/{});
}

ReputationWeightSnapshot ReputationPluginRuntime::SnapshotForSignedVote(
    const SignedVoteEvidence& evidence) const {
  return SnapshotFromFields(evidence.active_weight_root, evidence.weight_version,
                            /*weights=*/{});
}

std::vector<uint64_t> ReputationPluginRuntime::ScheduledLeaderCountsForWindow(
    int start_view, int end_view, const ReputationWeightSnapshot& snapshot) const {
  std::vector<uint64_t> counts(std::max(options_.total_replicas, 0), 0);
  if (end_view <= start_view || options_.total_replicas <= 0) {
    return counts;
  }
  const std::vector<int64_t> leader_weights = NormalizeLeaderWeights(
      snapshot.leader_weights.empty() ? snapshot.weights : snapshot.leader_weights,
      options_.total_replicas);
  const int64_t threshold = std::max<int64_t>(
      kMinLeaderWeight, snapshot.leader_eligible_min_weight);
  std::vector<int> sequence;
  if (snapshot.leader_selection_enabled &&
      !IsRoundRobinLeaderProfile(leader_weights, options_.total_replicas,
                                 threshold)) {
    sequence = BuildSmoothWeightedRoundRobin(leader_weights, threshold,
                                             options_.total_replicas);
  }
  for (int view = start_view; view < end_view; ++view) {
    int leader = 0;
    if (sequence.empty()) {
      leader = RoundRobinLeaderForView(view, options_.total_replicas);
    } else {
      const int idx = ((view % static_cast<int>(sequence.size())) +
                       static_cast<int>(sequence.size())) %
                      static_cast<int>(sequence.size());
      leader = sequence[idx];
    }
    if (leader >= 1 && leader <= options_.total_replicas) {
      ++counts[leader - 1];
    }
  }
  return counts;
}

void ReputationPluginRuntime::ProcessEvidence(
    CertifiedSignerEvidenceRecord evidence) {
  if (evidence.view_or_round < 0) {
    return;
  }
  const uint64_t window_index =
      static_cast<uint64_t>(evidence.view_or_round) / options_.window_size_views;
  const int window_start =
      static_cast<int>(window_index * options_.window_size_views);
  const int window_end =
      static_cast<int>(window_start + options_.window_size_views);
  ReputationWeightSnapshot snapshot = SnapshotForEvidence(evidence);
  WindowKey key;
  key.window_index = window_index;
  key.weight_root_hex = snapshot.weight_root_hex;
  key.weight_version = snapshot.weight_version;

  std::lock_guard<std::mutex> lk(mutex_);
  WindowBuffer& buffer = windows_[key];
  if (buffer.evidence.empty() && buffer.leader_outcomes.empty() &&
      buffer.signed_proposals.empty() && buffer.signed_votes.empty() &&
      buffer.first_signed_proposal_by_key.empty() &&
      buffer.first_signed_vote_by_key.empty()) {
    buffer.start_view = window_start;
    buffer.end_view = window_end;
    buffer.snapshot = std::move(snapshot);
  }
  const auto dedupe_key = std::make_tuple(
      evidence.view_or_round, evidence.slot_or_height, evidence.artifact_digest);
  if (!buffer.seen.insert(dedupe_key).second) {
    return;
  }
  buffer.evidence.push_back(std::move(evidence));
}

void ReputationPluginRuntime::ProcessLeaderOutcome(
    LeaderOutcomeEvidenceRecord evidence) {
  if (evidence.view_or_round < 0 || evidence.leader_id <= 0 ||
      evidence.outcome_class == OutcomeClass::kNone) {
    return;
  }
  const uint64_t window_index =
      static_cast<uint64_t>(evidence.view_or_round) / options_.window_size_views;
  const int window_start =
      static_cast<int>(window_index * options_.window_size_views);
  const int window_end =
      static_cast<int>(window_start + options_.window_size_views);
  ReputationWeightSnapshot snapshot = SnapshotForLeaderOutcome(evidence);
  WindowKey key;
  key.window_index = window_index;
  key.weight_root_hex = snapshot.weight_root_hex;
  key.weight_version = snapshot.weight_version;

  std::lock_guard<std::mutex> lk(mutex_);
  WindowBuffer& buffer = windows_[key];
  if (buffer.evidence.empty() && buffer.leader_outcomes.empty() &&
      buffer.signed_proposals.empty() && buffer.signed_votes.empty() &&
      buffer.first_signed_proposal_by_key.empty() &&
      buffer.first_signed_vote_by_key.empty()) {
    buffer.start_view = window_start;
    buffer.end_view = window_end;
    buffer.snapshot = std::move(snapshot);
  }
  const auto dedupe_key = std::make_tuple(
      evidence.view_or_round, evidence.leader_id, evidence.artifact_digest);
  if (!buffer.seen_leader_outcomes.insert(dedupe_key).second) {
    return;
  }
  buffer.leader_outcomes.push_back(std::move(evidence));
}

void ReputationPluginRuntime::ProcessSignedProposalEvidence(
    SignedProposalEvidence evidence) {
  if (evidence.view_or_round < 0 || evidence.leader_id <= 0 ||
      evidence.proposal_hash.empty() || !evidence.signature_verified) {
    return;
  }
  const uint64_t window_index =
      static_cast<uint64_t>(evidence.view_or_round) / options_.window_size_views;
  const int window_start =
      static_cast<int>(window_index * options_.window_size_views);
  const int window_end =
      static_cast<int>(window_start + options_.window_size_views);
  ReputationWeightSnapshot snapshot = SnapshotForSignedProposal(evidence);
  WindowKey key;
  key.window_index = window_index;
  key.weight_root_hex = snapshot.weight_root_hex;
  key.weight_version = snapshot.weight_version;

  std::lock_guard<std::mutex> lk(mutex_);
  WindowBuffer& buffer = windows_[key];
  if (buffer.evidence.empty() && buffer.leader_outcomes.empty() &&
      buffer.signed_proposals.empty() && buffer.signed_votes.empty() &&
      buffer.first_signed_proposal_by_key.empty() &&
      buffer.first_signed_vote_by_key.empty()) {
    buffer.start_view = window_start;
    buffer.end_view = window_end;
    buffer.snapshot = std::move(snapshot);
  }
  const auto proposal_key = std::make_tuple(
      evidence.leader_id, evidence.view_or_round, evidence.slot_or_height);
  const auto dedupe_key =
      std::make_tuple(evidence.leader_id, evidence.view_or_round,
                      evidence.slot_or_height, evidence.proposal_hash);
  if (!buffer.seen_signed_proposals.insert(dedupe_key).second) {
    return;
  }
  auto first_it = buffer.first_signed_proposal_by_key.find(proposal_key);
  if (first_it == buffer.first_signed_proposal_by_key.end()) {
    buffer.first_signed_proposal_by_key.emplace(proposal_key,
                                                std::move(evidence));
    return;
  }
  if (first_it->second.proposal_hash == evidence.proposal_hash) {
    return;
  }
  if (!buffer.emitted_signed_proposal_conflicts.insert(proposal_key).second) {
    return;
  }
  buffer.signed_proposals.push_back(first_it->second);
  buffer.signed_proposals.push_back(std::move(evidence));
}

void ReputationPluginRuntime::ProcessSignedVoteEvidence(
    SignedVoteEvidence evidence) {
  if (evidence.view_or_round < 0 || evidence.signer_id <= 0 ||
      evidence.proposal_hash.empty() || !evidence.signature_verified) {
    return;
  }
  const uint64_t window_index =
      static_cast<uint64_t>(evidence.view_or_round) / options_.window_size_views;
  const int window_start =
      static_cast<int>(window_index * options_.window_size_views);
  const int window_end =
      static_cast<int>(window_start + options_.window_size_views);
  ReputationWeightSnapshot snapshot = SnapshotForSignedVote(evidence);
  WindowKey key;
  key.window_index = window_index;
  key.weight_root_hex = snapshot.weight_root_hex;
  key.weight_version = snapshot.weight_version;

  std::lock_guard<std::mutex> lk(mutex_);
  WindowBuffer& buffer = windows_[key];
  if (buffer.evidence.empty() && buffer.leader_outcomes.empty() &&
      buffer.signed_proposals.empty() && buffer.signed_votes.empty() &&
      buffer.first_signed_proposal_by_key.empty() &&
      buffer.first_signed_vote_by_key.empty()) {
    buffer.start_view = window_start;
    buffer.end_view = window_end;
    buffer.snapshot = std::move(snapshot);
  }
  const auto vote_key = std::make_tuple(
      evidence.signer_id, evidence.view_or_round, evidence.slot_or_height);
  const auto dedupe_key =
      std::make_tuple(evidence.signer_id, evidence.view_or_round,
                      evidence.slot_or_height, evidence.proposal_hash);
  if (!buffer.seen_signed_votes.insert(dedupe_key).second) {
    return;
  }
  auto first_it = buffer.first_signed_vote_by_key.find(vote_key);
  if (first_it == buffer.first_signed_vote_by_key.end()) {
    buffer.first_signed_vote_by_key.emplace(vote_key, std::move(evidence));
    return;
  }
  if (first_it->second.proposal_hash == evidence.proposal_hash) {
    return;
  }
  if (!buffer.emitted_signed_vote_conflicts.insert(vote_key).second) {
    return;
  }
  buffer.signed_votes.push_back(first_it->second);
  buffer.signed_votes.push_back(std::move(evidence));
}

void ReputationPluginRuntime::ProcessWatermark(int view_or_round) {
  std::vector<std::pair<WindowKey, WindowBuffer>> ready;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    for (auto it = windows_.begin(); it != windows_.end();) {
      if (view_or_round >= it->second.end_view) {
        ready.emplace_back(it->first, std::move(it->second));
        it = windows_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto& item : ready) {
    FinalizeWindow(item.first, &item.second);
  }
}

void ReputationPluginRuntime::FinalizeWindow(const WindowKey& key,
                                             WindowBuffer* buffer) {
  if (buffer == nullptr ||
      (buffer->evidence.empty() && buffer->leader_outcomes.empty() &&
       buffer->signed_proposals.empty() && buffer->signed_votes.empty())) {
    return;
  }
  std::sort(buffer->evidence.begin(), buffer->evidence.end(),
            [](const CertifiedSignerEvidenceRecord& lhs,
               const CertifiedSignerEvidenceRecord& rhs) {
              return std::tie(lhs.view_or_round, lhs.slot_or_height,
                              lhs.artifact_digest) <
                     std::tie(rhs.view_or_round, rhs.slot_or_height,
                              rhs.artifact_digest);
            });
  std::sort(buffer->leader_outcomes.begin(), buffer->leader_outcomes.end(),
            [](const LeaderOutcomeEvidenceRecord& lhs,
               const LeaderOutcomeEvidenceRecord& rhs) {
              return std::tie(lhs.view_or_round, lhs.leader_id,
                              lhs.outcome_class, lhs.artifact_digest) <
                     std::tie(rhs.view_or_round, rhs.leader_id,
                              rhs.outcome_class, rhs.artifact_digest);
            });
  std::sort(buffer->signed_proposals.begin(), buffer->signed_proposals.end(),
            [](const SignedProposalEvidence& lhs,
               const SignedProposalEvidence& rhs) {
              return std::tie(lhs.leader_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.proposal_hash) <
                     std::tie(rhs.leader_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.proposal_hash);
            });
  std::sort(buffer->signed_votes.begin(), buffer->signed_votes.end(),
            [](const SignedVoteEvidence& lhs,
               const SignedVoteEvidence& rhs) {
              return std::tie(lhs.signer_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.proposal_hash) <
                     std::tie(rhs.signer_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.proposal_hash);
            });

  ReputationWindowInput input;
  input.total_replicas = options_.total_replicas;
  input.window_index = key.window_index;
  input.current_weights = buffer->snapshot.weights;
  input.old_weight_root_hex = buffer->snapshot.weight_root_hex;
  input.old_weight_version = buffer->snapshot.weight_version;
  input.activation_view = buffer->end_view +
                          options_.activation_delay_windows *
                              static_cast<int>(options_.window_size_views);
  input.certified_signer_evidence.reserve(buffer->evidence.size());
  for (const CertifiedSignerEvidenceRecord& record : buffer->evidence) {
    CertifiedSignerEvidence evidence;
    evidence.view_or_round = record.view_or_round;
    evidence.leader_id = record.leader_id;
    evidence.artifact_digest = record.artifact_digest;
    evidence.signer_bitmap = record.signer_bitmap;
    evidence.available_signer_bitmap = record.available_signer_bitmap;
    input.certified_signer_evidence.push_back(std::move(evidence));
  }
  input.leader_outcome_evidence.reserve(buffer->leader_outcomes.size());
  for (const LeaderOutcomeEvidenceRecord& record : buffer->leader_outcomes) {
    LeaderOutcomeEvidence evidence;
    evidence.view_or_round = record.view_or_round;
    evidence.leader_id = record.leader_id;
    evidence.outcome_class = record.outcome_class;
    evidence.artifact_digest = record.artifact_digest;
    input.leader_outcome_evidence.push_back(std::move(evidence));
  }
  input.signed_proposal_evidence = buffer->signed_proposals;
  input.signed_vote_evidence = buffer->signed_votes;
  input.scheduled_leader_counts = ScheduledLeaderCountsForWindow(
      buffer->start_view, buffer->end_view, buffer->snapshot);

  ReputationCandidate candidate = ComputeReputationCandidate(input, options_.config);
  candidate.window_index = key.window_index;
  candidate.start_view = buffer->start_view;
  candidate.end_view = buffer->end_view;
  candidate.activation_view = input.activation_view;
  candidate.old_weight_root_hex = input.old_weight_root_hex;
  candidate.old_weight_version = input.old_weight_version;
  candidate.event_count = input.certified_signer_evidence.size() +
                          input.leader_outcome_evidence.size() +
                          input.signed_proposal_evidence.size() +
                          input.signed_vote_evidence.size();
  ApplyPersistentStrongFaults(&candidate);
  RecomputeReputationCandidateRoots(&candidate);
  WriteAudit(candidate);
  PushCompleted(std::move(candidate));
  computed_window_count_.fetch_add(1);
}

void ReputationPluginRuntime::ApplyPersistentStrongFaults(
    ReputationCandidate* candidate) {
  if (candidate == nullptr || !options_.config.strong_fault_enabled) {
    return;
  }
  std::set<int> persistent_faults;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    for (const StrongFaultRecord& fault : candidate->strong_faults) {
      if (fault.validator_id >= 1 &&
          fault.validator_id <= options_.total_replicas) {
        persistent_strong_fault_validators_.insert(fault.validator_id);
      }
    }
    persistent_faults = persistent_strong_fault_validators_;
  }
  const int64_t penalty_weight = std::max<int64_t>(
      options_.config.min_weight,
      std::min<int64_t>(options_.config.max_weight,
                        options_.config.strong_fault_target_weight));
  for (int validator_id : persistent_faults) {
    if (validator_id < 1 ||
        validator_id > static_cast<int>(candidate->validators.size())) {
      continue;
    }
    ValidatorReputation& validator = candidate->validators[validator_id - 1];
    validator.recovery_credit = 0;
    validator.bonus_credit = 0;
    validator.reputation_score = 0;
    validator.penalty_points =
        std::max<int64_t>(validator.penalty_points,
                          std::max<int64_t>(0,
                                            validator.next_weight -
                                                penalty_weight));
    validator.next_weight = penalty_weight;
  }
}

void ReputationPluginRuntime::PushCompleted(ReputationCandidate candidate) {
  const ReputationCandidateKey key = ReputationCandidateKey::FromCandidate(candidate);
  std::lock_guard<std::mutex> lk(mutex_);
  completed_index_[key] = candidate;
  auto it = completed_by_version_.find(candidate.old_weight_version);
  if (it == completed_by_version_.end() ||
      CandidateComesBefore(candidate, it->second)) {
    completed_by_version_[candidate.old_weight_version] = std::move(candidate);
  }
}

void ReputationPluginRuntime::WriteAudit(const ReputationCandidate& candidate) {
  if (!audit_file_.is_open()) {
    return;
  }
  audit_file_ << "{\"algorithm\":\"" << candidate.algorithm << "\",";
  audit_file_ << "\"window_index\":" << candidate.window_index << ',';
  audit_file_ << "\"start_view\":" << candidate.start_view << ',';
  audit_file_ << "\"end_view\":" << candidate.end_view << ',';
  audit_file_ << "\"event_count\":" << candidate.event_count << ',';
  audit_file_ << "\"old_weight_version\":" << candidate.old_weight_version << ',';
  audit_file_ << "\"old_weight_root\":\"" << candidate.old_weight_root_hex << "\",";
  audit_file_ << "\"activation_view\":" << candidate.activation_view << ',';
  audit_file_ << "\"candidate_digest\":\"" << candidate.candidate_digest_hex << "\",";
  audit_file_ << "\"next_weights\":" << AuditWeightsJson(candidate.next_weights) << ',';
  audit_file_ << "\"validators\":" << AuditValidatorsJson(candidate.validators);
  audit_file_ << "}\n";
  audit_file_.flush();
}

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
