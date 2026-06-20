#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace resdb {
namespace consensus {
namespace reputation {

enum class ArtifactFamily {
  kUnknown = 0,
  kQc = 1,
  kTimeout = 2,
  kPrepare = 3,
  kCommit = 4,
  kCheckpoint = 5,
};

enum class OutcomeClass {
  kNone = 0,
  kCertified = 1,
  kTimeoutOrViewChange = 2,
  kCommitted = 3,
  kCertifyOnly = 4,
};

struct ReputationConfig {
  int decay_per_epoch = 3;
  int max_recovery_per_epoch = 3;
  int bonus_per_epoch = 1;
  int64_t min_weight = 1;
  int64_t max_weight = 100;
  uint64_t min_decay_opportunities = 1;
  int vote_beta_decay_per_mille = 900;
  int leader_dirichlet_decay_per_mille = 900;
  int leader_dirichlet_alpha_commit = 2;
  int leader_dirichlet_alpha_certify_only = 1;
  int leader_dirichlet_alpha_timeout = 1;
  int leader_certify_only_score = 70;
  int leader_timeout_score = 0;
  bool leader_timeout_outcome_enabled = false;
  bool leader_dirichlet_scoring_enabled = false;
  bool multiplicative_weight_formula_enabled = true;
  int stake_exponent_tau_per_mille = 1000;
  int stake_factor_min_per_mille = 1000;
  int stake_factor_max_per_mille = 1000;
  uint64_t stake_factor_seed = 0x9e3779b97f4a7c15ULL;
  int identity_factor_min_per_mille = 1000;
  int identity_factor_max_per_mille = 1000;
  uint64_t identity_factor_seed = 0xbf58476d1ce4e5b9ULL;
  uint64_t min_leader_opportunities = 8;
  int64_t leader_eligible_min_weight = 10;
  int64_t leader_diversity_soft_min_weight = 80;
  bool leader_recovery_enabled = false;
  bool strong_fault_enabled = false;
  bool double_proposal_detection_enabled = false;
  bool double_vote_detection_enabled = false;
  bool invalid_qc_proposal_detection_enabled = false;
  bool weight_update_vote_equivocation_detection_enabled = false;
  bool conflicting_qc_detection_enabled = false;
  int64_t strong_fault_target_weight = 1;
  bool peertrust_enabled = false;
  int peertrust_debt_increment = 20;
  int peertrust_debt_recovery = 5;
  int peertrust_debt_max = 95;
  int peertrust_debt_trigger_score = 67;
  int64_t peertrust_soft_min_weight = 67;
  bool sybil_graph_enabled = false;
  int sybil_graph_iterations = 0;
  int sybil_graph_max_discount = 40;
  int sybil_graph_debt_increment = 20;
  int sybil_graph_debt_recovery = 5;
  int sybil_graph_debt_max = 95;
  int sybil_graph_debt_trigger_score = 67;
  int sybil_graph_seed_min_reputation = 67;
  uint64_t sybil_graph_min_edges = 1;
};



struct ParticipationBetaCounter {
  uint64_t success = 0;
  uint64_t failure = 0;
};

struct LeaderDirichletCounter {
  uint64_t commit = 0;
  uint64_t certify_only = 0;
  uint64_t timeout = 0;
};

struct ValidatorReputation {
  int validator_id = 0;
  uint64_t opportunities = 0;
  uint64_t inclusions = 0;
  int vote_score = 0;
  uint64_t vote_beta_success = 0;
  uint64_t vote_beta_failure = 0;
  int stake_factor_per_mille = 1000;
  int stake_power_factor_per_mille = 1000;
  int identity_factor_per_mille = 1000;
  int reputation_factor_per_mille = 1000;
  int direct_penalty_factor_per_mille = 1000;
  uint64_t leader_certified_count = 0;
  uint64_t leader_opportunity_count = 0;
  uint64_t leader_dirichlet_commit = 0;
  uint64_t leader_dirichlet_certify_only = 0;
  uint64_t leader_dirichlet_timeout = 0;
  int leader_score = 100;
  int leader_diversity_score = 100;
  int peertrust_score = 100;
  int reviewer_credibility_score = 100;
  int transaction_context_score = 100;
  int community_context_score = 100;
  int reviewer_entropy_score = 100;
  int cross_leader_independence_score = 100;
  int reviewer_overuse_score = 100;
  int peertrust_leader_debt = 0;
  int peertrust_debt_delta = 0;
  uint64_t feedback_count = 0;
  int sybil_rank_score = 100;
  int sybil_cut_score = 100;
  int sybil_graph_score = 100;
  int sybil_graph_debt = 0;
  int sybil_graph_debt_delta = 0;
  uint64_t graph_degree = 0;
  int seed_trust_score = 100;
  int reputation_score = 100;
  int decay_applied = 0;
  int recovery_credit = 0;
  int bonus_credit = 0;
  uint64_t strong_fault_count = 0;
  int64_t penalty_points = 0;
  int64_t current_weight = 1;
  int64_t next_weight = 1;
};

struct SignedProposalEvidence {
  std::string protocol_id;
  int leader_id = 0;
  int view_or_round = 0;
  int slot_or_height = 0;
  std::string proposal_hash;
  bool signature_verified = false;
  std::string active_weight_root;
  uint64_t weight_version = 0;
};

struct SignedVoteEvidence {
  std::string protocol_id;
  int signer_id = 0;
  int view_or_round = 0;
  int slot_or_height = 0;
  std::string proposal_hash;
  bool signature_verified = false;
  std::string active_weight_root;
  uint64_t weight_version = 0;
};

struct InvalidQcProposalEvidence {
  std::string protocol_id;
  int leader_id = 0;
  int view_or_round = 0;
  int slot_or_height = 0;
  std::string proposal_hash;
  bool proposal_signature_verified = false;
  bool qc_verified = false;
  std::string invalid_reason;
  std::string active_weight_root;
  uint64_t weight_version = 0;
};

struct SignedWeightUpdateVoteEvidence {
  std::string protocol_id;
  int validator_id = 0;
  int view_or_round = 0;
  std::string old_weight_root;
  uint64_t old_weight_version = 0;
  int activation_view = 0;
  std::string candidate_digest;
  bool signature_verified = false;
  std::string active_weight_root;
  uint64_t weight_version = 0;
};

struct VerifiedQcArtifactEvidence {
  std::string protocol_id;
  int view_or_round = 0;
  int slot_or_height = 0;
  std::string qc_hash;
  std::string signer_bitmap;
  bool qc_verified = false;
  std::string active_weight_root;
  uint64_t weight_version = 0;
};

struct CertifiedSignerEvidence {
  int view_or_round = 0;
  int leader_id = 0;
  std::string artifact_digest;
  std::string signer_bitmap;
  std::string available_signer_bitmap;
};

struct LeaderOutcomeEvidence {
  int view_or_round = 0;
  int leader_id = 0;
  OutcomeClass outcome_class = OutcomeClass::kNone;
  std::string artifact_digest;
};

struct ReputationWindowInput {
  int local_node_id = 0;
  int total_replicas = 0;
  uint64_t window_index = 0;
  std::vector<int64_t> current_weights;
  std::vector<int64_t> current_leader_weights;
  std::string old_weight_root_hex;
  uint64_t old_weight_version = 0;
  int activation_view = 0;
  int leader_epoch_views = 0;
  std::vector<CertifiedSignerEvidence> certified_signer_evidence;
  std::vector<LeaderOutcomeEvidence> leader_outcome_evidence;
  std::vector<uint64_t> scheduled_leader_counts;
  std::vector<int> stake_factors_per_mille;
  std::vector<int> identity_factors_per_mille;
  std::vector<ParticipationBetaCounter> prior_vote_beta_counters;
  std::vector<LeaderDirichletCounter> prior_leader_dirichlet_counters;
  std::vector<SignedProposalEvidence> signed_proposal_evidence;
  std::vector<SignedVoteEvidence> signed_vote_evidence;
  std::vector<InvalidQcProposalEvidence> invalid_qc_proposal_evidence;
  std::vector<SignedWeightUpdateVoteEvidence> signed_weight_update_vote_evidence;
  std::vector<VerifiedQcArtifactEvidence> verified_qc_artifact_evidence;
  std::vector<int> prior_peertrust_leader_debt;
  std::vector<int> prior_sybil_graph_debt;
};

enum class StrongFaultType {
  kUnknown = 0,
  kDoubleProposal = 1,
  kDoubleVote = 2,
  kInvalidQcProposal = 3,
  kWeightUpdateVoteEquivocation = 4,
  kConflictingQc = 5,
};

struct StrongFaultRecord {
  StrongFaultType type = StrongFaultType::kUnknown;
  int validator_id = 0;
  int view_or_round = 0;
  int slot_or_height = 0;
  std::string first_artifact_digest;
  std::string second_artifact_digest;
};

struct ReputationCandidate {
  std::string algorithm = "bayes_v4";
  int local_node_id = 0;
  int total_replicas = 0;
  uint64_t window_index = 0;
  int start_view = 0;
  int end_view = 0;
  uint64_t event_count = 0;
  std::string old_weight_root_hex;
  uint64_t old_weight_version = 0;
  int activation_view = 0;
  std::vector<ValidatorReputation> validators;
  std::vector<StrongFaultRecord> strong_faults;
  std::vector<int64_t> next_weights;
  std::vector<int64_t> leader_weights;
  std::string leader_weight_root_hex;
  int leader_selection_version = 1;
  int64_t leader_eligible_min_weight = 10;
  int leader_epoch_start_view = 0;
  int leader_epoch_views = 0;
  std::vector<int> leader_epoch_leaders;
  std::string leader_schedule_root_hex;
  std::string metric_root_hex;
  std::string reputation_root_hex;
  std::string strong_fault_root_hex;
  std::string penalty_root_hex;
  std::string next_weight_root_hex;
  std::string candidate_digest_hex;
};


}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
