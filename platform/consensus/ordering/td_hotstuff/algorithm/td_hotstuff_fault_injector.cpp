#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff_fault_injector.h"

#include <algorithm>
#include <cstdlib>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "platform/consensus/ordering/td_hotstuff/algorithm/certificate_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_manager.h"

namespace resdb {
namespace td_hotstuff {
namespace {

bool EnvFlagEnabled(const char* name) {
  const char* raw = std::getenv(name);
  if (raw == nullptr) {
    return false;
  }
  const std::string value(raw);
  return value == "1" || value == "true" || value == "TRUE" || value == "yes" ||
         value == "YES" || value == "on" || value == "ON";
}

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


std::vector<int> PositiveIntListFromEnv(const char* name, int total_replicas) {
  std::vector<int> ids;
  const char* raw = std::getenv(name);
  if (raw == nullptr || std::string(raw).empty()) {
    return ids;
  }
  std::stringstream stream(raw);
  std::string token;
  while (std::getline(stream, token, ',')) {
    if (token.empty()) {
      continue;
    }
    try {
      const int id = std::stoi(token);
      if (id >= 1 && id <= total_replicas &&
          std::find(ids.begin(), ids.end(), id) == ids.end()) {
        ids.push_back(id);
      }
    } catch (...) {
    }
  }
  return ids;
}

std::string ProposalHashForExperiment(const Proposal& proposal) {
  std::string data;
  for (const auto& txn : proposal.transactions()) {
    std::string txn_data;
    txn.SerializeToString(&txn_data);
    data += txn_data;
  }

  std::string header_data;
  proposal.header().SerializeToString(&header_data);
  data += header_data;
  return SignatureVerifier::CalculateHash(data);
}

bool ResignProposalForExperiment(Proposal* proposal,
                                 SignatureVerifier* verifier) {
  if (proposal == nullptr || verifier == nullptr) {
    return false;
  }
  proposal->set_hash(ProposalHashForExperiment(*proposal));
  auto signature_or = verifier->SignMessage(ProposalSignaturePayload(*proposal));
  if (!signature_or.ok()) {
    LOG(ERROR) << "failed to sign TD-Hotstuff experiment proposal view:"
               << proposal->header().view();
    return false;
  }
  *proposal->mutable_signature() = *signature_or;
  return true;
}

}  // namespace

ExperimentFaultConfig ExperimentFaultConfigFromEnv(int total_replicas) {
  ExperimentFaultConfig config;
  config.silent_leader = EnvFlagEnabled("TD_HS_SILENT_LEADER");
  config.sybil_graph_attack = EnvFlagEnabled("TD_HS_SYBIL_GRAPH_ATTACK");
  config.unfair_leader = EnvFlagEnabled("TD_HS_UNFAIR_LEADER") ||
                         EnvFlagEnabled("TD_HS_PEERTRUST_CLIQUE") ||
                         config.sybil_graph_attack;
  config.double_proposal = EnvFlagEnabled("TD_HS_DOUBLE_PROPOSAL");
  config.double_vote = EnvFlagEnabled("TD_HS_DOUBLE_VOTE");
  config.invalid_qc = EnvFlagEnabled("TD_HS_INVALID_QC");
  config.weight_update_vote_equivocation =
      EnvFlagEnabled("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION");
  config.timeout_vote_equivocation =
      EnvFlagEnabled("TD_HS_TIMEOUT_VOTE_EQUIVOCATION");
  config.invalid_tc_proposal = EnvFlagEnabled("TD_HS_INVALID_TC_PROPOSAL");
  config.unfair_leader_signer_group_size =
      PositiveIntFromEnv("TD_HS_UNFAIR_LEADER_SIGNER_GROUP_SIZE",
                         total_replicas);
  config.peertrust_clique_reviewer_ids = PositiveIntListFromEnv(
      "TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS", total_replicas);
  if (config.peertrust_clique_reviewer_ids.empty()) {
    config.peertrust_clique_reviewer_ids = PositiveIntListFromEnv(
        "TD_HS_PEERTRUST_CLIQUE_SIGNER_IDS", total_replicas);
  }
  config.sybil_graph_reviewer_ids = PositiveIntListFromEnv(
      "TD_HS_SYBIL_GRAPH_REVIEWER_IDS", total_replicas);
  return config;
}

std::unique_ptr<Proposal> BuildConflictingProposalForExperiment(
    const Proposal& base_proposal, SignatureVerifier* verifier) {
  auto proposal = std::make_unique<Proposal>(base_proposal);
  const int base_id = proposal->header().proposal_id();
  proposal->mutable_header()->set_proposal_id(base_id + 1);
  if (proposal->header().proposal_id() == base_id) {
    proposal->mutable_header()->set_proposal_id(base_id == 1 ? 2 : 1);
  }
  if (!ResignProposalForExperiment(proposal.get(), verifier)) {
    return nullptr;
  }
  return proposal;
}

std::unique_ptr<Proposal> BuildInvalidQcProposalForExperiment(
    const Proposal& base_proposal, SignatureVerifier* verifier,
    int total_replicas) {
  if (base_proposal.header().qc().hash().empty()) {
    return nullptr;
  }
  auto proposal = std::make_unique<Proposal>(base_proposal);
  QC* qc = proposal->mutable_header()->mutable_qc();
  std::string forged_bitmap = qc->signer_bitmap();
  if (forged_bitmap.empty()) {
    forged_bitmap.assign((std::max(total_replicas, 1) + 7) / 8, '\0');
  }
  forged_bitmap[0] = static_cast<char>(forged_bitmap[0] ^ 0x01);
  qc->set_signer_bitmap(forged_bitmap);
  if (!ResignProposalForExperiment(proposal.get(), verifier)) {
    return nullptr;
  }
  return proposal;
}

std::unique_ptr<Proposal> BuildInvalidTcProposalForExperiment(
    const Proposal& base_proposal, SignatureVerifier* verifier) {
  if (base_proposal.header().view() <= 1) {
    return nullptr;
  }
  auto proposal = std::make_unique<Proposal>(base_proposal);
  TimeoutCert* cert = proposal->mutable_header()->mutable_timeout_cert();
  cert->Clear();
  cert->set_view(base_proposal.header().view() - 1);
  cert->set_quorum_rule_id("invalid_timeout_cert_for_experiment");
  if (!ResignProposalForExperiment(proposal.get(), verifier)) {
    return nullptr;
  }
  return proposal;
}

std::unique_ptr<Certificate> BuildConflictingVoteForExperiment(
    const Proposal& proposal, int signer_id, SignatureVerifier* verifier) {
  if (verifier == nullptr || signer_id <= 0) {
    return nullptr;
  }
  auto cert = std::make_unique<Certificate>();
  cert->set_hash(proposal.hash() + "#double_vote");
  cert->set_view(proposal.header().view());
  cert->set_signer(signer_id);
  cert->set_slot(proposal.header().slot());

  auto signature_or = verifier->SignMessage(VoteSignaturePayload(*cert));
  if (!signature_or.ok()) {
    LOG(ERROR) << "failed to sign TD-Hotstuff experiment conflicting vote";
    return nullptr;
  }
  *cert->mutable_sign() = *signature_or;
  return cert;
}

std::unique_ptr<WeightUpdateVote> BuildWeightUpdateVoteForExperiment(
    int validator_id, std::string candidate_digest, std::string old_weight_root,
    uint64_t old_weight_version, int activation_view,
    SignatureVerifier* verifier) {
  if (verifier == nullptr || validator_id <= 0 || candidate_digest.empty() ||
      old_weight_root.empty() || activation_view <= 0) {
    return nullptr;
  }
  auto vote = std::make_unique<WeightUpdateVote>();
  vote->set_candidate_digest(std::move(candidate_digest));
  vote->set_validator_id(validator_id);
  vote->set_old_weight_root(std::move(old_weight_root));
  vote->set_old_weight_version(old_weight_version);
  vote->set_activation_view(activation_view);
  auto signature_or = verifier->SignMessage(WeightUpdateVotePayload(*vote));
  if (!signature_or.ok()) {
    LOG(ERROR) << "failed to sign TD-Hotstuff experiment weight update vote";
    return nullptr;
  }
  *vote->mutable_signature() = *signature_or;
  return vote;
}

std::unique_ptr<WeightUpdateVote> BuildConflictingWeightUpdateVoteForExperiment(
    const WeightUpdateVote& base_vote, std::string conflicting_candidate_digest,
    SignatureVerifier* verifier) {
  if (conflicting_candidate_digest.empty() ||
      conflicting_candidate_digest == base_vote.candidate_digest()) {
    return nullptr;
  }
  return BuildWeightUpdateVoteForExperiment(
      base_vote.validator_id(), std::move(conflicting_candidate_digest),
      base_vote.old_weight_root(), base_vote.old_weight_version(),
      base_vote.activation_view(), verifier);
}

std::unique_ptr<TimeoutVote> BuildConflictingTimeoutVoteForExperiment(
    const TimeoutVote& base_vote, SignatureVerifier* verifier) {
  if (verifier == nullptr || base_vote.view() <= 0 ||
      base_vote.signer() <= 0 || base_vote.high_qc().hash().empty()) {
    return nullptr;
  }
  auto conflicting = std::make_unique<TimeoutVote>();
  conflicting->set_view(base_vote.view());
  conflicting->set_signer(base_vote.signer());
  conflicting->clear_high_qc();
  auto signature_or = verifier->SignMessage(TimeoutVotePayload(*conflicting));
  if (!signature_or.ok()) {
    LOG(ERROR) << "failed to sign TD-Hotstuff experiment timeout vote";
    return nullptr;
  }
  *conflicting->mutable_signature() = *signature_or;
  return conflicting;
}

}  // namespace td_hotstuff
}  // namespace resdb
