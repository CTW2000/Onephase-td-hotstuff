#include "platform/consensus/reputation/strong_fault_detector.h"

#include <algorithm>
#include <map>
#include <set>
#include <tuple>

#include "platform/consensus/reputation/reputation_utils.h"

namespace resdb {
namespace consensus {
namespace reputation {

std::vector<StrongFaultRecord> DetectDoubleProposalFaults(
    const std::vector<SignedProposalEvidence>& signed_proposal_evidence) {
  std::vector<SignedProposalEvidence> ordered = signed_proposal_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const SignedProposalEvidence& lhs,
               const SignedProposalEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.weight_version,
                              lhs.leader_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.proposal_hash) <
                     std::tie(rhs.protocol_id, rhs.weight_version,
                              rhs.leader_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.proposal_hash);
            });

  using ArtifactKey = std::tuple<std::string, uint64_t, int, int, int>;
  std::map<ArtifactKey, SignedProposalEvidence> first_artifact_by_key;
  std::set<ArtifactKey> emitted_keys;
  std::vector<StrongFaultRecord> faults;
  for (const SignedProposalEvidence& artifact : ordered) {
    if (!artifact.signature_verified || artifact.protocol_id.empty() ||
        artifact.leader_id <= 0 || artifact.view_or_round <= 0 ||
        artifact.proposal_hash.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.weight_version,
                          artifact.leader_id, artifact.view_or_round,
                          artifact.slot_or_height};
    auto inserted = first_artifact_by_key.emplace(key, artifact);
    const SignedProposalEvidence& first = inserted.first->second;
    if (inserted.second || first.proposal_hash == artifact.proposal_hash) {
      continue;
    }
    if (!emitted_keys.insert(key).second) {
      continue;
    }
    StrongFaultRecord fault;
    fault.type = StrongFaultType::kDoubleProposal;
    fault.validator_id = artifact.leader_id;
    fault.view_or_round = artifact.view_or_round;
    fault.slot_or_height = artifact.slot_or_height;
    fault.first_artifact_digest =
        std::min(first.proposal_hash, artifact.proposal_hash);
    fault.second_artifact_digest =
        std::max(first.proposal_hash, artifact.proposal_hash);
    faults.push_back(std::move(fault));
  }
  return faults;
}

std::vector<StrongFaultRecord> DetectDoubleVoteFaults(
    const std::vector<SignedVoteEvidence>& signed_vote_evidence) {
  std::vector<SignedVoteEvidence> ordered = signed_vote_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const SignedVoteEvidence& lhs,
               const SignedVoteEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.weight_version,
                              lhs.signer_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.proposal_hash) <
                     std::tie(rhs.protocol_id, rhs.weight_version,
                              rhs.signer_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.proposal_hash);
            });

  using ArtifactKey = std::tuple<std::string, uint64_t, int, int, int>;
  std::map<ArtifactKey, SignedVoteEvidence> first_artifact_by_key;
  std::set<ArtifactKey> emitted_keys;
  std::vector<StrongFaultRecord> faults;
  for (const SignedVoteEvidence& artifact : ordered) {
    if (!artifact.signature_verified || artifact.protocol_id.empty() ||
        artifact.signer_id <= 0 || artifact.view_or_round <= 0 ||
        artifact.proposal_hash.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.weight_version,
                          artifact.signer_id, artifact.view_or_round,
                          artifact.slot_or_height};
    auto inserted = first_artifact_by_key.emplace(key, artifact);
    const SignedVoteEvidence& first = inserted.first->second;
    if (inserted.second || first.proposal_hash == artifact.proposal_hash) {
      continue;
    }
    if (!emitted_keys.insert(key).second) {
      continue;
    }
    StrongFaultRecord fault;
    fault.type = StrongFaultType::kDoubleVote;
    fault.validator_id = artifact.signer_id;
    fault.view_or_round = artifact.view_or_round;
    fault.slot_or_height = artifact.slot_or_height;
    fault.first_artifact_digest =
        std::min(first.proposal_hash, artifact.proposal_hash);
    fault.second_artifact_digest =
        std::max(first.proposal_hash, artifact.proposal_hash);
    faults.push_back(std::move(fault));
  }
  return faults;
}

std::vector<StrongFaultRecord> DetectInvalidQcProposalFaults(
    const std::vector<InvalidQcProposalEvidence>& invalid_qc_proposal_evidence) {
  std::vector<InvalidQcProposalEvidence> ordered =
      invalid_qc_proposal_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const InvalidQcProposalEvidence& lhs,
               const InvalidQcProposalEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.weight_version,
                              lhs.leader_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.proposal_hash,
                              lhs.invalid_reason) <
                     std::tie(rhs.protocol_id, rhs.weight_version,
                              rhs.leader_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.proposal_hash,
                              rhs.invalid_reason);
            });

  using ArtifactKey =
      std::tuple<std::string, uint64_t, int, int, int, std::string>;
  std::set<ArtifactKey> emitted_keys;
  std::vector<StrongFaultRecord> faults;
  for (const InvalidQcProposalEvidence& artifact : ordered) {
    if (!artifact.proposal_signature_verified || artifact.qc_verified ||
        artifact.protocol_id.empty() || artifact.leader_id <= 0 ||
        artifact.view_or_round <= 0 || artifact.proposal_hash.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.weight_version,
                          artifact.leader_id, artifact.view_or_round,
                          artifact.slot_or_height, artifact.proposal_hash};
    if (!emitted_keys.insert(key).second) {
      continue;
    }
    StrongFaultRecord fault;
    fault.type = StrongFaultType::kInvalidQcProposal;
    fault.validator_id = artifact.leader_id;
    fault.view_or_round = artifact.view_or_round;
    fault.slot_or_height = artifact.slot_or_height;
    fault.first_artifact_digest = artifact.proposal_hash;
    fault.second_artifact_digest =
        artifact.invalid_reason.empty() ? "invalid_qc" : artifact.invalid_reason;
    faults.push_back(std::move(fault));
  }
  return faults;
}

std::vector<StrongFaultRecord> DetectWeightUpdateVoteEquivocationFaults(
    const std::vector<SignedWeightUpdateVoteEvidence>&
        signed_weight_update_vote_evidence) {
  std::vector<SignedWeightUpdateVoteEvidence> ordered =
      signed_weight_update_vote_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const SignedWeightUpdateVoteEvidence& lhs,
               const SignedWeightUpdateVoteEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.old_weight_root,
                              lhs.old_weight_version, lhs.activation_view,
                              lhs.validator_id, lhs.candidate_digest) <
                     std::tie(rhs.protocol_id, rhs.old_weight_root,
                              rhs.old_weight_version, rhs.activation_view,
                              rhs.validator_id, rhs.candidate_digest);
            });

  using ArtifactKey = std::tuple<std::string, std::string, uint64_t, int, int>;
  std::map<ArtifactKey, SignedWeightUpdateVoteEvidence> first_artifact_by_key;
  std::set<ArtifactKey> emitted_keys;
  std::vector<StrongFaultRecord> faults;
  for (const SignedWeightUpdateVoteEvidence& artifact : ordered) {
    if (!artifact.signature_verified || artifact.protocol_id.empty() ||
        artifact.validator_id <= 0 || artifact.old_weight_root.empty() ||
        artifact.activation_view <= 0 || artifact.candidate_digest.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.old_weight_root,
                          artifact.old_weight_version, artifact.activation_view,
                          artifact.validator_id};
    auto inserted = first_artifact_by_key.emplace(key, artifact);
    const SignedWeightUpdateVoteEvidence& first = inserted.first->second;
    if (inserted.second ||
        first.candidate_digest == artifact.candidate_digest) {
      continue;
    }
    if (!emitted_keys.insert(key).second) {
      continue;
    }
    StrongFaultRecord fault;
    fault.type = StrongFaultType::kWeightUpdateVoteEquivocation;
    fault.validator_id = artifact.validator_id;
    fault.view_or_round = artifact.activation_view;
    fault.slot_or_height = 0;
    fault.first_artifact_digest =
        std::min(first.candidate_digest, artifact.candidate_digest);
    fault.second_artifact_digest =
        std::max(first.candidate_digest, artifact.candidate_digest);
    faults.push_back(std::move(fault));
  }
  return faults;
}

std::vector<StrongFaultRecord> DetectConflictingQcFaults(
    const std::vector<VerifiedQcArtifactEvidence>& verified_qc_artifact_evidence,
    int total_replicas) {
  std::vector<VerifiedQcArtifactEvidence> ordered =
      verified_qc_artifact_evidence;
  std::sort(ordered.begin(), ordered.end(),
            [](const VerifiedQcArtifactEvidence& lhs,
               const VerifiedQcArtifactEvidence& rhs) {
              return std::tie(lhs.protocol_id, lhs.weight_version,
                              lhs.view_or_round, lhs.slot_or_height,
                              lhs.qc_hash) <
                     std::tie(rhs.protocol_id, rhs.weight_version,
                              rhs.view_or_round, rhs.slot_or_height,
                              rhs.qc_hash);
            });

  using ArtifactKey = std::tuple<std::string, uint64_t, int, int>;
  std::map<ArtifactKey, VerifiedQcArtifactEvidence> first_artifact_by_key;
  std::set<std::tuple<ArtifactKey, int>> emitted_validators;
  std::vector<StrongFaultRecord> faults;
  for (const VerifiedQcArtifactEvidence& artifact : ordered) {
    if (!artifact.qc_verified || artifact.protocol_id.empty() ||
        artifact.view_or_round <= 0 || artifact.qc_hash.empty() ||
        artifact.signer_bitmap.empty()) {
      continue;
    }
    const ArtifactKey key{artifact.protocol_id, artifact.weight_version,
                          artifact.view_or_round, artifact.slot_or_height};
    auto inserted = first_artifact_by_key.emplace(key, artifact);
    const VerifiedQcArtifactEvidence& first = inserted.first->second;
    if (inserted.second || first.qc_hash == artifact.qc_hash) {
      continue;
    }

    const std::vector<int> first_signers =
        DecodeSignerBitmap(first.signer_bitmap, total_replicas);
    const std::vector<int> second_signers =
        DecodeSignerBitmap(artifact.signer_bitmap, total_replicas);
    std::set<int> first_signer_set(first_signers.begin(), first_signers.end());
    for (int signer : second_signers) {
      if (first_signer_set.find(signer) == first_signer_set.end()) {
        continue;
      }
      if (!emitted_validators.insert(std::make_tuple(key, signer)).second) {
        continue;
      }
      StrongFaultRecord fault;
      fault.type = StrongFaultType::kConflictingQc;
      fault.validator_id = signer;
      fault.view_or_round = artifact.view_or_round;
      fault.slot_or_height = artifact.slot_or_height;
      fault.first_artifact_digest = std::min(first.qc_hash, artifact.qc_hash);
      fault.second_artifact_digest = std::max(first.qc_hash, artifact.qc_hash);
      faults.push_back(std::move(fault));
    }
  }
  std::sort(faults.begin(), faults.end(),
            [](const StrongFaultRecord& lhs, const StrongFaultRecord& rhs) {
              return std::tie(lhs.validator_id, lhs.view_or_round,
                              lhs.slot_or_height, lhs.first_artifact_digest,
                              lhs.second_artifact_digest) <
                     std::tie(rhs.validator_id, rhs.view_or_round,
                              rhs.slot_or_height, rhs.first_artifact_digest,
                              rhs.second_artifact_digest);
            });
  return faults;
}

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
