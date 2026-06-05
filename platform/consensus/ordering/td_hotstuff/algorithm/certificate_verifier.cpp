#include "platform/consensus/ordering/td_hotstuff/algorithm/certificate_verifier.h"

#include <algorithm>
#include <cstdint>
#include <set>
#include <sstream>
#include <vector>

#include <glog/logging.h>

namespace resdb {
namespace td_hotstuff {
namespace {

void SetError(std::string* error, const std::string& message) {
  if (error != nullptr) {
    *error = message;
  }
}

std::string DigestProto(const google::protobuf::Message& message) {
  std::string bytes;
  message.SerializeToString(&bytes);
  return SignatureVerifier::CalculateHash(bytes);
}

std::string BuildBitmap(const std::vector<int>& signers, int total_replicas) {
  if (total_replicas <= 0) {
    return "";
  }
  std::string bitmap((total_replicas + 7) / 8, '\0');
  for (int signer : signers) {
    if (signer < 1 || signer > total_replicas) {
      continue;
    }
    const int bit = signer - 1;
    bitmap[bit / 8] =
        static_cast<char>(bitmap[bit / 8] | (1 << (bit % 8)));
  }
  return bitmap;
}

bool SameQcForTimeout(const QC& left, const QC& right) {
  return left.view() == right.view() && left.hash() == right.hash() &&
         left.slot() == right.slot() &&
         left.signer_bitmap() == right.signer_bitmap();
}

bool BetterHighQc(const QC& candidate, const QC& current) {
  if (candidate.view() != current.view()) {
    return candidate.view() > current.view();
  }
  return candidate.hash() > current.hash();
}

std::string VoteSignaturePayloadFor(int signer, int view, int slot,
                                    const std::string& hash) {
  std::string payload = "td_hotstuff_vote_v1|";
  payload += std::to_string(signer);
  payload += '|';
  payload += std::to_string(view);
  payload += '|';
  payload += std::to_string(slot);
  payload += '|';
  payload += hash;
  return payload;
}

}  // namespace

std::string ProposalSignaturePayload(const Proposal& proposal) {
  std::string payload = "td_hotstuff_proposal_v1|";
  payload += std::to_string(proposal.sender());
  payload += '|';
  payload += std::to_string(proposal.header().view());
  payload += '|';
  payload += proposal.hash();
  return payload;
}

std::string VoteSignaturePayload(const Certificate& vote) {
  return VoteSignaturePayloadFor(vote.signer(), vote.view(), vote.slot(),
                                 vote.hash());
}

std::string TimeoutVotePayload(const TimeoutVote& vote) {
  std::ostringstream payload;
  payload << "td_hotstuff_timeout_vote_v1|" << vote.view() << '|'
          << vote.signer() << '|' << vote.high_qc().view() << '|'
          << vote.high_qc().hash() << '|' << vote.high_qc().slot() << '|'
          << vote.high_qc().signer_bitmap() << '|'
          << DigestProto(vote.high_qc());
  return payload.str();
}

CertificateVerifier::CertificateVerifier(
    int total_replicas, SignatureVerifier* verifier,
    std::shared_ptr<WeightSchedule> weight_schedule)
    : total_replicas_(total_replicas),
      verifier_(verifier),
      weight_schedule_(std::move(weight_schedule)) {}

bool CertificateVerifier::VerifyProposalSignature(
    const Proposal& proposal, std::string* error) const {
  if (verifier_ == nullptr) {
    SetError(error, "missing verifier");
    return false;
  }
  if (!proposal.has_signature()) {
    SetError(error, "missing proposal signature");
    return false;
  }
  if (proposal.signature().node_id() != proposal.sender()) {
    SetError(error, "proposal signature sender mismatch");
    return false;
  }
  if (!verifier_->VerifyMessage(ProposalSignaturePayload(proposal),
                                proposal.signature())) {
    SetError(error, "bad proposal signature");
    return false;
  }
  return true;
}

bool CertificateVerifier::VerifyVote(const Certificate& vote,
                                     std::string* error) const {
  if (verifier_ == nullptr) {
    SetError(error, "missing verifier");
    return false;
  }
  if (vote.view() <= 0) {
    SetError(error, "invalid vote view");
    return false;
  }
  if (vote.signer() != vote.sign().node_id()) {
    SetError(error, "vote signer mismatch");
    return false;
  }
  if (WeightForSigner(vote.signer(), vote.view()) <= 0) {
    SetError(error, "vote signer out of range");
    return false;
  }
  if (!verifier_->VerifyMessage(
          VoteSignaturePayloadFor(vote.signer(), vote.view(), vote.slot(),
                                  vote.hash()),
          vote.sign())) {
    SetError(error, "bad vote signature");
    return false;
  }
  return true;
}

bool CertificateVerifier::VerifyQC(const QC& qc, std::string* error) const {
  if (qc.hash().empty()) {
    return true;
  }
  if (qc.view() <= 0) {
    SetError(error, "invalid qc view");
    return false;
  }
  const size_t expected_size =
      total_replicas_ <= 0 ? 0 : (total_replicas_ + 7) / 8;
  if (qc.signer_bitmap().size() != expected_size) {
    SetError(error, "qc signer bitmap size mismatch");
    return false;
  }

  std::vector<uint8_t> seen_signers(total_replicas_ + 1, 0);
  std::string signer_bitmap(expected_size, '\0');
  int64_t total_weight = 0;
  for (const SignatureInfo& signature : qc.signatures()) {
    const int signer = signature.node_id();
    if (signer < 1 || signer > total_replicas_) {
      SetError(error, "qc signer out of range");
      return false;
    }
    if (seen_signers[signer] != 0) {
      SetError(error, "duplicate qc signer");
      return false;
    }
    seen_signers[signer] = 1;
    const int64_t signer_weight = WeightForSigner(signer, qc.view());
    if (signer_weight <= 0) {
      SetError(error, "qc signer has zero weight");
      return false;
    }
    if (!verifier_->VerifyMessage(
            VoteSignaturePayloadFor(signer, qc.view(), qc.slot(), qc.hash()),
            signature)) {
      SetError(error, "bad vote signature");
      return false;
    }
    const int bit = signer - 1;
    signer_bitmap[bit / 8] =
        static_cast<char>(signer_bitmap[bit / 8] | (1 << (bit % 8)));
    total_weight += signer_weight;
  }
  if (qc.signer_bitmap() != signer_bitmap) {
    SetError(error, "qc signer bitmap mismatch");
    return false;
  }
  if (total_weight < QuorumWeightForView(qc.view())) {
    SetError(error, "insufficient qc quorum");
    return false;
  }
  return true;
}

bool CertificateVerifier::VerifyTimeoutVote(const TimeoutVote& vote,
                                            std::string* error) const {
  if (verifier_ == nullptr) {
    SetError(error, "missing verifier");
    return false;
  }
  if (vote.view() <= 0) {
    SetError(error, "invalid timeout vote view");
    return false;
  }
  if (vote.signer() != vote.signature().node_id()) {
    SetError(error, "timeout vote signer mismatch");
    return false;
  }
  if (WeightForSigner(vote.signer(), vote.view()) <= 0) {
    SetError(error, "timeout vote signer out of range");
    return false;
  }
  if (!vote.high_qc().hash().empty()) {
    if (vote.high_qc().view() >= vote.view()) {
      SetError(error, "timeout high qc is not lower than timeout view");
      return false;
    }
    if (!VerifyQC(vote.high_qc(), error)) {
      return false;
    }
  }
  if (!verifier_->VerifyMessage(TimeoutVotePayload(vote), vote.signature())) {
    SetError(error, "bad timeout vote signature");
    return false;
  }
  return true;
}

bool CertificateVerifier::VerifyTimeoutCert(const TimeoutCert& cert,
                                            std::string* error) const {
  if (cert.view() <= 0) {
    SetError(error, "invalid timeout cert view");
    return false;
  }
  if (cert.quorum_rule_id() != kTimeoutQuorumRuleId) {
    SetError(error, "wrong timeout quorum rule");
    return false;
  }
  if (!cert.high_qc().hash().empty()) {
    if (cert.high_qc().view() >= cert.view()) {
      SetError(error, "timeout cert high qc is not lower than cert view");
      return false;
    }
    if (!VerifyQC(cert.high_qc(), error)) {
      return false;
    }
  }

  std::set<int> seen_signers;
  std::vector<int> signers;
  int64_t total_weight = 0;
  QC best_high_qc;
  for (const TimeoutVote& vote : cert.votes()) {
    if (vote.view() != cert.view()) {
      SetError(error, "timeout cert vote view mismatch");
      return false;
    }
    if (!seen_signers.insert(vote.signer()).second) {
      SetError(error, "duplicate timeout vote signer");
      return false;
    }
    if (!VerifyTimeoutVote(vote, error)) {
      return false;
    }
    signers.push_back(vote.signer());
    total_weight += WeightForSigner(vote.signer(), cert.view());
    if (BetterHighQc(vote.high_qc(), best_high_qc)) {
      best_high_qc = vote.high_qc();
    }
  }
  std::sort(signers.begin(), signers.end());
  if (cert.signer_bitmap() != BuildBitmap(signers, total_replicas_)) {
    SetError(error, "timeout cert signer bitmap mismatch");
    return false;
  }
  if (total_weight < QuorumWeightForView(cert.view())) {
    SetError(error, "insufficient timeout cert quorum");
    return false;
  }
  if (!SameQcForTimeout(cert.high_qc(), best_high_qc)) {
    SetError(error, "timeout cert high qc mismatch");
    return false;
  }
  return true;
}

int64_t CertificateVerifier::WeightForSigner(int signer, int view) const {
  if (weight_schedule_ != nullptr) {
    return weight_schedule_->WeightForSigner(signer, view);
  }
  return signer >= 1 && signer <= total_replicas_ ? 1 : 0;
}

int64_t CertificateVerifier::QuorumWeightForView(int view) const {
  if (weight_schedule_ != nullptr) {
    return weight_schedule_->QuorumWeightForView(view);
  }
  return total_replicas_ * 2 / 3 + 1;
}

}  // namespace td_hotstuff
}  // namespace resdb
