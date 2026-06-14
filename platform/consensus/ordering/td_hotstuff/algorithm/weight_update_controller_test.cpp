#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_controller.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <cstdlib>
#include <memory>
#include <vector>

#include "common/crypto/mock_signature_verifier.h"
#include "platform/consensus/reputation/reputation_roots.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff_digest.h"

namespace resdb {
namespace td_hotstuff {
namespace {

using ::testing::_;
using ::testing::Return;

SignatureInfo SignatureFor(int signer) {
  SignatureInfo signature;
  signature.set_node_id(signer);
  signature.set_signature("sig-" + std::to_string(signer));
  return signature;
}

resdb::consensus::reputation::ReputationCandidate Candidate() {
  resdb::consensus::reputation::ReputationCandidate candidate;
  candidate.total_replicas = 4;
  candidate.window_index = 0;
  candidate.start_view = 0;
  candidate.end_view = 4;
  candidate.event_count = 4;
  candidate.old_weight_version = 0;
  candidate.old_weight_root_hex =
      resdb::td_hotstuff::WeightRootHex({1, 1, 1, 1});
  candidate.activation_view = 8;
  for (int i = 1; i <= 4; ++i) {
    resdb::consensus::reputation::ValidatorReputation validator;
    validator.validator_id = i;
    validator.current_weight = 1;
    validator.next_weight = i == 4 ? 1 : 20;
    candidate.validators.push_back(validator);
  }
  resdb::consensus::reputation::RecomputeReputationCandidateRoots(&candidate);
  return candidate;
}


resdb::consensus::reputation::ReputationCandidate NoOpCandidate() {
  auto candidate = Candidate();
  for (auto& validator : candidate.validators) {
    validator.next_weight = 1;
  }
  resdb::consensus::reputation::RecomputeReputationCandidateRoots(&candidate);
  return candidate;
}

resdb::consensus::reputation::ReputationCandidate LeaderOnlyCandidate() {
  auto candidate = NoOpCandidate();
  candidate.leader_weights = {80, 100, 100, 100};
  candidate.leader_selection_version = 1;
  candidate.leader_eligible_min_weight = 10;
  candidate.leader_weight_root_hex =
      resdb::consensus::reputation::LeaderWeightRootHex(
          candidate.leader_weights, candidate.leader_eligible_min_weight,
          candidate.leader_selection_version);
  resdb::consensus::reputation::RecomputeReputationCandidateRoots(&candidate);
  return candidate;
}

resdb::consensus::reputation::ReputationCandidate CandidateWithWeights(
    const std::vector<int64_t>& current_weights,
    const std::vector<int64_t>& next_weights, uint64_t old_weight_version,
    const std::string& old_weight_root, int activation_view) {
  resdb::consensus::reputation::ReputationCandidate candidate;
  candidate.total_replicas = static_cast<int>(current_weights.size());
  candidate.window_index = static_cast<int>(old_weight_version);
  candidate.start_view = static_cast<int>(old_weight_version) * 4;
  candidate.end_view = candidate.start_view + 4;
  candidate.event_count = 4;
  candidate.old_weight_version = old_weight_version;
  candidate.old_weight_root_hex = old_weight_root;
  candidate.activation_view = activation_view;
  for (size_t i = 0; i < current_weights.size(); ++i) {
    resdb::consensus::reputation::ValidatorReputation validator;
    validator.validator_id = static_cast<int>(i) + 1;
    validator.current_weight = current_weights[i];
    validator.next_weight = next_weights[i];
    candidate.validators.push_back(validator);
  }
  resdb::consensus::reputation::RecomputeReputationCandidateRoots(&candidate);
  return candidate;
}

CandidateWeightUpdate CandidateMessage(
    const resdb::consensus::reputation::ReputationCandidate& candidate) {
  return ToCandidateWeightUpdate(candidate);
}


TEST(WeightUpdateControllerTest,
     ConflictingWeightUpdateVotePreservesScopeAndSignsDifferentDigest) {
  WeightUpdateVote vote;
  vote.set_signer(2);
  vote.set_old_weight_root("old-root");
  vote.set_old_weight_version(7);
  vote.set_activation_view(64);
  vote.set_candidate_digest("candidate-a");
  *vote.mutable_signature() = SignatureFor(2);

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(2)));

  std::unique_ptr<WeightUpdateVote> conflicting =
      MakeConflictingWeightUpdateVoteForExperiment(vote, /*node_id=*/2,
                                                   &verifier);

  ASSERT_NE(conflicting, nullptr);
  EXPECT_EQ(conflicting->signer(), vote.signer());
  EXPECT_EQ(conflicting->old_weight_root(), vote.old_weight_root());
  EXPECT_EQ(conflicting->old_weight_version(), vote.old_weight_version());
  EXPECT_EQ(conflicting->activation_view(), vote.activation_view());
  EXPECT_NE(conflicting->candidate_digest(), vote.candidate_digest());
  EXPECT_EQ(conflicting->signature().node_id(), 2);
}

TEST(WeightUpdateControllerTest, IgnoresNoOpLocalCandidate) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = NoOpCandidate();

  EXPECT_FALSE(controller.AddLocalCandidate(candidate));
  EXPECT_EQ(controller.HandleCandidate(CandidateMessage(candidate)), nullptr);
}

TEST(WeightUpdateControllerTest, AllowsNoOpLocalCandidateForExperiment) {
  setenv("TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_CANDIDATE", "1", /*overwrite=*/1);
  auto schedule = std::make_shared<WeightSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(1)));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = NoOpCandidate();

  EXPECT_TRUE(controller.AddLocalCandidate(candidate));
  std::unique_ptr<WeightUpdateVote> vote =
      controller.HandleCandidate(CandidateMessage(candidate));
  unsetenv("TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_CANDIDATE");

  ASSERT_NE(vote, nullptr);
  EXPECT_EQ(vote->signer(), 1);
  EXPECT_EQ(vote->candidate_digest(), candidate.candidate_digest_hex);
}

TEST(WeightUpdateControllerTest,
     CertifiesLeaderOnlyLocalCandidateWhenLeaderSelectionChanges) {
  auto schedule = std::make_shared<WeightSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier, leader_schedule);
  auto candidate = LeaderOnlyCandidate();

  ASSERT_TRUE(controller.AddLocalCandidate(candidate));
  std::unique_ptr<WeightUpdateVote> local_vote =
      controller.HandleCandidate(CandidateMessage(candidate));
  ASSERT_NE(local_vote, nullptr);
  EXPECT_EQ(local_vote->candidate_digest(), candidate.candidate_digest_hex);

  std::unique_ptr<WeightUpdateCert> cert;
  for (int signer : {1, 2, 3}) {
    WeightUpdateVote vote;
    vote.set_signer(signer);
    vote.set_old_weight_root(candidate.old_weight_root_hex);
    vote.set_old_weight_version(candidate.old_weight_version);
    vote.set_activation_view(candidate.activation_view);
    vote.set_candidate_digest(candidate.candidate_digest_hex);
    *vote.mutable_signature() = SignatureFor(signer);
    cert = controller.HandleVote(vote);
  }
  ASSERT_NE(cert, nullptr);
  EXPECT_TRUE(controller.HandleCert(*cert));
  EXPECT_TRUE(controller.ActivateReady(candidate.activation_view + 1));
  EXPECT_EQ(schedule->ActiveWeights(), (std::vector<int64_t>{1, 1, 1, 1}));
  EXPECT_EQ(leader_schedule->ActiveLeaderWeights(),
            (std::vector<int64_t>{80, 100, 100, 100}));
}

TEST(WeightUpdateControllerTest, IgnoresTrueNoOpLocalCandidateWithLeaderSelection) {
  auto schedule = std::make_shared<WeightSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier, leader_schedule);
  auto candidate = NoOpCandidate();
  candidate.leader_weights = {100, 100, 100, 100};
  candidate.leader_selection_version = 1;
  candidate.leader_eligible_min_weight = 10;
  resdb::consensus::reputation::RecomputeReputationCandidateRoots(&candidate);

  EXPECT_FALSE(controller.AddLocalCandidate(candidate));
  EXPECT_EQ(controller.HandleCandidate(CandidateMessage(candidate)), nullptr);
}

TEST(WeightUpdateControllerTest, VotesOnlyForMatchingLocalCandidate) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(1)));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = Candidate();
  controller.AddLocalCandidate(candidate);

  std::unique_ptr<WeightUpdateVote> vote = controller.HandleCandidate(
      CandidateMessage(candidate));

  ASSERT_NE(vote, nullptr);
  EXPECT_EQ(vote->signer(), 1);
  EXPECT_EQ(vote->candidate_digest(), candidate.candidate_digest_hex);

  CandidateWeightUpdate wrong = CandidateMessage(candidate);
  wrong.set_candidate_digest("wrong");
  EXPECT_EQ(controller.HandleCandidate(wrong), nullptr);

  CandidateWeightUpdate wrong_leader_root = CandidateMessage(candidate);
  wrong_leader_root.set_leader_weight_root("wrong");
  EXPECT_EQ(controller.HandleCandidate(wrong_leader_root), nullptr);
}

TEST(WeightUpdateControllerTest, RejectsLeaderEpochScheduleRootMismatch) {
  auto schedule = std::make_shared<WeightSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier, leader_schedule);
  auto candidate = LeaderOnlyCandidate();
  ASSERT_TRUE(controller.AddLocalCandidate(candidate));

  CandidateWeightUpdate message = CandidateMessage(candidate);
  message.set_leader_schedule_root("forged-root");
  EXPECT_EQ(controller.HandleCandidate(message), nullptr);
}

TEST(WeightUpdateControllerTest, ActivatesCertifiedExplicitLeaderEpochSchedule) {
  auto schedule = std::make_shared<WeightSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier, leader_schedule);
  auto candidate = LeaderOnlyCandidate();
  ASSERT_GE(static_cast<int>(candidate.leader_epoch_leaders.size()), 4);

  WeightUpdateCert cert;
  *cert.mutable_candidate() = CandidateMessage(candidate);
  cert.set_signer_bitmap(std::string(1, static_cast<char>(0x07)));
  cert.set_quorum_rule_id("old_weight_quorum_v1");
  for (int signer : {1, 2, 3}) {
    WeightUpdateVote* vote = cert.add_votes();
    vote->set_signer(signer);
    vote->set_old_weight_root(candidate.old_weight_root_hex);
    vote->set_old_weight_version(candidate.old_weight_version);
    vote->set_activation_view(candidate.activation_view);
    vote->set_candidate_digest(candidate.candidate_digest_hex);
    *vote->mutable_signature() = SignatureFor(signer);
  }

  EXPECT_TRUE(controller.HandleCert(cert));
  EXPECT_EQ(schedule->ActiveWeightVersion(), 0);
  EXPECT_EQ(leader_schedule->ActiveLeaderVersion(), 0);
  for (int offset = 0; offset < 4; ++offset) {
    const int view = candidate.activation_view + offset;
    const int expected = candidate.leader_epoch_leaders[
        offset % candidate.leader_epoch_leaders.size()];
    EXPECT_EQ(leader_schedule->LeaderForView(view), expected) << view;
  }
  EXPECT_TRUE(controller.ActivateReady(candidate.activation_view));
  EXPECT_EQ(schedule->ActiveWeights(), (std::vector<int64_t>{1, 1, 1, 1}));
  EXPECT_EQ(leader_schedule->ActiveLeaderWeights(),
            (std::vector<int64_t>{80, 100, 100, 100}));
  for (int offset = 0; offset < 4; ++offset) {
    const int view = candidate.activation_view + offset;
    const int expected = candidate.leader_epoch_leaders[
        offset % candidate.leader_epoch_leaders.size()];
    EXPECT_EQ(leader_schedule->LeaderForView(view), expected) << view;
  }
}

TEST(WeightUpdateControllerTest, FormsCertAfterOldWeightQuorumVotes) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = Candidate();
  CandidateWeightUpdate message = CandidateMessage(candidate);
  controller.AddLocalCandidate(candidate);
  ASSERT_NE(controller.HandleCandidate(message), nullptr);

  std::unique_ptr<WeightUpdateCert> cert;
  for (int signer : {1, 2, 3}) {
    WeightUpdateVote vote;
    vote.set_signer(signer);
    vote.set_old_weight_root(candidate.old_weight_root_hex);
    vote.set_old_weight_version(candidate.old_weight_version);
    vote.set_activation_view(candidate.activation_view);
    vote.set_candidate_digest(candidate.candidate_digest_hex);
    *vote.mutable_signature() = SignatureFor(signer);
    cert = controller.HandleVote(vote);
  }

  ASSERT_NE(cert, nullptr);
  EXPECT_EQ(cert->candidate().candidate_digest(), candidate.candidate_digest_hex);
  EXPECT_FALSE(cert->signer_bitmap().empty());
}




TEST(WeightUpdateControllerTest, CertifiedPendingWeightsAffectLookupAtBoundary) {
  WeightSchedule schedule(/*total_replicas=*/4,
                          std::vector<int64_t>{100, 1, 1, 1});
  const std::string old_root = schedule.ActiveWeightRoot();
  ASSERT_TRUE(schedule.ScheduleUpdate(
      /*activation_view=*/5, std::vector<int64_t>{1, 100, 100, 1},
      old_root, schedule.ActiveWeightVersion()));

  EXPECT_EQ(schedule.WeightForSigner(1, /*view=*/4), 100);
  EXPECT_EQ(schedule.QuorumWeightForView(/*view=*/4), 69);
  EXPECT_EQ(schedule.WeightForSigner(1, /*view=*/5), 1);
  EXPECT_EQ(schedule.QuorumWeightForView(/*view=*/5), 135);
  ASSERT_TRUE(schedule.ActivateUpTo(/*current_view=*/5));
  EXPECT_EQ(schedule.WeightForSigner(1, /*view=*/5), 1);
  EXPECT_EQ(schedule.QuorumWeightForView(/*view=*/5), 135);
}

TEST(WeightUpdateControllerTest, CertQuorumUsesCandidateOldWeightVersion) {
  auto schedule = std::make_shared<WeightSchedule>(
      4, std::vector<int64_t>{100, 1, 1, 1});
  ASSERT_TRUE(schedule->ScheduleUpdate(
      /*activation_view=*/5, std::vector<int64_t>{1, 100, 100, 1},
      schedule->ActiveWeightRoot(), schedule->ActiveWeightVersion()));
  ASSERT_TRUE(schedule->ActivateUpTo(/*current_view=*/6));

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = CandidateWithWeights(
      schedule->ActiveWeights(), std::vector<int64_t>{1, 100, 100, 20},
      schedule->ActiveWeightVersion(), schedule->ActiveWeightRoot(),
      /*activation_view=*/8);
  CandidateWeightUpdate message = CandidateMessage(candidate);
  controller.AddLocalCandidate(candidate);

  std::unique_ptr<WeightUpdateVote> local_vote = controller.HandleCandidate(message);
  ASSERT_NE(local_vote, nullptr);
  EXPECT_EQ(controller.HandleVote(*local_vote), nullptr)
      << "signer 1 had quorum in version 0, but not in candidate old version 1";

  std::unique_ptr<WeightUpdateCert> cert;
  for (int signer : {2, 3}) {
    WeightUpdateVote vote;
    vote.set_signer(signer);
    vote.set_old_weight_root(candidate.old_weight_root_hex);
    vote.set_old_weight_version(candidate.old_weight_version);
    vote.set_activation_view(candidate.activation_view);
    vote.set_candidate_digest(candidate.candidate_digest_hex);
    *vote.mutable_signature() = SignatureFor(signer);
    cert = controller.HandleVote(vote);
  }

  ASSERT_NE(cert, nullptr);
  EXPECT_EQ(cert->candidate().old_weight_version(), 1);
}

TEST(WeightUpdateControllerTest, PendingVotesContributeAfterCandidateArrives) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = Candidate();
  CandidateWeightUpdate message = CandidateMessage(candidate);

  for (int signer : {2, 3}) {
    WeightUpdateVote vote;
    vote.set_signer(signer);
    vote.set_old_weight_root(candidate.old_weight_root_hex);
    vote.set_old_weight_version(candidate.old_weight_version);
    vote.set_activation_view(candidate.activation_view);
    vote.set_candidate_digest(candidate.candidate_digest_hex);
    *vote.mutable_signature() = SignatureFor(signer);
    EXPECT_EQ(controller.HandleVote(vote), nullptr);
  }

  controller.AddLocalCandidate(candidate);
  std::unique_ptr<WeightUpdateVote> local_vote = controller.HandleCandidate(message);
  ASSERT_NE(local_vote, nullptr);
  std::unique_ptr<WeightUpdateCert> cert = controller.HandleVote(*local_vote);
  ASSERT_NE(cert, nullptr);
  EXPECT_EQ(cert->candidate().candidate_digest(), candidate.candidate_digest_hex);
}

TEST(WeightUpdateControllerTest,
     ObservedRemoteCandidateDoesNotVoteButCanVerifyQuorumVotes) {
  auto schedule = std::make_shared<WeightSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = Candidate();
  CandidateWeightUpdate message = CandidateMessage(candidate);

  EXPECT_EQ(controller.HandleCandidate(message), nullptr)
      << "observing a remote candidate must not sign without local plugin match";
  std::unique_ptr<WeightUpdateCert> observed_cert;
  EXPECT_TRUE(controller.ObserveCandidate(message, &observed_cert));
  EXPECT_EQ(observed_cert, nullptr);

  std::unique_ptr<WeightUpdateCert> cert;
  for (int signer : {1, 2, 3}) {
    WeightUpdateVote vote;
    vote.set_signer(signer);
    vote.set_old_weight_root(candidate.old_weight_root_hex);
    vote.set_old_weight_version(candidate.old_weight_version);
    vote.set_activation_view(candidate.activation_view);
    vote.set_candidate_digest(candidate.candidate_digest_hex);
    *vote.mutable_signature() = SignatureFor(signer);
    cert = controller.HandleVote(vote);
  }

  ASSERT_NE(cert, nullptr);
  EXPECT_EQ(cert->candidate().candidate_digest(), candidate.candidate_digest_hex);
  EXPECT_TRUE(controller.HandleCert(*cert));
}

TEST(WeightUpdateControllerTest,
     PendingVotesCanCertifyWhenLocalCandidateArrives) {
  auto schedule = std::make_shared<WeightSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = Candidate();

  for (int signer : {1, 2, 3}) {
    WeightUpdateVote vote;
    vote.set_signer(signer);
    vote.set_old_weight_root(candidate.old_weight_root_hex);
    vote.set_old_weight_version(candidate.old_weight_version);
    vote.set_activation_view(candidate.activation_view);
    vote.set_candidate_digest(candidate.candidate_digest_hex);
    *vote.mutable_signature() = SignatureFor(signer);
    EXPECT_EQ(controller.HandleVote(vote), nullptr);
  }

  std::unique_ptr<WeightUpdateCert> cert =
      controller.AddLocalCandidateAndMaybeCert(candidate);

  ASSERT_NE(cert, nullptr);
  EXPECT_EQ(cert->candidate().candidate_digest(), candidate.candidate_digest_hex);
  EXPECT_TRUE(controller.HandleCert(*cert));
}

TEST(WeightUpdateControllerTest, ActivatesCertifiedWeightsAtBoundary) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier, leader_schedule);
  auto candidate = Candidate();
  WeightUpdateCert cert;
  *cert.mutable_candidate() = CandidateMessage(candidate);
  cert.set_signer_bitmap(std::string(1, static_cast<char>(0x07)));
  cert.set_quorum_rule_id("old_weight_quorum_v1");
  for (int signer : {1, 2, 3}) {
    WeightUpdateVote* vote = cert.add_votes();
    vote->set_signer(signer);
    vote->set_old_weight_root(candidate.old_weight_root_hex);
    vote->set_old_weight_version(candidate.old_weight_version);
    vote->set_activation_view(candidate.activation_view);
    vote->set_candidate_digest(candidate.candidate_digest_hex);
    *vote->mutable_signature() = SignatureFor(signer);
  }

  EXPECT_TRUE(controller.HandleCert(cert));
  EXPECT_EQ(controller.EarliestPendingActivationView(), candidate.activation_view);
  EXPECT_FALSE(controller.HandleCert(cert));
  EXPECT_FALSE(controller.ActivateReady(/*current_view=*/7));
  EXPECT_EQ(controller.EarliestPendingActivationView(), candidate.activation_view);
  EXPECT_TRUE(controller.ActivateReady(/*current_view=*/8));
  EXPECT_EQ(controller.EarliestPendingActivationView(), 0);
  EXPECT_EQ(schedule->ActiveWeightVersion(), 1);
  EXPECT_EQ(schedule->ActiveWeights(), candidate.next_weights);
  EXPECT_EQ(leader_schedule->ActiveLeaderVersion(), 1);
  EXPECT_TRUE(leader_schedule->ContextHashForView(8).empty());
  for (int view = 8; view < 80; ++view) {
    EXPECT_NE(4, leader_schedule->LeaderForView(view));
  }
}

TEST(WeightUpdateControllerTest,
     LatestCertForProposalReturnsAcceptedCertBeforeAndAfterActivation) {
  auto schedule = std::make_shared<WeightSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier, leader_schedule);
  auto candidate = Candidate();
  WeightUpdateCert cert;
  *cert.mutable_candidate() = CandidateMessage(candidate);
  cert.set_signer_bitmap(std::string(1, static_cast<char>(0x07)));
  cert.set_quorum_rule_id("old_weight_quorum_v1");
  for (int signer : {1, 2, 3}) {
    WeightUpdateVote* vote = cert.add_votes();
    vote->set_signer(signer);
    vote->set_old_weight_root(candidate.old_weight_root_hex);
    vote->set_old_weight_version(candidate.old_weight_version);
    vote->set_activation_view(candidate.activation_view);
    vote->set_candidate_digest(candidate.candidate_digest_hex);
    *vote->mutable_signature() = SignatureFor(signer);
  }

  ASSERT_TRUE(controller.HandleCert(cert));
  std::unique_ptr<WeightUpdateCert> pending =
      controller.LatestCertForProposal(candidate.activation_view);
  ASSERT_NE(pending, nullptr);
  EXPECT_EQ(pending->candidate().candidate_digest(), candidate.candidate_digest_hex);
  ASSERT_TRUE(controller.ActivateReady(candidate.activation_view));
  std::unique_ptr<WeightUpdateCert> recent =
      controller.LatestCertForProposal(candidate.activation_view + 1);
  ASSERT_NE(recent, nullptr);
  EXPECT_EQ(recent->candidate().candidate_digest(), candidate.candidate_digest_hex);
}

TEST(WeightUpdateControllerTest, InvalidCertDoesNotPoisonLaterValidCert) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier);
  auto candidate = Candidate();
  WeightUpdateCert cert;
  *cert.mutable_candidate() = CandidateMessage(candidate);
  cert.set_quorum_rule_id("old_weight_quorum_v1");
  for (int signer : {1, 2, 3}) {
    WeightUpdateVote* vote = cert.add_votes();
    vote->set_signer(signer);
    vote->set_old_weight_root(candidate.old_weight_root_hex);
    vote->set_old_weight_version(candidate.old_weight_version);
    vote->set_activation_view(candidate.activation_view);
    vote->set_candidate_digest(candidate.candidate_digest_hex);
    *vote->mutable_signature() = SignatureFor(signer);
  }

  WeightUpdateCert invalid = cert;
  invalid.set_signer_bitmap(std::string(1, static_cast<char>(0x03)));
  EXPECT_FALSE(controller.HandleCert(invalid));

  cert.set_signer_bitmap(std::string(1, static_cast<char>(0x07)));
  EXPECT_TRUE(controller.HandleCert(cert));
}

TEST(WeightUpdateControllerTest, LateCertActivatesAtCertifiedBoundary) {
  auto schedule = std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{1, 1, 1, 1}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));
  WeightUpdateController controller(/*node_id=*/1, /*total_replicas=*/4,
                                    schedule, &verifier, leader_schedule);
  auto candidate = Candidate();
  WeightUpdateCert cert;
  *cert.mutable_candidate() = CandidateMessage(candidate);
  cert.set_signer_bitmap(std::string(1, static_cast<char>(0x07)));
  cert.set_quorum_rule_id("old_weight_quorum_v1");
  for (int signer : {1, 2, 3}) {
    WeightUpdateVote* vote = cert.add_votes();
    vote->set_signer(signer);
    vote->set_old_weight_root(candidate.old_weight_root_hex);
    vote->set_old_weight_version(candidate.old_weight_version);
    vote->set_activation_view(candidate.activation_view);
    vote->set_candidate_digest(candidate.candidate_digest_hex);
    *vote->mutable_signature() = SignatureFor(signer);
  }

  EXPECT_TRUE(controller.HandleCert(cert));
  EXPECT_TRUE(controller.ActivateReady(/*current_view=*/10));
  EXPECT_EQ(schedule->ActiveWeightVersion(), 1);
  EXPECT_EQ(schedule->ActiveWeights(), candidate.next_weights);
  EXPECT_EQ(leader_schedule->ActiveLeaderVersion(), 1);
  EXPECT_EQ(leader_schedule->ActiveLeaderWeights(), candidate.leader_weights);
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
