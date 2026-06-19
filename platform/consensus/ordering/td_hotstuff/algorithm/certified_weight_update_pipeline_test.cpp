#include "platform/consensus/ordering/td_hotstuff/algorithm/certified_weight_update_pipeline.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "common/crypto/mock_signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff_digest.h"
#include "platform/consensus/reputation/reputation_roots.h"

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
  candidate.old_weight_root_hex = WeightRootHex({1, 1, 1, 1});
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

WeightUpdateVote VoteFor(
    const resdb::consensus::reputation::ReputationCandidate& candidate,
    int signer) {
  WeightUpdateVote vote;
  vote.set_signer(signer);
  vote.set_old_weight_root(candidate.old_weight_root_hex);
  vote.set_old_weight_version(candidate.old_weight_version);
  vote.set_activation_view(candidate.activation_view);
  vote.set_candidate_digest(candidate.candidate_digest_hex);
  *vote.mutable_signature() = SignatureFor(signer);
  return vote;
}

TEST(CertifiedWeightUpdatePipelineTest,
     DrainCompletedCandidateBroadcastsCandidateAndVoteOncePerActiveVersion) {
  auto weight_schedule =
      std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  std::vector<CandidateWeightUpdate> broadcast_candidates;
  std::vector<WeightUpdateVote> broadcast_votes;
  std::vector<WeightUpdateCert> broadcast_certs;
  CertifiedWeightUpdatePipeline::Callbacks callbacks;
  callbacks.current_view = [] { return 0; };
  callbacks.broadcast_candidate =
      [&](const CandidateWeightUpdate& candidate) {
        broadcast_candidates.push_back(candidate);
      };
  callbacks.broadcast_vote = [&](const WeightUpdateVote& vote) {
    broadcast_votes.push_back(vote);
  };
  callbacks.broadcast_cert = [&](const WeightUpdateCert& cert) {
    broadcast_certs.push_back(cert);
  };

  CertifiedWeightUpdatePipeline pipeline(
      /*node_id=*/1, /*total_replicas=*/4, weight_schedule, leader_schedule,
      &verifier, /*reputation_adapter=*/nullptr, std::move(callbacks));

  const auto candidate = Candidate();
  pipeline.DrainCompletedCandidatesForTesting({candidate});
  pipeline.DrainCompletedCandidatesForTesting({candidate});

  ASSERT_EQ(broadcast_candidates.size(), 1);
  ASSERT_EQ(broadcast_votes.size(), 1);
  EXPECT_TRUE(broadcast_certs.empty());
  EXPECT_EQ(broadcast_candidates.front().candidate_digest(),
            candidate.candidate_digest_hex);
  EXPECT_EQ(broadcast_votes.front().candidate_digest(),
            candidate.candidate_digest_hex);
  EXPECT_EQ(broadcast_votes.front().signer(), 1);
}

TEST(CertifiedWeightUpdatePipelineTest,
     RemoteCandidateWithoutLocalDigestIsProcessedButNotVoted) {
  auto weight_schedule =
      std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;

  std::vector<CandidateWeightUpdate> broadcast_candidates;
  std::vector<WeightUpdateVote> broadcast_votes;
  std::vector<WeightUpdateCert> broadcast_certs;
  CertifiedWeightUpdatePipeline::Callbacks callbacks;
  callbacks.current_view = [] { return 0; };
  callbacks.broadcast_candidate =
      [&](const CandidateWeightUpdate& candidate) {
        broadcast_candidates.push_back(candidate);
      };
  callbacks.broadcast_vote = [&](const WeightUpdateVote& vote) {
    broadcast_votes.push_back(vote);
  };
  callbacks.broadcast_cert = [&](const WeightUpdateCert& cert) {
    broadcast_certs.push_back(cert);
  };

  CertifiedWeightUpdatePipeline pipeline(
      /*node_id=*/1, /*total_replicas=*/4, weight_schedule, leader_schedule,
      &verifier, /*reputation_adapter=*/nullptr, std::move(callbacks));

  auto candidate = std::make_unique<CandidateWeightUpdate>(
      ToCandidateWeightUpdate(Candidate()));

  EXPECT_TRUE(pipeline.ReceiveCandidate(std::move(candidate)));
  EXPECT_TRUE(broadcast_candidates.empty());
  EXPECT_TRUE(broadcast_votes.empty());
  EXPECT_TRUE(broadcast_certs.empty());
  EXPECT_EQ(weight_schedule->ActiveWeights(),
            (std::vector<int64_t>{1, 1, 1, 1}));
}

TEST(CertifiedWeightUpdatePipelineTest,
     WeightUpdateVoteEquivocationDoesNotVoteForRemoteCandidateByDefault) {
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION", "1", /*overwrite=*/1);
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_ON_CANDIDATE");

  auto weight_schedule =
      std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).Times(0);

  std::vector<WeightUpdateVote> broadcast_votes;
  CertifiedWeightUpdatePipeline::Callbacks callbacks;
  callbacks.current_view = [] { return 0; };
  callbacks.broadcast_vote = [&](const WeightUpdateVote& vote) {
    broadcast_votes.push_back(vote);
  };

  CertifiedWeightUpdatePipeline pipeline(
      /*node_id=*/1, /*total_replicas=*/4, weight_schedule, leader_schedule,
      &verifier, /*reputation_adapter=*/nullptr, std::move(callbacks));

  EXPECT_TRUE(pipeline.ReceiveCandidate(
      std::make_unique<CandidateWeightUpdate>(
          ToCandidateWeightUpdate(Candidate()))));
  EXPECT_TRUE(broadcast_votes.empty());

  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION");
}

TEST(CertifiedWeightUpdatePipelineTest,
     DuplicateObservedCertIsNotRebroadcast) {
  auto weight_schedule =
      std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  std::vector<WeightUpdateCert> broadcast_certs;
  CertifiedWeightUpdatePipeline::Callbacks callbacks;
  callbacks.current_view = [] { return 0; };
  callbacks.broadcast_cert = [&](const WeightUpdateCert& cert) {
    broadcast_certs.push_back(cert);
  };

  CertifiedWeightUpdatePipeline pipeline(
      /*node_id=*/1, /*total_replicas=*/4, weight_schedule, leader_schedule,
      &verifier, /*reputation_adapter=*/nullptr, std::move(callbacks));

  const auto candidate = Candidate();
  for (int signer : {1, 2, 3}) {
    EXPECT_TRUE(
        pipeline.ReceiveVote(std::make_unique<WeightUpdateVote>(
            VoteFor(candidate, signer))));
  }

  EXPECT_TRUE(pipeline.ReceiveCandidate(
      std::make_unique<CandidateWeightUpdate>(
          ToCandidateWeightUpdate(candidate))));
  EXPECT_TRUE(pipeline.ReceiveCandidate(
      std::make_unique<CandidateWeightUpdate>(
          ToCandidateWeightUpdate(candidate))));

  ASSERT_EQ(broadcast_certs.size(), 1);
  EXPECT_EQ(broadcast_certs.front().candidate().candidate_digest(),
            candidate.candidate_digest_hex);
}

TEST(CertifiedWeightUpdatePipelineTest,
     ViewAdvanceDoesNotBroadcastSyntheticWeightUpdateVoteEquivocation) {
  const std::string trigger_path = "/tmp/td_hs_wue_view_advance_absent";
  std::remove(trigger_path.c_str());
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION", "1", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW", "10",
         /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE",
         trigger_path.c_str(), /*overwrite=*/1);

  auto weight_schedule =
      std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).Times(0);

  int view = 10;
  std::vector<WeightUpdateVote> broadcast_votes;
  CertifiedWeightUpdatePipeline::Callbacks callbacks;
  callbacks.current_view = [&] { return view; };
  callbacks.broadcast_vote = [&](const WeightUpdateVote& vote) {
    broadcast_votes.push_back(vote);
  };

  CertifiedWeightUpdatePipeline pipeline(
      /*node_id=*/1, /*total_replicas=*/4, weight_schedule, leader_schedule,
      &verifier, /*reputation_adapter=*/nullptr, std::move(callbacks));

  pipeline.MaybeActivateReadyAfterViewAdvance();
  pipeline.MaybeActivateReadyAfterViewAdvance();

  EXPECT_TRUE(broadcast_votes.empty());

  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE");
}

TEST(CertifiedWeightUpdatePipelineTest,
     ViewAdvanceBroadcastsTriggeredSyntheticWeightUpdateVoteEquivocationOnce) {
  const std::string trigger_path = "/tmp/td_hs_wue_view_advance_present";
  std::ofstream(trigger_path).close();
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION", "1", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW", "10",
         /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE",
         trigger_path.c_str(), /*overwrite=*/1);

  auto weight_schedule =
      std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_))
      .Times(2)
      .WillRepeatedly(Return(SignatureFor(1)));

  int view = 10;
  std::vector<WeightUpdateVote> broadcast_votes;
  CertifiedWeightUpdatePipeline::Callbacks callbacks;
  callbacks.current_view = [&] { return view; };
  callbacks.broadcast_vote = [&](const WeightUpdateVote& vote) {
    broadcast_votes.push_back(vote);
  };

  CertifiedWeightUpdatePipeline pipeline(
      /*node_id=*/1, /*total_replicas=*/4, weight_schedule, leader_schedule,
      &verifier, /*reputation_adapter=*/nullptr, std::move(callbacks));

  pipeline.MaybeActivateReadyAfterViewAdvance();
  pipeline.MaybeActivateReadyAfterViewAdvance();

  ASSERT_EQ(broadcast_votes.size(), 2);
  EXPECT_EQ(broadcast_votes[0].signer(), 1);
  EXPECT_EQ(broadcast_votes[1].signer(), 1);
  EXPECT_EQ(broadcast_votes[0].old_weight_root(),
            weight_schedule->ActiveWeightRoot());
  EXPECT_EQ(broadcast_votes[1].old_weight_root(),
            weight_schedule->ActiveWeightRoot());
  EXPECT_EQ(broadcast_votes[0].old_weight_version(),
            weight_schedule->ActiveWeightVersion());
  EXPECT_EQ(broadcast_votes[1].old_weight_version(),
            weight_schedule->ActiveWeightVersion());
  EXPECT_EQ(broadcast_votes[0].activation_view(), 11);
  EXPECT_EQ(broadcast_votes[1].activation_view(), 11);
  EXPECT_NE(broadcast_votes[0].candidate_digest(),
            broadcast_votes[1].candidate_digest());

  std::remove(trigger_path.c_str());
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE");
}

TEST(CertifiedWeightUpdatePipelineTest,
     ArmedTriggerBeforeStartViewDoesNotBroadcastSyntheticWeightUpdateVote) {
  const std::string trigger_path = "/tmp/td_hs_wue_before_start_present";
  std::ofstream(trigger_path).close();
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION", "1", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW", "10",
         /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE",
         trigger_path.c_str(), /*overwrite=*/1);

  auto weight_schedule =
      std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).Times(0);

  int view = 9;
  std::vector<WeightUpdateVote> broadcast_votes;
  CertifiedWeightUpdatePipeline::Callbacks callbacks;
  callbacks.current_view = [&] { return view; };
  callbacks.broadcast_vote = [&](const WeightUpdateVote& vote) {
    broadcast_votes.push_back(vote);
  };

  CertifiedWeightUpdatePipeline pipeline(
      /*node_id=*/1, /*total_replicas=*/4, weight_schedule, leader_schedule,
      &verifier, /*reputation_adapter=*/nullptr, std::move(callbacks));

  pipeline.MaybeActivateReadyAfterViewAdvance();

  EXPECT_TRUE(broadcast_votes.empty());

  std::remove(trigger_path.c_str());
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE");
}

TEST(CertifiedWeightUpdatePipelineTest,
     CandidateDrainBroadcastsSyntheticWeightUpdateVoteEquivocationOnce) {
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION", "1", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW", "10",
         /*overwrite=*/1);

  auto weight_schedule =
      std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
  auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
      4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
      /*eligible_min_weight=*/10);
  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_))
      .Times(3)
      .WillRepeatedly(Return(SignatureFor(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  int view = 10;
  std::vector<WeightUpdateVote> broadcast_votes;
  CertifiedWeightUpdatePipeline::Callbacks callbacks;
  callbacks.current_view = [&] { return view; };
  callbacks.broadcast_vote = [&](const WeightUpdateVote& vote) {
    broadcast_votes.push_back(vote);
  };

  CertifiedWeightUpdatePipeline pipeline(
      /*node_id=*/1, /*total_replicas=*/4, weight_schedule, leader_schedule,
      &verifier, /*reputation_adapter=*/nullptr, std::move(callbacks));

  pipeline.DrainCompletedCandidatesForTesting({Candidate()});
  pipeline.DrainCompletedCandidatesForTesting({Candidate()});

  ASSERT_EQ(broadcast_votes.size(), 3);
  EXPECT_EQ(broadcast_votes[0].signer(), 1);
  EXPECT_EQ(broadcast_votes[1].signer(), 1);
  EXPECT_EQ(broadcast_votes[2].signer(), 1);
  EXPECT_EQ(broadcast_votes[1].old_weight_root(),
            weight_schedule->ActiveWeightRoot());
  EXPECT_EQ(broadcast_votes[2].old_weight_root(),
            weight_schedule->ActiveWeightRoot());
  EXPECT_EQ(broadcast_votes[1].old_weight_version(),
            weight_schedule->ActiveWeightVersion());
  EXPECT_EQ(broadcast_votes[2].old_weight_version(),
            weight_schedule->ActiveWeightVersion());
  EXPECT_EQ(broadcast_votes[1].activation_view(), 11);
  EXPECT_EQ(broadcast_votes[2].activation_view(), 11);
  EXPECT_NE(broadcast_votes[1].candidate_digest(),
            broadcast_votes[2].candidate_digest());

  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW");
}

TEST(CertifiedWeightUpdatePipelineTest,
     TriggerFileDelaysSyntheticWeightUpdateVoteEquivocation) {
  const std::string trigger_path = "/tmp/td_hs_wue_trigger_test";
  std::remove(trigger_path.c_str());
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION", "1", /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW", "0",
         /*overwrite=*/1);
  setenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE",
         trigger_path.c_str(), /*overwrite=*/1);

  {
    auto weight_schedule =
        std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
    auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
        4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
        /*eligible_min_weight=*/10);
    MockSignatureVerifier verifier;
    EXPECT_CALL(verifier, SignMessage(_)).WillOnce(Return(SignatureFor(1)));
    EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

    std::vector<WeightUpdateVote> broadcast_votes;
    CertifiedWeightUpdatePipeline::Callbacks callbacks;
    callbacks.current_view = [] { return 0; };
    callbacks.broadcast_vote = [&](const WeightUpdateVote& vote) {
      broadcast_votes.push_back(vote);
    };

    CertifiedWeightUpdatePipeline pipeline(
        /*node_id=*/1, /*total_replicas=*/4, weight_schedule, leader_schedule,
        &verifier, /*reputation_adapter=*/nullptr, std::move(callbacks));
    pipeline.DrainCompletedCandidatesForTesting({Candidate()});
    ASSERT_EQ(broadcast_votes.size(), 1);
  }

  std::ofstream(trigger_path).close();
  {
    auto weight_schedule =
        std::make_shared<WeightSchedule>(4, std::vector<int64_t>{1, 1, 1, 1});
    auto leader_schedule = std::make_shared<LeaderSelectionSchedule>(
        4, std::vector<int64_t>{100, 100, 100, 100}, /*enabled=*/true,
        /*eligible_min_weight=*/10);
    MockSignatureVerifier verifier;
    EXPECT_CALL(verifier, SignMessage(_))
        .Times(3)
        .WillRepeatedly(Return(SignatureFor(1)));
    EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

    std::vector<WeightUpdateVote> broadcast_votes;
    CertifiedWeightUpdatePipeline::Callbacks callbacks;
    callbacks.current_view = [] { return 0; };
    callbacks.broadcast_vote = [&](const WeightUpdateVote& vote) {
      broadcast_votes.push_back(vote);
    };

    CertifiedWeightUpdatePipeline pipeline(
        /*node_id=*/1, /*total_replicas=*/4, weight_schedule, leader_schedule,
        &verifier, /*reputation_adapter=*/nullptr, std::move(callbacks));
    pipeline.DrainCompletedCandidatesForTesting({Candidate()});
    ASSERT_EQ(broadcast_votes.size(), 3);
    EXPECT_NE(broadcast_votes[1].candidate_digest(),
              broadcast_votes[2].candidate_digest());
  }

  std::remove(trigger_path.c_str());
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW");
  unsetenv("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE");
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
