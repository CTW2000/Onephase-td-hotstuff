#include "platform/consensus/ordering/td_hotstuff/algorithm/weight_update_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "common/crypto/mock_signature_verifier.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/leader_selection_schedule.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/proposal_manager.h"
#include "platform/consensus/ordering/td_hotstuff/algorithm/vote_score_reputation_plugin.h"

namespace resdb {
namespace td_hotstuff {
namespace {

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Return;

SignatureInfo SignatureFrom(int signer) {
  SignatureInfo signature;
  signature.set_node_id(signer);
  signature.set_signature("signature-" + std::to_string(signer));
  return signature;
}

void UseNonWarmupWindow(VoteScoreCandidate* candidate) {
  candidate->window_index = 4;
  RecomputeVoteScoreCandidateRoots(candidate);
}

void UseNonBroadcasterWindow(VoteScoreCandidate* candidate) {
  candidate->window_index = 1;
  RecomputeVoteScoreCandidateRoots(candidate);
}

WeightUpdateVote MakeVote(const CandidateWeightUpdate& update, int signer) {
  WeightUpdateVote vote;
  vote.set_candidate_digest(update.candidate_digest());
  vote.set_validator_id(signer);
  vote.set_old_weight_root(update.old_weight_root());
  vote.set_old_weight_version(update.old_weight_version());
  vote.set_activation_view(update.activation_view());
  *vote.mutable_signature() = SignatureFrom(signer);
  return vote;
}

VoteScoreCandidate MakeCandidate(const WeightSchedule& schedule,
                                 int activation_view) {
  std::vector<ReputationQcEvent> events;
  events.push_back({4095, "hash-a", std::string(1, static_cast<char>(0x03))});
  events.push_back({4096, "hash-b", std::string(1, static_cast<char>(0x03))});
  VoteScoreCandidate candidate = ComputeBayesianReputationCandidate(
      /*node_id=*/1, /*total_replicas=*/4, /*window_index=*/0, events,
      schedule.ActiveWeights(), /*max_delta=*/2, schedule.ActiveWeightRoot(),
      schedule.ActiveWeightVersion(), activation_view);
  candidate.validators[0].next_weight =
      std::max<int64_t>(1, candidate.validators[0].current_weight - 1);
  RecomputeVoteScoreCandidateRoots(&candidate);
  return candidate;
}

TEST(WeightScheduleTest,
     KeepsOldWeightsAtActivationBoundaryAndNewWeightsAfterward) {
  WeightSchedule schedule(/*total_replicas=*/4, {10, 20, 30, 40});

  EXPECT_EQ(schedule.WeightForSigner(/*signer=*/4, /*view=*/9), 40);
  EXPECT_EQ(schedule.QuorumWeightForView(/*view=*/9), 67);
  EXPECT_EQ(schedule.WeightVersionForView(/*view=*/9), 0);

  const std::string old_root = schedule.ActiveWeightRoot();
  ASSERT_TRUE(schedule.ScheduleUpdate(/*activation_view=*/10,
                                      /*next_weights=*/{10, 20, 30, 38},
                                      old_root,
                                      /*old_weight_version=*/0));

  EXPECT_EQ(schedule.WeightForSigner(/*signer=*/4, /*view=*/9), 40);
  EXPECT_EQ(schedule.WeightForSigner(/*signer=*/4, /*view=*/10), 40);
  EXPECT_EQ(schedule.WeightVersionForView(/*view=*/10), 0);
  EXPECT_EQ(schedule.QuorumWeightForView(/*view=*/10), 67);
  EXPECT_EQ(schedule.WeightForSigner(/*signer=*/4, /*view=*/11), 38);
  EXPECT_EQ(schedule.WeightVersionForView(/*view=*/11), 1);
  EXPECT_EQ(schedule.QuorumWeightForView(/*view=*/11), 66);
}

TEST(WeightUpdateManagerTest, SignsVoteForMatchingFutureCandidate) {
  WeightSchedule schedule(/*total_replicas=*/4, {10, 20, 30, 40});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(WeightUpdateVotePayload(update, 1)))
      .WillOnce(Return(SignatureFrom(1)));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  std::unique_ptr<WeightUpdateVote> vote =
      manager.CreateVote(update, local, schedule);

  ASSERT_NE(vote, nullptr);
  EXPECT_EQ(vote->candidate_digest(), update.candidate_digest());
  EXPECT_EQ(vote->old_weight_root(), schedule.ActiveWeightRoot());
  EXPECT_EQ(vote->old_weight_version(), 0);
  EXPECT_EQ(vote->activation_view(), 8192);
  EXPECT_EQ(vote->signature().node_id(), 1);
}

TEST(WeightUpdateManagerTest, CandidateLeavesLeaderProfileEmptyForWeightV1) {
  WeightSchedule schedule(/*total_replicas=*/4, {10, 20, 30, 40});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);

  EXPECT_EQ(update.leader_validators_size(), 0);
  EXPECT_TRUE(CandidateLeaderWeights(update).empty());
  EXPECT_TRUE(update.leader_weight_root().empty());
  EXPECT_EQ(update.leader_params_version(), 0);
  EXPECT_TRUE(update.leader_randomness_ref().empty());

  MockSignatureVerifier verifier;
  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  std::string error;
  EXPECT_TRUE(manager.VerifyCandidate(update, local, schedule, &error))
      << error;
}

TEST(WeightUpdateManagerTest, RejectsStaleLeaderProfileFields) {
  WeightSchedule schedule(/*total_replicas=*/4, {10, 20, 30, 40});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);
  CandidateLeaderWeight* leader = update.add_leader_validators();
  leader->set_validator_id(1);
  leader->set_leader_weight(9);

  MockSignatureVerifier verifier;
  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  std::string error;
  EXPECT_FALSE(manager.VerifyCandidate(update, local, schedule, &error));
  EXPECT_NE(error.find("leader profile disabled"), std::string::npos);
}

TEST(WeightUpdateManagerTest, RejectsNonCanonicalCertEvenWithQuorum) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 4096;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);

  WeightUpdateCert cert;
  *cert.mutable_candidate_update() = update;
  cert.set_signer_bitmap(BuildSignerBitmap({1, 2}, 4));
  *cert.add_votes() = MakeVote(update, 1);
  *cert.add_votes() = MakeVote(update, 2);
  cert.set_quorum_rule_id("old-weight-2f-plus-1");

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  std::string error;
  EXPECT_FALSE(manager.VerifyCert(cert, schedule, &error));
  EXPECT_NE(error.find("not eligible"), std::string::npos);
}

TEST(WeightUpdateManagerTest, RejectsVoteCreationForNonCanonicalCandidate) {
  WeightSchedule schedule(/*total_replicas=*/4, {10, 20, 30, 40});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 4096;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).Times(0);
  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  std::unique_ptr<WeightUpdateVote> vote =
      manager.CreateVote(update, local, schedule);

  EXPECT_EQ(vote, nullptr);
}

TEST(WeightUpdateManagerTest, CertUsesOldWeightsNotCandidateWeights) {
  WeightSchedule schedule(/*total_replicas=*/4, {10, 10, 40, 40});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  local.next_weights = {100, 100, 1, 1};
  local.validators[0].next_weight = 100;
  local.validators[1].next_weight = 100;
  local.validators[2].next_weight = 1;
  local.validators[3].next_weight = 1;
  RecomputeVoteScoreCandidateRoots(&local);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);

  WeightUpdateCert cert;
  *cert.mutable_candidate_update() = update;
  cert.set_signer_bitmap(BuildSignerBitmap({1, 2}, 4));
  *cert.add_votes() = MakeVote(update, 1);
  *cert.add_votes() = MakeVote(update, 2);
  cert.set_quorum_rule_id("old-weight-2f-plus-1");

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  std::string error;
  EXPECT_FALSE(manager.VerifyCert(cert, schedule, &error));
  EXPECT_NE(error.find("quorum"), std::string::npos);
}

TEST(WeightUpdateManagerTest, CertInstallsWhenOldWeightQuorumSigns) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);

  WeightUpdateCert cert;
  *cert.mutable_candidate_update() = update;
  cert.set_signer_bitmap(BuildSignerBitmap({1, 2}, 4));
  *cert.add_votes() = MakeVote(update, 1);
  *cert.add_votes() = MakeVote(update, 2);
  cert.set_quorum_rule_id("old-weight-2f-plus-1");

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  std::string error;
  ASSERT_TRUE(manager.VerifyCert(cert, schedule, &error)) << error;
  ASSERT_TRUE(schedule.ScheduleUpdate(
      update.activation_view(), CandidateNextWeights(update),
      update.old_weight_root(), update.old_weight_version()));

  EXPECT_EQ(schedule.WeightVersionForView(8191), 0);
  EXPECT_EQ(schedule.WeightVersionForView(8192), 0);
  EXPECT_EQ(schedule.WeightVersionForView(8193), 1);
}

TEST(WeightUpdateManagerPluginTest,
     LeaderProfileOnlyCandidateDoesNotEnterConsensus) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 1});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  for (ValidatorVoteScore& validator : local.validators) {
    validator.next_weight = validator.current_weight;
  }
  RecomputeVoteScoreCandidateRoots(&local);

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(_)).Times(0);
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  WeightSnapshot snapshot = MakeWeightSnapshot(schedule, 4096);

  manager.AddLocalCandidates({local}, snapshot);
  WeightPluginOutboundMessages out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  EXPECT_TRUE(out.candidates.empty());
  EXPECT_TRUE(out.votes.empty());
  EXPECT_TRUE(out.certs.empty());
}

TEST(WeightUpdateManagerPluginTest,
     DrainsCandidateVoteCertAndInstallableUpdateFromPluginState) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(WeightUpdateVotePayload(update, 1)))
      .WillOnce(Return(SignatureFrom(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  const WeightSnapshot snapshot = MakeWeightSnapshot(schedule, 4096);
  manager.AddLocalCandidates({local}, snapshot);

  WeightPluginOutboundMessages first_out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  ASSERT_EQ(first_out.candidates.size(), 1);
  ASSERT_EQ(first_out.votes.size(), 1);
  EXPECT_TRUE(first_out.certs.empty());
  EXPECT_EQ(first_out.candidates[0].candidate_digest(),
            update.candidate_digest());
  EXPECT_EQ(first_out.votes[0].candidate_digest(), update.candidate_digest());

  manager.HandleVote(MakeVote(update, 2), snapshot);
  WeightPluginOutboundMessages second_out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  EXPECT_TRUE(second_out.candidates.empty());
  EXPECT_TRUE(second_out.votes.empty());
  ASSERT_EQ(second_out.certs.size(), 1);
  EXPECT_EQ(second_out.certs[0].candidate_update().candidate_digest(),
            update.candidate_digest());

  const WeightSnapshot activation_snapshot = MakeWeightSnapshot(schedule, 8192);
  std::vector<InstallableWeightUpdate> installable =
      manager.TakeInstallableUpdates(/*current_view=*/8192,
                                     activation_snapshot);
  ASSERT_EQ(installable.size(), 1);
  EXPECT_EQ(installable[0].activation_view, 8192);
  EXPECT_EQ(installable[0].old_weight_root, schedule.ActiveWeightRoot());
  EXPECT_EQ(installable[0].old_weight_version, 0);
  EXPECT_EQ(installable[0].next_weights, CandidateNextWeights(update));
}

TEST(WeightUpdateManagerPluginTest,
     FormsLateCertAtActivationBoundaryWhenStillOnOldSchedule) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(WeightUpdateVotePayload(update, 1)))
      .WillOnce(Return(SignatureFrom(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  const WeightSnapshot candidate_snapshot = MakeWeightSnapshot(schedule, 4096);
  manager.AddLocalCandidates({local}, candidate_snapshot);
  manager.DrainOutboundMessages(/*current_view=*/4096, candidate_snapshot);

  const WeightSnapshot activation_snapshot = MakeWeightSnapshot(schedule, 8192);
  manager.HandleVote(MakeVote(update, 2), activation_snapshot);
  WeightPluginOutboundMessages out =
      manager.DrainOutboundMessages(/*current_view=*/8192, activation_snapshot);
  ASSERT_EQ(out.certs.size(), 1);
  EXPECT_EQ(out.certs[0].candidate_update().candidate_digest(),
            update.candidate_digest());

  std::vector<InstallableWeightUpdate> installable =
      manager.TakeInstallableUpdates(/*current_view=*/8192,
                                     activation_snapshot);
  ASSERT_EQ(installable.size(), 1);
  EXPECT_EQ(installable[0].activation_view, 8192);
  EXPECT_EQ(installable[0].next_weights, CandidateNextWeights(update));
}

TEST(WeightUpdateManagerPluginTest, RebroadcastsValidReceivedCertOnce) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);

  WeightUpdateCert cert;
  *cert.mutable_candidate_update() = update;
  cert.set_signer_bitmap(BuildSignerBitmap({1, 2}, 4));
  *cert.add_votes() = MakeVote(update, 1);
  *cert.add_votes() = MakeVote(update, 2);
  cert.set_quorum_rule_id("old-weight-2f-plus-1");

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/3, /*total_replicas=*/4, &verifier,
                              config);
  const WeightSnapshot snapshot = MakeWeightSnapshot(schedule, 4096);
  manager.HandleCert(cert, snapshot);
  WeightPluginOutboundMessages first_out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  ASSERT_EQ(first_out.certs.size(), 1);
  EXPECT_EQ(first_out.certs[0].candidate_update().candidate_digest(),
            update.candidate_digest());

  manager.HandleCert(cert, snapshot);
  WeightPluginOutboundMessages second_out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  EXPECT_TRUE(second_out.certs.empty());
}

TEST(WeightUpdateManagerPluginTest, SkipsDuplicateCandidateDigest) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate first = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&first);
  const CandidateWeightUpdate first_update = BuildCandidateWeightUpdate(first);

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(WeightUpdateVotePayload(first_update, 1)))
      .WillOnce(Return(SignatureFrom(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  const WeightSnapshot snapshot = MakeWeightSnapshot(schedule, 4096);
  manager.AddLocalCandidates({first}, snapshot);
  WeightPluginOutboundMessages first_out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  ASSERT_EQ(first_out.candidates.size(), 1);
  ASSERT_EQ(first_out.votes.size(), 1);
  EXPECT_EQ(first_out.candidates[0].candidate_digest(),
            first_update.candidate_digest());

  manager.AddLocalCandidates({first}, snapshot);
  WeightPluginOutboundMessages second_out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  EXPECT_TRUE(second_out.candidates.empty());
  EXPECT_TRUE(second_out.votes.empty());
  EXPECT_TRUE(second_out.certs.empty());
}

TEST(WeightUpdateManagerPluginTest,
     BroadcastsLaterCandidateForSameOldWeightVersion) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate first = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&first);
  const CandidateWeightUpdate first_update = BuildCandidateWeightUpdate(first);

  VoteScoreCandidate second = first;
  second.window_index = 8;
  second.activation_view = first.activation_view + 2;
  RecomputeVoteScoreCandidateRoots(&second);
  const CandidateWeightUpdate second_update =
      BuildCandidateWeightUpdate(second);
  ASSERT_EQ(first_update.old_weight_version(),
            second_update.old_weight_version());
  ASSERT_NE(first_update.candidate_digest(), second_update.candidate_digest());

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(WeightUpdateVotePayload(first_update, 1)))
      .WillOnce(Return(SignatureFrom(1)));
  EXPECT_CALL(verifier, SignMessage(WeightUpdateVotePayload(second_update, 1)))
      .WillOnce(Return(SignatureFrom(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  const WeightSnapshot snapshot = MakeWeightSnapshot(schedule, 4096);
  manager.AddLocalCandidates({first}, snapshot);
  WeightPluginOutboundMessages first_out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  ASSERT_EQ(first_out.candidates.size(), 1);
  ASSERT_EQ(first_out.votes.size(), 1);

  manager.AddLocalCandidates({second}, snapshot);
  WeightPluginOutboundMessages second_out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  ASSERT_EQ(second_out.candidates.size(), 1);
  ASSERT_EQ(second_out.votes.size(), 1);
  EXPECT_EQ(second_out.candidates[0].candidate_digest(),
            second_update.candidate_digest());
  EXPECT_EQ(second_out.votes[0].candidate_digest(),
            second_update.candidate_digest());
}

TEST(WeightUpdateManagerPluginTest, SkipsNonCanonicalEpochWindow) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 4096;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);

  MockSignatureVerifier verifier;
  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  const WeightSnapshot snapshot = MakeWeightSnapshot(schedule, 4096);
  manager.AddLocalCandidates({local}, snapshot);

  WeightPluginOutboundMessages out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  EXPECT_TRUE(out.candidates.empty());
  EXPECT_TRUE(out.votes.empty());
  EXPECT_TRUE(out.certs.empty());
}

TEST(WeightUpdateManagerPluginTest,
     BroadcastsLocalCandidateEvenWhenOldBroadcasterWouldDiffer) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonBroadcasterWindow(&local);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(local);

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(WeightUpdateVotePayload(update, 1)))
      .WillOnce(Return(SignatureFrom(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  const WeightSnapshot snapshot = MakeWeightSnapshot(schedule, 4096);
  manager.AddLocalCandidates({local}, snapshot);

  WeightPluginOutboundMessages out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  ASSERT_EQ(out.candidates.size(), 1);
  ASSERT_EQ(out.votes.size(), 1);
  EXPECT_TRUE(out.certs.empty());
  EXPECT_EQ(out.candidates[0].candidate_digest(), update.candidate_digest());
  EXPECT_EQ(out.votes[0].candidate_digest(), update.candidate_digest());
}

TEST(WeightUpdateManagerPluginTest, BroadcastsChangingWarmupCandidate) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  const VoteScoreCandidate warmup = MakeCandidate(schedule, 8192);
  const CandidateWeightUpdate update = BuildCandidateWeightUpdate(warmup);

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(WeightUpdateVotePayload(update, 1)))
      .WillOnce(Return(SignatureFrom(1)));
  EXPECT_CALL(verifier, VerifyMessage(_, _)).WillRepeatedly(Return(true));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  const WeightSnapshot snapshot = MakeWeightSnapshot(schedule, 4096);
  manager.AddLocalCandidates({warmup}, snapshot);

  WeightPluginOutboundMessages out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  ASSERT_EQ(out.candidates.size(), 1);
  ASSERT_EQ(out.votes.size(), 1);
  EXPECT_EQ(out.candidates[0].candidate_digest(), update.candidate_digest());
  EXPECT_EQ(out.votes[0].candidate_digest(), update.candidate_digest());
}

TEST(WeightUpdateManagerPluginTest, SkipsNoOpCandidateWithoutBroadcasting) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 4096;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate noop = MakeCandidate(schedule, 8192);
  noop.window_index = 1;
  noop.next_weights = schedule.ActiveWeights();
  for (size_t i = 0; i < noop.validators.size(); ++i) {
    noop.validators[i].next_weight = noop.next_weights[i];
  }
  RecomputeVoteScoreCandidateRoots(&noop);

  MockSignatureVerifier verifier;
  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  const WeightSnapshot snapshot = MakeWeightSnapshot(schedule, 4096);
  manager.AddLocalCandidates({noop}, snapshot);

  WeightPluginOutboundMessages out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  EXPECT_TRUE(out.candidates.empty());
  EXPECT_TRUE(out.votes.empty());
  EXPECT_TRUE(out.certs.empty());
}

TEST(WeightUpdateManagerPluginTest, DoesNotVoteForConflictingCandidate) {
  WeightSchedule schedule(/*total_replicas=*/4, {40, 40, 10, 10});
  WeightUpdateConfig config;
  config.enabled = true;
  config.epoch_views = 2;
  config.activation_epoch_delay = 1;

  VoteScoreCandidate local = MakeCandidate(schedule, 8192);
  UseNonWarmupWindow(&local);
  const CandidateWeightUpdate local_update = BuildCandidateWeightUpdate(local);
  VoteScoreCandidate conflicting = local;
  conflicting.validators[0].next_weight = 42;
  RecomputeVoteScoreCandidateRoots(&conflicting);
  const CandidateWeightUpdate conflicting_update =
      BuildCandidateWeightUpdate(conflicting);
  ASSERT_NE(local_update.candidate_digest(),
            conflicting_update.candidate_digest());

  MockSignatureVerifier verifier;
  EXPECT_CALL(verifier, SignMessage(WeightUpdateVotePayload(local_update, 1)))
      .WillOnce(Return(SignatureFrom(1)));

  WeightUpdateManager manager(/*node_id=*/1, /*total_replicas=*/4, &verifier,
                              config);
  const WeightSnapshot snapshot = MakeWeightSnapshot(schedule, 4096);
  manager.AddLocalCandidates({local}, snapshot);
  manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);

  manager.HandleCandidate(conflicting_update, snapshot);
  WeightPluginOutboundMessages out =
      manager.DrainOutboundMessages(/*current_view=*/4096, snapshot);
  EXPECT_TRUE(out.votes.empty());
  EXPECT_TRUE(out.certs.empty());
}

}  // namespace
}  // namespace td_hotstuff
}  // namespace resdb
