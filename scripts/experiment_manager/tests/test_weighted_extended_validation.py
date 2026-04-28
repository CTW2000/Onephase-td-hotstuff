"""Tests for TD-HS weighted coverage in the extended validation matrix."""

import unittest

from experiment_manager.experiments.registry import EXPERIMENT_CLASSES, SUITES
from experiment_manager.experiments.scalability import ScalabilityExperiment
from experiment_manager.experiments.slow_vote import SlowVoteExperiment
from experiment_manager.experiments.two_attacker import TwoAttackerExperiment


EXPECTED_WEIGHT_TAGS = [
    "weights=uniform",
    "weights=seed1",
    "weights=seed2",
    "weights=seed3",
]


class WeightedExtendedValidationTest(unittest.TestCase):
    def assert_td_hs_weight_group(self, runs, replicas, tag_prefix=""):
        if tag_prefix:
            expected_tags = [
                f"{tag_prefix}__{tag}" for tag in EXPECTED_WEIGHT_TAGS
            ]
        else:
            expected_tags = EXPECTED_WEIGHT_TAGS

        self.assertEqual(expected_tags, [run.tag for run in runs])
        self.assertEqual(["TD-HS"] * 4, [run.protocol for run in runs])
        self.assertEqual({}, runs[0].env_vars)

        for run in runs[1:]:
            weights = run.env_vars.get("TD_HS_WEIGHTS", "").split(",")
            self.assertEqual(replicas, len(weights))
            self.assertTrue(all(weight in {"1", "2", "3"} for weight in weights))

    def test_scalability_td_hs_expands_each_replica_count_into_weight_profiles(self):
        experiment = ScalabilityExperiment(replica_counts=[5, 15])

        runs = experiment.generate_runs(protocols=["TD-HS"])

        self.assertEqual(8, len(runs))
        for replicas in [5, 15]:
            with self.subTest(replicas=replicas):
                group = [run for run in runs if run.replicas == replicas]
                self.assert_td_hs_weight_group(group, replicas)
                for run in group:
                    self.assertEqual(3, run.config_overrides["max_process_txn"])

        self.assertEqual(len(runs), len({run.run_id for run in runs}))

    def test_scalability_non_td_hs_remains_uniform(self):
        experiment = ScalabilityExperiment(replica_counts=[5, 15])

        runs = experiment.generate_runs(protocols=["HS-1-SLOT"])

        self.assertEqual(2, len(runs))
        self.assertEqual([5, 15], [run.replicas for run in runs])
        self.assertEqual(["", ""], [run.tag for run in runs])
        self.assertEqual([{}, {}], [run.env_vars for run in runs])

    def test_slow_vote_td_hs_expands_weight_profiles_for_each_vote_delay_count(self):
        experiment = SlowVoteExperiment(
            slow_vote_counts=[0, 6],
            mean_delay_ms=10,
            replicas=20,
        )

        runs = experiment.generate_runs(protocols=["TD-HS"])

        self.assertEqual(8, len(runs))
        for slow_vote_count in [0, 6]:
            with self.subTest(slow_vote_count=slow_vote_count):
                group = [
                    run for run in runs
                    if run.config_overrides["network_delay_num"] == slow_vote_count
                ]
                self.assert_td_hs_weight_group(group, 20)
                for run in group:
                    self.assertEqual(10, run.config_overrides["mean_network_delay"])

        self.assertEqual(len(runs), len({run.run_id for run in runs}))

    def test_two_attacker_td_hs_expands_rollback_and_combined_attack_profiles(self):
        experiment = TwoAttackerExperiment(
            rollback_counts=[0, 4],
            combined_pairs=[(0, 0), (2, 2)],
            replicas=20,
            timer_length=100,
        )

        runs = experiment.generate_runs(protocols=["TD-HS"])

        self.assertEqual(16, len(runs))
        rollback_runs = [run for run in runs if "rollback_num" in run.config_overrides]
        combined_runs = [run for run in runs if "fork_tail_num" in run.config_overrides]
        self.assertEqual(8, len(rollback_runs))
        self.assertEqual(8, len(combined_runs))

        for rollback_count in [0, 4]:
            with self.subTest(rollback_count=rollback_count):
                group = [
                    run for run in rollback_runs
                    if run.config_overrides["rollback_num"] == rollback_count
                ]
                self.assert_td_hs_weight_group(
                    group, 20, tag_prefix="attack=rollback"
                )
                self.assertTrue(all("attack=rollback" in run.tag for run in group))

        for fork_tail_num, slow_num in [(0, 0), (2, 2)]:
            with self.subTest(fork_tail_num=fork_tail_num, slow_num=slow_num):
                group = [
                    run for run in combined_runs
                    if run.config_overrides["fork_tail_num"] == fork_tail_num
                    and run.config_overrides["non_responsive_num"] == slow_num
                ]
                self.assert_td_hs_weight_group(
                    group, 20, tag_prefix="attack=combined"
                )
                self.assertTrue(all("attack=combined" in run.tag for run in group))

        self.assertEqual(len(runs), len({run.run_id for run in runs}))


    def test_extended_non_td_hs_runs_remain_uniform_without_weight_env(self):
        cases = [
            (
                SlowVoteExperiment(
                    slow_vote_counts=[0, 6],
                    mean_delay_ms=10,
                    replicas=20,
                ),
                2,
            ),
            (
                TwoAttackerExperiment(
                    rollback_counts=[0, 4],
                    combined_pairs=[(0, 0), (2, 2)],
                    replicas=20,
                    timer_length=100,
                ),
                4,
            ),
        ]

        for experiment, expected_count in cases:
            with self.subTest(experiment=experiment.name):
                runs = experiment.generate_runs(protocols=["HS-1-SLOT"])

                self.assertEqual(expected_count, len(runs))
                self.assertEqual(["HS-1-SLOT"] * expected_count, [run.protocol for run in runs])
                self.assertTrue(all(run.env_vars == {} for run in runs))
                self.assertTrue(all("TD_HS_WEIGHTS" not in run.env_vars for run in runs))

    def test_registry_exposes_extended_validation_experiments(self):
        self.assertIn("slow_vote", EXPERIMENT_CLASSES)
        self.assertIn("two_attacker", EXPERIMENT_CLASSES)
        self.assertIn("slow_vote", SUITES["byzantine"])
        self.assertIn("two_attacker", SUITES["byzantine"])
