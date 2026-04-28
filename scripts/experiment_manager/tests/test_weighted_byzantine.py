"""Tests for TD-HS weighted variants in Byzantine experiments."""

import unittest
from pathlib import Path

from experiment_manager.experiments.leader_slowness import LeaderSlownessExperiment
from experiment_manager.experiments.network_delay import NetworkDelayExperiment
from experiment_manager.experiments.rollback import RollbackExperiment
from experiment_manager.experiments.tail_forking import TailForkingExperiment


class WeightedByzantineRunsTest(unittest.TestCase):
    def test_td_hs_byzantine_runs_include_uniform_and_weighted_profiles(self):
        experiments = [
            LeaderSlownessExperiment(slow_counts=[1], delays=[10], replicas=5),
            NetworkDelayExperiment(delays=[50], impacted_counts=[2], replicas=5),
            RollbackExperiment(faulty_counts=[1], delays=[10], replicas=5),
            TailForkingExperiment(faulty_counts=[1], delays=[10], replicas=5),
        ]

        for experiment in experiments:
            with self.subTest(experiment=experiment.name):
                runs = experiment.generate_runs(protocols=["TD-HS"])

                self.assertEqual(
                    ["weights=uniform", "weights=seed1", "weights=seed2", "weights=seed3"],
                    [run.tag for run in runs],
                )
                self.assertEqual(["TD-HS"] * 4, [run.protocol for run in runs])
                self.assertEqual({}, runs[0].env_vars)

                weighted_envs = [run.env_vars for run in runs[1:]]
                for env in weighted_envs:
                    weights = env.get("TD_HS_WEIGHTS", "").split(",")
                    self.assertEqual(5, len(weights))
                    self.assertTrue(all(w in {"1", "2", "3"} for w in weights))

                self.assertEqual(len(runs), len({run.run_id for run in runs}))

    def test_non_td_hs_byzantine_runs_remain_uniform_without_weight_env(self):
        experiments = [
            LeaderSlownessExperiment(slow_counts=[1], delays=[10], replicas=5),
            NetworkDelayExperiment(delays=[50], impacted_counts=[2], replicas=5),
            RollbackExperiment(faulty_counts=[1], delays=[10], replicas=5),
            TailForkingExperiment(faulty_counts=[1], delays=[10], replicas=5),
        ]

        for experiment in experiments:
            with self.subTest(experiment=experiment.name):
                runs = experiment.generate_runs(protocols=["HS-1-SLOT"])

                self.assertEqual(1, len(runs))
                self.assertEqual("HS-1-SLOT", runs[0].protocol)
                self.assertEqual("", runs[0].tag)
                self.assertEqual({}, runs[0].env_vars)


class RemoteWeightForwardingTest(unittest.TestCase):
    def test_remote_deploy_forwards_td_hs_weights_to_started_replicas(self):
        scripts_dir = Path(__file__).resolve().parents[2]
        deploy_script = scripts_dir / "deploy" / "script" / "deploy_multi.sh"
        text = deploy_script.read_text()

        self.assertIn("TD_HS_WEIGHTS", text)
        self.assertIn("local_server_env", text)
        self.assertIn("remote_server_env", text)
        self.assertIn("env TD_HS_WEIGHTS", text)
        self.assertIn('"${local_server_env[@]}" setsid', text)
        self.assertIn("${remote_server_env}nohup ./${server_bin}", text)


if __name__ == "__main__":
    unittest.main()
