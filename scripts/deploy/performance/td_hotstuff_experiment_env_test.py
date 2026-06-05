import os
import subprocess
import tempfile
import unittest


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
ENV_HELPER = os.path.join(REPO_ROOT, "scripts", "deploy", "td_hotstuff_stable_env.sh")


def source_helper(extra_env=None):
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    command = f"source {ENV_HELPER} && env"
    result = subprocess.run(
        ["bash", "-lc", command],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    values = {}
    for line in result.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


class TdHotstuffExperimentEnvTest(unittest.TestCase):
    def test_stable_env_sets_pipeline_defaults(self):
        values = source_helper()

        self.assertEqual(values["TD_HS_REPUTATION_ENABLE"], "0")
        self.assertEqual(values["TD_HS_WEIGHT_UPDATE_ENABLE"], "0")
        self.assertEqual(values["TD_HS_LEADER_SELECTION_ENABLE"], "0")
        self.assertEqual(values["TD_HS_BENCHMARK_DYNAMIC_ROUTING_ENABLE"], "0")
        self.assertEqual(values["TD_HS_QC_DIVERSITY_ENABLE"], "0")
        self.assertEqual(values["TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE"], "0")
        self.assertEqual(values["TD_HS_TIMEOUT_ENABLE"], "0")
        self.assertEqual(values["TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT"], "11")
        self.assertEqual(values["TD_HS_WEIGHTS"], ",".join(["30"] * 20))

    def test_stable_env_preserves_explicit_overrides(self):
        values = source_helper(
            {
                "TD_HS_REPUTATION_WINDOW_SIZE": "128",
                "TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT": "12",
                "TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE": "1",
            }
        )

        self.assertEqual(values["TD_HS_REPUTATION_WINDOW_SIZE"], "128")
        self.assertEqual(values["TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT"], "12")
        self.assertEqual(values["TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE"], "1")

    def test_silent_leader_ids_are_not_shared_with_all_replicas_or_clients(self):
        deploy_multi = os.path.join(REPO_ROOT, "scripts", "deploy", "script", "deploy_multi.sh")
        with open(deploy_multi) as fp:
            body = fp.read()
        shared_env_block = body.split("td_env_names=(", 1)[1].split(")", 1)[0]
        self.assertNotIn("TD_HS_SILENT_LEADER_IDS", shared_env_block)
        self.assertIn("-u TD_HS_SILENT_LEADER_IDS", body)
        self.assertIn("-u TD_HS_BAD_NODE_IDS", body)
        self.assertIn("-u TD_HS_BAD_NODE_COUNT", body)
        self.assertIn("TD_HS_SILENT_LEADER=1", body)
        self.assertIn("TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE", shared_env_block)
        self.assertIn("is_silent_leader_node", body)

        for script in ["run_slow_leader_n20.sh", "run_slow_vote_n20.sh"]:
            with self.subTest(script=script):
                with open(os.path.join(REPO_ROOT, "scripts", "deploy", script)) as fp:
                    script_body = fp.read()
                self.assertIn("env -u TD_HS_SILENT_LEADER_IDS", script_body)

    def test_slow_experiment_scripts_restore_shared_configs(self):
        scripts = [
            "run_slow_vote_n20.sh",
            "run_slow_leader_n20.sh",
        ]

        for script in scripts:
            with self.subTest(script=script):
                with open(os.path.join(REPO_ROOT, "scripts", "deploy", script)) as fp:
                    body = fp.read()
                self.assertIn("CONFIG_FILES_TO_RESTORE", body)
                self.assertIn("trap restore_generated_configs EXIT", body)
                self.assertIn("config/performance.conf", body)
                self.assertIn("config/td_hotstuff.config", body)

    def test_generated_config_restore_helper_restores_baseline_content(self):
        helper = os.path.join(REPO_ROOT, "scripts", "deploy", "script", "generated_config_restore.sh")
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = os.path.join(temp_dir, "config")
            os.mkdir(config_dir)
            td_config = os.path.join(config_dir, "td_hotstuff.config")
            perf_config = os.path.join(config_dir, "performance.conf")
            with open(td_config, "w") as fp:
                fp.write("network_delay_num=0\n")
            with open(perf_config, "w") as fp:
                fp.write("client_num=1\n")

            command = f"""
source {helper}
CONFIG_FILES_TO_RESTORE=("config/td_hotstuff.config" "config/performance.conf")
backup_generated_configs
printf 'network_delay_num=6\n' > config/td_hotstuff.config
printf 'client_num=99\n' > config/performance.conf
restore_generated_configs
grep -qx 'network_delay_num=0' config/td_hotstuff.config
grep -qx 'client_num=1' config/performance.conf
"""
            subprocess.run(["bash", "-lc", command], cwd=temp_dir, check=True)


if __name__ == "__main__":
    unittest.main()
