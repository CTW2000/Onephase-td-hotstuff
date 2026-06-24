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

        self.assertNotIn("TD_HS_REPUTATION_ENABLE", values)
        self.assertNotIn("TD_HS_WEIGHT_UPDATE_ENABLE", values)
        self.assertNotIn("TD_HS_LEADER_SELECTION_ENABLE", values)
        self.assertNotIn("TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT", values)
        self.assertNotIn("TD_HS_QC_DIVERSITY_ENABLE", values)
        self.assertNotIn("TD_HS_PEERTRUST_BROAD_QC_SIGNERS", values)
        self.assertNotIn("TD_HS_PEERTRUST_BROAD_QC_MIN_SIGNERS", values)
        self.assertNotIn("TD_HS_WEIGHTS", values)
        self.assertEqual(values["TD_HS_BENCHMARK_DYNAMIC_ROUTING_ENABLE"], "1")
        self.assertEqual(values["TD_HS_TIMEOUT_ENABLE"], "0")
        self.assertEqual(values["TD_HS_TIMEOUT_MS"], "120")

    def test_stable_env_preserves_explicit_overrides(self):
        values = source_helper(
            {
                "TD_HS_REPUTATION_ENABLE": "1",
                "TD_HS_REPUTATION_WINDOW_SIZE": "128",
                "TD_HS_WEIGHT_UPDATE_ENABLE": "1",
                "TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS": "128",
                "TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY": "2",
                "TD_HS_LEADER_SELECTION_ENABLE": "1",
                "TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT": "12",
                "TD_HS_REPUTATION_MULTIPLICATIVE_WEIGHT_ENABLE": "1",
                "TD_HS_REPUTATION_STAKE_TAU_PER_MILLE": "750",
                "TD_HS_REPUTATION_STAKE_MIN_PER_MILLE": "900",
                "TD_HS_REPUTATION_STAKE_MAX_PER_MILLE": "1100",
                "TD_HS_REPUTATION_IDENTITY_MIN_PER_MILLE": "950",
                "TD_HS_REPUTATION_IDENTITY_MAX_PER_MILLE": "1050",
            }
        )

        self.assertEqual(values["TD_HS_REPUTATION_ENABLE"], "1")
        self.assertEqual(values["TD_HS_REPUTATION_WINDOW_SIZE"], "128")
        self.assertEqual(values["TD_HS_WEIGHT_UPDATE_ENABLE"], "1")
        self.assertEqual(values["TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS"], "128")
        self.assertEqual(values["TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY"], "2")
        self.assertEqual(values["TD_HS_LEADER_SELECTION_ENABLE"], "1")
        self.assertEqual(values["TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT"], "12")
        self.assertEqual(values["TD_HS_REPUTATION_MULTIPLICATIVE_WEIGHT_ENABLE"], "1")
        self.assertEqual(values["TD_HS_REPUTATION_STAKE_TAU_PER_MILLE"], "750")
        self.assertEqual(values["TD_HS_REPUTATION_STAKE_MIN_PER_MILLE"], "900")
        self.assertEqual(values["TD_HS_REPUTATION_STAKE_MAX_PER_MILLE"], "1100")
        self.assertEqual(values["TD_HS_REPUTATION_IDENTITY_MIN_PER_MILLE"], "950")
        self.assertEqual(values["TD_HS_REPUTATION_IDENTITY_MAX_PER_MILLE"], "1050")

    def test_stable_env_preserves_explicit_detector_overrides(self):
        values = source_helper(
            {
                "TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE": "1",
                "TD_HS_DOUBLE_VOTE_DETECT_ENABLE": "0",
                "TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE": "1",
                "TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE": "0",
                "TD_HS_CONFLICTING_QC_DETECT_ENABLE": "1",
            }
        )

        self.assertEqual(values["TD_HS_DOUBLE_PROPOSAL_DETECT_ENABLE"], "1")
        self.assertEqual(values["TD_HS_DOUBLE_VOTE_DETECT_ENABLE"], "0")
        self.assertEqual(values["TD_HS_INVALID_QC_PROPOSAL_DETECT_ENABLE"], "1")
        self.assertEqual(
            values["TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_DETECT_ENABLE"], "0"
        )
        self.assertEqual(values["TD_HS_CONFLICTING_QC_DETECT_ENABLE"], "1")

    def test_silent_leader_ids_are_not_shared_with_all_replicas_or_clients(self):
        deploy_multi = os.path.join(REPO_ROOT, "scripts", "deploy", "script", "deploy_multi.sh")
        with open(deploy_multi) as fp:
            body = fp.read()
        shared_env_block = body.split("td_env_names=(", 1)[1].split(")", 1)[0]
        self.assertNotIn("TD_HS_SILENT_LEADER_IDS", shared_env_block)
        self.assertNotIn("TD_HS_PEERTRUST_CLIQUE_IDS", shared_env_block)
        self.assertNotIn("TD_HS_PEERTRUST_CLIQUE_TARGET_IDS", shared_env_block)
        self.assertNotIn("TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS", shared_env_block)
        self.assertNotIn("TD_HS_DOUBLE_PROPOSAL_IDS", shared_env_block)
        self.assertNotIn("TD_HS_DOUBLE_VOTE_IDS", shared_env_block)
        self.assertNotIn("TD_HS_INVALID_QC_IDS", shared_env_block)
        self.assertNotIn("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS", shared_env_block)
        self.assertIn("-u TD_HS_SILENT_LEADER_IDS", body)
        self.assertIn("-u TD_HS_PEERTRUST_CLIQUE_IDS", body)
        self.assertIn("-u TD_HS_PEERTRUST_CLIQUE_TARGET_IDS", body)
        self.assertIn("-u TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS", body)
        self.assertIn("-u TD_HS_DOUBLE_PROPOSAL_IDS", body)
        self.assertIn("-u TD_HS_DOUBLE_VOTE_IDS", body)
        self.assertIn("-u TD_HS_INVALID_QC_IDS", body)
        self.assertIn("-u TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS", body)
        self.assertIn("-u TD_HS_BAD_NODE_IDS", body)
        self.assertIn("-u TD_HS_BAD_NODE_COUNT", body)
        self.assertIn("TD_HS_SILENT_LEADER=1", body)
        self.assertIn("TD_HS_PEERTRUST_CLIQUE=1", body)
        self.assertIn("TD_HS_PEERTRUST_CLIQUE_TARGET_IDS=", body)
        self.assertIn("TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS=", body)
        self.assertIn("TD_HS_DOUBLE_PROPOSAL=1", body)
        self.assertIn("TD_HS_DOUBLE_VOTE=1", body)
        self.assertIn("TD_HS_INVALID_QC=1", body)
        self.assertIn("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION=1", body)
        self.assertIn("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_TRIGGER_FILE=", body)
        self.assertIn("td_hs_wue_trigger", body)
        self.assertIn("TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_ENABLE", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_DEBT_INCREMENT", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_DEBT_RECOVERY", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_DEBT_MAX", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_DEBT_TRIGGER_SCORE", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_SOFT_MIN_WEIGHT", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_MIN_CANDIDATE_QCS", shared_env_block)
        self.assertIn("TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_CANDIDATE", shared_env_block)
        self.assertIn("TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_INITIAL_VERSION_ONLY", shared_env_block)
        self.assertIn("TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_MIN_START_VIEW", shared_env_block)
        self.assertIn("TD_HS_STRONG_FAULT_ATTACK_START_VIEW", shared_env_block)
        self.assertIn("TD_HS_DOUBLE_PROPOSAL_START_VIEW", shared_env_block)
        self.assertIn("TD_HS_DOUBLE_VOTE_START_VIEW", shared_env_block)
        self.assertIn("TD_HS_INVALID_QC_START_VIEW", shared_env_block)
        self.assertIn("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW", shared_env_block)
        self.assertIn("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_ON_CANDIDATE", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_VOTE_BETA_DECAY_PER_MILLE", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_MULTIPLICATIVE_WEIGHT_ENABLE", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_STAKE_TAU_PER_MILLE", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_STAKE_MIN_PER_MILLE", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_STAKE_MAX_PER_MILLE", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_IDENTITY_MIN_PER_MILLE", shared_env_block)
        self.assertIn("TD_HS_REPUTATION_IDENTITY_MAX_PER_MILLE", shared_env_block)
        self.assertNotIn("TD_HS_PEERTRUST_BROAD_QC_SIGNERS", shared_env_block)
        self.assertNotIn("TD_HS_PEERTRUST_BROAD_QC_MIN_SIGNERS", shared_env_block)
        self.assertIn("is_silent_leader_node", body)
        self.assertIn("is_peertrust_clique_node", body)
        self.assertIn("is_double_proposal_node", body)
        self.assertIn("is_double_vote_node", body)
        self.assertIn("is_invalid_qc_node", body)
        self.assertIn("is_weight_update_vote_equivocation_node", body)

        for script in [
            "run_slow_leader_n20.sh",
            "run_slow_vote_n20.sh",
            "run_double_proposal_n20.sh",
            "run_double_vote_n20.sh",
            "run_invalid_qc_n20.sh",
            "run_weight_update_vote_equivocation_n20.sh",
        ]:
            with self.subTest(script=script):
                with open(os.path.join(REPO_ROOT, "scripts", "deploy", script)) as fp:
                    script_body = fp.read()
                self.assertIn("env -u TD_HS_SILENT_LEADER_IDS", script_body)
                self.assertIn("-u TD_HS_DOUBLE_PROPOSAL_IDS", script_body)
                self.assertIn("-u TD_HS_DOUBLE_VOTE_IDS", script_body)
                self.assertIn("-u TD_HS_INVALID_QC_IDS", script_body)
                self.assertIn("-u TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS", script_body)


    def test_weight_update_equivocation_runner_creates_vote_artifacts(self):
        runner = os.path.join(
            REPO_ROOT,
            "scripts",
            "deploy",
            "run_weight_update_vote_equivocation_n20.sh",
        )
        with open(runner) as fp:
            body = fp.read()
        self.assertNotIn("TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_CANDIDATE=1", body)
        self.assertNotIn("TD_HS_WEIGHT_UPDATE_ALLOW_NOOP_CANDIDATE_ON_ATTACK=1", body)
        self.assertNotIn("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_ON_CANDIDATE=1", body)
        self.assertIn('TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW="${TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_START_VIEW:-1024}"', body)
        self.assertIn(
            'TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS="${WEIGHT_UPDATE_VOTE_EQUIVOCATION_TIMEOUT_EMPTY_PROPOSAL_VIEWS:-512}"',
            body,
        )
        self.assertNotIn(
            'TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS="${TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS:-512}"',
            body,
        )
        self.assertIn('TD_HS_REPUTATION_WINDOW_SIZE="${TD_HS_REPUTATION_WINDOW_SIZE:-256}"', body)
        self.assertIn('TD_HS_REPUTATION_MIN_CANDIDATE_QCS="${TD_HS_REPUTATION_MIN_CANDIDATE_QCS:-64}"', body)
        self.assertIn('TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS="${TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS:-128}"', body)
        self.assertIn(
            'TD_HS_STRONG_FAULT_ATTACK_START_VIEW="${TD_HS_STRONG_FAULT_ATTACK_START_VIEW:-1024}"',
            body,
        )

    def test_strong_fault_attack_hooks_support_delayed_start(self):
        td_hotstuff = os.path.join(
            REPO_ROOT,
            "platform",
            "consensus",
            "ordering",
            "td_hotstuff",
            "algorithm",
            "td_hotstuff.cpp",
        )
        with open(td_hotstuff) as fp:
            body = fp.read()
        self.assertIn("StrongFaultExperimentStartedAtView", body)
        self.assertIn('"TD_HS_STRONG_FAULT_ATTACK_START_VIEW"', body)
        self.assertIn('"TD_HS_DOUBLE_PROPOSAL_START_VIEW"', body)
        self.assertIn('"TD_HS_DOUBLE_VOTE_START_VIEW"', body)
        self.assertIn('"TD_HS_INVALID_QC_START_VIEW"', body)

    def test_wue_trigger_is_armed_after_benchmark_start_gate(self):
        runner = os.path.join(REPO_ROOT, "scripts", "deploy", "run_strong_fault_v2_n20.sh")
        with open(runner) as fp:
            body = fp.read()

        arm_idx = body.index('arm_weight_update_vote_equivocation_nodes "$bad_node_ids"')
        start_idx = body.index("if ! start_benchmark_clients; then")
        self.assertGreater(arm_idx, start_idx)

    def test_double_vote_runner_uses_dedicated_empty_proposal_override(self):
        runner = os.path.join(REPO_ROOT, "scripts", "deploy", "run_double_vote_n20.sh")
        with open(runner) as fp:
            body = fp.read()
        self.assertIn(
            'TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS="${DOUBLE_VOTE_TIMEOUT_EMPTY_PROPOSAL_VIEWS:-512}"',
            body,
        )
        self.assertNotIn(
            'TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS="${TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS:-512}"',
            body,
        )

    def test_strong_fault_runner_enables_benchmark_retry(self):
        runner = os.path.join(REPO_ROOT, "scripts", "deploy", "run_strong_fault_v2_n20.sh")
        with open(runner) as fp:
            body = fp.read()
        self.assertIn(
            'TD_HS_BENCHMARK_RETRY_ENABLE="${TD_HS_BENCHMARK_RETRY_ENABLE:-1}"',
            body,
        )
        self.assertIn('TD_HS_BENCHMARK_REQUEST_TIMEOUT_MS="${TD_HS_BENCHMARK_REQUEST_TIMEOUT_MS:-100}"', body)
        self.assertIn('local max_attempts="${BENCHMARK_START_ATTEMPTS:-3}"', body)

    def test_strong_fault_runner_treats_start_gate_as_advisory_by_default(self):
        runner = os.path.join(REPO_ROOT, "scripts", "deploy", "run_strong_fault_v2_n20.sh")
        with open(runner) as fp:
            body = fp.read()
        self.assertIn('BENCHMARK_REQUIRE_START_GATE:-0', body)
        self.assertIn('Continuing; final result parser will reject unhealthy throughput.', body)
        self.assertIn('Retrying full cell deployment after benchmark-start failure.', body)

    def test_strong_fault_runner_preserves_empty_proposal_override(self):
        runner = os.path.join(REPO_ROOT, "scripts", "deploy", "run_strong_fault_v2_n20.sh")
        with open(runner) as fp:
            body = fp.read()
        self.assertIn(
            'TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS="${TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS:-20}"',
            body,
        )
        self.assertNotIn("export TD_HS_TIMEOUT_EMPTY_PROPOSAL_VIEWS=20", body)

    def test_strong_fault_runner_supports_bad_node_id_override(self):
        runner = os.path.join(REPO_ROOT, "scripts", "deploy", "run_strong_fault_v2_n20.sh")
        with open(runner) as fp:
            body = fp.read()
        self.assertIn("STRONG_FAULT_BAD_NODE_IDS_OVERRIDE", body)
        self.assertIn('printf \'%s\' "${STRONG_FAULT_BAD_NODE_IDS_OVERRIDE}"', body)
        self.assertIn("arm_weight_update_vote_equivocation_nodes", body)
        self.assertIn("TD_HS_WEIGHT_UPDATE_VOTE_EQUIVOCATION_IDS", body)
        self.assertIn("td_hs_wue_trigger", body)

    def test_strong_fault_runners_use_fast_activation_by_default(self):
        expected_defaults = {
            "run_double_proposal_n20.sh": "1",
            "run_double_vote_n20.sh": "4",
            "run_invalid_qc_n20.sh": "1",
            "run_strong_fault_v2_n20.sh": "4",
        }
        for script, expected_delay in expected_defaults.items():
            with self.subTest(script=script):
                with open(os.path.join(REPO_ROOT, "scripts", "deploy", script)) as fp:
                    body = fp.read()
                self.assertIn(
                    'TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY="${TD_HS_WEIGHT_UPDATE_ACTIVATION_EPOCH_DELAY:-'
                    + expected_delay
                    + '}"',
                    body,
                )
                self.assertIn(
                    'TD_HS_STRONG_FAULT_ATTACK_START_VIEW="${TD_HS_STRONG_FAULT_ATTACK_START_VIEW:-1024}"',
                    body,
                )

    def test_strong_fault_runners_can_enable_all_detectors(self):
        for script in [
            "run_double_proposal_n20.sh",
            "run_double_vote_n20.sh",
            "run_invalid_qc_n20.sh",
            "run_strong_fault_v2_n20.sh",
        ]:
            with self.subTest(script=script):
                with open(os.path.join(REPO_ROOT, "scripts", "deploy", script)) as fp:
                    body = fp.read()
                self.assertIn("TD_HS_ALL_DETECTORS_ENABLE", body)
                self.assertIn("td_hs_enable_all_detectors", body)
                self.assertNotIn("_DETECT_ENABLE=0", body)

    def test_peertrust_clique_runner_isolates_attack_ids(self):
        runner = os.path.join(REPO_ROOT, "scripts", "deploy", "run_peertrust_clique_n20.sh")
        with open(runner) as fp:
            body = fp.read()
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_ENABLE", body)
        self.assertIn("TD_HS_REPUTATION_LEADER_RECOVERY_ENABLE=1", body)
        self.assertIn("TD_HS_STRONG_FAULT_ENABLE=1", body)
        self.assertIn("TD_HS_PEERTRUST_CLIQUE_IDS", body)
        self.assertIn("TD_HS_PEERTRUST_CLIQUE_TARGET_IDS", body)
        self.assertIn("TD_HS_PEERTRUST_CLIQUE_START_VIEW", body)
        self.assertIn("-u TD_HS_PEERTRUST_CLIQUE_IDS", body)
        self.assertIn("-u TD_HS_PEERTRUST_CLIQUE_TARGET_IDS", body)
        self.assertIn("-u TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS", body)
        self.assertIn("TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS", body)
        self.assertIn("-u TD_HS_PEERTRUST_CLIQUE_REVIEWER_IDS", body)
        self.assertIn("derive_clique_reviewer_ids", body)
        self.assertIn("derive_clique_collector_ids", body)
        self.assertIn('export TD_HS_PEERTRUST_CLIQUE_TARGET_IDS="$bad_node_ids"', body)
        self.assertIn('export TD_HS_PEERTRUST_CLIQUE_IDS="$clique_collector_ids"', body)
        self.assertIn('TD_HS_REPUTATION_AUDIT_JSONL_ENABLE="${TD_HS_REPUTATION_AUDIT_JSONL_ENABLE:-0}"', body)
        self.assertNotIn("TD_HS_PEERTRUST_BROAD_QC_SIGNERS=1", body)
        self.assertNotIn("TD_HS_PEERTRUST_BROAD_QC_MIN_SIGNERS=", body)
        self.assertIn("unset TD_HS_REPUTATION_AUDIT_JSONL_ENABLE", body)
        self.assertIn("unset TD_HS_PEERTRUST_CLIQUE_START_VIEW", body)
        self.assertNotIn("unset TD_HS_PEERTRUST_BROAD_QC_SIGNERS", body)
        self.assertNotIn("unset TD_HS_PEERTRUST_BROAD_QC_MIN_SIGNERS", body)
        self.assertIn("PEERTRUST_CLIQUE_REVIEWER_MODE:-all", body)
        self.assertIn("group_size=16", body)
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_DEBT_TRIGGER_SCORE", body)
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_DEBT_INCREMENT", body)
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_DEBT_RECOVERY", body)
        self.assertIn("TD_HS_REPUTATION_PEERTRUST_SOFT_MIN_WEIGHT", body)
        self.assertIn("TD_HS_REPUTATION_DECAY_PER_EPOCH:-2", body)
        self.assertIn('ids="${ids},${i}"', body)

    def test_peertrust_clique_hook_is_consumed_by_td_hotstuff(self):
        td_hotstuff = os.path.join(
            REPO_ROOT,
            "platform",
            "consensus",
            "ordering",
            "td_hotstuff",
            "algorithm",
            "td_hotstuff.cpp",
        )
        with open(td_hotstuff) as fp:
            body = fp.read()
        self.assertIn('EnvFlagEnabled("TD_HS_PEERTRUST_CLIQUE")', body)
        self.assertIn('ExperimentStartedAtView("TD_HS_PEERTRUST_CLIQUE_START_VIEW"', body)
        self.assertIn('EnvListContainsId("TD_HS_PEERTRUST_CLIQUE_TARGET_IDS"', body)
        self.assertIn("SelectPeerTrustCliqueSigners(certs, view, quorum_weight)", body)
        self.assertIn("clique_selected=", body)
        self.assertIn("shared_qc_metadata=public_available_set", body)
        self.assertIn("qc->set_available_signer_bitmap", body)
        self.assertNotIn("certs.find(reviewer_id)", body)
        self.assertNotIn("PeerTrustCliqueEvidenceSigners(certs, view, quorum_weight)", body)
        self.assertNotIn("PeerTrustCliqueCertificateSigners(certs, view, quorum_weight)", body)
        self.assertNotIn('EnvFlagEnabled("TD_HS_PEERTRUST_BROAD_QC_SIGNERS")', body)
        self.assertNotIn('PositiveIntFromEnv("TD_HS_PEERTRUST_BROAD_QC_MIN_SIGNERS"', body)
        self.assertNotIn("available_signers.size() < static_cast<size_t>(min_signers)", body)
        self.assertNotIn("retry_peertrust_broad_qc", body)

    def test_double_proposal_runner_builds_comma_separated_bad_ids(self):
        runner = os.path.join(REPO_ROOT, "scripts", "deploy", "run_double_proposal_n20.sh")
        with open(runner) as fp:
            body = fp.read()
        self.assertNotIn('ids=",$ids"', body)
        self.assertIn('ids="${ids},${i}"', body)

    def test_proposal_qc_evidence_is_deferred_until_after_vote_send(self):
        with open(os.path.join(REPO_ROOT, "platform", "consensus", "ordering", "td_hotstuff", "algorithm", "td_hotstuff.cpp")) as fp:
            body = fp.read()
        vote_path_idx = body.index("int send_result = 0;")
        normal_send_idx = body.index("send_result = SendMessage(MessageType::Vote", vote_path_idx)
        slow_send_idx = body.index("SendMessage(MessageType::Vote, delayed_vote", vote_path_idx)
        record_idx = body.index("TryRecordCertifiedQc(std::move(proposal_qc_snapshot))", vote_path_idx)
        self.assertLess(normal_send_idx, record_idx)
        self.assertLess(slow_send_idx, record_idx)

    def test_slow_vote_td_hotstuff_uses_vote_only_attack_hook(self):
        runner = os.path.join(
            REPO_ROOT, "scripts", "deploy", "run_slow_vote_n20.sh")
        with open(runner) as fp:
            body = fp.read()

        self.assertIn("config_network_delay_num=0", body)
        self.assertIn("TD_HS_SLOW_VOTE_IDS", body)
        self.assertIn(
            'TD_HS_REPUTATION_BONUS_PER_EPOCH="${TD_HS_SOFT_REPUTATION_BONUS_PER_EPOCH:-4}"',
            body,
        )
        self.assertIn(
            'TD_HS_WEIGHT_UPDATE_EPOCH_VIEWS="${TD_HS_SOFT_WEIGHT_UPDATE_EPOCH_VIEWS:-2048}"',
            body,
        )

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
