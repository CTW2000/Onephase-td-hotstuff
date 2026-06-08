import importlib.util
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("calculate_result.py")
SPEC = importlib.util.spec_from_file_location("calculate_result", MODULE_PATH)
calculate_result = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(calculate_result)


class CalculateResultTest(unittest.TestCase):
    def test_timestamped_logs_exclude_warmup_samples(self):
        old_warmup = calculate_result.RESULT_WARMUP_SECONDS
        calculate_result.RESULT_WARMUP_SECONDS = 30
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                log_path = pathlib.Path(temp_dir) / "node.log"
                log_path.write_text(
                    "\n".join(
                        [
                            "I20260608 10:00:00.000000 txn:100 req client latency:0.010",
                            "I20260608 10:00:10.000000 txn:200 req client latency:0.020",
                            "I20260608 10:00:31.000000 txn:300 req client latency:0.030",
                            "I20260608 10:00:40.000000 txn:400 req client latency:0.040",
                        ]
                    )
                    + "\n"
                )

                samples = calculate_result.read_tps(str(log_path))

            self.assertEqual(samples.warmup_tps, [100, 200])
            self.assertEqual(samples.stable_tps, [300, 400])
            self.assertEqual(samples.warmup_lat, [0.010, 0.020])
            self.assertEqual(samples.stable_lat, [0.030, 0.040])
            self.assertFalse(samples.stable_window_fallback)
        finally:
            calculate_result.RESULT_WARMUP_SECONDS = old_warmup

    def test_timestampless_logs_drop_first_sample_ratio(self):
        old_ratio = calculate_result.RESULT_WARMUP_SAMPLE_RATIO
        calculate_result.RESULT_WARMUP_SAMPLE_RATIO = 0.20
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                log_path = pathlib.Path(temp_dir) / "node.log"
                log_path.write_text(
                    "\n".join(
                        [
                            "txn:100 req client latency:0.010",
                            "txn:200 req client latency:0.020",
                            "txn:300 req client latency:0.030",
                            "txn:400 req client latency:0.040",
                            "txn:500 req client latency:0.050",
                        ]
                    )
                    + "\n"
                )

                samples = calculate_result.read_tps(str(log_path))

            self.assertEqual(samples.warmup_tps, [100])
            self.assertEqual(samples.stable_tps, [200, 300, 400, 500])
            self.assertEqual(samples.warmup_lat, [0.010])
            self.assertEqual(samples.stable_lat, [0.020, 0.030, 0.040, 0.050])
            self.assertFalse(samples.stable_window_fallback)
        finally:
            calculate_result.RESULT_WARMUP_SAMPLE_RATIO = old_ratio

    def test_stable_window_falls_back_to_raw_when_no_samples_remain(self):
        old_warmup = calculate_result.RESULT_WARMUP_SECONDS
        calculate_result.RESULT_WARMUP_SECONDS = 30
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                log_path = pathlib.Path(temp_dir) / "node.log"
                log_path.write_text(
                    "I20260608 10:00:00.000000 txn:100 req client latency:0.010\n"
                )

                samples = calculate_result.read_tps(str(log_path))

            self.assertEqual(samples.warmup_tps, [100])
            self.assertEqual(samples.stable_tps, [100])
            self.assertEqual(samples.warmup_lat, [0.010])
            self.assertEqual(samples.stable_lat, [0.010])
            self.assertTrue(samples.stable_window_fallback)
        finally:
            calculate_result.RESULT_WARMUP_SECONDS = old_warmup

    def test_final_numeric_output_uses_stable_averages(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = pathlib.Path(temp_dir) / "node.log"
            log_path.write_text(
                "\n".join(
                    [
                        "I20260608 10:00:00.000000 txn:100 req client latency:0.010",
                        "I20260608 10:00:10.000000 txn:200 req client latency:0.020",
                        "I20260608 10:00:31.000000 txn:300 req client latency:0.030",
                        "I20260608 10:00:40.000000 txn:400 req client latency:0.040",
                    ]
                )
                + "\n"
            )
            env = os.environ.copy()
            env["TD_HS_RESULT_WARMUP_SECONDS"] = "30"

            output = subprocess.check_output(
                [sys.executable, str(MODULE_PATH), str(log_path)],
                env=env,
                text=True,
            )

        numeric_lines = [
            line for line in output.splitlines()
            if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", line)
        ]
        self.assertEqual(float(numeric_lines[-2]), 350.0)
        self.assertAlmostEqual(float(numeric_lines[-1]), 0.035)

    def test_splits_throughput_after_bad_nodes_drop_below_threshold(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = pathlib.Path(temp_dir) / "node.log"
            log_path.write_text(
                "\n".join(
                    [
                        "txn:100 time:5",
                        "activated TD-Hotstuff weight update version:3 view:1536 "
                        "active_weights:[1,1,30,30] leader_profile_activation_view:2048",
                        "txn:300 time:10",
                        "txn:500 time:15",
                    ]
                )
                + "\n"
            )

            samples = calculate_result.read_tps(
                str(log_path), bad_node_count=2, eligible_min_weight=10
            )

        self.assertEqual(samples.tps, [100, 300, 500])
        self.assertEqual(samples.before_threshold_tps, [100])
        self.assertEqual(samples.after_threshold_tps, [300, 500])
        self.assertTrue(samples.threshold_seen)

    def test_supports_non_contiguous_bad_node_ids(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = pathlib.Path(temp_dir) / "node.log"
            log_path.write_text(
                "\n".join(
                    [
                        "txn:100 time:5",
                        "activated TD-Hotstuff weight update version:3 view:1536 "
                        "active_weights:[1,30,30,1] leader_profile_activation_view:2048",
                        "txn:300 time:10",
                    ]
                )
                + "\n"
            )

            samples = calculate_result.read_tps(
                str(log_path), bad_node_ids=[1, 4], eligible_min_weight=10
            )

        self.assertEqual(samples.before_threshold_tps, [100])
        self.assertEqual(samples.after_threshold_tps, [300])


if __name__ == "__main__":
    unittest.main()
