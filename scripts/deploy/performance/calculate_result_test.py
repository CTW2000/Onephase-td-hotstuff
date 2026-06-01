import importlib.util
import pathlib
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("calculate_result.py")
SPEC = importlib.util.spec_from_file_location("calculate_result", MODULE_PATH)
calculate_result = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(calculate_result)


class CalculateResultTest(unittest.TestCase):
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
