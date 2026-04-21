"""Pipeline orchestrator — ordered execution, regression gating, resume."""

import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from .checkpoint import PipelineCheckpoint
from .experiments.base import Experiment
from .metric_checker import MetricChecker
from .models import ExperimentCategory, RunConfig, RunResult
from .protocols import get_protocol
from .runner import Runner


class Pipeline:
    def __init__(
        self,
        experiments: List[Experiment],
        runner: Runner,
        metric_checker: MetricChecker,
        checkpoint_path: str,
        results_dir: str,
        runner_mode: str = "local",
        stop_on_regression: bool = True,
    ):
        self.experiments = experiments
        self.runner = runner
        self.metric_checker = metric_checker
        self.checkpoint_path = os.path.abspath(checkpoint_path)
        self.results_dir = os.path.abspath(results_dir)
        self.runner_mode = runner_mode
        self.stop_on_regression = stop_on_regression
        self.checkpoint = self._load_or_create_checkpoint()
        self.metric_checker.baselines = self.checkpoint.baselines

    def _load_or_create_checkpoint(self) -> PipelineCheckpoint:
        if os.path.exists(self.checkpoint_path):
            print(f"  Resuming from checkpoint: {self.checkpoint_path}")
            return PipelineCheckpoint.load(self.checkpoint_path)
        pipeline_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        return PipelineCheckpoint(
            pipeline_id=pipeline_id,
            runner_mode=self.runner_mode,
        )

    def run(self, protocols: Optional[List[str]] = None) -> bool:
        """Execute all experiments in order. Returns True if all passed."""
        total_start = time.time()
        total_runs = 0
        completed_runs = 0
        skipped_runs = 0
        failed_runs = 0

        os.makedirs(self.results_dir, exist_ok=True)

        for experiment in self.experiments:
            print(f"\n{'=' * 60}")
            print(f"  Experiment: {experiment.name} ({experiment.category.value})")
            print(f"  {experiment.description}")
            print(f"{'=' * 60}")

            runs = experiment.generate_runs(protocols=protocols)
            if not runs:
                print(f"  No runs to execute (protocol filter excluded all)")
                continue

            exp_results_dir = os.path.join(self.results_dir, experiment.name)
            os.makedirs(exp_results_dir, exist_ok=True)

            for i, run_config in enumerate(runs, 1):
                total_runs += 1
                run_id = run_config.run_id

                if self.checkpoint.is_completed(run_id):
                    print(f"  [{i}/{len(runs)}] SKIP {run_id} (completed)")
                    skipped_runs += 1
                    continue

                print(f"\n  [{i}/{len(runs)}] Running: {run_id}")
                print(f"    Protocol: {run_config.protocol}")
                print(f"    Replicas: {run_config.replicas}")
                print(f"    Config: {run_config.config_overrides}")
                if run_config.env_vars:
                    print(f"    Env: {run_config.env_vars}")

                self.checkpoint.mark_running(run_id, experiment.name)
                self.checkpoint.save(self.checkpoint_path)

                run_start = time.time()
                try:
                    protocol_info = get_protocol(run_config.protocol)
                    result = self.runner.execute(run_config, protocol_info)
                    elapsed = time.time() - run_start

                    self._save_result_artifact(
                        exp_results_dir, run_config, result
                    )

                    print(f"    Throughput: avg={result.avg_throughput:.1f} max={result.max_throughput:.1f}")
                    print(f"    Latency:    avg={result.avg_latency:.6f} max={result.max_latency:.6f}")
                    print(f"    Duration:   {elapsed:.1f}s")

                    if experiment.category == ExperimentCategory.SIMPLE:
                        self.checkpoint.baselines[run_id] = result.to_dict()
                        self.metric_checker.baselines = self.checkpoint.baselines

                    passed, reason = self.metric_checker.check(run_id, result)
                    if not passed:
                        print(f"    REGRESSION: {reason}")
                        self.checkpoint.mark_failed(run_id, f"regression: {reason}")
                        self.checkpoint.save(self.checkpoint_path)
                        failed_runs += 1
                        if self.stop_on_regression:
                            self._print_summary(
                                total_runs, completed_runs, skipped_runs,
                                failed_runs, total_start, stopped=True,
                            )
                            return False
                    else:
                        print(f"    PASSED")
                        self.checkpoint.mark_completed(run_id, result)
                        self.checkpoint.save(self.checkpoint_path)
                        completed_runs += 1

                except Exception as e:
                    elapsed = time.time() - run_start
                    print(f"    FAILED ({elapsed:.1f}s): {e}")
                    self.checkpoint.mark_failed(run_id, str(e))
                    self.checkpoint.save(self.checkpoint_path)
                    failed_runs += 1
                    if self.stop_on_regression:
                        self._print_summary(
                            total_runs, completed_runs, skipped_runs,
                            failed_runs, total_start, stopped=True,
                        )
                        return False

        self._print_summary(
            total_runs, completed_runs, skipped_runs,
            failed_runs, total_start, stopped=False,
        )
        return failed_runs == 0

    def _save_result_artifact(
        self,
        exp_results_dir: str,
        run_config: RunConfig,
        result: RunResult,
    ) -> None:
        """Copy results.log to the experiment results directory."""
        if result.raw_log_path and os.path.exists(result.raw_log_path):
            dest = os.path.join(exp_results_dir, f"{run_config.run_id}.log")
            shutil.copy2(result.raw_log_path, dest)

    def _print_summary(
        self,
        total: int,
        completed: int,
        skipped: int,
        failed: int,
        start_time: float,
        stopped: bool,
    ) -> None:
        elapsed = time.time() - start_time
        status = "STOPPED (regression detected)" if stopped else "COMPLETED"
        print(f"\n{'=' * 60}")
        print(f"  Pipeline {status}")
        print(f"  Total: {total} | Completed: {completed} | Skipped: {skipped} | Failed: {failed}")
        print(f"  Elapsed: {elapsed:.1f}s")
        print(f"  Checkpoint: {self.checkpoint_path}")
        if stopped:
            print(f"\n  Fix the issue, then resume with:")
            print(f"    python -m experiment_manager resume --checkpoint {self.checkpoint_path}")
        print(f"{'=' * 60}")
