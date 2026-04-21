"""Command-line interface for the experiment manager."""

import argparse
import os
import sys

from .checkpoint import PipelineCheckpoint
from .experiments.registry import (
    ALL_EXPERIMENT_NAMES,
    ALL_SUITE_NAMES,
    EXPERIMENT_CLASSES,
    SUITES,
    resolve_experiments,
)
from .metric_checker import MetricChecker, MetricThresholds
from .pipeline import Pipeline
from .protocols import ALL_PROTOCOL_NAMES
from .runner import LocalRunner, RemoteRunner


def find_deploy_dir() -> str:
    """Locate scripts/deploy/ relative to this package."""
    here = os.path.dirname(os.path.abspath(__file__))
    deploy_dir = os.path.join(os.path.dirname(here), "deploy")
    if os.path.isdir(deploy_dir):
        return deploy_dir
    raise RuntimeError(
        f"Cannot find scripts/deploy/ directory. "
        f"Expected at: {deploy_dir}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="experiment_manager",
        description="Experiment pipeline for Onephase-td-hotstuff consensus protocols",
    )
    sub = parser.add_subparsers(dest="command")

    # --- run ---
    run_p = sub.add_parser("run", help="Run experiments")
    run_p.add_argument(
        "experiments", nargs="+",
        help=(
            f"Experiment or suite names. "
            f"Experiments: {ALL_EXPERIMENT_NAMES}. "
            f"Suites: {ALL_SUITE_NAMES}."
        ),
    )
    _add_common_args(run_p)

    # --- resume ---
    resume_p = sub.add_parser("resume", help="Resume from checkpoint")
    _add_common_args(resume_p)

    # --- status ---
    status_p = sub.add_parser("status", help="Show checkpoint status")
    status_p.add_argument(
        "--checkpoint", default="./experiment_checkpoint.json",
        help="Path to checkpoint file",
    )

    # --- list ---
    sub.add_parser("list", help="List experiments, suites, and protocols")

    # --- reset ---
    reset_p = sub.add_parser("reset", help="Reset failed entries to pending")
    reset_p.add_argument(
        "--checkpoint", default="./experiment_checkpoint.json",
        help="Path to checkpoint file",
    )
    reset_p.add_argument(
        "--experiment",
        help="Only reset entries for this experiment",
    )

    return parser


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    """Add args shared between 'run' and 'resume' commands."""
    parser.add_argument(
        "--protocols", nargs="+", choices=ALL_PROTOCOL_NAMES,
        help="Restrict to specific protocols",
    )
    parser.add_argument(
        "--mode", choices=["local", "remote"], default="local",
        help="Run locally or on AWS (default: local)",
    )
    parser.add_argument(
        "--checkpoint", default="./experiment_checkpoint.json",
        help="Checkpoint file path (default: ./experiment_checkpoint.json)",
    )
    parser.add_argument(
        "--results-dir", default="./experiment_results",
        help="Results output directory (default: ./experiment_results)",
    )
    parser.add_argument(
        "--no-stop-on-regression", action="store_true",
        help="Continue running even if regression is detected",
    )
    parser.add_argument(
        "--min-throughput", type=float, default=None,
        help="Absolute minimum avg throughput (fail if below)",
    )
    parser.add_argument(
        "--max-latency", type=float, default=None,
        help="Absolute maximum avg latency (fail if above)",
    )
    parser.add_argument(
        "--max-throughput-drop", type=float, default=50.0,
        help="Max throughput drop %% from baseline before regression (default: 50)",
    )
    parser.add_argument(
        "--max-latency-increase", type=float, default=None,
        help="Max latency increase %% from baseline before regression",
    )
    parser.add_argument(
        "--bench-duration", type=int, default=40,
        help="Benchmark duration in seconds (default: 40)",
    )
    # Remote-specific
    parser.add_argument(
        "--ssh-key", default="~/hs1-ari.pem",
        help="SSH key for remote deployment",
    )
    parser.add_argument(
        "--machines-file", default="./config/us-east-1-machines",
        help="Machines list file for remote deployment",
    )
    parser.add_argument(
        "--region", default="us-east-1",
        help="AWS region for remote deployment",
    )


def cmd_run(args: argparse.Namespace) -> int:
    deploy_dir = find_deploy_dir()
    experiments = resolve_experiments(args.experiments)

    runner = _create_runner(args, deploy_dir)
    checker = _create_metric_checker(args)

    pipeline = Pipeline(
        experiments=experiments,
        runner=runner,
        metric_checker=checker,
        checkpoint_path=args.checkpoint,
        results_dir=args.results_dir,
        runner_mode=args.mode,
        stop_on_regression=not args.no_stop_on_regression,
    )

    success = pipeline.run(protocols=args.protocols)
    return 0 if success else 1


def cmd_resume(args: argparse.Namespace) -> int:
    if not os.path.exists(args.checkpoint):
        print(f"No checkpoint file found at {args.checkpoint}")
        return 1

    checkpoint = PipelineCheckpoint.load(args.checkpoint)

    # Determine which experiments had entries
    experiment_names = []
    seen = set()
    for entry in checkpoint.entries.values():
        if entry.experiment_name not in seen:
            seen.add(entry.experiment_name)
            experiment_names.append(entry.experiment_name)

    if not experiment_names:
        print("Checkpoint has no entries to resume.")
        return 0

    experiments = resolve_experiments(experiment_names)
    deploy_dir = find_deploy_dir()
    runner = _create_runner(args, deploy_dir)
    checker = _create_metric_checker(args)

    pipeline = Pipeline(
        experiments=experiments,
        runner=runner,
        metric_checker=checker,
        checkpoint_path=args.checkpoint,
        results_dir=args.results_dir,
        runner_mode=args.mode,
        stop_on_regression=not args.no_stop_on_regression,
    )

    success = pipeline.run(protocols=args.protocols)
    return 0 if success else 1


def cmd_status(args: argparse.Namespace) -> int:
    if not os.path.exists(args.checkpoint):
        print(f"No checkpoint file at {args.checkpoint}")
        return 1

    checkpoint = PipelineCheckpoint.load(args.checkpoint)
    summary = checkpoint.summary()
    exp_summary = checkpoint.experiment_summary()

    print(f"Pipeline: {checkpoint.pipeline_id}")
    print(f"Mode:     {checkpoint.runner_mode}")
    print(f"Created:  {checkpoint.created_at}")
    print(f"Updated:  {checkpoint.updated_at}")
    print()
    print("Overall:")
    for status, count in sorted(summary.items()):
        print(f"  {status}: {count}")
    print()
    print("By experiment:")
    for exp_name, statuses in exp_summary.items():
        status_str = ", ".join(f"{s}: {c}" for s, c in sorted(statuses.items()))
        print(f"  {exp_name}: {status_str}")

    # Show failed entries
    failed = [e for e in checkpoint.entries.values() if e.status.value == "failed"]
    if failed:
        print(f"\nFailed runs ({len(failed)}):")
        for entry in failed:
            print(f"  {entry.run_id}")
            if entry.error_message:
                print(f"    -> {entry.error_message}")

    return 0


def cmd_list(args: argparse.Namespace) -> int:
    print("Experiments:")
    for name, cls in EXPERIMENT_CLASSES.items():
        exp = cls()
        print(f"  {name:25s} [{exp.category.value:10s}] {exp.description}")

    print(f"\nSuites:")
    for name, experiments in SUITES.items():
        print(f"  {name:25s} {', '.join(experiments)}")

    print(f"\nProtocols:")
    for name in ALL_PROTOCOL_NAMES:
        print(f"  {name}")

    return 0


def cmd_reset(args: argparse.Namespace) -> int:
    if not os.path.exists(args.checkpoint):
        print(f"No checkpoint file at {args.checkpoint}")
        return 1

    checkpoint = PipelineCheckpoint.load(args.checkpoint)
    count = checkpoint.reset_failed(experiment_name=args.experiment)
    checkpoint.save(args.checkpoint)

    scope = f"experiment '{args.experiment}'" if args.experiment else "all experiments"
    print(f"Reset {count} failed entries to pending ({scope})")
    return 0


def _create_runner(args: argparse.Namespace, deploy_dir: str):
    if args.mode == "remote":
        return RemoteRunner(
            deploy_dir=deploy_dir,
            ssh_key=args.ssh_key,
            machines_file=args.machines_file,
            region=args.region,
        )
    return LocalRunner(deploy_dir=deploy_dir)


def _create_metric_checker(args: argparse.Namespace) -> MetricChecker:
    thresholds = MetricThresholds(
        min_avg_throughput=args.min_throughput,
        max_avg_latency=args.max_latency,
        max_throughput_drop_pct=args.max_throughput_drop,
        max_latency_increase_pct=getattr(args, "max_latency_increase", None),
    )
    return MetricChecker(thresholds=thresholds)


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    commands = {
        "run": cmd_run,
        "resume": cmd_resume,
        "status": cmd_status,
        "list": cmd_list,
        "reset": cmd_reset,
    }

    handler = commands.get(args.command)
    if handler:
        sys.exit(handler(args))
    else:
        parser.print_help()
        sys.exit(1)
