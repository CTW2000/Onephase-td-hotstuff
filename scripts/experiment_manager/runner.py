"""Runner abstraction — execute benchmarks locally or on AWS."""

import os
import subprocess
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from .config_gen import generate_local_conf, generate_protocol_config, generate_remote_conf
from .models import ProtocolInfo, RunConfig, RunResult
from .protocols import get_protocol
from .result_parser import parse_results_log


class Runner(ABC):
    def __init__(self, deploy_dir: str):
        self.deploy_dir = os.path.abspath(deploy_dir)

    @abstractmethod
    def execute(self, run_config: RunConfig, protocol_info: ProtocolInfo) -> RunResult:
        ...

    def cleanup_stale_processes(self) -> None:
        """Kill leftover processes from previous runs."""
        for proc_name in ["kv_server_performance", "kv_service_tools",
                          "kv_service", "kv_server"]:
            subprocess.run(
                ["killall", "-9", proc_name],
                capture_output=True,
                cwd=self.deploy_dir,
            )
        time.sleep(2)


class LocalRunner(Runner):
    """Runs benchmarks on localhost using performance_local/*.sh scripts."""

    def execute(self, run_config: RunConfig, protocol_info: ProtocolInfo) -> RunResult:
        self.cleanup_stale_processes()

        config_path = os.path.join(self.deploy_dir, protocol_info.config_path)
        conf_output = os.path.join(
            self.deploy_dir, "config", "performance_local_gen.conf"
        )
        results_path = os.path.join(self.deploy_dir, "results.log")

        merged_overrides = {"max_process_txn": protocol_info.max_process_txn}
        merged_overrides.update(run_config.config_overrides)
        generate_protocol_config(config_path, **merged_overrides)
        generate_local_conf(run_config.replicas, conf_output)

        env = os.environ.copy()
        env["BENCH_DURATION"] = str(run_config.bench_duration)
        env.update(run_config.env_vars)

        cmd = f"{protocol_info.local_script} {conf_output}"
        proc = subprocess.run(
            cmd,
            shell=True,
            cwd=self.deploy_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=run_config.bench_duration + 300,
        )

        if not os.path.exists(results_path):
            raise RuntimeError(
                f"No results.log produced.\n"
                f"stdout: {proc.stdout[-2000:]}\n"
                f"stderr: {proc.stderr[-2000:]}"
            )

        return parse_results_log(results_path)


class RemoteRunner(Runner):
    """Runs benchmarks on AWS using performance/*.sh scripts."""

    def __init__(
        self,
        deploy_dir: str,
        ssh_key: str,
        machines_file: str,
        region: str = "us-east-1",
    ):
        super().__init__(deploy_dir)
        self.ssh_key = os.path.expanduser(ssh_key)
        self.machines_file = os.path.join(self.deploy_dir, machines_file)
        self.region = region

    def setup_instances(self, replica_count: int) -> None:
        """Start AWS instances for the experiment."""
        start_script = os.path.join(
            self.deploy_dir, f"start_{self.region.replace('-', '_')}_instances.sh"
        )
        if os.path.exists(start_script):
            subprocess.run(
                [start_script, str(replica_count)],
                cwd=self.deploy_dir,
                check=True,
            )
            time.sleep(30)

    def teardown_instances(self) -> None:
        """Stop AWS instances."""
        stop_script = os.path.join(
            self.deploy_dir, f"stop_{self.region.replace('-', '_')}_instances.sh"
        )
        if os.path.exists(stop_script):
            subprocess.run(
                [stop_script],
                cwd=self.deploy_dir,
                check=True,
            )

    def execute(self, run_config: RunConfig, protocol_info: ProtocolInfo) -> RunResult:
        config_path = os.path.join(self.deploy_dir, protocol_info.config_path)
        conf_output = os.path.join(
            self.deploy_dir, "config", "performance_remote_gen.conf"
        )
        results_path = os.path.join(self.deploy_dir, "results.log")

        merged_overrides = {"max_process_txn": protocol_info.max_process_txn}
        merged_overrides.update(run_config.config_overrides)
        generate_protocol_config(config_path, **merged_overrides)
        generate_remote_conf(
            run_config.replicas, self.machines_file, conf_output
        )

        env = os.environ.copy()
        env["BENCH_DURATION"] = str(run_config.bench_duration)
        env["key"] = self.ssh_key
        env.update(run_config.env_vars)

        cmd = f"{protocol_info.remote_script} {conf_output}"
        proc = subprocess.run(
            cmd,
            shell=True,
            cwd=self.deploy_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=run_config.bench_duration + 600,
        )

        if not os.path.exists(results_path):
            raise RuntimeError(
                f"No results.log produced.\n"
                f"stdout: {proc.stdout[-2000:]}\n"
                f"stderr: {proc.stderr[-2000:]}"
            )

        return parse_results_log(results_path)
