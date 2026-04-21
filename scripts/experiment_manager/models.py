"""Core data types shared across all experiment manager modules."""

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional
import time


class ExperimentCategory(str, Enum):
    SIMPLE = "simple"
    BYZANTINE = "byzantine"
    WEIGHTED = "weighted"


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class ProtocolInfo:
    name: str
    local_script: str
    remote_script: str
    config_path: str
    max_process_txn: int


@dataclass
class RunConfig:
    """One single benchmark invocation (one point in the parameter sweep)."""

    protocol: str
    replicas: int
    config_overrides: dict = field(default_factory=dict)
    bench_duration: int = 40
    env_vars: dict = field(default_factory=dict)
    tag: str = ""  # optional disambiguator for runs with same config but different env

    @property
    def run_id(self) -> str:
        parts = [self.protocol, f"n{self.replicas}"]
        for k, v in sorted(self.config_overrides.items()):
            parts.append(f"{k}={v}")
        if self.tag:
            parts.append(self.tag)
        return "__".join(parts)


@dataclass
class RunResult:
    max_throughput: float
    avg_throughput: float
    max_latency: float
    avg_latency: float
    raw_log_path: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "RunResult":
        return cls(**d)


@dataclass
class CheckpointEntry:
    run_id: str
    experiment_name: str
    status: RunStatus
    result: Optional[RunResult] = None
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        d = {
            "run_id": self.run_id,
            "experiment_name": self.experiment_name,
            "status": self.status.value,
            "error_message": self.error_message,
        }
        if self.result:
            d["result"] = self.result.to_dict()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "CheckpointEntry":
        result = None
        if d.get("result"):
            result = RunResult.from_dict(d["result"])
        return cls(
            run_id=d["run_id"],
            experiment_name=d["experiment_name"],
            status=RunStatus(d["status"]),
            result=result,
            error_message=d.get("error_message"),
        )
