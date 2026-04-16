"""Rollback attack experiment — HS-1 and HS-1-SLOT only."""

from typing import List, Optional

from ..models import ExperimentCategory, RunConfig
from ..protocols import get_protocol
from .base import Experiment


class RollbackExperiment(Experiment):
    name = "rollback"
    category = ExperimentCategory.BYZANTINE
    description = "Performance under rollback attack"

    DEFAULT_PROTOCOLS = ["HS-1", "HS-1-SLOT"]
    DEFAULT_FAULTY_COUNTS = [0, 1]
    DEFAULT_DELAYS = [10, 100]
    DEFAULT_REPLICAS = 4

    def __init__(
        self,
        faulty_counts: Optional[List[int]] = None,
        delays: Optional[List[int]] = None,
        replicas: Optional[int] = None,
    ):
        self.faulty_counts = faulty_counts or self.DEFAULT_FAULTY_COUNTS
        self.delays = delays or self.DEFAULT_DELAYS
        self.replicas = replicas or self.DEFAULT_REPLICAS

    def generate_runs(self, protocols: Optional[List[str]] = None) -> List[RunConfig]:
        protocols = protocols or self.DEFAULT_PROTOCOLS
        valid = [p for p in protocols if p in self.DEFAULT_PROTOCOLS]
        if not valid:
            return []
        runs = []
        for delay in self.delays:
            for protocol in valid:
                info = get_protocol(protocol)
                for num_faulty in self.faulty_counts:
                    runs.append(RunConfig(
                        protocol=protocol,
                        replicas=self.replicas,
                        config_overrides={
                            "max_process_txn": info.max_process_txn,
                            "rollback_num": num_faulty,
                            "timer_length": delay,
                        },
                    ))
        return runs
