"""Tail-forking attack experiment for one-phase protocols."""

from typing import List, Optional

from ..models import ExperimentCategory, RunConfig
from ..protocols import get_protocol
from ..weight_profiles import expand_weight_profiles_for_protocol
from .base import Experiment


class TailForkingExperiment(Experiment):
    name = "tail_forking"
    category = ExperimentCategory.BYZANTINE
    description = "Performance under tail-forking attack"

    DEFAULT_PROTOCOLS = ["HS-1", "HS-1-SLOT", "TD-HS"]
    DEFAULT_FAULTY_COUNTS = [0, 1]
    DEFAULT_DELAYS = [10, 100]
    DEFAULT_REPLICAS = 20

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
                    runs.extend(expand_weight_profiles_for_protocol(
                        protocol=protocol,
                        replicas=self.replicas,
                        config_overrides={
                            "max_process_txn": info.max_process_txn,
                            "fork_tail_num": num_faulty,
                            "timer_length": delay,
                        },
                    ))
        return runs
