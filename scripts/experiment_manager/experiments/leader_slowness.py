"""Leader slowness experiment — performance under artificially slow leaders."""

from typing import List, Optional

from ..models import ExperimentCategory, RunConfig
from ..protocols import get_protocol
from .base import Experiment


class LeaderSlownessExperiment(Experiment):
    name = "leader_slowness"
    category = ExperimentCategory.BYZANTINE
    description = "Performance under artificially slow leaders"

    DEFAULT_PROTOCOLS = ["HS", "HS-2", "HS-1", "HS-1-SLOT", "TD-HS"]
    DEFAULT_SLOW_COUNTS = [0, 1]
    DEFAULT_DELAYS = [10, 100]
    DEFAULT_REPLICAS = 20

    def __init__(
        self,
        slow_counts: Optional[List[int]] = None,
        delays: Optional[List[int]] = None,
        replicas: Optional[int] = None,
    ):
        self.slow_counts = slow_counts or self.DEFAULT_SLOW_COUNTS
        self.delays = delays or self.DEFAULT_DELAYS
        self.replicas = replicas or self.DEFAULT_REPLICAS

    def generate_runs(self, protocols: Optional[List[str]] = None) -> List[RunConfig]:
        protocols = protocols or self.DEFAULT_PROTOCOLS
        runs = []
        for delay in self.delays:
            for protocol in protocols:
                info = get_protocol(protocol)
                for num_slow in self.slow_counts:
                    runs.append(RunConfig(
                        protocol=protocol,
                        replicas=self.replicas,
                        config_overrides={
                            "max_process_txn": info.max_process_txn,
                            "non_responsive_num": num_slow,
                            "timer_length": delay,
                        },
                    ))
        return runs
