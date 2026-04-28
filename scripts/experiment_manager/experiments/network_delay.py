"""Network delay experiment — tolerance to artificial delay on replicas."""

from typing import List, Optional

from ..models import ExperimentCategory, RunConfig
from ..protocols import get_protocol
from ..weight_profiles import expand_weight_profiles_for_protocol
from .base import Experiment


class NetworkDelayExperiment(Experiment):
    name = "network_delay"
    category = ExperimentCategory.BYZANTINE
    description = "Tolerance to artificial network delay on replicas"

    DEFAULT_PROTOCOLS = ["HS", "HS-2", "HS-1", "HS-1-SLOT", "TD-HS"]
    DEFAULT_DELAYS = [1, 50]
    DEFAULT_IMPACTED_COUNTS = [0, 1, 2]
    DEFAULT_REPLICAS = 20

    def __init__(
        self,
        delays: Optional[List[int]] = None,
        impacted_counts: Optional[List[int]] = None,
        replicas: Optional[int] = None,
    ):
        self.delays = delays or self.DEFAULT_DELAYS
        self.impacted_counts = impacted_counts or self.DEFAULT_IMPACTED_COUNTS
        self.replicas = replicas or self.DEFAULT_REPLICAS

    def generate_runs(self, protocols: Optional[List[str]] = None) -> List[RunConfig]:
        protocols = protocols or self.DEFAULT_PROTOCOLS
        runs = []
        for delay in self.delays:
            for protocol in protocols:
                info = get_protocol(protocol)
                for num_impacted in self.impacted_counts:
                    runs.extend(expand_weight_profiles_for_protocol(
                        protocol=protocol,
                        replicas=self.replicas,
                        config_overrides={
                            "max_process_txn": info.max_process_txn,
                            "network_delay_num": num_impacted,
                            "mean_network_delay": delay,
                        },
                    ))
        return runs
