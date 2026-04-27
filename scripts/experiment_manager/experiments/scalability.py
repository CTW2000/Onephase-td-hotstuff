"""Scalability experiment — throughput/latency vs. replica count."""

from typing import List, Optional

from ..models import ExperimentCategory, RunConfig
from ..protocols import get_protocol
from .base import Experiment


class ScalabilityExperiment(Experiment):
    name = "scalability"
    category = ExperimentCategory.SIMPLE
    description = "Throughput/latency vs. replica count"

    DEFAULT_PROTOCOLS = ["HS", "HS-2", "HS-1", "HS-1-SLOT", "TD-HS"]
    DEFAULT_REPLICA_COUNTS = [5, 15, 20]

    def __init__(self, replica_counts: Optional[List[int]] = None):
        self.replica_counts = replica_counts or self.DEFAULT_REPLICA_COUNTS

    def generate_runs(self, protocols: Optional[List[str]] = None) -> List[RunConfig]:
        protocols = protocols or self.DEFAULT_PROTOCOLS
        runs = []
        for protocol in protocols:
            info = get_protocol(protocol)
            for n in self.replica_counts:
                runs.append(RunConfig(
                    protocol=protocol,
                    replicas=n,
                    config_overrides={"max_process_txn": info.max_process_txn},
                ))
        return runs
