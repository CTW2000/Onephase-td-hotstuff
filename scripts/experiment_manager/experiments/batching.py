"""Batching experiment — effect of client batch size on throughput/latency."""

from typing import List, Optional

from ..models import ExperimentCategory, RunConfig
from ..protocols import get_protocol
from .base import Experiment


class BatchingExperiment(Experiment):
    name = "batching"
    category = ExperimentCategory.SIMPLE
    description = "Effect of client batch size on throughput/latency"

    DEFAULT_PROTOCOLS = ["HS", "HS-2", "HS-1", "HS-1-SLOT", "TD-HS"]
    DEFAULT_BATCH_SIZES = [100, 500, 1000, 2000]
    DEFAULT_REPLICAS = 20

    def __init__(
        self,
        batch_sizes: Optional[List[int]] = None,
        replicas: Optional[int] = None,
    ):
        self.batch_sizes = batch_sizes or self.DEFAULT_BATCH_SIZES
        self.replicas = replicas or self.DEFAULT_REPLICAS

    def generate_runs(self, protocols: Optional[List[str]] = None) -> List[RunConfig]:
        protocols = protocols or self.DEFAULT_PROTOCOLS
        runs = []
        for protocol in protocols:
            info = get_protocol(protocol)
            for batch in self.batch_sizes:
                runs.append(RunConfig(
                    protocol=protocol,
                    replicas=self.replicas,
                    config_overrides={
                        "max_process_txn": info.max_process_txn,
                        "clientBatchNum": batch,
                        "timer_length": 100,
                    },
                ))
        return runs
