"""Protocol comparison experiment — TD-HS random seeds + baseline protocols."""

import random
from typing import List, Optional

from ..models import ExperimentCategory, RunConfig
from ..protocols import get_protocol
from .base import Experiment


class ProtocolComparisonExperiment(Experiment):
    name = "protocol_comparison"
    category = ExperimentCategory.WEIGHTED
    description = "TD-HotStuff random weight seeds vs. baseline protocols"

    DEFAULT_REPLICAS = 10
    DEFAULT_SEEDS = list(range(1, 21))  # 20 random seeds
    DEFAULT_BASELINES = ["HS", "HS-1", "HS-2", "HS-1-SLOT", "PBFT"]
    DEFAULT_MIN_WEIGHT = 1
    DEFAULT_MAX_WEIGHT = 3
    DEFAULT_DURATION = 30

    def __init__(
        self,
        replicas: Optional[int] = None,
        seeds: Optional[List[int]] = None,
        baselines: Optional[List[str]] = None,
        duration: int = DEFAULT_DURATION,
    ):
        self.replicas = replicas or self.DEFAULT_REPLICAS
        self.seeds = seeds or self.DEFAULT_SEEDS
        self.baselines = baselines or self.DEFAULT_BASELINES
        self.duration = duration

    def generate_runs(self, protocols: Optional[List[str]] = None) -> List[RunConfig]:
        runs = []

        # TD-HS runs with random weights per seed
        for seed in self.seeds:
            rng = random.Random(seed)
            weights = [
                rng.randint(self.DEFAULT_MIN_WEIGHT, self.DEFAULT_MAX_WEIGHT)
                for _ in range(self.replicas)
            ]
            weights_str = ",".join(str(w) for w in weights)

            runs.append(RunConfig(
                protocol="TD-HS",
                replicas=self.replicas,
                config_overrides={"max_process_txn": 3},
                bench_duration=self.duration,
                env_vars={"TD_HS_WEIGHTS": weights_str},
                tag=f"seed{seed}",
            ))

        # Baseline protocol runs
        baselines = protocols if protocols else self.baselines
        for protocol in baselines:
            if protocol == "TD-HS":
                continue
            info = get_protocol(protocol)
            runs.append(RunConfig(
                protocol=protocol,
                replicas=self.replicas,
                config_overrides={"max_process_txn": info.max_process_txn},
                bench_duration=self.duration,
            ))

        return runs
