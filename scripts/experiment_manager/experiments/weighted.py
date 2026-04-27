"""Weighted experiment — TD-HotStuff with non-uniform per-replica weights."""

import random
from typing import List, Optional

from ..models import ExperimentCategory, RunConfig
from .base import Experiment


class WeightedExperiment(Experiment):
    name = "weighted"
    category = ExperimentCategory.WEIGHTED
    description = "TD-HotStuff with non-uniform per-replica weights"

    DEFAULT_REPLICAS = 20
    DEFAULT_SEEDS = [1, 2, 3, 4, 5]
    DEFAULT_MIN_WEIGHT = 1
    DEFAULT_MAX_WEIGHT = 3
    DEFAULT_DURATION = 30

    def __init__(
        self,
        replicas: Optional[int] = None,
        seeds: Optional[List[int]] = None,
        min_weight: int = DEFAULT_MIN_WEIGHT,
        max_weight: int = DEFAULT_MAX_WEIGHT,
        duration: int = DEFAULT_DURATION,
    ):
        self.replicas = replicas or self.DEFAULT_REPLICAS
        self.seeds = seeds or self.DEFAULT_SEEDS
        self.min_weight = min_weight
        self.max_weight = max_weight
        self.duration = duration

    def generate_runs(self, protocols: Optional[List[str]] = None) -> List[RunConfig]:
        # Weighted experiment is always TD-HS only
        runs = []
        for seed in self.seeds:
            rng = random.Random(seed)
            weights = [
                rng.randint(self.min_weight, self.max_weight)
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
        return runs
