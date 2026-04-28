"""Slow-vote experiment — delayed vote/message path under network delay."""

from typing import List, Optional

from ..models import ExperimentCategory, RunConfig
from ..protocols import get_protocol
from ..weight_profiles import expand_weight_profiles_for_protocol
from .base import Experiment


class SlowVoteExperiment(Experiment):
    name = "slow_vote"
    category = ExperimentCategory.BYZANTINE
    description = "Performance when vote traffic is slowed by network delay"

    DEFAULT_PROTOCOLS = ["HS", "HS-2", "HS-1", "HS-1-SLOT", "TD-HS"]
    DEFAULT_SLOW_VOTE_COUNTS = [0, 1, 4, 6]
    DEFAULT_MEAN_DELAY_MS = 10
    DEFAULT_REPLICAS = 20

    def __init__(
        self,
        slow_vote_counts: Optional[List[int]] = None,
        mean_delay_ms: int = DEFAULT_MEAN_DELAY_MS,
        replicas: Optional[int] = None,
    ):
        self.slow_vote_counts = slow_vote_counts or self.DEFAULT_SLOW_VOTE_COUNTS
        self.mean_delay_ms = mean_delay_ms
        self.replicas = replicas or self.DEFAULT_REPLICAS

    def generate_runs(self, protocols: Optional[List[str]] = None) -> List[RunConfig]:
        protocols = protocols or self.DEFAULT_PROTOCOLS
        runs = []
        for protocol in protocols:
            info = get_protocol(protocol)
            for num_slow in self.slow_vote_counts:
                runs.extend(expand_weight_profiles_for_protocol(
                    protocol=protocol,
                    replicas=self.replicas,
                    config_overrides={
                        "max_process_txn": info.max_process_txn,
                        "network_delay_num": num_slow,
                        "mean_network_delay": self.mean_delay_ms,
                    },
                ))
        return runs
