"""Two-attacker experiment — rollback plus combined fork/slow-leader attacks."""

from typing import List, Optional, Tuple

from ..models import ExperimentCategory, RunConfig
from ..protocols import get_protocol
from ..weight_profiles import expand_weight_profiles_for_protocol
from .base import Experiment


class TwoAttackerExperiment(Experiment):
    name = "two_attacker"
    category = ExperimentCategory.BYZANTINE
    description = "Rollback and combined tail-fork plus slow-leader attacks"

    DEFAULT_PROTOCOLS = ["HS", "HS-2", "HS-1", "HS-1-SLOT", "TD-HS"]
    DEFAULT_ROLLBACK_COUNTS = [0, 1, 4, 6]
    DEFAULT_COMBINED_PAIRS = [(0, 0), (1, 1), (2, 2), (4, 4)]
    DEFAULT_REPLICAS = 20
    DEFAULT_TIMER_LENGTH = 100

    def __init__(
        self,
        rollback_counts: Optional[List[int]] = None,
        combined_pairs: Optional[List[Tuple[int, int]]] = None,
        replicas: Optional[int] = None,
        timer_length: int = DEFAULT_TIMER_LENGTH,
    ):
        self.rollback_counts = rollback_counts or self.DEFAULT_ROLLBACK_COUNTS
        self.combined_pairs = combined_pairs or self.DEFAULT_COMBINED_PAIRS
        self.replicas = replicas or self.DEFAULT_REPLICAS
        self.timer_length = timer_length

    def generate_runs(self, protocols: Optional[List[str]] = None) -> List[RunConfig]:
        protocols = protocols or self.DEFAULT_PROTOCOLS
        runs = []
        for protocol in protocols:
            info = get_protocol(protocol)
            for rollback_count in self.rollback_counts:
                runs.extend(expand_weight_profiles_for_protocol(
                    protocol=protocol,
                    replicas=self.replicas,
                    config_overrides={
                        "max_process_txn": info.max_process_txn,
                        "rollback_num": rollback_count,
                        "timer_length": self.timer_length,
                    },
                    tag_prefix="attack=rollback",
                ))

            for fork_tail_num, slow_num in self.combined_pairs:
                runs.extend(expand_weight_profiles_for_protocol(
                    protocol=protocol,
                    replicas=self.replicas,
                    config_overrides={
                        "max_process_txn": info.max_process_txn,
                        "fork_tail_num": fork_tail_num,
                        "non_responsive_num": slow_num,
                        "timer_length": self.timer_length,
                    },
                    tag_prefix="attack=combined",
                ))
        return runs
