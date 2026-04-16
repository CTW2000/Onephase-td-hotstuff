"""Experiment registry and suite definitions."""

from .scalability import ScalabilityExperiment
from .batching import BatchingExperiment
from .leader_slowness import LeaderSlownessExperiment
from .network_delay import NetworkDelayExperiment
from .rollback import RollbackExperiment
from .tail_forking import TailForkingExperiment
from .weighted import WeightedExperiment
from .protocol_comparison import ProtocolComparisonExperiment
from .base import Experiment

EXPERIMENT_CLASSES = {
    "scalability": ScalabilityExperiment,
    "batching": BatchingExperiment,
    "leader_slowness": LeaderSlownessExperiment,
    "network_delay": NetworkDelayExperiment,
    "rollback": RollbackExperiment,
    "tail_forking": TailForkingExperiment,
    "weighted": WeightedExperiment,
    "protocol_comparison": ProtocolComparisonExperiment,
}

SUITES = {
    "simple": ["scalability", "batching"],
    "byzantine": ["leader_slowness", "network_delay", "rollback", "tail_forking"],
    "all": [
        "scalability", "batching",
        "leader_slowness", "network_delay", "rollback", "tail_forking",
    ],
    "full": [
        "scalability", "batching",
        "leader_slowness", "network_delay", "rollback", "tail_forking",
        "weighted", "protocol_comparison",
    ],
}

ALL_EXPERIMENT_NAMES = list(EXPERIMENT_CLASSES.keys())
ALL_SUITE_NAMES = list(SUITES.keys())


def resolve_experiments(names: list) -> list:
    """Resolve a list of experiment/suite names into Experiment instances.

    If a name is a suite name, it expands to the experiments in that suite.
    If a name is an experiment name, it instantiates that experiment.
    Preserves order. Deduplicates while keeping first occurrence.
    """
    seen = set()
    experiments = []

    for name in names:
        if name in SUITES:
            for exp_name in SUITES[name]:
                if exp_name not in seen:
                    seen.add(exp_name)
                    experiments.append(EXPERIMENT_CLASSES[exp_name]())
        elif name in EXPERIMENT_CLASSES:
            if name not in seen:
                seen.add(name)
                experiments.append(EXPERIMENT_CLASSES[name]())
        else:
            raise ValueError(
                f"Unknown experiment or suite: {name!r}. "
                f"Available experiments: {ALL_EXPERIMENT_NAMES}. "
                f"Available suites: {ALL_SUITE_NAMES}."
            )

    return experiments
