"""Deterministic TD-HotStuff weight profiles for experiment sweeps."""

import random
from dataclasses import dataclass
from typing import Dict, List, Optional

from .models import RunConfig


TD_HS_PROTOCOL = "TD-HS"
TD_HS_WEIGHTS_ENV = "TD_HS_WEIGHTS"
DEFAULT_BYZANTINE_WEIGHT_SEEDS = [1, 2, 3]
DEFAULT_MIN_WEIGHT = 1
DEFAULT_MAX_WEIGHT = 3


@dataclass(frozen=True)
class WeightProfile:
    """One deterministic TD-HS weight profile.

    An empty ``weights`` list means the protocol should use its built-in
    uniform weight path. Non-empty weights are passed with ``TD_HS_WEIGHTS``.
    """

    tag: str
    weights: List[int]

    @property
    def env_vars(self) -> Dict[str, str]:
        if not self.weights:
            return {}
        return {TD_HS_WEIGHTS_ENV: weights_to_env(self.weights)}


def generate_weight_vector(
    replicas: int,
    seed: int,
    min_weight: int = DEFAULT_MIN_WEIGHT,
    max_weight: int = DEFAULT_MAX_WEIGHT,
) -> List[int]:
    """Generate a deterministic positive integer weight vector."""
    if replicas <= 0:
        raise ValueError(f"replicas must be positive, got {replicas}")
    if min_weight <= 0:
        raise ValueError(f"min_weight must be positive, got {min_weight}")
    if max_weight < min_weight:
        raise ValueError(
            f"max_weight must be >= min_weight, got {max_weight} < {min_weight}"
        )

    rng = random.Random(seed)
    return [rng.randint(min_weight, max_weight) for _ in range(replicas)]


def weights_to_env(weights: List[int]) -> str:
    """Serialize a validated weight vector for TD_HS_WEIGHTS."""
    if not weights:
        raise ValueError("weights must not be empty")
    if any(weight <= 0 for weight in weights):
        raise ValueError(f"weights must be positive integers, got {weights}")
    return ",".join(str(weight) for weight in weights)


def tag_with_prefix(tag: str, tag_prefix: str = "") -> str:
    """Attach a stable scenario prefix without changing existing tag behavior."""
    if not tag_prefix:
        return tag
    if not tag:
        return tag_prefix
    return f"{tag_prefix}__{tag}"


def td_hs_weight_profiles(
    replicas: int,
    seeds: Optional[List[int]] = None,
    include_uniform: bool = True,
    min_weight: int = DEFAULT_MIN_WEIGHT,
    max_weight: int = DEFAULT_MAX_WEIGHT,
) -> List[WeightProfile]:
    """Build TD-HS uniform and seeded weighted profiles for one replica count."""
    selected_seeds = DEFAULT_BYZANTINE_WEIGHT_SEEDS if seeds is None else seeds
    profiles: List[WeightProfile] = []

    if include_uniform:
        profiles.append(WeightProfile(tag="weights=uniform", weights=[]))

    for seed in selected_seeds:
        profiles.append(
            WeightProfile(
                tag=f"weights=seed{seed}",
                weights=generate_weight_vector(
                    replicas=replicas,
                    seed=seed,
                    min_weight=min_weight,
                    max_weight=max_weight,
                ),
            )
        )

    return profiles


def expand_weight_profiles_for_protocol(
    protocol: str,
    replicas: int,
    config_overrides: dict,
    bench_duration: int = 40,
    td_hs_weight_seeds: Optional[List[int]] = None,
    tag_prefix: str = "",
) -> List[RunConfig]:
    """Return RunConfigs, expanding TD-HS into uniform + weighted profiles."""
    if protocol != TD_HS_PROTOCOL:
        return [
            RunConfig(
                protocol=protocol,
                replicas=replicas,
                config_overrides=dict(config_overrides),
                bench_duration=bench_duration,
            )
        ]

    return [
        RunConfig(
            protocol=protocol,
            replicas=replicas,
            config_overrides=dict(config_overrides),
            bench_duration=bench_duration,
            env_vars=profile.env_vars,
            tag=tag_with_prefix(profile.tag, tag_prefix),
        )
        for profile in td_hs_weight_profiles(
            replicas=replicas,
            seeds=td_hs_weight_seeds,
            include_uniform=True,
        )
    ]
