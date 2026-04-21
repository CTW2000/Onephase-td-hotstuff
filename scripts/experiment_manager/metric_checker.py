"""Metric regression detection — threshold checks and baseline comparison."""

from dataclasses import dataclass
from typing import Optional

from .models import RunResult


@dataclass
class MetricThresholds:
    min_avg_throughput: Optional[float] = None
    max_avg_latency: Optional[float] = None
    max_throughput_drop_pct: Optional[float] = None
    max_latency_increase_pct: Optional[float] = None


class MetricChecker:
    def __init__(
        self,
        thresholds: MetricThresholds,
        baselines: Optional[dict] = None,
    ):
        self.thresholds = thresholds
        self.baselines: dict[str, dict] = baselines or {}

    def check(self, run_id: str, result: RunResult) -> tuple:
        """Check a result against thresholds and baselines.

        Returns (passed: bool, reason: str). reason is empty if passed.
        """
        t = self.thresholds

        if t.min_avg_throughput is not None and result.avg_throughput < t.min_avg_throughput:
            return False, (
                f"avg throughput {result.avg_throughput:.1f} "
                f"below minimum {t.min_avg_throughput:.1f}"
            )

        if t.max_avg_latency is not None and result.avg_latency > t.max_avg_latency:
            return False, (
                f"avg latency {result.avg_latency:.6f} "
                f"exceeds maximum {t.max_avg_latency:.6f}"
            )

        baseline = self._find_baseline(run_id)
        if baseline:
            baseline_tput = baseline.get("avg_throughput", 0)
            baseline_lat = baseline.get("avg_latency", 0)

            if t.max_throughput_drop_pct is not None and baseline_tput > 0:
                drop_pct = (
                    (baseline_tput - result.avg_throughput) / baseline_tput * 100
                )
                if drop_pct > t.max_throughput_drop_pct:
                    return False, (
                        f"throughput dropped {drop_pct:.1f}% from baseline "
                        f"({baseline_tput:.1f} -> {result.avg_throughput:.1f}), "
                        f"threshold: {t.max_throughput_drop_pct:.1f}%"
                    )

            if t.max_latency_increase_pct is not None and baseline_lat > 0:
                increase_pct = (
                    (result.avg_latency - baseline_lat) / baseline_lat * 100
                )
                if increase_pct > t.max_latency_increase_pct:
                    return False, (
                        f"latency increased {increase_pct:.1f}% from baseline "
                        f"({baseline_lat:.6f} -> {result.avg_latency:.6f}), "
                        f"threshold: {t.max_latency_increase_pct:.1f}%"
                    )

        return True, ""

    def _find_baseline(self, run_id: str) -> Optional[dict]:
        """Find a baseline result for comparison.

        First tries exact match, then tries matching by protocol + replica count
        (ignoring fault-injection parameters).
        """
        if run_id in self.baselines:
            return self.baselines[run_id]

        parts = run_id.split("__")
        if len(parts) >= 2:
            prefix = "__".join(parts[:2])
            for baseline_id, baseline_result in self.baselines.items():
                if baseline_id.startswith(prefix):
                    return baseline_result

        return None
