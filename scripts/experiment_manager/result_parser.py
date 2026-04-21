"""Parse results.log files produced by calculate_result.py."""

import re
from pathlib import Path

from .models import RunResult


def parse_results_log(results_log_path: str) -> RunResult:
    """Parse a results.log file into a RunResult.

    Expected format (from performance_local/calculate_result.py):
        calculate results, number of nodes: 11
        max throughput: 110000
        average throughput: 96594.80
        max latency: 0.00364656
        average latency: 0.00332434
    """
    text = Path(results_log_path).read_text()

    max_tput_m = re.search(r"max throughput:\s*([\d.]+)", text)
    avg_tput_m = re.search(r"average throughput:\s*([\d.]+)", text)
    max_lat_m = re.search(r"max latency:\s*([\d.]+)", text)
    avg_lat_m = re.search(r"average latency:\s*([\d.]+)", text)

    if not all([max_tput_m, avg_tput_m, max_lat_m, avg_lat_m]):
        raise ValueError(
            f"Could not parse all metrics from {results_log_path}. "
            f"Content:\n{text[:500]}"
        )

    return RunResult(
        max_throughput=float(max_tput_m.group(1)),
        avg_throughput=float(avg_tput_m.group(1)),
        max_latency=float(max_lat_m.group(1)),
        avg_latency=float(avg_lat_m.group(1)),
        raw_log_path=str(results_log_path),
    )
