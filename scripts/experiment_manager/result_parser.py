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

    number = r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?)"
    max_tput_m = re.search(r"max throughput:\s*" + number, text, re.IGNORECASE)
    avg_tput_m = re.search(r"average throughput:\s*" + number, text, re.IGNORECASE)
    max_lat_m = re.search(r"max latency:\s*" + number, text, re.IGNORECASE)
    avg_lat_m = re.search(r"average latency:\s*" + number, text, re.IGNORECASE)

    if not all([avg_tput_m, avg_lat_m]):
        raise ValueError(
            f"Could not parse average metrics from {results_log_path}. "
            f"Content:\n{text[:500]}"
        )

    avg_throughput = float(avg_tput_m.group(1))
    avg_latency = float(avg_lat_m.group(1))

    return RunResult(
        max_throughput=float(max_tput_m.group(1)) if max_tput_m else avg_throughput,
        avg_throughput=avg_throughput,
        max_latency=float(max_lat_m.group(1)) if max_lat_m else avg_latency,
        avg_latency=avg_latency,
        raw_log_path=str(results_log_path),
    )
