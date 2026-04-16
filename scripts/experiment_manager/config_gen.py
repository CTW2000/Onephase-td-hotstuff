"""Config generation — wraps the logic from deploy/local_experiment.py."""

import json
from pathlib import Path


CONFIG_DEFAULTS = {
    "clientBatchNum": 100,
    "enable_viewchange": False,
    "recovery_enabled": False,
    "max_client_complaint_num": 10,
    "max_process_txn": 3,
    "worker_num": 8,
    "input_worker_num": 5,
    "output_worker_num": 5,
    "non_responsive_num": 0,
    "fork_tail_num": 0,
    "rollback_num": 0,
    "tpcc_enabled": False,
    "network_delay_num": 0,
    "mean_network_delay": 0,
    "timer_length": 10,
}


def generate_protocol_config(config_path: str, **overrides) -> None:
    """Write a JSON protocol config file.

    Matches the format produced by deploy/local_experiment.py:generate_config().
    Values are written without quotes for numbers/bools (matching the existing format).
    """
    params = dict(CONFIG_DEFAULTS)
    params.update(overrides)

    with open(config_path, "w") as f:
        f.write("{\n")
        items = list(params.items())
        for i, (key, value) in enumerate(items):
            if isinstance(value, bool):
                val_str = "true" if value else "false"
            else:
                val_str = str(value)
            comma = "," if i < len(items) - 1 else ""
            f.write(f'  "{key}": {val_str}{comma}\n')
        f.write("}\n")


def generate_local_conf(
    replica_count: int,
    output_file: str = "./config/performance_local_gen.conf",
) -> str:
    """Write a localhost IP-list conf file (N replicas + 1 client).

    Matches deploy/local_experiment.py:generate_local_conf().
    """
    with open(output_file, "w") as f:
        f.write("iplist=(\n")
        for _ in range(replica_count + 1):
            f.write("127.0.0.1\n")
        f.write(")\n\nclient_num=1\n")
    return output_file


def generate_remote_conf(
    replica_count: int,
    machines_file: str,
    output_file: str = "./config/performance_remote_gen.conf",
    client_num: int = 1,
) -> str:
    """Write a remote IP-list conf file from a machines list.

    Reads IPs from machines_file and selects the first replica_count + client_num.
    """
    ips = Path(machines_file).read_text().strip().splitlines()
    needed = replica_count + client_num
    if len(ips) < needed:
        raise ValueError(
            f"Need {needed} machines but {machines_file} only has {len(ips)}"
        )

    with open(output_file, "w") as f:
        f.write("iplist=(\n")
        for ip in ips[:needed]:
            f.write(f"{ip.strip()}\n")
        f.write(f")\n\nclient_num={client_num}\n")
    return output_file
