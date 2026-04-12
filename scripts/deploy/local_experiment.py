"""
Local experiment helper for running HotStuff-1 benchmarks on a single machine.
Generates protocol configs and local IP-list configs, then returns the command to run.
"""
import argparse
import os
import sys

PROTOCOLS = {
    "HS":        {"script": "./performance_local/hs_performance.sh",        "config": "./config/hs.config",        "max_process_txn": 5},
    "HS-1":      {"script": "./performance_local/hs1_performance.sh",       "config": "./config/hs1.config",       "max_process_txn": 3},
    "HS-2":      {"script": "./performance_local/hs2_performance.sh",       "config": "./config/hs2.config",       "max_process_txn": 4},
    "HS-1-SLOT": {"script": "./performance_local/slot_hs1_performance.sh",  "config": "./config/slot_hs1.config",  "max_process_txn": 3},
    "TD-HS":     {"script": "./performance_local/td_hotstuff_performance.sh","config": "./config/td_hotstuff.config","max_process_txn": 4},
    "PBFT":      {"script": "./performance_local/pbft_performance.sh",      "config": "./config/pbft.config",      "max_process_txn": 2048},
}

def generate_config(config_path, **kwargs):
    defaults = {
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
    defaults.update(kwargs)

    with open(config_path, "w") as f:
        f.write("{\n")
        items = list(defaults.items())
        for i, (key, value) in enumerate(items):
            if isinstance(value, bool):
                val_str = "true" if value else "false"
            else:
                val_str = str(value)
            comma = "," if i < len(items) - 1 else ""
            f.write(f'  "{key}": {val_str}{comma}\n')
        f.write("}\n")

def generate_local_conf(replica_count, output_file="./config/performance_local_gen.conf"):
    # replica_count replicas + 1 client
    with open(output_file, "w") as f:
        f.write("iplist=(\n")
        for _ in range(replica_count + 1):
            f.write("127.0.0.1\n")
        f.write(")\n\nclient_num=1\n")
    return output_file

def get_protocol_info(protocol):
    if protocol not in PROTOCOLS:
        print(f"Unknown protocol: {protocol}. Available: {list(PROTOCOLS.keys())}", file=sys.stderr)
        sys.exit(1)
    return PROTOCOLS[protocol]
