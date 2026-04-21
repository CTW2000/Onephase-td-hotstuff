"""Protocol registry — single source of truth for all consensus protocols."""

from .models import ProtocolInfo

PROTOCOLS = {
    "HS": ProtocolInfo(
        name="HS",
        local_script="./performance_local/hs_performance.sh",
        remote_script="./performance/hs_performance.sh",
        config_path="./config/hs.config",
        max_process_txn=5,
    ),
    "HS-1": ProtocolInfo(
        name="HS-1",
        local_script="./performance_local/hs1_performance.sh",
        remote_script="./performance/hs1_performance.sh",
        config_path="./config/hs1.config",
        max_process_txn=3,
    ),
    "HS-2": ProtocolInfo(
        name="HS-2",
        local_script="./performance_local/hs2_performance.sh",
        remote_script="./performance/hs2_performance.sh",
        config_path="./config/hs2.config",
        max_process_txn=4,
    ),
    "HS-1-SLOT": ProtocolInfo(
        name="HS-1-SLOT",
        local_script="./performance_local/slot_hs1_performance.sh",
        remote_script="./performance/slot_hs1_performance.sh",
        config_path="./config/slot_hs1.config",
        max_process_txn=3,
    ),
    "TD-HS": ProtocolInfo(
        name="TD-HS",
        local_script="./performance_local/td_hotstuff_performance.sh",
        remote_script="./performance/td_hotstuff_performance.sh",
        config_path="./config/td_hotstuff.config",
        max_process_txn=3,
    ),
    "PBFT": ProtocolInfo(
        name="PBFT",
        local_script="./performance_local/pbft_performance.sh",
        remote_script="./performance/pbft_performance.sh",
        config_path="./config/pbft.config",
        max_process_txn=2048,
    ),
}

ALL_PROTOCOL_NAMES = list(PROTOCOLS.keys())


def get_protocol(name: str) -> ProtocolInfo:
    if name not in PROTOCOLS:
        raise ValueError(
            f"Unknown protocol: {name}. Available: {ALL_PROTOCOL_NAMES}"
        )
    return PROTOCOLS[name]
