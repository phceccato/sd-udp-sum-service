import threading
import time
from enum import Enum
from typing import Dict, Optional, Tuple


class Role(Enum):
    PRIMARY   = "primary"
    BACKUP    = "backup"
    CANDIDATE = "candidate"


class RMState:
    def __init__(
        self,
        rm_id: int,
        my_ip: str,
        my_port: int,
        peers: Dict[int, Tuple[str, int]],
    ):
        self.rm_id   = rm_id
        self.my_ip   = my_ip
        self.my_port = my_port
        self.peers   = peers  # {rm_id: (ip, port)} — does NOT include self

        # The RM with the highest ID starts as the primary
        all_ids   = set(peers.keys()) | {rm_id}
        leader_id = max(all_ids)
        self.leader_id: int = leader_id
        self.leader_addr: Tuple[str, int] = (
            peers[leader_id] if leader_id != rm_id else (my_ip, my_port)
        )
        self.role: Role = Role.PRIMARY if rm_id == leader_id else Role.BACKUP

        # Backups update this every time a HEARTBEAT arrives from the primary
        self.last_heartbeat: float = time.monotonic()

        # Election state
        self.election_in_progress: bool = False
        self.received_ok: bool = False
        self.victory_timer: Optional[threading.Timer] = None
        self.coordinator_timer: Optional[threading.Timer] = None

        self.lock = threading.Lock()
