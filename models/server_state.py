import threading
from typing import Dict, Tuple

from models.client_state import ClientState

_UINT64_MASK = (1 << 64) - 1


class ServerState:
    def __init__(self):
        self.num_reqs: int = 0
        self.total_sum: int = 0
        self.clients: Dict[Tuple[str, int], ClientState] = {}
        self.lock = threading.Lock()

    def add_value(self, value: int) -> None:
        # wrap sum to uint64
        self.total_sum = (self.total_sum + value) & _UINT64_MASK
        self.num_reqs += 1
