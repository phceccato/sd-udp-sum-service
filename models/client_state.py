from typing import Tuple


class ClientState:
    """Per-client state tracked by the server for exactly-once delivery."""

    def __init__(self, address: Tuple[str, int]):
        self.address = address
        self.last_req: int = 0        # highest id_req successfully processed
        self.last_num_reqs: int = 0   # global num_reqs snapshot when last_req was processed
        self.last_total_sum: int = 0  # global total_sum snapshot when last_req was processed
