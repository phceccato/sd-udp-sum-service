from typing import Tuple


class ClientState:
    def __init__(self, address: Tuple[str, int]):
        self.address = address
        self.last_req: int = 0        # last successfully processed request id
        self.last_num_reqs: int = 0   # request count snapshot at last processed request
        self.last_total_sum: int = 0  # accumulated sum snapshot at last processed request
