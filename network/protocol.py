import json
from typing import Any, Dict

MSG_DISCOVERY = "DISCOVERY"                    # client → broadcast
MSG_DISCOVERY_RESPONSE = "DISCOVERY_RESPONSE"  # server → client
MSG_REQUEST = "REQUEST"                        # client → server: value to add
MSG_ACK = "ACK"                                # server → client: confirms processing


def encode(msg: Dict[str, Any]) -> bytes:
    return json.dumps(msg).encode('utf-8')


def decode(data: bytes) -> Dict[str, Any]:
    return json.loads(data.decode('utf-8'))


def make_discovery() -> bytes:
    return encode({"type": MSG_DISCOVERY})


def make_discovery_response(server_ip: str, port: int) -> bytes:
    return encode({"type": MSG_DISCOVERY_RESPONSE, "server_ip": server_ip, "port": port})


def make_request(client_id: str, id_req: int, value: int) -> bytes:
    # id_req is the sequence number that enables exactly-once delivery
    return encode({"type": MSG_REQUEST, "client_id": client_id, "id_req": id_req, "value": value})


def make_ack(id_req: int, num_reqs: int, total_sum: int) -> bytes:
    return encode({"type": MSG_ACK, "id_req": id_req, "num_reqs": num_reqs, "total_sum": total_sum})
