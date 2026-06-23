import json
from typing import Any, Dict

MSG_DISCOVERY = "DISCOVERY"                    # client → broadcast
MSG_DISCOVERY_RESPONSE = "DISCOVERY_RESPONSE"  # server → client
MSG_REQUEST = "REQUEST"                        # client → server: value to add
MSG_ACK = "ACK"                                # server → client: confirms processing

# Part 2 — replication and leader election
MSG_HEARTBEAT     = "HEARTBEAT"      # primary → each backup: liveness probe
MSG_HEARTBEAT_ACK = "HEARTBEAT_ACK"  # backup → primary: liveness reply
MSG_REPLICATE     = "REPLICATE"      # primary → backups: full state after each sum
MSG_REPLICATE_ACK = "REPLICATE_ACK"  # backup → primary: replication confirmed
MSG_ELECTION      = "ELECTION"       # RM → higher-ID RMs: start bully election
MSG_OK            = "OK"             # higher-ID RM → initiator: "I'm alive, stand down"
MSG_COORDINATOR   = "COORDINATOR"    # winner → all RMs: "I am the new leader"
MSG_NEW_LEADER    = "NEW_LEADER"     # new primary → known clients: redirect to new address


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


def make_heartbeat(rm_id: int) -> bytes:
    return encode({"type": MSG_HEARTBEAT, "rm_id": rm_id})


def make_heartbeat_ack(rm_id: int) -> bytes:
    return encode({"type": MSG_HEARTBEAT_ACK, "rm_id": rm_id})


def make_replicate(payload: dict) -> bytes:
    return encode({"type": MSG_REPLICATE, **payload})


def make_replicate_ack(rm_id: int) -> bytes:
    return encode({"type": MSG_REPLICATE_ACK, "rm_id": rm_id})


def make_election(rm_id: int) -> bytes:
    return encode({"type": MSG_ELECTION, "rm_id": rm_id})


def make_ok(rm_id: int) -> bytes:
    return encode({"type": MSG_OK, "rm_id": rm_id})


def make_coordinator(rm_id: int, ip: str, port: int) -> bytes:
    return encode({"type": MSG_COORDINATOR, "rm_id": rm_id, "ip": ip, "port": port})


def make_new_leader(ip: str, port: int) -> bytes:
    return encode({"type": MSG_NEW_LEADER, "ip": ip, "port": port})
