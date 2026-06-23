import socket
from typing import TYPE_CHECKING

from network import protocol, udp
from models.server_state import ServerState
from models.client_state import ClientState

if TYPE_CHECKING:
    from models.rm_state import RMState


def state_to_dict(ss: ServerState) -> dict:
    # state.clients is keyed by client_id string ("ip:port"); addr is in ClientState.address
    clients = {}
    for client_id, cs in ss.clients.items():
        addr = cs.address
        clients[client_id] = {
            "address":        [addr[0], addr[1]],
            "last_req":       cs.last_req,
            "last_num_reqs":  cs.last_num_reqs,
            "last_total_sum": cs.last_total_sum,
        }
    return {
        "num_reqs":  ss.num_reqs,
        "total_sum": ss.total_sum,
        "clients":   clients,
    }


def apply_replicate(msg: dict, ss: ServerState) -> None:
    with ss.lock:
        ss.num_reqs  = msg["num_reqs"]
        ss.total_sum = msg["total_sum"]
        ss.clients   = {}
        for client_id, cd in msg.get("clients", {}).items():
            addr_list = cd.get("address", client_id.rsplit(":", 1))
            addr = (addr_list[0], int(addr_list[1]))
            cs = ClientState(address=addr)
            cs.last_req       = cd["last_req"]
            cs.last_num_reqs  = cd["last_num_reqs"]
            cs.last_total_sum = cd["last_total_sum"]
            ss.clients[client_id] = cs


def replicate_to_backups(
    sock: socket.socket,
    ss: ServerState,
    rm: "RMState",
) -> None:
    with ss.lock:
        payload = state_to_dict(ss)
    msg = protocol.make_replicate(payload)
    for addr in rm.peers.values():
        try:
            udp.send(sock, msg, addr)
        except OSError:
            pass  # backup temporarily unreachable — continue
