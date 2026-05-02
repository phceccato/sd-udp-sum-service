import socket
import sys
from datetime import datetime
from typing import Dict, Tuple

from network import protocol, udp
from models.client_state import ClientState
from models.server_state import ServerState


def timestamp() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def server_handle_request(
    sock: socket.socket,
    addr: Tuple[str, int],
    msg: Dict,
    state: ServerState,
) -> None:
    """
    Apply exactly-once semantics:
      id_req == last_req + 1  -> new request: process and cache ACK
      id_req <= last_req      -> duplicate: replay cached ACK
      id_req >  last_req + 1  -> out-of-order: send NAK (ACK of last good req)
    """
    client_ip = addr[0]
    id_req = int(msg['id_req'])
    value = int(msg['value'])

    with state.lock:
        client = state.clients.get(addr)
        if client is None:
            client = ClientState(address=addr)
            state.clients[addr] = client

        if id_req == client.last_req + 1:
            state.add_value(value)
            client.last_req = id_req
            client.last_num_reqs = state.num_reqs
            client.last_total_sum = state.total_sum

            print(
                f"{timestamp()} client {client_ip} id_req {id_req} value {value} "
                f"num_reqs {state.num_reqs} total_sum {state.total_sum}"
            )
            sys.stdout.flush()

            ack = protocol.make_ack(id_req, state.num_reqs, state.total_sum)

        elif id_req <= client.last_req:
            # Duplicate: resend the cached ACK, do NOT modify global state
            print(
                f"{timestamp()} client {client_ip} DUP!! id_req {id_req} value {value} "
                f"num_reqs {client.last_num_reqs} total_sum {client.last_total_sum}"
            )
            sys.stdout.flush()

            ack = protocol.make_ack(
                client.last_req, client.last_num_reqs, client.last_total_sum
            )

        else:
            # Out-of-order (gap): tell the client where we are so it can retry
            ack = protocol.make_ack(
                client.last_req, client.last_num_reqs, client.last_total_sum
            )

    udp.send(sock, ack, addr)
