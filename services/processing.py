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
    client_ip = addr[0]
    id_req = int(msg['id_req'])
    value = int(msg['value'])

    # lock garante que dois clientes simultâneos não corrompam o estado global
    with state.lock:

        # cria um cadastro
        client = state.clients.get(addr)
        if client is None:
            client = ClientState(address=addr)
            state.clients[addr] = client

        if id_req == client.last_req + 1:
            # requisição nova e em ordem = processa normalmente
            state.add_value(value)
            
            # salva snapshot do estado global
            # usado para remontar o ACK caso esse pedido chegue duplicado depois
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
            # duplicata: o ACK anterior se perdeu e o cliente retransmitiu
            # não soma de novo — apenas reenvia o ACK guardado
            print(
                f"{timestamp()} client {client_ip} DUP!! id_req {id_req} value {value} "
                f"num_reqs {client.last_num_reqs} total_sum {client.last_total_sum}"
            )
            sys.stdout.flush()

            ack = protocol.make_ack(
                client.last_req, client.last_num_reqs, client.last_total_sum
            )

        else:
            # fora de ordem: responde onde o servidor parou para o cliente se sincronizar
            ack = protocol.make_ack(
                client.last_req, client.last_num_reqs, client.last_total_sum
            )

    udp.send(sock, ack, addr)
