import socket
import sys
from typing import Optional, Tuple
from models.client_state import ClientState

from network import protocol, udp

BROADCAST_ADDR = '255.255.255.255'
DISCOVERY_TIMEOUT = 3.0     # timeout de descoberta
DISCOVERY_RETRIES = 10      # quantidade de tentativas de descoberta


def get_local_ip_for_peer(peer_ip: str) -> str:
    """Ask the OS which local IP would be used to reach peer_ip."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect((peer_ip, 80))
            return s.getsockname()[0]
        except OSError:
            return '127.0.0.1'


def client_discover(sock: socket.socket, port: int) -> Optional[Tuple[str, int]]:
    """
    Broadcast DISCOVERY on the given port and wait for a DISCOVERY_RESPONSE.
    Returns (server_ip, server_port) or None on failure.
    """
    msg = protocol.make_discovery()
    for attempt in range(1, DISCOVERY_RETRIES + 1):
        udp.send(sock, msg, (BROADCAST_ADDR, port))
        try:
            data, _addr = udp.receive(sock, timeout=DISCOVERY_TIMEOUT)
            response = protocol.decode(data)
            if response.get('type') == protocol.MSG_DISCOVERY_RESPONSE:
                return response['server_ip'], response['port']
        except socket.timeout:
            print(f"Discovery attempt {attempt}/{DISCOVERY_RETRIES} timed out, retrying...",
                  file=sys.stderr)
    return None


def server_handle_discovery(
    sock: socket.socket,
    addr: Tuple[str, int],
    state,
    server_port: int,
) -> None:
    """
        Função que faz o tratamento da fase de descoberta.
        ->  Responde ao broadcast de descoberta com o endereço do servidor e registra o cliente, se ainda
            não foi registrado. 
    """

    server_ip = get_local_ip_for_peer(addr[0])
    response = protocol.make_discovery_response(server_ip, server_port)
    udp.send(sock, response, addr)

    with state.lock:
        if addr not in state.clients:
            state.clients[addr] = ClientState(address=addr)
