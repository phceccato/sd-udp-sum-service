import socket
from typing import Optional, Tuple

BUFFER_SIZE = 65535


def create_server_socket(port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # SO_REUSEADDR: allows restarting on the same port without waiting for OS to release it
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind(('', port))
    return sock


def create_client_socket() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    # port 0: OS assigns an ephemeral port
    sock.bind(('', 0))
    return sock


def send(sock: socket.socket, data: bytes, addr: Tuple[str, int]) -> None:
    sock.sendto(data, addr)


def receive(sock: socket.socket, timeout: Optional[float] = None) -> Tuple[bytes, Tuple[str, int]]:
    # save and restore timeout to avoid side effects on the shared socket
    old_timeout = sock.gettimeout()
    if timeout is not None:
        sock.settimeout(timeout)
    try:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        return data, addr
    finally:
        sock.settimeout(old_timeout)
