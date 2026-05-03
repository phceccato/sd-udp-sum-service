import socket
from typing import Optional, Tuple

BUFFER_SIZE = 65535

""""
    * Arquivo responsável por centralizar todas as funções/operações relacionadas ao protocolo UDP
"""

def create_server_socket(port: int) -> socket.socket:
    """
        Cria socket UDP do servidor com broadcast habilitado, vinculado a uma porta passada como parametro da função.

        Args:
            port (int): Porta que será utilizada para o recebimento de mensagens.

        Retornos:
            socket.socket: Socket do servidor.
    """
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # SO_REUSEADDR: permite reuso da porta para casos de reinicio do servidor
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind(('', port))
    
    return sock


def create_client_socket() -> socket.socket:
    """
        Cria um socket UDP do cliente com broadcast habilitado.

        Returns:
            socket.socket: Socket do cliente.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    # porta 0: SO define aleatoriamente uma porta para o socket
    sock.bind(('', 0))
    return sock


def send(sock: socket.socket, data: bytes, addr: Tuple[str, int]) -> None:
    """
        Envia dados, recebidos em bytes, para o endereço passado como parâmetro.
    """
    sock.sendto(data, addr)


def receive(sock: socket.socket, timeout: Optional[float] = None) -> Tuple[bytes, Tuple[str, int]]:
    """

    """
    old_timeout = sock.gettimeout()
    if timeout is not None:
        sock.settimeout(timeout)
    try:
        data, addr = sock.recvfrom(BUFFER_SIZE)
        return data, addr
    finally:
        sock.settimeout(old_timeout)
