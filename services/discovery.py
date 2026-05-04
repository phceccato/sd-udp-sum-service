import socket
import sys
from typing import Optional, Tuple
from models.client_state import ClientState

from network import protocol, udp

DISCOVERY_TIMEOUT = 3.0     # timeout de descoberta
DISCOVERY_RETRIES = 10      # quantidade de tentativas de descoberta


def get_local_ip_for_peer(peer_ip: str) -> str:
    # connect em UDP não envia pacote — apenas consulta a tabela de roteamento do SO
    # o getsockname retorna qual IP local seria usado para alcançar peer_ip
    # resolve o problema de o socket estar ligado em 0.0.0.0
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect((peer_ip, 80))
            return s.getsockname()[0]
        except OSError:
            return '127.0.0.1'


def get_broadcast_addr() -> str:
    # tenta detectar o broadcast automaticamente conforme o sistema operacional
    if sys.platform == 'win32':
        return _get_broadcast_addr_windows()
    return _get_broadcast_addr_linux()


def _get_broadcast_addr_linux() -> str:
    # no Linux usa ioctl para consultar o broadcast real da interface ativa
    try:
        import array
        import fcntl
        import struct

        SIOCGIFCONF    = 0x8912
        SIOCGIFBRDADDR = 0x8919
        buf = array.array('B', b'\0' * 1024)
        ifconf = struct.pack('iL', buf.buffer_info()[1], buf.buffer_info()[0])
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            result = fcntl.ioctl(s.fileno(), SIOCGIFCONF, ifconf)
        outbytes = struct.unpack('iL', result)[0]
        ifaces_raw = bytes(buf[:outbytes])

        offset = 0
        while offset < outbytes:
            ifname = ifaces_raw[offset:offset + 16].rstrip(b'\0').decode()
            family = struct.unpack_from('H', ifaces_raw, offset + 16)[0]
            if family == socket.AF_INET:
                ip = socket.inet_ntoa(ifaces_raw[offset + 20:offset + 24])
                # ignora loopback e interface docker
                if not ip.startswith('127.') and not ip.startswith('172.'):
                    ifreq = struct.pack('16sH2s4s8s', ifname.encode(),
                                       socket.AF_INET, b'\x00' * 2,
                                       b'\x00' * 4, b'\x00' * 8)
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                        brd_result = fcntl.ioctl(s.fileno(), SIOCGIFBRDADDR, ifreq)
                    brd = socket.inet_ntoa(brd_result[20:24])
                    if brd != '0.0.0.0':
                        return brd
            offset += 40
    except Exception:
        pass

    return '255.255.255.255'


def _get_broadcast_addr_windows() -> str:
    # no Windows deriva o broadcast a partir do IP local assumindo sub-rede /24
    # (padrao na grande maioria das redes de laboratorio)
    try:
        local_ip = get_local_ip_for_peer('8.8.8.8')
        if not local_ip.startswith('127.'):
            # pega os 3 primeiros octetos e substitui o ultimo por 255
            # ex: 192.168.0.11 -> 192.168.0.255
            prefix = local_ip.rsplit('.', 1)[0]
            return f"{prefix}.255"
    except Exception:
        pass

    return '255.255.255.255'


def client_discover(sock: socket.socket, port: int) -> Optional[Tuple[str, int]]:
    """
    Envia broadcast de descoberta na porta informada e aguarda resposta do servidor.
    Retorna (ip_servidor, porta) ou None se nao encontrar servidor.
    """
    # detecta o broadcast correto da rede automaticamente
    broadcast_addr = get_broadcast_addr()
    print(f"Endereco de broadcast detectado: {broadcast_addr}", file=sys.stderr)

    msg = protocol.make_discovery()

    for attempt in range(1, DISCOVERY_RETRIES + 1):
        # envia para toda a rede local: "tem servidor na porta X?"
        udp.send(sock, msg, (broadcast_addr, port))
        try:
            data, _addr = udp.receive(sock, timeout=DISCOVERY_TIMEOUT)
            response = protocol.decode(data)
            if response.get('type') == protocol.MSG_DISCOVERY_RESPONSE:
                # servidor respondeu com seu IP e porta reais
                return response['server_ip'], response['port']
        except socket.timeout:
            print(f"Tentativa {attempt}/{DISCOVERY_RETRIES} sem resposta, tentando novamente...",
                  file=sys.stderr)

    # esgotou todas as tentativas sem encontrar servidor
    return None


def server_handle_discovery(
    sock: socket.socket,
    addr: Tuple[str, int],
    state,
    server_port: int,
) -> None:
    """
        Funcao que faz o tratamento da fase de descoberta.
        ->  Responde ao broadcast de descoberta com o endereco do servidor e registra o cliente, se ainda
            nao foi registrado.
    """
    # descobre o IP real da interface que alcanca o cliente
    server_ip = get_local_ip_for_peer(addr[0])
    response = protocol.make_discovery_response(server_ip, server_port)

    # responde diretamente ao cliente (unicast), nao em broadcast
    udp.send(sock, response, addr)

    # registra o cliente na tabela se for a primeira vez que aparece
    with state.lock:
        if addr not in state.clients:
            state.clients[addr] = ClientState(address=addr)
