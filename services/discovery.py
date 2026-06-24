import socket
import sys
import time
from typing import Optional, Tuple
from models.client_state import ClientState

from network import protocol, udp

DISCOVERY_TIMEOUT = 3.0
DISCOVERY_RETRIES = 10


def get_local_ip_for_peer(peer_ip: str) -> str:
    # UDP connect does not send a packet — it only queries the OS routing table.
    # getsockname returns which local IP would be used to reach peer_ip,
    # solving the issue of the socket being bound to 0.0.0.0.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect((peer_ip, 80))
            return s.getsockname()[0]
        except OSError:
            return '127.0.0.1'


def get_broadcast_addr() -> str:
    if sys.platform == 'win32':
        return _get_broadcast_addr_windows()
    return _get_broadcast_addr_linux()


def _get_broadcast_addr_linux() -> str:
    # use ioctl to query the real broadcast address of the active interface
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
                # skip loopback and docker interfaces
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
    # derive broadcast from local IP assuming /24 subnet (common in lab networks)
    # e.g. 192.168.0.11 -> 192.168.0.255
    try:
        local_ip = get_local_ip_for_peer('8.8.8.8')
        if not local_ip.startswith('127.'):
            prefix = local_ip.rsplit('.', 1)[0]
            return f"{prefix}.255"
    except Exception:
        pass

    return '255.255.255.255'


def client_discover(sock: socket.socket, port: int) -> Optional[Tuple[str, int]]:
    broadcast_addr = get_broadcast_addr()
    print(f"Broadcast address: {broadcast_addr}", file=sys.stderr)

    msg = protocol.make_discovery()

    for attempt in range(1, DISCOVERY_RETRIES + 1):
        udp.send(sock, msg, (broadcast_addr, port))
        deadline = time.monotonic() + DISCOVERY_TIMEOUT
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                data, _addr = udp.receive(sock, timeout=remaining)
                response = protocol.decode(data)
                if response.get('type') == protocol.MSG_DISCOVERY_RESPONSE:
                    return response['server_ip'], response['port']
                if response.get('type') == protocol.MSG_NEW_LEADER:
                    # New primary pushed its address while we were re-discovering
                    return response['ip'], response['port']
                # Any other message type (stale ACK, etc.) — keep draining
            except socket.timeout:
                break
        print(f"Attempt {attempt}/{DISCOVERY_RETRIES} timed out, retrying...",
              file=sys.stderr)

    return None


def server_handle_discovery(
    sock: socket.socket,
    addr: Tuple[str, int],
    state,
    server_port: int,
    rm_state=None,
) -> None:
    # Only the primary responds; backups stay silent so the client always
    # reaches the current leader via broadcast
    if rm_state is not None:
        from models.rm_state import Role
        with rm_state.lock:
            if rm_state.role != Role.PRIMARY:
                return

    # get the actual interface IP that can reach the client
    server_ip = get_local_ip_for_peer(addr[0])
    response = protocol.make_discovery_response(server_ip, server_port)

    # respond via unicast, not broadcast
    udp.send(sock, response, addr)

    client_key = f"{addr[0]}:{addr[1]}"
    with state.lock:
        if client_key not in state.clients:
            state.clients[client_key] = ClientState(address=addr)


# ---------------------------------------------------------------------------
# RM peer auto-discovery (used when no peers are given on the command line)
# ---------------------------------------------------------------------------

RM_ANNOUNCE_WINDOW = 2.0  # seconds to wait for peer responses after broadcasting


def rm_discover_peers(
    sock: socket.socket,
    rm_id: int,
    my_ip: str,
    my_port: int,
) -> dict:
    """
    Broadcast RM_ANNOUNCE and collect peers that respond within the window.
    Also answers RM_ANNOUNCE from other RMs that are starting up simultaneously.
    Returns {peer_rm_id: (ip, port)}.
    """
    broadcast_addr = get_broadcast_addr()
    announce = protocol.make_rm_announce(rm_id, my_ip, my_port)
    udp.send(sock, announce, (broadcast_addr, my_port))
    print(f"RM {rm_id}: broadcasting presence on {broadcast_addr}:{my_port}, "
          f"waiting {RM_ANNOUNCE_WINDOW}s for peers...", file=sys.stderr)

    peers: dict = {}
    deadline = time.monotonic() + RM_ANNOUNCE_WINDOW
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            data, _ = udp.receive(sock, timeout=remaining)
            msg = protocol.decode(data)
            msg_type = msg.get('type')
            peer_id  = msg.get('rm_id')

            if peer_id is None or peer_id == rm_id:
                continue  # ignore own broadcast echo

            if msg_type == protocol.MSG_RM_ANNOUNCE_ACK:
                peers[peer_id] = (msg['ip'], int(msg['port']))

            elif msg_type == protocol.MSG_RM_ANNOUNCE:
                peers[peer_id] = (msg['ip'], int(msg['port']))
                # Reply so the other RM learns about us
                ack = protocol.make_rm_announce_ack(rm_id, my_ip, my_port)
                udp.send(sock, ack, (msg['ip'], int(msg['port'])))

        except socket.timeout:
            break

    if peers:
        ids = ', '.join(f"RM {pid} @ {ip}:{port}" for pid, (ip, port) in sorted(peers.items()))
        print(f"RM {rm_id}: found peers — {ids}", file=sys.stderr)
    else:
        print(f"RM {rm_id}: no peers found, starting as sole primary", file=sys.stderr)

    return peers
