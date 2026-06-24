#!/usr/bin/env python3
import socket
import sys
import threading
import time

from network import protocol, udp
from models.server_state import ServerState
from models.rm_state import RMState, Role
from services.discovery import server_handle_discovery, rm_discover_peers
from services.processing import server_handle_request, timestamp
from services.replication import apply_replicate
from services.election import start_election, handle_election_msg
from services.heartbeat import start_heartbeat_sender, start_heartbeat_monitor


def _parse_peers(args: list) -> dict:
    peers = {}
    for arg in args:
        parts = arg.split(':')
        if len(parts) != 3:
            print(f"Invalid peer '{arg}' — expected id:ip:port", file=sys.stderr)
            sys.exit(1)
        peers[int(parts[0])] = (parts[1], int(parts[2]))
    return peers


def main() -> None:
    if len(sys.argv) < 4:
        print(
            f"Usage: python3 {sys.argv[0]} <rm_id> <ip> <port> "
            "[peer_id:peer_ip:peer_port ...]",
            file=sys.stderr,
        )
        sys.exit(1)

    rm_id  = int(sys.argv[1])
    my_ip  = sys.argv[2]
    port   = int(sys.argv[3])
    peers  = _parse_peers(sys.argv[4:])

    state = ServerState()
    sock  = udp.create_server_socket(port)

    if not peers:
        peers = rm_discover_peers(sock, rm_id, my_ip, port)

    rm_state = RMState(rm_id=rm_id, my_ip=my_ip, my_port=port, peers=peers)

    print(
        f"{timestamp()} RM {rm_id} starting as {rm_state.role.value} "
        f"on {my_ip}:{port}",
        file=sys.stderr,
    )
    print(f"{timestamp()} num_reqs 0 total_sum 0")
    sys.stdout.flush()

    # hb_stop holds the stop-event for whichever heartbeat thread is active.
    # Using a list so the closure in on_become_primary can swap it safely.
    _hb_lock = threading.Lock()
    hb_stop  = [None]

    def on_become_primary() -> None:
        with _hb_lock:
            old        = hb_stop[0]
            hb_stop[0] = start_heartbeat_sender(sock, rm_state)
        if old:
            old.set()  # stop the monitor (or previous sender)
        print(f"{timestamp()} RM {rm_id} is now PRIMARY", file=sys.stderr)
        sys.stderr.flush()

        # Notify all known clients so they redirect immediately without waiting
        # for a timeout — required by the spec ("devem ser notificados")
        msg = protocol.make_new_leader(my_ip, port)
        with state.lock:
            client_addrs = [cs.address for cs in state.clients.values()]
        for addr in client_addrs:
            try:
                udp.send(sock, msg, addr)
            except OSError:
                pass

    def on_primary_failure() -> None:
        start_election(sock, rm_state, on_become_primary)

    # Start the appropriate heartbeat thread based on initial role
    with _hb_lock:
        if rm_state.role == Role.PRIMARY:
            hb_stop[0] = start_heartbeat_sender(sock, rm_state)
        else:
            hb_stop[0] = start_heartbeat_monitor(sock, rm_state, on_primary_failure)

    try:
        while True:
            try:
                data, addr = sock.recvfrom(udp.BUFFER_SIZE)
            except socket.error as exc:
                print(f"Socket error: {exc}", file=sys.stderr)
                continue

            try:
                msg = protocol.decode(data)
            except Exception as exc:
                print(f"Decode error from {addr}: {exc}", file=sys.stderr)
                continue

            msg_type = msg.get('type')

            if msg_type == protocol.MSG_DISCOVERY:
                server_handle_discovery(sock, addr, state, port, rm_state)

            elif msg_type == protocol.MSG_REQUEST:
                with rm_state.lock:
                    is_primary = rm_state.role == Role.PRIMARY
                if is_primary:
                    # One thread per request — as required by the spec
                    threading.Thread(
                        target=server_handle_request,
                        args=(sock, addr, msg, state, rm_state),
                        daemon=True,
                    ).start()
                # Backups silently drop requests — client will re-discover

            elif msg_type == protocol.MSG_HEARTBEAT:
                with rm_state.lock:
                    rm_state.last_heartbeat = time.monotonic()
                udp.send(sock, protocol.make_heartbeat_ack(rm_id), addr)

            elif msg_type == protocol.MSG_HEARTBEAT_ACK:
                pass  # could track backup liveness; not required for this spec

            elif msg_type == protocol.MSG_REPLICATE:
                with rm_state.lock:
                    is_backup = rm_state.role != Role.PRIMARY
                if is_backup:
                    apply_replicate(msg, state)

            elif msg_type in (
                protocol.MSG_ELECTION,
                protocol.MSG_OK,
                protocol.MSG_COORDINATOR,
            ):
                handle_election_msg(sock, msg, addr, rm_state, on_become_primary)

            elif msg_type == protocol.MSG_RM_ANNOUNCE:
                # A new RM joined — add it as a peer and introduce ourselves
                peer_id   = msg.get('rm_id')
                peer_ip   = msg.get('ip')
                peer_port = int(msg.get('port', 0))
                if peer_id and peer_id != rm_id:
                    with rm_state.lock:
                        rm_state.peers[peer_id] = (peer_ip, peer_port)
                    ack = protocol.make_rm_announce_ack(rm_id, my_ip, port)
                    udp.send(sock, ack, (peer_ip, peer_port))
                    print(f"{timestamp()} RM {rm_id}: discovered new peer RM {peer_id}",
                          file=sys.stderr)

            elif msg_type in (
                protocol.MSG_RM_ANNOUNCE_ACK,
                protocol.MSG_REPLICATE_ACK,
            ):
                pass  # handled during startup discovery or not required

            else:
                print(
                    f"Unknown message type '{msg_type}' from {addr}",
                    file=sys.stderr,
                )

    except KeyboardInterrupt:
        print("\nShutting down RM.", file=sys.stderr)
    finally:
        with _hb_lock:
            s = hb_stop[0]
        if s:
            s.set()
        sock.close()


if __name__ == '__main__':
    main()
