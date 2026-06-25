#!/usr/bin/env python3
import socket
import sys
import threading
import time

from network import protocol, udp
from models.server_state import ServerState
from models.rm_state import RMState, Role
from services.discovery import (
    server_handle_discovery,
    rm_discover_peers,
    get_local_ip_for_peer,
    probe_for_leader,
    sync_state_from_leader,
)
from services.processing import server_handle_request, timestamp
from services.replication import apply_replicate, state_to_dict
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
    # Accept both:
    #   servidor_rm.py <rm_id> <port> [peers...]        — IP auto-detected
    #   servidor_rm.py <rm_id> <ip> <port> [peers...]   — IP explicit
    args = sys.argv[1:]
    if len(args) < 2:
        print(
            f"Usage: python3 {sys.argv[0]} <rm_id> [ip] <port> "
            "[peer_id:peer_ip:peer_port ...]",
            file=sys.stderr,
        )
        sys.exit(1)

    rm_id = int(args[0])
    # If the second arg contains a dot it's an IP; otherwise it's the port
    if '.' in args[1]:
        my_ip = args[1]
        port  = int(args[2])
        peers = _parse_peers(args[3:])
    else:
        port  = int(args[1])
        my_ip = get_local_ip_for_peer('8.8.8.8')
        peers = _parse_peers(args[2:])

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

    # hb_stop holds the stop-event for whichever heartbeat thread is active.
    # Using a list so the role-change callbacks can swap it safely.
    _hb_lock = threading.Lock()
    hb_stop  = [None]

    def _swap_heartbeat(make_stop) -> None:
        with _hb_lock:
            old        = hb_stop[0]
            hb_stop[0] = make_stop()
        if old:
            old.set()  # stop whatever thread (sender/monitor) was running

    def on_become_primary() -> None:
        # Guard: a COORDINATOR may have arrived between winning the election
        # and this callback running — don't override a demotion.
        with rm_state.lock:
            if rm_state.role != Role.PRIMARY:
                return
        _swap_heartbeat(lambda: start_heartbeat_sender(sock, rm_state))
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

    def on_become_backup() -> None:
        _swap_heartbeat(
            lambda: start_heartbeat_monitor(sock, rm_state, on_primary_failure)
        )

    def on_primary_failure() -> None:
        start_election(sock, rm_state, on_become_primary)

    # ------------------------------------------------------------------
    # Rejoin / leader discovery — DO NOT trust max(ID) blindly.
    #
    # A restarting RM must learn who the *current* leader is and pull its state
    # BEFORE taking on any role. Otherwise a returning node would either become
    # an isolated empty primary ("works only locally") or sit as a backup
    # pointed at a dead leader. max(ID) is only the bully tie-breaker here, not
    # the source of truth about who leads right now.
    # ------------------------------------------------------------------

    # Announce our (re)entry so a live leader integrates us into its peer set.
    announce = protocol.make_rm_announce(rm_id, my_ip, port)
    for addr in list(rm_state.peers.values()):
        try:
            udp.send(sock, announce, addr)
        except OSError:
            pass

    leader = probe_for_leader(sock, rm_id, my_ip, port, rm_state.peers)

    if leader is not None:
        leader_id, leader_addr = leader
        with rm_state.lock:
            rm_state.peers[leader_id] = leader_addr
            rm_state.leader_id        = leader_id
            rm_state.leader_addr      = leader_addr
            rm_state.role             = Role.BACKUP
            rm_state.last_heartbeat   = time.monotonic()
        # Pull the current state from the leader before doing anything else.
        sync_state_from_leader(sock, leader_addr, rm_id, my_ip, port, state)

        if rm_id > leader_id:
            # Bully: we outrank the incumbent. We already hold the latest state,
            # so reclaiming leadership is now safe (no data loss). Run a clean
            # election so any even-higher peer still gets a chance to win.
            print(f"{timestamp()} RM {rm_id} outranks current leader RM "
                  f"{leader_id} — reclaiming leadership", file=sys.stderr)
            start_election(sock, rm_state, on_become_primary)
        else:
            on_become_backup()
    else:
        # Nobody is leading (cold start or full outage). Settle it via bully.
        if rm_state.peers:
            start_election(sock, rm_state, on_become_primary)
        elif rm_state.role == Role.PRIMARY:
            on_become_primary()
        else:
            on_become_backup()

    with state.lock:
        print(f"{timestamp()} num_reqs {state.num_reqs} "
              f"total_sum {state.total_sum}")
    sys.stdout.flush()

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
                sender_id = msg.get('rm_id')
                sender_addr = (
                    (msg['ip'], int(msg['port']))
                    if msg.get('ip') is not None else addr
                )
                demote = False
                with rm_state.lock:
                    rm_state.last_heartbeat = time.monotonic()
                    if sender_id is not None and sender_id != rm_id:
                        if rm_state.role == Role.PRIMARY:
                            # Split-brain: another node also claims leadership.
                            # The higher ID wins (bully); we step down and adopt
                            # it. A lower-ID claimant is ignored — our own
                            # heartbeats will make it step down instead.
                            if sender_id > rm_id:
                                rm_state.role        = Role.BACKUP
                                rm_state.leader_id   = sender_id
                                rm_state.leader_addr = sender_addr
                                demote = True
                        else:
                            # Backup: continuously (re)learn the current leader
                            # from the heartbeat — this is what corrects a node
                            # that came back pointed at a stale/dead leader.
                            if sender_id != rm_state.leader_id:
                                rm_state.leader_id   = sender_id
                                rm_state.leader_addr = sender_addr
                                print(f"{timestamp()} RM {rm_id}: leader is now "
                                      f"RM {sender_id}", file=sys.stderr)
                udp.send(sock, protocol.make_heartbeat_ack(rm_id), addr)
                if demote:
                    print(f"{timestamp()} RM {rm_id}: stepping down for higher "
                          f"RM {sender_id} (split-brain resolved)", file=sys.stderr)
                    on_become_backup()
                    # Re-sync state from the legitimate leader.
                    udp.send(sock, protocol.make_state_request(rm_id, my_ip, port),
                             sender_addr)

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
                handle_election_msg(sock, msg, addr, rm_state, on_become_primary, on_become_backup)

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

            elif msg_type == protocol.MSG_WHO_IS_LEADER:
                # A (re)joining RM is asking who leads. Only the primary answers,
                # with a COORDINATOR, so the asker learns the real leader instead
                # of guessing from max(ID). Also (re)integrate it as a peer.
                peer_id   = msg.get('rm_id')
                peer_ip   = msg.get('ip')
                peer_port = int(msg.get('port', 0))
                if peer_id and peer_id != rm_id:
                    with rm_state.lock:
                        rm_state.peers[peer_id] = (peer_ip, peer_port)
                with rm_state.lock:
                    am_primary = rm_state.role == Role.PRIMARY
                if am_primary:
                    udp.send(sock, protocol.make_coordinator(rm_id, my_ip, port), addr)

            elif msg_type == protocol.MSG_STATE_REQUEST:
                # Send the full current state so the requester can sync. Any node
                # can serve this; the requester targets the leader for freshness.
                with state.lock:
                    payload = state_to_dict(state)
                udp.send(sock, protocol.make_state_transfer(payload), addr)

            elif msg_type == protocol.MSG_STATE_TRANSFER:
                # Snapshot pushed in reply to our STATE_REQUEST (e.g. after a
                # split-brain step-down). Same shape as REPLICATE.
                apply_replicate(msg, state)
                with state.lock:
                    n, s = state.num_reqs, state.total_sum
                print(f"{timestamp()} RM {rm_id}: state synced — "
                      f"num_reqs {n} total_sum {s}", file=sys.stderr)

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
