import socket
import sys
import threading
from datetime import datetime
from typing import Callable, TYPE_CHECKING

from network import protocol, udp
from models.rm_state import RMState, Role

if TYPE_CHECKING:
    pass

# How long to wait for an OK before declaring victory
ELECTION_TIMEOUT = 2.0
# How long to wait for COORDINATOR after receiving OK before retrying election
COORDINATOR_TIMEOUT = 5.0


def _ts() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def start_election(
    sock: socket.socket,
    rm: RMState,
    on_become_primary: Callable,
) -> None:
    with rm.lock:
        if rm.election_in_progress:
            return  # another election is already running
        rm.election_in_progress = True
        rm.received_ok = False
        rm.role = Role.CANDIDATE

    print(f"{_ts()} RM {rm.rm_id} starting election", file=sys.stderr)
    sys.stderr.flush()

    higher_peers = [addr for pid, addr in rm.peers.items() if pid > rm.rm_id]

    msg = protocol.make_election(rm.rm_id)
    for addr in higher_peers:
        udp.send(sock, msg, addr)

    if not higher_peers:
        # No RM with a higher ID exists — win immediately
        declare_victory(sock, rm, on_become_primary)
        return

    # Wait ELECTION_TIMEOUT for an OK; if none arrives, declare victory
    t = threading.Timer(
        ELECTION_TIMEOUT,
        _on_election_timeout,
        args=(sock, rm, on_become_primary),
    )
    t.daemon = True
    with rm.lock:
        rm.victory_timer = t
    t.start()


def _on_election_timeout(
    sock: socket.socket,
    rm: RMState,
    on_become_primary: Callable,
) -> None:
    with rm.lock:
        if rm.received_ok:
            return  # an OK arrived; waiting for COORDINATOR
    declare_victory(sock, rm, on_become_primary)


def declare_victory(
    sock: socket.socket,
    rm: RMState,
    on_become_primary: Callable,
) -> None:
    print(f"{_ts()} RM {rm.rm_id} won election — broadcasting COORDINATOR", file=sys.stderr)
    sys.stderr.flush()

    msg = protocol.make_coordinator(rm.rm_id, rm.my_ip, rm.my_port)
    for addr in rm.peers.values():
        try:
            udp.send(sock, msg, addr)
        except OSError:
            pass

    with rm.lock:
        rm.leader_id            = rm.rm_id
        rm.leader_addr          = (rm.my_ip, rm.my_port)
        rm.role                 = Role.PRIMARY
        rm.election_in_progress = False

    on_become_primary()


def handle_election_msg(
    sock: socket.socket,
    msg: dict,
    addr: tuple,
    rm: RMState,
    on_become_primary: Callable,
) -> None:
    msg_type  = msg["type"]
    sender_id = msg.get("rm_id")

    if msg_type == protocol.MSG_ELECTION:
        if rm.rm_id > sender_id:
            # Respond OK and start our own election if not already running
            udp.send(sock, protocol.make_ok(rm.rm_id), addr)
            start_election(sock, rm, on_become_primary)

    elif msg_type == protocol.MSG_OK:
        with rm.lock:
            rm.received_ok = True
            if rm.victory_timer:
                rm.victory_timer.cancel()
                rm.victory_timer = None
        # Fallback: if COORDINATOR never arrives, restart election
        ct = threading.Timer(
            COORDINATOR_TIMEOUT,
            start_election,
            args=(sock, rm, on_become_primary),
        )
        ct.daemon = True
        with rm.lock:
            rm.coordinator_timer = ct
        ct.start()

    elif msg_type == protocol.MSG_COORDINATOR:
        import time
        print(
            f"{_ts()} RM {rm.rm_id} received COORDINATOR from RM {sender_id}",
            file=sys.stderr,
        )
        sys.stderr.flush()
        with rm.lock:
            rm.leader_id            = sender_id
            rm.leader_addr          = (msg["ip"], msg["port"])
            rm.role                 = Role.BACKUP
            rm.election_in_progress = False
            rm.received_ok          = False
            # Reset the failure clock so the monitor doesn't immediately re-trigger
            # before the new primary has a chance to send its first heartbeat.
            rm.last_heartbeat       = time.monotonic()
            if rm.victory_timer:
                rm.victory_timer.cancel()
                rm.victory_timer = None
            if rm.coordinator_timer:
                rm.coordinator_timer.cancel()
                rm.coordinator_timer = None
