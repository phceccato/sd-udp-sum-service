import socket
import sys
import threading
import time
from datetime import datetime
from typing import Callable

from network import protocol, udp
from models.rm_state import RMState, Role

HEARTBEAT_INTERVAL = 1.0   # seconds between heartbeat bursts from primary
FAILURE_TIMEOUT    = 3.0   # seconds without a heartbeat before triggering election


def _ts() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _sender_loop(
    sock: socket.socket,
    rm: RMState,
    stop: threading.Event,
) -> None:
    while not stop.is_set():
        msg = protocol.make_heartbeat(rm.rm_id, rm.my_ip, rm.my_port)
        for addr in rm.peers.values():
            try:
                udp.send(sock, msg, addr)
            except OSError:
                pass
        stop.wait(HEARTBEAT_INTERVAL)


def _monitor_loop(
    sock: socket.socket,
    rm: RMState,
    stop: threading.Event,
    on_failure: Callable,
) -> None:
    # Check every third of the timeout so we're not delayed by a full interval
    check_interval = FAILURE_TIMEOUT / 3
    while not stop.is_set():
        stop.wait(check_interval)
        if stop.is_set():
            break
        with rm.lock:
            elapsed = time.monotonic() - rm.last_heartbeat
            role    = rm.role
        if role == Role.BACKUP and elapsed >= FAILURE_TIMEOUT:
            print(
                f"{_ts()} RM {rm.rm_id}: primary silent for {elapsed:.1f}s — starting election",
                file=sys.stderr,
            )
            sys.stderr.flush()
            on_failure()


def start_heartbeat_sender(
    sock: socket.socket,
    rm: RMState,
) -> threading.Event:
    stop = threading.Event()
    threading.Thread(
        target=_sender_loop,
        args=(sock, rm, stop),
        daemon=True,
        name="hb-sender",
    ).start()
    return stop


def start_heartbeat_monitor(
    sock: socket.socket,
    rm: RMState,
    on_failure: Callable,
) -> threading.Event:
    stop = threading.Event()
    threading.Thread(
        target=_monitor_loop,
        args=(sock, rm, stop, on_failure),
        daemon=True,
        name="hb-monitor",
    ).start()
    return stop
