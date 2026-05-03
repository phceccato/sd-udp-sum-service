#!/usr/bin/env python3

import queue
import socket
import sys
import threading
from datetime import datetime
from typing import Tuple

from network import protocol, udp
from services.discovery import client_discover, get_local_ip_for_peer

# Timeout padrão de 10ms para request
REQUEST_TIMEOUT = 0.01

def timestamp() -> str:
    """
        Função auxiliar para retornar data e hora atual já formatada
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# ---------------------------------------------------------------------------
# Thread 1: read integers from stdin and place them on the shared queue.
# Sends None as a sentinel when stdin is exhausted.
# ---------------------------------------------------------------------------

def reader_thread(input_queue: queue.Queue, stop_event: threading.Event) -> None:
    try:
        for line in sys.stdin:
            if stop_event.is_set():
                break
            line = line.strip()
            if not line:
                continue
            try:
                input_queue.put(int(line))
            except ValueError:
                print(f"Ignored non-integer input: {line!r}", file=sys.stderr)
    except EOFError:
        pass
    finally:
        input_queue.put(None)   # sentinel: no more values


# ---------------------------------------------------------------------------
# Thread 2: dequeue values, send REQUEST, wait for matching ACK (retransmit
# on timeout).  Only one request in flight at any time.
# ---------------------------------------------------------------------------

def sender_thread(
    sock: socket.socket,
    server_addr: Tuple[str, int],
    client_id: str,
    input_queue: queue.Queue,
    stop_event: threading.Event,
) -> None:
    id_req = 0

    while not stop_event.is_set():
        # Block until a value (or sentinel) is available
        try:
            value = input_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        if value is None:
            break   # stdin exhausted — clean shutdown

        id_req += 1
        msg = protocol.make_request(client_id, id_req, value)

        # Send and wait for the matching ACK; retransmit on timeout
        while not stop_event.is_set():
            try:
                udp.send(sock, msg, server_addr)
            except OSError as exc:
                print(f"Send error: {exc}", file=sys.stderr)
                break

            try:
                data, _addr = udp.receive(sock, timeout=REQUEST_TIMEOUT)
                response = protocol.decode(data)
            except socket.timeout:
                continue   # timeout → retransmit
            except Exception as exc:
                print(f"Receive error: {exc}", file=sys.stderr)
                continue

            if response.get('type') != protocol.MSG_ACK:
                continue

            ack_id = response.get('id_req')

            if ack_id == id_req:
                # Correct ACK received
                num_reqs = response['num_reqs']
                total_sum = response['total_sum']
                print(
                    f"{timestamp()} server {server_addr[0]} id_req {id_req} value {value} "
                    f"num_reqs {num_reqs} total_sum {total_sum}"
                )
                sys.stdout.flush()
                break   # advance to next value

            # ACK for a different id_req (stale or out-of-order) — keep waiting


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} <porta>", file=sys.stderr)
        sys.exit(1)

    port = int(sys.argv[1])
    sock = udp.create_client_socket()

    # FASE 1: DESCOBERTA 
    server_addr = client_discover(sock, port)
    if server_addr is None:
        print("ERROR: Server not found after all retries.", file=sys.stderr)
        sock.close()
        sys.exit(1)

    print(f"{timestamp()} server_addr {server_addr[0]}")
    sys.stdout.flush()

    # Determine the local IP this socket will actually use
    local_ip, local_port = sock.getsockname()
    if local_ip == '0.0.0.0':
        local_ip = get_local_ip_for_peer(server_addr[0])
    client_id = f"{local_ip}:{local_port}"

    # Phase 2: process numbers from stdin
    input_queue: queue.Queue = queue.Queue()
    stop_event = threading.Event()

    t_reader = threading.Thread(
        target=reader_thread,
        args=(input_queue, stop_event),
        daemon=True,
        name="reader",
    )
    t_sender = threading.Thread(
        target=sender_thread,
        args=(sock, server_addr, client_id, input_queue, stop_event),
        daemon=True,
        name="sender",
    )

    t_reader.start()
    t_sender.start()

    # Wait for the sender to finish (it exits after the None sentinel)
    t_sender.join()
    stop_event.set()
    t_reader.join(timeout=1.0)
    sock.close()


if __name__ == '__main__':
    main()
