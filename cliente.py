import queue
import socket
import sys
import threading
import time
from datetime import datetime
from typing import Tuple

from network import protocol, udp
from services.discovery import client_discover, get_local_ip_for_peer

# Retransmit if the matching ACK is not received within this window. 200ms
# comfortably covers a LAN round-trip plus server processing, so we don't
# resend before the ACK has a chance to arrive. Small enough that failover
# detection (FAILOVER_THRESHOLD timeouts) still kicks in within ~1s.
REQUEST_TIMEOUT = 0.2

# After this many consecutive timeouts, try re-discovering a new primary
FAILOVER_THRESHOLD = 5
# Minimum interval between re-discovery attempts (avoids broadcast storms)
REDISCOVERY_INTERVAL = 1.0

def timestamp() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


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
        # sentinel: signals sender thread that no more values are coming
        input_queue.put(None)


def sender_thread(
    sock: socket.socket,
    server_addr_ref: list,
    client_id: str,
    input_queue: queue.Queue,
    stop_event: threading.Event,
    service_port: int,
) -> None:
    # exactly-once: each value gets a unique sequential id
    id_req = 0
    last_rediscovery: float = 0.0

    while not stop_event.is_set():
        try:
            value = input_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        if value is None:
            break

        id_req += 1
        msg = protocol.make_request(client_id, id_req, value)
        consecutive_timeouts = 0

        while not stop_event.is_set():
            server_addr = server_addr_ref[0]
            try:
                udp.send(sock, msg, server_addr)
            except OSError as exc:
                print(f"Send error: {exc}", file=sys.stderr)
                break

            # Wait for THIS request's ACK. Drain stale/unexpected packets
            # within the window WITHOUT resending — resending on every stray
            # packet (old ACKs, repeated NEW_LEADERs) creates a duplicate
            # feedback storm. Only a real timeout or a leader change resends.
            got_ack    = False
            new_leader = False
            deadline   = time.monotonic() + REQUEST_TIMEOUT
            while not stop_event.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break  # window elapsed → resend
                try:
                    data, _addr = udp.receive(sock, timeout=remaining)
                    response = protocol.decode(data)
                except socket.timeout:
                    break  # → resend
                except Exception as exc:
                    print(f"Receive error: {exc}", file=sys.stderr)
                    continue

                rtype = response.get('type')

                if rtype == protocol.MSG_NEW_LEADER:
                    # New primary notified us — redirect and resend once
                    server_addr_ref[0] = (response['ip'], response['port'])
                    print(f"{timestamp()} server_addr {response['ip']}")
                    sys.stdout.flush()
                    new_leader = True
                    break

                if rtype == protocol.MSG_ACK and response.get('id_req') == id_req:
                    num_reqs  = response['num_reqs']
                    total_sum = response['total_sum']
                    print(
                        f"{timestamp()} server {server_addr[0]} id_req {id_req} value {value} "
                        f"num_reqs {num_reqs} total_sum {total_sum}"
                    )
                    sys.stdout.flush()
                    got_ack = True
                    break

                # stale ACK or unrelated packet — drain it, keep waiting, NO resend
                continue

            if got_ack:
                consecutive_timeouts = 0
                break  # next value

            if new_leader:
                consecutive_timeouts = 0
                continue  # resend current value to the new primary

            # genuine timeout — no matching ACK arrived in the window
            consecutive_timeouts += 1
            now = time.monotonic()
            if (consecutive_timeouts >= FAILOVER_THRESHOLD
                    and now - last_rediscovery >= REDISCOVERY_INTERVAL):
                print("Primary unresponsive, rediscovering...", file=sys.stderr)
                new_addr = client_discover(sock, service_port)
                last_rediscovery = time.monotonic()
                if new_addr:
                    server_addr_ref[0] = new_addr
                    consecutive_timeouts = 0
                    print(f"{timestamp()} server_addr {new_addr[0]}", file=sys.stderr)
                    sys.stderr.flush()
            # loop → resend current value


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} <port>", file=sys.stderr)
        sys.exit(1)

    port = int(sys.argv[1])
    sock = udp.create_client_socket()

    server_addr = client_discover(sock, port)
    if server_addr is None:
        print("ERROR: Server not found after all retries.", file=sys.stderr)
        sock.close()
        sys.exit(1)

    print(f"{timestamp()} server_addr {server_addr[0]}")
    sys.stdout.flush()

    # resolve actual local IP since socket is bound to 0.0.0.0
    local_ip, local_port = sock.getsockname()
    if local_ip == '0.0.0.0':
        local_ip = get_local_ip_for_peer(server_addr[0])
    client_id = f"{local_ip}:{local_port}"

    # Use a mutable list so sender_thread can update the address on failover
    server_addr_ref = [server_addr]

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
        args=(sock, server_addr_ref, client_id, input_queue, stop_event, port),
        daemon=True,
        name="sender",
    )

    t_reader.start()
    t_sender.start()

    t_sender.join()
    stop_event.set()
    t_reader.join(timeout=1.0)
    sock.close()


if __name__ == '__main__':
    main()
