import queue
import socket
import sys
import threading
from datetime import datetime
from typing import Tuple

from network import protocol, udp
from services.discovery import client_discover, get_local_ip_for_peer

# 10ms timeout: retransmit if ACK is not received in time
REQUEST_TIMEOUT = 0.01

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
    server_addr: Tuple[str, int],
    client_id: str,
    input_queue: queue.Queue,
    stop_event: threading.Event,
) -> None:
    # exactly-once: each value gets a unique sequential id
    id_req = 0

    while not stop_event.is_set():
        try:
            value = input_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        if value is None:
            break

        id_req += 1
        msg = protocol.make_request(client_id, id_req, value)

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
                continue
            except Exception as exc:
                print(f"Receive error: {exc}", file=sys.stderr)
                continue

            if response.get('type') != protocol.MSG_ACK:
                continue

            ack_id = response.get('id_req')

            if ack_id == id_req:
                num_reqs = response['num_reqs']
                total_sum = response['total_sum']
                print(
                    f"{timestamp()} server {server_addr[0]} id_req {id_req} value {value} "
                    f"num_reqs {num_reqs} total_sum {total_sum}"
                )
                sys.stdout.flush()
                break

            # stale ACK — ignore and keep waiting


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

    t_sender.join()
    stop_event.set()
    t_reader.join(timeout=1.0)
    sock.close()


if __name__ == '__main__':
    main()
