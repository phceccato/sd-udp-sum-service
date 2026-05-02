#!/usr/bin/env python3
"""
servidor.py — UDP sum server with exactly-once delivery semantics.

Usage:
    python3 servidor.py <porta>
"""
import socket
import sys

from network import protocol, udp
from models.server_state import ServerState
from services.discovery import server_handle_discovery
from services.processing import server_handle_request, timestamp


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} <porta>", file=sys.stderr)
        sys.exit(1)

    port = int(sys.argv[1])
    state = ServerState()
    sock = udp.create_server_socket(port)

    print(f"{timestamp()} num_reqs 0 total_sum 0")
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
                server_handle_discovery(sock, addr, state, port)

            elif msg_type == protocol.MSG_REQUEST:
                server_handle_request(sock, addr, msg, state)

            else:
                print(f"Unknown message type '{msg_type}' from {addr}", file=sys.stderr)

    except KeyboardInterrupt:
        print("\nServer shutting down.", file=sys.stderr)
    finally:
        sock.close()


if __name__ == '__main__':
    main()
