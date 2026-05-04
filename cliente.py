import queue
import socket
import sys
import threading
from datetime import datetime
from typing import Tuple

from network import protocol, udp
from services.discovery import client_discover, get_local_ip_for_peer

# timeout de 20ms: retransmite caso o ACK não chegue
REQUEST_TIMEOUT = 0.02

def timestamp() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def reader_thread(input_queue: queue.Queue, stop_event: threading.Event) -> None:
    # thread 1: lê números do teclado (ou arquivo) e coloca na fila
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
        # sinal de fim: avisa a thread que não virão mais números
        input_queue.put(None)


def sender_thread(
    sock: socket.socket,
    server_addr: Tuple[str, int],
    client_id: str,
    input_queue: queue.Queue,
    stop_event: threading.Event,
) -> None:
    # thread 2: pega os números da fila e envia ao servidor
    id_req = 0

    while not stop_event.is_set():
        try:
            value = input_queue.get(timeout=0.5)
        except queue.Empty:
            continue

        if value is None:
            break

        # semântica exactly-once - cada número tem um id sequencial único
        id_req += 1
        msg = protocol.make_request(client_id, id_req, value)

        # loop de retransmissão: só avança quando receber o ACK correto
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
                # não chegou ACK - retransmite o mesmo id_req
                continue
            except Exception as exc:
                print(f"Receive error: {exc}", file=sys.stderr)
                continue

            if response.get('type') != protocol.MSG_ACK:
                continue

            ack_id = response.get('id_req')

            if ack_id == id_req:
                # ACK correto recebido = imprime resultado e vai para o próximo número
                num_reqs = response['num_reqs']
                total_sum = response['total_sum']
                print(
                    f"{timestamp()} server {server_addr[0]} id_req {id_req} value {value} "
                    f"num_reqs {num_reqs} total_sum {total_sum}"
                )
                sys.stdout.flush()
                break

            # ACK de outro id_req = ignora e continua esperando


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} <porta>", file=sys.stderr)
        sys.exit(1)

    port = int(sys.argv[1])
    sock = udp.create_client_socket()

    # FASE 1: encontra o servidor via broadcast
    server_addr = client_discover(sock, port)
    if server_addr is None:
        print("ERROR: Server not found after all retries.", file=sys.stderr)
        sock.close()
        sys.exit(1)

    print(f"{timestamp()} server_addr {server_addr[0]}")
    sys.stdout.flush()

    # descobre o IP local real (socket está em 0.0.0.0)
    local_ip, local_port = sock.getsockname()
    if local_ip == '0.0.0.0':
        local_ip = get_local_ip_for_peer(server_addr[0])
    client_id = f"{local_ip}:{local_port}"

    # FASE 2: PROCESSAMENTO — duas threads se comunicam pela fila
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

    # aguarda a thread enviadora terminar (ela sai ao receber None da fila)
    t_sender.join()
    stop_event.set()
    t_reader.join(timeout=1.0)
    sock.close()


if __name__ == '__main__':
    main()
