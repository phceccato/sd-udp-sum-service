#!/usr/bin/env python3
import socket
import sys

from network import protocol, udp
from models.server_state import ServerState
from services.discovery import server_handle_discovery
from services.processing import server_handle_request, timestamp


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Erro ao iniciar servidor. Modo de uso: python3 {sys.argv[0]} <porta>", file=sys.stderr)
        sys.exit(1)

    port = int(sys.argv[1])
    # inicializa estado do servidor
    state = ServerState()
    # cria um socket na porta recebida 
    sock = udp.create_server_socket(port)

    print(f"{timestamp()} num_reqs 0 total_sum 0")
    sys.stdout.flush()

    try:
        # loop principal: servidor fica bloqueado esperando pacotes UDP
        while True:
            try:
                # aguarda até chegar um pacote — retorna os bytes e o endereço do remetente
                data, addr = sock.recvfrom(udp.BUFFER_SIZE)
            except socket.error as exc:
                print(f"Erro de socket: {exc}", file=sys.stderr)
                continue

            try:
                # converte os bytes recebidos em dicionário Python
                msg = protocol.decode(data)
            except Exception as exc:
                print(f"Erro ao decodificar mensagem de {addr}: {exc}", file=sys.stderr)
                continue

            msg_type = msg.get('type')

            if msg_type == protocol.MSG_DISCOVERY:
                # fase 1: cliente procurando servidor na rede
                server_handle_discovery(sock, addr, state, port)

            elif msg_type == protocol.MSG_REQUEST:
                # fase 2: cliente enviando número para somar
                server_handle_request(sock, addr, msg, state)

            else:
                print(f"Tipo desconhecido de mensagem recebida. Tipo:'{msg_type}' | Origem: {addr}", file=sys.stderr)

    except KeyboardInterrupt:
        print("\nInterrumpção identificada. Encerrando servidor.", file=sys.stderr)
    finally:
        sock.close()


if __name__ == '__main__':
    main()
