import json
from typing import Any, Dict

MSG_DISCOVERY = "DISCOVERY"                    # cliente → broadcast
MSG_DISCOVERY_RESPONSE = "DISCOVERY_RESPONSE"  # servidor → cliente: "IP é X"
MSG_REQUEST = "REQUEST"                        # cliente → servidor: envia número para somar
MSG_ACK = "ACK"                                # servidor → cliente: confirma processamento


def encode(msg: Dict[str, Any]) -> bytes:
    # converte dicionário Python em bytes JSON para enviar pelo socket
    return json.dumps(msg).encode('utf-8')


def decode(data: bytes) -> Dict[str, Any]:
    # converte bytes recebidos do socket de volta em dicionário Python
    return json.loads(data.decode('utf-8'))


def make_discovery() -> bytes:
    # mensagem enviada pelo cliente em broadcast para encontrar o servidor
    return encode({"type": MSG_DISCOVERY})


def make_discovery_response(server_ip: str, port: int) -> bytes:
    # resposta do servidor com seu IP real e porta
    return encode({"type": MSG_DISCOVERY_RESPONSE, "server_ip": server_ip, "port": port})


def make_request(client_id: str, id_req: int, value: int) -> bytes:
    # requisição com o número a somar — id_req é o número de sequência (base do exactly-once)
    return encode({"type": MSG_REQUEST, "client_id": client_id, "id_req": id_req, "value": value})


def make_ack(id_req: int, num_reqs: int, total_sum: int) -> bytes:
    # confirmação do servidor: devolve o id_req confirmado + estado global atualizado
    return encode({"type": MSG_ACK, "id_req": id_req, "num_reqs": num_reqs, "total_sum": total_sum})
