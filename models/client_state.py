from typing import Tuple


class ClientState:
    """
        Classe responsável por inicializar e manter o estado dos clientes
    """

    def __init__(self, address: Tuple[str, int]):
        self.address = address
        self.last_req: int = 0        # ultimo id_req processado com sucesso
        self.last_num_reqs: int = 0   # ultima quantidade de requisições processados 
        self.last_total_sum: int = 0  # ultimo valor da soma acumulada enviada para o cliente
