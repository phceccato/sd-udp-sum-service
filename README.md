# Trabalho Sistemas Distribuídos

## Descrição

Implementação de um serviço distribuído de soma utilizando UDP, com comunicação confiável entre cliente e servidor.
Garante entrega **exatamente uma vez** (exactly-once), tolerando perdas, duplicatas e reordenação de mensagens.

## Integrantes

- BERNARDO CALLEGARI BOEIRA
- EDUARDO CAMOZZATO FONTE
- PAULO HENRIQUE CECCATO

## Estrutura

```
sd-udp-sum-service/
├── servidor.py              # ponto de entrada do servidor
├── cliente.py               # ponto de entrada do cliente
├── network/
│   ├── protocol.py          # encode/decode JSON e construtores de mensagens
│   └── udp.py               # criação de sockets e I/O UDP
├── services/
│   ├── discovery.py         # fase de descoberta broadcast/unicast
│   └── processing.py        # lógica exactly-once de processamento
└── models/
    ├── client_state.py      # estado por cliente mantido no servidor
    └── server_state.py      # estado global (num_reqs, total_sum uint64)
```

## Requisitos

- Python 3.8+
- Linux ou Windows
- Sem dependências externas (apenas stdlib)

## Como executar

### Servidor

```bash
python3 servidor.py <porta>
```

### Cliente

```bash
python3 cliente.py <porta>
```

O cliente lê inteiros da **entrada padrão** (um por linha) e os envia ao servidor.

### Exemplo

```bash
# Terminal 1
python3 servidor.py 4000

# Terminal 2
echo -e "10\n20\n30" | python3 cliente.py 4000
```

Saída do servidor:

```
2026-05-02 18:31:40 num_reqs 0 total_sum 0
2026-05-02 18:31:40 client 192.168.0.10 id_req 1 value 10 num_reqs 1 total_sum 10
2026-05-02 18:31:40 client 192.168.0.10 id_req 2 value 20 num_reqs 2 total_sum 30
2026-05-02 18:31:40 client 192.168.0.10 id_req 3 value 30 num_reqs 3 total_sum 60
```

Saída do cliente:

```
2026-05-02 18:31:40 server_addr 192.168.0.10
2026-05-02 18:31:40 server 192.168.0.10 id_req 1 value 10 num_reqs 1 total_sum 10
2026-05-02 18:31:40 server 192.168.0.10 id_req 2 value 20 num_reqs 2 total_sum 30
2026-05-02 18:31:40 server 192.168.0.10 id_req 3 value 30 num_reqs 3 total_sum 60
```

### Múltiplos clientes simultâneos

```bash
python3 cliente.py 4000 < numeros_a.txt &
python3 cliente.py 4000 < numeros_b.txt &
```

### Windows (PowerShell)

```powershell
# liberar porta no firewall (executar como administrador)
New-NetFirewallRule -DisplayName "UDP 4000" -Direction Inbound -Protocol UDP -LocalPort 4000 -Action Allow

# rodar servidor
python servidor.py 4000
```

## Protocolo

Mensagens JSON em UTF-8 sobre UDP.

| Tipo                   | Direção            | Campos                                            |
| ---------------------- | -------------------- | ------------------------------------------------- |
| `DISCOVERY`          | cliente → broadcast | `type`                                          |
| `DISCOVERY_RESPONSE` | servidor → cliente  | `type`, `server_ip`, `port`                 |
| `REQUEST`            | cliente → servidor  | `type`, `client_id`, `id_req`, `value`    |
| `ACK`                | servidor → cliente  | `type`, `id_req`, `num_reqs`, `total_sum` |

## Garantias de confiabilidade (Exactly-Once)

### Servidor (por cliente)

| Condição                 | Ação                                                       |
| -------------------------- | ------------------------------------------------------------ |
| `id_req == last_req + 1` | Processa, atualiza estado global, envia ACK novo             |
| `id_req <= last_req`     | Duplicata — reenvia último ACK cacheado sem alterar estado |
| `id_req > last_req + 1`  | Fora de ordem — envia ACK do último req processado         |

### Cliente

- Uma única requisição ativa por vez
- Timeout de **10 ms** → retransmite até receber o ACK correto
- Descoberta via broadcast com timeout de **3 segundos** e até **10 tentativas**
- `id_req` começa em 1 e incrementa a cada novo valor enviado

## Observações

- O endereço de broadcast é detectado automaticamente pela interface de rede ativa
- No Linux usa `ioctl` para consultar o broadcast real da interface
- No Windows deriva o broadcast a partir do IP local assumindo sub-rede `/24`
- Em caso de falha na detecção, usa `255.255.255.255` como fallback
