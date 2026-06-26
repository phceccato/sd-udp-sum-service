# Implementação de um Serviço Distribuído de Soma Utilizando UDP e Python

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
├── servidor.py              # servidor da Parte 1 (sem replicação)
├── servidor_rm.py           # réplica (Replica Manager) — Parte 2
├── cliente.py               # ponto de entrada do cliente
├── network/
│   ├── protocol.py          # encode/decode JSON e construtores de mensagens
│   └── udp.py               # criação de sockets e I/O UDP
├── services/
│   ├── discovery.py         # descoberta broadcast/unicast (cliente e réplicas)
│   ├── processing.py        # lógica exactly-once de processamento
│   ├── replication.py       # propagação de estado para os backups
│   ├── election.py          # algoritmo de eleição de líder (valentão/bully)
│   └── heartbeat.py         # envio e monitoramento de heartbeats
└── models/
    ├── client_state.py      # estado por cliente mantido no servidor
    ├── server_state.py      # estado global (num_reqs, total_sum uint64)
    └── rm_state.py          # estado da réplica (papel, peers, líder, eleição)
```

## Requisitos

- Python 3.8+
- Linux ou Windows
- Sem dependências externas (apenas stdlib)

## Como executar

### Servidor (Parte 1 — sem replicação)

```bash
python3 servidor.py <porta>
```

### Servidor replicado (Parte 2 — `servidor_rm.py`)

Cada réplica (Replica Manager) sobe com um identificador único (`rm_id`). O grupo
elege automaticamente um **primário** (maior `rm_id`, algoritmo do valentão); os
demais ficam como **backups** que mantêm uma cópia do estado. Em caso de falha do
primário, um backup assume e notifica os clientes. Uma réplica reiniciada
**reingressa automaticamente**: descobre o líder atual, sincroniza o estado sob
demanda e, se tiver o maior `rm_id`, reassume a liderança (valentão) sem perda de
estado.

```bash
# IP auto-detectado, peers descobertos por broadcast:
python3 servidor_rm.py <rm_id> <porta> [peer_id:peer_ip:peer_port ...]

# IP explícito:
python3 servidor_rm.py <rm_id> <ip> <porta> [peer_id:peer_ip:peer_port ...]
```

- **`rm_id`** — inteiro único por réplica. O maior `rm_id` ativo vira o primário.
- **`peers`** — opcional. Se omitidos, as réplicas se descobrem por broadcast na
  mesma porta. Informe-os explicitamente quando rodar várias réplicas na **mesma
  máquina** (portas diferentes impedem o broadcast de funcionar).

**Em máquinas distintas, mesma sub-rede (peers via broadcast):**

```bash
# Estação 1
python3 servidor_rm.py 1 4000
# Estação 2
python3 servidor_rm.py 2 4000
# Estação 3
python3 servidor_rm.py 3 4000   # maior id → assume o papel de primário
```

**Na mesma máquina (portas distintas, peers explícitos):**

```bash
# Terminal 1
python3 servidor_rm.py 1 5001 2:127.0.0.1:5002 3:127.0.0.1:5003
# Terminal 2
python3 servidor_rm.py 2 5002 1:127.0.0.1:5001 3:127.0.0.1:5003
# Terminal 3
python3 servidor_rm.py 3 5003 1:127.0.0.1:5001 2:127.0.0.1:5002
```

O cliente é o **mesmo** dos dois casos (`cliente.py <porta>`): ele descobre o
primário por broadcast e, se o primário cair, é notificado e migra para o novo
líder automaticamente.

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

Mensagens adicionais da versão replicada (Parte 2), trocadas entre réplicas — e
`NEW_LEADER`, enviada do novo primário ao cliente:

| Tipo                          | Direção           | Finalidade                                  |
| ----------------------------- | ------------------- | ------------------------------------------- |
| `HEARTBEAT` / `HEARTBEAT_ACK` | primário ↔ backup | detecção de falha do primário (o `HEARTBEAT` carrega id/endereço do líder) |
| `REPLICATE`                   | primário → backup | propaga o estado após cada soma             |
| `ELECTION` / `OK` / `COORDINATOR` | RM ↔ RM         | eleição de líder (algoritmo do valentão)    |
| `RM_ANNOUNCE` / `RM_ANNOUNCE_ACK` | RM ↔ RM         | descoberta de réplicas por broadcast        |
| `WHO_IS_LEADER`               | RM → RM/broadcast | réplica que (re)entra pergunta quem é o líder atual |
| `STATE_REQUEST` / `STATE_TRANSFER` | RM ↔ RM       | sincronização de estado sob demanda no reingresso |
| `NEW_LEADER`                  | primário → cliente | notifica o cliente do novo líder após failover |

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
