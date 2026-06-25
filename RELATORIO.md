# Relatório — Trabalho Prático Parte 2

## Serviço Distribuído de Soma de Inteiros com Replicação Passiva e Eleição de Líder

**INF01085 — Sistemas Distribuídos e Tolerantes a Falhas — UFRGS**

---

## 1. Descrição do ambiente de testes

O desenvolvimento e os testes funcionais (incluindo os testes multi-máquina de
failover) foram realizados no seguinte ambiente:

| Item                     | Especificação                                                      |
| ------------------------ | -------------------------------------------------------------------- |
| Sistema operacional      | Ubuntu 24.04.4 LTS (Noble Numbat)                                    |
| Kernel                   | Linux 6.17.0-35-generic                                              |
| Processador              | Intel® Core™ i7-1355U (13ª geração) — 10 núcleos / 12 threads |
| Memória RAM             | 31 GiB                                                               |
| Linguagem / "compilador" | Python 3.12.3 (CPython)                                              |

> **Observação sobre "compilador":** o projeto é escrito em Python 3, uma
> linguagem interpretada — não há etapa de compilação para código de máquina.
> O interpretador utilizado é o CPython 3.12.3. Para atender ao requisito de
> "compilação via scripts automatizados", o pacote inclui um `Makefile` com
> alvos que verificam a versão do Python e disparam servidores e clientes
> (`make servidor`, `make cliente`, etc.).

Os testes de tolerância a falhas entre máquinas distintas foram executados em
uma rede local (sub-rede `192.168.0.0/24`), com cada réplica em uma estação
física diferente, comunicando-se exclusivamente por UDP.

---

## 2. Visão geral da arquitetura

A aplicação é composta por dois executáveis e um conjunto de módulos de apoio:

```
servidor_rm.py        Réplica (Replica Manager) — primário ou backup
cliente.py            Cliente que lê inteiros da entrada padrão e os envia
servidor.py           Servidor da Parte 1 (mantido, sem replicação)

network/
  protocol.py         Codificação/decodificação das mensagens (JSON sobre UDP)
  udp.py              Criação de sockets e envio/recepção UDP
models/
  server_state.py     Estado do serviço (acumulador, nº de requisições, clientes)
  client_state.py     Estado por cliente (último id_req, último ACK)
  rm_state.py         Estado da réplica (papel, peers, líder, eleição)
services/
  processing.py       Processamento de requisições de soma (exactly-once)
  discovery.py        Descoberta por broadcast (cliente↔servidor e entre RMs)
  replication.py      Propagação de estado para os backups
  election.py         Algoritmo de eleição de líder (valentão / bully)
  heartbeat.py        Envio e monitoramento de heartbeats
```

Toda a comunicação entre processos usa **exclusivamente a API UDP**, conforme
exigido. Cada réplica utiliza **um único socket** (porta de serviço) para todos
os tipos de mensagem — requisições de clientes, heartbeats, replicação e
mensagens de eleição — distinguindo-os por um campo `type` no payload JSON.

### Concorrência (requisito obrigatório)

O laço principal de recepção apenas lê o datagrama e despacha. Cada requisição
de soma (`REQUEST`) é processada em **uma thread dedicada** (`threading.Thread`),
exatamente como pede a especificação ("uma thread para processar cada requisição
recebida"). O estado compartilhado (`ServerState`) é protegido por um
`threading.Lock`, garantindo que a soma permaneça correta sob acesso concorrente
de múltiplos clientes. Foi validado em teste: 2 clientes concorrentes, cada um
somando 1..25, produziram `num_reqs = 50` e `total_sum = 650` (= 2 × 325), sem
perda de parcelas.

---

## 3. (A) Algoritmo de eleição de líder

### 3.1 Algoritmo escolhido: Valentão (Bully)

A especificação determina explicitamente o uso do **algoritmo do valentão**. Sua
ideia central: cada réplica tem um identificador único (`rm_id`), e o processo
**de maior identificador** entre os vivos deve ser o coordenador (RM primário).

A implementação está em `services/election.py` e usa três mensagens:

| Mensagem        | Significado                                                               |
| --------------- | ------------------------------------------------------------------------- |
| `ELECTION`    | "Estou iniciando uma eleição" — enviada aos RMs de ID**maior**   |
| `OK`          | "Estou vivo e tenho ID maior, pode parar" — resposta de um RM superior   |
| `COORDINATOR` | "A eleição acabou,**eu** sou o novo primário" — enviada a todos |

### 3.2 Funcionamento passo a passo

1. **Detecção da falha.** Cada backup mantém uma thread monitora
   (`heartbeat.py`) que observa o instante do último heartbeat recebido do
   primário. Se passar `FAILURE_TIMEOUT` (3 s) sem heartbeat, dispara uma
   eleição.
2. **Início da eleição.** O RM que detectou a falha envia `ELECTION` a **todos
   os RMs com ID maior** que o seu e arma um temporizador (`ELECTION_TIMEOUT`,
   2 s).

   - Se **não existe** nenhum RM de ID maior, ele se declara vencedor
     imediatamente.
   - Se algum RM superior responde `OK`, ele desiste de se declarar líder e
     passa a aguardar o `COORDINATOR` daquele RM.
3. **Vitória.** Se o temporizador expira **sem** ter recebido `OK`, o RM se
   declara vencedor: envia `COORDINATOR` a todos os peers, assume o papel de
   primário e passa a enviar heartbeats.
4. **Aceitação.** Ao receber `COORDINATOR`, um RM reconhece o remetente como
   líder, assume o papel de backup e reinicia seu relógio de detecção de falha.
5. **Recursão.** Um RM que recebe `ELECTION` de um ID **menor** responde `OK` e
   inicia sua **própria** eleição — assim a disputa propaga-se para cima até o
   maior ID vivo vencer.

### 3.3 Justificativa da escolha

Além de ser o algoritmo exigido, o valentão é adequado a este cenário porque:

- **Determinístico e simples de raciocinar:** o vencedor é sempre o maior ID
  vivo, o que torna o resultado previsível e fácil de demonstrar.
- **Convergência rápida em grupos pequenos:** com poucas réplicas (cenário do
  laboratório), o número de mensagens e o tempo até a convergência são baixos.
- **Não exige conhecimento global prévio:** combinado com a descoberta por
  broadcast, cada réplica só precisa conhecer os peers atuais.

### 3.4 Extensão: afirmação de liderança na entrada

Implementamos um comportamento adicional coerente com o valentão: quando uma
réplica **entra** no sistema e descobre que possui o maior ID entre os peers
conhecidos, ela envia `COORDINATOR` imediatamente, em vez de esperar a próxima
falha. Isso evita "split-brain" quando um RM de ID superior é iniciado depois de
um cluster já em operação — o primário antigo (de ID menor) cede o papel na hora.

---

## 4. (B) Implementação da replicação passiva

### 4.1 Modelo

Adotamos **replicação passiva (primary-backup)**: um único RM **primário**
atende os clientes; os demais são **backups** que mantêm uma cópia do estado.

O estado replicado (`ServerState`) contém: o acumulador (`total_sum`), o número
de requisições (`num_reqs`) e a tabela de clientes (`client_id → último id_req, último num_reqs, último total_sum`).

### 4.2 As duas garantias exigidas

**(1) Todos os clientes sempre usam o RM primário.**

- Apenas o primário responde às mensagens de descoberta (`DISCOVERY`); os
  backups ficam **silenciosos** (`discovery.py`). Assim, o broadcast de um
  cliente sempre converge para o líder atual.
- Os backups que recebem uma `REQUEST` por engano simplesmente a descartam — o
  cliente acaba redescobrindo o primário correto.

**(2) Após cada soma, o primário propaga o estado aos backups.**

- Em `processing.py`, logo após responder o ACK ao cliente, o primário chama
  `replicate_to_backups`, que envia uma mensagem `REPLICATE` com o estado
  completo (serializado em JSON) a todas as réplicas backup (`replication.py`).
- Cada backup que recebe `REPLICATE` sobrescreve seu estado local
  (`apply_replicate`).

### 4.3 Transparência para o cliente (notificação do novo líder)

A especificação exige que a troca de servidor seja transparente e que os
clientes sejam **notificados** do novo primário. Implementamos isso de duas
formas complementares:

- **Notificação ativa (push):** ao assumir, o novo primário envia uma mensagem
  `NEW_LEADER` (com seu IP/porta) a todos os clientes que conhece — fazendo-os
  redirecionar **imediatamente**, sem esperar timeout.
- **Redescoberta (fallback):** se um cliente fica sem resposta por várias
  tentativas, ele dispara um novo broadcast de descoberta e migra para quem
  responder (o novo primário). Cobre clientes que o novo líder ainda não
  conhecia.

### 4.4 Consistência da soma através do failover (exactly-once)

Para que a soma permaneça **correta** mesmo com perdas de pacotes e troca de
servidor, cada requisição carrega um `id_req` sequencial por cliente:

- O primário só aplica a soma quando `id_req == último + 1`; uma requisição
  repetida (ACK perdido) é detectada como duplicata e **não** é somada de novo —
  apenas o ACK em cache é reenviado.
- Como o estado (incluindo o último `id_req` de cada cliente) é replicado, o
  novo primário continua a sequência exatamente de onde o anterior parou.

Validação em teste: 2 clientes enviando 25 valores cada, com o primário sendo
**morto no meio do fluxo**; o backup assumiu e a soma final foi exatamente
`650`, sem repetição nem perda.

### 4.5 Desafios encontrados na replicação

- **Escolha entre replicação síncrona e assíncrona.** Optamos por propagação
  *fire-and-forget* (o primário não espera ACK do backup antes de responder ao
  cliente). Isso favorece o critério secundário de desempenho (latência baixa
  por requisição). O risco teórico é a perda de uma atualização se o primário
  cair no instante exato entre o ACK ao cliente e o `REPLICATE`; em rede local
  esse risco é desprezível, e a especificação não exige replicação confirmada.
- **Serialização do estado para UDP.** As chaves da tabela de clientes eram
  tuplas `(ip, porta)`, não serializáveis diretamente em JSON. Passamos a
  indexar por uma string `client_id` (ver problema 5.2), o que também resolveu
  um bug de consistência no failover.
- **Identificação estável do cliente.** Detalhado no item 5.2.

---

## 5. Problemas encontrados e como foram resolvidos

Esta seção relata os principais problemas reais enfrentados durante a
implementação e depuração (todos registrados no histórico de commits).

### 5.1 Cliente não migrava após o failover

**Sintoma:** ao matar o primário, o cliente ficava preso retransmitindo, mesmo
após o novo primário assumir.

**Causa:** durante a redescoberta, a função de descoberta do cliente só aceitava
respostas do tipo `DISCOVERY_RESPONSE` e ignorava a notificação `NEW_LEADER`
que o novo primário enviava.

**Solução:** a descoberta passou a aceitar também `NEW_LEADER` e a drenar
mensagens dentro de cada janela de espera, em vez de descartar a janela ao
primeiro pacote inesperado.

### 5.2 Estado inconsistente ao trocar de interface de rede

**Sintoma:** após o failover, o servidor tratava o cliente como novo
(`last_req = 0`) e respondia ACKs obsoletos indefinidamente.

**Causa:** o estado dos clientes era indexado pelo endereço de origem UDP
`(ip, porta)`. Quando o cliente migrava entre interfaces (por exemplo, de uma
rede local para loopback em testes na mesma máquina), o IP de origem mudava e o
servidor não reconhecia o cliente.

**Solução:** passamos a indexar o estado por um `client_id` estável (definido na
inicialização do cliente e enviado em cada requisição), independente do endereço
de origem. Isso preservou a semântica *exactly-once* através do failover.

### 5.3 Re-eleições espúrias após `COORDINATOR`

**Sintoma:** com 3 ou mais réplicas, logo após uma eleição, um backup disparava
**outra** eleição sem necessidade.

**Causa:** ao voltar ao papel de backup, o relógio de detecção de falha
(`last_heartbeat`) ainda apontava para o primário antigo (já morto), então o
monitor estourava o timeout imediatamente.

**Solução:** ao receber `COORDINATOR`, o backup reinicia `last_heartbeat`, dando
ao novo primário tempo de enviar o primeiro heartbeat.

### 5.4 Transições de papel incorretas com entrada sequencial de réplicas

**Sintoma:** subindo 3 réplicas em sequência e derrubando o primário, a eleição
seguinte falhava ou gerava dois primários.

**Causas e soluções (três correções combinadas):**

- O remetente de um `COORDINATOR` não era adicionado à lista de peers; passamos
  a registrá-lo, para que eleições futuras enviem `ELECTION` corretamente.
- Um RM que era primário e recebia `COORDINATOR` continuava com a thread de
  **envio** de heartbeat; passou a trocar para a thread **monitora**, podendo
  assim detectar a falha do novo primário.
- Uma condição de corrida permitia que o callback "tornar-se primário"
  sobrescrevesse um rebaixamento recém-recebido; adicionamos uma verificação de
  papel antes de assumir.

### 5.5 "Split-brain" ao entrar um RM de ID maior

**Sintoma:** um primário de ID menor, já em operação, não cedia o papel quando
um RM de ID maior era iniciado depois.

**Solução:** ao iniciar, o RM de maior ID afirma a liderança enviando
`COORDINATOR` aos peers conhecidos (ver item 3.4).

### 5.6 Tempestade de requisições duplicadas entre máquinas

**Sintoma:** em testes entre máquinas distintas, o log do servidor enchia de
`DUP!!` — embora a soma permanecesse correta.

**Causas:**

- O timeout de retransmissão do cliente era de 10 ms, menor que o tempo de
  ida-e-volta real em rede local; o cliente retransmitia antes de o ACK chegar.
- O cliente reenviava a requisição a cada pacote inesperado recebido (ACK
  antigo, `NEW_LEADER` repetido), criando um laço de realimentação que se
  auto-alimentava.

**Solução:** aumentamos o timeout para 200 ms e reestruturamos o laço do
cliente: envia **uma vez** por tentativa e drena pacotes dentro da janela; só
retransmite em timeout real ou troca de líder; pacotes obsoletos são descartados
sem reenvio. Importante: graças à detecção *exactly-once*, as duplicatas **nunca
afetaram a corretude da soma** — eram apenas ruído de rede e de log.

### 5.7 Processos "fantasma" durante os testes (problema operacional)

**Sintoma:** clientes encontravam um servidor errado ou inexistente; resultados
de teste irreprodutíveis.

**Causa:** processos de servidor de testes anteriores permaneciam vivos,
presos à porta de serviço (sockets UDP não liberados após suspensão do
terminal). Cada "fantasma" participava da eleição e respondia ao broadcast.

**Solução (operacional):** estabelecemos um protocolo de limpeza antes de cada
teste (`pkill -9 -f servidor_rm.py` e verificação da porta com `ss -ulpn`), além
de orientar o uso de `Ctrl+C` (encerra) em vez de `Ctrl+Z` (apenas suspende).
Não é um defeito do código distribuído em si, mas afetou bastante a depuração e
vale registro.

---

## 6. Funcionalidades adicionais implementadas

- **Descoberta automática de réplicas por broadcast:** quando os peers não são
  informados na linha de comando, cada RM anuncia sua presença por broadcast e
  descobre os demais — útil no laboratório, onde todas as máquinas usam a mesma
  porta.
- **IP auto-detectado:** o RM pode ser iniciado apenas com `id` e `porta`; o IP
  local é descoberto via tabela de rotas do SO.
- **Notificação ativa de novo líder (`NEW_LEADER`)** além da redescoberta
  passiva, tornando o failover quase instantâneo para clientes já conhecidos.

---

## 7. Conclusão

Todos os requisitos obrigatórios foram implementados e validados em testes
funcionais, incluindo cenários multi-máquina de falha do primário:

- soma correta de todos os clientes, com processamento concorrente (uma thread
  por requisição) e sincronização por lock;
- comunicação exclusivamente via UDP;
- replicação passiva com as duas garantias exigidas;
- eleição de líder pelo algoritmo do valentão, com notificação transparente do
  novo líder aos clientes.

O principal aprendizado prático foi que a maior parte da dificuldade em sistemas
distribuídos não está no "caminho feliz", mas nas transições de estado
(failover, entrada/saída de réplicas) e nas condições de corrida entre threads e
temporizadores — exatamente os pontos descritos na seção 5.
