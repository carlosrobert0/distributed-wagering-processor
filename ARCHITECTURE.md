# ARCHITECTURE — Distributed Wagering Processor

Este documento registra **cada decisão técnica, o porquê, o trade-off e as limitações**.
Os requisitos rastreáveis (`AC-*`) e as interpretações do enunciado (`INT-*`) estão em [`SPEC.md`](SPEC.md).

---

## 1. Visão geral

```
                    ┌──────────────── instância N (idênticas, sem estado em memória) ───────────────┐
 Provedor ──HTTP──► │ interfaces/http ──┐                                                          │
                    │                   ├─► ProcessWagerTransactionUseCase ─► WagerSettlementService │
 SQS FIFO ─────────►│ interfaces/sqs ───┘        (único use case)              (domínio puro)       │
 wager-transactions │                                   │ UnitOfWork (1 transação SQL)              │
                    │ workers: outbox relay · pending-reference                                     │
                    └───────────────────────────────────┼───────────────────────────────────────────┘
                                                        ▼
                         PostgreSQL: wallets · wager_transactions · wallet_ledger_entries
                                     inbox_messages · outbox_messages   (+ constraints/triggers)
                                                        │ relay (FOR UPDATE SKIP LOCKED)
                                                        ▼
                                          SQS FIFO wagering-events (eventos de integração)
```

**Princípio central:** o banco é o árbitro de toda invariante. A aplicação decide as regras de negócio,
mas unicidade, não-negatividade, imutabilidade e "saldo == ledger" são garantidas por constraints e
triggers. Assim, N instâncias (ou um bug, ou um UPDATE manual) não conseguem corromper o estado.

### Fluxo de uma BET (uma transação SQL)

```
1. [fora da tx] valida forma + Money → payloadHash        (inválido: 400, nunca abre transação)
2. [tx curta]   replay rápido: inbox/idempotency key já existe? → devolve o resultado original
3. BEGIN
4.   inbox: INSERT … ON CONFLICT DO NOTHING                   (só quando a entrada é a fila)
5.   SELECT … FROM wallets WHERE id = ? FOR UPDATE            (serializa a wallet em TODAS as instâncias)
6.   idempotência de novo, agora sob lock                     (replay / 409)
7.   domínio: settleWagerTransaction(wallet, tx, referência)  (PROCESSED | REJECTED | PENDING_REFERENCE)
8.   INSERT wager_transactions · INSERT wallet_ledger_entries · UPDATE wallets … WHERE version = ?
9.   INSERT outbox_messages (eventos)
10. COMMIT  ── constraint triggers diferidas conferem saldo == último lançamento
11. resposta HTTP / DeleteMessage no SQS                       (ack só depois do commit)
12. [depois] relay publica a outbox                            (nunca antes do commit)
```

---

## 2. Camadas (Clean Architecture)

```
src/
  domain/           regras puras: Money, Wallet, WalletLedgerEntry, WagerTransaction, settlement,
                    InboxMessage, OutboxMessage, IntegrationEvent. Zero import de Nest/ORM/AWS.
  application/      use cases + ports (UnitOfWork, repositórios, Clock, IdGenerator, Metrics,
                    Logger, EventPublisher). Sem decorators do Nest.
  infrastructure/   adapters: MikroORM (EntitySchema, mappers, repositórios, migrations), SQS,
                    pino, prom-client, config.
  interfaces/       adapters de entrada: HTTP (Nest controllers), consumer SQS, workers de polling.
  app.module.ts     composition root — o único lugar que conhece todas as camadas.
```

Regra de dependência: `interfaces → application → domain` e `infrastructure → application/domain`.
O domínio nunca importa de fora. Use cases são classes comuns instanciadas por factory no
`AppModule` — por isso podem ser testados sem Nest e reaproveitados pelo HTTP e pelo SQS.

**Por que um serviço de domínio (`settleWagerTransaction`) em vez de métodos na Wallet?** A decisão
envolve dois agregados (Wallet e WagerTransaction, mais a referência). Colocar isso num serviço puro
mantém cada agregado responsável pelas próprias invariantes (Wallet: saldo/versão/lançamento;
WagerTransaction: máquina de estados) e deixa as regras do §7 testáveis em unidade, sem banco.

---

## 3. Modelo de domínio

### 3.1 Money — `bigint` em centavos

- Representação interna: `bigint` de centavos (escala fixa 2) + moeda ISO-4217.
- Entrada de contrato (`Money.from`): regex `^(0|[1-9]\d{0,17})\.\d{2}$` → rejeita `NaN`, `Infinity`,
  `1e3`, `""`, `1.0`, `1.001`, `-1.00`, `+1.00`, `01.00`, espaços e `number`.
- Reidratação (`Money.rehydrate`): aceita sinal (diferenças de reconciliação), formato igualmente estrito.
- **Arredondamento:** não existe. O domínio só soma e subtrai; escala fixa ⇒ aritmética exata. Entradas
  com mais de 2 casas são rejeitadas, não arredondadas (o enunciado manda rejeitar).
- Limite: 18 dígitos inteiros, o que cabe em `NUMERIC(20,2)`.

**Por que não `decimal.js`?** Com escala fixa e só soma/subtração, `bigint` é exato, nativo, sem
dependência e trivial de explicar. Se um dia houver divisão (ex.: rateio), troca-se a implementação
interna de `Money` sem tocar no resto — a API pública é string.

**Persistência:** `NUMERIC(20,2)` + `CHAR(3)`. O driver `pg` devolve NUMERIC como **string**; os mappers
fazem `Money.rehydrate(string)`. Somas de reconciliação são feitas em SQL (`NUMERIC`) e devolvidas
como texto. Nenhum valor monetário passa por `number` em nenhuma camada.

### 3.2 Wallet (aggregate root)

- `Wallet.open` cria com `version = 1` e, se o saldo inicial for > 0, já devolve o lançamento `CREDIT`
  de abertura (`walletVersion = 1`).
- `debit/credit` **retornam o `WalletLedgerEntry`**: é impossível mudar o saldo sem produzir o
  lançamento correspondente. O lançamento é criado antes de mutar o estado (se a aritmética falhar,
  a wallet fica intacta).
- `version` incrementa somente quando o saldo muda; cada lançamento carrega a versão que produziu.

### 3.3 WagerTransaction — máquina de estados

```
PENDING ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
PENDING_REFERENCE ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE (nova verificação agendada)
PROCESSED, REJECTED, FAILED: terminais → qualquer transição lança InvalidTransactionStateError
```

`PENDING` existe só em memória durante o processamento (nunca é persistido em fluxo normal).
`OPENING` só nasce por `WagerTransaction.opening()` — `create()` (entrada externa) o rejeita, assim
como o `providerId` reservado `internal`.

### 3.4 WalletLedgerEntry

Campos `readonly` + `Object.freeze` + nenhum método de transição. `create` valida
`balanceBefore ± money == balanceAfter`, valor positivo e saldos não negativos. Ledger de entrada
simples (uma linha por movimento); partidas dobradas ficaram fora (opcional no enunciado).

---

## 4. ORM: MikroORM

**Escolha:** MikroORM 7 (preferencial no enunciado). O que usamos dele, e por quê:

| Recurso | Uso |
|---|---|
| `EntitySchema` (sem decorators) | Records de persistência separados do domínio; evita depender de `emitDecoratorMetadata`/`reflect-metadata` no ORM sob Bun. |
| `em.fork()` + `em.transactional()` | Um contexto isolado por transação; nada compartilhado entre requisições concorrentes da mesma instância. |
| `LockMode.PESSIMISTIC_WRITE` / `PESSIMISTIC_READ` | `FOR UPDATE` na wallet (escrita) e `FOR SHARE` (reconciliação). |
| `LockMode.PESSIMISTIC_PARTIAL_WRITE` | `FOR UPDATE SKIP LOCKED` no relay da outbox e no worker de referências. |
| `em.insert` / `em.nativeUpdate` | Escritas explícitas, em ordem determinística. |
| Migrator + `migrationsList` | Migrations versionadas, transacionais e reversíveis (`up`/`down`), registradas explicitamente (sem glob — funciona igual no Bun, testes e container). |

**Trade-off — escritas explícitas em vez de `flush()` implícito do Unit of Work.** O domínio é
separado dos records (mapper), então a UoW não "enxerga" mudanças nas entidades de domínio. Além disso,
a ordem dos comandos importa (a transação precisa existir antes do lançamento que a referencia; o
`UPDATE` da wallet é condicionado à versão). Escrever explicitamente deixa o SQL previsível e
auditável. O preço é abrir mão do change-tracking automático — aceitável num serviço cujo coração
são poucas escritas muito bem definidas. Leituras usam `disableIdentityMap: true`: o record vira
domínio imediatamente; o identity map não agregaria e poderia servir dado velho dentro de retries.

---

## 5. Concorrência

**Estratégia: lock pessimista por linha (`SELECT … FOR UPDATE` na wallet) dentro de `READ COMMITTED`.**

- **Por que pessimista e não otimista?** A unidade de concorrência é a wallet, e uma hot wallet
  (jogador fazendo muitas apostas por segundo, ou rajadas de retries) teria alta contenção. Com
  optimistic locking, cada conflito vira abort + retry, que sob contenção vira tempestade de retries e
  latência imprevisível. O lock pessimista transforma contenção em **fila ordenada** no Postgres.
- **Sem lock global:** o lock é na linha da wallet; wallets diferentes rodam em paralelo (testado).
- **Espera limitada:** cada transação faz `set_config('lock_timeout', …, true)` (5 s por padrão). Estourou →
  `TransientInfrastructureError` → HTTP 503 + `Retry-After` / SQS volta com backoff. Nunca fila infinita.
- **Defesa em profundidade:** o `UPDATE wallets … WHERE id = ? AND version = ?` (condicionado) falharia
  se o lock fosse burlado; e no schema, `UNIQUE (wallet_id, wallet_version)` no ledger + o trigger de
  encadeamento tornam um lost update **impossível de commitar**.
- **Por que não SERIALIZABLE?** Toda escrita de saldo já passa pelo lock da wallet, e as corridas
  restantes (mesma idempotency key em wallets diferentes, por exemplo) são resolvidas por `UNIQUE`.
  SERIALIZABLE adicionaria aborts de serialização sem ganho de correção.
- **Ordem de lock (sem deadlock):** quem espera por lock sempre trava wallet → transação. A única
  exceção é a reivindicação de candidatas do worker (`UPDATE … WHERE id IN (… FOR UPDATE SKIP LOCKED)`),
  que trava linhas de transação sem a wallet — mas com `SKIP LOCKED` ela nunca espera, logo não fecha
  ciclo. Deadlocks residuais (detectados pelo Postgres) viram erro transitório e são re-tentados.

**Corridas entre instâncias que o lock da wallet não cobre** — ex.: a mesma idempotency key enviada
para duas wallets diferentes — terminam em violação de `UNIQUE`; o use case captura
(`DuplicateTransactionError`), repete, e na segunda tentativa encontra a vencedora → replay ou 409.

**Trade-off assumido:** o throughput de UMA wallet é limitado pela serialização (uma transação por vez).
É o comportamento correto para dinheiro; wallets diferentes escalam horizontalmente.

---

## 6. Idempotência

- **Fonte da verdade:** header `Idempotency-Key` (HTTP) ou `data.idempotencyKey` (SQS). Persistido com
  `UNIQUE (idempotency_key)`; também `UNIQUE (provider_id, external_transaction_id)`.
- **`payloadHash`** = `sha256( canonicalJson(campos de negócio) )` em hex.
  - Campos: `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind,
    money{amount,currency}, referenceExternalTransactionId?`.
  - JSON canônico: chaves ordenadas recursivamente, sem espaços, `undefined` omitido.
  - Fora do hash: a própria key, `messageId`, `occurredAt`, correlation ids, campos desconhecidos (descartados).
- **Replay:** mesma key + mesmo hash → mesma resposta com `idempotentReplay: true`, incluindo o **saldo
  observado no momento da decisão** (`balance_after` gravado na transação), não o saldo atual.
- **Conflito:** mesma key + hash diferente → `409 IDEMPOTENCY_KEY_CONFLICT`. Mesmo
  `(provider, externalTransactionId)` com outra key → `409 EXTERNAL_TRANSACTION_CONFLICT`. Nunca replay.
- **Caminho rápido:** antes de pegar o lock, uma transação curta procura a key. Assim, 50 duplicatas de
  uma aposta não fazem fila no lock da hot wallet. A checagem decisiva é repetida **sob o lock**.
- **Nada em memória:** não há cache; toda decisão de idempotência consulta o banco.

---

## 7. Schema e constraints (restrição 9)

| Garantia | Objeto no banco |
|---|---|
| Saldo nunca negativo | `wallets_balance_non_negative_ck`, `ledger_balances_non_negative_ck` |
| Uma wallet por player+moeda | `wallets_player_currency_uq` |
| Idempotência | `wager_tx_idempotency_key_uq`, `wager_tx_provider_external_uq` |
| Reversão única por referência | índice único parcial `wager_tx_single_reversal_uq` (`WHERE kind IN ('REFUND','ROLLBACK') AND status='PROCESSED'`) |
| ≤ 1 lançamento por transação/wallet | `ledger_transaction_wallet_uq` |
| Sem lost update | `ledger_wallet_version_uq` + trigger `ledger_validate_entry` (versão n = n−1 + 1, `before(n) = after(n−1)`) |
| Aritmética do lançamento | `ledger_arithmetic_ck` |
| Lançamento na moeda da wallet | FK composta `(wallet_id, currency) → wallets(id, currency)` |
| Só PROCESSED gera lançamento; valor = valor da transação | trigger `ledger_validate_entry` |
| Ledger imutável | triggers `ledger_append_only` (UPDATE/DELETE) e `ledger_no_truncate` |
| Transação terminal imutável; campos de negócio imutáveis | trigger `wager_tx_guard` |
| Coerência de status | `wager_tx_failure_code_ck`, `wager_tx_terminal_timestamp_ck`, `wager_tx_processed_balance_ck`, `wager_tx_unsettled_balance_ck`, `wager_tx_balance_currency_ck`, `wager_tx_pending_schedule_ck`, `wager_tx_reference_resolved_ck` |
| REFUND/ROLLBACK exigem referência | `wager_tx_reversal_requires_reference_ck` (implicação — WIN pode referenciar, INT-03) |
| **saldo materializado == ledger** | constraint triggers `DEFERRABLE INITIALLY DEFERRED` em `wallets` e no ledger, checadas no **COMMIT** |
| Evento da outbox imutável | trigger `outbox_guard` |

Índices: parciais para o que só interessa enquanto pendente (`PENDING_REFERENCE`, outbox não publicada),
`(wallet_id, wallet_version)` serve a paginação do ledger, `(provider_id, reference_external_transaction_id)`
parcial para "acordar" dependentes.

**Por que constraint triggers diferidas?** A igualdade saldo == último lançamento só faz sentido com o
estado final da transação: ela é checada no COMMIT, então o UPDATE da wallet pode vir antes ou depois do
INSERT do lançamento. Já o encadeamento do ledger (`ledger_validate_entry`) é imediato (`BEFORE INSERT`):
exige que a transação já exista como `PROCESSED` e que versão/saldo continuem a cadeia — por isso a
aplicação grava sempre transação → lançamento → saldo.

**Saldo observado com moeda própria.** `balance_after` vem acompanhado de `balance_currency` (a moeda da
wallet). Sem isso, o replay de uma rejeição `CURRENCY_MISMATCH` devolveria o saldo BRL rotulado com a
moeda USD da transação.

Migrations versionadas e reversíveis: `Migration20261021000000_initial_schema` (domínio, ledger,
inbox, outbox) e `Migration20261022000000_consumer_message_attempts` (contagem de tentativas do
consumer). Testadas com up → down → up.

---

## 8. Regras de negócio e referências fora de ordem

Regras do §7 implementadas em `settleWagerTransaction` (domínio). Ordem de validação da referência:
`REFERENCE_NOT_PROCESSED` → `REFERENCE_MISMATCH` (provider/player/wallet/rodada/moeda) →
`INVALID_REFERENCE_KIND` → (WIN/LOSS) `REFERENCE_REVERSED` | (REFUND/ROLLBACK) `AMOUNT_MISMATCH` →
`ALREADY_REVERSED`. Depois, regras de saldo: `INSUFFICIENT_FUNDS` / `REVERSAL_WOULD_OVERDRAW` no débito e
`BALANCE_LIMIT_EXCEEDED` no crédito.

- **ROLLBACK** aplica o inverso da direção da referência (BET→CREDIT, WIN→DEBIT, REFUND→DEBIT).
- **Débito que deixaria saldo negativo:** `INSUFFICIENT_FUNDS` para aposta; `REVERSAL_WOULD_OVERDRAW`
  para reversão — situações operacionais diferentes (a segunda exige ação humana), ambas auditáveis.
- **Reversão única (INT-05):** uma referência é revertida no máximo uma vez **por qualquer tipo** —
  a leitura literal permitiria REFUND + ROLLBACK da mesma BET (crédito em dobro).

**PENDING_REFERENCE (§7.1):**
- Referência ausente (ou presente mas ainda pendente) → persiste `PENDING_REFERENCE` + evento.
- Worker agendado (em todas as instâncias): reivindica as vencidas de forma atômica
  (`UPDATE … SET next_reference_check_at = now + 30s WHERE id IN (… FOR UPDATE SKIP LOCKED) RETURNING`).
  O valor do lease funciona como **token**: ao processar, a linha só é travada se ainda tiver exatamente
  aquele `next_reference_check_at` — se o lease expirou e outra instância a pegou, ou se ela foi
  "acordada", esta instância desiste. O lote também para antes do lease expirar. Assim cada verificação
  conta uma tentativa só uma vez.
- Backoff: 2s·2ⁿ, teto 5 min, **8 verificações (~8,5 min)**. Justificativa: provedores reenviam a
  referência em segundos; uma janela longa deixaria rodadas abertas e o jogador sem resposta.
  Esgotou → `REJECTED REFERENCE_NOT_FOUND` + `WagerTransactionRejected`.
- **Wake-up:** quando uma transação termina, as dependentes que a aguardam têm `next_reference_check_at`
  antecipado para agora — latência de segundos sem polling agressivo.
- Erro não transitório no reprocessamento (dado corrompido/bug) → `FAILED PROCESSING_ERROR` (INT-12).

---

## 9. Mensageria

### 9.1 Consumer SQS (`wager-transactions.fifo`)

- Duas peças: `WagerMessageHandler` decide o destino de cada mensagem (`ack` / `retry` / `dead-letter`)
  sem conhecer o SQS — testável em unidade —, e `WagerTransactionsConsumer` executa a decisão (long-poll,
  grupos, ack, visibilidade, DLQ, shutdown).
- Reusa **o mesmo** `ProcessWagerTransactionUseCase` do HTTP.
- **Inbox** `(consumer_name, message_id)` gravada **na mesma transação** do efeito
  (`INSERT … ON CONFLICT DO NOTHING`) e marcada `processed_at` ao final. É uma barreira de verdade:
  mensagem já recebida ⇒ devolve o resultado original sem novo efeito (métrica `inbox_duplicate`);
  mesmo `messageId` com payload diferente ⇒ `INBOX_MESSAGE_CONFLICT`, erro permanente (DLQ). A
  idempotency key continua como segunda barreira (mesma transação chegando com outro `messageId`).
- **Ack (`DeleteMessage`) somente após o commit** — o use case só retorna depois do COMMIT.
- **Classificação de erros:**

| Classe | Exemplos | Ação |
|---|---|---|
| Negócio (terminal) | transação REJECTED, wallet inexistente, conflito de key | ack |
| Transitório | banco fora, lock/statement timeout, deadlock, corrida de unicidade, erro desconhecido | `ChangeMessageVisibility` com backoff exponencial (2s → 120s) |
| Permanente | JSON malformado, envelope inválido, payload inválido, `INBOX_MESSAGE_CONFLICT`, violação de integridade ou invariante | copia para a DLQ com o motivo + ack |

- **Limite de tentativas na aplicação:** cada falha transitória é registrada em
  `consumer_message_attempts (consumer_name, message_id)` numa transação própria (sobrevive ao rollback
  do processamento). Ao atingir `CONSUMER_MAX_ATTEMPTS` (5), a aplicação copia a mensagem para a DLQ com o
  motivo (`MAX_ATTEMPTS_EXCEEDED: …`) e faz ack. Conta-se tentativa **real**, não `ApproximateReceiveCount`:
  mensagens irmãs do mesmo grupo FIFO acumulam recebimentos enquanto esperam e iriam para a DLQ sem
  nunca terem falhado. A redrive policy (`maxReceiveCount = 20`) fica como rede de segurança.
- **Heartbeat de visibilidade:** enquanto um lote é processado, a visibilidade das mensagens ainda não
  finalizadas é estendida a cada `visibilityTimeout/2`, para que um grupo lento não seja reentregue a
  outra instância no meio do processamento.
- **Falhas isoladas:** grupos rodam com `Promise.allSettled`; falha de ack é logada (a redelivery é
  deduplicada pela inbox).
- **Ordem:** paralelo entre `MessageGroupId` (wallets), sequencial dentro do grupo. Se uma mensagem do
  grupo falha, o resto do grupo volta junto (preserva a ordem FIFO). FIFO é otimização — a correção
  vem do banco (§8 do enunciado).
- **SIGTERM:** `enableShutdownHooks` → `beforeApplicationShutdown`: `/health/ready` passa a responder
  `draining`, o long-poll é abortado, mensagens não iniciadas voltam com `VisibilityTimeout=0` e as em
  andamento terminam dentro do grace period; se ele estourar, as restantes também são devolvidas. Só
  depois o pool do banco é fechado.

**Crash entre commit e ack:** a mensagem volta após o visibility timeout; a outra instância encontra a
inbox (e a idempotency key) → só faz ack. Testado com um processo real morto por `SIGKILL`.

### 9.2 Transactional Outbox

- Eventos gravados em `outbox_messages` **na mesma transação** do efeito. Nada é publicado antes do commit.
- **Relay** (em todas as instâncias), em três passos:
  1. transação curta: `SELECT … WHERE published_at IS NULL AND next_attempt_at <= now() ORDER BY occurred_at
     FOR UPDATE SKIP LOCKED LIMIT N` e **lease** (`next_attempt_at = now + 60s`) → commit;
  2. publica no SQS **fora de transação** (cliente com timeout de conexão/requisição);
  3. grava `published_at` com `UPDATE … WHERE published_at IS NULL`; retry e devolução do lote só são
     gravados se a instância ainda detém o lease (`AND next_attempt_at = lease`).
  Nenhum lock de banco fica preso durante a chamada de rede; publishers concorrentes pegam lotes disjuntos.
- **At-least-once:** se o processo morre depois de publicar e antes do passo 3, o lease expira e outra
  instância republica com o **mesmo `eventId`**. Deduplicação: `MessageDeduplicationId = eventId` na
  fila FIFO (janela de 5 min, otimização) e, como garantia, o consumidor deduplica por `eventId`.
- **Falha de publicação:** `attempts++`, `next_attempt_at` com backoff 1s·2ⁿ (teto 5 min), `last_error`.
  **Nunca desiste** (INT-08): descartar violaria "não perder eventos confirmados". O atraso aparece em
  `wagering_outbox_lag_seconds`.
- **Destino (INT-09):** `wagering-events.fifo`, `MessageGroupId = walletId`.
- **Ordem (INT-15):** dentro de um lote, em ordem de `occurred_at`; no primeiro erro, o evento que falhou
  e todo o resto do lote recebem o **mesmo** `next_attempt_at`, então nenhum evento **do lote** passa à
  frente do que falhou (eventos enfileirados depois da falha ainda podem passar). Entre relays concorrentes a ordem por wallet é melhor esforço;
  `WalletBalanceChanged` traz `walletVersion` para o consumidor ordenar/descartar.

**Eventos:** `WagerTransactionProcessed` (inclusive LOSS e OPENING), `WagerTransactionRejected`,
`WalletBalanceChanged` (somente quando o saldo muda), `WagerTransactionPendingReference`. Envelope =
classe abstrata `IntegrationEvent<T>` com `eventType`/`version` no tipo concreto; `data` só com
`MoneyProps` (strings).

---

## 10. API HTTP — mapa de status (consistente em todos os endpoints)

| Situação | Status | O provedor deve |
|---|---|---|
| Payload inválido / sem `Idempotency-Key` | 400 | corrigir; não reenviar igual |
| Recurso inexistente | 404 | corrigir |
| Conflito (idempotência, external id, wallet duplicada) | 409 | não é replay; investigar |
| Rejeição de negócio (`REJECTED`/`FAILED` persistida) | 422 + `failureCode` | decidir pelo `failureCode` |
| Aceito, pendente de referência | 202 | aguardar / consultar |
| Processado agora / replay | 201 / 200 | sucesso |
| Falha transitória | 503 + `Retry-After` | reenviar com a mesma key |

Rejeição de negócio é 422 (e não 200 com `status: REJECTED`) para que o provedor nunca confunda
"aplicado" com "recusado" olhando só o status. O replay de uma rejeição devolve o mesmo 422.

---

## 11. Observabilidade

- **Logs:** pino JSON; `correlationId`/`messageId` entram automaticamente via `AsyncLocalStorage`;
  `transactionId`, `walletId`, `providerId` nos eventos de domínio. A porta `LogFields` só aceita
  escalares (não dá para logar um objeto `Money` ou um payload), e o `redact` do pino é uma segunda barreira.
  Valores e saldos **não** são logados.
- **Métricas** (`GET /metrics`, Prometheus):
  `wagering_transactions_total{status,kind,source}`, `wagering_duplicates_total{type}`,
  `wagering_retries_total{component}`, `wagering_dlq_messages_total{reason}`,
  `wagering_lock_conflicts_total{kind}`, `wagering_outbox_pending`, `wagering_outbox_lag_seconds`,
  `wagering_processing_duration_seconds{source}`, `wagering_wallet_lock_wait_seconds`,
  `wagering_reconciliations_total{result}`.
- **Health:** `/health/live` (processo responde; não toca dependências para evitar restart em cascata)
  e `/health/ready` (Postgres `SELECT 1` + SQS `GetQueueAttributes`, com timeout). Ambos públicos.
- **Reconciliação:** `POST /wallets/:id/reconciliation` lê sob `FOR SHARE`, recalcula em NUMERIC,
  confere encadeamento e versão; divergência → log de erro + métrica + `consistent:false`. Nunca corrige.

---

## 12. Autenticação (decisão: não implementada)

Vale 0 pontos e competiria com o que importa. O ponto de extensão está explícito no código:
`ProviderAuthGuard` (global) delega à porta `ProviderAuthenticator`, que recebe o header
`Authorization` e o `providerId` do payload. A implementação registrada é
`AllowAllProviderAuthenticator`; rotas `@Public()` (health/métricas) nem consultam a porta.
Ligar a autenticação = registrar outra implementação da porta, sem tocar em controllers.

**Desenho que adotaria (Keycloak, OIDC):**
1. Realm `wagering`; cada provedor é um *client confidencial* que obtém token via **client credentials**.
2. O guard valida o JWT (assinatura por JWKS em cache, `iss`, `aud`, `exp`).
3. A claim `provider_id` do token é comparada com `body.providerId`; divergência → 403. Um provedor
   não consegue movimentar transações de outro.
4. Mensagens da fila são canal interno confiável, mas `providerId` continua sujeito às regras de domínio.
5. Health/métricas seguem abertos (`@Public()`); métricas ficariam numa porta interna em produção.

---

## 13. Testes

| Tipo | O que prova | Onde |
|---|---|---|
| Unidade — domínio | Money, Wallet, ledger, máquina de estados, regras BET/WIN/LOSS/REFUND/ROLLBACK, backoff, hash canônico | `test/unit/domain` |
| Unidade — aplicação | orquestração dos use cases (replay, conflitos, inbox, corrida de unicidade, PENDING_REFERENCE, outbox) sobre um `UnitOfWork` em memória com commit/rollback | `test/unit/application` |
| Unidade — bordas | erro → status HTTP, resultado → status, classificação SQS, SQLSTATE → transitório, decisões do handler SQS, guard, config | `test/unit/interfaces` |
| Integração | migrations up/down, cada constraint/trigger, atomicidade (falha injetada antes do commit), API, outbox (2 publishers, crash, retry, duplicata), consumer (inbox, redelivery, retry, DLQ, shutdown), fora de ordem | `test/integration` |
| Concorrência | 100−80−80 (25×), hot wallet, 50× mesma aposta, wallets em paralelo, BET/WIN e REFUND/ROLLBACK simultâneos, 3 processos, crash antes do ack, SIGKILL + reinício | `test/concurrency` |

- **Sem mocks de Postgres ou SQS nos testes de integração e concorrência:** rodam contra os containers
  do `docker compose`. O `UnitOfWork` em memória existe só para testar orquestração em milissegundos; as
  garantias (locks, constraints, atomicidade) são provadas apenas contra o Postgres real.
- **Paralelismo real:** `Promise.all` de requisições HTTP (pool com conexões distintas) e processos
  separados via `Bun.spawn` (`bun run src/main.ts`).
- **Invariante final:** `expectWalletMatchesLedger` / `expectAllWalletsMatchLedger` recalculam o saldo
  pelo ledger em NUMERIC e conferem encadeamento e versão.
- **Determinismo:** cada arquivo recebe um banco próprio (clone de um template migrado) e filas próprias,
  então workers de instâncias de um teste nunca processam dados de outro; esperas são por condição
  (`waitFor`), nunca `sleep` cego para asserção; filas são drenadas apagando as mensagens (sem isso o FIFO
  bloqueia o grupo).
- **Injeção de falha:** `FAULT_INJECTION=crash-after-commit` (somente teste) mata o processo com SIGKILL
  entre o commit e o ack; `FailAfterOutboxUnitOfWork` força rollback depois de todas as escritas.

---

## 14. Convenções de código

- Sem comentários no código: nomes de classes, métodos, constantes e constraints carregam a intenção;
  o *porquê* das decisões vive neste documento e no `SPEC.md`.
- Formatação única com Prettier (`bun run format` / `bun run format:check`) e TypeScript strict
  (`noUncheckedIndexedAccess`, `noImplicitOverride`).
- `DuplicateTransactionError` e `ConcurrencyViolationError` são transitórios (`reason: 'race'`): HTTP 503 e
  retry na fila. Violações de constraint não-únicas viram `IntegrityViolationError` (bug/dado corrompido):
  HTTP 500 e DLQ imediata.
- Erros de entrada de JSON (malformado, grande demais) são tratados por um parser próprio e respondem no
  mesmo formato `{ error, message }` do resto da API.

## 15. Trade-offs e limitações conhecidas

- **Throughput por wallet é serializado** (lock pessimista). Correto para dinheiro; escala por wallets.
- **Lease da outbox (60 s):** o relay para de publicar 10 s antes do lease expirar e devolve o resto; se
  ainda assim uma publicação passar do lease, outra instância pode republicar o evento — continua
  at-least-once com o mesmo `eventId`, e a gravação só acontece se o evento ainda não foi publicado.
- **Head-of-line entre grupos:** o consumer só busca o próximo lote quando o atual termina; um grupo lento
  atrasa os demais da mesma instância (outras instâncias seguem consumindo).
- **Crescimento de `outbox_messages` e `inbox_messages`:** não há job de retenção (já
  `consumer_message_attempts` é limpa no ack/DLQ, então um redrive manual recomeça a contagem). Em produção:
  particionar por data ou expurgar publicados/processados após N dias (a inbox precisa reter pelo
  menos a janela de redelivery possível).
- **`FAILED` não publica evento** (INT-12); fica auditável no banco e no log de erro.
- **Correlation id do worker de referências** é derivado do id da transação; o original não é persistido.
- **Replay de transação que evoluiu** (`PENDING_REFERENCE` → `PROCESSED`) devolve o estado atual (INT-07).
- **Reversão parcial** está fora de escopo (enunciado). **Multi-moeda:** modelo e schema suportam; os
  testes usam BRL e conflitos de moeda.
- **LocalStack** é fixado na 4.4 (community). FIFO/redrive no LocalStack se comportam como na AWS para
  o que usamos, mas `ApproximateNumberOf*` é aproximado — os testes esperam por condição.
- **Erros desconhecidos são tratados como transitórios** no consumer: preferimos re-tentar e, no limite,
  mandar para a DLQ a descartar algo que poderia ter sucesso.
- **Autenticação** não implementada (§12).
