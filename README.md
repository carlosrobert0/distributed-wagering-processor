# Distributed Wagering Processor

Serviço financeiro distribuído que processa transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) vindas de provedores de jogos por **HTTP** e por **SQS**, e permanece correto quando as mensagens chegam **duplicadas**, **fora de ordem** ou **ao mesmo tempo**, em **várias instâncias**.

> Resolução do desafio técnico da Jungle Gaming (vaga Backend Sênior) — enunciado em [`README-desafio.md`](README-desafio.md).

**Em números:** 384 testes passando contra Postgres e LocalStack reais · 98 % de cobertura · TypeScript `strict` · ~900 req/s em 3 instâncias numa máquina só, com 0 inconsistências.

---

## Sumário

1. [O que é](#1-o-que-é)
2. [Guia rápido para quem avalia](#2-guia-rápido-para-quem-avalia)
3. [Como rodar](#3-como-rodar)
4. [Arquitetura](#4-arquitetura)
5. [API HTTP](#5-api-http)
6. [Mensageria: SQS, inbox e outbox](#6-mensageria-sqs-inbox-e-outbox)
7. [Garantias e como são provadas](#7-garantias-e-como-são-provadas)
8. [Testes](#8-testes)
9. [Configuração](#9-configuração)
10. [Operação](#10-operação)
11. [Estrutura do repositório](#11-estrutura-do-repositório)
12. [Limitações e trade-offs](#12-limitações-e-trade-offs)
13. [Documentação relacionada](#13-documentação-relacionada)

---

## 1. O que é

Um provedor de jogos envia operações de uma rodada: `BET → WIN | LOSS | REFUND | ROLLBACK`. A entrega é *at-least-once*, então o serviço assume que a mesma operação pode chegar várias vezes, que uma reversão pode chegar antes da aposta que ela referencia, que várias instâncias tocam a mesma wallet ao mesmo tempo e que o processo pode morrer antes ou depois do commit.

O que o sistema garante, mesmo assim:

- nenhum débito ou crédito duplicado;
- saldo nunca negativo, nem por corrida;
- `saldo da wallet == soma do ledger`, sempre, verificado pelo próprio banco;
- nenhum evento confirmado se perde, e nenhum evento é publicado antes do commit;
- funciona com N instâncias — não existe estado de coordenação em memória.

| Camada | Tecnologia |
|---|---|
| Runtime / testes | **Bun 1.4**, `bun test` |
| Linguagem | **TypeScript** em modo estrito (`noUncheckedIndexedAccess`, `noImplicitOverride`) |
| Framework | **NestJS 11** (só na borda; domínio e aplicação são classes puras) |
| Banco | **PostgreSQL 17** |
| ORM | **MikroORM 7** (`EntitySchema`, `EntityManager.transactional()`, `LockMode`) |
| Mensageria | **AWS SQS FIFO** via **LocalStack** |
| Orquestração | **Docker Compose** (Postgres, LocalStack, migrate e 3 instâncias da app) |
| Observabilidade | pino (JSON), prom-client (Prometheus) |

---

## 2. Guia rápido para quem avalia

### Rodar e ver funcionando (3 comandos)

```bash
bun install
docker compose up -d --build     # Postgres, LocalStack, migrations e 3 instâncias (:3001 :3002 :3003)
bun run demo                     # percorre os cenários principais com a saída comentada
```

### Onde cada critério de avaliação (§14) está

| Área | Pts | Onde olhar no código | Onde está provado |
|---|---|---|---|
| **Correção financeira** | 20 | [`Money`](src/domain/shared/money.ts) (`bigint` em centavos), [`Wallet`](src/domain/wallet/wallet.ts), [`WalletLedgerEntry`](src/domain/wallet/wallet-ledger-entry.ts), [`settleWagerTransaction`](src/domain/wagering/wager-settlement.ts), [reconciliação](src/application/wallets/reconcile-wallet.use-case.ts) | [`money.test.ts`](test/unit/domain/money.test.ts), [`wager-settlement.test.ts`](test/unit/domain/wager-settlement.test.ts), [`schema-constraints.test.ts`](test/integration/schema-constraints.test.ts), [`out-of-order.test.ts`](test/integration/out-of-order.test.ts) |
| **Concorrência** | 20 | `SELECT … FOR UPDATE` por wallet em [`mikro-orm-unit-of-work.ts`](src/infrastructure/persistence/mikro-orm-unit-of-work.ts) e [`wallet.repository.ts`](src/infrastructure/persistence/repositories/wallet.repository.ts) | [`double-spend.test.ts`](test/concurrency/double-spend.test.ts) (100−80−80, 25 rodadas), [`same-bet-50x.test.ts`](test/concurrency/same-bet-50x.test.ts), [`multi-instance.test.ts`](test/concurrency/multi-instance.test.ts) (3 processos) |
| **Idempotência** | 15 | [`process-wager-transaction.use-case.ts`](src/application/wagering/process-wager-transaction.use-case.ts), [`payload-hash.ts`](src/application/wagering/payload-hash.ts), `UNIQUE` na [migration](src/infrastructure/persistence/migrations/Migration20261021000000_initial_schema.ts) | [`process-wager-transaction.test.ts`](test/unit/application/process-wager-transaction.test.ts), [`http-api.test.ts`](test/integration/http-api.test.ts) |
| **Mensageria e falhas** | 15 | [`wager-transactions.consumer.ts`](src/interfaces/sqs/wager-transactions.consumer.ts), [`wager-message.handler.ts`](src/interfaces/sqs/wager-message.handler.ts), [`publish-outbox.use-case.ts`](src/application/messaging/publish-outbox.use-case.ts), [`retry-pending-references.use-case.ts`](src/application/wagering/retry-pending-references.use-case.ts) | [`sqs-consumer.test.ts`](test/integration/sqs-consumer.test.ts), [`outbox-relay.test.ts`](test/integration/outbox-relay.test.ts), [`crash-before-ack.test.ts`](test/concurrency/crash-before-ack.test.ts), [`restart-recovery.test.ts`](test/concurrency/restart-recovery.test.ts) |
| **Modelagem e arquitetura** | 10 | [`src/domain`](src/domain) sem nenhum import de Nest/ORM/AWS; construtores privados + `create`/`rehydrate`; portas em [`src/application/ports`](src/application/ports) | — |
| **Testes** | 10 | [`test/`](test) — 384 testes, banco e fila reais, paralelismo real | veja a [seção 8](#8-testes) |
| **Observabilidade** | 5 | [`pino-logger.ts`](src/infrastructure/observability/pino-logger.ts), [`prometheus-metrics.ts`](src/infrastructure/observability/prometheus-metrics.ts), [`health.controller.ts`](src/interfaces/http/health.controller.ts) | [`pino-logger.test.ts`](test/unit/infrastructure/pino-logger.test.ts), [`readiness.test.ts`](test/integration/readiness.test.ts) |
| **Documentação** | 5 | este README, [`ARCHITECTURE.md`](ARCHITECTURE.md), [`SPEC.md`](SPEC.md) | — |

### Falhas eliminatórias: como cada uma é impedida

| Eliminatória | Como é impedida |
|---|---|
| `number` para dinheiro | `Money` usa `bigint` de centavos; contratos trafegam strings (`"25.00"`); colunas são `NUMERIC(20,2)` e o driver devolve texto. Não há `parseFloat`/`toFixed`/`Number()` sobre valores monetários no código. |
| Saldo negativo por race | Lock de linha na wallet (`FOR UPDATE`) + `CHECK (balance >= 0)` + `balance_before/after >= 0` no ledger. |
| Débito ou crédito duplicado | `UNIQUE(idempotency_key)`, `UNIQUE(provider_id, external_transaction_id)`, `UNIQUE(transaction_id, wallet_id)` e `UNIQUE(wallet_id, wallet_version)` no ledger, índice único parcial de reversão, inbox por `(consumer, messageId)`. |
| Idempotência só em memória | Não há cache em memória em nenhum lugar; toda decisão consulta o banco. |
| Só funciona com 1 instância | Toda coordenação é feita no Postgres (`FOR UPDATE`, `SKIP LOCKED`, leases). Testado com 3 processos separados. |
| Evento publicado antes do commit | *Transactional Outbox*: o evento é gravado na mesma transação SQL e publicado por um relay depois. |
| Ledger que não dá para auditar | Append-only por trigger (`UPDATE`/`DELETE`/`TRUNCATE` bloqueados), encadeamento `balance_before(n) = balance_after(n−1)` verificado no banco, reconciliação. |
| Postgres ou SQS mockados | Testes de integração e concorrência rodam contra Postgres e LocalStack reais em containers. |

### Interpretações que vale ler primeiro

O enunciado deixa espaço para interpretação em alguns pontos; todas as escolhas estão em [`SPEC.md`](SPEC.md) §13 (`INT-01` a `INT-15`). As que mais mudam o comportamento:

- **Uma referência é revertida no máximo uma vez, por qualquer tipo de reversão** (`INT-05`). Lida ao pé da letra, a regra 4 permitiria `REFUND` **e** `ROLLBACK` sobre a mesma BET — creditando em dobro.
- **`WIN`/`LOSS` sobre uma BET já estornada é rejeitada** com `REFERENCE_REVERSED` (`INT-13`), pelo mesmo motivo (duplo pagamento).
- **Rejeição de negócio é `422`, aceite pendente é `202`** (`ARCHITECTURE.md` §10), para o provedor distinguir "aplicado" de "recusado" olhando só o status.
- **`PLAYER_WALLET_MISMATCH` não devolve o saldo** (`INT-14`): quem chamou não é dono da wallet.
- **Autenticação não foi implementada** (vale 0 pontos); o ponto de extensão e o desenho com Keycloak estão documentados na [seção 12](#12-limitações-e-trade-offs).

---

## 3. Como rodar

### Pré-requisitos

- **Bun ≥ 1.4** (o `bun.lock` e a imagem Docker usam 1.4.4) — `brew install oven-sh/bun/bun`
- **Docker** com Compose (Docker Desktop, OrbStack ou Colima)

### Ambiente completo

```bash
bun install
docker compose up -d --build
```

Sobe, nesta ordem: Postgres → LocalStack (o script [`init-sqs.sh`](docker/localstack/init-sqs.sh) cria as filas) → `migrate` (aplica as migrations e sai) → `app-1`, `app-2`, `app-3`.

| Serviço | Endereço |
|---|---|
| API (3 instâncias idênticas) | `http://localhost:3001`, `:3002`, `:3003` |
| Postgres | `localhost:5432` (usuário, senha e banco: `wagering`) |
| SQS (LocalStack) | `http://localhost:4566` |
| Métricas Prometheus | `http://localhost:3001/metrics` |

```bash
curl -s localhost:3001/health/ready
# {"status":"ok","checks":{"postgres":"up","sqs":"up"}}

bun run demo                     # cenários guiados (veja abaixo)
docker compose logs -f app-1 app-2 app-3
docker compose down              # para tudo  (down -v também apaga os dados)
```

### O que o `bun run demo` mostra

Roda [`scripts/demo.sh`](scripts/demo.sh) contra as três instâncias e imprime, com explicação, cada resposta:

| # | Cenário | Resultado esperado |
|---|---|---|
| 1–2 | Criar wallet; criar de novo para o mesmo jogador | `201` com ledger de abertura; `409 WALLET_ALREADY_EXISTS` |
| 3–4 | BET na instância 2; **a mesma BET** na instância 3 | `201`; `200` com `idempotentReplay: true` e o mesmo `transactionId` |
| 5 | Mesma `Idempotency-Key` com valor diferente | `409 IDEMPOTENCY_KEY_CONFLICT` (nunca replay) |
| 6 | 3 casas decimais, `kind: OPENING`, sem `Idempotency-Key` | `400` |
| 7 | Aposta acima do saldo | `422 INSUFFICIENT_FUNDS`, sem lançamento no ledger |
| 8–9 | REFUND; REFUND/ROLLBACK repetido; valor diferente | `201`; `ALREADY_REVERSED`; `AMOUNT_MISMATCH` |
| 10 | LOSS | saldo e versão da wallet não mudam |
| 11 | REFUND **antes** da BET que ele referencia | `202 PENDING_REFERENCE`; depois o worker aplica sozinho |
| 12 | **100,00 de saldo, duas BETs de 80,00 simultâneas**, em instâncias diferentes | uma `PROCESSED`, outra `REJECTED`, saldo `20.00`, um único débito |
| 13 | Mensagem pela fila SQS | mesmo caso de uso do HTTP; saldo atualizado |
| 14 | Ledger paginado e reconciliação | `consistent: true` |

### Rodando a aplicação fora do Docker (desenvolvimento)

```bash
docker compose up -d postgres localstack
bun run migration:up
AWS_ENDPOINT_URL=http://localhost:4566 bun run start      # http://localhost:3000
# ou com recarga automática:
AWS_ENDPOINT_URL=http://localhost:4566 bun run dev
```

### Scripts

| Comando | O que faz |
|---|---|
| `bun run start` / `dev` | sobe a aplicação (`dev` recarrega ao salvar) |
| `bun run typecheck` | `tsc --noEmit` em modo estrito |
| `bun run format` / `format:check` | Prettier |
| `bun run migration:up` / `migration:down` | aplica / reverte todas as migrations |
| `bun test` | todos os testes (exige `docker compose up -d postgres localstack`) |
| `bun run test:unit` / `test:integration` / `test:concurrency` | uma camada de cada vez |
| `bun run test:load` | teste de carga (exige o compose completo) |
| `bun run demo` | cenários guiados |

---

## 4. Arquitetura

### Visão geral

```mermaid
flowchart LR
  P["Provedor de jogos"] -->|"HTTP POST /wagering/transactions"| API
  P -->|"WagerTransactionRequested"| Q[("SQS wager-transactions.fifo")]
  Q --> C["Consumer SQS"]

  subgraph App["Instância da aplicação (N cópias idênticas, sem estado)"]
    API["Controllers HTTP"] --> UC["ProcessWagerTransactionUseCase"]
    C --> H["WagerMessageHandler"] --> UC
    UC --> DOM["Domínio puro: Wallet, WagerTransaction, settlement"]
    R["Relay da outbox"]
    W["Worker de PENDING_REFERENCE"]
  end

  UC -->|"1 transação SQL"| PG[("PostgreSQL")]
  R --> PG
  W --> PG
  R -->|"eventos de integração"| E[("SQS wagering-events.fifo")]
  Q -. "falhas permanentes" .-> D[("DLQ")]
```

O banco é o árbitro de toda invariante. A aplicação decide as regras de negócio, mas unicidade, não-negatividade, imutabilidade e *saldo == ledger* são garantidas por constraints e triggers — assim N instâncias, um bug ou um `UPDATE` manual não corrompem o estado.

### Fluxo de uma aposta

```mermaid
sequenceDiagram
  autonumber
  participant P as Provedor
  participant A as Instância (HTTP ou SQS)
  participant DB as PostgreSQL
  participant R as Relay (qualquer instância)
  participant E as SQS de eventos

  P->>A: POST /wagering/transactions (Idempotency-Key)
  A->>DB: já existe? (transação curta, sem lock)
  A->>DB: BEGIN
  A->>DB: inbox: INSERT ... ON CONFLICT DO NOTHING (só no SQS)
  A->>DB: SELECT ... FROM wallets FOR UPDATE
  A->>DB: idempotência de novo, agora sob o lock
  Note over A: domínio decide: PROCESSED, REJECTED ou PENDING_REFERENCE
  A->>DB: INSERT transação, INSERT ledger, UPDATE wallet, INSERT outbox
  A->>DB: COMMIT (triggers diferidas conferem saldo == ledger)
  A-->>P: 201 / 422 / 202 (ou DeleteMessage no SQS)
  R->>DB: reivindica eventos (FOR UPDATE SKIP LOCKED + lease)
  R->>E: publica, fora de qualquer transação
  R->>DB: marca published_at
```

O `ack` do SQS e a resposta HTTP só acontecem **depois do commit**; o evento só é publicado **depois do commit**.

### Camadas (Clean Architecture)

| Camada | Conteúdo | Pode importar |
|---|---|---|
| [`src/domain`](src/domain) | `Money`, `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, serviço de liquidação, `InboxMessage`, `OutboxMessage`, `IntegrationEvent` e os 4 eventos | nada de fora |
| [`src/application`](src/application) | casos de uso e **portas** (`UnitOfWork`, repositórios, `Clock`, `IdGenerator`, `Metrics`, `AppLogger`, `EventPublisher`) | `domain` |
| [`src/infrastructure`](src/infrastructure) | MikroORM (schemas, mappers, repositórios, migrations), SQS, pino, Prometheus, configuração | `domain`, `application` |
| [`src/interfaces`](src/interfaces) | controllers HTTP, consumer SQS, workers (relay da outbox e de referências) | `application` |
| [`src/app.module.ts`](src/app.module.ts) | *composition root*: o único lugar que conhece todas as camadas | tudo |

O domínio não depende de Nest, do ORM nem da AWS; os casos de uso são classes comuns instanciadas por *factory* no módulo — por isso são os mesmos para HTTP e SQS e podem ser testados sem framework.

### Decisões principais (resumo)

| Tema | Decisão | Por quê |
|---|---|---|
| Dinheiro | `bigint` de centavos, escala fixa 2, sem arredondamento; entrada com mais de 2 casas é **rejeitada** | exato, sem dependência, simples de explicar; só há soma e subtração |
| Concorrência | lock **pessimista por linha** (`FOR UPDATE`) em `READ COMMITTED`, com `lock_timeout` de 5 s | hot wallet sob otimista vira tempestade de retries; o lock é por wallet, então wallets diferentes rodam em paralelo (sem lock global) |
| Idempotência | `UNIQUE` no banco + `payloadHash = sha256(JSON canônico dos campos de negócio)`; replay devolve o saldo **daquele momento** | a mesma key com payload diferente é conflito, não replay |
| Schema | garantias no banco: `CHECK`, `UNIQUE`, índice único parcial, triggers append-only, *constraint triggers* diferidas | restrição inviolável 9 do enunciado |
| ORM | `EntitySchema` (sem decorators) + mappers via `rehydrate`; escritas explícitas | domínio puro; ordem de comandos previsível e auditável |
| Outbox | evento na mesma transação; relay com **lease** configurável + `SKIP LOCKED`; publica fora de transação | nenhum lock de banco preso durante chamada de rede; recuperação rápida após crash |
| Consumer | inbox persistente; `ack` só após o commit; limite de tentativas **reais** na aplicação; heartbeat de visibilidade | erro de negócio → ack, transitório → retry com backoff, permanente → DLQ |
| Referência ausente | `PENDING_REFERENCE` + worker com backoff exponencial (8 tentativas, 2 s·2ⁿ, teto 5 min ≈ 8,5 min) | o provedor costuma reenviar a referência em segundos; esgotou → `REJECTED REFERENCE_NOT_FOUND` + evento |

A justificativa completa de cada uma, com os trade-offs, está em [`ARCHITECTURE.md`](ARCHITECTURE.md).

---

## 5. API HTTP

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/wallets` | cria wallet; saldo inicial > 0 gera a transação interna `OPENING` + lançamento `CREDIT` na mesma transação SQL |
| `GET` | `/wallets/:walletId` | saldo e versão |
| `GET` | `/wallets/:walletId/ledger?cursor=&limit=50` | lançamentos, ordenados por versão, com cursor opaco e estável |
| `POST` | `/wallets/:walletId/reconciliation` | compara o saldo materializado com o saldo reconstruído pelo ledger |
| `POST` | `/wagering/transactions` | submete uma transação (`Idempotency-Key` obrigatório) |
| `GET` | `/wagering/transactions/:transactionId` | consulta por id interno |
| `GET` | `/providers/:providerId/wagering/transactions/:externalTransactionId` | consulta pelo id do provedor |
| `GET` | `/health/live`, `/health/ready` | liveness / readiness (Postgres + SQS); sem autenticação |
| `GET` | `/metrics` | métricas Prometheus |

### Submeter uma transação

```http
POST /wagering/transactions
Idempotency-Key: provider-a:transaction-123
Content-Type: application/json

{
  "providerId": "provider-a",
  "externalTransactionId": "transaction-123",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "roundId": "round-987",
  "gameId": "fortune-chimp",
  "kind": "BET",
  "money": { "amount": "25.00", "currency": "BRL" }
}
```

```json
{
  "transactionId": "0192f298-345e-7e38-af88-e43f851a819d",
  "status": "PROCESSED",
  "balance": { "amount": "975.00", "currency": "BRL" },
  "idempotentReplay": false
}
```

`REFUND` e `ROLLBACK` exigem `referenceExternalTransactionId` (o id **do provedor** da transação referenciada). `WIN` pode, opcionalmente, referenciar a BET.

**Idempotência.** O header `Idempotency-Key` é a fonte da verdade (padrão sugerido: `{providerId}:{externalTransactionId}`). O `payloadHash` é o SHA-256 do JSON canônico (chaves ordenadas, recursivamente) de: `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}, referenceExternalTransactionId`. O header e metadados de transporte não entram no hash.

### Mapa de status HTTP (consistente em todos os endpoints)

| Situação | Status | O provedor deve |
|---|---|---|
| Processado agora | `201` | — |
| Replay de transação já processada | `200` com `idempotentReplay: true` | — |
| Aceito, aguardando a referência | `202` | aguardar / consultar depois |
| Rejeição de negócio (transação gravada como `REJECTED`) | `422` + `failureCode` | decidir pelo `failureCode` |
| Payload inválido, sem `Idempotency-Key`, JSON malformado | `400` | corrigir; não reenviar igual |
| Recurso inexistente | `404` | corrigir |
| Conflito de idempotência, `externalTransactionId` com outra key, wallet duplicada | `409` | não é replay; investigar |
| Falha transitória (banco/SQS fora, timeout de lock) | `503` + `Retry-After` | reenviar com a mesma `Idempotency-Key` |

Erros seguem sempre o formato `{ "error": "CODIGO_ESTAVEL", "message": "...", "details": [...] }`.

### Códigos de falha (`failureCode`)

Estáveis e legíveis por máquina; o provedor decide reenviar, corrigir ou desistir só olhando o código.

| `failureCode` | Quando | Provedor deve |
|---|---|---|
| `INSUFFICIENT_FUNDS` | BET sem saldo suficiente | desistir |
| `REVERSAL_WOULD_OVERDRAW` | ROLLBACK de WIN/REFUND que deixaria o saldo negativo | escalar para operação manual |
| `REFERENCE_NOT_FOUND` | a referência não chegou dentro do limite | reenviar com nova chave depois de enviar a referência |
| `REFERENCE_NOT_PROCESSED` | a referência existe mas foi rejeitada | desistir |
| `INVALID_REFERENCE_KIND` | REFUND sobre não-BET; ROLLBACK sobre LOSS; etc. | corrigir o payload |
| `REFERENCE_MISMATCH` | referência de outro jogador, wallet, rodada ou moeda | corrigir o payload |
| `AMOUNT_MISMATCH` | valor da reversão ≠ valor da referência | corrigir o payload |
| `ALREADY_REVERSED` | a referência já foi revertida | desistir (já foi feito) |
| `REFERENCE_REVERSED` | WIN/LOSS referenciando uma BET já revertida | desistir |
| `CURRENCY_MISMATCH` | moeda da operação ≠ moeda da wallet | corrigir o payload |
| `PLAYER_WALLET_MISMATCH` | `playerId` não é o dono da wallet | corrigir o payload |
| `BALANCE_LIMIT_EXCEEDED` | crédito que estouraria o limite de `NUMERIC(20,2)` | escalar para operação |
| `PROCESSING_ERROR` | erro permanente de infraestrutura no reprocessamento (`FAILED`) | contatar a operação |

Erros que **não** viram transação persistida (respondidos direto): `INVALID_PAYLOAD`, `IDEMPOTENCY_KEY_REQUIRED`, `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`, `WALLET_NOT_FOUND`, `TRANSIENT_FAILURE`.

### Reconciliação

```json
{
  "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "storedBalance":     { "amount": "975.00", "currency": "BRL" },
  "calculatedBalance": { "amount": "975.00", "currency": "BRL" },
  "difference":        { "amount": "0.00",   "currency": "BRL" },
  "consistent": true,
  "checkedEntries": 42,
  "issues": []
}
```

Divergências **nunca** são corrigidas em silêncio: são logadas, contadas em `wagering_reconciliations_total{result="mismatch"}` e sinalizadas (`consistent: false`, com `issues`).

---

## 6. Mensageria: SQS, inbox e outbox

### Filas

| Fila | Função |
|---|---|
| `wager-transactions.fifo` | comandos de entrada (`MessageGroupId = walletId`) |
| `wager-transactions-dlq.fifo` | mensagens permanentes ou que esgotaram as tentativas |
| `wagering-events.fifo` | eventos de integração publicados pela outbox |

### Mensagem de entrada

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-07-29T15:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

### Como o consumer trata cada caso

| Situação | Ação |
|---|---|
| Processada (inclusive rejeição de negócio persistida) | `DeleteMessage` **depois do commit** |
| Mensagem já recebida (mesmo `messageId`) | devolve o resultado original e dá ack; **nenhum efeito novo** |
| Mesmo `messageId` com conteúdo diferente | DLQ (`INBOX_MESSAGE_CONFLICT`) |
| Wallet inexistente, conflito de idempotência | erro de negócio terminal → ack |
| JSON malformado, envelope inválido, payload inválido, violação de integridade | erro permanente → **DLQ** com o motivo |
| Banco/SQS fora, timeout de lock, deadlock, erro desconhecido | transitório → visibilidade com **backoff exponencial** (2 s → 120 s) |
| 5ª tentativa **real** falhando | DLQ com `MAX_ATTEMPTS_EXCEEDED` (a redrive policy de 20 recebimentos é só rede de segurança) |
| `SIGTERM` | para de buscar, termina o que está em andamento, devolve o resto com visibilidade 0 |

Mensagens de wallets diferentes são processadas em paralelo; dentro da mesma wallet, em ordem. O FIFO é só otimização — a correção vem do banco.

### Eventos de integração

| Evento | Quando |
|---|---|
| `WagerTransactionProcessed` | qualquer transação aplicada, inclusive `LOSS` e `OPENING` |
| `WagerTransactionRejected` | rejeição por regra de negócio |
| `WalletBalanceChanged` | **somente** quando o saldo muda |
| `WagerTransactionPendingReference` | referência ausente |

```json
{
  "eventId": "0192f2a0-1b3c-7a10-9d3e-0b5c8d1a2f44",
  "eventType": "WalletBalanceChanged",
  "aggregateId": "0192f291-27dd-7d3f-8071-5f8685deef37",
  "correlationId": "7c1e2b3a-...",
  "causationId": "msg-123",
  "occurredAt": "2026-10-23T11:28:02.314Z",
  "version": 1,
  "data": {
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "transactionId": "0192f298-345e-7e38-af88-e43f851a819d",
    "direction": "DEBIT",
    "money": { "amount": "25.00", "currency": "BRL" },
    "balanceBefore": { "amount": "1000.00", "currency": "BRL" },
    "balanceAfter": { "amount": "975.00", "currency": "BRL" },
    "walletVersion": 2
  }
}
```

O `data` carrega só `MoneyProps` (strings), nunca instâncias de `Money` — o payload é JSON estável e versionável. A entrega é *at-least-once*: o `eventId` é estável entre republicações (mesmo depois da janela de 5 minutos de deduplicação do SQS), então **o consumidor deduplica por `eventId`**.

### Cenário de crash (§11 do enunciado)

1. o Postgres confirma o commit; 2. o processo morre antes de publicar; 3. o lease do evento expira e **outra instância** o reivindica; 4. o evento é publicado; 5. se a primeira instância tinha chegado a publicar, a republicação é segura para o consumidor. Coberto por [`outbox-relay.test.ts`](test/integration/outbox-relay.test.ts) e [`restart-recovery.test.ts`](test/concurrency/restart-recovery.test.ts).

---

## 7. Garantias e como são provadas

| Garantia | Mecanismo no banco / código | Prova |
|---|---|---|
| Saldo nunca negativo | `wallets_balance_non_negative_ck`, `ledger_balances_non_negative_ck`, `Wallet.debit` | [`schema-constraints.test.ts`](test/integration/schema-constraints.test.ts), [`double-spend.test.ts`](test/concurrency/double-spend.test.ts) |
| Sem *lost update* | `FOR UPDATE` por wallet; `UNIQUE (wallet_id, wallet_version)` no ledger; `UPDATE … WHERE version = ?` | [`double-spend.test.ts`](test/concurrency/double-spend.test.ts) (hot wallet: 40 apostas de 5,00 sobre 100,00 → exatamente 20 aceitas) |
| Uma wallet por jogador e moeda | `wallets_player_currency_uq` | [`http-api.test.ts`](test/integration/http-api.test.ts) |
| Idempotência persistente | `UNIQUE(idempotency_key)`, `UNIQUE(provider_id, external_transaction_id)` | [`same-bet-50x.test.ts`](test/concurrency/same-bet-50x.test.ts) (50 envios paralelos → 1 débito, 49 replays) |
| Uma reversão por referência | índice único parcial `wager_tx_single_reversal_uq` | [`out-of-order.test.ts`](test/integration/out-of-order.test.ts), [`out-of-order-race.test.ts`](test/concurrency/out-of-order-race.test.ts) |
| Ledger imutável | triggers `ledger_append_only` e `ledger_no_truncate` | [`schema-constraints.test.ts`](test/integration/schema-constraints.test.ts) |
| Ledger encadeado | trigger `ledger_validate_entry` (versão n = n−1+1; `before(n) = after(n−1)`) | [`schema-constraints.test.ts`](test/integration/schema-constraints.test.ts) |
| **Saldo == ledger** | *constraint triggers* diferidas, checadas no `COMMIT` | em todo teste que grava wallet (`expectWalletMatchesLedger`); reconciliação |
| Transação terminal imutável | trigger `wager_tx_guard` | [`schema-constraints.test.ts`](test/integration/schema-constraints.test.ts) |
| Atomicidade (transação + saldo + ledger + inbox + outbox) | uma única transação SQL | [`atomicity.test.ts`](test/integration/atomicity.test.ts) (falha injetada depois de todas as escritas → nada persiste) |
| Evento só depois do commit | *Transactional Outbox* | [`outbox-relay.test.ts`](test/integration/outbox-relay.test.ts) |
| Crash entre commit e ack | inbox deduplica a redelivery | [`crash-before-ack.test.ts`](test/concurrency/crash-before-ack.test.ts) (processo real morto por `SIGKILL`) |
| Reinício converge | leases expiram; workers retomam | [`restart-recovery.test.ts`](test/concurrency/restart-recovery.test.ts) |
| Funciona com N instâncias | só estado no Postgres | [`multi-instance.test.ts`](test/concurrency/multi-instance.test.ts) (3 processos) |

---

## 8. Testes

Os testes de integração e concorrência usam **Postgres e LocalStack reais**. Nada de mock de banco ou de fila onde se afirma uma garantia.

```bash
docker compose up -d postgres localstack     # dependências
bun install

bun run typecheck           # tsc --noEmit (strict)
bun run test:unit           # 255 testes, ~0,6 s, sem I/O
bun run test:integration    # constraints, migrations, atomicidade, API, outbox, consumer, health
bun run test:concurrency    # 100−80−80, 50× mesma aposta, 3 processos, crash antes do ack, reinício
bun test                    # tudo: 384 testes, ~50 s
bun test --coverage         # 98 % das linhas e das funções
```

| Camada | O que cobre |
|---|---|
| **Unidade — domínio** | `Money` (escala, entradas inválidas, conflito de moeda), `Wallet`, ledger, máquina de estados, regras de BET/WIN/LOSS/REFUND/ROLLBACK, backoff, hash canônico |
| **Unidade — aplicação** | orquestração dos casos de uso sobre um `UnitOfWork` em memória com *commit/rollback* de verdade: replay, conflitos, inbox, corrida de unicidade, `PENDING_REFERENCE`, outbox, lease expirado |
| **Unidade — bordas** | erro → status HTTP, classificação de erros do SQS, SQLSTATE → transitório, decisões do handler, **consumer SQS com cliente roteirizado** (poll que falha, ordem do grupo, tentativas reais, DLQ, ack que falha, heartbeat, shutdown com grace estourado), guard, config, logs |
| **Integração** | migrations *up → down → up*, cada constraint e trigger, atomicidade, API completa, outbox (2 publishers, lease, retry, duplicata), consumer (inbox, redelivery, DLQ, shutdown), referências fora de ordem, health checks com o SQS inalcançável |
| **Concorrência** | paralelismo real: requisições HTTP simultâneas e **processos separados** (`Bun.spawn`) |

### O que a suíte de concorrência prova (§13 do enunciado)

| # | Cenário exigido | Arquivo |
|---|---|---|
| 1 | a mesma aposta 50× em paralelo → um único débito | [`same-bet-50x.test.ts`](test/concurrency/same-bet-50x.test.ts) |
| 2 | saldo 100,00, duas apostas de 80,00 → 1 `PROCESSED`, 1 `INSUFFICIENT_FUNDS`, saldo 20,00 (**25 rodadas**) | [`double-spend.test.ts`](test/concurrency/double-spend.test.ts) |
| 3 | wallets distintas em paralelo; uma wallet travada não bloqueia as outras | [`parallel-wallets.test.ts`](test/concurrency/parallel-wallets.test.ts) |
| 4 | ≥ 3 processos simultâneos | [`multi-instance.test.ts`](test/concurrency/multi-instance.test.ts) |
| 5 | worker morto depois do commit e antes do ack | [`crash-before-ack.test.ts`](test/concurrency/crash-before-ack.test.ts) |
| 6 | dois publishers sobre a mesma outbox | [`outbox-relay.test.ts`](test/integration/outbox-relay.test.ts) |
| 7 | `ROLLBACK`/`REFUND` entregue antes (e ao mesmo tempo que) a referência | [`out-of-order.test.ts`](test/integration/out-of-order.test.ts), [`out-of-order-race.test.ts`](test/concurrency/out-of-order-race.test.ts) |
| 8 | reinício do serviço com consistência final | [`restart-recovery.test.ts`](test/concurrency/restart-recovery.test.ts) |

A **invariante final** (`wallet.balance == saldo reconstruído pelo ledger`, recalculado em `NUMERIC` pelo banco, com checagem de encadeamento e versão) é conferida ao final de todo teste que grava wallets.

**Isolamento e determinismo.** Cada arquivo de teste recebe um banco próprio (clone de um template já migrado) e filas SQS próprias, então workers de um teste nunca tocam dados de outro. As esperas são por condição (`waitFor`), nunca `sleep` cego para asserção. Cada execução remove os próprios bancos ao terminar.

### Teste de carga

```bash
docker compose up -d --build
bun run test:load      # variáveis: LOAD_REQUESTS, LOAD_CONCURRENCY, LOAD_WALLETS, LOAD_DUPLICATE_RATIO
```

Resultado de uma execução (Apple M4 Pro, 14 núcleos, OrbStack; gerador de carga, Postgres, LocalStack e as 3 instâncias na **mesma máquina**): 5.506 requisições (5.000 + 506 duplicatas), **~903 req/s**, p50 **25 ms**, p95 **330 ms**, p99 **615 ms**, **0 % de erro**, 0 conflitos de lock e **0 de 50 wallets inconsistentes** na reconciliação final. A cauda vem das duas wallets quentes (50 % do tráfego), onde as apostas são serializadas pelo lock — o custo declarado da estratégia pessimista. Metodologia e ressalvas em [`ARCHITECTURE.md`](ARCHITECTURE.md) §14: não é um teste de capacidade.

---

## 9. Configuração

Toda a configuração vem de variáveis de ambiente, validada no boot por `zod` ([`app-config.ts`](src/infrastructure/config/app-config.ts)); um valor inválido derruba a aplicação com mensagem clara.

| Variável | Padrão | Descrição |
|---|---|---|
| `PORT` | `3000` | porta HTTP |
| `INSTANCE_ID` | `instance-<pid>` | identifica a instância nos logs e métricas |
| `LOG_LEVEL` | `info` | `fatal`…`trace`, `silent` |
| `DATABASE_URL` | `postgres://wagering:wagering@localhost:5432/wagering` | Postgres |
| `DB_POOL_MAX` | `20` | conexões por instância |
| `DB_LOCK_TIMEOUT_MS` | `5000` | espera máxima pelo lock de uma wallet (estourou → `503`/retry) |
| `DB_STATEMENT_TIMEOUT_MS` | `15000` | teto de qualquer comando dentro da transação |
| `AWS_ENDPOINT_URL` | — | endpoint do LocalStack (`http://localhost:4566`) |
| `AWS_REGION` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `us-east-1` / `test` / `test` | credenciais do SQS |
| `SQS_WAGER_QUEUE_NAME` / `SQS_WAGER_DLQ_NAME` / `SQS_EVENTS_QUEUE_NAME` | `wager-transactions.fifo` / `wager-transactions-dlq.fifo` / `wagering-events.fifo` | filas |
| `CONSUMER_ENABLED` | `true` | liga o consumer SQS |
| `CONSUMER_MAX_ATTEMPTS` | `5` | tentativas **reais** antes de a aplicação mandar a mensagem para a DLQ |
| `CONSUMER_VISIBILITY_TIMEOUT_SECONDS` | `30` | visibilidade das mensagens em processamento (estendida por heartbeat) |
| `CONSUMER_BATCH_SIZE` / `CONSUMER_WAIT_TIME_SECONDS` | `10` / `10` | tamanho do lote e *long poll* |
| `CONSUMER_RETRY_BASE_SECONDS` / `CONSUMER_RETRY_MAX_SECONDS` | `2` / `120` | backoff de retry |
| `CONSUMER_SHUTDOWN_GRACE_MS` | `20000` | tempo para terminar o que está em andamento no `SIGTERM` |
| `OUTBOX_RELAY_ENABLED` | `true` | liga o relay da outbox |
| `OUTBOX_RELAY_INTERVAL_MS` / `OUTBOX_RELAY_BATCH_SIZE` | `500` / `50` | cadência do relay |
| `OUTBOX_LEASE_MS` | `30000` | reserva dos eventos reivindicados; também o tempo de recuperação após a morte de uma instância |
| `PENDING_REFERENCE_WORKER_ENABLED` | `true` | liga o worker de referências pendentes |
| `PENDING_REFERENCE_INTERVAL_MS` / `PENDING_REFERENCE_BATCH_SIZE` | `1000` / `50` | cadência do worker |
| `PENDING_REFERENCE_LEASE_MS` | `30000` | reserva das candidatas reivindicadas |
| `PENDING_REFERENCE_BASE_DELAY_MS` / `PENDING_REFERENCE_MAX_DELAY_MS` | `2000` / `300000` | backoff exponencial das verificações |
| `PENDING_REFERENCE_MAX_ATTEMPTS` | `8` | verificações antes de `REFERENCE_NOT_FOUND` (~8,5 min no total) |
| `FAULT_INJECTION` | `none` | `crash-after-commit` mata o processo entre o commit e o ack — **somente para testes** |

---

## 10. Operação

### Logs

JSON estruturado, uma linha por evento, com `correlationId` (propagado do header `x-correlation-id` ou gerado), `messageId`, `transactionId`, `walletId` e `providerId`. **Nenhum valor monetário, saldo ou payload é logado**: a porta de log só aceita escalares, o `redact` do pino censura campos financeiros e uma camada extra mascara valores decimais em mensagens de erro.

```json
{"level":"info","time":"2026-10-23T11:28:02.364Z","service":"wagering-processor","instance":"app-2","correlationId":"90873781-…","transactionId":"01a16884-…","walletId":"01a16884-…","providerId":"provider-a","kind":"BET","status":"PROCESSED","outcome":"processed","message":"wager transaction settled"}
```

### Métricas (`GET /metrics`)

| Métrica | O que mede |
|---|---|
| `wagering_transactions_total{status,kind,source}` | transações por status final |
| `wagering_duplicates_total{type}` | replays idempotentes, conflitos de key, mensagens repetidas no inbox |
| `wagering_retries_total{component}` | retries do consumer, relay, worker e corridas de unicidade |
| `wagering_dlq_messages_total{reason}` | mensagens enviadas à DLQ (`permanent`, `max_attempts`) |
| `wagering_lock_conflicts_total{kind}` | `lock_timeout`, `deadlock`, `unique_race` |
| `wagering_wallet_lock_wait_seconds` | espera pelo lock da wallet (contenção de *hot wallet*) |
| `wagering_processing_duration_seconds{source}` | latência de processamento |
| `wagering_outbox_pending`, `wagering_outbox_lag_seconds`, `wagering_outbox_published_total` | backlog e atraso da outbox |
| `wagering_reconciliations_total{result}` | reconciliações consistentes × divergentes |

### Health checks

- `GET /health/live` — o processo responde. Não toca dependências (evita reinício em cascata).
- `GET /health/ready` — Postgres (`SELECT 1`) e SQS alcançáveis, com timeout. Responde `503` com a dependência que caiu (`"sqs":"down"`), e `503 draining` durante o desligamento para o balanceador parar de enviar tráfego.

### Migrations

Versionadas e reversíveis, registradas explicitamente em [`mikro-orm.config.ts`](src/infrastructure/persistence/mikro-orm.config.ts): `Migration20261021000000_initial_schema` (domínio, ledger, inbox, outbox) e `Migration20261022000000_consumer_message_attempts`.

```bash
bun run migration:up
bun run migration:down     # reverte todas
```

### Desligamento gracioso

`SIGTERM` → readiness passa a `draining` → o consumer para de buscar e termina o que está em andamento (mensagens não iniciadas voltam com visibilidade 0) → os workers terminam o ciclo → o pool do banco é fechado. Medido no Docker: o container para em menos de 1 s com `exit 0`.

### Problemas comuns

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `Cannot connect to the Docker daemon` | OrbStack/Docker Desktop fechado | abrir o aplicativo e repetir |
| porta `5432`, `4566` ou `3001–3003` ocupada | outro Postgres/LocalStack local | parar o outro serviço ou ajustar as portas no `docker-compose.yml` |
| testes falham no `beforeAll` | Postgres/LocalStack fora do ar | `docker compose up -d postgres localstack` |
| `/health/ready` em `503` logo após subir | LocalStack ainda criando as filas | aguardar alguns segundos (o Compose só libera as apps quando o LocalStack está saudável) |
| quer recomeçar do zero | — | `docker compose down -v && docker compose up -d --build` |

---

## 11. Estrutura do repositório

```
src/
  domain/            Money, Wallet, WalletLedgerEntry, WagerTransaction, settlement,
                     InboxMessage, OutboxMessage, IntegrationEvent e eventos concretos
  application/       casos de uso, portas (UnitOfWork, repositórios, métricas, logger, publisher),
                     erros da aplicação, LeasePolicy
  infrastructure/    MikroORM (schemas, mappers, repositórios, migrations), SQS, pino, Prometheus, config
  interfaces/        http/ (controllers, filtro de exceção, guard), sqs/ (consumer e handler),
                     workers/ (relay da outbox, referências pendentes), contracts/ (zod)
  app.module.ts      composition root
  main.ts            entrada (bootstrap.ts monta a aplicação e é reutilizado pelos testes)
test/
  unit/              domain/ application/ interfaces/ infrastructure/
  integration/       Postgres e LocalStack reais
  concurrency/       paralelismo real e processos separados
  load/              teste de carga
  support/           ambiente, banco, SQS, instâncias, fakes em memória
scripts/demo.sh      cenários guiados
docker/localstack/   criação das filas FIFO
docker-compose.yml   Postgres, LocalStack, migrate e 3 instâncias
Dockerfile           imagem da aplicação (Bun executa TypeScript direto, sem etapa de build)
```

Convenções: sem comentários no código TypeScript de `src/` e `test/` (a intenção está nos nomes; o *porquê* das decisões vive em [`ARCHITECTURE.md`](ARCHITECTURE.md) e [`SPEC.md`](SPEC.md)), Prettier como formatador único e TypeScript estrito.

---

## 12. Limitações e trade-offs

- **O throughput de uma única wallet é serializado** pelo lock pessimista. É o comportamento correto para dinheiro; wallets diferentes escalam em paralelo e horizontalmente.
- **Autenticação não foi implementada** (não vale pontos e não deve competir com correção financeira). O ponto de extensão é explícito: o `ProviderAuthGuard` global delega à porta `ProviderAuthenticator` (hoje `AllowAllProviderAuthenticator`); rotas `@Public()` (health, métricas) nem consultam a porta. **Desenho previsto:** Keycloak (OIDC), um *client* confidencial por provedor com *client credentials*, validação do JWT por JWKS, e a claim `provider_id` do token comparada com o `providerId` do payload (divergência → `403`). Mensagens da fila são canal interno confiável, mas a identidade do provedor continua sujeita às validações de domínio.
- **Sem job de retenção** para `outbox_messages` e `inbox_messages`. Em produção: particionar por data ou expurgar publicados/processados após N dias (a inbox precisa reter pelo menos a janela de redelivery).
- **A ordem de eventos por wallet é garantida dentro de um lote do relay**; entre relays concorrentes é melhor esforço. `WalletBalanceChanged` traz `walletVersion` para o consumidor ordenar ou descartar.
- **O consumer dos eventos não faz parte deste serviço**: ele só os publica; a deduplicação a jusante por `eventId` é do consumidor.
- **`/health/ready` foi testado automaticamente com o SQS fora do ar**; com o Postgres fora não, porque a aplicação não sobe sem ele.
- **Reversão parcial** está fora do escopo (o enunciado a exclui). O modelo e o schema são multi-moeda; os testes usam BRL e cobrem conflitos de moeda.
- **O teste de carga** é uma execução única, com tudo na mesma máquina: mostra que nada quebra sob contenção e onde está o gargalo, não a capacidade do sistema.
- **LocalStack** está fixado na 4.4 (community), por ser a última linha que não exige token.

---

## 13. Documentação relacionada

| Documento | Conteúdo |
|---|---|
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | **cada decisão técnica**, com justificativa, trade-offs e limitações: Money, ORM, concorrência, idempotência, schema, regras de negócio, mensageria, API, observabilidade, autenticação, testes, carga |
| [`SPEC.md`](SPEC.md) | requisitos derivados do enunciado, com **critérios de aceite numerados** (`AC-*`), a taxonomia de falhas e as **interpretações adotadas** (`INT-*`); os testes referenciam esses IDs |
| [`README-desafio.md`](README-desafio.md) | o enunciado oficial |
