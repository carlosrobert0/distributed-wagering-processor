# Distributed Wagering Processor

Serviço financeiro distribuído que processa transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`,
`ROLLBACK`) vindas de provedores de jogos por **HTTP** e por **SQS**, e continua correto com
mensagens duplicadas, fora de ordem e processadas por várias instâncias ao mesmo tempo.

- **Stack:** Bun 1.x · TypeScript strict · NestJS 11 · PostgreSQL 17 · MikroORM 7 · SQS (LocalStack) · Docker Compose
- **Documentos:** [`SPEC.md`](SPEC.md) (critérios de aceite e interpretações) ·
  [`ARCHITECTURE.md`](ARCHITECTURE.md) (decisões, trade-offs e limitações)

## Garantias principais

| Garantia | Como |
|---|---|
| Dinheiro exato | `Money` em `bigint` de centavos; `NUMERIC(20,2)` no banco; strings nos contratos; nunca `number` |
| Sem saldo negativo, sem lost update | `SELECT … FOR UPDATE` por wallet + `CHECK` + versão única por lançamento |
| Sem débito/crédito duplicado | `UNIQUE` de idempotency key e de `(provider, externalTransactionId)` + inbox `(consumer, messageId)` |
| Ledger auditável e imutável | triggers append-only, encadeamento `before(n) = after(n−1)` verificado no banco |
| `saldo == ledger` | constraint triggers diferidas checadas no COMMIT + reconciliação |
| Evento só depois do commit | Transactional Outbox + relay com `FOR UPDATE SKIP LOCKED` |
| N instâncias | nenhum estado em memória; toda coordenação no Postgres |

## Como rodar

Pré-requisitos: **Bun ≥ 1.2** e **Docker** (Docker Desktop, OrbStack ou Colima).

```bash
bun install

# Ambiente completo: Postgres, LocalStack (filas criadas no init), migrations e 3 instâncias
docker compose up -d --build
#   app-1 → http://localhost:3001   app-2 → :3002   app-3 → :3003

curl -s localhost:3001/health/ready
```

Rodando a aplicação no host (sem container da app):

```bash
docker compose up -d postgres localstack
bun run migration:up
AWS_ENDPOINT_URL=http://localhost:4566 bun run start      # http://localhost:3000
```

## Testes

Os testes de integração e concorrência usam **Postgres e LocalStack reais** (nada de mock).

```bash
docker compose up -d postgres localstack     # dependências
bun install

bun run typecheck           # tsc --noEmit (strict)
bun run test:unit           # domínio e aplicação, sem I/O
bun run test:integration    # constraints, migrations, atomicidade, API, outbox, consumer SQS
bun run test:concurrency    # 100−80−80, 50× mesma aposta, 3 processos, crash antes do ack, reinício
bun test                    # tudo
```

O banco `wagering_test` é recriado a cada execução; cada arquivo de teste cria suas próprias filas.
Para ver os logs das instâncias filhas: `TEST_INSTANCE_OUTPUT=1`; para logs da app em processo:
`TEST_LOG_LEVEL=info`.

Teste de carga (opcional, com o compose completo de pé): `bun run test:load`
(variáveis `LOAD_REQUESTS`, `LOAD_CONCURRENCY`, `LOAD_WALLETS`, `LOAD_DUPLICATE_RATIO`).

## API

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/wallets` | cria wallet (saldo inicial > 0 gera transação `OPENING` + lançamento) |
| `GET` | `/wallets/:walletId` | saldo e versão |
| `GET` | `/wallets/:walletId/ledger?cursor=&limit=50` | lançamentos, cursor opaco e estável |
| `POST` | `/wallets/:walletId/reconciliation` | compara saldo materializado com o ledger |
| `POST` | `/wagering/transactions` | submete transação (header `Idempotency-Key` obrigatório) |
| `GET` | `/wagering/transactions/:transactionId` | consulta por id interno |
| `GET` | `/providers/:providerId/wagering/transactions/:externalTransactionId` | consulta por id do provedor |
| `GET` | `/health/live`, `/health/ready` | liveness / readiness (Postgres + SQS) |
| `GET` | `/metrics` | métricas Prometheus |

Status: `400` payload inválido · `404` inexistente · `409` conflito de idempotência / wallet duplicada ·
`422` rejeição de negócio (com `failureCode`) · `202` pendente de referência · `201` processado ·
`200` replay · `503` falha transitória (+ `Retry-After`). Detalhes em [`ARCHITECTURE.md` §10](ARCHITECTURE.md).

```bash
WALLET=$(curl -s localhost:3001/wallets -H 'content-type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}')
WALLET_ID=$(echo "$WALLET" | sed -E 's/.*"id":"([^"]+)".*/\1/')

curl -s localhost:3002/wagering/transactions \
  -H 'content-type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",
       \"playerId\":\"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1\",\"walletId\":\"$WALLET_ID\",
       \"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
```

### Enviando pela fila

```bash
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET_ID" \
  --message-body "{\"messageId\":\"msg-1\",\"type\":\"WagerTransactionRequested\",
    \"occurredAt\":\"2026-10-21T15:00:00.000Z\",\"data\":{\"providerId\":\"provider-a\",
    \"externalTransactionId\":\"transaction-124\",\"idempotencyKey\":\"provider-a:transaction-124\",
    \"playerId\":\"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1\",\"walletId\":\"$WALLET_ID\",
    \"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
    \"money\":{\"amount\":\"10.00\",\"currency\":\"BRL\"}}}"
```

Filas: `wager-transactions.fifo` (entrada, redrive para a DLQ após 5 recebimentos),
`wager-transactions-dlq.fifo` e `wagering-events.fifo` (eventos de integração publicados pela outbox).

## Configuração

| Variável | Padrão | Descrição |
|---|---|---|
| `PORT` | `3000` | porta HTTP |
| `DATABASE_URL` | `postgres://wagering:wagering@localhost:5432/wagering` | Postgres |
| `DB_POOL_MAX` | `20` | conexões por instância |
| `DB_LOCK_TIMEOUT_MS` | `5000` | espera máxima pelo lock de uma wallet (estourou → 503 / retry) |
| `AWS_ENDPOINT_URL` | — | endpoint do LocalStack (`http://localhost:4566`) |
| `SQS_WAGER_QUEUE_NAME` / `SQS_WAGER_DLQ_NAME` / `SQS_EVENTS_QUEUE_NAME` | nomes acima | filas |
| `CONSUMER_ENABLED` / `OUTBOX_RELAY_ENABLED` / `PENDING_REFERENCE_WORKER_ENABLED` | `true` | liga os processos em segundo plano |
| `PENDING_REFERENCE_MAX_ATTEMPTS` | `8` | verificações antes de `REFERENCE_NOT_FOUND` |
| `LOG_LEVEL` | `info` | nível do log JSON |

A lista completa, com validação, está em [`src/infrastructure/config/app-config.ts`](src/infrastructure/config/app-config.ts).

## Estrutura

```
src/
  domain/          Money, Wallet, WalletLedgerEntry, WagerTransaction, settlement, inbox/outbox, eventos
  application/     use cases e ports (UnitOfWork, repositórios, métricas, logger, publisher)
  infrastructure/  MikroORM (schemas, mappers, repositórios, migration), SQS, pino, Prometheus, config
  interfaces/      HTTP (Nest), consumer SQS, workers (outbox relay, pending reference)
test/
  unit/  integration/  concurrency/  load/  support/
docker/localstack/init-sqs.sh   docker-compose.yml   Dockerfile
```

## Autenticação

Não implementada por decisão (vale 0 pontos). O ponto de extensão é o `ProviderAuthGuard` (no-op), e o
desenho com Keycloak (client credentials por provedor, `providerId` do token comparado com o do payload)
está em [`ARCHITECTURE.md` §12](ARCHITECTURE.md).
