# SPEC — Distributed Wagering Processor

Especificação derivada do enunciado (`README-desafio.md`). Cada requisito tem um ID estável.
Código e testes referenciam esses IDs (busque por `AC-` no repositório). Quando o enunciado
deixa espaço para interpretação, a decisão está registrada na seção **Interpretações** (`INT-`).

Legenda de origem: `§N` = seção do enunciado.

---

## 1. Invariantes globais (§3, §5, §14 eliminatórias)

| ID | Invariante | Onde é garantida |
|---|---|---|
| INV-01 | Nenhum valor monetário trafega como `number`/`float`/`double` | `Money` (bigint em centavos), `NUMERIC(20,2)` no banco, strings nos contratos |
| INV-02 | Saldo nunca negativo | `Wallet.debit` + `CHECK (balance >= 0)` em `wallets` e no ledger |
| INV-03 | Sem débito/crédito duplicado | `UNIQUE(idempotency_key)`, `UNIQUE(provider_id, external_transaction_id)`, `UNIQUE(transaction_id, wallet_id)` no ledger, índice único parcial de reversão |
| INV-04 | Idempotência persistente (nunca em memória) | tabela `wager_transactions` + `inbox_messages` |
| INV-05 | Correto com N instâncias | lock pessimista por linha (`SELECT … FOR UPDATE`) + constraints; nenhum estado compartilhado em memória |
| INV-06 | Evento nunca publicado antes do commit | Transactional Outbox: evento gravado na mesma transação SQL e publicado por um relay depois |
| INV-07 | Ledger auditável e imutável | triggers que bloqueiam `UPDATE/DELETE/TRUNCATE`, encadeamento `balance_before/after` verificado no banco |
| INV-08 | `wallet.balance == saldo reconstruído pelo ledger` | trigger de constraint diferida + reconciliação + asserção final em todos os testes de integração/concorrência |

---

## 2. Money (§6.1)

| ID | Critério de aceite |
|---|---|
| AC-MON-01 | `Money.from({amount, currency})` aceita somente string decimal com exatamente 2 casas (`^(0|[1-9]\d*)\.\d{2}$`) |
| AC-MON-02 | Rejeita `NaN`, `Infinity`, notação científica, string vazia, mais de 2 casas, valores negativos, sinal `+`, espaços, zeros à esquerda |
| AC-MON-03 | Moeda ISO-4217: 3 letras maiúsculas |
| AC-MON-04 | Imutável: `add/subtract/negate` retornam nova instância |
| AC-MON-05 | Operação entre moedas diferentes lança `CurrencyMismatchError` |
| AC-MON-06 | `toJSON()` → `{ amount: "25.00", currency: "BRL" }`, sempre 2 casas; `toString()` → `"25.00 BRL"` |
| AC-MON-07 | Arredondamento: nunca acontece. Escala fixa + apenas soma/subtração ⇒ aritmética exata. Entrada com > 2 casas é **rejeitada**, não arredondada (INT-01) |
| AC-MON-08 | Domínio não depende de ORM nem de Nest |

## 3. Wallet (§6.2)

| ID | Critério de aceite |
|---|---|
| AC-WAL-01 | No máximo uma wallet por `(playerId, currency)` — `UNIQUE` no banco; duplicata ⇒ `409 WALLET_ALREADY_EXISTS` |
| AC-WAL-02 | Saldo nunca negativo (domínio + `CHECK`) |
| AC-WAL-03 | Toda alteração de saldo produz exatamente um lançamento no ledger e vice-versa (domínio: `debit/credit` retornam o lançamento; banco: constraint trigger diferida) |
| AC-WAL-04 | Moeda da operação = moeda da wallet |
| AC-WAL-05 | `version` = 1 após criação e incrementa **somente** quando o saldo muda |
| AC-WAL-06 | Saldo inicial > 0 gera transação `OPENING` + lançamento `CREDIT` na mesma transação SQL (§9) |
| AC-WAL-07 | `rehydrate` reconstrói sem revalidar transições |

## 4. WagerTransaction (§6.3, §7)

| ID | Critério de aceite |
|---|---|
| AC-TX-01 | Nasce em `PENDING` via `create`; construtor privado |
| AC-TX-02 | Transições válidas: `PENDING → PROCESSED | REJECTED | FAILED | PENDING_REFERENCE`; `PENDING_REFERENCE → PROCESSED | REJECTED | FAILED`. Terminais (`PROCESSED`, `REJECTED`, `FAILED`) lançam `InvalidTransactionStateError` |
| AC-TX-03 | `OPENING` não pode ser submetido pela API nem pela fila |
| AC-TX-04 | `REFUND` e `ROLLBACK` exigem `referenceExternalTransactionId` (domínio + `CHECK`) |
| AC-TX-05 | Referência resolvida por `(providerId, referenceExternalTransactionId)`; deve ter mesmo provider, player, wallet, moeda e rodada ⇒ senão `REJECTED REFERENCE_MISMATCH` |
| AC-TX-06 | `REFUND` só referencia `BET`; `ROLLBACK` referencia `BET`, `WIN` ou `REFUND`; `WIN` (opcional) só referencia `BET` ⇒ senão `REJECTED INVALID_REFERENCE_KIND` |
| AC-TX-07 | Uma referência só é revertida uma vez (INT-05) ⇒ `REJECTED ALREADY_REVERSED`; garantido também por índice único parcial |
| AC-TX-08 | Valor de `REFUND`/`ROLLBACK` = valor da referência ⇒ senão `REJECTED AMOUNT_MISMATCH` |
| AC-TX-09 | `REJECTED` não altera saldo nem gera ledger |
| AC-TX-10 | Replay devolve o resultado original, incluindo o saldo observado naquele momento (`balance_after` persistido) |
| AC-TX-11 | Referência ausente ⇒ `PENDING_REFERENCE` persistido e reprocessado depois |
| AC-TX-12 | Reversão que deixaria saldo negativo ⇒ `REJECTED REVERSAL_WOULD_OVERDRAW` (distinto de `INSUFFICIENT_FUNDS`) |
| AC-TX-13 | `BET` sem saldo ⇒ `REJECTED INSUFFICIENT_FUNDS` |
| AC-TX-14 | `LOSS` não move saldo nem gera ledger, mas é `PROCESSED` e publica `WagerTransactionProcessed` |
| AC-TX-15 | `ROLLBACK` aplica a direção inversa da referência (BET→CREDIT, WIN→DEBIT, REFUND→DEBIT) |
| AC-TX-16 | Referência existente mas ainda não aplicada (`PENDING`/`PENDING_REFERENCE`) ⇒ a dependente também fica `PENDING_REFERENCE`; referência `REJECTED`/`FAILED` ⇒ `REJECTED REFERENCE_NOT_PROCESSED` |

### 4.1 PENDING_REFERENCE (§7.1)

| ID | Critério de aceite |
|---|---|
| AC-PEND-01 | Worker agendado reprocessa `PENDING_REFERENCE` com backoff exponencial |
| AC-PEND-02 | Limite: 8 tentativas (2s, 4s, …, 256s ≈ 8,5 min no total) — INT-06 |
| AC-PEND-03 | Esgotado ⇒ `REJECTED REFERENCE_NOT_FOUND` + `WagerTransactionRejected` publicado |
| AC-PEND-04 | Seguro com múltiplos workers/instâncias (`FOR UPDATE SKIP LOCKED`, ordem de lock wallet → transação) |
| AC-PEND-05 | Ao processar uma transação, dependentes pendentes que a referenciam são "acordadas" (`next_reference_check_at = now`) — otimização de latência |

### 4.2 Taxonomia de falhas (§7.2)

| failureCode | Quando | Provedor deve |
|---|---|---|
| `INSUFFICIENT_FUNDS` | BET sem saldo suficiente | desistir / não reenviar |
| `REVERSAL_WOULD_OVERDRAW` | ROLLBACK de WIN/REFUND que deixaria saldo negativo | escalar para operação manual |
| `REFERENCE_NOT_FOUND` | referência não chegou dentro do limite | reenviar com nova chave após enviar a referência |
| `REFERENCE_NOT_PROCESSED` | referência existe mas foi `REJECTED`/`FAILED` | desistir |
| `INVALID_REFERENCE_KIND` | REFUND sobre não-BET, ROLLBACK sobre LOSS/ROLLBACK, WIN sobre não-BET | corrigir payload |
| `REFERENCE_MISMATCH` | referência de outro player/wallet/rodada/moeda | corrigir payload |
| `AMOUNT_MISMATCH` | valor da reversão ≠ valor da referência | corrigir payload |
| `ALREADY_REVERSED` | referência já revertida | desistir (já foi feito) |
| `CURRENCY_MISMATCH` | moeda da operação ≠ moeda da wallet | corrigir payload |
| `PLAYER_WALLET_MISMATCH` | `playerId` ≠ dono da wallet | corrigir payload |
| `PROCESSING_ERROR` | erro permanente de infraestrutura no reprocessamento (status `FAILED`) | contatar operação |

Erros que **não** viram transação persistida (respondidos diretamente): `INVALID_PAYLOAD`, `IDEMPOTENCY_KEY_REQUIRED`,
`IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`, `WALLET_NOT_FOUND`, `TRANSIENT_FAILURE`.

## 5. Ledger (§6.4)

| ID | Critério de aceite |
|---|---|
| AC-LED-01 | `WalletLedgerEntry` sem campos mutáveis nem métodos de transição |
| AC-LED-02 | `create` valida `balanceBefore ± money == balanceAfter` |
| AC-LED-03 | No máximo um lançamento por transação por wallet (`UNIQUE(transaction_id, wallet_id)`) |
| AC-LED-04 | `UPDATE`, `DELETE` e `TRUNCATE` bloqueados por trigger |
| AC-LED-05 | Encadeamento verificado no banco: `balance_before` do lançamento N = `balance_after` do N−1 (por wallet, ordenado por `wallet_version`) |
| AC-LED-06 | `UNIQUE(wallet_id, wallet_version)`: cada versão da wallet tem no máximo um lançamento — prova de ausência de lost update no schema |

## 6. Idempotência (§9)

| ID | Critério de aceite |
|---|---|
| AC-IDEM-01 | Header `Idempotency-Key` obrigatório (`400 IDEMPOTENCY_KEY_REQUIRED`) |
| AC-IDEM-02 | `payloadHash = sha256(canonicalJson(campos de negócio))` com chaves ordenadas recursivamente; campos: `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}, referenceExternalTransactionId?`. Header, `messageId`, `occurredAt` e `idempotencyKey` não entram |
| AC-IDEM-03 | Requisição idêntica ⇒ mesma resposta, com `idempotentReplay: true` |
| AC-IDEM-04 | Mesma key, payload diferente ⇒ `409 IDEMPOTENCY_KEY_CONFLICT`, nunca replay |
| AC-IDEM-05 | Mesmo `(providerId, externalTransactionId)` com outra key ⇒ `409 EXTERNAL_TRANSACTION_CONFLICT` |
| AC-IDEM-06 | Garantido no banco (`UNIQUE`), inclusive em corrida entre instâncias |

## 7. Concorrência (§8)

| ID | Critério de aceite |
|---|---|
| AC-CONC-01 | Unidade de concorrência = `walletId` (lock de linha; sem lock global) |
| AC-CONC-02 | Cenário obrigatório: saldo 100, duas BET de 80 simultâneas ⇒ 1 `PROCESSED`, 1 `REJECTED INSUFFICIENT_FUNDS`, saldo 20, 1 débito |
| AC-CONC-03 | Mesma aposta 50× em paralelo ⇒ 1 débito |
| AC-CONC-04 | Wallets distintas processadas em paralelo |
| AC-CONC-05 | ≥ 3 instâncias simultâneas |
| AC-CONC-06 | Espera de lock limitada (`lock_timeout`) ⇒ `503` / retry, nunca espera infinita |

## 8. API HTTP (§9)

| Situação | Status | Corpo |
|---|---|---|
| Payload inválido / sem Idempotency-Key | **400** | `{ error, message, details? }` |
| Recurso inexistente | **404** | `{ error }` |
| Conflito (idempotência, external id, wallet duplicada) | **409** | `{ error }` |
| Rejeição de negócio (transação `REJECTED`/`FAILED` persistida) | **422** | resultado + `failureCode` |
| Aceito, pendente de referência | **202** | resultado |
| Processado (primeira vez) | **201** | resultado |
| Processado (replay) | **200** | resultado com `idempotentReplay: true` |
| Falha transitória (banco/SQS fora, lock timeout) | **503** + `Retry-After` | `{ error: "TRANSIENT_FAILURE" }` |

| ID | Endpoint |
|---|---|
| AC-API-01 | `POST /wallets` |
| AC-API-02 | `GET /wallets/:walletId` |
| AC-API-03 | `GET /wallets/:walletId/ledger?cursor&limit` — cursor opaco (base64url) e estável (`wallet_version`) |
| AC-API-04 | `GET /wagering/transactions/:transactionId` |
| AC-API-05 | `GET /providers/:providerId/wagering/transactions/:externalTransactionId` |
| AC-API-06 | `POST /wagering/transactions` |
| AC-API-07 | `POST /wallets/:walletId/reconciliation` — divergência logada, contabilizada em métrica e sinalizada (`consistent:false`); nunca corrigida |
| AC-API-08 | `GET /health/live`, `GET /health/ready` (Postgres + SQS), sem autenticação |
| AC-API-09 | `GET /metrics` (Prometheus) |

## 9. SQS (§10)

| ID | Critério de aceite |
|---|---|
| AC-SQS-01 | Filas `wager-transactions.fifo` e `wager-transactions-dlq.fifo` (redrive `maxReceiveCount`) |
| AC-SQS-02 | Consumer reutiliza o mesmo use case do HTTP |
| AC-SQS-03 | Inbox persistente `(consumer_name, message_id)` gravada na mesma transação SQL |
| AC-SQS-04 | `DeleteMessage` (ack) somente após o commit |
| AC-SQS-05 | Erro de negócio ⇒ ack; transitório ⇒ visibilidade com backoff exponencial; permanente ⇒ DLQ imediata |
| AC-SQS-06 | Limite de tentativas antes da DLQ (redrive `maxReceiveCount`) |
| AC-SQS-07 | `SIGTERM`: para de buscar, conclui mensagens em andamento e devolve visibilidade das não iniciadas |
| AC-SQS-08 | Redelivery não duplica efeitos |

## 10. Outbox (§11)

| ID | Critério de aceite |
|---|---|
| AC-OUT-01 | Transação, saldo, ledger, inbox e evento gravados atomicamente |
| AC-OUT-02 | Relay com múltiplos publishers concorrentes (`FOR UPDATE SKIP LOCKED`) |
| AC-OUT-03 | Commit → processo morre → outra instância publica |
| AC-OUT-04 | Publicação duplicada é segura (`eventId` estável = `MessageDeduplicationId`; consumidor deduplica por `eventId`) |
| AC-OUT-05 | Eventos: `WagerTransactionProcessed` (inclusive LOSS e OPENING), `WagerTransactionRejected`, `WalletBalanceChanged` (somente quando o saldo muda), `WagerTransactionPendingReference` |
| AC-OUT-06 | Envelope: classe abstrata `IntegrationEvent<T>`, `eventType`/`version` no tipo, `data` com `MoneyProps` |
| AC-OUT-07 | Falha de publicação ⇒ `attempts++` e backoff exponencial com teto; nunca descarta (INT-08) |

## 11. Observabilidade (§12)

| ID | Critério de aceite |
|---|---|
| AC-OBS-01 | Logs JSON com `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId` |
| AC-OBS-02 | Sem payload financeiro completo nem dados sensíveis nos logs (sem valores, sem saldo) |
| AC-OBS-03 | Métricas: transações por status, duplicatas, retries, DLQ, conflitos de lock, outbox lag, latência |

## 12. Testes (§13) — mapa para arquivos

| Requisito do enunciado | Arquivo |
|---|---|
| Money (escala, arredondamento, inválidos) | `test/unit/domain/money.test.ts` |
| Invariantes da Wallet | `test/unit/domain/wallet.test.ts` |
| Regras BET/WIN/LOSS/REFUND/ROLLBACK | `test/unit/domain/wager-settlement.test.ts`, `wager-transaction.test.ts` |
| Conflito de moeda | `money.test.ts`, `wallet.test.ts`, `wager-settlement.test.ts` |
| Key com payload divergente | `test/unit/application/payload-hash.test.ts`, `test/unit/application/process-wager-transaction.test.ts` |
| Classificação de erros (HTTP e SQS) | `test/unit/interfaces/error-mapping.test.ts`, `test/unit/interfaces/wager-message.handler.test.ts` |
| Migrations e constraints | `test/integration/migrations.test.ts`, `schema-constraints.test.ts` |
| Atomicidade wallet/ledger/inbox/outbox | `test/integration/atomicity.test.ts` |
| Inbox e redelivery | `test/integration/sqs-consumer.test.ts` |
| Publishers concorrentes | `test/integration/outbox-relay.test.ts` |
| Retry e DLQ | `test/integration/sqs-consumer.test.ts` |
| Recuperação após reinício | `test/concurrency/restart-recovery.test.ts` |
| 50× mesma aposta | `test/concurrency/same-bet-50x.test.ts` |
| 100 − 80 − 80 | `test/concurrency/double-spend.test.ts` |
| Wallets distintas em paralelo | `test/concurrency/parallel-wallets.test.ts` |
| ≥ 3 instâncias | `test/concurrency/multi-instance.test.ts` |
| Worker morto após commit, antes do ack | `test/concurrency/crash-before-ack.test.ts` |
| Dois publishers na mesma outbox | `test/integration/outbox-relay.test.ts` |
| ROLLBACK/REFUND antes da referência | `test/integration/out-of-order.test.ts` |
| Invariante final `balance == ledger` | `test/support/invariants.ts`, chamado em todos os testes de integração/concorrência |

---

## 13. Interpretações adotadas (§7 "qualquer interpretação adicional deve ser documentada")

- **INT-01 — Arredondamento.** Não há divisão nem multiplicação por fração no domínio; logo nunca há arredondamento. Entradas com mais de 2 casas são rejeitadas (o enunciado manda rejeitar).
- **INT-02 — Valor zero.** `amount > 0` para todos os kinds, exceto `LOSS`, que aceita `0.00` (provedores costumam reportar LOSS sem valor). Saldo inicial `0.00` é aceito e não gera `OPENING`.
- **INT-03 — Referência na WIN.** `WIN` pode (não precisa) trazer referência. Se trouxer, ela é resolvida e validada como as demais (inclusive `PENDING_REFERENCE` se ausente). O `CHECK` do banco é uma implicação (`REFUND/ROLLBACK ⇒ ref NOT NULL`), não uma equivalência.
- **INT-04 — Wallet inexistente.** A FK impede persistir a transação; responde `404 WALLET_NOT_FOUND` sem persistir (na fila: erro de negócio ⇒ ack).
- **INT-05 — Reversão única por referência.** A regra 4 ("não pode ser revertida duas vezes pelo mesmo tipo") lida literalmente permitiria REFUND **e** ROLLBACK sobre a mesma BET, creditando duas vezes. Adotamos a leitura mais segura: **uma referência é revertida no máximo uma vez, por qualquer tipo de reversão.** Índice único parcial `(reference_transaction_id) WHERE kind IN ('REFUND','ROLLBACK') AND status='PROCESSED'`.
- **INT-06 — Limite do PENDING_REFERENCE.** 8 tentativas com backoff 2s·2ⁿ (teto 5 min) ≈ 8,5 min. Provedores tipicamente reenviam a referência em segundos; uma janela longa demais deixaria rodadas abertas e o jogador sem feedback.
- **INT-07 — Replay e status.** O status HTTP depende do status da transação: `PROCESSED` → 201 (novo) / 200 (replay); `PENDING_REFERENCE` → 202; `REJECTED`/`FAILED` → 422 (novo e replay). Se a transação evoluiu (ex.: `PENDING_REFERENCE` → `PROCESSED`), o replay devolve o estado atual — o resultado "original" ainda não existia.
- **INT-08 — Outbox nunca desiste.** Publicação falha ⇒ backoff com teto (1s → 5 min) para sempre; o `outbox_lag_seconds` alerta. Descartar evento violaria "não perder eventos confirmados".
- **INT-09 — Fila de eventos.** O enunciado define apenas a fila de comandos; os eventos de integração vão para `wagering-events.fifo` (`MessageGroupId = walletId`, `MessageDeduplicationId = eventId`).
- **INT-10 — OPENING.** Transação interna com `providerId = "internal"`, `externalTransactionId = "opening:{walletId}"`, `idempotencyKey = "internal:opening:{walletId}"`, `roundId = "opening"`, `gameId = "internal"`. Validações de entrada rejeitam `kind = OPENING` e `providerId = "internal"`.
- **INT-11 — Conflito de idempotência na fila.** Mesma key com payload diferente vindo do SQS é tratado como erro de negócio terminal (ack + log + métrica), igual ao 409 do HTTP.
- **INT-12 — FAILED.** Usado quando o reprocessamento de `PENDING_REFERENCE` encontra erro não transitório (ex.: dado corrompido que viola invariante do domínio). Fica auditável com `PROCESSING_ERROR`.
