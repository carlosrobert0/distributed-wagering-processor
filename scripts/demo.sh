#!/usr/bin/env bash
# Demonstração guiada dos cenários principais contra o ambiente do docker compose.
#   docker compose up -d --build && bun run demo
# Requer apenas curl (e, opcionalmente, docker para a etapa do SQS).
set -uo pipefail

APP_1="${APP_1:-http://localhost:3001}"
APP_2="${APP_2:-http://localhost:3002}"
APP_3="${APP_3:-http://localhost:3003}"
RUN="$(date +%s)$RANDOM"
PLAYER="demo-player-$RUN"

title() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
note() { printf '   \033[2m%s\033[0m\n' "$*"; }

request() {
  local method=$1 url=$2 key=${3:-} body=${4:-}
  local args=(-sS -X "$method" "$url" -H 'content-type: application/json' -w '\n   -> HTTP %{http_code}\n')
  [ -n "$key" ] && args+=(-H "Idempotency-Key: $key")
  [ -n "$body" ] && args+=(-d "$body")
  curl "${args[@]}"
}

field() { sed -E "s/.*\"$1\":\"([^\"]+)\".*/\1/"; }

wager_body() {
  local player=$1 wallet=$2 kind=$3 external=$4 amount=$5 reference=${6:-} currency=${7:-BRL} ref_json=""
  [ -n "$reference" ] && ref_json=",\"referenceExternalTransactionId\":\"$reference\""
  printf '{"providerId":"demo","externalTransactionId":"%s","playerId":"%s","walletId":"%s","roundId":"round-1","gameId":"fortune-chimp","kind":"%s","money":{"amount":"%s","currency":"%s"}%s}' \
    "$external" "$player" "$wallet" "$kind" "$amount" "$currency" "$ref_json"
}

submit() {
  local host=$1 player=$2 wallet=$3 kind=$4 external=$5 amount=$6 reference=${7:-} currency=${8:-BRL}
  request POST "$host/wagering/transactions" "demo:$external" "$(wager_body "$player" "$wallet" "$kind" "$external" "$amount" "$reference" "$currency")"
}

create_wallet() {
  curl -sS "$APP_1/wallets" -H 'content-type: application/json' \
    -d "{\"playerId\":\"$1\",\"initialBalance\":{\"amount\":\"$2\",\"currency\":\"BRL\"}}" | field id
}

balance() { curl -sS "$APP_1/wallets/$1" | sed -E 's/.*"balance":\{"amount":"([^"]+)".*"version":([0-9]+).*/saldo \1 (versão \2)/'; }

title "0. As três instâncias estão prontas?"
for app in "$APP_1" "$APP_2" "$APP_3"; do
  printf '   %s -> ' "$app"
  curl -sS "$app/health/ready"
  echo
done

title "1. Criar wallet com 100,00 BRL (gera a transação OPENING e o lançamento CREDIT na mesma transação SQL)"
P_MAIN="$PLAYER-main"
CREATED="$(curl -sS -w '\n   -> HTTP %{http_code}\n' "$APP_1/wallets" -H 'content-type: application/json' \
  -d "{\"playerId\":\"$P_MAIN\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")"
echo "$CREATED"
WALLET="$(echo "$CREATED" | head -1 | field id)"
note "wallet usada nos próximos passos: $WALLET (jogador $P_MAIN)"

title "2. Wallet duplicada para o mesmo jogador e moeda -> 409"
request POST "$APP_1/wallets" "" "{\"playerId\":\"$P_MAIN\",\"initialBalance\":{\"amount\":\"5.00\",\"currency\":\"BRL\"}}"

title "3. BET de 30,00 enviada à instância 2 -> 201 PROCESSED"
submit "$APP_2" "$P_MAIN" "$WALLET" BET "bet-$RUN" 30.00

title "4. A MESMA BET reenviada à instância 3 -> 200 com idempotentReplay=true (saldo daquele momento, mesmo transactionId)"
submit "$APP_3" "$P_MAIN" "$WALLET" BET "bet-$RUN" 30.00

title "5. Mesma Idempotency-Key com valor diferente -> 409 (conflito, nunca replay)"
submit "$APP_1" "$P_MAIN" "$WALLET" BET "bet-$RUN" 31.00

title "6. Pedidos inválidos -> 400 (3 casas decimais; kind OPENING; sem Idempotency-Key)"
submit "$APP_1" "$P_MAIN" "$WALLET" BET "bad-$RUN-1" 1.001
submit "$APP_1" "$P_MAIN" "$WALLET" OPENING "bad-$RUN-2" 1.00
request POST "$APP_1/wagering/transactions" "" "$(wager_body "$P_MAIN" "$WALLET" BET "bad-$RUN-3" 1.00)"

title "7. Aposta acima do saldo -> 422 REJECTED INSUFFICIENT_FUNDS (auditável, sem lançamento no ledger)"
submit "$APP_1" "$P_MAIN" "$WALLET" BET "big-$RUN" 500.00

title "8. REFUND da BET -> 201 e o saldo volta; um segundo REFUND ou um ROLLBACK da mesma BET -> 422 ALREADY_REVERSED"
submit "$APP_2" "$P_MAIN" "$WALLET" REFUND "refund-$RUN" 30.00 "bet-$RUN"
submit "$APP_2" "$P_MAIN" "$WALLET" REFUND "refund2-$RUN" 30.00 "bet-$RUN"
submit "$APP_2" "$P_MAIN" "$WALLET" ROLLBACK "rollback-$RUN" 30.00 "bet-$RUN"

title "9. Reversão com valor diferente do original -> 422 AMOUNT_MISMATCH"
submit "$APP_1" "$P_MAIN" "$WALLET" BET "bet2-$RUN" 10.00 > /dev/null
submit "$APP_1" "$P_MAIN" "$WALLET" REFUND "refund3-$RUN" 9.99 "bet2-$RUN"

title "10. LOSS não move saldo nem incrementa a versão da wallet"
note "antes : $(balance "$WALLET")"
submit "$APP_1" "$P_MAIN" "$WALLET" LOSS "loss-$RUN" 0.00
note "depois: $(balance "$WALLET")"

title "11. REFUND chega ANTES da BET que ele referencia -> 202 PENDING_REFERENCE"
submit "$APP_1" "$P_MAIN" "$WALLET" REFUND "late-refund-$RUN" 5.00 "late-bet-$RUN"
note "a BET referenciada chega depois..."
submit "$APP_1" "$P_MAIN" "$WALLET" BET "late-bet-$RUN" 5.00 > /dev/null
note "o worker agendado reprocessa a pendente (consulta por provedor + id externo):"
for _ in $(seq 1 15); do
  STATUS="$(curl -sS "$APP_1/providers/demo/wagering/transactions/late-refund-$RUN" | field status)"
  [ "$STATUS" = "PROCESSED" ] && break
  sleep 1
done
curl -sS "$APP_1/providers/demo/wagering/transactions/late-refund-$RUN"; echo
note "saldo: $(balance "$WALLET")"

title "12. Cenário obrigatório: 100,00 de saldo, duas BETs de 80,00 SIMULTÂNEAS em instâncias diferentes"
P_RACE="$PLAYER-race"
RACE_WALLET="$(create_wallet "$P_RACE" 100.00)"
TMP_A="$(mktemp)"; TMP_B="$(mktemp)"
submit "$APP_2" "$P_RACE" "$RACE_WALLET" BET "race-a-$RUN" 80.00 > "$TMP_A" &
submit "$APP_3" "$P_RACE" "$RACE_WALLET" BET "race-b-$RUN" 80.00 > "$TMP_B" &
wait
cat "$TMP_A" "$TMP_B"; rm -f "$TMP_A" "$TMP_B"
note "resultado esperado: uma PROCESSED, outra REJECTED, saldo 20,00 e exatamente um débito no ledger"
note "$(balance "$RACE_WALLET")"

title "13. Mensagem pela fila SQS (mesmo caso de uso do HTTP; inbox + ack após o commit)"
if command -v docker >/dev/null 2>&1 && docker compose ps localstack >/dev/null 2>&1; then
  BODY="{\"messageId\":\"msg-$RUN\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"data\":{\"providerId\":\"demo\",\"externalTransactionId\":\"sqs-$RUN\",\"idempotencyKey\":\"demo:sqs-$RUN\",\"playerId\":\"$P_RACE\",\"walletId\":\"$RACE_WALLET\",\"roundId\":\"round-1\",\"gameId\":\"fortune-chimp\",\"kind\":\"WIN\",\"money\":{\"amount\":\"15.00\",\"currency\":\"BRL\"}}}"
  docker compose exec -T localstack awslocal sqs send-message \
    --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
    --message-group-id "$RACE_WALLET" --message-body "$BODY" > /dev/null
  sleep 4
  note "WIN de 15,00 enviada pela fila -> $(balance "$RACE_WALLET")"
else
  note "docker compose indisponível: etapa do SQS ignorada"
fi

title "14. Ledger paginado (cursor opaco) e reconciliação saldo x ledger"
curl -sS "$APP_1/wallets/$WALLET/ledger?limit=3"; echo
request POST "$APP_1/wallets/$WALLET/reconciliation"
request POST "$APP_1/wallets/$RACE_WALLET/reconciliation"

title "Fim. Métricas em $APP_1/metrics, logs com: docker compose logs -f app-1 app-2 app-3"
