#!/bin/bash
# Cria as filas FIFO na inicialização do LocalStack (hook ready.d).
#  - wager-transactions.fifo      comandos de entrada; redrive para a DLQ após 5 recebimentos
#  - wager-transactions-dlq.fifo  mensagens permanentes ou que esgotaram as tentativas
#  - wagering-events.fifo         eventos de integração publicados pelo relay da outbox
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
MAX_RECEIVE_COUNT="${WAGER_MAX_RECEIVE_COUNT:-5}"

awslocal --region "$REGION" sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true,MessageRetentionPeriod=1209600 >/dev/null

DLQ_URL=$(awslocal --region "$REGION" sqs get-queue-url --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text)
DLQ_ARN=$(awslocal --region "$REGION" sqs get-queue-attributes --queue-url "$DLQ_URL" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

ATTRIBUTES_FILE=$(mktemp)
cat >"$ATTRIBUTES_FILE" <<JSON
{
  "FifoQueue": "true",
  "ContentBasedDeduplication": "true",
  "VisibilityTimeout": "30",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"${MAX_RECEIVE_COUNT}\"}"
}
JSON
awslocal --region "$REGION" sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "file://${ATTRIBUTES_FILE}" >/dev/null
rm -f "$ATTRIBUTES_FILE"

awslocal --region "$REGION" sqs create-queue \
  --queue-name wagering-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false >/dev/null

echo "SQS queues ready: wager-transactions.fifo (maxReceiveCount=${MAX_RECEIVE_COUNT}), wager-transactions-dlq.fifo, wagering-events.fifo"
