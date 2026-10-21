import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { randomUUID } from 'node:crypto';
import { createSqsClient } from '../../src/infrastructure/messaging/sqs';
import { AWS_ENDPOINT_URL, TestQueues } from './environment';

export const sqs: SQSClient = createSqsClient({
  region: 'us-east-1',
  endpoint: AWS_ENDPOINT_URL,
  accessKeyId: 'test',
  secretAccessKey: 'test',
});

export async function createTestQueues(
  options: { maxReceiveCount?: number; visibilityTimeoutSeconds?: number } = {},
): Promise<TestQueues & { cleanup: () => Promise<void> }> {
  const prefix = `t-${randomUUID().slice(0, 8)}`;
  const names: TestQueues = {
    wagerTransactions: `${prefix}-wager-transactions.fifo`,
    wagerTransactionsDlq: `${prefix}-wager-transactions-dlq.fifo`,
    events: `${prefix}-events.fifo`,
  };

  const dlq = await sqs.send(
    new CreateQueueCommand({
      QueueName: names.wagerTransactionsDlq,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'true' },
    }),
  );
  const dlqArn = (
    await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ['QueueArn'] }))
  ).Attributes?.QueueArn;

  await sqs.send(
    new CreateQueueCommand({
      QueueName: names.wagerTransactions,
      Attributes: {
        FifoQueue: 'true',
        ContentBasedDeduplication: 'true',
        VisibilityTimeout: String(options.visibilityTimeoutSeconds ?? 30),
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: dlqArn,
          maxReceiveCount: String(options.maxReceiveCount ?? 5),
        }),
      },
    }),
  );
  await sqs.send(
    new CreateQueueCommand({
      QueueName: names.events,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
    }),
  );

  return {
    ...names,
    cleanup: async () => {
      await Promise.all(
        Object.values(names).map(async (name) => {
          const url = await queueUrl(name).catch(() => undefined);
          if (url) await sqs.send(new DeleteQueueCommand({ QueueUrl: url })).catch(() => undefined);
        }),
      );
    },
  };
}

export async function queueUrl(name: string): Promise<string> {
  const result = await sqs.send(new GetQueueUrlCommand({ QueueName: name }));
  if (!result.QueueUrl) throw new Error(`queue ${name} not found`);
  return result.QueueUrl;
}

export interface WagerMessageData {
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: { amount: string; currency: string };
  referenceExternalTransactionId?: string;
}

export async function sendWagerMessage(
  queueName: string,
  data: WagerMessageData,
  options: { messageId?: string; deduplicationId?: string } = {},
): Promise<string> {
  const messageId = options.messageId ?? `msg-${randomUUID()}`;
  await sendRawMessage(
    queueName,
    JSON.stringify({ messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data }),
    { groupId: data.walletId, deduplicationId: options.deduplicationId ?? randomUUID() },
  );
  return messageId;
}

export async function sendRawMessage(
  queueName: string,
  body: string,
  options: { groupId: string; deduplicationId?: string },
): Promise<void> {
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: await queueUrl(queueName),
      MessageBody: body,
      MessageGroupId: options.groupId,
      MessageDeduplicationId: options.deduplicationId ?? randomUUID(),
    }),
  );
}

export async function peekMessages(queueName: string, waitSeconds = 1, visibilitySeconds = 0): Promise<Message[]> {
  const result = await sqs.send(
    new ReceiveMessageCommand({
      QueueUrl: await queueUrl(queueName),
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: waitSeconds,
      VisibilityTimeout: visibilitySeconds,
      MessageAttributeNames: ['All'],
      MessageSystemAttributeNames: ['All'],
      AttributeNames: ['All'],
    }),
  );
  return result.Messages ?? [];
}

export async function drainMessages(queueName: string, visibilitySeconds = 60): Promise<Message[]> {
  const all: Message[] = [];
  for (let round = 0; round < 50; round++) {
    const batch = await peekMessages(queueName, 1, visibilitySeconds);
    if (batch.length === 0) break;
    all.push(...batch);
  }
  return all;
}

export async function approximateCounts(queueName: string): Promise<{ visible: number; inFlight: number }> {
  const result = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: await queueUrl(queueName),
      AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
    }),
  );
  return {
    visible: Number(result.Attributes?.ApproximateNumberOfMessages ?? 0),
    inFlight: Number(result.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0),
  };
}
