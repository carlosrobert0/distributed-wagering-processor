import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { EventPublisher } from '../../application/ports/system';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { SqsQueues } from './sqs';

/**
 * Publica eventos da outbox na fila FIFO de eventos (INT-09).
 *
 * - `MessageGroupId = partitionKey (walletId)`: ordem por wallet;
 * - `MessageDeduplicationId = eventId`: republicações dentro de 5 min são descartadas pelo SQS.
 *   É otimização — a garantia final é o consumidor deduplicar por `eventId` (AC-OUT-04).
 */
export class SqsEventPublisher implements EventPublisher {
  constructor(
    private readonly queues: SqsQueues,
    private readonly queueName: string,
  ) {}

  async publish(message: OutboxMessage): Promise<void> {
    await this.queues.client.send(
      new SendMessageCommand({
        QueueUrl: await this.queues.url(this.queueName),
        MessageBody: JSON.stringify(message.payload),
        MessageGroupId: message.partitionKey,
        MessageDeduplicationId: message.id,
        MessageAttributes: {
          eventType: { DataType: 'String', StringValue: message.eventType },
          eventId: { DataType: 'String', StringValue: message.id },
        },
      }),
    );
  }
}
