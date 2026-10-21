import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  Message,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { AppLogger, Metrics } from '../../application/ports/system';
import { ExponentialBackoff } from '../../domain/shared/exponential-backoff';
import { SqsQueues } from '../../infrastructure/messaging/sqs';
import { WagerMessageHandler } from './wager-message.handler';

export interface WagerConsumerOptions {
  queueName: string;
  dlqName: string;
  waitTimeSeconds: number;
  visibilityTimeoutSeconds: number;
  batchSize: number;
  retryBaseSeconds: number;
  retryMaxSeconds: number;
  shutdownGraceMs: number;
  faultInjection: 'none' | 'crash-after-commit';
}

export type MessageDisposition = 'acked' | 'retry-scheduled' | 'dead-lettered';

const MAX_VISIBILITY_TIMEOUT_SECONDS = 43_200;
const POLL_FAILURE_BACKOFF_MS = 1_000;
const MAX_DEAD_LETTER_REASON_LENGTH = 1_000;

export class WagerTransactionsConsumer {
  private running = false;
  private loop?: Promise<void>;
  private pollAbort = new AbortController();
  private readonly inFlight = new Set<Promise<unknown>>();
  private readonly backoff: ExponentialBackoff;
  private maxReceiveCount?: number;

  constructor(
    private readonly handler: WagerMessageHandler,
    private readonly queues: SqsQueues,
    private readonly options: WagerConsumerOptions,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {
    const base = Math.max(1, options.retryBaseSeconds);
    this.backoff = new ExponentialBackoff(base, Math.max(base, options.retryMaxSeconds));
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.pollAbort = new AbortController();
    this.loop = this.pollLoop();
    this.logger.info('sqs consumer started', { queue: this.options.queueName });
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.pollAbort.abort();
    const drained = Promise.allSettled([...this.inFlight]).then(() => true);
    const timedOut = new Promise<boolean>((resolve) => setTimeout(() => resolve(false), this.options.shutdownGraceMs));
    const drainedInTime = await Promise.race([drained, timedOut]);
    if (drainedInTime) {
      await this.loop?.catch(() => undefined);
    }
    this.logger.info('sqs consumer stopped', { drained: drainedInTime });
  }

  private async pollLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.pollOnce();
      } catch (error) {
        if (!this.running) break;
        this.logger.warn('sqs poll failed; backing off', { reason: describe(error) });
        await sleep(POLL_FAILURE_BACKOFF_MS);
      }
    }
  }

  private async pollOnce(): Promise<void> {
    const queueUrl = await this.queues.url(this.options.queueName);
    this.maxReceiveCount ??= await this.queues.maxReceiveCount(this.options.queueName).catch(() => undefined);
    const response = await this.queues.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: this.options.batchSize,
        WaitTimeSeconds: this.options.waitTimeSeconds,
        VisibilityTimeout: this.options.visibilityTimeoutSeconds,
        MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
        AttributeNames: ['All'],
      }),
      { abortSignal: this.pollAbort.signal },
    );
    const messages = response.Messages ?? [];
    if (!this.running) {
      await this.releaseAll(queueUrl, messages);
      return;
    }
    if (messages.length > 0) {
      await this.track(this.processBatch(queueUrl, messages));
    }
  }

  private async processBatch(queueUrl: string, messages: Message[]): Promise<void> {
    const groups = new Map<string, Message[]>();
    for (const message of messages) {
      const group = message.Attributes?.MessageGroupId ?? message.MessageId ?? 'default';
      groups.set(group, [...(groups.get(group) ?? []), message]);
    }
    await Promise.all([...groups.values()].map((group) => this.processGroup(queueUrl, group)));
  }

  private async processGroup(queueUrl: string, group: Message[]): Promise<void> {
    for (const [index, message] of group.entries()) {
      if (!this.running) {
        await this.releaseAll(queueUrl, group.slice(index));
        return;
      }
      if ((await this.process(queueUrl, message)) === 'retry-scheduled') {
        const delay = this.retryDelaySeconds(message);
        await Promise.all(group.slice(index + 1).map((rest) => this.changeVisibility(queueUrl, rest, delay)));
        return;
      }
    }
  }

  private async process(queueUrl: string, message: Message): Promise<MessageDisposition> {
    const decision = await this.handler.handle(message.Body);
    switch (decision.action) {
      case 'ack':
        if (decision.committed) {
          this.injectCrashAfterCommit();
        }
        await this.ack(queueUrl, message);
        return 'acked';
      case 'dead-letter':
        await this.deadLetter(queueUrl, message, decision.reason);
        return 'dead-lettered';
      case 'retry':
        await this.scheduleRetry(queueUrl, message, decision.reason);
        return 'retry-scheduled';
    }
  }

  private async scheduleRetry(queueUrl: string, message: Message, reason: string): Promise<void> {
    const receiveCount = receiveCountOf(message);
    const delay = this.retryDelaySeconds(message);
    this.metrics.retry('sqs_consumer');
    if (this.maxReceiveCount !== undefined && receiveCount >= this.maxReceiveCount) {
      this.metrics.deadLettered('max_receive_count');
    }
    this.logger.warn('sqs message failed transiently; retry scheduled', {
      sqsMessageId: message.MessageId,
      error: reason,
      receiveCount,
      retryInSeconds: delay,
    });
    await this.changeVisibility(queueUrl, message, delay);
  }

  private async deadLetter(queueUrl: string, message: Message, reason: string): Promise<void> {
    const truncatedReason = reason.slice(0, MAX_DEAD_LETTER_REASON_LENGTH);
    await this.queues.client.send(
      new SendMessageCommand({
        QueueUrl: await this.queues.url(this.options.dlqName),
        MessageBody: message.Body ?? '',
        MessageGroupId: message.Attributes?.MessageGroupId ?? 'dead-letter',
        MessageDeduplicationId: message.MessageId ?? `${Date.now()}`,
        MessageAttributes: {
          deadLetterReason: { DataType: 'String', StringValue: truncatedReason },
          originalMessageId: { DataType: 'String', StringValue: message.MessageId ?? 'unknown' },
        },
      }),
    );
    await this.ack(queueUrl, message);
    this.metrics.deadLettered('permanent');
    this.logger.error('sqs message moved to DLQ', { sqsMessageId: message.MessageId, reason: truncatedReason });
  }

  private async ack(queueUrl: string, message: Message): Promise<void> {
    await this.queues.client.send(
      new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }),
    );
  }

  private async changeVisibility(queueUrl: string, message: Message, seconds: number): Promise<void> {
    try {
      await this.queues.client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: seconds,
        }),
      );
    } catch (error) {
      this.logger.warn('could not change message visibility', {
        sqsMessageId: message.MessageId,
        reason: describe(error),
      });
    }
  }

  private async releaseAll(queueUrl: string, messages: Message[]): Promise<void> {
    await Promise.all(messages.map((message) => this.changeVisibility(queueUrl, message, 0)));
  }

  private retryDelaySeconds(message: Message): number {
    return Math.min(MAX_VISIBILITY_TIMEOUT_SECONDS, Math.round(this.backoff.delayFor(receiveCountOf(message))));
  }

  private async track<T>(work: Promise<T>): Promise<T> {
    this.inFlight.add(work);
    try {
      return await work;
    } finally {
      this.inFlight.delete(work);
    }
  }

  private injectCrashAfterCommit(): void {
    if (this.options.faultInjection === 'crash-after-commit') {
      this.logger.warn('fault injection: killing process after commit, before ack');
      process.kill(process.pid, 'SIGKILL');
    }
  }
}

function receiveCountOf(message: Message): number {
  return Number(message.Attributes?.ApproximateReceiveCount ?? '1');
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
