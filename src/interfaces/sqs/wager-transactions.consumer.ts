import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  Message,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { AppLogger, Clock, DeliveryAttempts, LogFields, Metrics } from '../../application/ports/system';
import { ExponentialBackoff } from '../../domain/shared/exponential-backoff';
import { SQS_REQUEST_TIMEOUT_MS, SqsQueues } from '../../infrastructure/messaging/sqs';
import { MessageContext, WagerMessageHandler } from './wager-message.handler';

export interface WagerConsumerOptions {
  consumerName: string;
  queueName: string;
  dlqName: string;
  waitTimeSeconds: number;
  visibilityTimeoutSeconds: number;
  batchSize: number;
  maxAttempts: number;
  retryBaseSeconds: number;
  retryMaxSeconds: number;
  shutdownGraceMs: number;
  faultInjection: 'none' | 'crash-after-commit';
  pollFailureBackoffMs?: number;
  heartbeatIntervalMs?: number;
}

export type MessageDisposition = 'acked' | 'retry-scheduled' | 'dead-lettered';

const MAX_VISIBILITY_TIMEOUT_SECONDS = 43_200;
const DEFAULT_POLL_FAILURE_BACKOFF_MS = 1_000;
const MIN_HEARTBEAT_INTERVAL_MS = 1_000;
const MAX_DEAD_LETTER_REASON_LENGTH = 1_000;

interface InFlightMessage {
  queueUrl: string;
  message: Message;
}

export class WagerTransactionsConsumer {
  private running = false;
  private loop?: Promise<void>;
  private pollAbort = new AbortController();
  private readonly inFlight = new Map<string, InFlightMessage>();
  private readonly retryBackoff: ExponentialBackoff;

  constructor(
    private readonly handler: WagerMessageHandler,
    private readonly queues: SqsQueues,
    private readonly options: WagerConsumerOptions,
    private readonly attempts: DeliveryAttempts,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {
    const base = Math.max(1, options.retryBaseSeconds);
    this.retryBackoff = new ExponentialBackoff(base, Math.max(base, options.retryMaxSeconds));
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

    let graceTimer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<false>((resolve) => {
      graceTimer = setTimeout(() => resolve(false), this.options.shutdownGraceMs);
    });
    const drained = (this.loop ?? Promise.resolve()).then(() => true as const);
    const drainedInTime = await Promise.race([drained, timedOut]);
    clearTimeout(graceTimer);

    if (!drainedInTime) {
      await this.releaseAll([...this.inFlight.values()]);
    }
    this.logger.info('sqs consumer stopped', { drained: drainedInTime });
  }

  private async pollLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.pollOnce();
      } catch (error) {
        if (!this.running) break;
        this.logger.warn('sqs poll failed; backing off', {}, error);
        await sleep(this.options.pollFailureBackoffMs ?? DEFAULT_POLL_FAILURE_BACKOFF_MS);
      }
    }
  }

  private async pollOnce(): Promise<void> {
    const queueUrl = await this.queues.url(this.options.queueName);
    const response = await this.queues.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: this.options.batchSize,
        WaitTimeSeconds: this.options.waitTimeSeconds,
        VisibilityTimeout: this.options.visibilityTimeoutSeconds,
        MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
        AttributeNames: ['All'],
      }),
      {
        abortSignal: this.pollAbort.signal,
        requestTimeout: this.options.waitTimeSeconds * 1_000 + SQS_REQUEST_TIMEOUT_MS,
      },
    );
    const received = (response.Messages ?? []).map((message) => ({ queueUrl, message }));
    if (!this.running) {
      await this.releaseAll(received);
      return;
    }
    if (received.length > 0) {
      await this.processBatch(received);
    }
  }

  private async processBatch(batch: InFlightMessage[]): Promise<void> {
    batch.forEach((entry) => this.inFlight.set(handleOf(entry.message), entry));
    const heartbeat = this.startVisibilityHeartbeat();
    try {
      const groups = new Map<string, InFlightMessage[]>();
      for (const entry of batch) {
        const group = entry.message.Attributes?.MessageGroupId ?? entry.message.MessageId ?? 'default';
        groups.set(group, [...(groups.get(group) ?? []), entry]);
      }
      await Promise.allSettled([...groups.values()].map((group) => this.processGroup(group)));
    } finally {
      clearInterval(heartbeat);
      batch.forEach((entry) => this.inFlight.delete(handleOf(entry.message)));
    }
  }

  private async processGroup(group: InFlightMessage[]): Promise<void> {
    for (const [index, entry] of group.entries()) {
      if (!this.running) {
        await this.releaseAll(group.slice(index));
        return;
      }
      const disposition = await this.process(entry).catch((error: unknown) => {
        this.logger.error('sqs message handling failed', { sqsMessageId: entry.message.MessageId }, error);
        return 'retry-scheduled' as const;
      });
      if (disposition === 'retry-scheduled') {
        const rest = group.slice(index + 1);
        rest.forEach((sibling) => this.finalize(sibling));
        const delay = this.retryDelaySeconds(receiveCountOf(entry.message));
        await Promise.all(rest.map((sibling) => this.changeVisibility(sibling, delay)));
        return;
      }
    }
  }

  private async process(entry: InFlightMessage): Promise<MessageDisposition> {
    const decision = await this.handler.handle(entry.message.Body).finally(() => this.finalize(entry));
    switch (decision.action) {
      case 'ack':
        if (decision.committed) {
          this.injectCrashAfterCommit();
        }
        await this.ack(entry);
        await this.clearAttempts(decision.context);
        return 'acked';
      case 'dead-letter':
        await this.deadLetter(entry, decision.reason, decision.context, 'permanent');
        await this.clearAttempts(decision.context);
        return 'dead-lettered';
      case 'retry':
        return this.scheduleRetry(entry, decision.reason, decision.context);
    }
  }

  private async scheduleRetry(
    entry: InFlightMessage,
    reason: string,
    context: MessageContext,
  ): Promise<MessageDisposition> {
    const attempt = await this.countAttempt(context, reason);
    if (attempt !== undefined && attempt >= this.options.maxAttempts) {
      await this.deadLetter(entry, `MAX_ATTEMPTS_EXCEEDED after ${attempt}: ${reason}`, context, 'max_attempts');
      await this.clearAttempts(context);
      return 'dead-lettered';
    }
    const delay = this.retryDelaySeconds(attempt ?? receiveCountOf(entry.message));
    this.metrics.retry('sqs_consumer');
    this.logger.warn('sqs message retry scheduled', {
      ...logFields(entry, context),
      error: reason,
      attempt,
      retryInSeconds: delay,
    });
    await this.changeVisibility(entry, delay);
    return 'retry-scheduled';
  }

  private async deadLetter(
    entry: InFlightMessage,
    reason: string,
    context: MessageContext,
    kind: 'permanent' | 'max_attempts',
  ): Promise<void> {
    const truncatedReason = reason.slice(0, MAX_DEAD_LETTER_REASON_LENGTH);
    await this.queues.client.send(
      new SendMessageCommand({
        QueueUrl: await this.queues.url(this.options.dlqName),
        MessageBody: entry.message.Body ?? '',
        MessageGroupId: entry.message.Attributes?.MessageGroupId ?? 'dead-letter',
        MessageDeduplicationId: entry.message.MessageId ?? `${Date.now()}`,
        MessageAttributes: {
          deadLetterReason: { DataType: 'String', StringValue: truncatedReason },
          originalMessageId: { DataType: 'String', StringValue: entry.message.MessageId ?? 'unknown' },
        },
      }),
    );
    await this.ack(entry);
    this.metrics.deadLettered(kind);
    this.logger.error('sqs message moved to DLQ', { ...logFields(entry, context), reason: truncatedReason });
  }

  private async countAttempt(context: MessageContext, reason: string): Promise<number | undefined> {
    if (!context.messageId) {
      return undefined;
    }
    try {
      return await this.attempts.recordFailure(this.options.consumerName, context.messageId, reason, this.clock.now());
    } catch (error) {
      this.logger.warn('could not record delivery attempt; relying on the redrive policy', { ...context }, error);
      return undefined;
    }
  }

  private async clearAttempts(context: MessageContext): Promise<void> {
    if (!context.messageId) {
      return;
    }
    await this.attempts.clear(this.options.consumerName, context.messageId).catch((error: unknown) => {
      this.logger.warn('could not clear delivery attempts', { ...context }, error);
    });
  }

  private finalize(entry: InFlightMessage): void {
    this.inFlight.delete(handleOf(entry.message));
  }

  private async ack(entry: InFlightMessage): Promise<void> {
    try {
      await this.queues.client.send(
        new DeleteMessageCommand({ QueueUrl: entry.queueUrl, ReceiptHandle: entry.message.ReceiptHandle }),
      );
    } catch (error) {
      this.logger.warn('sqs ack failed; message will be redelivered and deduplicated', logFields(entry, {}), error);
    }
  }

  private startVisibilityHeartbeat(): ReturnType<typeof setInterval> {
    const extendEveryMs =
      this.options.heartbeatIntervalMs ??
      Math.max(MIN_HEARTBEAT_INTERVAL_MS, (this.options.visibilityTimeoutSeconds * 1_000) / 2);
    return setInterval(() => {
      for (const entry of this.inFlight.values()) {
        void this.changeVisibility(entry, this.options.visibilityTimeoutSeconds);
      }
    }, extendEveryMs);
  }

  private async changeVisibility(entry: InFlightMessage, seconds: number): Promise<void> {
    try {
      await this.queues.client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: entry.queueUrl,
          ReceiptHandle: entry.message.ReceiptHandle,
          VisibilityTimeout: seconds,
        }),
      );
    } catch (error) {
      this.logger.warn('could not change message visibility', logFields(entry, {}), error);
    }
  }

  private async releaseAll(entries: InFlightMessage[]): Promise<void> {
    entries.forEach((entry) => this.finalize(entry));
    await Promise.all(entries.map((entry) => this.changeVisibility(entry, 0)));
  }

  private retryDelaySeconds(attempt: number): number {
    return Math.min(MAX_VISIBILITY_TIMEOUT_SECONDS, Math.round(this.retryBackoff.delayFor(attempt)));
  }

  private injectCrashAfterCommit(): void {
    if (this.options.faultInjection === 'crash-after-commit') {
      this.logger.warn('fault injection: killing process after commit, before ack');
      process.kill(process.pid, 'SIGKILL');
    }
  }
}

function handleOf(message: Message): string {
  return message.ReceiptHandle ?? message.MessageId ?? '';
}

function receiveCountOf(message: Message): number {
  return Number(message.Attributes?.ApproximateReceiveCount ?? '1');
}

function logFields(entry: InFlightMessage, context: MessageContext): LogFields {
  return { ...context, sqsMessageId: entry.message.MessageId };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
