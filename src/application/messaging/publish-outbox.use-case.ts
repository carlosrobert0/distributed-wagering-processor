import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { AppLogger, Clock, EventPublisher, Metrics } from '../ports/system';
import { UnitOfWork } from '../ports/unit-of-work';

export interface OutboxRelayResult {
  claimed: number;
  published: number;
  failed: number;
}

export const OUTBOX_LEASE_MS = 60_000;

export class PublishOutboxUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly publisher: EventPublisher,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async publishBatch(batchSize: number): Promise<OutboxRelayResult> {
    const leased = await this.claimAndLease(batchSize);
    const result: OutboxRelayResult = { claimed: leased.length, published: 0, failed: 0 };

    for (const [index, message] of leased.entries()) {
      const published = await this.publish(message);
      await this.save(message);
      if (!published) {
        result.failed += 1;
        await this.releaseAll(leased.slice(index + 1));
        break;
      }
      result.published += 1;
    }

    if (result.published > 0) {
      this.metrics.outboxPublished(result.published);
    }
    return result;
  }

  async refreshBacklogMetrics(): Promise<void> {
    const stats = await this.uow.run((scope) => scope.outbox.stats());
    const lagSeconds = stats.oldestPendingOccurredAt
      ? Math.max(0, (this.clock.now().getTime() - stats.oldestPendingOccurredAt.getTime()) / 1000)
      : 0;
    this.metrics.setOutboxBacklog(stats.pending, lagSeconds);
  }

  private claimAndLease(batchSize: number): Promise<OutboxMessage[]> {
    return this.uow.run(async (scope) => {
      const now = this.clock.now();
      const messages = await scope.outbox.claimDue(now, batchSize);
      for (const message of messages) {
        message.lease(new Date(now.getTime() + OUTBOX_LEASE_MS));
        await scope.outbox.save(message);
      }
      return messages;
    });
  }

  private async publish(message: OutboxMessage): Promise<boolean> {
    try {
      await this.publisher.publish(message);
      message.markPublished(this.clock.now());
      return true;
    } catch (error) {
      message.scheduleRetry(this.clock.now(), error instanceof Error ? error.message : String(error));
      this.metrics.retry('outbox_relay');
      this.logger.warn(
        'outbox publish failed; retry scheduled',
        {
          eventId: message.id,
          eventType: message.eventType,
          walletId: message.partitionKey,
          attempts: message.attempts,
          nextAttemptAt: message.nextAttemptAt?.toISOString(),
        },
        error,
      );
      return false;
    }
  }

  private async releaseAll(messages: OutboxMessage[]): Promise<void> {
    if (messages.length === 0) return;
    const now = this.clock.now();
    messages.forEach((message) => message.release(now));
    await this.uow.run(async (scope) => {
      for (const message of messages) {
        await scope.outbox.save(message);
      }
    });
  }

  private save(message: OutboxMessage): Promise<void> {
    return this.uow.run((scope) => scope.outbox.save(message));
  }
}
