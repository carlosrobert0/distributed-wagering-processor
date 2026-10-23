import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { DEFAULT_LEASE_POLICY, LeasePolicy } from '../lease-policy';
import { AppLogger, Clock, EventPublisher, Metrics } from '../ports/system';
import { UnitOfWork } from '../ports/unit-of-work';

export interface OutboxRelayResult {
  claimed: number;
  published: number;
  failed: number;
}

export class PublishOutboxUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly publisher: EventPublisher,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
    private readonly lease: LeasePolicy = DEFAULT_LEASE_POLICY,
  ) {}

  async publishBatch(batchSize: number): Promise<OutboxRelayResult> {
    const leaseUntil = this.lease.expiresAt(this.clock.now());
    const leased = await this.claimAndLease(batchSize, leaseUntil);
    const result: OutboxRelayResult = { claimed: leased.length, published: 0, failed: 0 };

    for (const [index, message] of leased.entries()) {
      const remaining = leased.slice(index + 1);
      if (!this.leaseStillValid(leaseUntil)) {
        await this.releaseAll([message, ...remaining], this.clock.now(), leaseUntil);
        break;
      }
      const published = await this.publish(message);
      if (!(await this.save(message, published ? undefined : leaseUntil))) {
        break;
      }
      if (!published) {
        result.failed += 1;
        await this.releaseAll(remaining, message.nextAttemptAt ?? this.clock.now(), leaseUntil);
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

  private claimAndLease(batchSize: number, leaseUntil: Date): Promise<OutboxMessage[]> {
    return this.uow.run(async (scope) => {
      const messages = await scope.outbox.claimDue(this.clock.now(), batchSize);
      for (const message of messages) {
        message.lease(leaseUntil);
        await scope.outbox.save(message);
      }
      return messages;
    });
  }

  private leaseStillValid(leaseUntil: Date): boolean {
    return this.lease.isStillValid(this.clock.now(), leaseUntil);
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

  private async releaseAll(messages: OutboxMessage[], dueAt: Date, heldLease: Date): Promise<void> {
    if (messages.length === 0) return;
    messages.forEach((message) => message.release(dueAt));
    try {
      await this.uow.run(async (scope) => {
        for (const message of messages) {
          await scope.outbox.save(message, heldLease);
        }
      });
    } catch (error) {
      this.logger.warn('could not release outbox lease; it will expire on its own', { count: messages.length }, error);
    }
  }

  private async save(message: OutboxMessage, heldLease?: Date): Promise<boolean> {
    try {
      await this.uow.run((scope) => scope.outbox.save(message, heldLease));
      return true;
    } catch (error) {
      this.logger.error(
        'could not record outbox publication; the lease will expire and the event may be republished',
        { eventId: message.id, walletId: message.partitionKey },
        error,
      );
      return false;
    }
  }
}
