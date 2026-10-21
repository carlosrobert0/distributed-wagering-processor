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
  ) {}

  async publishBatch(batchSize: number): Promise<OutboxRelayResult> {
    const result = await this.uow.run(async (scope) => {
      const messages = await scope.outbox.claimDue(this.clock.now(), batchSize);
      const summary: OutboxRelayResult = { claimed: messages.length, published: 0, failed: 0 };

      for (const message of messages) {
        try {
          await this.publisher.publish(message);
          message.markPublished(this.clock.now());
          summary.published += 1;
        } catch (error) {
          message.scheduleRetry(this.clock.now(), error instanceof Error ? error.message : String(error));
          summary.failed += 1;
          this.metrics.retry('outbox_relay');
          this.logger.warn('outbox publish failed; retry scheduled', {
            eventId: message.id,
            eventType: message.eventType,
            attempts: message.attempts,
            nextAttemptAt: message.nextAttemptAt?.toISOString(),
          });
        }
        await scope.outbox.save(message);
      }
      return summary;
    });

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
}
