import { BeforeApplicationShutdown, OnApplicationBootstrap } from '@nestjs/common';
import { PublishOutboxUseCase } from '../../application/messaging/publish-outbox.use-case';
import { AppLogger } from '../../application/ports/system';
import { RetryPendingReferencesUseCase } from '../../application/wagering/retry-pending-references.use-case';
import { WagerTransactionsConsumer } from '../sqs/wager-transactions.consumer';
import { PollingWorker } from './polling-worker';

export interface PollingSettings {
  enabled: boolean;
  intervalMs: number;
  batchSize: number;
}

export interface BackgroundWorkersOptions {
  consumerEnabled: boolean;
  outboxRelay: PollingSettings;
  pendingReference: PollingSettings;
}

function outboxRelayWorker(useCase: PublishOutboxUseCase, settings: PollingSettings, logger: AppLogger) {
  return new PollingWorker(
    {
      name: 'outbox-relay',
      intervalMs: settings.intervalMs,
      tick: async () => {
        try {
          const result = await useCase.publishBatch(settings.batchSize);
          return result.claimed === settings.batchSize && result.failed === 0;
        } finally {
          await useCase.refreshBacklogMetrics();
        }
      },
    },
    logger,
  );
}

function pendingReferenceWorker(useCase: RetryPendingReferencesUseCase, settings: PollingSettings, logger: AppLogger) {
  return new PollingWorker(
    {
      name: 'pending-reference',
      intervalMs: settings.intervalMs,
      tick: async () => (await useCase.runOnce(settings.batchSize)).candidates === settings.batchSize,
    },
    logger,
  );
}

export class BackgroundWorkers implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly workers: PollingWorker[];

  constructor(
    private readonly consumer: WagerTransactionsConsumer,
    publishOutbox: PublishOutboxUseCase,
    retryPendingReferences: RetryPendingReferencesUseCase,
    private readonly options: BackgroundWorkersOptions,
    logger: AppLogger,
  ) {
    this.workers = [
      ...(options.outboxRelay.enabled ? [outboxRelayWorker(publishOutbox, options.outboxRelay, logger)] : []),
      ...(options.pendingReference.enabled
        ? [pendingReferenceWorker(retryPendingReferences, options.pendingReference, logger)]
        : []),
    ];
  }

  onApplicationBootstrap(): void {
    if (this.options.consumerEnabled) {
      this.consumer.start();
    }
    this.workers.forEach((worker) => worker.start());
  }

  async beforeApplicationShutdown(): Promise<void> {
    await Promise.all([this.consumer.stop(), ...this.workers.map((worker) => worker.stop())]);
  }
}
