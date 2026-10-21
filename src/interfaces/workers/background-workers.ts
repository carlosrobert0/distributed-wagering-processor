import { BeforeApplicationShutdown, OnApplicationBootstrap } from '@nestjs/common';
import { PublishOutboxUseCase } from '../../application/messaging/publish-outbox.use-case';
import { AppLogger } from '../../application/ports/system';
import { RetryPendingReferencesUseCase } from '../../application/wagering/retry-pending-references.use-case';
import { WagerTransactionsConsumer } from '../sqs/wager-transactions.consumer';
import { PollingWorker } from './polling-worker';

export interface BackgroundWorkersOptions {
  consumerEnabled: boolean;
  outboxRelay: { enabled: boolean; intervalMs: number; batchSize: number };
  pendingReference: { enabled: boolean; intervalMs: number; batchSize: number };
}

/**
 * Liga/desliga os processos em segundo plano junto com o ciclo de vida do Nest.
 *
 * `beforeApplicationShutdown` roda no SIGTERM (com `enableShutdownHooks`) ANTES de fechar o
 * HTTP e o pool do banco: consumer para de buscar e drena; workers terminam o ciclo atual.
 */
export class BackgroundWorkers implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly workers: PollingWorker[] = [];

  constructor(
    private readonly consumer: WagerTransactionsConsumer,
    publishOutbox: PublishOutboxUseCase,
    retryPendingReferences: RetryPendingReferencesUseCase,
    private readonly options: BackgroundWorkersOptions,
    logger: AppLogger,
  ) {
    if (options.outboxRelay.enabled) {
      this.workers.push(
        new PollingWorker(
          {
            name: 'outbox-relay',
            intervalMs: options.outboxRelay.intervalMs,
            tick: async () => {
              const result = await publishOutbox.publishBatch(options.outboxRelay.batchSize);
              await publishOutbox.refreshBacklogMetrics();
              // Lote cheio e sem falhas → provavelmente há mais; não espera o intervalo.
              return result.claimed === options.outboxRelay.batchSize && result.failed === 0;
            },
          },
          logger,
        ),
      );
    }
    if (options.pendingReference.enabled) {
      this.workers.push(
        new PollingWorker(
          {
            name: 'pending-reference',
            intervalMs: options.pendingReference.intervalMs,
            tick: async () => {
              const result = await retryPendingReferences.runOnce(options.pendingReference.batchSize);
              return result.candidates === options.pendingReference.batchSize;
            },
          },
          logger,
        ),
      );
    }
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
