import { describe, expect, mock, test } from 'bun:test';
import { PublishOutboxUseCase } from '../../../src/application/messaging/publish-outbox.use-case';
import { RetryPendingReferencesUseCase } from '../../../src/application/wagering/retry-pending-references.use-case';
import { BackgroundWorkers, BackgroundWorkersOptions } from '../../../src/interfaces/workers/background-workers';
import { WagerTransactionsConsumer } from '../../../src/interfaces/sqs/wager-transactions.consumer';
import { eventually } from '../../support/eventually';
import { RecordingLogger } from '../../support/in-memory';

const ioDelay = () => new Promise((resolve) => setTimeout(resolve, 1));

const baseOptions: BackgroundWorkersOptions = {
  consumerEnabled: true,
  outboxRelay: { enabled: true, intervalMs: 1_000, batchSize: 50 },
  pendingReference: { enabled: true, intervalMs: 1_000, batchSize: 20 },
};

function build(
  options: Partial<BackgroundWorkersOptions> = {},
  fakes: {
    publishBatch?: () => Promise<unknown>;
    refresh?: () => Promise<void>;
    runOnce?: () => Promise<unknown>;
  } = {},
) {
  const consumer = { start: mock(() => undefined), stop: mock(async () => undefined) };
  const publishBatch = mock(fakes.publishBatch ?? (async () => ({ claimed: 0, published: 0, failed: 0 })));
  const refresh = mock(fakes.refresh ?? (async () => undefined));
  const runOnce = mock(fakes.runOnce ?? (async () => ({ candidates: 0, settled: 0, stillPending: 0, failed: 0 })));
  const logger = new RecordingLogger();
  const workers = new BackgroundWorkers(
    consumer as unknown as WagerTransactionsConsumer,
    { publishBatch, refreshBacklogMetrics: refresh } as unknown as PublishOutboxUseCase,
    { runOnce } as unknown as RetryPendingReferencesUseCase,
    { ...baseOptions, ...options },
    logger,
  );
  return { workers, consumer, publishBatch, refresh, runOnce, logger };
}

describe('BackgroundWorkers', () => {
  test('sobe consumer e workers no bootstrap e para tudo no shutdown', async () => {
    const { workers, consumer, publishBatch, runOnce } = build();
    workers.onApplicationBootstrap();
    await eventually(() => publishBatch.mock.calls.length > 0 && runOnce.mock.calls.length > 0);

    await workers.beforeApplicationShutdown();

    expect(consumer.start).toHaveBeenCalledTimes(1);
    expect(consumer.stop).toHaveBeenCalledTimes(1);
    const callsAfterStop = publishBatch.mock.calls.length;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(publishBatch.mock.calls.length).toBe(callsAfterStop);
  });

  test('componentes desligados por configuração não sobem', async () => {
    const { workers, consumer, publishBatch, runOnce } = build({
      consumerEnabled: false,
      outboxRelay: { enabled: false, intervalMs: 1_000, batchSize: 50 },
      pendingReference: { enabled: false, intervalMs: 1_000, batchSize: 20 },
    });
    workers.onApplicationBootstrap();
    await new Promise((resolve) => setTimeout(resolve, 20));
    await workers.beforeApplicationShutdown();

    expect(consumer.start).not.toHaveBeenCalled();
    expect(publishBatch).not.toHaveBeenCalled();
    expect(runOnce).not.toHaveBeenCalled();
  });

  test('relay: lote cheio e sem falha encadeia o próximo ciclo sem esperar o intervalo', async () => {
    const { workers, publishBatch } = build(
      {},
      {
        publishBatch: async () => {
          await ioDelay();
          return { claimed: 50, published: 50, failed: 0 };
        },
      },
    );
    workers.onApplicationBootstrap();
    await eventually(() => publishBatch.mock.calls.length >= 5, { description: 'back-to-back ticks' });
    await workers.beforeApplicationShutdown();
  });

  test('relay: lote incompleto, ou com falha, espera o intervalo', async () => {
    const { workers, publishBatch } = build(
      {},
      { publishBatch: async () => ({ claimed: 50, published: 49, failed: 1 }) },
    );
    workers.onApplicationBootstrap();
    await new Promise((resolve) => setTimeout(resolve, 30));
    await workers.beforeApplicationShutdown();
    expect(publishBatch.mock.calls.length).toBe(1);
  });

  test('relay: o backlog é atualizado mesmo quando a publicação falha, e falha da métrica não derruba o ciclo', async () => {
    const { workers, refresh, logger } = build(
      {},
      {
        publishBatch: async () => {
          throw new Error('sqs down');
        },
        refresh: async () => {
          throw new Error('db down');
        },
      },
    );
    workers.onApplicationBootstrap();
    await eventually(() => refresh.mock.calls.length > 0, { description: 'backlog refreshed' });
    await workers.beforeApplicationShutdown();

    expect(logger.has('warn', 'could not refresh outbox backlog metrics')).toBe(true);
    expect(logger.has('error', 'worker tick failed')).toBe(true);
  });

  test('worker de referências: lote cheio encadeia o próximo ciclo', async () => {
    const { workers, runOnce } = build(
      {},
      {
        runOnce: async () => {
          await ioDelay();
          return { candidates: 20, settled: 20, stillPending: 0, failed: 0 };
        },
      },
    );
    workers.onApplicationBootstrap();
    await eventually(() => runOnce.mock.calls.length >= 5, { description: 'back-to-back ticks' });
    await workers.beforeApplicationShutdown();
  });
});
