import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import pino from 'pino';
import { PublishOutboxUseCase } from '../../src/application/messaging/publish-outbox.use-case';
import { EventPublisher } from '../../src/application/ports/system';
import { OutboxMessage } from '../../src/domain/messaging/outbox-message';
import { createSqsClient, SqsQueues } from '../../src/infrastructure/messaging/sqs';
import { SqsEventPublisher } from '../../src/infrastructure/messaging/sqs-event-publisher';
import { PinoAppLogger } from '../../src/infrastructure/observability/pino-logger';
import { PrometheusMetrics } from '../../src/infrastructure/observability/prometheus-metrics';
import { OutboxMessageMapper } from '../../src/infrastructure/persistence/mappers';
import { OutboxMessageRecord } from '../../src/infrastructure/persistence/records';
import { SystemClock } from '../../src/infrastructure/system/system-adapters';
import { AWS_ENDPOINT_URL, prepareTestDatabase } from '../support/environment';
import { countRows, db } from '../support/database';
import { createTestQueues, drainMessages } from '../support/sqs';
import { buildUseCases } from '../support/use-cases';

/**
 * §11 Transactional Outbox com SQS real: publishers concorrentes, morte entre commit e
 * publicação, falha de publicação com retry, e duplicata segura.
 */
describe('outbox relay', () => {
  let queues: Awaited<ReturnType<typeof createTestQueues>>;
  let instanceA: Awaited<ReturnType<typeof buildUseCases>>;
  let instanceB: Awaited<ReturnType<typeof buildUseCases>>;
  const logger = new PinoAppLogger(pino({ level: 'silent' }));
  const clock = new SystemClock();
  const sqsQueues = new SqsQueues(
    createSqsClient({ region: 'us-east-1', endpoint: AWS_ENDPOINT_URL, accessKeyId: 'test', secretAccessKey: 'test' }),
  );

  beforeAll(async () => {
    await prepareTestDatabase();
    queues = await createTestQueues();
    // Duas "instâncias": ORMs e pools de conexão independentes.
    instanceA = await buildUseCases();
    instanceB = await buildUseCases();
  });

  afterAll(async () => {
    await instanceA.close();
    await instanceB.close();
    await queues.cleanup();
  });

  function relay(instance: typeof instanceA, publisher: EventPublisher = new SqsEventPublisher(sqsQueues, queues.events)) {
    return new PublishOutboxUseCase(instance.uow, publisher, clock, new PrometheusMetrics({ collectDefaults: false }), logger);
  }

  /** Gera transações (e portanto eventos na outbox) sem nenhum relay rodando. */
  async function produceEvents(wallets: number, betsPerWallet: number): Promise<string[]> {
    const walletIds: string[] = [];
    for (let w = 0; w < wallets; w++) {
      const wallet = await instanceA.createWallet.execute(
        { playerId: randomUUID(), initialBalance: { amount: '100.00', currency: 'BRL' } },
        { correlationId: 'outbox-test' },
      );
      walletIds.push(wallet.id);
      for (let b = 0; b < betsPerWallet; b++) {
        const externalTransactionId = `tx-${randomUUID()}`;
        await instanceA.processWager().execute(
          {
            idempotencyKey: `provider-a:${externalTransactionId}`,
            providerId: 'provider-a',
            externalTransactionId,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: 'r',
            gameId: 'g',
            kind: 'BET',
            money: { amount: '1.00', currency: 'BRL' },
          },
          { correlationId: 'outbox-test', source: 'http' },
        );
      }
    }
    return walletIds;
  }

  async function pendingFor(walletIds: string[]): Promise<number> {
    return countRows(`SELECT COUNT(*) FROM outbox_messages WHERE partition_key = ANY($1) AND published_at IS NULL`, [walletIds]);
  }

  test('commit → "processo morre" antes de publicar → outra instância publica tudo (AC-OUT-03)', async () => {
    const walletIds = await produceEvents(2, 2);
    // Nenhum relay rodou: o processo "morreu" depois do commit. Os eventos estão seguros na tabela.
    const pending = await pendingFor(walletIds);
    expect(pending).toBe(2 * (2 + 2 * 2)); // por wallet: abertura (2 eventos) + 2 BETs × 2 eventos

    // Outra instância assume.
    let published = 0;
    // Eventos mais antigos (de outros arquivos de teste) saem antes — a ordem é por occurred_at.
    for (let i = 0; i < 200 && (await pendingFor(walletIds)) > 0; i++) {
      published += (await relay(instanceB).publishBatch(100)).published;
    }
    expect(await pendingFor(walletIds)).toBe(0);
    expect(published).toBeGreaterThanOrEqual(pending);

    const delivered = (await drainMessages(queues.events)).map((m) => JSON.parse(m.Body!) as { eventId: string; eventType: string });
    const ours = delivered.filter((event) => walletIds.includes((event as { data?: { walletId?: string } }).data?.walletId ?? ''));
    expect(new Set(ours.map((event) => event.eventId)).size).toBe(ours.length);
    expect(ours.length).toBe(pending);
  });

  test('dois publishers concorrentes sobre a mesma outbox: cada evento publicado exatamente uma vez (AC-OUT-02)', async () => {
    const walletIds = await produceEvents(5, 4);
    const expected = await pendingFor(walletIds);

    const published: string[] = [];
    const recording = (inner: EventPublisher): EventPublisher => ({
      publish: async (message: OutboxMessage) => {
        await inner.publish(message);
        published.push(message.id);
      },
    });
    const sqsPublisher = new SqsEventPublisher(sqsQueues, queues.events);

    // Os dois relays rodam em paralelo, com lotes pequenos para forçar disputa pelas linhas.
    const runRelay = async (instance: typeof instanceA) => {
      const useCase = relay(instance, recording(sqsPublisher));
      for (let i = 0; i < 500; i++) {
        const result = await useCase.publishBatch(3);
        if (result.claimed === 0 && (await pendingFor(walletIds)) === 0) return;
      }
    };
    await Promise.all([runRelay(instanceA), runRelay(instanceB)]);

    expect(await pendingFor(walletIds)).toBe(0);
    const ours = await db.query<{ id: string }>(`SELECT id FROM outbox_messages WHERE partition_key = ANY($1)`, [walletIds]);
    const ourIds = new Set(ours.rows.map((row) => row.id));
    const publishedOurs = published.filter((id) => ourIds.has(id));
    // SKIP LOCKED: nenhum evento foi publicado duas vezes pelos dois relays.
    expect(publishedOurs.length).toBe(expected);
    expect(new Set(publishedOurs).size).toBe(expected);
    await drainMessages(queues.events);
  });

  test('falha de publicação → attempts++ e backoff; nada é perdido (AC-OUT-07)', async () => {
    const walletIds = await produceEvents(1, 0);
    let failures = 0;
    const flaky: EventPublisher = {
      publish: async (message) => {
        if (failures < 2) {
          failures += 1;
          throw new Error('SQS unavailable');
        }
        await new SqsEventPublisher(sqsQueues, queues.events).publish(message);
      },
    };

    const useCase = relay(instanceA, flaky);
    const first = await useCase.publishBatch(50);
    expect(first.failed).toBeGreaterThan(0);

    const { rows } = await db.query<{ attempts: number; last_error: string | null; due_in_future: boolean }>(
      `SELECT attempts, last_error, next_attempt_at > now() AS due_in_future FROM outbox_messages
        WHERE partition_key = ANY($1) AND published_at IS NULL`,
      [walletIds],
    );
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((row) => row.attempts >= 1 && row.last_error === 'SQS unavailable' && row.due_in_future)).toBe(true);

    // Força o vencimento e publica de novo.
    await db.query(`UPDATE outbox_messages SET next_attempt_at = now() WHERE partition_key = ANY($1) AND published_at IS NULL`, [walletIds]);
    for (let i = 0; i < 5 && (await pendingFor(walletIds)) > 0; i++) {
      await useCase.publishBatch(50);
      await db.query(`UPDATE outbox_messages SET next_attempt_at = now() WHERE partition_key = ANY($1) AND published_at IS NULL`, [walletIds]);
    }
    expect(await pendingFor(walletIds)).toBe(0);
    await drainMessages(queues.events);
  });

  test('relay que morre com o lote reivindicado (rollback) não perde eventos', async () => {
    const walletIds = await produceEvents(1, 1);
    const crashing: EventPublisher = {
      publish: async () => {
        throw Object.assign(new Error('process killed mid-batch'), { fatal: true });
      },
    };
    // Simula a morte: o callback lança DENTRO da transação de claim → rollback, locks liberados.
    await expect(
      instanceA.uow.run(async (scope) => {
        const claimed = await scope.outbox.claimDue(new Date(), 50);
        expect(claimed.length).toBeGreaterThan(0);
        await crashing.publish(claimed[0]!);
      }),
    ).rejects.toThrow('process killed');

    // Linhas continuam pendentes, sem tentativa contabilizada, e outra instância publica.
    expect(await pendingFor(walletIds)).toBeGreaterThan(0);
    for (let i = 0; i < 5 && (await pendingFor(walletIds)) > 0; i++) {
      await relay(instanceB).publishBatch(50);
    }
    expect(await pendingFor(walletIds)).toBe(0);
    await drainMessages(queues.events);
  });

  test('publicação duplicada é segura: mesmo eventId, deduplicado pela fila FIFO (AC-OUT-04)', async () => {
    const walletIds = await produceEvents(1, 0);
    const { rows } = await db.query<{ id: string }>(
      `SELECT id FROM outbox_messages WHERE partition_key = $1 LIMIT 1`,
      [walletIds[0]],
    );
    const stored = await instanceA.orm.em.fork().findOne(OutboxMessageRecord, { id: rows[0]!.id });
    expect(stored).not.toBeNull();
    const record = OutboxMessageMapper.toDomain(stored!);

    const publisher = new SqsEventPublisher(sqsQueues, queues.events);
    await publisher.publish(record);
    await publisher.publish(record); // "republicação" após crash

    const delivered = (await drainMessages(queues.events)).filter(
      (message) => (JSON.parse(message.Body!) as { eventId: string }).eventId === record.id,
    );
    expect(delivered).toHaveLength(1);
  });
});
