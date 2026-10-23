import { beforeEach, describe, expect, test } from 'bun:test';
import { TransientInfrastructureError } from '../../../src/application/errors';
import { TransactionScope } from '../../../src/application/ports/unit-of-work';
import { FailureCode } from '../../../src/domain/wagering/failure-code';
import { WagerTransactionStatus } from '../../../src/domain/wagering/wager-transaction';
import { buildInMemoryContext, HTTP, InMemoryContext } from '../../support/in-memory-context';

describe('RetryPendingReferencesUseCase', () => {
  let ctx: InMemoryContext;

  beforeEach(() => {
    ctx = buildInMemoryContext();
  });

  async function pendingRefund(reference: string) {
    const wallet = await ctx.openWallet('100.00');
    const refund = await ctx.processWager.execute(
      ctx.command(wallet, { kind: 'REFUND', referenceExternalTransactionId: reference }),
      HTTP,
    );
    expect(refund.status).toBe(WagerTransactionStatus.PendingReference);
    return { wallet, refund };
  }

  test('nada vencido → nenhum candidato', async () => {
    await pendingRefund('bet-1');
    expect(await ctx.retryPending.runOnce(10)).toEqual({ candidates: 0, settled: 0, stillPending: 0, failed: 0 });
  });

  test('referência chega → a BET acorda a dependente e o worker aplica o REFUND', async () => {
    const { wallet, refund } = await pendingRefund('bet-1');
    await ctx.processWager.execute(ctx.command(wallet, { externalTransactionId: 'bet-1' }), HTTP);

    const result = await ctx.retryPending.runOnce(10);

    expect(result).toMatchObject({ candidates: 1, settled: 1 });
    expect(ctx.uow.store.transactions.get(refund.transactionId)!.status).toBe(WagerTransactionStatus.Processed);
    expect(ctx.uow.store.wallets.get(wallet.id)!.balance.toJSON().amount).toBe('100.00');
  });

  test('referência ausente: reagenda com backoff e rejeita ao esgotar as tentativas', async () => {
    const { refund } = await pendingRefund('never');

    ctx.clock.advance(1_000);
    expect(await ctx.retryPending.runOnce(10)).toMatchObject({ stillPending: 1 });
    ctx.clock.advance(2_000);
    expect(await ctx.retryPending.runOnce(10)).toMatchObject({ stillPending: 1 });
    ctx.clock.advance(4_000);
    expect(await ctx.retryPending.runOnce(10)).toMatchObject({ settled: 1 });

    const rejected = ctx.uow.store.transactions.get(refund.transactionId)!;
    expect(rejected.status).toBe(WagerTransactionStatus.Rejected);
    expect(rejected.failureCode).toBe(FailureCode.ReferenceNotFound);
    expect(ctx.uow.store.outboxTypesFor(refund.transactionId)).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionRejected',
    ]);
  });

  test('erro transitório mantém a transação pendente para o próximo ciclo', async () => {
    const { refund } = await pendingRefund('bet-x');
    ctx.clock.advance(1_000);
    const run = ctx.uow.run.bind(ctx.uow);
    let calls = 0;
    ctx.uow.run = async <T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> => {
      calls += 1;
      if (calls === 2) throw new TransientInfrastructureError('db down', 'connection');
      return run(work);
    };

    expect(await ctx.retryPending.runOnce(10)).toMatchObject({ candidates: 1, failed: 0 });
    expect(ctx.uow.store.transactions.get(refund.transactionId)!.status).toBe(WagerTransactionStatus.PendingReference);
    expect(ctx.metrics.count('retry:pending_reference')).toBe(1);
  });

  test('erro permanente marca a transação como FAILED (auditável)', async () => {
    const { refund } = await pendingRefund('bet-y');
    ctx.clock.advance(1_000);
    const run = ctx.uow.run.bind(ctx.uow);
    let calls = 0;
    ctx.uow.run = async <T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> => {
      calls += 1;
      if (calls === 2) throw new Error('corrupted row');
      return run(work);
    };

    expect(await ctx.retryPending.runOnce(10)).toMatchObject({ failed: 1 });
    const failed = ctx.uow.store.transactions.get(refund.transactionId)!;
    expect(failed.status).toBe(WagerTransactionStatus.Failed);
    expect(failed.failureCode).toBe(FailureCode.ProcessingError);
  });
});

describe('PublishOutboxUseCase', () => {
  let ctx: InMemoryContext;

  beforeEach(() => {
    ctx = buildInMemoryContext();
  });

  test('publica os eventos pendentes e marca como publicados', async () => {
    await ctx.openWallet('100.00');
    const result = await ctx.publishOutbox.publishBatch(10);

    expect(result).toEqual({ claimed: 2, published: 2, failed: 0 });
    expect(ctx.publisher.published).toHaveLength(2);
    expect([...ctx.uow.store.outbox.values()].every((message) => !message.isPending())).toBe(true);
    expect(await ctx.publishOutbox.publishBatch(10)).toEqual({ claimed: 0, published: 0, failed: 0 });
  });

  test('falha do broker: o evento e o resto do lote esperam o mesmo backoff (ordem preservada)', async () => {
    await ctx.openWallet('100.00');
    ctx.publisher.failuresLeft = 2;

    expect(await ctx.publishOutbox.publishBatch(10)).toEqual({ claimed: 2, published: 0, failed: 1 });
    const [failed, held] = [...ctx.uow.store.outbox.values()].sort((a, b) => a.eventType.localeCompare(b.eventType));
    expect(failed!.attempts + held!.attempts).toBe(1);
    expect(failed!.nextAttemptAt).toEqual(held!.nextAttemptAt);
    expect(await ctx.publishOutbox.publishBatch(10)).toEqual({ claimed: 0, published: 0, failed: 0 });

    ctx.clock.advance(1_000);
    expect(await ctx.publishOutbox.publishBatch(10)).toEqual({ claimed: 2, published: 0, failed: 1 });

    ctx.clock.advance(2_000);
    expect(await ctx.publishOutbox.publishBatch(10)).toEqual({ claimed: 2, published: 2, failed: 0 });
    expect(ctx.metrics.count('retry:outbox_relay')).toBe(2);
  });

  test('eventos já publicados não são reivindicados de novo', async () => {
    await ctx.openWallet('100.00');
    await ctx.publishOutbox.publishBatch(10);
    expect(await ctx.publishOutbox.publishBatch(10)).toEqual({ claimed: 0, published: 0, failed: 0 });
    expect(ctx.publisher.published).toHaveLength(2);
  });
});

describe('lease expirado no meio do lote', () => {
  let ctx: InMemoryContext;

  beforeEach(() => {
    ctx = buildInMemoryContext();
  });

  test('outbox: ao se aproximar do fim do lease, o relay devolve o resto do lote sem publicar', async () => {
    await ctx.openWallet('100.00');
    const publish = ctx.publisher.publish.bind(ctx.publisher);
    ctx.publisher.publish = async (message) => {
      await publish(message);
      ctx.clock.advance(25_000);
    };

    expect(await ctx.publishOutbox.publishBatch(10)).toEqual({ claimed: 2, published: 1, failed: 0 });

    const stillPending = [...ctx.uow.store.outbox.values()].filter((message) => message.isPending());
    expect(stillPending).toHaveLength(1);
    expect(stillPending[0]!.isDue(ctx.clock.now())).toBe(true);
    expect(await ctx.publishOutbox.publishBatch(10)).toEqual({ claimed: 1, published: 1, failed: 0 });
  });

  test('outbox: falha ao gravar a publicação é registrada e o relay para sem mascarar o erro', async () => {
    await ctx.openWallet('100.00');
    const run = ctx.uow.run.bind(ctx.uow);
    let calls = 0;
    ctx.uow.run = async <T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> => {
      calls += 1;
      if (calls === 2) throw new TransientInfrastructureError('db down', 'connection');
      return run(work);
    };

    expect(await ctx.publishOutbox.publishBatch(10)).toEqual({ claimed: 2, published: 0, failed: 0 });
    expect(
      ctx.logger.has(
        'error',
        'could not record outbox publication; the lease will expire and the event may be republished',
      ),
    ).toBe(true);
  });

  test('outbox: falha ao devolver o lote não derruba o relay', async () => {
    await ctx.openWallet('100.00');
    ctx.publisher.failuresLeft = 1;
    const run = ctx.uow.run.bind(ctx.uow);
    let calls = 0;
    ctx.uow.run = async <T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> => {
      calls += 1;
      if (calls === 3) throw new TransientInfrastructureError('db down', 'connection');
      return run(work);
    };

    expect(await ctx.publishOutbox.publishBatch(10)).toEqual({ claimed: 2, published: 0, failed: 1 });
    expect(ctx.logger.has('warn', 'could not release outbox lease; it will expire on its own')).toBe(true);
  });

  test('worker de referências: não processa candidatas depois que o lease passou da margem', async () => {
    const first = await pendingRefundWoken(ctx, 'bet-a');
    const second = await pendingRefundWoken(ctx, 'bet-b');
    const run = ctx.uow.run.bind(ctx.uow);
    let calls = 0;
    ctx.uow.run = async <T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> => {
      calls += 1;
      const result = await run(work);
      if (calls === 2) ctx.clock.advance(25_000);
      return result;
    };

    const result = await ctx.retryPending.runOnce(10);

    expect(result).toEqual({ candidates: 2, settled: 1, stillPending: 0, failed: 0 });
    const statuses = [first, second].map((id) => ctx.uow.store.transactions.get(id)!.status);
    expect(statuses.sort()).toEqual([WagerTransactionStatus.PendingReference, WagerTransactionStatus.Processed]);

    ctx.clock.advance(10_000);
    ctx.uow.run = run;
    expect(await ctx.retryPending.runOnce(10)).toMatchObject({ candidates: 1, settled: 1 });
  });
});

async function pendingRefundWoken(ctx: InMemoryContext, reference: string): Promise<string> {
  const wallet = await ctx.openWallet('100.00');
  const refund = await ctx.processWager.execute(
    ctx.command(wallet, { kind: 'REFUND', referenceExternalTransactionId: reference }),
    HTTP,
  );
  await ctx.processWager.execute(ctx.command(wallet, { externalTransactionId: reference }), HTTP);
  return refund.transactionId;
}
