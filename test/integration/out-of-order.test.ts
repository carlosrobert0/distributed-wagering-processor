import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { ExponentialBackoff } from '../../src/domain/shared/exponential-backoff';
import { prepareTestDatabase, sleep } from '../support/environment';
import {
  expectWalletMatchesLedger,
  ledgerRows,
  outboxEventTypes,
  transactionRows,
  walletRow,
} from '../support/database';
import { buildUseCases } from '../support/use-cases';

describe('reversões e referências fora de ordem', () => {
  let ctx: Awaited<ReturnType<typeof buildUseCases>>;

  beforeAll(async () => {
    await prepareTestDatabase();
    ctx = await buildUseCases({ referencePolicy: new ExponentialBackoff(50, 200, 3) });
  });
  afterAll(() => ctx.close());

  async function newWallet(amount = '100.00') {
    return ctx.createWallet.execute(
      { playerId: randomUUID(), initialBalance: { amount, currency: 'BRL' } },
      { correlationId: 'test' },
    );
  }

  function submit(
    wallet: { id: string; playerId: string },
    kind: string,
    amount: string,
    options: { externalTransactionId?: string; reference?: string; roundId?: string } = {},
  ) {
    const externalTransactionId = options.externalTransactionId ?? `${kind.toLowerCase()}-${randomUUID()}`;
    return ctx.processWager().execute(
      {
        idempotencyKey: `provider-a:${externalTransactionId}`,
        providerId: 'provider-a',
        externalTransactionId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: options.roundId ?? 'round-1',
        gameId: 'fortune-chimp',
        kind,
        money: { amount, currency: 'BRL' },
        referenceExternalTransactionId: options.reference,
      },
      { correlationId: 'test', source: 'http' },
    );
  }

  async function drainPendingReferences(maxRounds = 20): Promise<void> {
    for (let round = 0; round < maxRounds; round++) {
      await sleep(250);
      const result = await ctx.retryPending.runOnce(100);
      if (result.candidates === 0) {
        const pending = await ctx.orm.em
          .fork()
          .execute<{ c: number }[]>(
            `SELECT COUNT(*)::int AS c FROM wager_transactions WHERE status = 'PENDING_REFERENCE'`,
          );
        if (Number(pending[0]?.c ?? 0) === 0) return;
      }
    }
  }

  test('REFUND antes da BET → PENDING_REFERENCE; BET chega; worker aplica o REFUND', async () => {
    const wallet = await newWallet('100.00');
    const refund = await submit(wallet, 'REFUND', '30.00', { reference: 'bet-ooo-1' });
    expect(refund).toMatchObject({ status: 'PENDING_REFERENCE', idempotentReplay: false });
    expect(refund.balance).toBeUndefined();
    expect(await outboxEventTypes([refund.transactionId])).toEqual(['WagerTransactionPendingReference']);

    await submit(wallet, 'BET', '30.00', { externalTransactionId: 'bet-ooo-1' });
    expect((await walletRow(wallet.id)).balance).toBe('70.00');

    await drainPendingReferences();
    const rows = await transactionRows(wallet.id);
    expect(rows.find((row) => row.id === refund.transactionId)).toMatchObject({
      status: 'PROCESSED',
      balance_after: '100.00',
    });
    expect((await walletRow(wallet.id)).balance).toBe('100.00');
    await expectWalletMatchesLedger(wallet.id);
  });

  test('ROLLBACK de WIN entregue antes da WIN', async () => {
    const wallet = await newWallet('0.00');
    const rollback = await submit(wallet, 'ROLLBACK', '40.00', { reference: 'win-ooo-1' });
    expect(rollback.status as string).toBe('PENDING_REFERENCE');

    await submit(wallet, 'WIN', '40.00', { externalTransactionId: 'win-ooo-1' });
    await drainPendingReferences();

    expect((await walletRow(wallet.id)).balance).toBe('0.00');
    const entries = await ledgerRows(wallet.id);
    expect(entries.map((entry) => entry.direction)).toEqual(['CREDIT', 'DEBIT']);
    await expectWalletMatchesLedger(wallet.id);
  });

  test('referência que nunca chega → REJECTED REFERENCE_NOT_FOUND + evento (AC-PEND-03)', async () => {
    const wallet = await newWallet('100.00');
    const refund = await submit(wallet, 'REFUND', '10.00', { reference: `ghost-${randomUUID()}` });
    await drainPendingReferences();

    const rows = await transactionRows(wallet.id);
    expect(rows.find((row) => row.id === refund.transactionId)).toMatchObject({
      status: 'REJECTED',
      failure_code: 'REFERENCE_NOT_FOUND',
    });
    expect(await outboxEventTypes([refund.transactionId])).toEqual([
      'WagerTransactionPendingReference',
      'WagerTransactionRejected',
    ]);
    expect((await walletRow(wallet.id)).balance).toBe('100.00');
  });

  test('replay de uma transação pendente devolve o estado atual (INT-07)', async () => {
    const wallet = await newWallet('100.00');
    const reference = `bet-${randomUUID()}`;
    const externalTransactionId = `refund-${randomUUID()}`;
    const first = await submit(wallet, 'REFUND', '5.00', { reference, externalTransactionId });
    expect(first.status as string).toBe('PENDING_REFERENCE');
    const replayWhilePending = await submit(wallet, 'REFUND', '5.00', { reference, externalTransactionId });
    expect(replayWhilePending).toMatchObject({ status: 'PENDING_REFERENCE', idempotentReplay: true });

    await submit(wallet, 'BET', '5.00', { externalTransactionId: reference });
    await drainPendingReferences();
    const replayAfter = await submit(wallet, 'REFUND', '5.00', { reference, externalTransactionId });
    expect(replayAfter).toMatchObject({ status: 'PROCESSED', idempotentReplay: true, balance: { amount: '100.00' } });
  });

  describe('regras de reversão', () => {
    test('REFUND duas vezes da mesma BET → segundo ALREADY_REVERSED (AC-TX-07)', async () => {
      const wallet = await newWallet('100.00');
      await submit(wallet, 'BET', '20.00', { externalTransactionId: 'bet-r1' + wallet.id });
      const first = await submit(wallet, 'REFUND', '20.00', { reference: 'bet-r1' + wallet.id });
      const second = await submit(wallet, 'REFUND', '20.00', { reference: 'bet-r1' + wallet.id });
      expect(first.status as string).toBe('PROCESSED');
      expect(second).toMatchObject({ status: 'REJECTED', failureCode: 'ALREADY_REVERSED' });
      expect((await walletRow(wallet.id)).balance).toBe('100.00');
    });

    test('REFUND e depois ROLLBACK da mesma BET → ROLLBACK ALREADY_REVERSED (INT-05)', async () => {
      const wallet = await newWallet('100.00');
      const betId = `bet-${randomUUID()}`;
      await submit(wallet, 'BET', '20.00', { externalTransactionId: betId });
      await submit(wallet, 'REFUND', '20.00', { reference: betId });
      const rollback = await submit(wallet, 'ROLLBACK', '20.00', { reference: betId });
      expect(rollback).toMatchObject({ status: 'REJECTED', failureCode: 'ALREADY_REVERSED' });
      expect((await walletRow(wallet.id)).balance).toBe('100.00');
      await expectWalletMatchesLedger(wallet.id);
    });

    test('ROLLBACK de WIN já gasto → REVERSAL_WOULD_OVERDRAW, auditável (AC-TX-12)', async () => {
      const wallet = await newWallet('0.00');
      const winId = `win-${randomUUID()}`;
      await submit(wallet, 'WIN', '50.00', { externalTransactionId: winId });
      await submit(wallet, 'BET', '45.00');
      const rollback = await submit(wallet, 'ROLLBACK', '50.00', { reference: winId });
      expect(rollback).toMatchObject({
        status: 'REJECTED',
        failureCode: 'REVERSAL_WOULD_OVERDRAW',
        balance: { amount: '5.00' },
      });
      expect(await outboxEventTypes([rollback.transactionId])).toEqual(['WagerTransactionRejected']);
      await expectWalletMatchesLedger(wallet.id);
    });

    test('REFUND com valor diferente → AMOUNT_MISMATCH; rodada diferente → REFERENCE_MISMATCH', async () => {
      const wallet = await newWallet('100.00');
      const betId = `bet-${randomUUID()}`;
      await submit(wallet, 'BET', '20.00', { externalTransactionId: betId });
      expect(await submit(wallet, 'REFUND', '19.99', { reference: betId })).toMatchObject({
        failureCode: 'AMOUNT_MISMATCH',
      });
      expect(await submit(wallet, 'REFUND', '20.00', { reference: betId, roundId: 'other' })).toMatchObject({
        failureCode: 'REFERENCE_MISMATCH',
      });
    });

    test('ROLLBACK de REFUND reaplica o débito', async () => {
      const wallet = await newWallet('100.00');
      const betId = `bet-${randomUUID()}`;
      const refundId = `refund-${randomUUID()}`;
      await submit(wallet, 'BET', '20.00', { externalTransactionId: betId });
      await submit(wallet, 'REFUND', '20.00', { reference: betId, externalTransactionId: refundId });
      const rollback = await submit(wallet, 'ROLLBACK', '20.00', { reference: refundId });
      expect(rollback).toMatchObject({ status: 'PROCESSED', balance: { amount: '80.00' } });
      await expectWalletMatchesLedger(wallet.id);
    });
  });
});
