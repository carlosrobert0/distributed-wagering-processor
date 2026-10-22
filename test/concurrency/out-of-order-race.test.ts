import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { prepareTestDatabase, waitFor } from '../support/environment';
import { startTestApp, TestApp } from '../support/app';
import { countRows, expectAllWalletsMatchLedger, ledgerRows, transactionRows, walletRow } from '../support/database';
import { wager } from '../support/http';
import { createTestQueues } from '../support/sqs';

describe('reversão e referência chegando ao mesmo tempo (§13 concorrência 7)', () => {
  let ctx: TestApp;
  let cleanup: () => Promise<void>;

  beforeAll(async () => {
    await prepareTestDatabase();
    const queues = await createTestQueues();
    cleanup = queues.cleanup;
    ctx = await startTestApp(queues, {
      PENDING_REFERENCE_WORKER_ENABLED: 'true',
      PENDING_REFERENCE_INTERVAL_MS: '50',
      PENDING_REFERENCE_BASE_DELAY_MS: '50',
      PENDING_REFERENCE_MAX_DELAY_MS: '200',
      PENDING_REFERENCE_MAX_ATTEMPTS: '50',
    });
  });

  afterAll(async () => {
    await ctx.close();
    await cleanup();
  });

  async function pendingFor(walletIds: string[]): Promise<number> {
    return countRows(
      `SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = ANY($1) AND status = 'PENDING_REFERENCE'`,
      [walletIds],
    );
  }

  test('BET + REFUND (duplicado) em paralelo em 20 wallets: exatamente um débito e um crédito por wallet', async () => {
    const wallets = await Promise.all(Array.from({ length: 20 }, () => ctx.api.createWallet('100.00')));

    const responses = await Promise.all(
      wallets.flatMap((wallet) => {
        const betId = `bet-${randomUUID()}`;
        const refund = wager(wallet, { kind: 'REFUND', amount: '30.00', referenceExternalTransactionId: betId });
        return [
          ctx.api.submit(wager(wallet, { externalTransactionId: betId, amount: '30.00' })),
          ctx.api.submit(refund),
          ctx.api.submit(refund),
        ];
      }),
    );
    expect(responses.filter((r) => r.status >= 500)).toEqual([]);

    const walletIds = wallets.map((wallet) => wallet.id);
    await waitFor(async () => (await pendingFor(walletIds)) === 0, {
      timeoutMs: 30_000,
      description: 'every pending refund settled by the worker',
    });

    for (const wallet of wallets) {
      expect((await walletRow(wallet.id)).balance).toBe('100.00');
      expect((await ledgerRows(wallet.id)).map((entry) => entry.direction)).toEqual(['CREDIT', 'DEBIT', 'CREDIT']);
      const refunds = (await transactionRows(wallet.id)).filter((tx) => tx.kind === 'REFUND');
      expect(refunds).toHaveLength(1);
      expect(refunds[0]!.status).toBe('PROCESSED');
    }
    await expectAllWalletsMatchLedger();
  });

  test('ROLLBACK de WIN chegando junto com a WIN: o crédito é desfeito exatamente uma vez', async () => {
    const wallets = await Promise.all(Array.from({ length: 20 }, () => ctx.api.createWallet('0.00')));

    const responses = await Promise.all(
      wallets.flatMap((wallet) => {
        const winId = `win-${randomUUID()}`;
        return [
          ctx.api.submit(wager(wallet, { kind: 'WIN', externalTransactionId: winId, amount: '50.00' })),
          ctx.api.submit(wager(wallet, { kind: 'ROLLBACK', amount: '50.00', referenceExternalTransactionId: winId })),
        ];
      }),
    );

    expect(responses.filter((r) => r.status >= 500)).toEqual([]);
    const walletIds = wallets.map((wallet) => wallet.id);
    await waitFor(async () => (await pendingFor(walletIds)) === 0, { timeoutMs: 30_000 });

    for (const wallet of wallets) {
      expect(await walletRow(wallet.id)).toMatchObject({ balance: '0.00', version: 3 });
    }
    await expectAllWalletsMatchLedger();
  });
});
