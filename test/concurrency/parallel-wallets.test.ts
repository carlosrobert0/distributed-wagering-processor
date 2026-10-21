import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { prepareTestDatabase } from '../support/environment';
import { startTestApp, TestApp } from '../support/app';
import { db, expectWalletMatchesLedger, walletRow } from '../support/database';
import { wager } from '../support/http';
import { createTestQueues } from '../support/sqs';

describe('wallets distintas em paralelo', () => {
  let ctx: TestApp;
  let cleanup: () => Promise<void>;

  beforeAll(async () => {
    await prepareTestDatabase();
    const queues = await createTestQueues();
    cleanup = queues.cleanup;
    ctx = await startTestApp(queues);
  });

  afterAll(async () => {
    await ctx.close();
    await cleanup();
  });

  test('20 wallets × 10 operações mistas simultâneas: cada wallet termina correta', async () => {
    const wallets = await Promise.all(Array.from({ length: 20 }, () => ctx.api.createWallet('100.00')));

    await Promise.all(
      wallets.flatMap((wallet) => [
        ...Array.from({ length: 6 }, () => ctx.api.submit(wager(wallet, { amount: '10.00' }))),
        ...Array.from({ length: 4 }, () => ctx.api.submit(wager(wallet, { kind: 'WIN', amount: '2.50' }))),
      ]),
    );

    for (const wallet of wallets) {
      expect(await walletRow(wallet.id)).toMatchObject({ balance: '50.00', version: 11 });
      await expectWalletMatchesLedger(wallet.id);
    }
  });

  test('uma wallet travada não bloqueia as outras (sem lock global)', async () => {
    const [slow, fast] = await Promise.all([ctx.api.createWallet('100.00'), ctx.api.createWallet('100.00')]);
    const client = await db.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT id FROM wallets WHERE id = $1 FOR UPDATE', [slow.id]);

      const started = Date.now();
      const response = await ctx.api.submit(wager(fast, { amount: '1.00' }));
      expect(response.status).toBe(201);
      expect(Date.now() - started).toBeLessThan(2_000);
    } finally {
      await client.query('ROLLBACK');
      client.release();
    }
  });
});
