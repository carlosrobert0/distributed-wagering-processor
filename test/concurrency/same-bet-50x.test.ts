import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { prepareTestDatabase } from '../support/environment';
import { startTestApp, TestApp } from '../support/app';
import { countRows, expectWalletMatchesLedger, ledgerRows, walletRow } from '../support/database';
import { wager } from '../support/http';
import { createTestQueues } from '../support/sqs';

describe('a mesma aposta 50× em paralelo', () => {
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

  test('1 processamento original + 49 replays, mesmo transactionId e mesmo saldo observado', async () => {
    const wallet = await ctx.api.createWallet('1000.00');
    const bet = wager(wallet, { amount: '25.00' });

    const responses = await Promise.all(Array.from({ length: 50 }, () => ctx.api.submit(bet)));

    const originals = responses.filter((r) => r.body.idempotentReplay === false);
    const replays = responses.filter((r) => r.body.idempotentReplay === true);
    expect(originals).toHaveLength(1);
    expect(originals[0]!.status).toBe(201);
    expect(replays).toHaveLength(49);
    expect(replays.every((r) => r.status === 200)).toBe(true);

    const ids = new Set(responses.map((r) => r.body.transactionId));
    expect(ids.size).toBe(1);
    expect(new Set(responses.map((r) => r.body.balance?.amount))).toEqual(new Set(['975.00']));

    expect((await walletRow(wallet.id)).balance).toBe('975.00');
    expect((await ledgerRows(wallet.id)).filter((entry) => entry.direction === 'DEBIT')).toHaveLength(1);
    expect(
      await countRows(`SELECT COUNT(*) FROM wager_transactions WHERE external_transaction_id = $1`, [
        bet.externalTransactionId,
      ]),
    ).toBe(1);
    await expectWalletMatchesLedger(wallet.id);
  });

  test('50 cópias com payload divergente na mesma key: 1 vence, as demais são 409 ou replay', async () => {
    const wallet = await ctx.api.createWallet('1000.00');
    const bet = wager(wallet, { amount: '10.00' });
    const key = `provider-a:${bet.externalTransactionId}`;

    const responses = await Promise.all(
      Array.from({ length: 50 }, (_, i) =>
        ctx.api.submit({ ...bet, money: { amount: i % 2 === 0 ? '10.00' : '11.00', currency: 'BRL' } }, key),
      ),
    );
    const winners = responses.filter((r) => r.status === 201);
    expect(winners).toHaveLength(1);
    const winnerAmount = winners[0]!.body.balance?.amount === '990.00' ? '10.00' : '11.00';
    for (const response of responses.filter((r) => r !== winners[0])) {
      expect([200, 409]).toContain(response.status);
    }
    expect((await ledgerRows(wallet.id)).filter((entry) => entry.direction === 'DEBIT')).toEqual([
      expect.objectContaining({ amount: winnerAmount }),
    ]);
    await expectWalletMatchesLedger(wallet.id);
  });
});
