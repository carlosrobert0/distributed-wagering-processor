import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { prepareTestDatabase } from '../support/environment';
import { startTestApp, TestApp } from '../support/app';
import { expectWalletMatchesLedger, ledgerRows, transactionRows, walletRow } from '../support/database';
import { wager } from '../support/http';
import { createTestQueues } from '../support/sqs';

/**
 * CENÁRIO OBRIGATÓRIO (§8, §13 concorrência 2):
 * saldo 100.00, duas BET de 80.00 simultâneas → 1 PROCESSED, 1 REJECTED INSUFFICIENT_FUNDS,
 * saldo 20.00, exatamente 1 débito, e nenhum retry duplica o débito.
 *
 * Paralelismo real: requisições HTTP concorrentes, cada uma com sua conexão do pool.
 */
describe('100.00 − 80.00 − 80.00 em paralelo', () => {
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

  // Repetido várias vezes: uma race que passa "por sorte" uma vez não passa 25.
  test.each(Array.from({ length: 25 }, (_, i) => i + 1))('rodada %i', async () => {
    const wallet = await ctx.api.createWallet('100.00');
    const betA = wager(wallet, { amount: '80.00' });
    const betB = wager(wallet, { amount: '80.00' });

    const [a, b] = await Promise.all([ctx.api.submit(betA), ctx.api.submit(betB)]);
    const results = [a, b].sort((x, y) => x.status - y.status);

    expect(results.map((r) => r.status)).toEqual([201, 422]);
    expect(results[0]!.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '20.00', currency: 'BRL' } });
    expect(results[1]!.body).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS' });

    expect(await walletRow(wallet.id)).toMatchObject({ balance: '20.00', version: 2 });
    const debits = (await ledgerRows(wallet.id)).filter((entry) => entry.direction === 'DEBIT');
    expect(debits).toHaveLength(1);
    expect(debits[0]).toMatchObject({ amount: '80.00', balance_before: '100.00', balance_after: '20.00' });

    // Retries (inclusive concorrentes) de AMBAS as apostas não duplicam nada.
    const retries = await Promise.all([
      ctx.api.submit(betA),
      ctx.api.submit(betB),
      ctx.api.submit(betA),
      ctx.api.submit(betB),
    ]);
    expect(retries.every((r) => r.body.idempotentReplay === true)).toBe(true);
    expect(retries.map((r) => r.status).sort()).toEqual([200, 200, 422, 422]);
    expect((await ledgerRows(wallet.id)).filter((entry) => entry.direction === 'DEBIT')).toHaveLength(1);
    expect((await transactionRows(wallet.id)).filter((tx) => tx.kind === 'BET')).toHaveLength(2);
    expect((await walletRow(wallet.id)).balance).toBe('20.00');

    await expectWalletMatchesLedger(wallet.id);
  });

  test('hot wallet: 40 apostas de 5.00 simultâneas sobre 100.00 → exatamente 20 aceitas, saldo nunca negativo', async () => {
    const wallet = await ctx.api.createWallet('100.00');
    const responses = await Promise.all(
      Array.from({ length: 40 }, () => ctx.api.submit(wager(wallet, { amount: '5.00' }))),
    );
    expect(responses.filter((r) => r.status === 201)).toHaveLength(20);
    expect(responses.filter((r) => r.status === 422 && r.body.failureCode === 'INSUFFICIENT_FUNDS')).toHaveLength(20);
    expect(await walletRow(wallet.id)).toMatchObject({ balance: '0.00', version: 21 });
    await expectWalletMatchesLedger(wallet.id);
  });
});
