import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { prepareTestDatabase, sleep, waitFor } from '../support/environment';
import { spawnInstance } from '../support/app';
import { countRows, expectAllWalletsMatchLedger, expectWalletMatchesLedger, walletRow } from '../support/database';
import { WagerBody, wager } from '../support/http';
import { approximateCounts, createTestQueues, sendWagerMessage } from '../support/sqs';

/**
 * §13 concorrência 8: reinício do serviço com comprovação da consistência final.
 *
 * A instância A recebe carga por HTTP e SQS e é morta com SIGKILL no meio (sem drenar nada).
 * A instância B sobe, os provedores reenviam o que não teve resposta, a fila reentrega o que
 * estava em voo e o relay publica a outbox pendente. Ao final: cada operação aplicada uma vez,
 * saldo == ledger e nenhum evento confirmado perdido.
 */
describe('reinício com consistência final', () => {
  let queues: Awaited<ReturnType<typeof createTestQueues>>;

  beforeAll(async () => {
    await prepareTestDatabase();
    queues = await createTestQueues({ visibilityTimeoutSeconds: 3 });
  });

  afterAll(() => queues.cleanup());

  test('SIGKILL no meio da carga → nova instância converge para o estado correto', async () => {
    const workerEnv = {
      CONSUMER_ENABLED: 'true',
      CONSUMER_VISIBILITY_TIMEOUT_SECONDS: '3',
      OUTBOX_RELAY_ENABLED: 'true',
      OUTBOX_RELAY_INTERVAL_MS: '100',
      PENDING_REFERENCE_WORKER_ENABLED: 'true',
    };
    const instanceA = await spawnInstance('instance-a', queues, workerEnv);

    const wallets = await Promise.all(Array.from({ length: 5 }, () => instanceA.api.createWallet('100.00')));

    // Carga SQS: 10 apostas de 1.00 por wallet.
    for (const wallet of wallets) {
      for (let n = 0; n < 10; n++) {
        const externalTransactionId = `sqs-${randomUUID()}`;
        await sendWagerMessage(queues.wagerTransactions, {
          providerId: 'provider-a',
          externalTransactionId,
          idempotencyKey: `provider-a:${externalTransactionId}`,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-1',
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '1.00', currency: 'BRL' },
        });
      }
    }

    // Carga HTTP: 5 apostas de 1.00 por wallet, disparadas sem esperar.
    const httpBets: WagerBody[] = wallets.flatMap((wallet) =>
      Array.from({ length: 5 }, () => wager(wallet, { amount: '1.00' })),
    );
    const inFlight = Promise.allSettled(httpBets.map((bet) => instanceA.api.submit(bet)));

    // Morte abrupta no meio do processamento.
    await sleep(300);
    await instanceA.kill();
    await inFlight;

    // Nova instância assume.
    const instanceB = await spawnInstance('instance-b', queues, workerEnv);
    try {
      // O provedor reenvia as requisições HTTP (sem saber quais commitaram) — mesmas keys.
      const retried = await Promise.all(httpBets.map((bet) => instanceB.api.submit(bet)));
      expect(retried.every((response) => response.status === 200 || response.status === 201)).toBe(true);

      await waitFor(
        async () => {
          const counts = await approximateCounts(queues.wagerTransactions);
          return counts.visible === 0 && counts.inFlight === 0;
        },
        { timeoutMs: 60_000, description: 'queue drained after restart' },
      );
      await waitFor(
        async () =>
          (await countRows(
            `SELECT COUNT(*) FROM outbox_messages WHERE partition_key = ANY($1) AND published_at IS NULL`,
            [wallets.map((w) => w.id)],
          )) === 0,
        { timeoutMs: 30_000, description: 'outbox fully published after restart' },
      );

      for (const wallet of wallets) {
        // 100 − 10 (SQS) − 5 (HTTP) = 85.00: cada operação exatamente uma vez.
        expect(await walletRow(wallet.id)).toMatchObject({ balance: '85.00', version: 16 });
        await expectWalletMatchesLedger(wallet.id);
      }

      // Nenhum evento confirmado perdido: toda transação processada tem seu WagerTransactionProcessed.
      const missingEvents = await countRows(
        `SELECT COUNT(*) FROM wager_transactions t
          WHERE t.wallet_id = ANY($1) AND t.status = 'PROCESSED'
            AND NOT EXISTS (SELECT 1 FROM outbox_messages o
                             WHERE o.aggregate_id = t.id::text AND o.event_type = 'WagerTransactionProcessed'
                               AND o.published_at IS NOT NULL)`,
        [wallets.map((w) => w.id)],
      );
      expect(missingEvents).toBe(0);
      await expectAllWalletsMatchLedger();
    } finally {
      await instanceB.stop();
    }
  }, 120_000);
});
