import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { prepareTestDatabase, waitFor } from '../support/environment';
import { spawnInstance, SpawnedInstance } from '../support/app';
import { countRows, expectAllWalletsMatchLedger, expectWalletMatchesLedger, ledgerRows, walletRow } from '../support/database';
import { wager } from '../support/http';
import { approximateCounts, createTestQueues, drainMessages, sendWagerMessage } from '../support/sqs';

/**
 * §13 concorrência 4: ≥ 3 PROCESSOS simultâneos (`bun run src/main.ts`), cada um com seu pool
 * de conexões, consumer SQS, relay de outbox e worker de referências — exatamente como no deploy.
 */
describe('3 instâncias simultâneas', () => {
  let queues: Awaited<ReturnType<typeof createTestQueues>>;
  let instances: SpawnedInstance[] = [];

  beforeAll(async () => {
    await prepareTestDatabase();
    queues = await createTestQueues({ visibilityTimeoutSeconds: 5 });
    instances = await Promise.all(
      ['instance-1', 'instance-2', 'instance-3'].map((name) =>
        spawnInstance(name, queues, {
          CONSUMER_ENABLED: 'true',
          OUTBOX_RELAY_ENABLED: 'true',
          OUTBOX_RELAY_INTERVAL_MS: '100',
          PENDING_REFERENCE_WORKER_ENABLED: 'true',
          PENDING_REFERENCE_INTERVAL_MS: '100',
          PENDING_REFERENCE_BASE_DELAY_MS: '100',
          CONSUMER_VISIBILITY_TIMEOUT_SECONDS: '5',
        }),
      ),
    );
  }, 60_000);

  afterAll(async () => {
    await Promise.all(instances.map((instance) => instance.stop()));
    await queues.cleanup();
  });

  const instance = (i: number) => instances[i % instances.length]!;

  test('100 − 80 − 80 com cada aposta numa instância diferente', async () => {
    const wallet = await instance(0).api.createWallet('100.00');
    const [a, b] = await Promise.all([
      instance(1).api.submit(wager(wallet, { amount: '80.00' })),
      instance(2).api.submit(wager(wallet, { amount: '80.00' })),
    ]);
    expect([a.status, b.status].sort()).toEqual([201, 422]);
    expect((await walletRow(wallet.id)).balance).toBe('20.00');
    expect((await ledgerRows(wallet.id)).filter((e) => e.direction === 'DEBIT')).toHaveLength(1);
    await expectWalletMatchesLedger(wallet.id);
  });

  test('hot wallet: 30 apostas espalhadas nas 3 instâncias, cada uma duplicada em outra instância', async () => {
    const wallet = await instance(0).api.createWallet('100.00');
    const bets = Array.from({ length: 30 }, () => wager(wallet, { amount: '10.00' }));

    const responses = await Promise.all(
      bets.flatMap((bet, i) => [instance(i).api.submit(bet), instance(i + 1).api.submit(bet)]),
    );

    // Cada par (original, duplicata) termina com o mesmo transactionId.
    for (let i = 0; i < bets.length; i++) {
      expect(responses[2 * i]!.body.transactionId).toBe(responses[2 * i + 1]!.body.transactionId);
    }
    const processed = new Set(responses.filter((r) => r.body.status === 'PROCESSED').map((r) => r.body.transactionId));
    const rejected = new Set(responses.filter((r) => r.body.status === 'REJECTED').map((r) => r.body.transactionId));
    expect(processed.size).toBe(10);
    expect(rejected.size).toBe(20);
    expect(await walletRow(wallet.id)).toMatchObject({ balance: '0.00', version: 11 });
    await expectWalletMatchesLedger(wallet.id);
  });

  test('fila consumida pelas 3 instâncias: mensagens duplicadas, 5 wallets, cada efeito exatamente uma vez', async () => {
    const wallets = await Promise.all(Array.from({ length: 5 }, (_, i) => instance(i).api.createWallet('100.00')));
    const sends: Promise<string>[] = [];
    for (const wallet of wallets) {
      for (let n = 0; n < 8; n++) {
        const externalTransactionId = `tx-${randomUUID()}`;
        const data = {
          providerId: 'provider-a',
          externalTransactionId,
          idempotencyKey: `provider-a:${externalTransactionId}`,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-1',
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '1.00', currency: 'BRL' },
        };
        const messageId = `msg-${randomUUID()}`;
        sends.push(sendWagerMessage(queues.wagerTransactions, data, { messageId })); // original
        sends.push(sendWagerMessage(queues.wagerTransactions, data, { messageId })); // redelivery
        sends.push(sendWagerMessage(queues.wagerTransactions, data)); // outro messageId, mesma key
      }
    }
    await Promise.all(sends);

    await waitFor(
      async () => {
        const counts = await approximateCounts(queues.wagerTransactions);
        return counts.visible === 0 && counts.inFlight === 0;
      },
      { timeoutMs: 60_000, description: 'queue drained by 3 consumers' },
    );
    for (const wallet of wallets) {
      expect(await walletRow(wallet.id)).toMatchObject({ balance: '92.00', version: 9 });
      await expectWalletMatchesLedger(wallet.id);
    }
  });

  test('3 relays de outbox concorrentes publicam cada evento uma única vez', async () => {
    const wallet = await instance(0).api.createWallet('100.00');
    await Promise.all(Array.from({ length: 10 }, (_, i) => instance(i).api.submit(wager(wallet, { amount: '1.00' }))));

    await waitFor(
      async () =>
        (await countRows(
          `SELECT COUNT(*) FROM outbox_messages WHERE partition_key = $1 AND published_at IS NULL`,
          [wallet.id],
        )) === 0,
      { timeoutMs: 30_000, description: 'outbox drained' },
    );
    const events = (await drainMessages(queues.events))
      .map((m) => JSON.parse(m.Body!) as { eventId: string; data: { walletId?: string } })
      .filter((event) => event.data.walletId === wallet.id);
    // abertura (2) + 10 BETs × 2 eventos
    expect(events).toHaveLength(22);
    expect(new Set(events.map((event) => event.eventId)).size).toBe(22);
  });

  test('invariante global: toda wallet bate com o ledger', async () => {
    await expectAllWalletsMatchLedger();
  });
});
