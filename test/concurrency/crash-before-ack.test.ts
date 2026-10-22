import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { prepareTestDatabase, waitFor } from '../support/environment';
import { killSpawnedInstances, spawnInstance, startTestApp } from '../support/app';
import { countRows, expectWalletMatchesLedger, ledgerRows, walletRow } from '../support/database';
import { approximateCounts, createTestQueues, sendWagerMessage } from '../support/sqs';

describe('worker morto entre commit e ack', () => {
  let queues: Awaited<ReturnType<typeof createTestQueues>>;

  beforeAll(async () => {
    await prepareTestDatabase();
    queues = await createTestQueues({ visibilityTimeoutSeconds: 3 });
  });

  afterAll(async () => {
    await killSpawnedInstances();
    await queues.cleanup();
  });

  test('redelivery após o crash não duplica o débito', async () => {
    const crashing = await spawnInstance('crashing-instance', queues, {
      CONSUMER_ENABLED: 'true',
      CONSUMER_VISIBILITY_TIMEOUT_SECONDS: '3',
      FAULT_INJECTION: 'crash-after-commit',
    });
    const wallet = await crashing.api.createWallet('100.00');

    const externalTransactionId = `tx-${randomUUID()}`;
    const messageId = await sendWagerMessage(queues.wagerTransactions, {
      providerId: 'provider-a',
      externalTransactionId,
      idempotencyKey: `provider-a:${externalTransactionId}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '30.00', currency: 'BRL' },
    });

    await waitFor(
      async () => await Promise.race([crashing.exited.then(() => true), Bun.sleep(100).then(() => false)]),
      {
        timeoutMs: 20_000,
        description: 'instance crashed after commit',
      },
    );

    expect((await walletRow(wallet.id)).balance).toBe('70.00');
    expect(await countRows(`SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1`, [messageId])).toBe(1);
    const counts = await approximateCounts(queues.wagerTransactions);
    expect(counts.visible + counts.inFlight).toBe(1);

    const healthy = await startTestApp(queues, { CONSUMER_VISIBILITY_TIMEOUT_SECONDS: '3' });
    healthy.consumer.start();
    try {
      await waitFor(
        async () => {
          const now = await approximateCounts(queues.wagerTransactions);
          return now.visible === 0 && now.inFlight === 0;
        },
        { timeoutMs: 30_000, description: 'redelivered message acked' },
      );
      const metrics = await fetch(`${healthy.api.baseUrl}/metrics`).then((r) => r.text());
      expect(metrics).toMatch(/wagering_duplicates_total\{[^}]*type="inbox_duplicate"[^}]*\} 1$/m);
    } finally {
      await healthy.close();
    }

    expect((await walletRow(wallet.id)).balance).toBe('70.00');
    expect((await ledgerRows(wallet.id)).filter((entry) => entry.direction === 'DEBIT')).toHaveLength(1);
    await expectWalletMatchesLedger(wallet.id);
  }, 90_000);
});
