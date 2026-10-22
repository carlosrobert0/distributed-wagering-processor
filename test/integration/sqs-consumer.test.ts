import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { PoolClient } from 'pg';
import { prepareTestDatabase, sleep, waitFor } from '../support/environment';
import { startTestApp, TestApp } from '../support/app';
import { countRows, db, expectWalletMatchesLedger, ledgerRows, walletRow } from '../support/database';
import {
  approximateCounts,
  createTestQueues,
  drainMessages,
  sendRawMessage,
  sendWagerMessage,
  WagerMessageData,
} from '../support/sqs';

function betMessage(wallet: { id: string; playerId: string }, amount = '10.00'): WagerMessageData {
  const externalTransactionId = `tx-${randomUUID()}`;
  return {
    providerId: 'provider-a',
    externalTransactionId,
    idempotencyKey: `provider-a:${externalTransactionId}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: 'round-1',
    gameId: 'fortune-chimp',
    kind: 'BET',
    money: { amount, currency: 'BRL' },
  };
}

async function transactionStatus(externalTransactionId: string): Promise<string | undefined> {
  const { rows } = await db.query<{ status: string }>(
    `SELECT status FROM wager_transactions WHERE provider_id = 'provider-a' AND external_transaction_id = $1`,
    [externalTransactionId],
  );
  return rows[0]?.status;
}

async function queueIsEmpty(queueName: string): Promise<boolean> {
  const counts = await approximateCounts(queueName);
  return counts.visible === 0 && counts.inFlight === 0;
}

async function holdWalletLock(walletId: string): Promise<{ release: () => Promise<void> }> {
  const client: PoolClient = await db.connect();
  await client.query('BEGIN');
  await client.query('SELECT id FROM wallets WHERE id = $1 FOR UPDATE', [walletId]);
  return {
    release: async () => {
      await client.query('ROLLBACK');
      client.release();
    },
  };
}

describe('consumer SQS — caminho feliz, inbox e redelivery', () => {
  let ctx: TestApp;
  let queues: Awaited<ReturnType<typeof createTestQueues>>;

  beforeAll(async () => {
    await prepareTestDatabase();
    queues = await createTestQueues();
    ctx = await startTestApp(queues, {
      DB_LOCK_TIMEOUT_MS: '300',
      CONSUMER_RETRY_BASE_SECONDS: '1',
      CONSUMER_RETRY_MAX_SECONDS: '2',
    });
    ctx.consumer.start();
  });

  afterAll(async () => {
    await ctx.close();
    await queues.cleanup();
  });

  test('processa a mensagem com o mesmo use case do HTTP e faz ack após o commit (AC-SQS-02/04)', async () => {
    const wallet = await ctx.api.createWallet('100.00');
    const data = betMessage(wallet, '25.00');
    const messageId = await sendWagerMessage(queues.wagerTransactions, data);

    await waitFor(async () => (await transactionStatus(data.externalTransactionId)) === 'PROCESSED', {
      description: 'message processed',
    });
    await waitFor(() => queueIsEmpty(queues.wagerTransactions), { description: 'message acked' });

    expect((await walletRow(wallet.id)).balance).toBe('75.00');
    expect(await countRows(`SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1`, [messageId])).toBe(1);
    await expectWalletMatchesLedger(wallet.id);
  });

  test('mesma mensagem entregue de novo (mesmo messageId) → inbox detecta, ack, um único débito (AC-SQS-08)', async () => {
    const wallet = await ctx.api.createWallet('100.00');
    const data = betMessage(wallet, '10.00');
    const messageId = `msg-${randomUUID()}`;

    await sendWagerMessage(queues.wagerTransactions, data, { messageId });
    await waitFor(async () => (await transactionStatus(data.externalTransactionId)) === 'PROCESSED');
    await sendWagerMessage(queues.wagerTransactions, data, { messageId });
    await sendWagerMessage(queues.wagerTransactions, data, { messageId });
    await waitFor(() => queueIsEmpty(queues.wagerTransactions), { description: 'duplicates acked' });

    expect((await walletRow(wallet.id)).balance).toBe('90.00');
    expect(await ledgerRows(wallet.id)).toHaveLength(2);
    expect(await countRows(`SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1`, [messageId])).toBe(1);
  });

  test('mesma transação com outro messageId → replay idempotente pela key, um único débito', async () => {
    const wallet = await ctx.api.createWallet('100.00');
    const data = betMessage(wallet, '10.00');
    await Promise.all([1, 2, 3].map(() => sendWagerMessage(queues.wagerTransactions, data)));
    await waitFor(() => queueIsEmpty(queues.wagerTransactions));
    await waitFor(async () => (await transactionStatus(data.externalTransactionId)) === 'PROCESSED');
    expect((await walletRow(wallet.id)).balance).toBe('90.00');
    expect(await ledgerRows(wallet.id)).toHaveLength(2);
  });

  test('a mesma transação pela fila e pelo HTTP → um único débito', async () => {
    const wallet = await ctx.api.createWallet('100.00');
    const data = betMessage(wallet, '10.00');
    const { idempotencyKey, ...body } = data;
    await sendWagerMessage(queues.wagerTransactions, data);
    const http = await ctx.api.submit(body, idempotencyKey);
    expect([200, 201]).toContain(http.status);
    await waitFor(() => queueIsEmpty(queues.wagerTransactions));
    expect(await ledgerRows(wallet.id)).toHaveLength(2);
    expect((await walletRow(wallet.id)).balance).toBe('90.00');
  });

  test('erro de negócio (wallet inexistente) → ack, sem DLQ', async () => {
    const data = betMessage({ id: randomUUID(), playerId: 'ghost' });
    await sendWagerMessage(queues.wagerTransactions, data);
    await waitFor(() => queueIsEmpty(queues.wagerTransactions), { description: 'business error acked' });
    expect(await approximateCounts(queues.wagerTransactionsDlq)).toEqual({ visible: 0, inFlight: 0 });
  });

  test('rejeição de negócio (saldo insuficiente) → REJECTED persistido e ack', async () => {
    const wallet = await ctx.api.createWallet('1.00');
    const data = betMessage(wallet, '10.00');
    await sendWagerMessage(queues.wagerTransactions, data);
    await waitFor(async () => (await transactionStatus(data.externalTransactionId)) === 'REJECTED');
    await waitFor(() => queueIsEmpty(queues.wagerTransactions));
  });

  test('erro permanente (JSON malformado / envelope inválido) → DLQ imediata com motivo (AC-SQS-05)', async () => {
    await sendRawMessage(queues.wagerTransactions, '{not json', { groupId: 'bad-1' });
    await sendRawMessage(
      queues.wagerTransactions,
      JSON.stringify({
        messageId: 'm',
        type: 'WagerTransactionRequested',
        occurredAt: new Date().toISOString(),
        data: { kind: 'BET', money: { amount: 10 } },
      }),
      { groupId: 'bad-2' },
    );
    await waitFor(() => queueIsEmpty(queues.wagerTransactions), { description: 'invalid messages removed' });

    const dead = await drainMessages(queues.wagerTransactionsDlq);
    expect(dead).toHaveLength(2);
    const reasons = dead.map((message) => message.MessageAttributes?.deadLetterReason?.StringValue ?? '');
    expect(reasons.some((reason) => reason.startsWith('MALFORMED_JSON'))).toBe(true);
    expect(reasons.some((reason) => reason.startsWith('INVALID_ENVELOPE'))).toBe(true);
  });

  test('erro transitório (lock ocupado) → retry com backoff de visibilidade e sucesso depois (AC-SQS-05)', async () => {
    const wallet = await ctx.api.createWallet('100.00');
    const data = betMessage(wallet, '10.00');
    const lock = await holdWalletLock(wallet.id);
    try {
      await sendWagerMessage(queues.wagerTransactions, data);
      await sleep(1_500);
      expect(await transactionStatus(data.externalTransactionId)).toBeUndefined();
    } finally {
      await lock.release();
    }
    await waitFor(async () => (await transactionStatus(data.externalTransactionId)) === 'PROCESSED', {
      timeoutMs: 30_000,
      description: 'processed after lock released',
    });
    expect((await walletRow(wallet.id)).balance).toBe('90.00');
  });
});

describe('consumer SQS — limite de tentativas e DLQ por redrive (AC-SQS-06)', () => {
  let ctx: TestApp;
  let queues: Awaited<ReturnType<typeof createTestQueues>>;

  beforeAll(async () => {
    await prepareTestDatabase();
    queues = await createTestQueues({ maxReceiveCount: 20 });
    ctx = await startTestApp(queues, {
      CONSUMER_MAX_ATTEMPTS: '2',
      DB_LOCK_TIMEOUT_MS: '200',
      CONSUMER_RETRY_BASE_SECONDS: '1',
      CONSUMER_RETRY_MAX_SECONDS: '1',
      CONSUMER_VISIBILITY_TIMEOUT_SECONDS: '5',
    });
    ctx.consumer.start();
  });

  afterAll(async () => {
    await ctx.close();
    await queues.cleanup();
  });

  test('falha transitória persistente vai para a DLQ ao atingir o limite de tentativas, com motivo e sem efeito', async () => {
    const wallet = await ctx.api.createWallet('100.00');
    const data = betMessage(wallet, '10.00');
    const lock = await holdWalletLock(wallet.id);
    try {
      await sendWagerMessage(queues.wagerTransactions, data);
      const dead = await waitFor(
        async () => {
          const messages = await drainMessages(queues.wagerTransactionsDlq);
          return messages.length > 0 ? messages : undefined;
        },
        { timeoutMs: 30_000, description: 'message in DLQ' },
      );
      expect(JSON.parse(dead[0]!.Body!).data.externalTransactionId).toBe(data.externalTransactionId);
      expect(dead[0]!.MessageAttributes?.deadLetterReason?.StringValue).toMatch(/^MAX_ATTEMPTS_EXCEEDED after 2/);
    } finally {
      await lock.release();
    }
    expect(await transactionStatus(data.externalTransactionId)).toBeUndefined();
    expect((await walletRow(wallet.id)).balance).toBe('100.00');

    const metrics = await fetch(`${ctx.api.baseUrl}/metrics`).then((r) => r.text());
    expect(metrics).toMatch(/wagering_retries_total\{[^}]*component="sqs_consumer"[^}]*\} [1-9]/);
    expect(metrics).toMatch(/wagering_lock_conflicts_total\{[^}]*kind="lock_timeout"[^}]*\} [1-9]/);
    expect(metrics).toMatch(/wagering_dlq_messages_total\{[^}]*reason="max_attempts"[^}]*\} 1/);
  });
});

describe('consumer SQS — shutdown gracioso (AC-SQS-07)', () => {
  let ctx: TestApp;
  let queues: Awaited<ReturnType<typeof createTestQueues>>;

  beforeAll(async () => {
    await prepareTestDatabase();
    queues = await createTestQueues({ visibilityTimeoutSeconds: 2 });
    ctx = await startTestApp(queues, { CONSUMER_VISIBILITY_TIMEOUT_SECONDS: '2', CONSUMER_BATCH_SIZE: '10' });
  });

  afterAll(async () => {
    await ctx.close();
    await queues.cleanup();
  });

  test('parar no meio do lote: a mensagem em andamento termina, as demais voltam para a fila, nada duplica', async () => {
    const wallet = await ctx.api.createWallet('100.00');
    for (let i = 0; i < 8; i++) {
      await sendWagerMessage(queues.wagerTransactions, betMessage(wallet, '1.00'));
    }
    const betsProcessed = async () =>
      Number(
        (
          await db.query<{ c: number }>(
            `SELECT COUNT(*)::int AS c FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET'`,
            [wallet.id],
          )
        ).rows[0]!.c,
      );

    const lock = await holdWalletLock(wallet.id);
    ctx.consumer.start();
    await waitFor(async () => (await approximateCounts(queues.wagerTransactions)).inFlight > 0, {
      description: 'batch received',
    });
    await sleep(500);
    const stopping = ctx.consumer.stop();
    await sleep(300);
    await lock.release();
    await stopping;

    expect(await betsProcessed()).toBe(1);
    await waitFor(async () => (await approximateCounts(queues.wagerTransactions)).visible > 0, {
      timeoutMs: 5_000,
      description: 'unstarted messages released immediately',
    });

    ctx.consumer.start();
    await waitFor(async () => (await betsProcessed()) === 8, { timeoutMs: 30_000, description: 'all 8 processed' });
    await waitFor(() => queueIsEmpty(queues.wagerTransactions), { timeoutMs: 30_000 });
    await ctx.consumer.stop();

    expect((await walletRow(wallet.id)).balance).toBe('92.00');
    expect(await ledgerRows(wallet.id)).toHaveLength(9);
    await expectWalletMatchesLedger(wallet.id);
  });
});
