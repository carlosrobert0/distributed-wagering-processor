import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { prepareTestDatabase } from '../support/environment';
import { startTestApp, TestApp } from '../support/app';
import { db, expectWalletMatchesLedger, ledgerRows, walletRow } from '../support/database';
import { wager } from '../support/http';
import { createTestQueues } from '../support/sqs';

describe('API HTTP', () => {
  let ctx: TestApp;
  let cleanupQueues: () => Promise<void>;

  beforeAll(async () => {
    await prepareTestDatabase();
    const queues = await createTestQueues();
    cleanupQueues = queues.cleanup;
    ctx = await startTestApp(queues);
  });

  afterAll(async () => {
    await ctx.close();
    await cleanupQueues();
  });

  describe('POST /wallets (AC-API-01)', () => {
    test('201 com o formato do enunciado', async () => {
      const playerId = randomUUID();
      const response = await ctx.api.request('POST', '/wallets', {
        playerId,
        initialBalance: { amount: '1000.00', currency: 'BRL' },
      });
      expect(response.status).toBe(201);
      expect(response.body).toEqual({
        id: expect.any(String),
        playerId,
        balance: { amount: '1000.00', currency: 'BRL' },
        version: 1,
      });
    });

    test('wallet duplicada para player+moeda → 409 WALLET_ALREADY_EXISTS (AC-WAL-01)', async () => {
      const playerId = randomUUID();
      await ctx.api.createWallet('1.00', { playerId });
      const duplicate = await ctx.api.request('POST', '/wallets', {
        playerId,
        initialBalance: { amount: '5.00', currency: 'BRL' },
      });
      expect(duplicate.status).toBe(409);
      expect(duplicate.body).toMatchObject({ error: 'WALLET_ALREADY_EXISTS' });

      expect((await ctx.api.createWallet('5.00', { playerId, currency: 'USD' })).balance.amount).toBe('5.00');
    });

    test.each([
      ['amount numérico (number nunca é aceito)', { amount: 10, currency: 'BRL' }],
      ['3 casas decimais', { amount: '1.001', currency: 'BRL' }],
      ['notação científica', { amount: '1e3', currency: 'BRL' }],
      ['negativo', { amount: '-1.00', currency: 'BRL' }],
      ['moeda inválida', { amount: '1.00', currency: 'real' }],
    ])('payload inválido → 400 (%s)', async (_label, initialBalance) => {
      const response = await ctx.api.request('POST', '/wallets', { playerId: randomUUID(), initialBalance });
      expect(response.status).toBe(400);
      expect(response.body).toMatchObject({ error: 'INVALID_PAYLOAD' });
    });
  });

  describe('POST /wagering/transactions (AC-API-06, AC-IDEM)', () => {
    test('BET processada → 201 com saldo; replay idêntico → 200 com o MESMO resultado (regra 7)', async () => {
      const wallet = await ctx.api.createWallet('1000.00');
      const body = wager(wallet, { amount: '25.00' });

      const first = await ctx.api.submit(body);
      expect(first.status).toBe(201);
      expect(first.body).toEqual({
        transactionId: expect.any(String),
        status: 'PROCESSED',
        balance: { amount: '975.00', currency: 'BRL' },
        idempotentReplay: false,
      });

      await ctx.api.submit(wager(wallet, { amount: '100.00' }));

      const replay = await ctx.api.submit(body);
      expect(replay.status).toBe(200);
      expect(replay.body).toEqual({ ...first.body, idempotentReplay: true });
      expect(await ledgerRows(wallet.id)).toHaveLength(3);
      await expectWalletMatchesLedger(wallet.id);
    });

    test('mesma key com payload diferente → 409 IDEMPOTENCY_KEY_CONFLICT, nunca replay (AC-IDEM-04)', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      const body = wager(wallet, { amount: '10.00' });
      await ctx.api.submit(body);

      const conflict = await ctx.api.submit({ ...body, money: { amount: '10.01', currency: 'BRL' } });
      expect(conflict.status).toBe(409);
      expect(conflict.body).toMatchObject({ error: 'IDEMPOTENCY_KEY_CONFLICT' });
      expect((await walletRow(wallet.id)).balance).toBe('90.00');
    });

    test('mesmo external id com outra key → 409 EXTERNAL_TRANSACTION_CONFLICT (AC-IDEM-05)', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      const body = wager(wallet);
      await ctx.api.submit(body);
      const conflict = await ctx.api.submit(body, `different-key-${randomUUID()}`);
      expect(conflict.status).toBe(409);
      expect(conflict.body).toMatchObject({ error: 'EXTERNAL_TRANSACTION_CONFLICT' });
    });

    test('sem Idempotency-Key → 400 IDEMPOTENCY_KEY_REQUIRED (AC-IDEM-01)', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      const response = await ctx.api.submit(wager(wallet), null);
      expect(response.status).toBe(400);
      expect(response.body).toMatchObject({ error: 'IDEMPOTENCY_KEY_REQUIRED' });
    });

    test.each([
      ['OPENING não pode ser submetido (AC-TX-03)', { kind: 'OPENING' }],
      ['kind desconhecido', { kind: 'JACKPOT' }],
      ['REFUND sem referência (AC-TX-04)', { kind: 'REFUND' }],
      ['amount number', { money: { amount: 25 as unknown as string, currency: 'BRL' } }],
      ['amount com 3 casas', { money: { amount: '25.000', currency: 'BRL' } }],
      ['walletId não-UUID', { walletId: 'not-a-uuid' }],
    ])('payload inválido → 400: %s', async (_label, override) => {
      const wallet = await ctx.api.createWallet('100.00');
      const response = await ctx.api.submit({ ...wager(wallet), ...override });
      expect(response.status).toBe(400);
      expect(String(response.body.error)).toMatch(/INVALID_PAYLOAD/);
    });

    test('JSON malformado → 400', async () => {
      const response = await ctx.api.request('POST', '/wagering/transactions', '{"broken":', {
        'idempotency-key': 'k',
      });
      expect(response.status).toBe(400);
      expect(response.body).toEqual({ error: 'INVALID_PAYLOAD', message: 'Malformed JSON body' });
    });

    test('rejeição de negócio → 422 com failureCode; replay → 422 idêntico (AC-TX-13)', async () => {
      const wallet = await ctx.api.createWallet('5.00');
      const body = wager(wallet, { amount: '10.00' });
      const rejected = await ctx.api.submit(body);
      expect(rejected.status).toBe(422);
      expect(rejected.body).toMatchObject({
        status: 'REJECTED',
        failureCode: 'INSUFFICIENT_FUNDS',
        balance: { amount: '5.00', currency: 'BRL' },
        idempotentReplay: false,
      });
      const replay = await ctx.api.submit(body);
      expect(replay.status).toBe(422);
      expect(replay.body).toEqual({ ...rejected.body, idempotentReplay: true });
    });

    test('moeda diferente da wallet → 422 CURRENCY_MISMATCH', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      const response = await ctx.api.submit(wager(wallet, { money: { amount: '1.00', currency: 'USD' } }));
      expect(response.status).toBe(422);
      expect(response.body.failureCode).toBe('CURRENCY_MISMATCH');
    });

    test('wallet inexistente → 404 WALLET_NOT_FOUND (INT-04)', async () => {
      const response = await ctx.api.submit(wager({ id: randomUUID(), playerId: 'p' }));
      expect(response.status).toBe(404);
      expect(response.body).toMatchObject({ error: 'WALLET_NOT_FOUND' });
    });

    test('ciclo BET → WIN → LOSS: WIN credita, LOSS não move saldo nem version (AC-TX-14)', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      const bet = wager(wallet, { externalTransactionId: `bet-${randomUUID()}`, amount: '20.00' });
      expect((await ctx.api.submit(bet)).status).toBe(201);
      const win = await ctx.api.submit(
        wager(wallet, { kind: 'WIN', amount: '50.00', referenceExternalTransactionId: bet.externalTransactionId }),
      );
      expect(win.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '130.00' } });

      const before = await walletRow(wallet.id);
      const loss = await ctx.api.submit(wager(wallet, { kind: 'LOSS', amount: '0.00', roundId: 'round-2' }));
      expect(loss.status).toBe(201);
      expect(loss.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '130.00' } });
      expect(await walletRow(wallet.id)).toEqual(before);
      await expectWalletMatchesLedger(wallet.id);
    });

    test('correlation id é propagado na resposta', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      const response = await ctx.api.request('POST', '/wagering/transactions', wager(wallet), {
        'idempotency-key': `k-${randomUUID()}`,
        'x-correlation-id': 'corr-123',
      });
      expect(response.headers.get('x-correlation-id')).toBe('corr-123');
    });
  });

  describe('consultas (AC-API-02..05)', () => {
    test('GET wallet, GET transação por id e por (provider, external id)', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      const body = wager(wallet, { amount: '1.50' });
      const submitted = await ctx.api.submit(body);

      const fetched = await ctx.api.request('GET', `/wallets/${wallet.id}`);
      expect(fetched.body).toMatchObject({ id: wallet.id, balance: { amount: '98.50', currency: 'BRL' }, version: 2 });

      const byId = await ctx.api.request('GET', `/wagering/transactions/${submitted.body.transactionId}`);
      expect(byId.status).toBe(200);
      expect(byId.body).toMatchObject({
        transactionId: submitted.body.transactionId,
        status: 'PROCESSED',
        kind: 'BET',
      });

      const byExternal = await ctx.api.request(
        'GET',
        `/providers/${body.providerId}/wagering/transactions/${body.externalTransactionId}`,
      );
      expect(byExternal.body).toMatchObject({ transactionId: submitted.body.transactionId });
    });

    test('404 consistente para recursos inexistentes', async () => {
      expect((await ctx.api.request('GET', `/wallets/${randomUUID()}`)).status).toBe(404);
      expect((await ctx.api.request('GET', `/wagering/transactions/${randomUUID()}`)).status).toBe(404);
      expect((await ctx.api.request('GET', `/providers/x/wagering/transactions/nope`)).status).toBe(404);
      expect((await ctx.api.request('GET', `/wallets/not-a-uuid`)).status).toBe(400);
    });

    test('ledger paginado com cursor opaco e estável (AC-API-03)', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      for (let i = 0; i < 6; i++) {
        await ctx.api.submit(wager(wallet, { amount: '1.00' }));
      }
      const versions: number[] = [];
      let cursor: string | undefined;
      do {
        const page = await ctx.api.request<{ entries: { walletVersion: number }[]; nextCursor?: string }>(
          'GET',
          `/wallets/${wallet.id}/ledger?limit=3${cursor ? `&cursor=${cursor}` : ''}`,
        );
        expect(page.status).toBe(200);
        versions.push(...page.body.entries.map((entry) => entry.walletVersion));
        cursor = page.body.nextCursor;
      } while (cursor);
      expect(versions).toEqual([1, 2, 3, 4, 5, 6, 7]);

      expect((await ctx.api.request('GET', `/wallets/${wallet.id}/ledger?cursor=garbage`)).status).toBe(400);
      expect((await ctx.api.request('GET', `/wallets/${wallet.id}/ledger?limit=0`)).status).toBe(400);
    });
  });

  describe('reconciliação (AC-API-07)', () => {
    test('wallet consistente', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      await ctx.api.submit(wager(wallet, { amount: '25.00' }));
      const response = await ctx.api.request('POST', `/wallets/${wallet.id}/reconciliation`);
      expect(response.status).toBe(200);
      expect(response.body).toEqual({
        walletId: wallet.id,
        storedBalance: { amount: '75.00', currency: 'BRL' },
        calculatedBalance: { amount: '75.00', currency: 'BRL' },
        difference: { amount: '0.00', currency: 'BRL' },
        consistent: true,
        checkedEntries: 2,
        issues: [],
      });
    });

    test('divergência é sinalizada, contada em métrica e NÃO corrigida', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      const client = await db.connect();
      try {
        await client.query('BEGIN');
        await client.query('ALTER TABLE wallets DISABLE TRIGGER USER');
        await client.query(`UPDATE wallets SET balance = balance + 1.00 WHERE id = $1`, [wallet.id]);
        await client.query('ALTER TABLE wallets ENABLE TRIGGER USER');
        await client.query('COMMIT');
      } finally {
        client.release();
      }

      const response = await ctx.api.request('POST', `/wallets/${wallet.id}/reconciliation`);
      expect(response.body).toMatchObject({
        consistent: false,
        storedBalance: { amount: '101.00' },
        calculatedBalance: { amount: '100.00' },
        difference: { amount: '1.00' },
        issues: expect.arrayContaining(['STORED_BALANCE_DIFFERS_FROM_LEDGER']),
      });
      expect((await walletRow(wallet.id)).balance).toBe('101.00');

      const metrics = await fetch(`${ctx.api.baseUrl}/metrics`).then((r) => r.text());
      expect(metrics).toMatch(/wagering_reconciliations_total\{[^}]*result="mismatch"[^}]*\} [1-9]/);
    });
  });

  describe('health e métricas (AC-API-08/09)', () => {
    test('/health/live e /health/ready', async () => {
      expect((await ctx.api.request('GET', '/health/live')).body).toEqual({ status: 'ok' });
      const ready = await ctx.api.request('GET', '/health/ready');
      expect(ready.status).toBe(200);
      expect(ready.body).toEqual({ status: 'ok', checks: { postgres: 'up', sqs: 'up' } });
    });

    test('/metrics expõe amostras das métricas obrigatórias', async () => {
      const wallet = await ctx.api.createWallet('100.00');
      const bet = wager(wallet);
      await ctx.api.submit(bet);
      await ctx.api.submit(bet);
      const text = await fetch(`${ctx.api.baseUrl}/metrics`).then((r) => r.text());
      expect(text).toMatch(/wagering_transactions_total\{[^}]*status="PROCESSED"[^}]*\} [1-9]/);
      expect(text).toMatch(/wagering_duplicates_total\{[^}]*type="idempotent_replay"[^}]*\} [1-9]/);
      expect(text).toMatch(/wagering_processing_duration_seconds_count\{[^}]*source="http"[^}]*\} [1-9]/);
      expect(text).toMatch(/wagering_wallet_lock_wait_seconds_count(\{[^}]*\})? [1-9]/);
    });
  });
});
