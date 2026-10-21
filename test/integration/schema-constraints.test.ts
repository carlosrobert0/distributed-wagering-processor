import { beforeAll, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { PoolClient } from 'pg';
import { prepareTestDatabase } from '../support/environment';
import { db, inTransaction } from '../support/database';

interface Seed {
  walletId: string;
  playerId: string;
  betId: string;
}

async function seedWallet(): Promise<Seed> {
  const walletId = randomUUID();
  const playerId = randomUUID();
  const openingId = randomUUID();
  const betId = randomUUID();
  await inTransaction(async (c) => {
    await c.query(
      `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
       VALUES ($1, $2, 'BRL', 70.00, 2, now(), now())`,
      [walletId, playerId],
    );
    await insertTx(c, {
      id: openingId,
      walletId,
      playerId,
      kind: 'OPENING',
      amount: '100.00',
      providerId: 'internal',
      balanceAfter: '100.00',
    });
    await insertTx(c, { id: betId, walletId, playerId, kind: 'BET', amount: '30.00', balanceAfter: '70.00' });
    await insertEntry(c, {
      walletId,
      transactionId: openingId,
      version: 1,
      direction: 'CREDIT',
      amount: '100.00',
      before: '0.00',
      after: '100.00',
    });
    await insertEntry(c, {
      walletId,
      transactionId: betId,
      version: 2,
      direction: 'DEBIT',
      amount: '30.00',
      before: '100.00',
      after: '70.00',
    });
  });
  return { walletId, playerId, betId };
}

interface TxSeed {
  id: string;
  walletId: string;
  playerId: string;
  kind: string;
  amount: string;
  providerId?: string;
  status?: string;
  balanceAfter?: string | null;
  referenceExternalId?: string | null;
  referenceId?: string | null;
  idempotencyKey?: string;
  externalId?: string;
}

async function insertTx(c: PoolClient, tx: TxSeed): Promise<void> {
  const externalId = tx.externalId ?? `ext-${tx.id}`;
  const providerId = tx.providerId ?? 'provider-a';
  await c.query(
    `INSERT INTO wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash,
       wallet_id, player_id, round_id, game_id, kind, amount, currency, reference_external_transaction_id,
       reference_transaction_id, status, balance_after, created_at, processed_at, updated_at)
     VALUES ($1, $2, $3, $4, repeat('a', 64), $5, $6, 'round-1', 'game', $7, $8, 'BRL', $9, $10, $11, $12, now(), now(), now())`,
    [
      tx.id,
      providerId,
      externalId,
      tx.idempotencyKey ?? `${providerId}:${externalId}`,
      tx.walletId,
      tx.playerId,
      tx.kind,
      tx.amount,
      tx.referenceExternalId ?? null,
      tx.referenceId ?? null,
      tx.status ?? 'PROCESSED',
      tx.balanceAfter === undefined ? '0.00' : tx.balanceAfter,
    ],
  );
}

interface EntrySeed {
  walletId: string;
  transactionId: string;
  version: number;
  direction: string;
  amount: string;
  before: string;
  after: string;
  currency?: string;
}

async function insertEntry(c: PoolClient, e: EntrySeed): Promise<void> {
  await c.query(
    `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, wallet_version, direction, amount, currency,
       balance_before, balance_after, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())`,
    [
      randomUUID(),
      e.walletId,
      e.transactionId,
      e.version,
      e.direction,
      e.amount,
      e.currency ?? 'BRL',
      e.before,
      e.after,
    ],
  );
}

async function expectRejected(work: (c: PoolClient) => Promise<unknown>, pattern: RegExp): Promise<void> {
  let failure: unknown;
  try {
    await inTransaction(async (c) => {
      await work(c);
    });
  } catch (error) {
    failure = error;
  }
  expect(failure).toBeDefined();
  expect(String((failure as Error).message)).toMatch(pattern);
}

describe('constraints do schema (§5 restrição 9)', () => {
  let seed: Seed;

  beforeAll(async () => {
    await prepareTestDatabase();
    seed = await seedWallet();
  });

  describe('wallets', () => {
    test('saldo negativo é recusado (CHECK)', async () => {
      await expectRejected(
        (c) => c.query(`UPDATE wallets SET balance = -0.01 WHERE id = $1`, [seed.walletId]),
        /wallets_balance_non_negative_ck/,
      );
    });

    test('segunda wallet para o mesmo player+moeda é recusada (UNIQUE)', async () => {
      await expectRejected(
        (c) =>
          c.query(
            `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
             VALUES ($1, $2, 'BRL', 0, 1, now(), now())`,
            [randomUUID(), seed.playerId],
          ),
        /wallets_player_currency_uq/,
      );
    });

    test('mudar o saldo sem lançamento no ledger falha no COMMIT (constraint trigger diferida)', async () => {
      await expectRejected(
        (c) => c.query(`UPDATE wallets SET balance = 1000.00 WHERE id = $1`, [seed.walletId]),
        /diverges from its ledger/,
      );
    });

    test('wallet não pode ser apagada', async () => {
      await expectRejected((c) => c.query(`DELETE FROM wallets WHERE id = $1`, [seed.walletId]), /append-only/);
    });
  });

  describe('wallet_ledger_entries (imutável e auditável)', () => {
    test('UPDATE é bloqueado', async () => {
      await expectRejected(
        (c) => c.query(`UPDATE wallet_ledger_entries SET amount = 1 WHERE wallet_id = $1`, [seed.walletId]),
        /append-only/,
      );
    });

    test('DELETE é bloqueado', async () => {
      await expectRejected(
        (c) => c.query(`DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, [seed.walletId]),
        /append-only/,
      );
    });

    test('TRUNCATE é bloqueado', async () => {
      await expectRejected((c) => c.query(`TRUNCATE wallet_ledger_entries CASCADE`), /append-only/);
    });

    test('aritmética errada é recusada (CHECK balance_after = before ± amount)', async () => {
      await expectRejected(async (c) => {
        const txId = randomUUID();
        await insertTx(c, {
          id: txId,
          walletId: seed.walletId,
          playerId: seed.playerId,
          kind: 'WIN',
          amount: '5.00',
          balanceAfter: '75.00',
        });
        await insertEntry(c, {
          walletId: seed.walletId,
          transactionId: txId,
          version: 3,
          direction: 'CREDIT',
          amount: '5.00',
          before: '70.00',
          after: '76.00',
        });
      }, /ledger_arithmetic_ck/);
    });

    test('encadeamento quebrado é recusado (before ≠ after anterior)', async () => {
      await expectRejected(async (c) => {
        const txId = randomUUID();
        await insertTx(c, {
          id: txId,
          walletId: seed.walletId,
          playerId: seed.playerId,
          kind: 'WIN',
          amount: '5.00',
          balanceAfter: '55.00',
        });
        await insertEntry(c, {
          walletId: seed.walletId,
          transactionId: txId,
          version: 3,
          direction: 'CREDIT',
          amount: '5.00',
          before: '50.00',
          after: '55.00',
        });
      }, /ledger chain broken/);
    });

    test('mesma versão da wallet duas vezes é recusada (prova de ausência de lost update)', async () => {
      await expectRejected(async (c) => {
        const txId = randomUUID();
        await insertTx(c, {
          id: txId,
          walletId: seed.walletId,
          playerId: seed.playerId,
          kind: 'BET',
          amount: '1.00',
          balanceAfter: '69.00',
        });
        await insertEntry(c, {
          walletId: seed.walletId,
          transactionId: txId,
          version: 2,
          direction: 'DEBIT',
          amount: '1.00',
          before: '70.00',
          after: '69.00',
        });
      }, /ledger_wallet_version_uq|ledger chain broken/);
    });

    test('dois lançamentos para a mesma transação são recusados', async () => {
      await expectRejected(
        (c) =>
          insertEntry(c, {
            walletId: seed.walletId,
            transactionId: seed.betId,
            version: 3,
            direction: 'DEBIT',
            amount: '30.00',
            before: '70.00',
            after: '40.00',
          }),
        /ledger_transaction_wallet_uq/,
      );
    });

    test('lançamento em moeda diferente da wallet é recusado (FK composta)', async () => {
      await expectRejected(async (c) => {
        const txId = randomUUID();
        await insertTx(c, {
          id: txId,
          walletId: seed.walletId,
          playerId: seed.playerId,
          kind: 'WIN',
          amount: '5.00',
          balanceAfter: '75.00',
        });
        await insertEntry(c, {
          walletId: seed.walletId,
          transactionId: txId,
          version: 3,
          direction: 'CREDIT',
          amount: '5.00',
          before: '70.00',
          after: '75.00',
          currency: 'USD',
        });
      }, /ledger_wallet_currency_fk|does not match transaction/);
    });

    test('lançamento para transação REJECTED é recusado', async () => {
      await expectRejected(async (c) => {
        const txId = randomUUID();
        await c.query(
          `INSERT INTO wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash,
             wallet_id, player_id, round_id, game_id, kind, amount, currency, status, failure_code, balance_after,
             created_at, processed_at, updated_at)
           VALUES ($1, 'provider-a', $2, $2, repeat('a', 64), $3, $4, 'r', 'g', 'BET', 500.00, 'BRL', 'REJECTED',
             'INSUFFICIENT_FUNDS', 70.00, now(), now(), now())`,
          [txId, `ext-${txId}`, seed.walletId, seed.playerId],
        );
        await insertEntry(c, {
          walletId: seed.walletId,
          transactionId: txId,
          version: 3,
          direction: 'DEBIT',
          amount: '500.00',
          before: '70.00',
          after: '0.00',
        });
      }, /only PROCESSED|ledger_arithmetic_ck/);
    });
  });

  describe('wager_transactions', () => {
    test('idempotency key duplicada é recusada (UNIQUE)', async () => {
      await expectRejected(
        (c) =>
          insertTx(c, {
            id: randomUUID(),
            walletId: seed.walletId,
            playerId: seed.playerId,
            kind: 'LOSS',
            amount: '0.00',
            externalId: `other-${randomUUID()}`,
            idempotencyKey: `provider-a:ext-${seed.betId}`,
          }),
        /wager_tx_idempotency_key_uq/,
      );
    });

    test('(provider, external id) duplicado é recusado (UNIQUE)', async () => {
      await expectRejected(
        (c) =>
          insertTx(c, {
            id: randomUUID(),
            walletId: seed.walletId,
            playerId: seed.playerId,
            kind: 'LOSS',
            amount: '0.00',
            externalId: `ext-${seed.betId}`,
            idempotencyKey: `another-key-${randomUUID()}`,
          }),
        /wager_tx_provider_external_uq/,
      );
    });

    test('REFUND sem referência é recusado (CHECK)', async () => {
      await expectRejected(
        (c) =>
          insertTx(c, {
            id: randomUUID(),
            walletId: seed.walletId,
            playerId: seed.playerId,
            kind: 'REFUND',
            amount: '1.00',
          }),
        /wager_tx_reversal_requires_reference_ck|wager_tx_reference_resolved_ck/,
      );
    });

    test('BET com valor zero é recusada; LOSS com zero é aceita', async () => {
      await expectRejected(
        (c) =>
          insertTx(c, {
            id: randomUUID(),
            walletId: seed.walletId,
            playerId: seed.playerId,
            kind: 'BET',
            amount: '0.00',
          }),
        /wager_tx_amount_ck/,
      );
      await inTransaction((c) =>
        insertTx(c, {
          id: randomUUID(),
          walletId: seed.walletId,
          playerId: seed.playerId,
          kind: 'LOSS',
          amount: '0.00',
          balanceAfter: '70.00',
        }),
      );
    });

    test('a mesma referência não pode ser revertida duas vezes (índice único parcial)', async () => {
      await expectRejected(async (c) => {
        for (const kind of ['REFUND', 'ROLLBACK']) {
          await insertTx(c, {
            id: randomUUID(),
            walletId: seed.walletId,
            playerId: seed.playerId,
            kind,
            amount: '30.00',
            referenceExternalId: `ext-${seed.betId}`,
            referenceId: seed.betId,
            balanceAfter: '100.00',
          });
        }
      }, /wager_tx_single_reversal_uq/);
    });

    test('transação terminal não pode mudar de estado (trigger)', async () => {
      await expectRejected(
        (c) =>
          c.query(`UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'X' WHERE id = $1`, [seed.betId]),
        /is terminal/,
      );
    });

    test('transação não pode ser apagada', async () => {
      await expectRejected(
        (c) => c.query(`DELETE FROM wager_transactions WHERE id = $1`, [seed.betId]),
        /cannot be deleted/,
      );
    });

    test('REJECTED exige failure_code; PROCESSED não pode ter (CHECK)', async () => {
      await expectRejected(
        (c) =>
          insertTx(c, {
            id: randomUUID(),
            walletId: seed.walletId,
            playerId: seed.playerId,
            kind: 'BET',
            amount: '1.00',
            status: 'REJECTED',
          }),
        /wager_tx_failure_code_ck/,
      );
    });
  });

  test('o seed continua íntegro após todas as tentativas', async () => {
    const { rows } = await db.query<{ balance: string; version: number }>(
      `SELECT balance::text, version FROM wallets WHERE id = $1`,
      [seed.walletId],
    );
    expect(rows[0]).toEqual({ balance: '70.00', version: 2 });
  });
});
