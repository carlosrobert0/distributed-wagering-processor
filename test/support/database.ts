import { expect } from 'bun:test';
import { Pool, PoolClient } from 'pg';
import { TEST_DATABASE_URL } from './environment';

/**
 * Conexão direta ao banco de teste para ASSERÇÕES (fora da aplicação). Valores monetários são
 * lidos como texto (NUMERIC chega como string no driver) e comparados como string.
 */
export const db = new Pool({ connectionString: TEST_DATABASE_URL, max: 10 });

export interface WalletRow {
  id: string;
  balance: string;
  version: number;
}

export interface LedgerRow {
  wallet_version: number;
  direction: 'DEBIT' | 'CREDIT';
  amount: string;
  balance_before: string;
  balance_after: string;
  transaction_id: string;
}

export interface TransactionRow {
  id: string;
  external_transaction_id: string;
  kind: string;
  status: string;
  failure_code: string | null;
  balance_after: string | null;
  reference_transaction_id: string | null;
}

export async function walletRow(walletId: string): Promise<WalletRow> {
  const { rows } = await db.query<WalletRow>('SELECT id, balance::text, version FROM wallets WHERE id = $1', [walletId]);
  if (!rows[0]) throw new Error(`wallet ${walletId} not found`);
  return rows[0];
}

export async function ledgerRows(walletId: string): Promise<LedgerRow[]> {
  const { rows } = await db.query<LedgerRow>(
    `SELECT wallet_version, direction, amount::text, balance_before::text, balance_after::text, transaction_id
       FROM wallet_ledger_entries WHERE wallet_id = $1 ORDER BY wallet_version`,
    [walletId],
  );
  return rows;
}

export async function transactionRows(walletId: string): Promise<TransactionRow[]> {
  const { rows } = await db.query<TransactionRow>(
    `SELECT id, external_transaction_id, kind, status, failure_code, balance_after::text, reference_transaction_id
       FROM wager_transactions WHERE wallet_id = $1 ORDER BY created_at, id`,
    [walletId],
  );
  return rows;
}

export async function outboxEventTypes(aggregateIds: string[]): Promise<string[]> {
  const { rows } = await db.query<{ event_type: string }>(
    `SELECT event_type FROM outbox_messages WHERE aggregate_id = ANY($1) ORDER BY occurred_at, event_type`,
    [aggregateIds],
  );
  return rows.map((row) => row.event_type);
}

export async function countRows(sql: string, params: unknown[] = []): Promise<number> {
  const { rows } = await db.query<{ count: string }>(sql, params);
  return Number(rows[0]?.count ?? 0);
}

/**
 * INVARIANTE FINAL de todos os testes (§13):  wallet.balance == saldo reconstruído pelo ledger.
 * Também confere o encadeamento e a versão — tudo calculado em NUMERIC no banco.
 */
export async function expectWalletMatchesLedger(walletId: string): Promise<void> {
  const { rows } = await db.query<{
    balance: string;
    rebuilt: string;
    version: number;
    last_version: number | null;
    chain_breaks: number;
  }>(
    `WITH ordered AS (
       SELECT direction, amount, balance_before, wallet_version,
              LAG(balance_after) OVER (ORDER BY wallet_version) AS previous_after
         FROM wallet_ledger_entries WHERE wallet_id = $1)
     SELECT w.balance::text AS balance,
            COALESCE((SELECT SUM(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END) FROM ordered), 0)::numeric(22,2)::text AS rebuilt,
            w.version,
            (SELECT MAX(wallet_version) FROM ordered) AS last_version,
            (SELECT COUNT(*) FROM ordered WHERE previous_after IS NOT NULL AND balance_before <> previous_after)::int AS chain_breaks
       FROM wallets w WHERE w.id = $1`,
    [walletId],
  );
  const row = rows[0];
  expect(row).toBeDefined();
  expect(row!.rebuilt).toBe(row!.balance);
  expect(row!.chain_breaks).toBe(0);
  if (row!.last_version !== null) {
    expect(Number(row!.last_version)).toBe(row!.version);
  }
}

/** A mesma invariante para TODAS as wallets do banco de teste. */
export async function expectAllWalletsMatchLedger(): Promise<void> {
  const { rows } = await db.query<{ id: string; balance: string; rebuilt: string }>(
    `SELECT w.id, w.balance::text AS balance,
            COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0)::numeric(22,2)::text AS rebuilt
       FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
      GROUP BY w.id, w.balance
     HAVING w.balance <> COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END), 0)`,
  );
  expect(rows).toEqual([]);
}

/** Executa `fn` numa transação manual (usado para segurar locks e testar constraints). */
export async function inTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}
