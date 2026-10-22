import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Client } from 'pg';
import { createOrm } from '../../src/infrastructure/persistence/mikro-orm.config';
import { POSTGRES_ADMIN_URL, recreateDatabase, withDatabase } from '../support/environment';

const DATABASE = 'wagering_migrations_test';
const URL = withDatabase(POSTGRES_ADMIN_URL, DATABASE);

const TABLES = [
  'consumer_message_attempts',
  'inbox_messages',
  'outbox_messages',
  'wager_transactions',
  'wallet_ledger_entries',
  'wallets',
];

async function tables(): Promise<string[]> {
  const client = new Client({ connectionString: URL });
  await client.connect();
  try {
    const { rows } = await client.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name <> 'mikro_orm_migrations' ORDER BY table_name`,
    );
    return rows.map((row) => row.table_name);
  } finally {
    await client.end();
  }
}

describe('migrations versionadas e reversíveis (§4, §13 integração)', () => {
  beforeAll(() => recreateDatabase(DATABASE));
  afterAll(async () => {
    const admin = new Client({ connectionString: POSTGRES_ADMIN_URL });
    await admin.connect();
    await admin.query(`DROP DATABASE IF EXISTS "${DATABASE}" WITH (FORCE)`);
    await admin.end();
  });

  test('up cria o schema, down remove tudo, up de novo recria', async () => {
    const orm = await createOrm({ url: URL, poolMax: 2 });
    try {
      const up = await orm.migrator.up();
      expect(up.map((m) => m.name)).toEqual([
        'Migration20261021000000_initial_schema',
        'Migration20261022000000_consumer_message_attempts',
      ]);
      expect(await tables()).toEqual(TABLES);

      expect(await orm.migrator.up()).toEqual([]);

      await orm.migrator.down({ to: 0 });
      expect(await tables()).toEqual([]);

      await orm.migrator.up();
      expect(await tables()).toEqual(TABLES);
    } finally {
      await orm.close(true);
    }
  });
});
