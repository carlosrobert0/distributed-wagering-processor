import { randomUUID } from 'node:crypto';
import { Client } from 'pg';
import { AppConfig, loadConfig } from '../../src/infrastructure/config/app-config';
import { createOrm } from '../../src/infrastructure/persistence/mikro-orm.config';

export const POSTGRES_ADMIN_URL =
  process.env.TEST_POSTGRES_ADMIN_URL ?? 'postgres://wagering:wagering@localhost:5432/postgres';
export const TEST_DATABASE_NAME = process.env.TEST_DATABASE_NAME ?? 'wagering_test';
export const TEST_DATABASE_URL = withDatabase(POSTGRES_ADMIN_URL, TEST_DATABASE_NAME);
export const AWS_ENDPOINT_URL = process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566';

export function withDatabase(url: string, database: string): string {
  const parsed = new URL(url);
  parsed.pathname = `/${database}`;
  return parsed.toString();
}

let prepared: Promise<void> | undefined;

export function prepareTestDatabase(): Promise<void> {
  prepared ??= (async () => {
    await recreateDatabase(TEST_DATABASE_NAME);
    const orm = await createOrm({ url: TEST_DATABASE_URL, poolMax: 2 });
    try {
      await orm.migrator.up();
    } finally {
      await orm.close(true);
    }
  })();
  return prepared;
}

export async function recreateDatabase(name: string): Promise<void> {
  const admin = new Client({ connectionString: POSTGRES_ADMIN_URL });
  await admin.connect();
  try {
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.query(`CREATE DATABASE "${name}"`);
  } finally {
    await admin.end();
  }
}

export interface TestQueues {
  wagerTransactions: string;
  wagerTransactionsDlq: string;
  events: string;
}

export function testEnv(queues: TestQueues, overrides: Record<string, string> = {}): Record<string, string> {
  return {
    DATABASE_URL: TEST_DATABASE_URL,
    DB_POOL_MAX: '30',
    AWS_ENDPOINT_URL,
    AWS_REGION: 'us-east-1',
    AWS_ACCESS_KEY_ID: 'test',
    AWS_SECRET_ACCESS_KEY: 'test',
    SQS_WAGER_QUEUE_NAME: queues.wagerTransactions,
    SQS_WAGER_DLQ_NAME: queues.wagerTransactionsDlq,
    SQS_EVENTS_QUEUE_NAME: queues.events,
    LOG_LEVEL: process.env.TEST_LOG_LEVEL ?? 'silent',
    METRICS_COLLECT_DEFAULTS: 'false',
    CONSUMER_ENABLED: 'false',
    OUTBOX_RELAY_ENABLED: 'false',
    PENDING_REFERENCE_WORKER_ENABLED: 'false',
    CONSUMER_WAIT_TIME_SECONDS: '1',
    CONSUMER_SHUTDOWN_GRACE_MS: '5000',
    INSTANCE_ID: `test-${randomUUID().slice(0, 8)}`,
    PORT: '0',
    ...overrides,
  };
}

export function testConfig(queues: TestQueues, overrides: Record<string, string> = {}): AppConfig {
  return loadConfig(testEnv(queues, overrides));
}

export async function waitFor<T>(
  probe: () => Promise<T | undefined | false | null>,
  options: { timeoutMs?: number; intervalMs?: number; description?: string } = {},
): Promise<T> {
  const deadline = Date.now() + (options.timeoutMs ?? 15_000);
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await probe();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await sleep(options.intervalMs ?? 100);
  }
  throw new Error(
    `Timed out waiting for ${options.description ?? 'condition'}${lastError ? `: ${String(lastError)}` : ''}`,
  );
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
