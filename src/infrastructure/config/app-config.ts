import { z } from 'zod';

const bool = z
  .enum(['true', 'false', '1', '0'])
  .transform((value) => value === 'true' || value === '1');

const EnvSchema = z.object({
  PORT: z.coerce.number().int().min(0).max(65535).default(3000),
  INSTANCE_ID: z.string().min(1).default(`instance-${process.pid}`),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  METRICS_COLLECT_DEFAULTS: bool.default('true'),

  DATABASE_URL: z.string().url().default('postgres://wagering:wagering@localhost:5432/wagering'),
  DB_POOL_MAX: z.coerce.number().int().min(1).default(20),
  DB_LOCK_TIMEOUT_MS: z.coerce.number().int().min(1).default(5_000),
  DB_STATEMENT_TIMEOUT_MS: z.coerce.number().int().min(1).default(15_000),

  AWS_REGION: z.string().default('us-east-1'),
  AWS_ENDPOINT_URL: z.string().url().optional(),
  AWS_ACCESS_KEY_ID: z.string().default('test'),
  AWS_SECRET_ACCESS_KEY: z.string().default('test'),
  SQS_WAGER_QUEUE_NAME: z.string().default('wager-transactions.fifo'),
  SQS_WAGER_DLQ_NAME: z.string().default('wager-transactions-dlq.fifo'),
  SQS_EVENTS_QUEUE_NAME: z.string().default('wagering-events.fifo'),

  CONSUMER_ENABLED: bool.default('true'),
  CONSUMER_NAME: z.string().default('wager-transactions-consumer'),
  CONSUMER_WAIT_TIME_SECONDS: z.coerce.number().int().min(0).max(20).default(10),
  CONSUMER_VISIBILITY_TIMEOUT_SECONDS: z.coerce.number().int().min(1).default(30),
  CONSUMER_BATCH_SIZE: z.coerce.number().int().min(1).max(10).default(10),
  CONSUMER_RETRY_BASE_SECONDS: z.coerce.number().int().min(0).default(2),
  CONSUMER_RETRY_MAX_SECONDS: z.coerce.number().int().min(0).default(120),
  CONSUMER_SHUTDOWN_GRACE_MS: z.coerce.number().int().min(0).default(20_000),

  OUTBOX_RELAY_ENABLED: bool.default('true'),
  OUTBOX_RELAY_INTERVAL_MS: z.coerce.number().int().min(10).default(500),
  OUTBOX_RELAY_BATCH_SIZE: z.coerce.number().int().min(1).default(50),

  PENDING_REFERENCE_WORKER_ENABLED: bool.default('true'),
  PENDING_REFERENCE_INTERVAL_MS: z.coerce.number().int().min(10).default(1_000),
  PENDING_REFERENCE_BATCH_SIZE: z.coerce.number().int().min(1).default(50),
  /** Base do backoff de PENDING_REFERENCE (ms). Testes reduzem para não esperar minutos. */
  PENDING_REFERENCE_BASE_DELAY_MS: z.coerce.number().int().min(1).default(2_000),
  PENDING_REFERENCE_MAX_DELAY_MS: z.coerce.number().int().min(1).default(300_000),
  PENDING_REFERENCE_MAX_ATTEMPTS: z.coerce.number().int().min(1).default(8),

  /**
   * Ponto de injeção de falha usado SOMENTE pelos testes de crash (§13, cenário 5):
   * `crash-after-commit` mata o processo depois do commit e antes do ack ao SQS.
   */
  FAULT_INJECTION: z.enum(['none', 'crash-after-commit']).default('none'),
});

export type AppConfig = ReturnType<typeof loadConfig>;

export function loadConfig(env: Record<string, string | undefined> = process.env) {
  const parsed = EnvSchema.safeParse(env);
  if (!parsed.success) {
    const issues = parsed.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`).join('; ');
    throw new Error(`Invalid configuration: ${issues}`);
  }
  const e = parsed.data;
  return {
    port: e.PORT,
    instanceId: e.INSTANCE_ID,
    logLevel: e.LOG_LEVEL,
    metricsCollectDefaults: e.METRICS_COLLECT_DEFAULTS,
    database: {
      url: e.DATABASE_URL,
      poolMax: e.DB_POOL_MAX,
      lockTimeoutMs: e.DB_LOCK_TIMEOUT_MS,
      statementTimeoutMs: e.DB_STATEMENT_TIMEOUT_MS,
    },
    aws: {
      region: e.AWS_REGION,
      endpoint: e.AWS_ENDPOINT_URL,
      accessKeyId: e.AWS_ACCESS_KEY_ID,
      secretAccessKey: e.AWS_SECRET_ACCESS_KEY,
    },
    queues: {
      wagerTransactions: e.SQS_WAGER_QUEUE_NAME,
      wagerTransactionsDlq: e.SQS_WAGER_DLQ_NAME,
      events: e.SQS_EVENTS_QUEUE_NAME,
    },
    consumer: {
      enabled: e.CONSUMER_ENABLED,
      name: e.CONSUMER_NAME,
      waitTimeSeconds: e.CONSUMER_WAIT_TIME_SECONDS,
      visibilityTimeoutSeconds: e.CONSUMER_VISIBILITY_TIMEOUT_SECONDS,
      batchSize: e.CONSUMER_BATCH_SIZE,
      retryBaseSeconds: e.CONSUMER_RETRY_BASE_SECONDS,
      retryMaxSeconds: e.CONSUMER_RETRY_MAX_SECONDS,
      shutdownGraceMs: e.CONSUMER_SHUTDOWN_GRACE_MS,
    },
    outboxRelay: {
      enabled: e.OUTBOX_RELAY_ENABLED,
      intervalMs: e.OUTBOX_RELAY_INTERVAL_MS,
      batchSize: e.OUTBOX_RELAY_BATCH_SIZE,
    },
    pendingReference: {
      enabled: e.PENDING_REFERENCE_WORKER_ENABLED,
      intervalMs: e.PENDING_REFERENCE_INTERVAL_MS,
      batchSize: e.PENDING_REFERENCE_BATCH_SIZE,
      baseDelayMs: e.PENDING_REFERENCE_BASE_DELAY_MS,
      maxDelayMs: e.PENDING_REFERENCE_MAX_DELAY_MS,
      maxAttempts: e.PENDING_REFERENCE_MAX_ATTEMPTS,
    },
    faultInjection: e.FAULT_INJECTION,
  };
}
