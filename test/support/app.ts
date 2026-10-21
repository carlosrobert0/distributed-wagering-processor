import { MikroORM } from '@mikro-orm/postgresql';
import { INestApplication } from '@nestjs/common';
import type { Subprocess } from 'bun';
import { join } from 'node:path';
import { createApp } from '../../src/bootstrap';
import { WagerTransactionsConsumer } from '../../src/interfaces/sqs/wager-transactions.consumer';
import { PublishOutboxUseCase } from '../../src/application/messaging/publish-outbox.use-case';
import { RetryPendingReferencesUseCase } from '../../src/application/wagering/retry-pending-references.use-case';
import { ApiClient } from './http';
import { TestQueues, testConfig, testEnv, waitFor } from './environment';

export interface TestApp {
  app: INestApplication;
  api: ApiClient;
  orm: MikroORM;
  consumer: WagerTransactionsConsumer;
  outbox: PublishOutboxUseCase;
  pendingReferences: RetryPendingReferencesUseCase;
  close: () => Promise<void>;
}

/** Sobe a aplicação real (Nest + MikroORM + SQS) no mesmo processo, numa porta aleatória. */
export async function startTestApp(queues: TestQueues, overrides: Record<string, string> = {}): Promise<TestApp> {
  const app = await createApp(testConfig(queues, overrides), { shutdownHooks: false });
  await app.listen(0, '127.0.0.1');
  const url = (await app.getUrl()).replace('[::1]', '127.0.0.1');
  return {
    app,
    api: new ApiClient(url),
    orm: app.get(MikroORM),
    consumer: app.get(WagerTransactionsConsumer),
    outbox: app.get(PublishOutboxUseCase),
    pendingReferences: app.get(RetryPendingReferencesUseCase),
    close: () => app.close(),
  };
}

const PROJECT_ROOT = join(import.meta.dir, '..', '..');

export interface SpawnedInstance {
  name: string;
  api: ApiClient;
  process: Subprocess;
  /** SIGTERM (graceful) — espera o processo sair. */
  stop: () => Promise<number>;
  /** SIGKILL — morte abrupta, sem drenar nada. */
  kill: () => Promise<number>;
  exited: Promise<number>;
}

let nextPort = 39_000 + Math.floor(Math.random() * 1_000);

/**
 * Sobe uma instância REAL em um processo separado (`bun run src/main.ts`), como no deploy.
 * Usado nos testes de múltiplas instâncias, crash e reinício.
 */
export async function spawnInstance(
  name: string,
  queues: TestQueues,
  overrides: Record<string, string> = {},
  options: { waitReady?: boolean } = {},
): Promise<SpawnedInstance> {
  const port = nextPort++;
  const env = testEnv(queues, { PORT: String(port), INSTANCE_ID: name, ...overrides });
  const child = Bun.spawn(['bun', 'run', 'src/main.ts'], {
    cwd: PROJECT_ROOT,
    env: { ...process.env, ...env },
    stdout: process.env.TEST_INSTANCE_OUTPUT ? 'inherit' : 'ignore',
    stderr: process.env.TEST_INSTANCE_OUTPUT ? 'inherit' : 'ignore',
  });
  const api = new ApiClient(`http://127.0.0.1:${port}`);
  const instance: SpawnedInstance = {
    name,
    api,
    process: child,
    exited: child.exited,
    stop: async () => {
      child.kill('SIGTERM');
      return child.exited;
    },
    kill: async () => {
      child.kill('SIGKILL');
      return child.exited;
    },
  };
  if (options.waitReady ?? true) {
    await waitFor(
      async () => {
        const response = await fetch(`http://127.0.0.1:${port}/health/ready`);
        return response.ok;
      },
      { timeoutMs: 30_000, description: `${name} ready` },
    );
  }
  return instance;
}
