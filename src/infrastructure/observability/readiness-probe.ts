import { MikroORM } from '@mikro-orm/postgresql';
import { SqsQueues } from '../messaging/sqs';

export type DependencyStatus = 'up' | 'down';

export interface ReadinessReport {
  status: 'ok' | 'unavailable';
  checks: { postgres: DependencyStatus; sqs: DependencyStatus };
}

const CHECK_TIMEOUT_MS = 2_000;

export class ReadinessProbe {
  constructor(
    private readonly orm: MikroORM,
    private readonly queues: SqsQueues,
    private readonly queueNames: readonly string[],
  ) {}

  async check(): Promise<ReadinessReport> {
    const [postgres, sqs] = await Promise.all([
      this.probe(() => this.orm.em.fork().execute('SELECT 1')),
      this.probe(() => Promise.all(this.queueNames.map((name) => this.queues.ping(name)))),
    ]);
    return { status: postgres === 'up' && sqs === 'up' ? 'ok' : 'unavailable', checks: { postgres, sqs } };
  }

  private async probe(check: () => Promise<unknown>): Promise<DependencyStatus> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error('timeout')), CHECK_TIMEOUT_MS);
    });
    try {
      await Promise.race([check(), timeout]);
      return 'up';
    } catch {
      return 'down';
    } finally {
      clearTimeout(timer);
    }
  }
}
