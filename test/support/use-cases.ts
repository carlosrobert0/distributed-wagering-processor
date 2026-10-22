import { MikroORM } from '@mikro-orm/postgresql';
import pino from 'pino';
import { TransactionScope, UnitOfWork } from '../../src/application/ports/unit-of-work';
import { ProcessWagerTransactionUseCase } from '../../src/application/wagering/process-wager-transaction.use-case';
import { RetryPendingReferencesUseCase } from '../../src/application/wagering/retry-pending-references.use-case';
import { WagerSettlementService } from '../../src/application/wagering/wager-settlement.service';
import { CreateWalletUseCase } from '../../src/application/wallets/create-wallet.use-case';
import { ExponentialBackoff } from '../../src/domain/shared/exponential-backoff';
import { PinoAppLogger } from '../../src/infrastructure/observability/pino-logger';
import { PrometheusMetrics } from '../../src/infrastructure/observability/prometheus-metrics';
import { createOrm } from '../../src/infrastructure/persistence/mikro-orm.config';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/persistence/mikro-orm-unit-of-work';
import { SystemClock, UuidV7Generator } from '../../src/infrastructure/system/system-adapters';
import { testDatabaseUrl } from './environment';

export async function buildUseCases(
  options: { lockTimeoutMs?: number; referencePolicy?: ExponentialBackoff; poolMax?: number } = {},
) {
  const orm: MikroORM = await createOrm({ url: testDatabaseUrl(), poolMax: options.poolMax ?? 20 });
  const metrics = new PrometheusMetrics({ collectDefaults: false });
  const logger = new PinoAppLogger(pino({ level: process.env.TEST_LOG_LEVEL ?? 'silent' }));
  const ids = new UuidV7Generator();
  const clock = new SystemClock();
  const uow = new MikroOrmUnitOfWork(
    orm,
    { lockTimeoutMs: options.lockTimeoutMs ?? 5_000, statementTimeoutMs: 15_000 },
    metrics,
  );
  const settlement = new WagerSettlementService(ids, clock, metrics, options.referencePolicy);

  return {
    orm,
    uow,
    metrics,
    logger,
    ids,
    clock,
    settlement,
    createWallet: new CreateWalletUseCase(uow, ids, clock, metrics, logger),
    processWager: (unitOfWork: UnitOfWork = uow) =>
      new ProcessWagerTransactionUseCase(unitOfWork, settlement, ids, clock, metrics, logger),
    retryPending: new RetryPendingReferencesUseCase(uow, settlement, clock, metrics, logger),
    close: () => orm.close(true),
  };
}

export class FailAfterOutboxUnitOfWork implements UnitOfWork {
  constructor(private readonly inner: UnitOfWork) {}

  run<T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> {
    return this.inner.run((scope) => {
      const outbox = scope.outbox;
      const sabotaged: TransactionScope = {
        ...scope,
        outbox: {
          enqueue: async (messages) => {
            await outbox.enqueue(messages);
            throw new Error('simulated crash after writing the outbox, before commit');
          },
          claimDue: (now, limit) => outbox.claimDue(now, limit),
          save: (message) => outbox.save(message),
          stats: () => outbox.stats(),
        },
      };
      return work(sabotaged);
    });
  }
}
