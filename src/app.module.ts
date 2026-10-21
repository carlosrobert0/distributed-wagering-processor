import { MikroORM } from '@mikro-orm/postgresql';
import { DynamicModule, Module, OnApplicationShutdown, Provider } from '@nestjs/common';
import { APP_GUARD, Reflector } from '@nestjs/core';
import type { Logger } from 'pino';
import { PublishOutboxUseCase } from './application/messaging/publish-outbox.use-case';
import {
  APP_LOGGER,
  AppLogger,
  CLOCK,
  Clock,
  EVENT_PUBLISHER,
  EventPublisher,
  ID_GENERATOR,
  IdGenerator,
  METRICS,
  Metrics,
} from './application/ports/system';
import { UNIT_OF_WORK, UnitOfWork } from './application/ports/unit-of-work';
import { ProcessWagerTransactionUseCase } from './application/wagering/process-wager-transaction.use-case';
import { RetryPendingReferencesUseCase } from './application/wagering/retry-pending-references.use-case';
import { WagerSettlementService } from './application/wagering/wager-settlement.service';
import { WagerTransactionQueries } from './application/wagering/wager-transaction.queries';
import { CreateWalletUseCase } from './application/wallets/create-wallet.use-case';
import { ReconcileWalletUseCase } from './application/wallets/reconcile-wallet.use-case';
import { WalletQueries } from './application/wallets/wallet.queries';
import { ExponentialBackoff } from './domain/shared/exponential-backoff';
import { AppConfig } from './infrastructure/config/app-config';
import { createSqsClient, SqsQueues } from './infrastructure/messaging/sqs';
import { SqsEventPublisher } from './infrastructure/messaging/sqs-event-publisher';
import { PinoAppLogger } from './infrastructure/observability/pino-logger';
import { PrometheusMetrics } from './infrastructure/observability/prometheus-metrics';
import { ReadinessProbe } from './infrastructure/observability/readiness-probe';
import { createOrm } from './infrastructure/persistence/mikro-orm.config';
import { MikroOrmUnitOfWork } from './infrastructure/persistence/mikro-orm-unit-of-work';
import { SystemClock, UuidV7Generator } from './infrastructure/system/system-adapters';
import {
  AllowAllProviderAuthenticator,
  PROVIDER_AUTHENTICATOR,
  ProviderAuthenticator,
  ProviderAuthGuard,
} from './interfaces/http/auth.guard';
import { HealthController } from './interfaces/http/health.controller';
import { WageringController } from './interfaces/http/wagering.controller';
import { WalletsController } from './interfaces/http/wallets.controller';
import { WagerMessageHandler } from './interfaces/sqs/wager-message.handler';
import { WagerTransactionsConsumer } from './interfaces/sqs/wager-transactions.consumer';
import { BackgroundWorkers } from './interfaces/workers/background-workers';

class OrmLifecycle implements OnApplicationShutdown {
  constructor(private readonly orm: MikroORM) {}

  async onApplicationShutdown(): Promise<void> {
    await this.orm.close();
  }
}

@Module({})
export class AppModule {
  static register(config: AppConfig, rootLogger: Logger): DynamicModule {
    const providers: Provider[] = [
      { provide: APP_LOGGER, useValue: new PinoAppLogger(rootLogger) },
      { provide: CLOCK, useClass: SystemClock },
      { provide: ID_GENERATOR, useClass: UuidV7Generator },
      {
        provide: PrometheusMetrics,
        useValue: new PrometheusMetrics({
          instanceId: config.instanceId,
          collectDefaults: config.metricsCollectDefaults,
        }),
      },
      { provide: METRICS, useExisting: PrometheusMetrics },

      {
        provide: MikroORM,
        useFactory: () => createOrm({ url: config.database.url, poolMax: config.database.poolMax }),
      },
      { provide: OrmLifecycle, useFactory: (orm: MikroORM) => new OrmLifecycle(orm), inject: [MikroORM] },
      {
        provide: UNIT_OF_WORK,
        useFactory: (orm: MikroORM, metrics: Metrics) =>
          new MikroOrmUnitOfWork(
            orm,
            { lockTimeoutMs: config.database.lockTimeoutMs, statementTimeoutMs: config.database.statementTimeoutMs },
            metrics,
          ),
        inject: [MikroORM, METRICS],
      },
      { provide: SqsQueues, useFactory: () => new SqsQueues(createSqsClient(config.aws)) },
      {
        provide: EVENT_PUBLISHER,
        useFactory: (queues: SqsQueues) => new SqsEventPublisher(queues, config.queues.events),
        inject: [SqsQueues],
      },
      {
        provide: ReadinessProbe,
        useFactory: (orm: MikroORM, queues: SqsQueues) =>
          new ReadinessProbe(orm, queues, [config.queues.wagerTransactions, config.queues.events]),
        inject: [MikroORM, SqsQueues],
      },

      {
        provide: WagerSettlementService,
        useFactory: (ids: IdGenerator, clock: Clock, metrics: Metrics) =>
          new WagerSettlementService(
            ids,
            clock,
            metrics,
            new ExponentialBackoff(
              config.pendingReference.baseDelayMs,
              config.pendingReference.maxDelayMs,
              config.pendingReference.maxAttempts,
            ),
          ),
        inject: [ID_GENERATOR, CLOCK, METRICS],
      },
      {
        provide: ProcessWagerTransactionUseCase,
        useFactory: (
          uow: UnitOfWork,
          settlement: WagerSettlementService,
          ids: IdGenerator,
          clock: Clock,
          metrics: Metrics,
          logger: AppLogger,
        ) => new ProcessWagerTransactionUseCase(uow, settlement, ids, clock, metrics, logger),
        inject: [UNIT_OF_WORK, WagerSettlementService, ID_GENERATOR, CLOCK, METRICS, APP_LOGGER],
      },
      {
        provide: CreateWalletUseCase,
        useFactory: (uow: UnitOfWork, ids: IdGenerator, clock: Clock, metrics: Metrics, logger: AppLogger) =>
          new CreateWalletUseCase(uow, ids, clock, metrics, logger),
        inject: [UNIT_OF_WORK, ID_GENERATOR, CLOCK, METRICS, APP_LOGGER],
      },
      { provide: WalletQueries, useFactory: (uow: UnitOfWork) => new WalletQueries(uow), inject: [UNIT_OF_WORK] },
      {
        provide: WagerTransactionQueries,
        useFactory: (uow: UnitOfWork) => new WagerTransactionQueries(uow),
        inject: [UNIT_OF_WORK],
      },
      {
        provide: ReconcileWalletUseCase,
        useFactory: (uow: UnitOfWork, metrics: Metrics, logger: AppLogger) =>
          new ReconcileWalletUseCase(uow, metrics, logger),
        inject: [UNIT_OF_WORK, METRICS, APP_LOGGER],
      },
      {
        provide: PublishOutboxUseCase,
        useFactory: (uow: UnitOfWork, publisher: EventPublisher, clock: Clock, metrics: Metrics, logger: AppLogger) =>
          new PublishOutboxUseCase(uow, publisher, clock, metrics, logger),
        inject: [UNIT_OF_WORK, EVENT_PUBLISHER, CLOCK, METRICS, APP_LOGGER],
      },
      {
        provide: RetryPendingReferencesUseCase,
        useFactory: (
          uow: UnitOfWork,
          settlement: WagerSettlementService,
          clock: Clock,
          metrics: Metrics,
          logger: AppLogger,
        ) => new RetryPendingReferencesUseCase(uow, settlement, clock, metrics, logger),
        inject: [UNIT_OF_WORK, WagerSettlementService, CLOCK, METRICS, APP_LOGGER],
      },

      {
        provide: WagerMessageHandler,
        useFactory: (process: ProcessWagerTransactionUseCase, logger: AppLogger) =>
          new WagerMessageHandler(process, config.consumer.name, logger),
        inject: [ProcessWagerTransactionUseCase, APP_LOGGER],
      },
      {
        provide: WagerTransactionsConsumer,
        useFactory: (handler: WagerMessageHandler, queues: SqsQueues, metrics: Metrics, logger: AppLogger) =>
          new WagerTransactionsConsumer(
            handler,
            queues,
            {
              queueName: config.queues.wagerTransactions,
              dlqName: config.queues.wagerTransactionsDlq,
              waitTimeSeconds: config.consumer.waitTimeSeconds,
              visibilityTimeoutSeconds: config.consumer.visibilityTimeoutSeconds,
              batchSize: config.consumer.batchSize,
              retryBaseSeconds: config.consumer.retryBaseSeconds,
              retryMaxSeconds: config.consumer.retryMaxSeconds,
              shutdownGraceMs: config.consumer.shutdownGraceMs,
              faultInjection: config.faultInjection,
            },
            metrics,
            logger,
          ),
        inject: [WagerMessageHandler, SqsQueues, METRICS, APP_LOGGER],
      },
      {
        provide: BackgroundWorkers,
        useFactory: (
          consumer: WagerTransactionsConsumer,
          publishOutbox: PublishOutboxUseCase,
          retryPending: RetryPendingReferencesUseCase,
          logger: AppLogger,
        ) =>
          new BackgroundWorkers(
            consumer,
            publishOutbox,
            retryPending,
            {
              consumerEnabled: config.consumer.enabled,
              outboxRelay: config.outboxRelay,
              pendingReference: config.pendingReference,
            },
            logger,
          ),
        inject: [WagerTransactionsConsumer, PublishOutboxUseCase, RetryPendingReferencesUseCase, APP_LOGGER],
      },

      { provide: PROVIDER_AUTHENTICATOR, useClass: AllowAllProviderAuthenticator },
      {
        provide: APP_GUARD,
        useFactory: (reflector: Reflector, authenticator: ProviderAuthenticator) =>
          new ProviderAuthGuard(reflector, authenticator),
        inject: [Reflector, PROVIDER_AUTHENTICATOR],
      },
    ];

    return {
      module: AppModule,
      controllers: [WalletsController, WageringController, HealthController],
      providers,
    };
  }
}
