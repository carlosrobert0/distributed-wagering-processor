import { randomUUID } from 'node:crypto';
import { PublishOutboxUseCase } from '../../src/application/messaging/publish-outbox.use-case';
import {
  ProcessingContext,
  ProcessWagerTransactionUseCase,
  SubmitWagerTransactionCommand,
} from '../../src/application/wagering/process-wager-transaction.use-case';
import { RetryPendingReferencesUseCase } from '../../src/application/wagering/retry-pending-references.use-case';
import { WagerSettlementService } from '../../src/application/wagering/wager-settlement.service';
import { WagerTransactionQueries } from '../../src/application/wagering/wager-transaction.queries';
import { CreateWalletUseCase } from '../../src/application/wallets/create-wallet.use-case';
import { ReconcileWalletUseCase } from '../../src/application/wallets/reconcile-wallet.use-case';
import { WalletQueries } from '../../src/application/wallets/wallet.queries';
import { WalletView } from '../../src/application/wallets/wallet.views';
import { ExponentialBackoff } from '../../src/domain/shared/exponential-backoff';
import {
  FixedClock,
  InMemoryUnitOfWork,
  RandomIds,
  RecordingMetrics,
  RecordingLogger,
  RecordingPublisher,
} from './in-memory';

export const HTTP: ProcessingContext = { correlationId: 'test', source: 'http' };

export function buildInMemoryContext() {
  const uow = new InMemoryUnitOfWork();
  const clock = new FixedClock();
  const ids = new RandomIds();
  const metrics = new RecordingMetrics();
  const logger = new RecordingLogger();
  const publisher = new RecordingPublisher();
  const settlement = new WagerSettlementService(ids, clock, metrics, new ExponentialBackoff(1_000, 8_000, 3));

  const processWager = new ProcessWagerTransactionUseCase(uow, settlement, ids, clock, metrics, logger);
  const createWallet = new CreateWalletUseCase(uow, ids, clock, metrics, logger);

  return {
    uow,
    clock,
    metrics,
    logger,
    publisher,
    processWager,
    createWallet,
    walletQueries: new WalletQueries(uow),
    transactionQueries: new WagerTransactionQueries(uow),
    reconcile: new ReconcileWalletUseCase(uow, metrics, logger),
    retryPending: new RetryPendingReferencesUseCase(uow, settlement, clock, metrics, logger),
    publishOutbox: new PublishOutboxUseCase(uow, publisher, clock, metrics, logger),

    openWallet(amount = '100.00', currency = 'BRL'): Promise<WalletView> {
      return createWallet.execute({ playerId: randomUUID(), initialBalance: { amount, currency } }, HTTP);
    },

    command(
      wallet: { id: string; playerId: string },
      overrides: Partial<SubmitWagerTransactionCommand> = {},
    ): SubmitWagerTransactionCommand {
      const externalTransactionId = overrides.externalTransactionId ?? `tx-${randomUUID()}`;
      return {
        providerId: 'provider-a',
        externalTransactionId,
        idempotencyKey: `provider-a:${externalTransactionId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-1',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '10.00', currency: 'BRL' },
        ...overrides,
      };
    },
  };
}

export type InMemoryContext = ReturnType<typeof buildInMemoryContext>;
