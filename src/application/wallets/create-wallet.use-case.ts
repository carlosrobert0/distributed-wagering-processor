import { WagerTransactionProcessed, WalletBalanceChanged } from '../../domain/events/wager-events';
import { IntegrationEvent } from '../../domain/events/integration-event';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { Money, MoneyProps } from '../../domain/shared/money';
import { WagerTransaction, WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { Wallet } from '../../domain/wallet/wallet';
import { AppLogger, Clock, IdGenerator, Metrics } from '../ports/system';
import { UnitOfWork } from '../ports/unit-of-work';
import { asPayloadError } from '../validation';
import { canonicalJson, sha256Hex } from '../wagering/payload-hash';
import { toWalletView, WalletView } from './wallet.views';

export interface CreateWalletCommand {
  playerId: string;
  initialBalance: MoneyProps;
}

/**
 * Abre a wallet e, se houver saldo inicial, cria a transação interna OPENING e o lançamento
 * CREDIT correspondente na MESMA transação SQL (AC-WAL-06). Duplicidade (playerId, currency)
 * é barrada pelo UNIQUE do banco e devolvida como conflito (AC-WAL-01).
 */
export class CreateWalletUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly ids: IdGenerator,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async execute(command: CreateWalletCommand, ctx: { correlationId: string }): Promise<WalletView> {
    const initialBalance = asPayloadError(() => Money.from(command.initialBalance));

    const wallet = await this.uow.run(async (scope) => {
      const now = this.clock.now();
      const { wallet, openingEntry } = asPayloadError(() =>
        Wallet.open({
          id: this.ids.next(),
          playerId: command.playerId,
          initialBalance,
          at: now,
          openingTransactionId: this.ids.next(),
          openingLedgerEntryId: this.ids.next(),
        }),
      );
      await scope.wallets.insert(wallet);

      if (openingEntry) {
        const opening = WagerTransaction.opening({
          id: openingEntry.transactionId,
          walletId: wallet.id,
          playerId: wallet.playerId,
          money: initialBalance,
          payloadHash: sha256Hex(
            canonicalJson({ kind: 'OPENING', walletId: wallet.id, playerId: wallet.playerId, money: initialBalance.toJSON() }),
          ),
          createdAt: now,
        });
        opening.markProcessed({ referenceTransactionId: undefined, balanceAfter: wallet.balance, at: now });
        await scope.transactions.insert(opening);
        await scope.ledger.append(openingEntry);

        const eventContext = { correlationId: ctx.correlationId, occurredAt: now, newEventId: () => this.ids.next() };
        const events: IntegrationEvent<unknown>[] = [
          WagerTransactionProcessed.from(opening, eventContext),
          WalletBalanceChanged.from(wallet, openingEntry, eventContext),
        ];
        await scope.outbox.enqueue(events.map((event) => OutboxMessage.enqueue(event)));
        this.metrics.transactionSettled(WagerTransactionStatus.Processed, opening.kind, 'http');
      }
      return wallet;
    });

    this.logger.info('wallet opened', { correlationId: ctx.correlationId, walletId: wallet.id });
    return toWalletView(wallet);
  }
}
