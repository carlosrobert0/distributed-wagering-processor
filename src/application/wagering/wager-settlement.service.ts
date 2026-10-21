import { EventContext, IntegrationEvent } from '../../domain/events/integration-event';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../../domain/events/wager-events';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { ExponentialBackoff } from '../../domain/shared/exponential-backoff';
import {
  DEFAULT_REFERENCE_RETRY_POLICY,
  ReferenceLookup,
  settleWagerTransaction,
  SettlementOutcome,
} from '../../domain/wagering/wager-settlement';
import { WagerTransaction } from '../../domain/wagering/wager-transaction';
import { Wallet } from '../../domain/wallet/wallet';
import { Clock, IdGenerator, Metrics, ProcessingSource } from '../ports/system';
import { TransactionScope } from '../ports/unit-of-work';

export interface SettlementContext {
  correlationId: string;
  causationId?: string;
  source: ProcessingSource;
}

/**
 * Orquestra uma liquidação dentro de uma transação SQL já aberta e com a wallet travada:
 * busca a referência, chama o serviço de domínio e persiste — transação, lançamento, saldo e
 * eventos de outbox — no mesmo escopo (AC-OUT-01). Compartilhado pela entrada (HTTP/SQS) e pelo
 * worker de PENDING_REFERENCE, garantindo que ambos apliquem exatamente as mesmas regras.
 */
export class WagerSettlementService {
  constructor(
    private readonly ids: IdGenerator,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly referenceRetryPolicy: ExponentialBackoff = DEFAULT_REFERENCE_RETRY_POLICY,
  ) {}

  async settle(
    scope: TransactionScope,
    wallet: Wallet,
    transaction: WagerTransaction,
    ctx: SettlementContext,
    persistence: 'insert' | 'update',
  ): Promise<SettlementOutcome> {
    const now = this.clock.now();
    const versionBefore = wallet.version;
    const reference = transaction.hasReference() ? await this.lookupReference(scope, transaction) : undefined;

    const outcome = settleWagerTransaction({
      wallet,
      transaction,
      reference,
      ledgerEntryId: this.ids.next(),
      now,
      referenceRetryPolicy: this.referenceRetryPolicy,
    });

    // Ordem importa: a transação existe antes do lançamento que a referencia (FK).
    if (persistence === 'insert') {
      await scope.transactions.insert(transaction);
    } else {
      await scope.transactions.update(transaction);
    }
    if (outcome.type === 'processed' && outcome.ledgerEntry) {
      await scope.ledger.append(outcome.ledgerEntry);
      await scope.wallets.saveBalance(wallet, versionBefore);
    }

    const events = this.eventsFor(outcome, transaction, wallet, {
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: now,
      newEventId: () => this.ids.next(),
    });
    await scope.outbox.enqueue(events.map((event) => OutboxMessage.enqueue(event)));

    if (transaction.isTerminal()) {
      // Dependentes que aguardam esta transação são verificadas já, sem esperar o backoff.
      await scope.transactions.wakeDependents(transaction.providerId, transaction.externalTransactionId, now);
    }
    if (outcome.type !== 'pending-reference' || outcome.firstTime) {
      this.metrics.transactionSettled(transaction.status, transaction.kind, ctx.source);
    }
    return outcome;
  }

  private async lookupReference(scope: TransactionScope, transaction: WagerTransaction): Promise<ReferenceLookup> {
    const referenceId = transaction.referenceExternalTransactionId;
    if (referenceId === undefined) {
      return { found: false };
    }
    const reference = await scope.transactions.findByExternalId(transaction.providerId, referenceId);
    if (!reference) {
      return { found: false };
    }
    const alreadyReversed = transaction.isReversal()
      ? await scope.transactions.hasProcessedReversal(reference.id)
      : false;
    return { found: true, transaction: reference, alreadyReversed };
  }

  private eventsFor(
    outcome: SettlementOutcome,
    transaction: WagerTransaction,
    wallet: Wallet,
    ctx: EventContext,
  ): IntegrationEvent<unknown>[] {
    switch (outcome.type) {
      case 'processed':
        return outcome.ledgerEntry
          ? [WagerTransactionProcessed.from(transaction, ctx), WalletBalanceChanged.from(wallet, outcome.ledgerEntry, ctx)]
          : [WagerTransactionProcessed.from(transaction, ctx)];
      case 'rejected':
        return [WagerTransactionRejected.from(transaction, ctx)];
      case 'pending-reference':
        return outcome.firstTime ? [WagerTransactionPendingReference.from(transaction, ctx)] : [];
    }
  }
}
