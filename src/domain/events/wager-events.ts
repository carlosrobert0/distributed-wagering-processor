import { InvariantViolationError } from '../shared/domain-error';
import { MoneyProps } from '../shared/money';
import { FailureCode } from '../wagering/failure-code';
import { WagerTransaction, WagerTransactionKind } from '../wagering/wager-transaction';
import { Wallet } from '../wallet/wallet';
import { LedgerDirection, WalletLedgerEntry } from '../wallet/wallet-ledger-entry';
import { EventContext, IntegrationEvent } from './integration-event';

interface WagerTransactionIdentity {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
}

function identityOf(transaction: WagerTransaction): WagerTransactionIdentity {
  return {
    transactionId: transaction.id,
    providerId: transaction.providerId,
    externalTransactionId: transaction.externalTransactionId,
    walletId: transaction.walletId,
    playerId: transaction.playerId,
    roundId: transaction.roundId,
    gameId: transaction.gameId,
    kind: transaction.kind,
    money: transaction.money.toJSON(),
  };
}

function requireDate(value: Date | undefined, field: string): string {
  if (!value) {
    throw new InvariantViolationError(`Cannot build event: ${field} is missing`);
  }
  return value.toISOString();
}

export interface WagerTransactionProcessedData extends WagerTransactionIdentity {
  referenceTransactionId?: string;
  balanceAfter: MoneyProps;
  processedAt: string;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  static from(transaction: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    if (!transaction.balanceAfter) {
      throw new InvariantViolationError('Processed transaction must carry the observed balance');
    }
    return new WagerTransactionProcessed({
      eventId: ctx.newEventId(),
      aggregateId: transaction.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt,
      data: {
        ...identityOf(transaction),
        ...(transaction.referenceTransactionId ? { referenceTransactionId: transaction.referenceTransactionId } : {}),
        balanceAfter: transaction.balanceAfter.toJSON(),
        processedAt: requireDate(transaction.processedAt, 'processedAt'),
      },
    });
  }

  override get partitionKey(): string {
    return this.data.walletId;
  }
}

export interface WagerTransactionRejectedData extends WagerTransactionIdentity {
  failureCode: FailureCode;
  referenceExternalTransactionId?: string;
  rejectedAt: string;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  static from(transaction: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    if (!transaction.failureCode) {
      throw new InvariantViolationError('Rejected transaction must carry a failure code');
    }
    return new WagerTransactionRejected({
      eventId: ctx.newEventId(),
      aggregateId: transaction.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt,
      data: {
        ...identityOf(transaction),
        failureCode: transaction.failureCode,
        ...(transaction.referenceExternalTransactionId
          ? { referenceExternalTransactionId: transaction.referenceExternalTransactionId }
          : {}),
        rejectedAt: requireDate(transaction.processedAt, 'processedAt'),
      },
    });
  }

  override get partitionKey(): string {
    return this.data.walletId;
  }
}

export interface WagerTransactionPendingReferenceData extends WagerTransactionIdentity {
  referenceExternalTransactionId: string;
  nextCheckAt: string;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  static from(transaction: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    if (!transaction.referenceExternalTransactionId) {
      throw new InvariantViolationError('Pending reference event requires a reference');
    }
    return new WagerTransactionPendingReference({
      eventId: ctx.newEventId(),
      aggregateId: transaction.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt,
      data: {
        ...identityOf(transaction),
        referenceExternalTransactionId: transaction.referenceExternalTransactionId,
        nextCheckAt: requireDate(transaction.nextReferenceCheckAt, 'nextReferenceCheckAt'),
      },
    });
  }

  override get partitionKey(): string {
    return this.data.walletId;
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    if (entry.walletId !== wallet.id || entry.walletVersion !== wallet.version) {
      throw new InvariantViolationError('Ledger entry does not correspond to the current wallet state');
    }
    return new WalletBalanceChanged({
      eventId: ctx.newEventId(),
      aggregateId: wallet.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: entry.walletVersion,
      },
    });
  }
}
