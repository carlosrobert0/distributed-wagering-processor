import { InboxMessage } from '../../domain/messaging/inbox-message';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { Money } from '../../domain/shared/money';
import { FailureCode, isFailureCode } from '../../domain/wagering/failure-code';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/wagering/wager-transaction';
import { Wallet } from '../../domain/wallet/wallet';
import { LedgerDirection, WalletLedgerEntry } from '../../domain/wallet/wallet-ledger-entry';
import {
  InboxMessageRecord,
  LedgerEntryRecord,
  OutboxMessageRecord,
  WagerTransactionRecord,
  WalletRecord,
} from './records';

/**
 * Tradução record ⇄ domínio. `toDomain` usa sempre `rehydrate` (não revalida transições, §6.0);
 * `Money.rehydrate` mantém a validação de formato, então um valor corrompido no banco falha alto.
 */

const money = (amount: string, currency: string): Money => Money.rehydrate({ amount, currency: currency.trim() });

export const WalletMapper = {
  toDomain(record: WalletRecord): Wallet {
    return Wallet.rehydrate({
      id: record.id,
      playerId: record.playerId,
      currency: record.currency,
      balance: money(record.balance, record.currency),
      version: record.version,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
    });
  },

  toRecord(wallet: Wallet): WalletRecord {
    return Object.assign(new WalletRecord(), {
      id: wallet.id,
      playerId: wallet.playerId,
      currency: wallet.currency,
      balance: wallet.balance.toJSON().amount,
      version: wallet.version,
      createdAt: wallet.createdAt,
      updatedAt: wallet.updatedAt,
    });
  },
};

export const WagerTransactionMapper = {
  toDomain(record: WagerTransactionRecord): WagerTransaction {
    const failureCode = record.failureCode ?? undefined;
    if (failureCode !== undefined && !isFailureCode(failureCode)) {
      throw new Error(`Unknown failure code persisted: ${failureCode}`);
    }
    return WagerTransaction.rehydrate({
      id: record.id,
      providerId: record.providerId,
      externalTransactionId: record.externalTransactionId,
      idempotencyKey: record.idempotencyKey,
      payloadHash: record.payloadHash,
      walletId: record.walletId,
      playerId: record.playerId,
      roundId: record.roundId,
      gameId: record.gameId,
      kind: record.kind as WagerTransactionKind,
      money: money(record.amount, record.currency),
      referenceExternalTransactionId: record.referenceExternalTransactionId ?? undefined,
      createdAt: record.createdAt,
      status: record.status as WagerTransactionStatus,
      referenceTransactionId: record.referenceTransactionId ?? undefined,
      failureCode: failureCode as FailureCode | undefined,
      processedAt: record.processedAt ?? undefined,
      balanceAfter: record.balanceAfter != null ? money(record.balanceAfter, record.currency) : undefined,
      referenceAttempts: record.referenceAttempts,
      nextReferenceCheckAt: record.nextReferenceCheckAt ?? undefined,
    });
  },

  /** Somente os campos que mudam ao longo da vida da transação (o trigger barra o resto). */
  toStateChange(transaction: WagerTransaction, now: Date): Partial<WagerTransactionRecord> {
    return {
      status: transaction.status,
      referenceTransactionId: transaction.referenceTransactionId ?? null,
      failureCode: transaction.failureCode ?? null,
      balanceAfter: transaction.balanceAfter?.toJSON().amount ?? null,
      referenceAttempts: transaction.referenceAttempts,
      nextReferenceCheckAt: transaction.nextReferenceCheckAt ?? null,
      processedAt: transaction.processedAt ?? null,
      updatedAt: now,
    };
  },

  toRecord(transaction: WagerTransaction, now: Date): WagerTransactionRecord {
    return Object.assign(new WagerTransactionRecord(), {
      id: transaction.id,
      providerId: transaction.providerId,
      externalTransactionId: transaction.externalTransactionId,
      idempotencyKey: transaction.idempotencyKey,
      payloadHash: transaction.payloadHash,
      walletId: transaction.walletId,
      playerId: transaction.playerId,
      roundId: transaction.roundId,
      gameId: transaction.gameId,
      kind: transaction.kind,
      amount: transaction.money.toJSON().amount,
      currency: transaction.money.currency,
      referenceExternalTransactionId: transaction.referenceExternalTransactionId ?? null,
      createdAt: transaction.createdAt,
      ...WagerTransactionMapper.toStateChange(transaction, now),
    });
  },
};

export const LedgerEntryMapper = {
  toDomain(record: LedgerEntryRecord): WalletLedgerEntry {
    return WalletLedgerEntry.rehydrate({
      id: record.id,
      walletId: record.walletId,
      transactionId: record.transactionId,
      walletVersion: record.walletVersion,
      direction: record.direction as LedgerDirection,
      money: money(record.amount, record.currency),
      balanceBefore: money(record.balanceBefore, record.currency),
      balanceAfter: money(record.balanceAfter, record.currency),
      createdAt: record.createdAt,
    });
  },

  toRecord(entry: WalletLedgerEntry): LedgerEntryRecord {
    return Object.assign(new LedgerEntryRecord(), {
      id: entry.id,
      walletId: entry.walletId,
      transactionId: entry.transactionId,
      walletVersion: entry.walletVersion,
      direction: entry.direction,
      amount: entry.money.toJSON().amount,
      currency: entry.money.currency,
      balanceBefore: entry.balanceBefore.toJSON().amount,
      balanceAfter: entry.balanceAfter.toJSON().amount,
      createdAt: entry.createdAt,
    });
  },
};

export const InboxMessageMapper = {
  toDomain(record: InboxMessageRecord): InboxMessage {
    return InboxMessage.rehydrate({
      consumerName: record.consumerName,
      messageId: record.messageId,
      payloadHash: record.payloadHash,
      receivedAt: record.receivedAt,
      processedAt: record.processedAt ?? undefined,
    });
  },
};

export const OutboxMessageMapper = {
  toDomain(record: OutboxMessageRecord): OutboxMessage {
    return OutboxMessage.rehydrate({
      id: record.id,
      aggregateId: record.aggregateId,
      partitionKey: record.partitionKey,
      eventType: record.eventType,
      payload: record.payload,
      occurredAt: record.occurredAt,
      attempts: record.attempts,
      nextAttemptAt: record.nextAttemptAt ?? undefined,
      publishedAt: record.publishedAt ?? undefined,
      lastError: record.lastError ?? undefined,
    });
  },

  toRecord(message: OutboxMessage): OutboxMessageRecord {
    return Object.assign(new OutboxMessageRecord(), {
      id: message.id,
      aggregateId: message.aggregateId,
      partitionKey: message.partitionKey,
      eventType: message.eventType,
      payload: message.payload as Record<string, unknown>,
      occurredAt: message.occurredAt,
      attempts: message.attempts,
      nextAttemptAt: message.nextAttemptAt ?? null,
      publishedAt: message.publishedAt ?? null,
      lastError: message.lastError ?? null,
    });
  },
};
