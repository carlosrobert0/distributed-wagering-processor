import { InboxMessage } from '../../domain/messaging/inbox-message';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { WagerTransaction } from '../../domain/wagering/wager-transaction';
import { Wallet } from '../../domain/wallet/wallet';
import { WalletLedgerEntry } from '../../domain/wallet/wallet-ledger-entry';

export interface WalletRepository {
  findById(id: string): Promise<Wallet | null>;
  findByIdForUpdate(id: string): Promise<Wallet | null>;
  findByIdForShare(id: string): Promise<Wallet | null>;
  insert(wallet: Wallet): Promise<void>;
  saveBalance(wallet: Wallet, expectedVersion: number): Promise<void>;
}

export interface PendingReferenceCandidate {
  transactionId: string;
  walletId: string;
}

export interface WagerTransactionRepository {
  findById(id: string): Promise<WagerTransaction | null>;
  findByIdempotencyKey(idempotencyKey: string): Promise<WagerTransaction | null>;
  findByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null>;
  insert(transaction: WagerTransaction): Promise<void>;
  update(transaction: WagerTransaction): Promise<void>;
  hasProcessedReversal(referenceTransactionId: string): Promise<boolean>;
  wakeDependents(providerId: string, externalTransactionId: string, now: Date): Promise<number>;
  claimDuePendingReferences(now: Date, limit: number, leaseUntil: Date): Promise<PendingReferenceCandidate[]>;
  lockPendingReference(id: string, leaseUntil: Date): Promise<WagerTransaction | null>;
}

export interface LedgerSummary {
  netAmount: string;
  entries: number;
  lastBalanceAfter?: string;
  lastWalletVersion?: number;
  chainBreaks: number;
}

export interface LedgerRepository {
  append(entry: WalletLedgerEntry): Promise<void>;
  page(walletId: string, afterVersion: number | undefined, limit: number): Promise<WalletLedgerEntry[]>;
  summarize(walletId: string): Promise<LedgerSummary>;
}

export interface InboxRepository {
  tryReceive(message: InboxMessage): Promise<boolean>;
  find(consumerName: string, messageId: string): Promise<InboxMessage | null>;
  markProcessed(message: InboxMessage): Promise<void>;
}

export interface OutboxStats {
  pending: number;
  oldestPendingOccurredAt?: Date;
}

export interface OutboxRepository {
  enqueue(messages: readonly OutboxMessage[]): Promise<void>;
  claimDue(now: Date, limit: number): Promise<OutboxMessage[]>;
  save(message: OutboxMessage, heldLease?: Date): Promise<void>;
  stats(): Promise<OutboxStats>;
}
