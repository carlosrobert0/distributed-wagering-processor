import { randomUUID } from 'node:crypto';
import {
  ConcurrencyViolationError,
  DuplicateTransactionError,
  WalletAlreadyExistsError,
} from '../../src/application/errors';
import {
  InboxRepository,
  LedgerRepository,
  LedgerSummary,
  OutboxRepository,
  OutboxStats,
  PendingReferenceCandidate,
  WagerTransactionRepository,
  WalletRepository,
} from '../../src/application/ports/repositories';
import {
  AppLogger,
  Clock,
  EventPublisher,
  IdGenerator,
  Metrics,
  ProcessingSource,
} from '../../src/application/ports/system';
import { TransactionScope, UnitOfWork } from '../../src/application/ports/unit-of-work';
import { InboxMessage } from '../../src/domain/messaging/inbox-message';
import { OutboxMessage } from '../../src/domain/messaging/outbox-message';
import {
  WagerTransaction,
  WagerTransactionState,
  WagerTransactionStatus,
} from '../../src/domain/wagering/wager-transaction';
import { Wallet } from '../../src/domain/wallet/wallet';
import { WalletLedgerEntry } from '../../src/domain/wallet/wallet-ledger-entry';

function cloneWallet(wallet: Wallet): Wallet {
  return Wallet.rehydrate({
    id: wallet.id,
    playerId: wallet.playerId,
    currency: wallet.currency,
    balance: wallet.balance,
    version: wallet.version,
    createdAt: wallet.createdAt,
    updatedAt: wallet.updatedAt,
  });
}

function cloneTransaction(tx: WagerTransaction, overrides: Partial<WagerTransactionState> = {}): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    idempotencyKey: tx.idempotencyKey,
    payloadHash: tx.payloadHash,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    gameId: tx.gameId,
    kind: tx.kind,
    money: tx.money,
    referenceExternalTransactionId: tx.referenceExternalTransactionId,
    createdAt: tx.createdAt,
    status: tx.status,
    referenceTransactionId: tx.referenceTransactionId,
    failureCode: tx.failureCode,
    processedAt: tx.processedAt,
    balanceAfter: tx.balanceAfter,
    referenceAttempts: tx.referenceAttempts,
    nextReferenceCheckAt: tx.nextReferenceCheckAt,
    ...overrides,
  });
}

function cloneInbox(message: InboxMessage): InboxMessage {
  return InboxMessage.rehydrate({
    messageId: message.messageId,
    consumerName: message.consumerName,
    payloadHash: message.payloadHash,
    receivedAt: message.receivedAt,
    processedAt: message.processedAt,
  });
}

function cloneOutbox(message: OutboxMessage): OutboxMessage {
  return OutboxMessage.rehydrate({
    id: message.id,
    aggregateId: message.aggregateId,
    partitionKey: message.partitionKey,
    eventType: message.eventType,
    payload: message.payload,
    occurredAt: message.occurredAt,
    attempts: message.attempts,
    nextAttemptAt: message.nextAttemptAt,
    publishedAt: message.publishedAt,
    lastError: message.lastError,
  });
}

export class InMemoryStore {
  wallets = new Map<string, Wallet>();
  transactions = new Map<string, WagerTransaction>();
  ledger: WalletLedgerEntry[] = [];
  inbox = new Map<string, InboxMessage>();
  outbox = new Map<string, OutboxMessage>();

  clone(): InMemoryStore {
    const copy = new InMemoryStore();
    this.wallets.forEach((wallet, id) => copy.wallets.set(id, cloneWallet(wallet)));
    this.transactions.forEach((tx, id) => copy.transactions.set(id, cloneTransaction(tx)));
    copy.ledger = [...this.ledger];
    this.inbox.forEach((message, key) => copy.inbox.set(key, cloneInbox(message)));
    this.outbox.forEach((message, id) => copy.outbox.set(id, cloneOutbox(message)));
    return copy;
  }

  ledgerFor(walletId: string): WalletLedgerEntry[] {
    return this.ledger.filter((entry) => entry.walletId === walletId);
  }

  outboxTypesFor(aggregateId: string): string[] {
    return [...this.outbox.values()].filter((m) => m.aggregateId === aggregateId).map((m) => m.eventType);
  }
}

class InMemoryWalletRepository implements WalletRepository {
  constructor(private readonly store: InMemoryStore) {}

  async findById(id: string): Promise<Wallet | null> {
    const wallet = this.store.wallets.get(id);
    return wallet ? cloneWallet(wallet) : null;
  }

  findByIdForUpdate(id: string): Promise<Wallet | null> {
    return this.findById(id);
  }

  findByIdForShare(id: string): Promise<Wallet | null> {
    return this.findById(id);
  }

  async insert(wallet: Wallet): Promise<void> {
    const duplicate = [...this.store.wallets.values()].some(
      (existing) => existing.playerId === wallet.playerId && existing.currency === wallet.currency,
    );
    if (duplicate) {
      throw new WalletAlreadyExistsError(wallet.playerId, wallet.currency);
    }
    this.store.wallets.set(wallet.id, cloneWallet(wallet));
  }

  async saveBalance(wallet: Wallet, expectedVersion: number): Promise<void> {
    const stored = this.store.wallets.get(wallet.id);
    if (!stored || stored.version !== expectedVersion) {
      throw new ConcurrencyViolationError(wallet.id, expectedVersion);
    }
    this.store.wallets.set(wallet.id, cloneWallet(wallet));
  }
}

class InMemoryWagerTransactionRepository implements WagerTransactionRepository {
  constructor(
    private readonly store: InMemoryStore,
    private readonly hooks: InMemoryHooks,
  ) {}

  async findById(id: string): Promise<WagerTransaction | null> {
    return this.copy([...this.store.transactions.values()].find((tx) => tx.id === id));
  }

  async findByIdempotencyKey(idempotencyKey: string): Promise<WagerTransaction | null> {
    return this.copy([...this.store.transactions.values()].find((tx) => tx.idempotencyKey === idempotencyKey));
  }

  async findByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null> {
    return this.copy(
      [...this.store.transactions.values()].find(
        (tx) => tx.providerId === providerId && tx.externalTransactionId === externalTransactionId,
      ),
    );
  }

  async insert(transaction: WagerTransaction): Promise<void> {
    this.hooks.beforeTransactionInsert?.(transaction);
    const clash = [...this.store.transactions.values()].some(
      (tx) =>
        tx.idempotencyKey === transaction.idempotencyKey ||
        (tx.providerId === transaction.providerId && tx.externalTransactionId === transaction.externalTransactionId),
    );
    if (clash) {
      throw new DuplicateTransactionError('in-memory unique');
    }
    this.store.transactions.set(transaction.id, cloneTransaction(transaction));
  }

  async update(transaction: WagerTransaction): Promise<void> {
    this.store.transactions.set(transaction.id, cloneTransaction(transaction));
  }

  async hasProcessedReversal(referenceTransactionId: string): Promise<boolean> {
    return [...this.store.transactions.values()].some(
      (tx) =>
        tx.referenceTransactionId === referenceTransactionId &&
        tx.requiresReference() &&
        tx.status === WagerTransactionStatus.Processed,
    );
  }

  async wakeDependents(providerId: string, externalTransactionId: string, now: Date): Promise<number> {
    let woken = 0;
    for (const tx of this.store.transactions.values()) {
      if (
        tx.status === WagerTransactionStatus.PendingReference &&
        tx.providerId === providerId &&
        tx.referenceExternalTransactionId === externalTransactionId &&
        (tx.nextReferenceCheckAt?.getTime() ?? 0) > now.getTime()
      ) {
        this.store.transactions.set(tx.id, cloneTransaction(tx, { nextReferenceCheckAt: now }));
        woken += 1;
      }
    }
    return woken;
  }

  async claimDuePendingReferences(now: Date, limit: number, leaseUntil: Date): Promise<PendingReferenceCandidate[]> {
    const due = [...this.store.transactions.values()]
      .filter((tx) => tx.status === WagerTransactionStatus.PendingReference && this.isDue(tx, now))
      .sort((a, b) => (a.nextReferenceCheckAt?.getTime() ?? 0) - (b.nextReferenceCheckAt?.getTime() ?? 0))
      .slice(0, limit);
    due.forEach((tx) => this.store.transactions.set(tx.id, cloneTransaction(tx, { nextReferenceCheckAt: leaseUntil })));
    return due.map((tx) => ({ transactionId: tx.id, walletId: tx.walletId }));
  }

  async lockPendingReference(id: string, leaseUntil: Date): Promise<WagerTransaction | null> {
    const tx = this.store.transactions.get(id);
    const holdsLease = tx?.nextReferenceCheckAt?.getTime() === leaseUntil.getTime();
    return tx && tx.status === WagerTransactionStatus.PendingReference && holdsLease ? cloneTransaction(tx) : null;
  }

  private isDue(tx: WagerTransaction, now: Date): boolean {
    return (tx.nextReferenceCheckAt?.getTime() ?? 0) <= now.getTime();
  }

  private copy(tx: WagerTransaction | undefined): WagerTransaction | null {
    return tx ? cloneTransaction(tx) : null;
  }
}

class InMemoryLedgerRepository implements LedgerRepository {
  constructor(private readonly store: InMemoryStore) {}

  async append(entry: WalletLedgerEntry): Promise<void> {
    this.store.ledger.push(entry);
  }

  async page(walletId: string, afterVersion: number | undefined, limit: number): Promise<WalletLedgerEntry[]> {
    return this.store
      .ledgerFor(walletId)
      .filter((entry) => afterVersion === undefined || entry.walletVersion > afterVersion)
      .sort((a, b) => a.walletVersion - b.walletVersion)
      .slice(0, limit);
  }

  async summarize(walletId: string): Promise<LedgerSummary> {
    const entries = this.store.ledgerFor(walletId).sort((a, b) => a.walletVersion - b.walletVersion);
    const last = entries.at(-1);
    if (!last) {
      return { netAmount: '0.00', entries: 0, chainBreaks: 0 };
    }
    const net = entries.slice(1).reduce((sum, entry) => sum.add(entry.signedAmount()), entries[0]!.signedAmount());
    const chainBreaks = entries
      .slice(1)
      .filter((entry, i) => !entry.balanceBefore.equals(entries[i]!.balanceAfter)).length;
    return {
      netAmount: net.toJSON().amount,
      entries: entries.length,
      lastBalanceAfter: last.balanceAfter.toJSON().amount,
      lastWalletVersion: last.walletVersion,
      chainBreaks,
    };
  }
}

class InMemoryInboxRepository implements InboxRepository {
  constructor(private readonly store: InMemoryStore) {}

  async tryReceive(message: InboxMessage): Promise<boolean> {
    const key = `${message.consumerName}/${message.messageId}`;
    if (this.store.inbox.has(key)) {
      return false;
    }
    this.store.inbox.set(key, message);
    return true;
  }

  async find(consumerName: string, messageId: string): Promise<InboxMessage | null> {
    return this.store.inbox.get(`${consumerName}/${messageId}`) ?? null;
  }

  async markProcessed(message: InboxMessage): Promise<void> {
    this.store.inbox.set(`${message.consumerName}/${message.messageId}`, message);
  }
}

class InMemoryOutboxRepository implements OutboxRepository {
  constructor(private readonly store: InMemoryStore) {}

  async enqueue(messages: readonly OutboxMessage[]): Promise<void> {
    messages.forEach((message) => this.store.outbox.set(message.id, cloneOutbox(message)));
  }

  async claimDue(now: Date, limit: number): Promise<OutboxMessage[]> {
    return [...this.store.outbox.values()]
      .filter((message) => message.isDue(now))
      .sort((a, b) => a.occurredAt.getTime() - b.occurredAt.getTime() || a.id.localeCompare(b.id))
      .slice(0, limit)
      .map(cloneOutbox);
  }

  async save(message: OutboxMessage, heldLease?: Date): Promise<void> {
    const stored = this.store.outbox.get(message.id);
    const leaseLost = heldLease !== undefined && stored?.nextAttemptAt?.getTime() !== heldLease.getTime();
    if (stored?.isPending() !== true || leaseLost) {
      return;
    }
    this.store.outbox.set(message.id, cloneOutbox(message));
  }

  async stats(): Promise<OutboxStats> {
    const pending = [...this.store.outbox.values()].filter((message) => message.isPending());
    const oldest = pending.map((message) => message.occurredAt).sort((a, b) => a.getTime() - b.getTime())[0];
    return { pending: pending.length, ...(oldest ? { oldestPendingOccurredAt: oldest } : {}) };
  }
}

export interface InMemoryHooks {
  beforeTransactionInsert?: (transaction: WagerTransaction) => void;
}

export class InMemoryUnitOfWork implements UnitOfWork {
  store = new InMemoryStore();
  hooks: InMemoryHooks = {};
  commits = 0;

  async run<T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> {
    const draft = this.store.clone();
    const result = await work({
      wallets: new InMemoryWalletRepository(draft),
      transactions: new InMemoryWagerTransactionRepository(draft, this.hooks),
      ledger: new InMemoryLedgerRepository(draft),
      inbox: new InMemoryInboxRepository(draft),
      outbox: new InMemoryOutboxRepository(draft),
    });
    this.store = draft;
    this.commits += 1;
    return result;
  }
}

export class FixedClock implements Clock {
  constructor(public current = new Date('2026-10-21T12:00:00.000Z')) {}

  now(): Date {
    return new Date(this.current);
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export class RandomIds implements IdGenerator {
  next(): string {
    return randomUUID();
  }
}

export class RecordingMetrics implements Metrics {
  readonly calls: string[] = [];

  count(name: string): number {
    return this.calls.filter((call) => call === name).length;
  }

  transactionSettled(status: string, kind: string, source: ProcessingSource): void {
    this.calls.push(`settled:${status}:${kind}:${source}`);
  }

  duplicateDetected(type: string): void {
    this.calls.push(`duplicate:${type}`);
  }

  retry(component: string): void {
    this.calls.push(`retry:${component}`);
  }

  deadLettered(reason: string): void {
    this.calls.push(`dlq:${reason}`);
  }

  lockConflict(kind: string): void {
    this.calls.push(`lock:${kind}`);
  }

  observeProcessingLatency(): void {}

  observeLockWait(): void {}

  outboxPublished(count: number): void {
    this.calls.push(`published:${count}`);
  }

  setOutboxBacklog(): void {}

  reconciliationChecked(consistent: boolean): void {
    this.calls.push(`reconciliation:${consistent}`);
  }
}

export class SilentLogger implements AppLogger {
  debug(): void {}
  info(): void {}
  warn(): void {}
  error(): void {}
}

export class RecordingPublisher implements EventPublisher {
  readonly published: string[] = [];
  failuresLeft = 0;

  async publish(message: OutboxMessage): Promise<void> {
    if (this.failuresLeft > 0) {
      this.failuresLeft -= 1;
      throw new Error('broker unavailable');
    }
    this.published.push(message.id);
  }
}
