import { InboxMessage } from '../../domain/messaging/inbox-message';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { WagerTransaction } from '../../domain/wagering/wager-transaction';
import { Wallet } from '../../domain/wallet/wallet';
import { WalletLedgerEntry } from '../../domain/wallet/wallet-ledger-entry';

export interface WalletRepository {
  findById(id: string): Promise<Wallet | null>;
  /** `SELECT … FOR UPDATE`: serializa todas as escritas na mesma wallet, em qualquer instância. */
  findByIdForUpdate(id: string): Promise<Wallet | null>;
  /** `SELECT … FOR SHARE`: impede mudança de saldo enquanto lemos o ledger (reconciliação). */
  findByIdForShare(id: string): Promise<Wallet | null>;
  /** @throws WalletAlreadyExistsError quando já existe wallet para (playerId, currency). */
  insert(wallet: Wallet): Promise<void>;
  /**
   * Persiste o novo saldo condicionado à versão anterior (`WHERE version = expected`).
   * Defesa em profundidade: com o lock de linha isso nunca falha; se falhar, há bug.
   */
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
  /** @throws DuplicateTransactionError em violação de unicidade (corrida entre instâncias). */
  insert(transaction: WagerTransaction): Promise<void>;
  /** Persiste a mudança de status de uma transação existente. */
  update(transaction: WagerTransaction): Promise<void>;
  hasProcessedReversal(referenceTransactionId: string): Promise<boolean>;
  /** Antecipa a próxima verificação das transações que aguardam esta referência. */
  wakeDependents(providerId: string, externalTransactionId: string, now: Date): Promise<number>;
  findDuePendingReferences(now: Date, limit: number): Promise<PendingReferenceCandidate[]>;
  /** `FOR UPDATE SKIP LOCKED`: outra instância já está cuidando dela → `null`. */
  lockDuePendingReference(id: string, now: Date): Promise<WagerTransaction | null>;
}

export interface LedgerSummary {
  /** Soma com sinal (créditos − débitos), como string decimal exata. */
  netAmount: string;
  entries: number;
  /** `balance_after` do último lançamento, se houver. */
  lastBalanceAfter?: string;
  lastWalletVersion?: number;
  /** Quantos lançamentos quebram o encadeamento before(n) == after(n−1). */
  chainBreaks: number;
}

export interface LedgerRepository {
  append(entry: WalletLedgerEntry): Promise<void>;
  /** Página ordenada por `wallet_version` ascendente, a partir de (exclusive) `afterVersion`. */
  page(walletId: string, afterVersion: number | undefined, limit: number): Promise<WalletLedgerEntry[]>;
  summarize(walletId: string): Promise<LedgerSummary>;
}

export interface InboxRepository {
  /** `INSERT … ON CONFLICT DO NOTHING`. `false` = mensagem já recebida antes. */
  tryReceive(message: InboxMessage): Promise<boolean>;
  find(consumerName: string, messageId: string): Promise<InboxMessage | null>;
}

export interface OutboxStats {
  pending: number;
  oldestPendingOccurredAt?: Date;
}

export interface OutboxRepository {
  enqueue(messages: readonly OutboxMessage[]): Promise<void>;
  /** `FOR UPDATE SKIP LOCKED`: publishers concorrentes nunca pegam a mesma linha. */
  claimDue(now: Date, limit: number): Promise<OutboxMessage[]>;
  save(message: OutboxMessage): Promise<void>;
  stats(): Promise<OutboxStats>;
}
