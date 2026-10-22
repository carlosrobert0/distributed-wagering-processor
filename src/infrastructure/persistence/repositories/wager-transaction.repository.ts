import { LockMode } from '@mikro-orm/core';
import { EntityManager } from '@mikro-orm/postgresql';
import { DuplicateTransactionError, TransactionNotFoundError } from '../../../application/errors';
import { PendingReferenceCandidate, WagerTransactionRepository } from '../../../application/ports/repositories';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../../domain/wagering/wager-transaction';
import { constraintOf, isUniqueViolation } from '../database-errors';
import { WagerTransactionMapper } from '../mappers';
import { WagerTransactionRecord } from '../records';

const FRESH = { disableIdentityMap: true } as const;
const CLAIM_DUE_PENDING_REFERENCES_SQL = `
  UPDATE wager_transactions
     SET next_reference_check_at = ?, updated_at = ?
   WHERE id IN (
     SELECT id FROM wager_transactions
      WHERE status = 'PENDING_REFERENCE' AND next_reference_check_at <= ?
      ORDER BY next_reference_check_at
      LIMIT ?
      FOR UPDATE SKIP LOCKED)
  RETURNING id, wallet_id`;

const REVERSAL_KINDS = [WagerTransactionKind.Refund, WagerTransactionKind.Rollback];

export class MikroOrmWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly em: EntityManager) {}

  async findById(id: string): Promise<WagerTransaction | null> {
    return this.toDomain(await this.em.findOne(WagerTransactionRecord, { id }, FRESH));
  }

  async findByIdempotencyKey(idempotencyKey: string): Promise<WagerTransaction | null> {
    return this.toDomain(await this.em.findOne(WagerTransactionRecord, { idempotencyKey }, FRESH));
  }

  async findByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null> {
    return this.toDomain(await this.em.findOne(WagerTransactionRecord, { providerId, externalTransactionId }, FRESH));
  }

  async insert(transaction: WagerTransaction): Promise<void> {
    try {
      await this.em.insert(WagerTransactionRecord, WagerTransactionMapper.toRecord(transaction, new Date()));
    } catch (error) {
      if (isUniqueViolation(error)) {
        throw new DuplicateTransactionError(constraintOf(error));
      }
      throw error;
    }
  }

  async update(transaction: WagerTransaction): Promise<void> {
    try {
      const affected = await this.em.nativeUpdate(
        WagerTransactionRecord,
        { id: transaction.id },
        WagerTransactionMapper.toStateChange(transaction, new Date()),
      );
      if (affected !== 1) {
        throw new TransactionNotFoundError(transaction.id);
      }
    } catch (error) {
      if (isUniqueViolation(error)) {
        throw new DuplicateTransactionError(constraintOf(error));
      }
      throw error;
    }
  }

  async hasProcessedReversal(referenceTransactionId: string): Promise<boolean> {
    const count = await this.em.count(WagerTransactionRecord, {
      referenceTransactionId,
      kind: { $in: REVERSAL_KINDS },
      status: WagerTransactionStatus.Processed,
    });
    return count > 0;
  }

  async wakeDependents(providerId: string, externalTransactionId: string, now: Date): Promise<number> {
    return this.em.nativeUpdate(
      WagerTransactionRecord,
      {
        status: WagerTransactionStatus.PendingReference,
        providerId,
        referenceExternalTransactionId: externalTransactionId,
        nextReferenceCheckAt: { $gt: now },
      },
      { nextReferenceCheckAt: now, updatedAt: now },
    );
  }

  async claimDuePendingReferences(now: Date, limit: number, leaseUntil: Date): Promise<PendingReferenceCandidate[]> {
    const rows = await this.em.execute<{ id: string; wallet_id: string }[]>(CLAIM_DUE_PENDING_REFERENCES_SQL, [
      leaseUntil.toISOString(),
      now.toISOString(),
      now.toISOString(),
      limit,
    ]);
    return rows.map((row) => ({ transactionId: row.id, walletId: row.wallet_id }));
  }

  async lockPendingReference(id: string): Promise<WagerTransaction | null> {
    const record = await this.em.findOne(
      WagerTransactionRecord,
      { id, status: WagerTransactionStatus.PendingReference },
      { ...FRESH, lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE },
    );
    return this.toDomain(record);
  }

  private toDomain(record: WagerTransactionRecord | null): WagerTransaction | null {
    return record ? WagerTransactionMapper.toDomain(record) : null;
  }
}
