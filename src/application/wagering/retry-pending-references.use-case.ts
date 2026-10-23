import { FailureCode } from '../../domain/wagering/failure-code';
import { SettlementOutcome } from '../../domain/wagering/wager-settlement';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { TransientInfrastructureError } from '../errors';
import { DEFAULT_LEASE_POLICY, LeasePolicy } from '../lease-policy';
import { PendingReferenceCandidate } from '../ports/repositories';
import { AppLogger, Clock, Metrics } from '../ports/system';
import { UnitOfWork } from '../ports/unit-of-work';
import { WagerSettlementService } from './wager-settlement.service';

export interface PendingReferenceRunResult {
  candidates: number;
  settled: number;
  stillPending: number;
  failed: number;
}

type CandidateOutcome = 'settled' | 'still-pending' | 'skipped' | 'deferred' | 'failed';

export class RetryPendingReferencesUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly settlement: WagerSettlementService,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
    private readonly lease: LeasePolicy = DEFAULT_LEASE_POLICY,
  ) {}

  async runOnce(batchSize: number): Promise<PendingReferenceRunResult> {
    const now = this.clock.now();
    const leaseUntil = this.lease.expiresAt(now);
    const candidates = await this.uow.run((scope) =>
      scope.transactions.claimDuePendingReferences(now, batchSize, leaseUntil),
    );
    const result: PendingReferenceRunResult = { candidates: candidates.length, settled: 0, stillPending: 0, failed: 0 };

    for (const candidate of candidates) {
      if (!this.leaseStillValid(leaseUntil)) {
        break;
      }
      const outcome = await this.retry(candidate, leaseUntil);
      if (outcome === 'settled') result.settled += 1;
      if (outcome === 'still-pending') result.stillPending += 1;
      if (outcome === 'failed') result.failed += 1;
    }
    return result;
  }

  private leaseStillValid(leaseUntil: Date): boolean {
    return this.lease.isStillValid(this.clock.now(), leaseUntil);
  }

  private async retry(candidate: PendingReferenceCandidate, leaseUntil: Date): Promise<CandidateOutcome> {
    try {
      const outcome = await this.settleCandidate(candidate, leaseUntil);
      if (!outcome) {
        return 'skipped';
      }
      if (outcome.type === 'pending-reference') {
        this.metrics.retry('pending_reference');
        return 'still-pending';
      }
      this.logger.info('pending reference resolved', {
        transactionId: candidate.transactionId,
        walletId: candidate.walletId,
        outcome: outcome.type,
        failureCode: outcome.type === 'rejected' ? outcome.failureCode : undefined,
      });
      return 'settled';
    } catch (error) {
      if (error instanceof TransientInfrastructureError) {
        this.metrics.retry('pending_reference');
        this.logger.warn('pending reference retry deferred', {
          transactionId: candidate.transactionId,
          walletId: candidate.walletId,
          reason: error.code,
        });
        return 'deferred';
      }
      await this.markFailed(candidate, error);
      return 'failed';
    }
  }

  private settleCandidate(
    candidate: PendingReferenceCandidate,
    leaseUntil: Date,
  ): Promise<SettlementOutcome | undefined> {
    return this.uow.run(async (scope) => {
      const wallet = await scope.wallets.findByIdForUpdate(candidate.walletId);
      const transaction = await scope.transactions.lockPendingReference(candidate.transactionId, leaseUntil);
      if (!wallet || !transaction) {
        return undefined;
      }
      return this.settlement.settle(scope, wallet, transaction, {
        correlationId: `pending-reference:${transaction.id}`,
        causationId: transaction.id,
        source: 'worker',
      });
    });
  }

  private async markFailed(candidate: PendingReferenceCandidate, cause: unknown): Promise<void> {
    const fields = { transactionId: candidate.transactionId, walletId: candidate.walletId };
    this.logger.error('pending reference failed permanently', fields, cause);
    try {
      const kind = await this.uow.run(async (scope) => {
        await scope.wallets.findByIdForUpdate(candidate.walletId);
        const transaction = await scope.transactions.findById(candidate.transactionId);
        if (!transaction || transaction.status !== WagerTransactionStatus.PendingReference) {
          return undefined;
        }
        transaction.fail(FailureCode.ProcessingError, this.clock.now());
        await scope.transactions.update(transaction);
        return transaction.kind;
      });
      if (kind) {
        this.metrics.transactionSettled(WagerTransactionStatus.Failed, kind, 'worker');
      }
    } catch (error) {
      this.logger.error('could not mark transaction as FAILED', fields, error);
    }
  }
}
