import { FailureCode } from '../../domain/wagering/failure-code';
import { SettlementOutcome } from '../../domain/wagering/wager-settlement';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { DuplicateTransactionError, TransientInfrastructureError } from '../errors';
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

export const PENDING_REFERENCE_LEASE_MS = 30_000;

type CandidateOutcome = 'settled' | 'still-pending' | 'skipped' | 'deferred' | 'failed';

export class RetryPendingReferencesUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly settlement: WagerSettlementService,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async runOnce(batchSize: number): Promise<PendingReferenceRunResult> {
    const candidates = await this.uow.run((scope) =>
      scope.transactions.claimDuePendingReferences(
        this.clock.now(),
        batchSize,
        new Date(this.clock.now().getTime() + PENDING_REFERENCE_LEASE_MS),
      ),
    );
    const result: PendingReferenceRunResult = { candidates: candidates.length, settled: 0, stillPending: 0, failed: 0 };

    for (const candidate of candidates) {
      const outcome = await this.retry(candidate);
      if (outcome === 'settled') result.settled += 1;
      if (outcome === 'still-pending') result.stillPending += 1;
      if (outcome === 'failed') result.failed += 1;
    }
    return result;
  }

  private async retry(candidate: PendingReferenceCandidate): Promise<CandidateOutcome> {
    try {
      const outcome = await this.settleCandidate(candidate);
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
      if (error instanceof TransientInfrastructureError || error instanceof DuplicateTransactionError) {
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

  private settleCandidate(candidate: PendingReferenceCandidate): Promise<SettlementOutcome | undefined> {
    return this.uow.run(async (scope) => {
      const wallet = await scope.wallets.findByIdForUpdate(candidate.walletId);
      const transaction = await scope.transactions.lockPendingReference(candidate.transactionId);
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
