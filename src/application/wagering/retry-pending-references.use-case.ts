import { FailureCode } from '../../domain/wagering/failure-code';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { TransientInfrastructureError } from '../errors';
import { AppLogger, Clock, Metrics } from '../ports/system';
import { UnitOfWork } from '../ports/unit-of-work';
import { WagerSettlementService } from './wager-settlement.service';

export interface PendingReferenceRunResult {
  candidates: number;
  settled: number;
  stillPending: number;
  failed: number;
}

/**
 * Worker de PENDING_REFERENCE (§7.1, AC-PEND).
 *
 * Cada candidata é processada em sua PRÓPRIA transação SQL, travando na mesma ordem da entrada
 * (wallet → transação) para não haver deadlock, e com `SKIP LOCKED` na transação para que várias
 * instâncias dividam o trabalho sem processar a mesma linha.
 */
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
      scope.transactions.findDuePendingReferences(this.clock.now(), batchSize),
    );
    const result: PendingReferenceRunResult = { candidates: candidates.length, settled: 0, stillPending: 0, failed: 0 };

    for (const candidate of candidates) {
      try {
        const outcome = await this.uow.run(async (scope) => {
          const wallet = await scope.wallets.findByIdForUpdate(candidate.walletId);
          const transaction = await scope.transactions.lockDuePendingReference(candidate.transactionId, this.clock.now());
          if (!wallet || !transaction) {
            return undefined; // outra instância pegou, ou deixou de estar pendente
          }
          return this.settlement.settle(
            scope,
            wallet,
            transaction,
            { correlationId: `pending-reference:${transaction.id}`, causationId: transaction.id, source: 'worker' },
            'update',
          );
        });
        if (!outcome) continue;
        if (outcome.type === 'pending-reference') {
          result.stillPending += 1;
          this.metrics.retry('pending_reference');
        } else {
          result.settled += 1;
          this.logger.info('pending reference resolved', {
            transactionId: candidate.transactionId,
            walletId: candidate.walletId,
            outcome: outcome.type,
            failureCode: outcome.type === 'rejected' ? outcome.failureCode : undefined,
          });
        }
      } catch (error) {
        if (error instanceof TransientInfrastructureError) {
          // Tenta de novo no próximo ciclo; a linha continua PENDING_REFERENCE e due.
          this.metrics.retry('pending_reference');
          this.logger.warn('pending reference retry deferred', {
            transactionId: candidate.transactionId,
            walletId: candidate.walletId,
            reason: error.reason,
          });
          continue;
        }
        result.failed += 1;
        await this.markFailed(candidate.transactionId, candidate.walletId, error);
      }
    }
    return result;
  }

  /** INT-12: erro não transitório (dado corrompido, bug) → FAILED auditável, em transação separada. */
  private async markFailed(transactionId: string, walletId: string, cause: unknown): Promise<void> {
    this.logger.error('pending reference failed permanently', { transactionId, walletId }, cause);
    try {
      const kind = await this.uow.run(async (scope) => {
        await scope.wallets.findByIdForUpdate(walletId);
        const transaction = await scope.transactions.findById(transactionId);
        if (!transaction || transaction.status !== WagerTransactionStatus.PendingReference) return undefined;
        transaction.fail(FailureCode.ProcessingError, this.clock.now());
        await scope.transactions.update(transaction);
        return transaction.kind;
      });
      if (kind) {
        this.metrics.transactionSettled(WagerTransactionStatus.Failed, kind, 'worker');
      }
    } catch (error) {
      this.logger.error('could not mark transaction as FAILED', { transactionId, walletId }, error);
    }
  }
}
