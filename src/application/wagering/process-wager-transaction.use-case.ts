import { InboxMessage } from '../../domain/messaging/inbox-message';
import { Money } from '../../domain/shared/money';
import { WagerTransaction, WagerTransactionKind } from '../../domain/wagering/wager-transaction';
import {
  DuplicateTransactionError,
  ExternalTransactionConflictError,
  IdempotencyKeyConflictError,
  InvalidPayloadError,
  WalletNotFoundError,
} from '../errors';
import { AppLogger, Clock, IdGenerator, Metrics, ProcessingSource } from '../ports/system';
import { TransactionScope, UnitOfWork } from '../ports/unit-of-work';
import { asPayloadError, assertUuid } from '../validation';
import { computeWagerPayloadHash, WagerBusinessFields } from './payload-hash';
import { toResult, WagerTransactionResult } from './wager-transaction.views';
import { WagerSettlementService } from './wager-settlement.service';

export interface SubmitWagerTransactionCommand extends WagerBusinessFields {
  /** Fonte da verdade da idempotência (header HTTP ou campo da mensagem). */
  idempotencyKey: string;
}

/** Presente quando a entrada é a fila: registro de inbox gravado na mesma transação SQL. */
export interface InboxReceipt {
  consumerName: string;
  messageId: string;
  payloadHash: string;
}

export interface ProcessingContext {
  correlationId: string;
  causationId?: string;
  source: ProcessingSource;
  inbox?: InboxReceipt;
}

/** Corridas de unicidade se resolvem na 2ª tentativa (que encontra a transação vencedora). */
const MAX_RACE_ATTEMPTS = 3;

/**
 * ÚNICO use case de submissão — usado pelo controller HTTP e pelo consumer SQS (AC-SQS-02).
 *
 * Fluxo (uma transação SQL):
 *   1. inbox (se SQS)  →  2. lock da wallet (FOR UPDATE)  →  3. idempotência (replay/conflito)
 *   →  4. liquidação (domínio)  →  5. transação + ledger + saldo + outbox  →  commit.
 * O replay rápido (passo 0, sem lock) evita que duplicatas façam fila no lock de uma hot wallet.
 */
export class ProcessWagerTransactionUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly settlement: WagerSettlementService,
    private readonly ids: IdGenerator,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async execute(command: SubmitWagerTransactionCommand, ctx: ProcessingContext): Promise<WagerTransactionResult> {
    const startedAt = performance.now();
    try {
      // Valida a forma uma vez, antes de tocar no banco: payload inválido nunca abre transação.
      const probe = this.buildDraft(command);

      const fastReplay = await this.uow.run((scope) => this.findExisting(scope, probe));
      if (fastReplay) {
        return this.replay(fastReplay, probe);
      }

      for (let attempt = 1; ; attempt++) {
        try {
          return await this.uow.run((scope) => this.process(scope, this.buildDraft(command), ctx));
        } catch (error) {
          if (!(error instanceof DuplicateTransactionError) || attempt >= MAX_RACE_ATTEMPTS) {
            throw error;
          }
          // Outra instância inseriu a mesma transação entre o nosso lock e o insert (ex.: mesma key
          // apontando para wallets diferentes). A próxima tentativa verá a vencedora e fará replay.
          this.metrics.lockConflict('unique_race');
          this.metrics.retry('race');
        }
      }
    } finally {
      this.metrics.observeProcessingLatency(ctx.source, (performance.now() - startedAt) / 1000);
    }
  }

  private async process(
    scope: TransactionScope,
    draft: WagerTransaction,
    ctx: ProcessingContext,
  ): Promise<WagerTransactionResult> {
    const now = this.clock.now();

    if (ctx.inbox) {
      const inbox = InboxMessage.receive({ ...ctx.inbox, receivedAt: now });
      inbox.markProcessed(now);
      const fresh = await scope.inbox.tryReceive(inbox);
      if (!fresh) {
        this.metrics.duplicateDetected('inbox_duplicate');
      }
    }

    const lockStartedAt = performance.now();
    const wallet = await scope.wallets.findByIdForUpdate(draft.walletId);
    this.metrics.observeLockWait((performance.now() - lockStartedAt) / 1000);
    if (!wallet) {
      throw new WalletNotFoundError(draft.walletId);
    }

    // Com a wallet travada, nenhuma outra escrita na mesma wallet acontece: a checagem é decisiva.
    const existing = await this.findExisting(scope, draft);
    if (existing) {
      return this.replay(existing, draft);
    }

    const outcome = await this.settlement.settle(scope, wallet, draft, ctx, 'insert');

    this.logger.info('wager transaction settled', {
      correlationId: ctx.correlationId,
      messageId: ctx.inbox?.messageId,
      transactionId: draft.id,
      walletId: draft.walletId,
      providerId: draft.providerId,
      kind: draft.kind,
      status: draft.status,
      outcome: outcome.type,
      failureCode: draft.failureCode,
    });
    return toResult(draft, false);
  }

  private async findExisting(scope: TransactionScope, draft: WagerTransaction): Promise<WagerTransaction | null> {
    return (
      (await scope.transactions.findByIdempotencyKey(draft.idempotencyKey)) ??
      (await scope.transactions.findByExternalId(draft.providerId, draft.externalTransactionId))
    );
  }

  /** Mesma key + mesmo payload → resultado original. Qualquer divergência → conflito, nunca replay. */
  private replay(existing: WagerTransaction, draft: WagerTransaction): WagerTransactionResult {
    if (existing.idempotencyKey !== draft.idempotencyKey) {
      this.metrics.duplicateDetected('idempotency_conflict');
      throw new ExternalTransactionConflictError(draft.providerId, draft.externalTransactionId);
    }
    if (!existing.matchesPayload(draft.payloadHash)) {
      this.metrics.duplicateDetected('idempotency_conflict');
      throw new IdempotencyKeyConflictError(draft.idempotencyKey);
    }
    this.metrics.duplicateDetected('idempotent_replay');
    return toResult(existing, true);
  }

  private buildDraft(command: SubmitWagerTransactionCommand): WagerTransaction {
    assertUuid('walletId', command.walletId);
    const kind = parseSubmittableKind(command.kind);
    return asPayloadError(() =>
      WagerTransaction.create({
        id: this.ids.next(),
        providerId: command.providerId,
        externalTransactionId: command.externalTransactionId,
        idempotencyKey: command.idempotencyKey,
        payloadHash: computeWagerPayloadHash(command),
        walletId: command.walletId.toLowerCase(),
        playerId: command.playerId,
        roundId: command.roundId,
        gameId: command.gameId,
        kind,
        money: Money.from(command.money),
        referenceExternalTransactionId: command.referenceExternalTransactionId,
        createdAt: this.clock.now(),
      }),
    );
  }
}

const SUBMITTABLE = new Set<string>([
  WagerTransactionKind.Bet,
  WagerTransactionKind.Win,
  WagerTransactionKind.Loss,
  WagerTransactionKind.Refund,
  WagerTransactionKind.Rollback,
]);

function parseSubmittableKind(kind: string): WagerTransactionKind {
  if (!SUBMITTABLE.has(kind)) {
    throw new InvalidPayloadError(`kind must be one of ${[...SUBMITTABLE].join(', ')}`, [`kind: "${kind}"`]);
  }
  return kind as WagerTransactionKind;
}
