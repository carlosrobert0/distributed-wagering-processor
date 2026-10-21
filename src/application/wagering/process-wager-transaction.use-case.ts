import { InboxMessage } from '../../domain/messaging/inbox-message';
import { Money } from '../../domain/shared/money';
import {
  isSubmittableKind,
  SUBMITTABLE_KINDS,
  WagerTransaction,
  WagerTransactionKind,
} from '../../domain/wagering/wager-transaction';
import { Wallet } from '../../domain/wallet/wallet';
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
import { WagerSettlementService } from './wager-settlement.service';
import { toResult, WagerTransactionResult } from './wager-transaction.views';

export interface SubmitWagerTransactionCommand extends WagerBusinessFields {
  idempotencyKey: string;
}

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

export const MAX_RACE_ATTEMPTS = 3;

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
      const draft = this.buildDraft(command);
      const replay = await this.replayWithoutLock(draft, ctx);
      return replay ?? (await this.processResolvingRaces(command, ctx));
    } finally {
      this.metrics.observeProcessingLatency(ctx.source, (performance.now() - startedAt) / 1000);
    }
  }

  private async replayWithoutLock(
    draft: WagerTransaction,
    ctx: ProcessingContext,
  ): Promise<WagerTransactionResult | undefined> {
    const existing = await this.uow.run(async (scope) => {
      if (ctx.inbox && (await scope.inbox.find(ctx.inbox.consumerName, ctx.inbox.messageId))) {
        this.metrics.duplicateDetected('inbox_duplicate');
      }
      return this.findExisting(scope, draft);
    });
    return existing ? this.replay(existing, draft) : undefined;
  }

  private async processResolvingRaces(
    command: SubmitWagerTransactionCommand,
    ctx: ProcessingContext,
  ): Promise<WagerTransactionResult> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.uow.run((scope) => this.process(scope, this.buildDraft(command), ctx));
      } catch (error) {
        if (!(error instanceof DuplicateTransactionError) || attempt >= MAX_RACE_ATTEMPTS) {
          throw error;
        }
        this.metrics.lockConflict('unique_race');
        this.metrics.retry('race');
      }
    }
  }

  private async process(
    scope: TransactionScope,
    draft: WagerTransaction,
    ctx: ProcessingContext,
  ): Promise<WagerTransactionResult> {
    if (ctx.inbox) {
      await this.receiveInbox(scope, ctx.inbox);
    }
    const wallet = await this.lockWallet(scope, draft.walletId);
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

  private async receiveInbox(scope: TransactionScope, receipt: InboxReceipt): Promise<void> {
    const now = this.clock.now();
    const inbox = InboxMessage.receive({ ...receipt, receivedAt: now });
    inbox.markProcessed(now);
    if (!(await scope.inbox.tryReceive(inbox))) {
      this.metrics.duplicateDetected('inbox_duplicate');
    }
  }

  private async lockWallet(scope: TransactionScope, walletId: string): Promise<Wallet> {
    const startedAt = performance.now();
    const wallet = await scope.wallets.findByIdForUpdate(walletId);
    this.metrics.observeLockWait((performance.now() - startedAt) / 1000);
    if (!wallet) {
      throw new WalletNotFoundError(walletId);
    }
    return wallet;
  }

  private async findExisting(scope: TransactionScope, draft: WagerTransaction): Promise<WagerTransaction | null> {
    return (
      (await scope.transactions.findByIdempotencyKey(draft.idempotencyKey)) ??
      (await scope.transactions.findByExternalId(draft.providerId, draft.externalTransactionId))
    );
  }

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

  private buildDraft(input: SubmitWagerTransactionCommand): WagerTransaction {
    assertUuid('walletId', input.walletId);
    const command = { ...input, walletId: input.walletId.toLowerCase() };
    const kind = parseSubmittableKind(command.kind);
    return asPayloadError(() =>
      WagerTransaction.create({
        id: this.ids.next(),
        providerId: command.providerId,
        externalTransactionId: command.externalTransactionId,
        idempotencyKey: command.idempotencyKey,
        payloadHash: computeWagerPayloadHash(command),
        walletId: command.walletId,
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

function parseSubmittableKind(kind: string): WagerTransactionKind {
  if (!isSubmittableKind(kind)) {
    throw new InvalidPayloadError(`kind must be one of ${SUBMITTABLE_KINDS.join(', ')}`, [`kind: "${kind}"`]);
  }
  return kind;
}
