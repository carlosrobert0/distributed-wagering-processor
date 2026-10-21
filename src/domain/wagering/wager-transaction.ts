import {
  InvalidTransactionStateError,
  InvalidWagerTransactionError,
  InvariantViolationError,
} from '../shared/domain-error';
import { Money } from '../shared/money';
import { LedgerDirection } from '../wallet/wallet-ledger-entry';
import { FailureCode } from './failure-code';

export enum WagerTransactionKind {
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  Pending = 'PENDING',
  PendingReference = 'PENDING_REFERENCE',
  Processed = 'PROCESSED',
  Rejected = 'REJECTED',
  Failed = 'FAILED',
}

export const INTERNAL_PROVIDER_ID = 'internal';

const TERMINAL_STATUSES: ReadonlySet<WagerTransactionStatus> = new Set([
  WagerTransactionStatus.Processed,
  WagerTransactionStatus.Rejected,
  WagerTransactionStatus.Failed,
]);

export const SUBMITTABLE_KINDS: readonly WagerTransactionKind[] = [
  WagerTransactionKind.Bet,
  WagerTransactionKind.Win,
  WagerTransactionKind.Loss,
  WagerTransactionKind.Refund,
  WagerTransactionKind.Rollback,
];

export function isSubmittableKind(kind: unknown): kind is WagerTransactionKind {
  return SUBMITTABLE_KINDS.includes(kind as WagerTransactionKind);
}

const ALLOWED_REFERENCE_KINDS: Readonly<Record<WagerTransactionKind, ReadonlySet<WagerTransactionKind>>> = {
  [WagerTransactionKind.Opening]: new Set(),
  [WagerTransactionKind.Bet]: new Set(),
  [WagerTransactionKind.Win]: new Set([WagerTransactionKind.Bet]),
  [WagerTransactionKind.Loss]: new Set([WagerTransactionKind.Bet]),
  [WagerTransactionKind.Refund]: new Set([WagerTransactionKind.Bet]),
  [WagerTransactionKind.Rollback]: new Set([
    WagerTransactionKind.Bet,
    WagerTransactionKind.Win,
    WagerTransactionKind.Refund,
  ]),
};

const MAX_ID_LENGTH = 128;
const MAX_IDEMPOTENCY_KEY_LENGTH = 255;

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
}

export interface WagerTransactionState extends CreateWagerTransactionProps {
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
  balanceAfter?: Money;
  referenceAttempts: number;
  nextReferenceCheckAt?: Date;
}

export interface OpeningTransactionProps {
  id: string;
  walletId: string;
  playerId: string;
  money: Money;
  payloadHash: string;
  createdAt: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
    private _balanceAfter?: Money,
    private _referenceAttempts: number = 0,
    private _nextReferenceCheckAt?: Date,
  ) {}

  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (!isSubmittableKind(props.kind)) {
      throw new InvalidWagerTransactionError(`kind "${String(props.kind)}" cannot be submitted`);
    }
    if (props.providerId === INTERNAL_PROVIDER_ID) {
      throw new InvalidWagerTransactionError(`providerId "${INTERNAL_PROVIDER_ID}" is reserved`);
    }
    WagerTransaction.assertIdentifier('providerId', props.providerId);
    WagerTransaction.assertIdentifier('externalTransactionId', props.externalTransactionId);
    WagerTransaction.assertIdentifier('playerId', props.playerId);
    WagerTransaction.assertIdentifier('roundId', props.roundId);
    WagerTransaction.assertIdentifier('gameId', props.gameId);
    WagerTransaction.assertIdentifier('idempotencyKey', props.idempotencyKey, MAX_IDEMPOTENCY_KEY_LENGTH);

    if (props.money.isNegative()) {
      throw new InvalidWagerTransactionError('money cannot be negative');
    }
    if (props.kind !== WagerTransactionKind.Loss && !props.money.isPositive()) {
      throw new InvalidWagerTransactionError(`money must be positive for ${props.kind}`);
    }

    const reference = props.referenceExternalTransactionId;
    const acceptsReference = ALLOWED_REFERENCE_KINDS[props.kind].size > 0;
    const requiresReference = WagerTransaction.kindRequiresReference(props.kind);
    if (reference !== undefined) {
      if (!acceptsReference) {
        throw new InvalidWagerTransactionError(`${props.kind} cannot reference another transaction`);
      }
      WagerTransaction.assertIdentifier('referenceExternalTransactionId', reference);
      if (reference === props.externalTransactionId) {
        throw new InvalidWagerTransactionError('A transaction cannot reference itself');
      }
    } else if (requiresReference) {
      throw new InvalidWagerTransactionError(`${props.kind} requires referenceExternalTransactionId`);
    }

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      reference,
      props.createdAt,
      WagerTransactionStatus.Pending,
    );
  }

  static opening(props: OpeningTransactionProps): WagerTransaction {
    if (!props.money.isPositive()) {
      throw new InvariantViolationError('OPENING requires a positive amount');
    }
    return new WagerTransaction(
      props.id,
      INTERNAL_PROVIDER_ID,
      `opening:${props.walletId}`,
      `${INTERNAL_PROVIDER_ID}:opening:${props.walletId}`,
      props.payloadHash,
      props.walletId,
      props.playerId,
      'opening',
      INTERNAL_PROVIDER_ID,
      WagerTransactionKind.Opening,
      props.money,
      undefined,
      props.createdAt,
      WagerTransactionStatus.Pending,
    );
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      new Date(state.createdAt),
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt ? new Date(state.processedAt) : undefined,
      state.balanceAfter,
      state.referenceAttempts,
      state.nextReferenceCheckAt ? new Date(state.nextReferenceCheckAt) : undefined,
    );
  }

  static kindRequiresReference(kind: WagerTransactionKind): boolean {
    return kind === WagerTransactionKind.Refund || kind === WagerTransactionKind.Rollback;
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  get balanceAfter(): Money | undefined {
    return this._balanceAfter;
  }

  get referenceAttempts(): number {
    return this._referenceAttempts;
  }

  get nextReferenceCheckAt(): Date | undefined {
    return this._nextReferenceCheckAt;
  }

  markProcessed(params: { referenceTransactionId: string | undefined; balanceAfter: Money; at: Date }): void {
    this.assertNotTerminal(WagerTransactionStatus.Processed);
    if (this.requiresReference() && !params.referenceTransactionId) {
      throw new InvariantViolationError(`${this.kind} cannot be processed without a resolved reference`);
    }
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = params.referenceTransactionId;
    this._balanceAfter = params.balanceAfter;
    this._processedAt = params.at;
    this._nextReferenceCheckAt = undefined;
  }

  markPendingReference(nextCheckAt: Date): void {
    if (this._status !== WagerTransactionStatus.Pending) {
      throw new InvalidTransactionStateError(this.id, this._status, WagerTransactionStatus.PendingReference);
    }
    this._status = WagerTransactionStatus.PendingReference;
    this._referenceAttempts = 0;
    this._nextReferenceCheckAt = nextCheckAt;
  }

  scheduleReferenceRetry(nextCheckAt: Date): void {
    if (this._status !== WagerTransactionStatus.PendingReference) {
      throw new InvalidTransactionStateError(this.id, this._status, WagerTransactionStatus.PendingReference);
    }
    this._referenceAttempts += 1;
    this._nextReferenceCheckAt = nextCheckAt;
  }

  recordReferenceAttempt(): void {
    if (this._status !== WagerTransactionStatus.PendingReference) {
      throw new InvalidTransactionStateError(this.id, this._status, 'REFERENCE_ATTEMPT');
    }
    this._referenceAttempts += 1;
  }

  reject(code: FailureCode, params: { balanceAfter: Money; at: Date }): void {
    this.assertNotTerminal(WagerTransactionStatus.Rejected);
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
    this._balanceAfter = params.balanceAfter;
    this._processedAt = params.at;
    this._nextReferenceCheckAt = undefined;
  }

  fail(code: FailureCode, at: Date): void {
    this.assertNotTerminal(WagerTransactionStatus.Failed);
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
    this._processedAt = at;
    this._nextReferenceCheckAt = undefined;
  }

  isTerminal(): boolean {
    return TERMINAL_STATUSES.has(this._status);
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return WagerTransaction.kindRequiresReference(this.kind);
  }

  hasReference(): boolean {
    return this.referenceExternalTransactionId !== undefined;
  }

  isReversal(): boolean {
    return this.requiresReference();
  }

  canReference(kind: WagerTransactionKind): boolean {
    return ALLOWED_REFERENCE_KINDS[this.kind].has(kind);
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
      case WagerTransactionKind.Opening:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback: {
        if (!reference) {
          throw new InvariantViolationError('ROLLBACK direction depends on the referenced transaction');
        }
        const original = reference.ledgerDirectionFor();
        return original === LedgerDirection.Debit ? LedgerDirection.Credit : LedgerDirection.Debit;
      }
      case WagerTransactionKind.Loss:
        throw new InvariantViolationError('LOSS does not move balance');
    }
  }

  private assertNotTerminal(attempted: WagerTransactionStatus): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(this.id, this._status, attempted);
    }
  }

  private static assertIdentifier(field: string, value: unknown, maxLength = MAX_ID_LENGTH): void {
    if (typeof value !== 'string' || value.trim().length === 0 || value.length > maxLength) {
      throw new InvalidWagerTransactionError(`${field} must be a non-empty string up to ${maxLength} characters`);
    }
  }
}
