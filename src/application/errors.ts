/**
 * Erros da camada de aplicação. Cada um tem um `code` estável que as bordas (HTTP, SQS)
 * traduzem para status/ação de forma consistente (SPEC §8).
 */
export abstract class ApplicationError extends Error {
  abstract readonly code: string;

  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Entrada malformada: corrigir o payload. HTTP 400; fila: DLQ (não vai se corrigir sozinha). */
export class InvalidPayloadError extends ApplicationError {
  readonly code: string;

  constructor(
    message: string,
    readonly details?: readonly string[],
    code = 'INVALID_PAYLOAD',
  ) {
    super(message);
    this.code = code;
  }
}

/** HTTP 404. */
export abstract class NotFoundError extends ApplicationError {}

export class WalletNotFoundError extends NotFoundError {
  readonly code = 'WALLET_NOT_FOUND';

  constructor(walletId: string) {
    super(`Wallet ${walletId} not found`);
  }
}

export class TransactionNotFoundError extends NotFoundError {
  readonly code = 'TRANSACTION_NOT_FOUND';

  constructor(reference: string) {
    super(`Transaction ${reference} not found`);
  }
}

/** HTTP 409. Nunca é replay: o provedor precisa corrigir algo. */
export abstract class ConflictError extends ApplicationError {}

export class IdempotencyKeyConflictError extends ConflictError {
  readonly code = 'IDEMPOTENCY_KEY_CONFLICT';

  constructor(idempotencyKey: string) {
    super(`Idempotency-Key "${idempotencyKey}" was already used with a different payload`);
  }
}

export class ExternalTransactionConflictError extends ConflictError {
  readonly code = 'EXTERNAL_TRANSACTION_CONFLICT';

  constructor(providerId: string, externalTransactionId: string) {
    super(`Transaction ${providerId}/${externalTransactionId} was already submitted with a different idempotency key`);
  }
}

export class WalletAlreadyExistsError extends ConflictError {
  readonly code = 'WALLET_ALREADY_EXISTS';

  constructor(playerId: string, currency: string) {
    super(`Player ${playerId} already has a ${currency} wallet`);
  }
}

/** Falha temporária (banco/SQS fora, lock timeout, deadlock). HTTP 503 + Retry-After; fila: retry. */
export class TransientInfrastructureError extends ApplicationError {
  readonly code = 'TRANSIENT_FAILURE';

  constructor(
    message: string,
    readonly reason: 'connection' | 'lock_timeout' | 'deadlock' | 'unavailable',
    options?: { cause?: unknown },
  ) {
    super(message);
    if (options?.cause !== undefined) {
      (this as { cause?: unknown }).cause = options.cause;
    }
  }
}

/**
 * Violação de unicidade em `wager_transactions` causada por corrida entre instâncias.
 * Interno: o use case repete a operação e, na segunda vez, encontra a transação existente.
 */
export class DuplicateTransactionError extends ApplicationError {
  readonly code = 'DUPLICATE_TRANSACTION';

  constructor(constraint?: string) {
    super(`Concurrent duplicate transaction${constraint ? ` (${constraint})` : ''}`);
  }
}

/** A atualização condicionada de saldo não encontrou a versão esperada. Indica bug — nunca esperado. */
export class ConcurrencyViolationError extends ApplicationError {
  readonly code = 'CONCURRENCY_VIOLATION';

  constructor(walletId: string, expectedVersion: number) {
    super(`Wallet ${walletId} was modified concurrently (expected version ${expectedVersion})`);
  }
}
