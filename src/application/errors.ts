export abstract class ApplicationError extends Error {
  abstract readonly code: string;

  protected constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

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

export class InboxMessageConflictError extends InvalidPayloadError {
  constructor(consumerName: string, messageId: string) {
    super(
      `Message ${consumerName}/${messageId} was already received with a different payload`,
      undefined,
      'INBOX_MESSAGE_CONFLICT',
    );
  }
}

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

export type TransientReason = 'connection' | 'lock_timeout' | 'statement_timeout' | 'deadlock' | 'race';

export class TransientInfrastructureError extends ApplicationError {
  readonly code = 'TRANSIENT_FAILURE';

  constructor(
    message: string,
    readonly reason: TransientReason,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export class DuplicateTransactionError extends TransientInfrastructureError {
  constructor(constraint?: string) {
    super(`Concurrent duplicate transaction${constraint ? ` (${constraint})` : ''}`, 'race');
  }
}

export class ConcurrencyViolationError extends TransientInfrastructureError {
  constructor(walletId: string, expectedVersion: number) {
    super(`Wallet ${walletId} was modified concurrently (expected version ${expectedVersion})`, 'race');
  }
}

export class IntegrityViolationError extends ApplicationError {
  readonly code = 'INTEGRITY_VIOLATION';

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
  }
}
