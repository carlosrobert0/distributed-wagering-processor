export abstract class DomainError extends Error {
  abstract readonly code: string;

  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export abstract class DomainValidationError extends DomainError {}

export abstract class BusinessRuleViolationError extends DomainError {}

export class InvalidMoneyError extends DomainValidationError {
  readonly code = 'INVALID_MONEY';

  constructor(message: string) {
    super(message);
  }
}

export class InvalidWagerTransactionError extends DomainValidationError {
  readonly code = 'INVALID_WAGER_TRANSACTION';

  constructor(message: string) {
    super(message);
  }
}

export class InvalidWalletError extends DomainValidationError {
  readonly code = 'INVALID_WALLET';

  constructor(message: string) {
    super(message);
  }
}

export class CurrencyMismatchError extends BusinessRuleViolationError {
  readonly code = 'CURRENCY_MISMATCH';

  constructor(expected: string, actual: string) {
    super(`Currency mismatch: expected ${expected}, got ${actual}`);
  }
}

export class InsufficientFundsError extends BusinessRuleViolationError {
  readonly code = 'INSUFFICIENT_FUNDS';

  constructor(walletId: string) {
    super(`Insufficient funds in wallet ${walletId}`);
  }
}

export class InvariantViolationError extends DomainError {
  readonly code = 'INVARIANT_VIOLATION';

  constructor(message: string) {
    super(message);
  }
}

export class InvalidTransactionStateError extends DomainError {
  readonly code = 'INVALID_TRANSACTION_STATE';

  constructor(transactionId: string, from: string, attempted: string) {
    super(`Transaction ${transactionId} cannot go from ${from} to ${attempted}`);
  }
}
