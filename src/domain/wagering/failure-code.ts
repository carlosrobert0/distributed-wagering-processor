/**
 * Taxonomia estável de rejeições (legível por máquina). Ver SPEC.md §4.2 para a ação esperada do
 * provedor em cada caso. Os valores são persistidos — nunca renomeie, apenas adicione.
 */
export enum FailureCode {
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  ReversalWouldOverdraw = 'REVERSAL_WOULD_OVERDRAW',
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',
  InvalidReferenceKind = 'INVALID_REFERENCE_KIND',
  ReferenceMismatch = 'REFERENCE_MISMATCH',
  AmountMismatch = 'AMOUNT_MISMATCH',
  AlreadyReversed = 'ALREADY_REVERSED',
  CurrencyMismatch = 'CURRENCY_MISMATCH',
  PlayerWalletMismatch = 'PLAYER_WALLET_MISMATCH',
  ProcessingError = 'PROCESSING_ERROR',
}

const FAILURE_CODES = new Set<string>(Object.values(FailureCode));

export function isFailureCode(value: string): value is FailureCode {
  return FAILURE_CODES.has(value);
}
