import { InvariantViolationError } from '../shared/domain-error';
import { ExponentialBackoff } from '../shared/exponential-backoff';
import { Wallet } from '../wallet/wallet';
import { LedgerDirection, WalletLedgerEntry } from '../wallet/wallet-ledger-entry';
import { FailureCode } from './failure-code';
import { WagerTransaction, WagerTransactionStatus } from './wager-transaction';

/** Resultado da busca da referência, feita pela aplicação antes de chamar o domínio. */
export type ReferenceLookup =
  | { readonly found: false }
  | { readonly found: true; readonly transaction: WagerTransaction; readonly alreadyReversed: boolean };

export interface SettlementInput {
  /** Wallet já travada (`FOR UPDATE`) pela aplicação. */
  wallet: Wallet;
  /** Transação em PENDING ou PENDING_REFERENCE. */
  transaction: WagerTransaction;
  /** Obrigatório quando a transação traz `referenceExternalTransactionId`. */
  reference?: ReferenceLookup;
  ledgerEntryId: string;
  now: Date;
  referenceRetryPolicy: ExponentialBackoff;
}

export type SettlementOutcome =
  | { readonly type: 'processed'; readonly ledgerEntry?: WalletLedgerEntry; readonly reference?: WagerTransaction }
  | { readonly type: 'rejected'; readonly failureCode: FailureCode }
  | { readonly type: 'pending-reference'; readonly firstTime: boolean };

/**
 * Política padrão para PENDING_REFERENCE (INT-06): 8 verificações, 2s·2ⁿ, teto 5 min ≈ 8,5 min no total.
 */
export const DEFAULT_REFERENCE_RETRY_POLICY = new ExponentialBackoff(2_000, 300_000, 8);

/**
 * Serviço de domínio que decide e aplica o efeito de uma transação sobre a wallet.
 *
 * É puro (sem I/O): recebe a wallet travada e a referência já buscada, muta wallet e transação
 * em memória e devolve o desfecho. A aplicação persiste o resultado na mesma transação SQL.
 * Usado tanto pela entrada HTTP/SQS quanto pelo worker de PENDING_REFERENCE.
 */
export function settleWagerTransaction(input: SettlementInput): SettlementOutcome {
  const { wallet, transaction, now } = input;

  if (transaction.walletId !== wallet.id) {
    throw new InvariantViolationError('Settlement received a wallet that does not own the transaction');
  }
  if (transaction.isTerminal()) {
    throw new InvariantViolationError(`Transaction ${transaction.id} is already ${transaction.status}`);
  }

  const reject = (code: FailureCode): SettlementOutcome => {
    transaction.reject(code, { balanceAfter: wallet.balance, at: now });
    return { type: 'rejected', failureCode: code };
  };

  if (transaction.playerId !== wallet.playerId) {
    return reject(FailureCode.PlayerWalletMismatch);
  }
  if (!transaction.money.hasCurrency(wallet.currency)) {
    return reject(FailureCode.CurrencyMismatch);
  }

  let reference: WagerTransaction | undefined;
  if (transaction.hasReference()) {
    const lookup = input.reference;
    if (!lookup) {
      throw new InvariantViolationError('Reference lookup is required for transactions with a reference');
    }
    // Referência ausente, ou presente mas ainda não aplicada (AC-TX-11, AC-TX-16): espera.
    if (!lookup.found || isAwaitingSettlement(lookup.transaction)) {
      return awaitReference(transaction, input, reject);
    }
    const failure = validateReference(transaction, lookup.transaction, lookup.alreadyReversed);
    if (failure) {
      return reject(failure);
    }
    reference = lookup.transaction;
  }

  if (!transaction.affectsBalance()) {
    // LOSS: registra o resultado sem mover saldo nem gerar lançamento (AC-TX-14).
    transaction.markProcessed({ referenceTransactionId: reference?.id, balanceAfter: wallet.balance, at: now });
    return { type: 'processed', reference };
  }

  const movement = {
    transactionId: transaction.id,
    ledgerEntryId: input.ledgerEntryId,
    money: transaction.money,
    at: now,
  };
  const direction = transaction.ledgerDirectionFor(reference);

  let ledgerEntry: WalletLedgerEntry;
  if (direction === LedgerDirection.Debit) {
    if (!wallet.canDebit(transaction.money)) {
      // AC-TX-12/13: códigos distintos para aposta sem saldo e reversão que estouraria o saldo.
      return reject(transaction.isReversal() ? FailureCode.ReversalWouldOverdraw : FailureCode.InsufficientFunds);
    }
    ledgerEntry = wallet.debit(movement);
  } else {
    ledgerEntry = wallet.credit(movement);
  }

  transaction.markProcessed({ referenceTransactionId: reference?.id, balanceAfter: wallet.balance, at: now });
  return { type: 'processed', ledgerEntry, reference };
}

function isAwaitingSettlement(reference: WagerTransaction): boolean {
  return (
    reference.status === WagerTransactionStatus.Pending || reference.status === WagerTransactionStatus.PendingReference
  );
}

function awaitReference(
  transaction: WagerTransaction,
  input: SettlementInput,
  reject: (code: FailureCode) => SettlementOutcome,
): SettlementOutcome {
  const policy = input.referenceRetryPolicy;

  if (transaction.status === WagerTransactionStatus.Pending) {
    transaction.markPendingReference(policy.nextAttemptAt(input.now, 1));
    return { type: 'pending-reference', firstTime: true };
  }

  const attemptsMade = transaction.referenceAttempts + 1;
  if (policy.isExhausted(attemptsMade)) {
    transaction.recordReferenceAttempt();
    return reject(FailureCode.ReferenceNotFound);
  }
  transaction.scheduleReferenceRetry(policy.nextAttemptAt(input.now, attemptsMade + 1));
  return { type: 'pending-reference', firstTime: false };
}

/** Regras 2, 3, 4 e 5 da §7. A ordem define qual código vence quando há mais de uma violação. */
function validateReference(
  transaction: WagerTransaction,
  reference: WagerTransaction,
  alreadyReversed: boolean,
): FailureCode | undefined {
  if (reference.status !== WagerTransactionStatus.Processed) {
    return FailureCode.ReferenceNotProcessed;
  }
  const sameContext =
    reference.providerId === transaction.providerId &&
    reference.playerId === transaction.playerId &&
    reference.walletId === transaction.walletId &&
    reference.roundId === transaction.roundId &&
    reference.money.currency === transaction.money.currency;
  if (!sameContext) {
    return FailureCode.ReferenceMismatch;
  }
  if (!transaction.canReference(reference.kind)) {
    return FailureCode.InvalidReferenceKind;
  }
  if (transaction.isReversal()) {
    if (!reference.money.equals(transaction.money)) {
      return FailureCode.AmountMismatch;
    }
    if (alreadyReversed) {
      return FailureCode.AlreadyReversed;
    }
  }
  return undefined;
}
