import { InvariantViolationError } from '../shared/domain-error';
import { ExponentialBackoff } from '../shared/exponential-backoff';
import { Wallet } from '../wallet/wallet';
import { LedgerDirection, WalletLedgerEntry } from '../wallet/wallet-ledger-entry';
import { FailureCode } from './failure-code';
import { WagerTransaction, WagerTransactionStatus } from './wager-transaction';

export type ReferenceLookup =
  | { readonly found: false }
  | { readonly found: true; readonly transaction: WagerTransaction; readonly alreadyReversed: boolean };

export interface SettlementInput {
  wallet: Wallet;
  transaction: WagerTransaction;
  reference?: ReferenceLookup;
  ledgerEntryId: string;
  now: Date;
  referenceRetryPolicy: ExponentialBackoff;
}

export type SettlementOutcome =
  | { readonly type: 'processed'; readonly ledgerEntry?: WalletLedgerEntry; readonly reference?: WagerTransaction }
  | { readonly type: 'rejected'; readonly failureCode: FailureCode }
  | { readonly type: 'pending-reference'; readonly firstTime: boolean };

export const DEFAULT_REFERENCE_RETRY_POLICY = new ExponentialBackoff(2_000, 300_000, 8);

export function settleWagerTransaction(input: SettlementInput): SettlementOutcome {
  return new Settlement(input).run();
}

class Settlement {
  private readonly wallet: Wallet;
  private readonly transaction: WagerTransaction;
  private readonly now: Date;

  constructor(private readonly input: SettlementInput) {
    this.wallet = input.wallet;
    this.transaction = input.transaction;
    this.now = input.now;
    this.assertSettleable();
  }

  run(): SettlementOutcome {
    if (this.transaction.playerId !== this.wallet.playerId) {
      return this.rejectWithoutExposingBalance(FailureCode.PlayerWalletMismatch);
    }
    if (!this.transaction.money.hasCurrency(this.wallet.currency)) {
      return this.reject(FailureCode.CurrencyMismatch);
    }
    if (!this.transaction.hasReference()) {
      return this.apply(undefined);
    }

    const lookup = this.requireLookup();
    if (!lookup.found || isAwaitingSettlement(lookup.transaction)) {
      return this.awaitReference();
    }
    const failure = validateReference(this.transaction, lookup.transaction, lookup.alreadyReversed);
    if (failure) {
      return this.reject(failure, lookup.transaction.id);
    }
    return this.apply(lookup.transaction);
  }

  private apply(reference: WagerTransaction | undefined): SettlementOutcome {
    if (!this.transaction.affectsBalance()) {
      this.markProcessed(reference);
      return { type: 'processed', reference };
    }

    const direction = this.transaction.ledgerDirectionFor(reference);
    const blocked = this.balanceRuleViolation(direction);
    if (blocked) {
      return this.reject(blocked, reference?.id);
    }

    const movement = {
      transactionId: this.transaction.id,
      ledgerEntryId: this.input.ledgerEntryId,
      money: this.transaction.money,
      at: this.now,
    };
    const ledgerEntry =
      direction === LedgerDirection.Debit ? this.wallet.debit(movement) : this.wallet.credit(movement);
    this.markProcessed(reference);
    return { type: 'processed', ledgerEntry, reference };
  }

  private balanceRuleViolation(direction: LedgerDirection): FailureCode | undefined {
    if (direction === LedgerDirection.Credit) {
      return this.wallet.canCredit(this.transaction.money) ? undefined : FailureCode.BalanceLimitExceeded;
    }
    if (this.wallet.canDebit(this.transaction.money)) {
      return undefined;
    }
    return this.transaction.requiresReference() ? FailureCode.ReversalWouldOverdraw : FailureCode.InsufficientFunds;
  }

  private awaitReference(): SettlementOutcome {
    const policy = this.input.referenceRetryPolicy;
    if (this.transaction.status === WagerTransactionStatus.Pending) {
      this.transaction.markPendingReference(policy.nextAttemptAt(this.now, 1));
      return { type: 'pending-reference', firstTime: true };
    }

    const attemptsMade = this.transaction.referenceAttempts + 1;
    if (policy.isExhausted(attemptsMade)) {
      this.transaction.expireReference({ balanceAfter: this.wallet.balance, at: this.now });
      return { type: 'rejected', failureCode: FailureCode.ReferenceNotFound };
    }
    this.transaction.scheduleReferenceRetry(policy.nextAttemptAt(this.now, attemptsMade + 1));
    return { type: 'pending-reference', firstTime: false };
  }

  private markProcessed(reference: WagerTransaction | undefined): void {
    this.transaction.markProcessed({
      referenceTransactionId: reference?.id,
      balanceAfter: this.wallet.balance,
      at: this.now,
    });
  }

  private reject(code: FailureCode, referenceTransactionId?: string): SettlementOutcome {
    this.transaction.reject(code, { balanceAfter: this.wallet.balance, at: this.now, referenceTransactionId });
    return { type: 'rejected', failureCode: code };
  }

  private rejectWithoutExposingBalance(code: FailureCode): SettlementOutcome {
    this.transaction.reject(code, { at: this.now });
    return { type: 'rejected', failureCode: code };
  }

  private requireLookup(): ReferenceLookup {
    if (!this.input.reference) {
      throw new InvariantViolationError('Reference lookup is required for transactions with a reference');
    }
    return this.input.reference;
  }

  private assertSettleable(): void {
    if (this.transaction.walletId !== this.wallet.id) {
      throw new InvariantViolationError('Settlement received a wallet that does not own the transaction');
    }
    if (this.transaction.isTerminal()) {
      throw new InvariantViolationError(`Transaction ${this.transaction.id} is already ${this.transaction.status}`);
    }
  }
}

function isAwaitingSettlement(reference: WagerTransaction): boolean {
  return (
    reference.status === WagerTransactionStatus.Pending || reference.status === WagerTransactionStatus.PendingReference
  );
}

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
  if (!transaction.requiresReference()) {
    return alreadyReversed ? FailureCode.ReferenceReversed : undefined;
  }
  if (!reference.money.equals(transaction.money)) {
    return FailureCode.AmountMismatch;
  }
  return alreadyReversed ? FailureCode.AlreadyReversed : undefined;
}
