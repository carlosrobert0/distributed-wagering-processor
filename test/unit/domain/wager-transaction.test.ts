import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  InvalidTransactionStateError,
  InvalidWagerTransactionError,
} from '../../../src/domain/shared/domain-error';
import { FailureCode } from '../../../src/domain/wagering/failure-code';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../../src/domain/wagering/wager-transaction';
import { LedgerDirection } from '../../../src/domain/wallet/wallet-ledger-entry';
import { brl, newTransaction, NOW, openWallet } from '../../support/domain-builders';

const wallet = openWallet('100.00');
const later = new Date(NOW.getTime() + 1_000);

describe('WagerTransaction (AC-TX)', () => {
  describe('create', () => {
    test('nasce em PENDING (AC-TX-01)', () => {
      const tx = newTransaction(wallet);
      expect(tx.status).toBe(WagerTransactionStatus.Pending);
      expect(tx.isTerminal()).toBe(false);
    });

    test('OPENING não pode ser submetido (AC-TX-03)', () => {
      expect(() => newTransaction(wallet, { kind: WagerTransactionKind.Opening })).toThrow(InvalidWagerTransactionError);
    });

    test('providerId "internal" é reservado', () => {
      expect(() => newTransaction(wallet, { providerId: 'internal' })).toThrow(InvalidWagerTransactionError);
    });

    test.each([WagerTransactionKind.Refund, WagerTransactionKind.Rollback])(
      '%s exige referenceExternalTransactionId (AC-TX-04)',
      (kind) => {
        expect(() => newTransaction(wallet, { kind })).toThrow(InvalidWagerTransactionError);
        expect(newTransaction(wallet, { kind, referenceExternalTransactionId: 'bet-1' }).requiresReference()).toBe(true);
      },
    );

    test('WIN pode (opcionalmente) referenciar; BET não pode referenciar (INT-03)', () => {
      expect(newTransaction(wallet, { kind: WagerTransactionKind.Win }).hasReference()).toBe(false);
      expect(
        newTransaction(wallet, { kind: WagerTransactionKind.Win, referenceExternalTransactionId: 'bet-1' }).hasReference(),
      ).toBe(true);
      expect(() => newTransaction(wallet, { referenceExternalTransactionId: 'bet-0' })).toThrow(
        InvalidWagerTransactionError,
      );
    });

    test('não pode referenciar a si mesma', () => {
      expect(() =>
        newTransaction(wallet, {
          kind: WagerTransactionKind.Refund,
          externalTransactionId: 'same',
          referenceExternalTransactionId: 'same',
        }),
      ).toThrow(InvalidWagerTransactionError);
    });

    test('valor zero só é aceito para LOSS (INT-02)', () => {
      expect(() => newTransaction(wallet, { money: brl('0.00') })).toThrow(InvalidWagerTransactionError);
      expect(newTransaction(wallet, { kind: WagerTransactionKind.Loss, money: brl('0.00') }).money.isZero()).toBe(true);
    });

    test('identificadores vazios são rejeitados', () => {
      expect(() => newTransaction(wallet, { roundId: '' })).toThrow(InvalidWagerTransactionError);
    });
  });

  describe('máquina de estados (AC-TX-02)', () => {
    test('PENDING → PROCESSED guarda referência, saldo observado e data', () => {
      const tx = newTransaction(wallet);
      tx.markProcessed({ referenceTransactionId: undefined, balanceAfter: brl('90.00'), at: later });
      expect(tx.status).toBe(WagerTransactionStatus.Processed);
      expect(tx.balanceAfter?.toJSON().amount).toBe('90.00');
      expect(tx.processedAt).toEqual(later);
    });

    test('PENDING → REJECTED carrega failureCode', () => {
      const tx = newTransaction(wallet);
      tx.reject(FailureCode.InsufficientFunds, { balanceAfter: brl('5.00'), at: later });
      expect(tx.status).toBe(WagerTransactionStatus.Rejected);
      expect(tx.failureCode).toBe(FailureCode.InsufficientFunds);
    });

    test('PENDING → PENDING_REFERENCE → (retry) → PROCESSED', () => {
      const tx = newTransaction(wallet, { kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'bet-1' });
      tx.markPendingReference(later);
      expect(tx.status).toBe(WagerTransactionStatus.PendingReference);
      tx.scheduleReferenceRetry(new Date(later.getTime() + 2_000));
      expect(tx.referenceAttempts).toBe(1);
      tx.markProcessed({ referenceTransactionId: randomUUID(), balanceAfter: brl('110.00'), at: later });
      expect(tx.status).toBe(WagerTransactionStatus.Processed);
      expect(tx.nextReferenceCheckAt).toBeUndefined();
    });

    test('PENDING_REFERENCE → FAILED', () => {
      const tx = newTransaction(wallet, { kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: 'x' });
      tx.markPendingReference(later);
      tx.fail(FailureCode.ProcessingError, later);
      expect(tx.status).toBe(WagerTransactionStatus.Failed);
    });

    test.each([
      ['PROCESSED', (tx: WagerTransaction) => tx.markProcessed({ referenceTransactionId: undefined, balanceAfter: brl('1.00'), at: later })],
      ['REJECTED', (tx: WagerTransaction) => tx.reject(FailureCode.InsufficientFunds, { balanceAfter: brl('1.00'), at: later })],
      ['FAILED', (tx: WagerTransaction) => tx.fail(FailureCode.ProcessingError, later)],
    ])('%s é terminal: qualquer transição lança InvalidTransactionStateError', (_status, toTerminal) => {
      const tx = newTransaction(wallet);
      toTerminal(tx);
      expect(tx.isTerminal()).toBe(true);
      expect(() => tx.markProcessed({ referenceTransactionId: undefined, balanceAfter: brl('1.00'), at: later })).toThrow(
        InvalidTransactionStateError,
      );
      expect(() => tx.reject(FailureCode.InsufficientFunds, { balanceAfter: brl('1.00'), at: later })).toThrow(
        InvalidTransactionStateError,
      );
      expect(() => tx.fail(FailureCode.ProcessingError, later)).toThrow(InvalidTransactionStateError);
      expect(() => tx.markPendingReference(later)).toThrow(InvalidTransactionStateError);
    });

    test('scheduleReferenceRetry só vale em PENDING_REFERENCE', () => {
      expect(() => newTransaction(wallet).scheduleReferenceRetry(later)).toThrow(InvalidTransactionStateError);
    });
  });

  describe('consultas de domínio', () => {
    test('affectsBalance é false somente para LOSS', () => {
      expect(newTransaction(wallet, { kind: WagerTransactionKind.Loss }).affectsBalance()).toBe(false);
      expect(newTransaction(wallet).affectsBalance()).toBe(true);
    });

    test('ledgerDirectionFor: BET débito, WIN/REFUND crédito, ROLLBACK inverte a referência (AC-TX-15)', () => {
      const bet = newTransaction(wallet);
      const win = newTransaction(wallet, { kind: WagerTransactionKind.Win });
      const refund = newTransaction(wallet, { kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'b' });
      const rollback = newTransaction(wallet, { kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: 'b' });

      expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
      expect(win.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
      expect(refund.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
      expect(rollback.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
      expect(rollback.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
      expect(rollback.ledgerDirectionFor(refund)).toBe(LedgerDirection.Debit);
    });

    test('matchesPayload: mesma key com hash diferente não é replay (AC-IDEM-04)', () => {
      const tx = newTransaction(wallet, { payloadHash: 'abc' });
      expect(tx.matchesPayload('abc')).toBe(true);
      expect(tx.matchesPayload('xyz')).toBe(false);
    });

    test('canReference aplica a regra 3 da §7 (AC-TX-06)', () => {
      const refund = newTransaction(wallet, { kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'b' });
      const rollback = newTransaction(wallet, { kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: 'b' });
      expect(refund.canReference(WagerTransactionKind.Bet)).toBe(true);
      expect(refund.canReference(WagerTransactionKind.Win)).toBe(false);
      expect(rollback.canReference(WagerTransactionKind.Win)).toBe(true);
      expect(rollback.canReference(WagerTransactionKind.Refund)).toBe(true);
      expect(rollback.canReference(WagerTransactionKind.Loss)).toBe(false);
      expect(rollback.canReference(WagerTransactionKind.Rollback)).toBe(false);
    });
  });

  test('OPENING interno é construído só pela factory dedicada (INT-10)', () => {
    const tx = WagerTransaction.opening({
      id: randomUUID(),
      walletId: wallet.id,
      playerId: wallet.playerId,
      money: brl('100.00'),
      payloadHash: 'h',
      createdAt: NOW,
    });
    expect(tx.kind).toBe(WagerTransactionKind.Opening);
    expect(tx.providerId).toBe('internal');
    expect(tx.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
  });
});
