import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { ExponentialBackoff } from '../../../src/domain/shared/exponential-backoff';
import { FailureCode } from '../../../src/domain/wagering/failure-code';
import {
  ReferenceLookup,
  settleWagerTransaction,
  SettlementOutcome,
} from '../../../src/domain/wagering/wager-settlement';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../../src/domain/wagering/wager-transaction';
import { Wallet } from '../../../src/domain/wallet/wallet';
import { LedgerDirection } from '../../../src/domain/wallet/wallet-ledger-entry';
import { brl, newTransaction, NOW, openWallet, TxOverrides, usd } from '../../support/domain-builders';

const policy = new ExponentialBackoff(2_000, 300_000, 3);

function settle(wallet: Wallet, tx: WagerTransaction, reference?: ReferenceLookup, now = NOW): SettlementOutcome {
  return settleWagerTransaction({
    wallet,
    transaction: tx,
    reference,
    ledgerEntryId: randomUUID(),
    now,
    referenceRetryPolicy: policy,
  });
}

function found(transaction: WagerTransaction, alreadyReversed = false): ReferenceLookup {
  return { found: true, transaction, alreadyReversed };
}

function applied(wallet: Wallet, overrides: TxOverrides = {}, reference?: WagerTransaction): WagerTransaction {
  const tx = newTransaction(wallet, overrides);
  const outcome = settle(wallet, tx, reference ? found(reference) : undefined);
  expect(outcome.type).toBe('processed');
  return tx;
}

describe('settleWagerTransaction — regras de negócio (§7)', () => {
  describe('BET', () => {
    test('debita e gera 1 lançamento DEBIT', () => {
      const wallet = openWallet('100.00');
      const tx = newTransaction(wallet, { money: brl('25.00') });
      const outcome = settle(wallet, tx);

      expect(outcome.type).toBe('processed');
      if (outcome.type !== 'processed') return;
      expect(outcome.ledgerEntry?.direction).toBe(LedgerDirection.Debit);
      expect(wallet.balance.toJSON().amount).toBe('75.00');
      expect(tx.balanceAfter?.toJSON().amount).toBe('75.00');
    });

    test('saldo insuficiente → REJECTED INSUFFICIENT_FUNDS, sem lançamento e sem mudar version (AC-TX-13, AC-TX-09)', () => {
      const wallet = openWallet('100.00');
      applied(wallet, { money: brl('80.00') });
      const second = newTransaction(wallet, { money: brl('80.00') });
      const outcome = settle(wallet, second);

      expect(outcome).toEqual({ type: 'rejected', failureCode: FailureCode.InsufficientFunds });
      expect(second.status).toBe(WagerTransactionStatus.Rejected);
      expect(second.balanceAfter?.toJSON().amount).toBe('20.00');
      expect(wallet.balance.toJSON().amount).toBe('20.00');
      expect(wallet.version).toBe(2);
    });

    test('moeda diferente da wallet → REJECTED CURRENCY_MISMATCH', () => {
      const wallet = openWallet('100.00');
      const tx = newTransaction(wallet, { money: usd('1.00') });
      expect(settle(wallet, tx)).toEqual({ type: 'rejected', failureCode: FailureCode.CurrencyMismatch });
      expect(wallet.version).toBe(1);
    });

    test('player diferente do dono da wallet → REJECTED PLAYER_WALLET_MISMATCH', () => {
      const wallet = openWallet('100.00');
      const tx = newTransaction(wallet, { playerId: 'someone-else' });
      expect(settle(wallet, tx)).toEqual({ type: 'rejected', failureCode: FailureCode.PlayerWalletMismatch });
    });
  });

  describe('WIN', () => {
    test('credita sem referência', () => {
      const wallet = openWallet('10.00');
      const tx = newTransaction(wallet, { kind: WagerTransactionKind.Win, money: brl('50.00') });
      const outcome = settle(wallet, tx);
      expect(outcome.type).toBe('processed');
      expect(wallet.balance.toJSON().amount).toBe('60.00');
    });

    test('credita referenciando a BET da mesma rodada', () => {
      const wallet = openWallet('100.00');
      const bet = applied(wallet, { externalTransactionId: 'bet-1', money: brl('10.00') });
      const win = applied(
        wallet,
        { kind: WagerTransactionKind.Win, referenceExternalTransactionId: 'bet-1', money: brl('30.00') },
        bet,
      );
      expect(win.referenceTransactionId).toBe(bet.id);
      expect(wallet.balance.toJSON().amount).toBe('120.00');
    });

    test('referenciando BET de outra rodada → REFERENCE_MISMATCH', () => {
      const wallet = openWallet('100.00');
      const bet = applied(wallet, { externalTransactionId: 'bet-1', roundId: 'round-A' });
      const win = newTransaction(wallet, {
        kind: WagerTransactionKind.Win,
        referenceExternalTransactionId: 'bet-1',
        roundId: 'round-B',
      });
      expect(settle(wallet, win, found(bet))).toEqual({ type: 'rejected', failureCode: FailureCode.ReferenceMismatch });
    });
  });

  describe('LOSS', () => {
    test('PROCESSED sem mover saldo, sem lançamento e sem incrementar version (AC-TX-14)', () => {
      const wallet = openWallet('100.00');
      const tx = newTransaction(wallet, { kind: WagerTransactionKind.Loss, money: brl('0.00') });
      const outcome = settle(wallet, tx);

      expect(outcome).toEqual({ type: 'processed', reference: undefined });
      expect(tx.status).toBe(WagerTransactionStatus.Processed);
      expect(tx.balanceAfter?.toJSON().amount).toBe('100.00');
      expect(wallet.version).toBe(1);
    });
  });

  describe('REFUND', () => {
    test('credita o valor da BET referenciada', () => {
      const wallet = openWallet('100.00');
      const bet = applied(wallet, { externalTransactionId: 'bet-1', money: brl('40.00') });
      const refund = applied(
        wallet,
        { kind: WagerTransactionKind.Refund, referenceExternalTransactionId: 'bet-1', money: brl('40.00') },
        bet,
      );
      expect(refund.referenceTransactionId).toBe(bet.id);
      expect(wallet.balance.toJSON().amount).toBe('100.00');
      expect(wallet.version).toBe(3);
    });

    test('só referencia BET → INVALID_REFERENCE_KIND', () => {
      const wallet = openWallet('100.00');
      const win = applied(wallet, { kind: WagerTransactionKind.Win, externalTransactionId: 'win-1' });
      const refund = newTransaction(wallet, {
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: 'win-1',
      });
      expect(settle(wallet, refund, found(win))).toEqual({
        type: 'rejected',
        failureCode: FailureCode.InvalidReferenceKind,
      });
    });

    test('valor diferente da referência → AMOUNT_MISMATCH (AC-TX-08)', () => {
      const wallet = openWallet('100.00');
      const bet = applied(wallet, { externalTransactionId: 'bet-1', money: brl('40.00') });
      const refund = newTransaction(wallet, {
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: 'bet-1',
        money: brl('39.99'),
      });
      expect(settle(wallet, refund, found(bet))).toEqual({ type: 'rejected', failureCode: FailureCode.AmountMismatch });
      expect(wallet.balance.toJSON().amount).toBe('60.00');
    });

    test('referência já revertida → ALREADY_REVERSED (AC-TX-07)', () => {
      const wallet = openWallet('100.00');
      const bet = applied(wallet, { externalTransactionId: 'bet-1', money: brl('40.00') });
      const refund = newTransaction(wallet, {
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: 'bet-1',
        money: brl('40.00'),
      });
      expect(settle(wallet, refund, found(bet, true))).toEqual({
        type: 'rejected',
        failureCode: FailureCode.AlreadyReversed,
      });
    });

    test('referência REJECTED → REFERENCE_NOT_PROCESSED', () => {
      const wallet = openWallet('10.00');
      const bet = newTransaction(wallet, { externalTransactionId: 'bet-1', money: brl('40.00') });
      settle(wallet, bet);
      const refund = newTransaction(wallet, {
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: 'bet-1',
        money: brl('40.00'),
      });
      expect(settle(wallet, refund, found(bet))).toEqual({
        type: 'rejected',
        failureCode: FailureCode.ReferenceNotProcessed,
      });
    });
  });

  describe('ROLLBACK', () => {
    test('de BET credita (inverso)', () => {
      const wallet = openWallet('100.00');
      const bet = applied(wallet, { externalTransactionId: 'bet-1', money: brl('30.00') });
      const rollback = newTransaction(wallet, {
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: 'bet-1',
        money: brl('30.00'),
      });
      const outcome = settle(wallet, rollback, found(bet));
      expect(outcome.type === 'processed' && outcome.ledgerEntry?.direction).toBe(LedgerDirection.Credit);
      expect(wallet.balance.toJSON().amount).toBe('100.00');
    });

    test('de WIN debita (inverso)', () => {
      const wallet = openWallet('0.00');
      const win = applied(wallet, {
        kind: WagerTransactionKind.Win,
        externalTransactionId: 'win-1',
        money: brl('50.00'),
      });
      const rollback = newTransaction(wallet, {
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: 'win-1',
        money: brl('50.00'),
      });
      const outcome = settle(wallet, rollback, found(win));
      expect(outcome.type === 'processed' && outcome.ledgerEntry?.direction).toBe(LedgerDirection.Debit);
      expect(wallet.balance.isZero()).toBe(true);
    });

    test('de WIN cujo crédito já foi gasto → REVERSAL_WOULD_OVERDRAW, distinto de INSUFFICIENT_FUNDS (AC-TX-12)', () => {
      const wallet = openWallet('0.00');
      const win = applied(wallet, {
        kind: WagerTransactionKind.Win,
        externalTransactionId: 'win-1',
        money: brl('50.00'),
      });
      applied(wallet, { money: brl('45.00') });
      const rollback = newTransaction(wallet, {
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: 'win-1',
        money: brl('50.00'),
      });
      expect(settle(wallet, rollback, found(win))).toEqual({
        type: 'rejected',
        failureCode: FailureCode.ReversalWouldOverdraw,
      });
      expect(wallet.balance.toJSON().amount).toBe('5.00');
      expect(rollback.status).toBe(WagerTransactionStatus.Rejected);
    });

    test('de REFUND debita (inverso)', () => {
      const wallet = openWallet('100.00');
      const bet = applied(wallet, { externalTransactionId: 'bet-1', money: brl('20.00') });
      const refund = applied(
        wallet,
        {
          kind: WagerTransactionKind.Refund,
          externalTransactionId: 'refund-1',
          referenceExternalTransactionId: 'bet-1',
          money: brl('20.00'),
        },
        bet,
      );
      const rollback = newTransaction(wallet, {
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: 'refund-1',
        money: brl('20.00'),
      });
      const outcome = settle(wallet, rollback, found(refund));
      expect(outcome.type === 'processed' && outcome.ledgerEntry?.direction).toBe(LedgerDirection.Debit);
      expect(wallet.balance.toJSON().amount).toBe('80.00');
    });

    test('de LOSS → INVALID_REFERENCE_KIND', () => {
      const wallet = openWallet('100.00');
      const loss = applied(wallet, {
        kind: WagerTransactionKind.Loss,
        externalTransactionId: 'loss-1',
        money: brl('5.00'),
      });
      const rollback = newTransaction(wallet, {
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: 'loss-1',
        money: brl('5.00'),
      });
      expect(settle(wallet, rollback, found(loss))).toEqual({
        type: 'rejected',
        failureCode: FailureCode.InvalidReferenceKind,
      });
    });
  });

  describe('referência fora de ordem (AC-TX-11, §7.1)', () => {
    const missing: ReferenceLookup = { found: false };

    test('referência ausente → PENDING_REFERENCE, sem tocar no saldo', () => {
      const wallet = openWallet('100.00');
      const refund = newTransaction(wallet, {
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: 'bet-x',
      });
      expect(settle(wallet, refund, missing)).toEqual({ type: 'pending-reference', firstTime: true });
      expect(refund.status).toBe(WagerTransactionStatus.PendingReference);
      expect(refund.nextReferenceCheckAt).toEqual(new Date(NOW.getTime() + 2_000));
      expect(wallet.version).toBe(1);
    });

    test('backoff exponencial e REFERENCE_NOT_FOUND ao esgotar o limite', () => {
      const wallet = openWallet('100.00');
      const refund = newTransaction(wallet, {
        kind: WagerTransactionKind.Refund,
        referenceExternalTransactionId: 'bet-x',
      });
      settle(wallet, refund, missing);

      expect(settle(wallet, refund, missing)).toEqual({ type: 'pending-reference', firstTime: false });
      expect(refund.nextReferenceCheckAt).toEqual(new Date(NOW.getTime() + 4_000));
      expect(settle(wallet, refund, missing)).toEqual({ type: 'pending-reference', firstTime: false });
      expect(refund.nextReferenceCheckAt).toEqual(new Date(NOW.getTime() + 8_000));
      expect(settle(wallet, refund, missing)).toEqual({
        type: 'rejected',
        failureCode: FailureCode.ReferenceNotFound,
      });
      expect(refund.referenceAttempts).toBe(3);
    });

    test('referência chega depois → aplicada pelo reprocessamento', () => {
      const wallet = openWallet('100.00');
      const rollback = newTransaction(wallet, {
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: 'bet-1',
        money: brl('10.00'),
      });
      settle(wallet, rollback, missing);
      const bet = applied(wallet, { externalTransactionId: 'bet-1', money: brl('10.00') });

      const outcome = settle(wallet, rollback, found(bet));
      expect(outcome.type).toBe('processed');
      expect(wallet.balance.toJSON().amount).toBe('100.00');
    });

    test('referência existente mas ainda pendente → continua esperando (AC-TX-16)', () => {
      const wallet = openWallet('100.00');
      const pendingRefund = newTransaction(wallet, {
        kind: WagerTransactionKind.Refund,
        externalTransactionId: 'refund-1',
        referenceExternalTransactionId: 'bet-x',
      });
      settle(wallet, pendingRefund, missing);
      const rollback = newTransaction(wallet, {
        kind: WagerTransactionKind.Rollback,
        referenceExternalTransactionId: 'refund-1',
      });
      expect(settle(wallet, rollback, found(pendingRefund))).toEqual({ type: 'pending-reference', firstTime: true });
    });
  });
});
