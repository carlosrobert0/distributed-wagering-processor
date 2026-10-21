import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  CurrencyMismatchError,
  InsufficientFundsError,
  InvalidWalletError,
  InvariantViolationError,
} from '../../../src/domain/shared/domain-error';
import { Money } from '../../../src/domain/shared/money';
import { Wallet } from '../../../src/domain/wallet/wallet';
import { LedgerDirection, WalletLedgerEntry } from '../../../src/domain/wallet/wallet-ledger-entry';
import { brl, NOW, openWallet, usd } from '../../support/domain-builders';

const movement = (money: Money) => ({
  transactionId: randomUUID(),
  ledgerEntryId: randomUUID(),
  money,
  at: NOW,
});

describe('Wallet (AC-WAL)', () => {
  describe('open', () => {
    test('saldo inicial positivo gera lançamento CREDIT de abertura com version 1 (AC-WAL-05/06)', () => {
      const { wallet, openingEntry } = Wallet.open({
        id: randomUUID(),
        playerId: 'player-1',
        initialBalance: brl('1000.00'),
        at: NOW,
        openingTransactionId: 'opening-tx',
        openingLedgerEntryId: 'opening-entry',
      });

      expect(wallet.version).toBe(1);
      expect(wallet.balance.toJSON()).toEqual({ amount: '1000.00', currency: 'BRL' });
      expect(wallet.currency).toBe('BRL');
      expect(openingEntry).toBeDefined();
      expect(openingEntry!.direction).toBe(LedgerDirection.Credit);
      expect(openingEntry!.walletVersion).toBe(1);
      expect(openingEntry!.transactionId).toBe('opening-tx');
      expect(openingEntry!.balanceBefore.isZero()).toBe(true);
      expect(openingEntry!.balanceAfter.equals(wallet.balance)).toBe(true);
    });

    test('saldo inicial zero não gera lançamento', () => {
      const { wallet, openingEntry } = Wallet.open({
        id: randomUUID(),
        playerId: 'player-1',
        initialBalance: brl('0.00'),
        at: NOW,
        openingTransactionId: 'x',
        openingLedgerEntryId: 'y',
      });
      expect(openingEntry).toBeUndefined();
      expect(wallet.version).toBe(1);
      expect(wallet.balance.isZero()).toBe(true);
    });

    test('playerId vazio é inválido', () => {
      expect(() =>
        Wallet.open({
          id: randomUUID(),
          playerId: '  ',
          initialBalance: brl('1.00'),
          at: NOW,
          openingTransactionId: 'x',
          openingLedgerEntryId: 'y',
        }),
      ).toThrow(InvalidWalletError);
    });
  });

  describe('debit/credit', () => {
    test('debit reduz saldo, incrementa version e retorna lançamento balanceado (AC-WAL-03/05)', () => {
      const wallet = openWallet('100.00');
      const entry = wallet.debit(movement(brl('80.00')));

      expect(wallet.balance.toJSON().amount).toBe('20.00');
      expect(wallet.version).toBe(2);
      expect(entry.direction).toBe(LedgerDirection.Debit);
      expect(entry.walletVersion).toBe(2);
      expect(entry.balanceBefore.toJSON().amount).toBe('100.00');
      expect(entry.balanceAfter.toJSON().amount).toBe('20.00');
      expect(entry.isBalanced()).toBe(true);
    });

    test('credit aumenta saldo e incrementa version', () => {
      const wallet = openWallet('10.00');
      const entry = wallet.credit(movement(brl('5.55')));
      expect(wallet.balance.toJSON().amount).toBe('15.55');
      expect(wallet.version).toBe(2);
      expect(entry.direction).toBe(LedgerDirection.Credit);
    });

    test('debit que deixaria saldo negativo lança e não altera estado (AC-WAL-02)', () => {
      const wallet = openWallet('100.00');
      wallet.debit(movement(brl('80.00')));

      expect(() => wallet.debit(movement(brl('80.00')))).toThrow(InsufficientFundsError);
      expect(wallet.balance.toJSON().amount).toBe('20.00');
      expect(wallet.version).toBe(2);
    });

    test('debit do saldo exato zera a wallet', () => {
      const wallet = openWallet('25.00');
      wallet.debit(movement(brl('25.00')));
      expect(wallet.balance.isZero()).toBe(true);
    });

    test('moeda diferente da wallet é rejeitada (AC-WAL-04)', () => {
      const wallet = openWallet('100.00');
      expect(() => wallet.debit(movement(usd('1.00')))).toThrow(CurrencyMismatchError);
      expect(() => wallet.credit(movement(usd('1.00')))).toThrow(CurrencyMismatchError);
      expect(wallet.version).toBe(1);
    });

    test('movimento de valor zero é erro de invariante', () => {
      const wallet = openWallet('100.00');
      expect(() => wallet.credit(movement(brl('0.00')))).toThrow(InvariantViolationError);
    });

    test('sequência de lançamentos forma uma cadeia: before(n) == after(n-1)', () => {
      const wallet = openWallet('100.00');
      const entries = [
        wallet.debit(movement(brl('30.00'))),
        wallet.credit(movement(brl('12.34'))),
        wallet.debit(movement(brl('0.01'))),
      ];
      for (let i = 1; i < entries.length; i++) {
        expect(entries[i]!.balanceBefore.equals(entries[i - 1]!.balanceAfter)).toBe(true);
        expect(entries[i]!.walletVersion).toBe(entries[i - 1]!.walletVersion + 1);
      }
      expect(wallet.balance.toJSON().amount).toBe('82.33');
    });
  });

  test('rehydrate reconstrói estado sem revalidar (AC-WAL-07)', () => {
    const wallet = Wallet.rehydrate({
      id: 'w1',
      playerId: 'p1',
      currency: 'BRL',
      balance: brl('42.00'),
      version: 7,
      createdAt: NOW,
      updatedAt: NOW,
    });
    expect(wallet.version).toBe(7);
    expect(wallet.balance.toJSON().amount).toBe('42.00');
  });
});

describe('WalletLedgerEntry (AC-LED)', () => {
  const base = {
    id: 'e1',
    walletId: 'w1',
    transactionId: 't1',
    walletVersion: 2,
    createdAt: NOW,
  };

  test('create valida a aritmética (AC-LED-02)', () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Debit,
        money: brl('10.00'),
        balanceBefore: brl('100.00'),
        balanceAfter: brl('95.00'),
      }),
    ).toThrow(InvariantViolationError);
  });

  test('é estruturalmente imutável (AC-LED-01)', () => {
    const entry = WalletLedgerEntry.create({
      ...base,
      direction: LedgerDirection.Credit,
      money: brl('10.00'),
      balanceBefore: brl('0.00'),
      balanceAfter: brl('10.00'),
    });
    expect(Object.isFrozen(entry)).toBe(true);
    expect(() => {
      (entry as unknown as { walletVersion: number }).walletVersion = 99;
    }).toThrow();
  });

  test('rejeita valor não positivo e saldo negativo', () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Credit,
        money: brl('0.00'),
        balanceBefore: brl('0.00'),
        balanceAfter: brl('0.00'),
      }),
    ).toThrow(InvariantViolationError);
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Debit,
        money: brl('10.00'),
        balanceBefore: brl('5.00'),
        balanceAfter: Money.rehydrate({ amount: '-5.00', currency: 'BRL' }),
      }),
    ).toThrow(InvariantViolationError);
  });
});
