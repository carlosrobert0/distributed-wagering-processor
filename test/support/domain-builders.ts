import { randomUUID } from 'node:crypto';
import { Money } from '../../src/domain/shared/money';
import { WagerTransaction, WagerTransactionKind } from '../../src/domain/wagering/wager-transaction';
import { Wallet } from '../../src/domain/wallet/wallet';

export const NOW = new Date('2026-10-21T12:00:00.000Z');

export const brl = (amount: string): Money => Money.from({ amount, currency: 'BRL' });
export const usd = (amount: string): Money => Money.from({ amount, currency: 'USD' });

export function openWallet(initial = '100.00', playerId = 'player-1', currency = 'BRL'): Wallet {
  return Wallet.open({
    id: randomUUID(),
    playerId,
    initialBalance: Money.from({ amount: initial, currency }),
    at: NOW,
    openingTransactionId: randomUUID(),
    openingLedgerEntryId: randomUUID(),
  }).wallet;
}

export interface TxOverrides {
  kind?: WagerTransactionKind;
  money?: Money;
  externalTransactionId?: string;
  referenceExternalTransactionId?: string;
  roundId?: string;
  playerId?: string;
  providerId?: string;
  payloadHash?: string;
}

export function newTransaction(wallet: Wallet, overrides: TxOverrides = {}): WagerTransaction {
  const externalTransactionId = overrides.externalTransactionId ?? `ext-${randomUUID()}`;
  const providerId = overrides.providerId ?? 'provider-a';
  return WagerTransaction.create({
    id: randomUUID(),
    providerId,
    externalTransactionId,
    idempotencyKey: `${providerId}:${externalTransactionId}`,
    payloadHash: overrides.payloadHash ?? 'hash',
    walletId: wallet.id,
    playerId: overrides.playerId ?? wallet.playerId,
    roundId: overrides.roundId ?? 'round-1',
    gameId: 'fortune-chimp',
    kind: overrides.kind ?? WagerTransactionKind.Bet,
    money: overrides.money ?? brl('10.00'),
    referenceExternalTransactionId: overrides.referenceExternalTransactionId,
    createdAt: NOW,
  });
}
