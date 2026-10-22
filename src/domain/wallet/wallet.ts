import {
  CurrencyMismatchError,
  InsufficientFundsError,
  InvalidWalletError,
  InvariantViolationError,
} from '../shared/domain-error';
import { Money } from '../shared/money';
import { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface BalanceMovement {
  transactionId: string;
  ledgerEntryId: string;
  money: Money;
  at: Date;
}

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  at: Date;
  openingTransactionId: string;
  openingLedgerEntryId: string;
}

export interface OpenedWallet {
  wallet: Wallet;
  openingEntry?: WalletLedgerEntry;
}

const MAX_PLAYER_ID_LENGTH = 128;

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: OpenWalletProps): OpenedWallet {
    const playerId = props.playerId?.trim();
    if (!playerId || playerId.length > MAX_PLAYER_ID_LENGTH) {
      throw new InvalidWalletError(`playerId must have between 1 and ${MAX_PLAYER_ID_LENGTH} characters`);
    }
    if (props.initialBalance.isNegative()) {
      throw new InvalidWalletError('Initial balance cannot be negative');
    }

    const currency = props.initialBalance.currency;
    const wallet = new Wallet(props.id, playerId, currency, props.initialBalance, 1, props.at, props.at);

    if (props.initialBalance.isZero()) {
      return { wallet };
    }

    const openingEntry = WalletLedgerEntry.create({
      id: props.openingLedgerEntryId,
      walletId: wallet.id,
      transactionId: props.openingTransactionId,
      walletVersion: 1,
      direction: LedgerDirection.Credit,
      money: props.initialBalance,
      balanceBefore: Money.zero(currency),
      balanceAfter: props.initialBalance,
      createdAt: props.at,
    });
    return { wallet, openingEntry };
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      new Date(state.createdAt),
      new Date(state.updatedAt),
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  canDebit(money: Money): boolean {
    this.assertSameCurrency(money);
    return !this._balance.isLessThan(money);
  }

  canCredit(money: Money): boolean {
    this.assertSameCurrency(money);
    return this._balance.canAdd(money);
  }

  debit(movement: BalanceMovement): WalletLedgerEntry {
    if (!this.canDebit(movement.money)) {
      throw new InsufficientFundsError(this.id);
    }
    return this.apply(LedgerDirection.Debit, movement);
  }

  credit(movement: BalanceMovement): WalletLedgerEntry {
    this.assertSameCurrency(movement.money);
    return this.apply(LedgerDirection.Credit, movement);
  }

  private apply(direction: LedgerDirection, movement: BalanceMovement): WalletLedgerEntry {
    if (!movement.money.isPositive()) {
      throw new InvariantViolationError('A balance movement must have a positive amount');
    }

    const balanceBefore = this._balance;
    const balanceAfter =
      direction === LedgerDirection.Credit ? balanceBefore.add(movement.money) : balanceBefore.subtract(movement.money);

    const entry = WalletLedgerEntry.create({
      id: movement.ledgerEntryId,
      walletId: this.id,
      transactionId: movement.transactionId,
      walletVersion: this._version + 1,
      direction,
      money: movement.money,
      balanceBefore,
      balanceAfter,
      createdAt: movement.at,
    });

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = movement.at;
    return entry;
  }

  private assertSameCurrency(money: Money): void {
    if (!money.hasCurrency(this.currency)) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
