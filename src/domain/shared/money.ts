import { CurrencyMismatchError, InvalidMoneyError } from './domain-error';

export interface MoneyProps {
  amount: string;
  currency: string;
}

const SCALE = 2;
const CENTS_PER_UNIT = 100n;
const MAX_ABSOLUTE_CENTS = 10n ** 20n - 1n;
const CONTRACT_AMOUNT = /^(0|[1-9]\d{0,17})\.\d{2}$/;
const PERSISTED_AMOUNT = /^-?(0|[1-9]\d{0,17})\.\d{2}$/;
const CURRENCY = /^[A-Z]{3}$/;

export class Money {
  private constructor(
    private readonly cents: bigint,
    public readonly currency: string,
  ) {
    if (!Money.isWithinLimits(cents)) {
      throw new InvalidMoneyError('Amount exceeds the supported precision (18 integer digits)');
    }
    Object.freeze(this);
  }

  static from(props: MoneyProps): Money {
    Money.assertCurrency(props?.currency);
    if (typeof props.amount !== 'string' || !CONTRACT_AMOUNT.test(props.amount)) {
      throw new InvalidMoneyError(
        `Invalid amount "${String(props.amount)}": expected a non-negative decimal string with exactly ${SCALE} places`,
      );
    }
    return new Money(Money.parseCents(props.amount), props.currency);
  }

  static rehydrate(props: MoneyProps): Money {
    Money.assertCurrency(props?.currency);
    if (typeof props.amount !== 'string' || !PERSISTED_AMOUNT.test(props.amount)) {
      throw new InvalidMoneyError(`Invalid persisted amount "${String(props.amount)}"`);
    }
    return new Money(Money.parseCents(props.amount), props.currency);
  }

  static zero(currency: string): Money {
    Money.assertCurrency(currency);
    return new Money(0n, currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.cents + other.cents, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.cents - other.cents, this.currency);
  }

  canAdd(other: Money): boolean {
    this.assertSameCurrency(other);
    return Money.isWithinLimits(this.cents + other.cents);
  }

  negate(): Money {
    return new Money(-this.cents, this.currency);
  }

  isZero(): boolean {
    return this.cents === 0n;
  }

  isPositive(): boolean {
    return this.cents > 0n;
  }

  isNegative(): boolean {
    return this.cents < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.cents < other.cents;
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.cents === other.cents;
  }

  hasCurrency(currency: string): boolean {
    return this.currency === currency;
  }

  toJSON(): MoneyProps {
    return { amount: this.formatAmount(), currency: this.currency };
  }

  toString(): string {
    return `${this.formatAmount()} ${this.currency}`;
  }

  private formatAmount(): string {
    const negative = this.cents < 0n;
    const absolute = negative ? -this.cents : this.cents;
    const units = absolute / CENTS_PER_UNIT;
    const fraction = (absolute % CENTS_PER_UNIT).toString().padStart(SCALE, '0');
    return `${negative ? '-' : ''}${units}.${fraction}`;
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }

  private static isWithinLimits(cents: bigint): boolean {
    return cents <= MAX_ABSOLUTE_CENTS && cents >= -MAX_ABSOLUTE_CENTS;
  }

  private static parseCents(amount: string): bigint {
    return BigInt(amount.replace('.', ''));
  }

  private static assertCurrency(currency: unknown): asserts currency is string {
    if (typeof currency !== 'string' || !CURRENCY.test(currency)) {
      throw new InvalidMoneyError(`Invalid currency "${String(currency)}": expected ISO-4217 code (e.g. BRL)`);
    }
  }
}
