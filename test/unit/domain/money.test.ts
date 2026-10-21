import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InvalidMoneyError } from '../../../src/domain/shared/domain-error';
import { Money } from '../../../src/domain/shared/money';

describe('Money (AC-MON)', () => {
  describe('from — validação de contrato (AC-MON-01/02/03)', () => {
    test.each(['0.00', '0.01', '25.00', '1000.00', '999999999999999999.99'])('aceita "%s"', (amount) => {
      expect(Money.from({ amount, currency: 'BRL' }).toJSON()).toEqual({ amount, currency: 'BRL' });
    });

    test.each([
      ['string vazia', ''],
      ['NaN', 'NaN'],
      ['Infinity', 'Infinity'],
      ['-Infinity', '-Infinity'],
      ['notação científica', '1e3'],
      ['notação científica com casas', '1.00e2'],
      ['mais de 2 casas', '1.001'],
      ['1 casa', '1.0'],
      ['sem casas', '10'],
      ['negativo', '-1.00'],
      ['sinal +', '+1.00'],
      ['zero à esquerda', '01.00'],
      ['espaços', ' 1.00'],
      ['vírgula', '1,00'],
      ['hexadecimal', '0x10'],
      ['ponto inicial', '.50'],
      ['acima da precisão do NUMERIC(20,2)', '1000000000000000000.00'],
    ])('rejeita %s', (_label, amount) => {
      expect(() => Money.from({ amount, currency: 'BRL' })).toThrow(InvalidMoneyError);
    });

    test('rejeita amount que não é string (number nunca entra no domínio)', () => {
      expect(() => Money.from({ amount: 10 as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
    });

    test.each(['brl', 'BR', 'BRLL', '', '123'])('rejeita moeda inválida "%s"', (currency) => {
      expect(() => Money.from({ amount: '1.00', currency })).toThrow(InvalidMoneyError);
    });
  });

  describe('aritmética exata e imutável (AC-MON-04/07)', () => {
    test('0.10 + 0.20 == 0.30 (sem erro de ponto flutuante)', () => {
      const sum = Money.from({ amount: '0.10', currency: 'BRL' }).add(Money.from({ amount: '0.20', currency: 'BRL' }));
      expect(sum.toJSON().amount).toBe('0.30');
    });

    test('valores grandes preservam todas as casas', () => {
      const a = Money.from({ amount: '999999999999999998.99', currency: 'BRL' });
      const b = Money.from({ amount: '1.00', currency: 'BRL' });
      expect(a.add(b).toJSON().amount).toBe('999999999999999999.99');
    });

    test('operações retornam nova instância e não mutam a original', () => {
      const original = Money.from({ amount: '10.00', currency: 'BRL' });
      const result = original.subtract(Money.from({ amount: '3.50', currency: 'BRL' }));
      expect(result).not.toBe(original);
      expect(original.toJSON().amount).toBe('10.00');
      expect(result.toJSON().amount).toBe('6.50');
      expect(Object.isFrozen(original)).toBe(true);
    });

    test('subtract pode produzir negativo (usado em diferenças), negate inverte o sinal', () => {
      const diff = Money.from({ amount: '1.00', currency: 'BRL' }).subtract(
        Money.from({ amount: '2.50', currency: 'BRL' }),
      );
      expect(diff.isNegative()).toBe(true);
      expect(diff.toJSON().amount).toBe('-1.50');
      expect(diff.negate().toJSON().amount).toBe('1.50');
    });

    test('escala sempre fixa em 2 casas na serialização', () => {
      expect(Money.zero('BRL').toJSON()).toEqual({ amount: '0.00', currency: 'BRL' });
      expect(Money.from({ amount: '5.05', currency: 'BRL' }).toString()).toBe('5.05 BRL');
    });

    test('predicados', () => {
      const zero = Money.zero('BRL');
      const one = Money.from({ amount: '1.00', currency: 'BRL' });
      expect(zero.isZero()).toBe(true);
      expect(one.isPositive()).toBe(true);
      expect(zero.isLessThan(one)).toBe(true);
      expect(one.isLessThan(zero)).toBe(false);
      expect(one.equals(Money.from({ amount: '1.00', currency: 'BRL' }))).toBe(true);
      expect(one.equals(Money.from({ amount: '1.00', currency: 'USD' }))).toBe(false);
    });
  });

  describe('conflito de moeda (AC-MON-05)', () => {
    const brl = Money.from({ amount: '1.00', currency: 'BRL' });
    const usd = Money.from({ amount: '1.00', currency: 'USD' });

    test.each([
      ['add', () => brl.add(usd)],
      ['subtract', () => brl.subtract(usd)],
      ['isLessThan', () => brl.isLessThan(usd)],
    ])('%s entre moedas diferentes lança CurrencyMismatchError', (_op, operation) => {
      expect(operation).toThrow(CurrencyMismatchError);
    });
  });

  describe('rehydrate (fonte confiável)', () => {
    test('aceita negativo no formato estrito', () => {
      expect(Money.rehydrate({ amount: '-5.00', currency: 'BRL' }).isNegative()).toBe(true);
    });

    test('continua rejeitando formatos inexatos', () => {
      expect(() => Money.rehydrate({ amount: '5', currency: 'BRL' })).toThrow(InvalidMoneyError);
    });
  });
});
