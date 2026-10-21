import { describe, expect, test } from 'bun:test';
import { canonicalJson, computeWagerPayloadHash, WagerBusinessFields } from '../../../src/application/wagering/payload-hash';
import { LedgerCursor } from '../../../src/application/wallets/ledger-cursor';
import { InvalidPayloadError } from '../../../src/application/errors';

const fields: WagerBusinessFields = {
  providerId: 'provider-a',
  externalTransactionId: 'transaction-123',
  playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
  walletId: '0192f291-27dd-7d3f-8071-5f8685deef37',
  roundId: 'round-987',
  gameId: 'fortune-chimp',
  kind: 'BET',
  money: { amount: '25.00', currency: 'BRL' },
};

describe('payloadHash (AC-IDEM-02)', () => {
  test('JSON canônico ordena chaves recursivamente e omite undefined', () => {
    expect(canonicalJson({ b: 1, a: { d: 'x', c: [2, 1] }, z: undefined })).toBe('{"a":{"c":[2,1],"d":"x"},"b":1}');
  });

  test('mesma informação em ordem diferente → mesmo hash', () => {
    const reordered = {
      money: { currency: 'BRL', amount: '25.00' },
      kind: 'BET',
      gameId: 'fortune-chimp',
      roundId: 'round-987',
      walletId: fields.walletId,
      playerId: fields.playerId,
      externalTransactionId: 'transaction-123',
      providerId: 'provider-a',
    } as WagerBusinessFields;
    expect(computeWagerPayloadHash(reordered)).toBe(computeWagerPayloadHash(fields));
  });

  test('sha256 hexadecimal de 64 caracteres', () => {
    expect(computeWagerPayloadHash(fields)).toMatch(/^[0-9a-f]{64}$/);
  });

  test.each([
    ['amount', { money: { amount: '25.01', currency: 'BRL' } }],
    ['currency', { money: { amount: '25.00', currency: 'USD' } }],
    ['kind', { kind: 'WIN' }],
    ['roundId', { roundId: 'round-988' }],
    ['referência', { referenceExternalTransactionId: 'bet-1' }],
  ])('mudar %s muda o hash → mesma key vira conflito, não replay (AC-IDEM-04)', (_field, change) => {
    expect(computeWagerPayloadHash({ ...fields, ...change })).not.toBe(computeWagerPayloadHash(fields));
  });

  test('metadados de transporte não entram no hash', () => {
    const withTransport = { ...fields, idempotencyKey: 'other', messageId: 'm-1', occurredAt: 'now' } as WagerBusinessFields;
    expect(computeWagerPayloadHash(withTransport)).toBe(computeWagerPayloadHash(fields));
  });
});

describe('LedgerCursor (AC-API-03)', () => {
  test('round-trip opaco', () => {
    const cursor = LedgerCursor.encode(42);
    expect(cursor).not.toContain('42');
    expect(LedgerCursor.decode(cursor)).toBe(42);
    expect(LedgerCursor.decode(undefined)).toBeUndefined();
  });

  test.each(['garbage', Buffer.from('{"v":2,"after":1}').toString('base64url'), Buffer.from('{"v":1,"after":-1}').toString('base64url')])(
    'cursor inválido → InvalidPayloadError (%s)',
    (cursor) => {
      expect(() => LedgerCursor.decode(cursor)).toThrow(InvalidPayloadError);
    },
  );
});
